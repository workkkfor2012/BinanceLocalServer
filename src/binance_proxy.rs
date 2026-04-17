use crate::config::BinanceConfig;
use axum::{
    extract::ws::{Message as AxumMessage, WebSocket, WebSocketUpgrade},
    response::Response,
};
use futures::{SinkExt, StreamExt};
use serde::Serialize;
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::sync::{broadcast, mpsc, Mutex, RwLock};
use tokio::time::{interval, Duration, Instant};
use tokio_tungstenite::{
    tungstenite::protocol::Message as TungsteniteMessage, MaybeTlsStream, WebSocketStream,
};
use tracing::{debug, error, info, warn};

const PROXY_WS_URL: &str = "wss://fstream.binance.com/ws";
const SOCKS5_PROXY: &str = "127.0.0.1:1080";
const CONNECTION_LIFETIME_SECS: u64 = 23 * 3600;
const KEEPALIVE_PING_INTERVAL_SECS: u64 = 20;
const BROADCAST_CAPACITY: usize = 10000;
const PENDING_MSG_CAPACITY: usize = 5000;
const RECONNECT_BACKOFF_MIN_SECS: u64 = 1;
const RECONNECT_BACKOFF_MAX_SECS: u64 = 30;

#[derive(Debug, Clone)]
enum SubscriptionRequest {
    Subscribe {
        client_id: u64,
        streams: Vec<String>,
        request_id: Option<u64>,
    },
    Unsubscribe {
        client_id: u64,
        streams: Vec<String>,
        request_id: Option<u64>,
    },
    ListSubscriptions {
        client_id: u64,
        request_id: u64,
    },
    ClientDisconnected {
        client_id: u64,
    },
}

#[derive(Debug, Clone, Serialize)]
#[serde(untagged)]
enum FrontendResponse {
    BinanceData(Value),
    SubscriptionResult { result: Option<Value>, id: u64 },
    ListResult { result: Vec<String>, id: u64 },
}

#[derive(Debug, Default)]
struct SubscriptionState {
    stream_clients: HashMap<String, HashSet<u64>>,
    client_streams: HashMap<u64, HashSet<String>>,
}

impl SubscriptionState {
    fn add(&mut self, client_id: u64, streams: &[String]) -> Vec<String> {
        let mut new_streams = Vec::new();

        for stream in streams {
            let clients = self.stream_clients.entry(stream.clone()).or_default();
            if clients.is_empty() {
                new_streams.push(stream.clone());
            }
            clients.insert(client_id);
            self.client_streams
                .entry(client_id)
                .or_default()
                .insert(stream.clone());
        }

        new_streams
    }

    fn remove(&mut self, client_id: u64, streams: &[String]) -> Vec<String> {
        let mut removed_streams = Vec::new();

        for stream in streams {
            let should_remove_stream = if let Some(clients) = self.stream_clients.get_mut(stream) {
                clients.remove(&client_id);
                clients.is_empty()
            } else {
                false
            };

            if should_remove_stream {
                self.stream_clients.remove(stream);
                removed_streams.push(stream.clone());
            }

            let should_remove_client =
                if let Some(client_subs) = self.client_streams.get_mut(&client_id) {
                    client_subs.remove(stream);
                    client_subs.is_empty()
                } else {
                    false
                };

            if should_remove_client {
                self.client_streams.remove(&client_id);
            }
        }

        removed_streams
    }

    fn client_disconnected(&mut self, client_id: u64) -> Vec<String> {
        let streams = self
            .client_streams
            .get(&client_id)
            .map(|set| set.iter().cloned().collect::<Vec<_>>())
            .unwrap_or_default();
        self.remove(client_id, &streams)
    }

    fn all_streams(&self) -> Vec<String> {
        let mut streams = self.stream_clients.keys().cloned().collect::<Vec<_>>();
        streams.sort();
        streams
    }

    fn streams_for_client(&self, client_id: u64) -> Vec<String> {
        let mut streams = self
            .client_streams
            .get(&client_id)
            .map(|set| set.iter().cloned().collect::<Vec<_>>())
            .unwrap_or_default();
        streams.sort();
        streams
    }
}

pub struct BinanceProxy {
    broadcast_tx: broadcast::Sender<(Option<u64>, FrontendResponse)>,
    sub_tx: mpsc::Sender<SubscriptionRequest>,
    sub_rx: Arc<Mutex<mpsc::Receiver<SubscriptionRequest>>>,
    state: Arc<RwLock<SubscriptionState>>,
    client_id_counter: Arc<std::sync::atomic::AtomicU64>,
    binance_tx: Arc<Mutex<Option<mpsc::Sender<String>>>>,
    pending_messages: Arc<Mutex<VecDeque<String>>>,
    ws_url: String,
    ws_proxy_addr: String,
}

impl BinanceProxy {
    pub fn new(config: Option<&BinanceConfig>) -> Self {
        let (broadcast_tx, _) = broadcast::channel(BROADCAST_CAPACITY);
        let (sub_tx, sub_rx) = mpsc::channel(1000);

        Self {
            broadcast_tx,
            sub_tx,
            sub_rx: Arc::new(Mutex::new(sub_rx)),
            state: Arc::new(RwLock::new(SubscriptionState::default())),
            client_id_counter: Arc::new(std::sync::atomic::AtomicU64::new(1)),
            binance_tx: Arc::new(Mutex::new(None)),
            pending_messages: Arc::new(Mutex::new(VecDeque::new())),
            ws_url: config
                .map(|cfg| cfg.proxy_public_ws_url())
                .unwrap_or_else(|| PROXY_WS_URL.to_string()),
            ws_proxy_addr: config
                .map(|cfg| cfg.ws_proxy_addr().to_string())
                .unwrap_or_else(|| SOCKS5_PROXY.to_string()),
        }
    }

    pub async fn start(self: Arc<Self>) {
        info!("starting binance websocket proxy");

        let proxy = self.clone();
        tokio::spawn(async move {
            proxy.run_subscription_manager().await;
        });

        let proxy = self.clone();
        tokio::spawn(async move {
            proxy.run_binance_connection().await;
        });
    }

    pub fn frontend_ws_upgrade(self: Arc<Self>, ws: WebSocketUpgrade) -> Response {
        let client_id = self
            .client_id_counter
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let broadcast_rx = self.broadcast_tx.subscribe();
        let sub_tx = self.sub_tx.clone();

        ws.on_upgrade(move |socket| async move {
            handle_frontend_socket(socket, client_id, broadcast_rx, sub_tx).await;
        })
    }

    async fn run_subscription_manager(&self) {
        let mut sub_rx = self.sub_rx.lock().await;

        while let Some(request) = sub_rx.recv().await {
            match request {
                SubscriptionRequest::Subscribe {
                    client_id,
                    streams,
                    request_id,
                } => {
                    let new_streams = {
                        let mut state = self.state.write().await;
                        state.add(client_id, &streams)
                    };

                    if !new_streams.is_empty() {
                        info!("subscribing {} new streams", new_streams.len());
                        self.send_to_binance("SUBSCRIBE", &new_streams).await;
                    }

                    if let Some(id) = request_id {
                        let _ = self.broadcast_tx.send((
                            Some(client_id),
                            FrontendResponse::SubscriptionResult { result: None, id },
                        ));
                    }
                }
                SubscriptionRequest::Unsubscribe {
                    client_id,
                    streams,
                    request_id,
                } => {
                    let removed_streams = {
                        let mut state = self.state.write().await;
                        state.remove(client_id, &streams)
                    };

                    if !removed_streams.is_empty() {
                        info!("unsubscribing {} streams", removed_streams.len());
                        self.send_to_binance("UNSUBSCRIBE", &removed_streams).await;
                    }

                    if let Some(id) = request_id {
                        let _ = self.broadcast_tx.send((
                            Some(client_id),
                            FrontendResponse::SubscriptionResult { result: None, id },
                        ));
                    }
                }
                SubscriptionRequest::ListSubscriptions {
                    client_id,
                    request_id,
                } => {
                    let streams = {
                        let state = self.state.read().await;
                        state.streams_for_client(client_id)
                    };

                    let _ = self.broadcast_tx.send((
                        Some(client_id),
                        FrontendResponse::ListResult {
                            result: streams,
                            id: request_id,
                        },
                    ));
                }
                SubscriptionRequest::ClientDisconnected { client_id } => {
                    let removed_streams = {
                        let mut state = self.state.write().await;
                        state.client_disconnected(client_id)
                    };

                    if !removed_streams.is_empty() {
                        info!(
                            "client {} disconnected, removing {} streams",
                            client_id,
                            removed_streams.len()
                        );
                        self.send_to_binance("UNSUBSCRIBE", &removed_streams).await;
                    }
                }
            }
        }
    }

    async fn send_to_binance(&self, method: &str, streams: &[String]) {
        let msg = json!({
            "method": method,
            "params": streams,
            "id": rand::random::<u32>()
        })
        .to_string();

        let tx = {
            let guard = self.binance_tx.lock().await;
            guard.as_ref().cloned()
        };

        if let Some(tx) = tx {
            if let Err(e) = tx.send(msg.clone()).await {
                warn!("failed to send message to binance; queued for retry: {}", e);
                self.enqueue_pending_message(msg).await;
            }
        } else {
            self.enqueue_pending_message(msg).await;
            warn!("binance connection is not ready; message queued for replay");
        }
    }

    async fn enqueue_pending_message(&self, msg: String) {
        let mut pending = self.pending_messages.lock().await;
        if pending.len() >= PENDING_MSG_CAPACITY {
            pending.pop_front();
            warn!("pending message queue is full; dropping oldest message");
        }
        pending.push_back(msg);
    }

    fn parse_stream_delta(msg: &str) -> Option<(String, Vec<String>)> {
        let value: Value = serde_json::from_str(msg).ok()?;
        let method = value.get("method")?.as_str()?.to_string();
        let streams = value
            .get("params")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        Some((method, streams))
    }

    async fn flush_pending_messages(&self, tx: &mpsc::Sender<String>) -> HashSet<String> {
        let pending = {
            let mut queue = self.pending_messages.lock().await;
            queue.drain(..).collect::<Vec<_>>()
        };

        let mut simulated_remote_subscriptions = HashSet::new();
        if pending.is_empty() {
            return simulated_remote_subscriptions;
        }

        for msg in &pending {
            if let Some((method, streams)) = Self::parse_stream_delta(msg) {
                match method.as_str() {
                    "SUBSCRIBE" => {
                        for stream in streams {
                            simulated_remote_subscriptions.insert(stream);
                        }
                    }
                    "UNSUBSCRIBE" => {
                        for stream in streams {
                            simulated_remote_subscriptions.remove(&stream);
                        }
                    }
                    _ => {}
                }
            }
        }

        info!("replaying {} queued messages", pending.len());
        for (idx, msg) in pending.iter().enumerate() {
            if tx.send(msg.clone()).await.is_err() {
                warn!(
                    "queued message replay interrupted, requeueing {} messages",
                    pending.len().saturating_sub(idx)
                );
                let mut queue = self.pending_messages.lock().await;
                for rest in pending.iter().skip(idx) {
                    if queue.len() >= PENDING_MSG_CAPACITY {
                        queue.pop_front();
                    }
                    queue.push_back(rest.clone());
                }
                return HashSet::new();
            }
        }

        simulated_remote_subscriptions
    }

    async fn restore_subscriptions_after_reconnect(
        &self,
        replayed_subscriptions: &HashSet<String>,
    ) {
        let streams = {
            let state = self.state.read().await;
            state.all_streams()
        };

        if streams.is_empty() && replayed_subscriptions.is_empty() {
            return;
        }

        if !streams.is_empty() {
            info!("restoring {} subscriptions", streams.len());
            for chunk in streams.chunks(200) {
                self.send_to_binance("SUBSCRIBE", chunk).await;
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }

        let target_set: HashSet<String> = streams.iter().cloned().collect();
        let stale_streams: Vec<String> = replayed_subscriptions
            .difference(&target_set)
            .cloned()
            .collect();
        if !stale_streams.is_empty() {
            info!("cleaning up {} stale subscriptions", stale_streams.len());
            for chunk in stale_streams.chunks(200) {
                self.send_to_binance("UNSUBSCRIBE", chunk).await;
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }

    async fn run_binance_connection(&self) {
        let mut reconnect_backoff = Duration::from_secs(RECONNECT_BACKOFF_MIN_SECS);

        loop {
            let connection_start = Instant::now();
            match self.try_connect().await {
                Ok(ws_stream) => {
                    info!("connected to binance through proxy");
                    reconnect_backoff = Duration::from_secs(RECONNECT_BACKOFF_MIN_SECS);
                    self.run_binance_loop(ws_stream, connection_start).await;
                }
                Err(e) => {
                    error!(
                        "failed to connect to binance: {}; retrying in {} seconds",
                        e,
                        reconnect_backoff.as_secs()
                    );
                    tokio::time::sleep(reconnect_backoff).await;
                    let next_secs = reconnect_backoff
                        .as_secs()
                        .saturating_mul(2)
                        .min(RECONNECT_BACKOFF_MAX_SECS);
                    reconnect_backoff =
                        Duration::from_secs(next_secs.max(RECONNECT_BACKOFF_MIN_SECS));
                }
            }
        }
    }

    async fn try_connect(
        &self,
    ) -> Result<WebSocketStream<MaybeTlsStream<TcpStream>>, Box<dyn std::error::Error + Send + Sync>>
    {
        info!("connecting to {}", self.ws_url);
        match tokio::time::timeout(
            Duration::from_secs(15),
            connect_via_socks5_proxy(&self.ws_url, &self.ws_proxy_addr),
        )
        .await
        {
            Ok(Ok(ws)) => Ok(ws),
            Ok(Err(e)) => Err(e),
            Err(_) => Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "SOCKS5 proxy connection timed out",
            )
            .into()),
        }
    }

    async fn run_binance_loop(
        &self,
        ws_stream: WebSocketStream<MaybeTlsStream<TcpStream>>,
        connection_start: Instant,
    ) {
        let (mut write, mut read) = ws_stream.split();
        let (tx, mut rx) = mpsc::channel::<String>(1000);
        *self.binance_tx.lock().await = Some(tx);

        let tx_for_flush = {
            let guard = self.binance_tx.lock().await;
            guard.as_ref().cloned()
        };
        let replayed_subscriptions = if let Some(active_tx) = tx_for_flush.as_ref() {
            self.flush_pending_messages(active_tx).await
        } else {
            HashSet::new()
        };
        self.restore_subscriptions_after_reconnect(&replayed_subscriptions)
            .await;

        let mut lifetime_check = interval(Duration::from_secs(300));
        let mut keepalive_ping = interval(Duration::from_secs(KEEPALIVE_PING_INTERVAL_SECS));
        keepalive_ping.tick().await;

        loop {
            tokio::select! {
                msg = read.next() => {
                    match msg {
                        Some(Ok(TungsteniteMessage::Text(text))) => {
                            if let Ok(data) = serde_json::from_str::<Value>(&text) {
                                let _ = self.broadcast_tx.send((None, FrontendResponse::BinanceData(data)));
                            }
                        }
                        Some(Ok(TungsteniteMessage::Ping(payload))) => {
                            debug!("received Ping, sending Pong");
                            if let Err(e) = write.send(TungsteniteMessage::Pong(payload)).await {
                                error!("failed to send Pong: {}", e);
                                break;
                            }
                        }
                        Some(Ok(TungsteniteMessage::Close(_))) => {
                            warn!("binance closed the websocket connection");
                            break;
                        }
                        Some(Err(e)) => {
                            error!("binance websocket error: {}", e);
                            break;
                        }
                        None => {
                            warn!("binance websocket closed");
                            break;
                        }
                        _ => {}
                    }
                }
                Some(msg) = rx.recv() => {
                    if let Err(e) = write.send(TungsteniteMessage::Text(msg.clone().into())).await {
                        error!("failed to send message to binance: {}", e);
                        self.enqueue_pending_message(msg).await;
                        break;
                    }
                }
                _ = lifetime_check.tick() => {
                    if connection_start.elapsed() > Duration::from_secs(CONNECTION_LIFETIME_SECS) {
                        info!("connection lifetime reached; reconnecting");
                        break;
                    }
                }
                _ = keepalive_ping.tick() => {
                    debug!(
                        "sending upstream keepalive ping after {}s interval",
                        KEEPALIVE_PING_INTERVAL_SECS
                    );
                    if let Err(e) = write.send(TungsteniteMessage::Ping(Vec::new().into())).await {
                        error!("failed to send upstream keepalive ping: {}", e);
                        break;
                    }
                }
            }
        }

        *self.binance_tx.lock().await = None;
    }
}

async fn connect_via_socks5_proxy(
    ws_url: &str,
    proxy_addr: &str,
) -> Result<WebSocketStream<MaybeTlsStream<TcpStream>>, Box<dyn std::error::Error + Send + Sync>> {
    use tokio_socks::tcp::Socks5Stream;
    use tokio_tungstenite::client_async_tls_with_config;

    let url = url::Url::parse(ws_url)?;
    let host = url.host_str().ok_or("invalid host")?;
    let port = url.port().unwrap_or(443);

    info!(
        "connecting via SOCKS5 proxy {} to {}:{}",
        proxy_addr, host, port
    );

    let socks_stream = Socks5Stream::connect(proxy_addr, (host, port)).await?;
    let tcp_stream = socks_stream.into_inner();

    let request = http::Request::builder()
        .method("GET")
        .uri(ws_url)
        .header("Host", host)
        .header("Upgrade", "websocket")
        .header("Connection", "Upgrade")
        .header(
            "Sec-WebSocket-Key",
            tokio_tungstenite::tungstenite::handshake::client::generate_key(),
        )
        .header("Sec-WebSocket-Version", "13")
        .body(())?;

    let connector = tokio_tungstenite::Connector::Rustls(Arc::new(
        rustls::ClientConfig::builder()
            .with_root_certificates(rustls::RootCertStore {
                roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
            })
            .with_no_client_auth(),
    ));

    let (ws_stream, _) =
        client_async_tls_with_config(request, tcp_stream, None, Some(connector)).await?;
    Ok(ws_stream)
}

async fn handle_frontend_socket(
    socket: WebSocket,
    client_id: u64,
    mut broadcast_rx: broadcast::Receiver<(Option<u64>, FrontendResponse)>,
    sub_tx: mpsc::Sender<SubscriptionRequest>,
) {
    info!("frontend client connected (ID: {})", client_id);
    let (mut write, mut read) = socket.split();

    loop {
        tokio::select! {
            msg = read.next() => {
                match msg {
                    Some(Ok(AxumMessage::Text(text))) => {
                        if let Ok(val) = serde_json::from_str::<Value>(text.as_str()) {
                            handle_frontend_message(client_id, val, &sub_tx).await;
                        }
                    }
                    Some(Ok(AxumMessage::Ping(payload))) => {
                        if let Err(e) = write.send(AxumMessage::Pong(payload)).await {
                            warn!("failed to send Pong to frontend client {}: {}", client_id, e);
                            break;
                        }
                    }
                    Some(Ok(AxumMessage::Close(_))) | None => break,
                    Some(Err(e)) => {
                        warn!("frontend client {} websocket error: {}", client_id, e);
                        break;
                    }
                    _ => {}
                }
            }
            msg = broadcast_rx.recv() => {
                match msg {
                    Ok((target_client, response)) => {
                        if let Some(target) = target_client {
                            if target != client_id {
                                continue;
                            }
                        }

                        if let Ok(json) = serde_json::to_string(&response) {
                            if let Err(e) = write.send(AxumMessage::Text(json.into())).await {
                                warn!("failed to send message to frontend client {}: {}", client_id, e);
                                break;
                            }
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        warn!("client {} lagged by {} messages", client_id, n);
                    }
                    Err(_) => break,
                }
            }
        }
    }

    let _ = sub_tx
        .send(SubscriptionRequest::ClientDisconnected { client_id })
        .await;
    info!("frontend client disconnected (ID: {})", client_id);
}

async fn handle_frontend_message(
    client_id: u64,
    val: Value,
    sub_tx: &mpsc::Sender<SubscriptionRequest>,
) {
    let method = val.get("method").and_then(|v| v.as_str());
    let params = val.get("params").and_then(|v| v.as_array());
    let request_id = val.get("id").and_then(|v| v.as_u64());

    match method {
        Some("SUBSCRIBE") => {
            if let Some(streams) = params {
                let streams = streams
                    .iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect::<Vec<_>>();
                debug!("client {} subscribe: {:?}", client_id, streams);
                let _ = sub_tx
                    .send(SubscriptionRequest::Subscribe {
                        client_id,
                        streams,
                        request_id,
                    })
                    .await;
            }
        }
        Some("UNSUBSCRIBE") => {
            if let Some(streams) = params {
                let streams = streams
                    .iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect::<Vec<_>>();
                debug!("client {} unsubscribe: {:?}", client_id, streams);
                let _ = sub_tx
                    .send(SubscriptionRequest::Unsubscribe {
                        client_id,
                        streams,
                        request_id,
                    })
                    .await;
            }
        }
        Some("LIST_SUBSCRIPTIONS") => {
            if let Some(id) = request_id {
                let _ = sub_tx
                    .send(SubscriptionRequest::ListSubscriptions {
                        client_id,
                        request_id: id,
                    })
                    .await;
            }
        }
        _ => {
            debug!("client {} sent unknown message: {}", client_id, val);
        }
    }
}
