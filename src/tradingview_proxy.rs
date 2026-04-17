use axum::{
    extract::ws::{Message as AxumMessage, WebSocket, WebSocketUpgrade},
    response::Response,
};
use futures::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::sync::{broadcast, mpsc, Mutex, RwLock};
use tokio_tungstenite::{tungstenite::protocol::Message, MaybeTlsStream, WebSocketStream};
use tracing::{error, info, warn};

const TV_WS_URL: &str = "wss://data.tradingview.com/socket.io/websocket?type=chart";
const TV_ORIGIN: &str = "https://www.tradingview.com";
const DEFAULT_PROXY_ADDR: &str = "127.0.0.1:1080";
const MAX_CACHE_SIZE: usize = 3000;

const PERIODS: [(&str, &str); 6] = [
    ("1m", "1"),
    ("5m", "5"),
    ("30m", "30"),
    ("4h", "240"),
    ("1d", "1D"),
    ("1w", "1W"),
];

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Kline {
    pub time: i64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct HistoryPayload {
    pub symbol: String,
    pub data: Vec<Kline>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct UpdateLastPayload {
    pub symbol: String,
    pub kline: KlineUpdate,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct KlineUpdate {
    pub timestamp: i64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
}

const MESSAGE_TYPE_CUSTOM: u8 = 0x04;

fn build_custom_message(msg: &FrontendMessage) -> Vec<u8> {
    let json_string = serde_json::to_string(msg).unwrap_or_default();
    let json_bytes = json_string.as_bytes();

    let mut buffer = Vec::with_capacity(1 + 8 + 4 + json_bytes.len());
    buffer.push(MESSAGE_TYPE_CUSTOM);

    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as f64;
    buffer.extend_from_slice(&timestamp.to_be_bytes());
    buffer.extend_from_slice(&(json_bytes.len() as u32).to_be_bytes());
    buffer.extend_from_slice(json_bytes);

    buffer
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(tag = "type", content = "payload")]
pub enum FrontendMessage {
    #[serde(rename = "history")]
    History(HistoryPayload),
    #[serde(rename = "updateLast")]
    UpdateLast(UpdateLastPayload),
}

struct TvProtocol;

impl TvProtocol {
    fn format_packet(content: &Value) -> String {
        let msg = content.to_string();
        format!("~m~{}~m~{}", msg.len(), msg)
    }

    fn format_heartbeat(num: &str) -> String {
        let msg = format!("~h~{}", num);
        format!("~m~{}~m~{}", msg.len(), msg)
    }

    fn parse_packets(raw: &str) -> Vec<TvPacket> {
        let mut packets = Vec::new();
        let parts: Vec<&str> = raw.split("~m~").collect();
        let mut i = 1;

        while i < parts.len() {
            let len_str = parts[i];
            if let Ok(len) = len_str.parse::<usize>() {
                if i + 1 < parts.len() {
                    let content = parts[i + 1];
                    let actual_content = if content.len() > len {
                        &content[..len]
                    } else {
                        content
                    };

                    if let Some(stripped) = actual_content.strip_prefix("~h~") {
                        packets.push(TvPacket::Heartbeat(stripped.to_string()));
                    } else if let Ok(val) = serde_json::from_str::<Value>(actual_content) {
                        packets.push(TvPacket::Data(val));
                    }
                }
                i += 2;
            } else {
                if let Some(stripped) = len_str.strip_prefix("~h~") {
                    packets.push(TvPacket::Heartbeat(stripped.to_string()));
                }
                i += 1;
            }
        }

        if packets.is_empty() && raw.starts_with("~h~") {
            packets.push(TvPacket::Heartbeat(raw[3..].to_string()));
        }

        packets
    }
}

enum TvPacket {
    Heartbeat(String),
    Data(Value),
}

async fn connect_via_socks5_proxy(
    proxy_addr: &str,
) -> Result<WebSocketStream<MaybeTlsStream<TcpStream>>, Box<dyn std::error::Error + Send + Sync>> {
    use tokio_socks::tcp::Socks5Stream;
    use tokio_tungstenite::client_async_tls_with_config;

    info!("connecting TradingView via SOCKS5 {}", proxy_addr);

    let target_host = "data.tradingview.com";
    let target_port = 443u16;

    let socks_stream = Socks5Stream::connect(proxy_addr, (target_host, target_port)).await?;
    let tcp_stream = socks_stream.into_inner();

    let request = http::Request::builder()
        .method("GET")
        .uri(TV_WS_URL)
        .header("Host", target_host)
        .header("Origin", TV_ORIGIN)
        .header("User-Agent", "Mozilla/5.0")
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
    info!("TradingView websocket connected");
    Ok(ws_stream)
}

pub struct TradingViewProxy {
    broadcast_tx: broadcast::Sender<FrontendMessage>,
    sub_tx: mpsc::Sender<String>,
    sub_rx: Arc<Mutex<mpsc::Receiver<String>>>,
    cache: Arc<RwLock<HashMap<String, Vec<Kline>>>>,
    active_symbols: Arc<Mutex<HashSet<String>>>,
    ws_proxy_addr: String,
}

impl TradingViewProxy {
    pub fn new(proxy_addr: Option<&str>) -> Self {
        let (tx, _) = broadcast::channel(1000);
        let (sub_tx, sub_rx) = mpsc::channel(100);

        Self {
            broadcast_tx: tx,
            sub_tx,
            sub_rx: Arc::new(Mutex::new(sub_rx)),
            cache: Arc::new(RwLock::new(HashMap::new())),
            active_symbols: Arc::new(Mutex::new(HashSet::new())),
            ws_proxy_addr: proxy_addr.unwrap_or(DEFAULT_PROXY_ADDR).to_string(),
        }
    }

    pub async fn start(self: Arc<Self>) {
        info!("starting TradingView proxy");

        let proxy = self.clone();
        tokio::spawn(async move {
            proxy.run_subscription_manager().await;
        });
    }

    pub fn frontend_ws_upgrade(self: Arc<Self>, ws: WebSocketUpgrade) -> Response {
        let tx = self.broadcast_tx.clone();
        let sub_tx = self.sub_tx.clone();
        let cache = self.cache.clone();

        ws.on_upgrade(move |socket| async move {
            handle_frontend_connection(socket, tx, sub_tx, cache).await;
        })
    }

    async fn run_subscription_manager(&self) {
        let mut sub_rx = self.sub_rx.lock().await;

        while let Some(symbol) = sub_rx.recv().await {
            let should_spawn = {
                let mut active = self.active_symbols.lock().await;
                active.insert(symbol.clone())
            };

            if !should_spawn {
                info!(
                    "symbol {} is already active, skipping duplicate subscription",
                    symbol
                );
                continue;
            }

            let tx = self.broadcast_tx.clone();
            let cache = self.cache.clone();
            let active_symbols = self.active_symbols.clone();
            let ws_proxy_addr = self.ws_proxy_addr.clone();
            tokio::spawn(async move {
                Self::connect_and_stream_multi_period(
                    symbol,
                    tx,
                    cache,
                    active_symbols,
                    ws_proxy_addr,
                )
                .await;
            });
        }
    }

    async fn connect_and_stream_multi_period(
        symbol: String,
        broadcast_tx: broadcast::Sender<FrontendMessage>,
        cache: Arc<RwLock<HashMap<String, Vec<Kline>>>>,
        active_symbols: Arc<Mutex<HashSet<String>>>,
        ws_proxy_addr: String,
    ) {
        let result = async {
            let mut socket = connect_via_socks5_proxy(&ws_proxy_addr).await?;

            socket
                .send(Message::Text(
                    TvProtocol::format_packet(&json!({
                        "m": "set_auth_token",
                        "p": ["unauthorized_user_token"]
                    }))
                    .into(),
                ))
                .await
                .ok();

            let mut session_to_period = HashMap::new();
            for (period_name, tv_timeframe) in PERIODS {
                let session_id = format!("cs_{}", crate::utils::generate_random_string(12));
                session_to_period.insert(session_id.clone(), period_name.to_string());

                socket
                    .send(Message::Text(
                        TvProtocol::format_packet(&json!({
                            "m": "chart_create_session",
                            "p": [&session_id]
                        }))
                        .into(),
                    ))
                    .await
                    .ok();

                let series_id = "ser_1";
                socket
                    .send(Message::Text(
                        TvProtocol::format_packet(&json!({
                            "m": "resolve_symbol",
                            "p": [&session_id, series_id, format!("={}", json!({ "symbol": &symbol }))]
                        }))
                        .into(),
                    ))
                    .await
                    .ok();

                socket
                    .send(Message::Text(
                        TvProtocol::format_packet(&json!({
                            "m": "create_series",
                            "p": [&session_id, "$prices", "s1", series_id, tv_timeframe, 2000]
                        }))
                        .into(),
                    ))
                    .await
                    .ok();

                info!("{} subscribed period {}", symbol, period_name);
            }

            let mut history_sent: HashMap<String, bool> = PERIODS
                .iter()
                .map(|(period_name, _)| (period_name.to_string(), false))
                .collect();

            while let Some(msg) = socket.next().await {
                match msg {
                    Ok(Message::Text(text)) => {
                        for packet in TvProtocol::parse_packets(&text) {
                            match packet {
                                TvPacket::Heartbeat(num) => {
                                    socket
                                        .send(Message::Text(TvProtocol::format_heartbeat(&num).into()))
                                        .await
                                        .ok();
                                }
                                TvPacket::Data(val) => {
                                    Self::process_tv_data_by_session(
                                        &symbol,
                                        val,
                                        &broadcast_tx,
                                        &cache,
                                        &mut history_sent,
                                        &session_to_period,
                                    )
                                    .await;
                                }
                            }
                        }
                    }
                    Ok(Message::Close(_)) => {
                        info!("TradingView stream closed for {}", symbol);
                        break;
                    }
                    Err(e) => {
                        warn!("TradingView stream error for {}: {}", symbol, e);
                        break;
                    }
                    _ => {}
                }
            }

            Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
        }
        .await;

        if let Err(e) = result {
            error!("failed to stream {} from TradingView: {}", symbol, e);
        }

        active_symbols.lock().await.remove(&symbol);
        info!("TradingView task ended for {}", symbol);
    }

    async fn process_tv_data_by_session(
        symbol: &str,
        val: Value,
        broadcast_tx: &broadcast::Sender<FrontendMessage>,
        cache: &Arc<RwLock<HashMap<String, Vec<Kline>>>>,
        history_sent: &mut HashMap<String, bool>,
        session_to_period: &HashMap<String, String>,
    ) {
        let m = val.get("m").and_then(|v| v.as_str());
        let p = val.get("p").and_then(|v| v.as_array());

        match (m, p) {
            (Some("timescale_update"), Some(p)) if p.len() >= 2 => {
                let Some(session_id) = p[0].as_str() else {
                    return;
                };
                let Some(period_name) = session_to_period.get(session_id) else {
                    return;
                };
                let cache_key = format!("{}_{}", symbol, period_name);

                if let Some(prices) = p[1]
                    .get("$prices")
                    .and_then(|v| v.get("s"))
                    .and_then(|v| v.as_array())
                {
                    let is_first = !*history_sent.get(period_name).unwrap_or(&true);
                    if is_first {
                        let data = prices
                            .iter()
                            .filter_map(Self::parse_price_item)
                            .collect::<Vec<_>>();

                        if !data.is_empty() {
                            cache.write().await.insert(cache_key.clone(), data.clone());
                            let _ = broadcast_tx.send(FrontendMessage::History(HistoryPayload {
                                symbol: cache_key,
                                data,
                            }));
                            history_sent.insert(period_name.clone(), true);
                        }
                    } else {
                        for kline in prices.iter().filter_map(Self::parse_price_item) {
                            Self::apply_kline_update(cache, &cache_key, &kline).await;
                            let _ =
                                broadcast_tx.send(FrontendMessage::UpdateLast(UpdateLastPayload {
                                    symbol: cache_key.clone(),
                                    kline: Self::to_update_payload(&kline),
                                }));
                        }
                    }
                }
            }
            (Some("du"), Some(p)) if p.len() >= 2 => {
                let Some(session_id) = p[0].as_str() else {
                    return;
                };
                let Some(period_name) = session_to_period.get(session_id) else {
                    return;
                };
                let cache_key = format!("{}_{}", symbol, period_name);

                if let Some(prices) = p[1]
                    .get("$prices")
                    .and_then(|v| v.get("s"))
                    .and_then(|v| v.as_array())
                {
                    for kline in prices.iter().filter_map(Self::parse_price_item) {
                        Self::apply_kline_update(cache, &cache_key, &kline).await;
                        let _ = broadcast_tx.send(FrontendMessage::UpdateLast(UpdateLastPayload {
                            symbol: cache_key.clone(),
                            kline: Self::to_update_payload(&kline),
                        }));
                    }
                }
            }
            _ => {}
        }
    }

    fn parse_price_item(item: &Value) -> Option<Kline> {
        let values = item.get("v")?.as_array()?;
        if values.len() < 6 {
            return None;
        }

        Some(Kline {
            time: values[0].as_f64().unwrap_or(0.0) as i64,
            open: values[1].as_f64().unwrap_or(0.0),
            high: values[2].as_f64().unwrap_or(0.0),
            low: values[3].as_f64().unwrap_or(0.0),
            close: values[4].as_f64().unwrap_or(0.0),
            volume: values[5].as_f64().unwrap_or(0.0),
        })
    }

    fn to_update_payload(kline: &Kline) -> KlineUpdate {
        KlineUpdate {
            timestamp: kline.time * 1000,
            open: kline.open,
            high: kline.high,
            low: kline.low,
            close: kline.close,
            volume: kline.volume,
        }
    }

    async fn apply_kline_update(
        cache: &Arc<RwLock<HashMap<String, Vec<Kline>>>>,
        cache_key: &str,
        kline: &Kline,
    ) {
        let mut cache_guard = cache.write().await;
        let arr = cache_guard.entry(cache_key.to_string()).or_default();

        if let Some(last) = arr.last_mut() {
            if last.time == kline.time {
                *last = kline.clone();
                return;
            }
            if last.time < kline.time {
                arr.push(kline.clone());
                if arr.len() > MAX_CACHE_SIZE {
                    arr.remove(0);
                }
                return;
            }
        }

        if let Some(existing) = arr.iter_mut().find(|existing| existing.time == kline.time) {
            *existing = kline.clone();
            return;
        }

        arr.push(kline.clone());
        arr.sort_by_key(|entry| entry.time);
        if arr.len() > MAX_CACHE_SIZE {
            let overflow = arr.len() - MAX_CACHE_SIZE;
            arr.drain(..overflow);
        }
    }
}

async fn handle_frontend_connection(
    socket: WebSocket,
    broadcast_tx: broadcast::Sender<FrontendMessage>,
    sub_tx: mpsc::Sender<String>,
    cache: Arc<RwLock<HashMap<String, Vec<Kline>>>>,
) {
    info!("TradingView frontend connected");
    let (mut write, mut read) = socket.split();
    let mut rx = broadcast_tx.subscribe();

    loop {
        tokio::select! {
            msg = read.next() => {
                match msg {
                    Some(Ok(AxumMessage::Text(text))) => {
                        if let Ok(val) = serde_json::from_str::<Value>(&text) {
                            if val.is_array() && val[0] == "addSubscriptions" {
                                if let Some(symbols) = val[1].get("symbols").and_then(|s| s.as_array()) {
                                    for sym in symbols {
                                        if let Some(s) = sym.as_str() {
                                            let _ = sub_tx.send(s.to_string()).await;
                                        }
                                    }
                                }
                            } else if val.is_array() && val[0] == "getHistory" {
                                let symbol = val[1].get("symbol").and_then(|s| s.as_str()).unwrap_or("");
                                let period = val[1].get("period").and_then(|s| s.as_str()).unwrap_or("1m");
                                let cache_key = format!("{}_{}", symbol, period);

                                let cache_guard = cache.read().await;
                                let data = cache_guard.get(&cache_key).cloned().unwrap_or_default();
                                let msg = FrontendMessage::History(HistoryPayload {
                                    symbol: cache_key,
                                    data,
                                });
                                let binary_msg = build_custom_message(&msg);
                                if write.send(AxumMessage::Binary(binary_msg.into())).await.is_err() {
                                    break;
                                }
                            }
                        }
                    }
                    Some(Ok(AxumMessage::Ping(payload))) => {
                        if write.send(AxumMessage::Pong(payload)).await.is_err() {
                            break;
                        }
                    }
                    Some(Ok(AxumMessage::Close(_))) | None => {
                        info!("TradingView frontend disconnected");
                        break;
                    }
                    Some(Err(e)) => {
                        warn!("TradingView frontend error: {}", e);
                        break;
                    }
                    _ => {}
                }
            }
            Ok(msg) = rx.recv() => {
                let binary_msg = build_custom_message(&msg);
                if write.send(AxumMessage::Binary(binary_msg.into())).await.is_err() {
                    break;
                }
            }
        }
    }
}
