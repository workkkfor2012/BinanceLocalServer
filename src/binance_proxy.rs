// src/binance_proxy.rs
//! 币安 WebSocket 代理模块
//!
//! 该模块负责：
//! 1. 维护与币安服务器的中心化 WebSocket 连接
//! 2. 监听本地端口 6002，接受前端连接
//! 3. 管理订阅状态（引用计数）
//! 4. 广播行情数据给所有前端客户端

use futures::{SinkExt, StreamExt};
use serde::Serialize;
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, mpsc, Mutex, RwLock};
use tokio::time::{interval, Duration, Instant};
use tokio_tungstenite::{
    accept_async,
    connect_async,
    tungstenite::protocol::Message,
    MaybeTlsStream,
    WebSocketStream,
};
use tracing::{debug, error, info, warn};

// --- 常量配置 ---

/// 直连端点（无需翻墙，但有订阅密度限制）
const DIRECT_WS_URL: &str = "wss://fstream.mokexapp.org/ws";
/// 代理端点（需要翻墙，无订阅限制）
const PROXY_WS_URL: &str = "wss://fstream.binance.com/ws";
/// SOCKS5 代理地址
const SOCKS5_PROXY: &str = "127.0.0.1:1080";
/// 本地前端服务端口
const LOCAL_SERVER_PORT: u16 = 6002;
/// 连接有效期（24小时后需要重连）
const CONNECTION_LIFETIME_SECS: u64 = 23 * 3600; // 23小时，留出缓冲
/// 广播通道容量
const BROADCAST_CAPACITY: usize = 10000;
/// 断线期间待发送消息队列容量（超出会丢弃最旧消息）
const PENDING_MSG_CAPACITY: usize = 5000;
/// 重连最小退避秒数
const RECONNECT_BACKOFF_MIN_SECS: u64 = 1;
/// 重连最大退避秒数
const RECONNECT_BACKOFF_MAX_SECS: u64 = 30;

// --- 数据结构 ---

/// 订阅请求类型
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

/// 发送给前端的响应消息
#[derive(Debug, Clone, Serialize)]
#[serde(untagged)]
enum FrontendResponse {
    /// 币安原始数据（K线更新等）
    BinanceData(Value),
    /// 订阅响应
    SubscriptionResult {
        result: Option<Value>,
        id: u64,
    },
    /// 订阅列表响应
    ListResult {
        result: Vec<String>,
        id: u64,
    },
    /// 错误响应
    ErrorResult {
        error: BinanceError,
        id: u64,
    },
}

#[derive(Debug, Clone, Serialize)]
struct BinanceError {
    code: i32,
    msg: String,
}

/// 订阅状态：记录每个 stream 被哪些客户端订阅
#[derive(Debug, Default)]
struct SubscriptionState {
    /// stream_name -> Set<client_id>
    stream_clients: HashMap<String, Vec<u64>>,
    /// client_id -> Set<stream_name>
    client_streams: HashMap<u64, Vec<String>>,
}

impl SubscriptionState {
    /// 添加订阅，返回新增的 streams（需要向币安订阅的）
    fn add(&mut self, client_id: u64, streams: &[String]) -> Vec<String> {
        let mut new_streams = Vec::new();
        
        for stream in streams {
            let clients = self.stream_clients.entry(stream.clone()).or_default();
            if clients.is_empty() {
                new_streams.push(stream.clone());
            }
            if !clients.contains(&client_id) {
                clients.push(client_id);
            }
            
            let client_subs = self.client_streams.entry(client_id).or_default();
            if !client_subs.contains(stream) {
                client_subs.push(stream.clone());
            }
        }
        
        new_streams
    }
    
    /// 移除订阅，返回需要向币安取消的 streams
    fn remove(&mut self, client_id: u64, streams: &[String]) -> Vec<String> {
        let mut removed_streams = Vec::new();
        
        for stream in streams {
            if let Some(clients) = self.stream_clients.get_mut(stream) {
                clients.retain(|&id| id != client_id);
                if clients.is_empty() {
                    self.stream_clients.remove(stream);
                    removed_streams.push(stream.clone());
                }
            }
            
            if let Some(client_subs) = self.client_streams.get_mut(&client_id) {
                client_subs.retain(|s| s != stream);
            }
        }
        
        removed_streams
    }
    
    /// 客户端断开，移除该客户端所有订阅，返回需要向币安取消的 streams
    fn client_disconnected(&mut self, client_id: u64) -> Vec<String> {
        let streams = self.client_streams.remove(&client_id).unwrap_or_default();
        self.remove(client_id, &streams)
    }
    
    /// 获取当前所有活跃的 streams
    fn all_streams(&self) -> Vec<String> {
        self.stream_clients.keys().cloned().collect()
    }
}

// --- 币安代理核心 ---

pub struct BinanceProxy {
    /// 广播通道：向所有前端客户端广播数据
    broadcast_tx: broadcast::Sender<(Option<u64>, FrontendResponse)>,
    /// 订阅请求通道
    sub_tx: mpsc::Sender<SubscriptionRequest>,
    /// 订阅请求接收端
    sub_rx: Arc<Mutex<mpsc::Receiver<SubscriptionRequest>>>,
    /// 订阅状态
    state: Arc<RwLock<SubscriptionState>>,
    /// 客户端 ID 计数器
    client_id_counter: Arc<std::sync::atomic::AtomicU64>,
    /// 向币安发送消息的通道
    binance_tx: Arc<Mutex<Option<mpsc::Sender<String>>>>,
    /// 断线期间待发送到币安的消息
    pending_messages: Arc<Mutex<VecDeque<String>>>,
}

impl BinanceProxy {
    pub fn new() -> Self {
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
        }
    }
    
    pub async fn start(self: Arc<Self>) {
        info!("🚀 启动币安 WebSocket 代理服务...");
        
        // 启动订阅管理器
        let proxy_clone = self.clone();
        tokio::spawn(async move {
            proxy_clone.run_subscription_manager().await;
        });
        
        // 启动币安连接
        let proxy_clone = self.clone();
        tokio::spawn(async move {
            proxy_clone.run_binance_connection().await;
        });
        
        // 启动前端服务器
        let proxy_clone = self.clone();
        tokio::spawn(async move {
            proxy_clone.run_frontend_server().await;
        });
    }
    
    /// 订阅管理器：处理来自前端的订阅请求
    async fn run_subscription_manager(&self) {
        let mut sub_rx = self.sub_rx.lock().await;
        
        while let Some(request) = sub_rx.recv().await {
            match request {
                SubscriptionRequest::Subscribe { client_id, streams, request_id } => {
                    let new_streams = {
                        let mut state = self.state.write().await;
                        state.add(client_id, &streams)
                    };
                    
                    if !new_streams.is_empty() {
                        info!("📥 新增订阅 {} 个流: {:?}", new_streams.len(), &new_streams[..std::cmp::min(3, new_streams.len())]);
                        self.send_to_binance("SUBSCRIBE", &new_streams).await;
                    }
                    
                    // 向特定客户端发送成功响应
                    if let Some(id) = request_id {
                        let _ = self.broadcast_tx.send((
                            Some(client_id),
                            FrontendResponse::SubscriptionResult { result: None, id }
                        ));
                    }
                }
                
                SubscriptionRequest::Unsubscribe { client_id, streams, request_id } => {
                    let removed_streams = {
                        let mut state = self.state.write().await;
                        state.remove(client_id, &streams)
                    };
                    
                    if !removed_streams.is_empty() {
                        info!("📤 取消订阅 {} 个流", removed_streams.len());
                        self.send_to_binance("UNSUBSCRIBE", &removed_streams).await;
                    }
                    
                    if let Some(id) = request_id {
                        let _ = self.broadcast_tx.send((
                            Some(client_id),
                            FrontendResponse::SubscriptionResult { result: None, id }
                        ));
                    }
                }
                
                SubscriptionRequest::ListSubscriptions { client_id, request_id } => {
                    let streams = {
                        let state = self.state.read().await;
                        state.all_streams()
                    };
                    
                    let _ = self.broadcast_tx.send((
                        Some(client_id),
                        FrontendResponse::ListResult { result: streams, id: request_id }
                    ));
                }
                
                SubscriptionRequest::ClientDisconnected { client_id } => {
                    let removed_streams = {
                        let mut state = self.state.write().await;
                        state.client_disconnected(client_id)
                    };
                    
                    if !removed_streams.is_empty() {
                        info!("🔌 客户端 {} 断开，取消订阅 {} 个流", client_id, removed_streams.len());
                        self.send_to_binance("UNSUBSCRIBE", &removed_streams).await;
                    }
                }
            }
        }
    }
    
    /// 向币安发送订阅/取消订阅请求
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
                warn!("发送消息到币安失败，将消息入队等待重连: {}", e);
                self.enqueue_pending_message(msg).await;
            }
        } else {
            self.enqueue_pending_message(msg).await;
            warn!("币安连接尚未建立，消息已入队，重连后恢复");
        }
    }

    async fn enqueue_pending_message(&self, msg: String) {
        let mut pending = self.pending_messages.lock().await;
        if pending.len() >= PENDING_MSG_CAPACITY {
            pending.pop_front();
            warn!("断线待发送队列已满，已丢弃最旧消息");
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

    async fn flush_pending_messages(
        &self,
        tx: &mpsc::Sender<String>,
    ) -> HashSet<String> {
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

        info!("♻️ 重放 {} 条断线期间积压消息...", pending.len());
        for (idx, msg) in pending.iter().enumerate() {
            if tx.send(msg.clone()).await.is_err() {
                warn!(
                    "积压消息重放中断，剩余 {} 条消息重新入队",
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
            info!("🔄 恢复 {} 个订阅...", streams.len());
            for chunk in streams.chunks(200) {
                self.send_to_binance("SUBSCRIBE", chunk).await;
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }

        // 新连接初始无订阅。若重放消息形成了“当前状态不需要”的订阅，这里主动清理。
        let target_set: HashSet<String> = streams.iter().cloned().collect();
        let stale_streams: Vec<String> = replayed_subscriptions
            .difference(&target_set)
            .cloned()
            .collect();
        if !stale_streams.is_empty() {
            info!("🧹 清理 {} 个过期订阅...", stale_streams.len());
            for chunk in stale_streams.chunks(200) {
                self.send_to_binance("UNSUBSCRIBE", chunk).await;
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
    
    /// 币安连接管理器：维护与币安的连接
    async fn run_binance_connection(&self) {
        let mut reconnect_backoff = Duration::from_secs(RECONNECT_BACKOFF_MIN_SECS);

        loop {
            let connection_start = Instant::now();
            
            // 尝试连接（直连优先）
            match self.try_connect().await {
                Ok((ws_stream, endpoint)) => {
                    info!("✅ 已连接到币安: {}", endpoint);
                    reconnect_backoff = Duration::from_secs(RECONNECT_BACKOFF_MIN_SECS);
                    
                    // 运行连接维护循环
                    self.run_binance_loop(ws_stream, connection_start).await;
                }
                Err(e) => {
                    error!(
                        "❌ 连接币安失败: {}，{}秒后重试...",
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
    
    /// 尝试连接到币安（直连优先，失败则使用代理）
    async fn try_connect(&self) -> Result<(WebSocketStream<MaybeTlsStream<TcpStream>>, &'static str), Box<dyn std::error::Error + Send + Sync>> {
        // 首先尝试直连
        info!("🔗 尝试直连币安 ({})...", DIRECT_WS_URL);
        match tokio::time::timeout(
            Duration::from_secs(10),
            connect_async(DIRECT_WS_URL)
        ).await {
            Ok(Ok((ws, _))) => return Ok((ws, "直连")),
            Ok(Err(e)) => warn!("直连失败: {}，尝试代理...", e),
            Err(_) => warn!("直连超时，尝试代理..."),
        }
        
        // 直连失败，使用代理
        info!("🔗 通过代理连接币安 ({})...", PROXY_WS_URL);
        let ws = match tokio::time::timeout(
            Duration::from_secs(15),
            connect_via_socks5_proxy(PROXY_WS_URL, SOCKS5_PROXY),
        )
        .await
        {
            Ok(Ok(ws)) => ws,
            Ok(Err(e)) => return Err(e),
            Err(_) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "SOCKS5 代理连接超时",
                )
                .into())
            }
        };
        Ok((ws, "代理"))
    }
    
    /// 维护与币安的连接循环
    async fn run_binance_loop(
        &self,
        ws_stream: WebSocketStream<MaybeTlsStream<TcpStream>>,
        connection_start: Instant,
    ) {
        let (mut write, mut read) = ws_stream.split();
        
        // 创建向币安发送消息的通道
        let (tx, mut rx) = mpsc::channel::<String>(1000);
        *self.binance_tx.lock().await = Some(tx);

        // 注意：binance_tx 建立后再恢复订阅，避免“恢复消息发送到空通道”的竞态。
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
        
        // 定时器：检查连接生命周期
        let mut lifetime_check = interval(Duration::from_secs(300));
        
        loop {
            tokio::select! {
                // 接收来自币安的消息
                msg = read.next() => {
                    match msg {
                        Some(Ok(Message::Text(text))) => {
                            if let Ok(data) = serde_json::from_str::<Value>(&text) {
                                // 广播给所有前端
                                let _ = self.broadcast_tx.send((
                                    None,
                                    FrontendResponse::BinanceData(data)
                                ));
                            }
                        }
                        Some(Ok(Message::Ping(payload))) => {
                            debug!("收到 Ping，回复 Pong");
                            if let Err(e) = write.send(Message::Pong(payload)).await {
                                error!("发送 Pong 失败: {}", e);
                                break;
                            }
                        }
                        Some(Ok(Message::Close(_))) => {
                            warn!("币安主动关闭连接");
                            break;
                        }
                        Some(Err(e)) => {
                            error!("币安 WebSocket 错误: {}", e);
                            break;
                        }
                        None => {
                            warn!("币安连接已断开");
                            break;
                        }
                        _ => {}
                    }
                }
                
                // 发送消息到币安
                Some(msg) = rx.recv() => {
                    if let Err(e) = write.send(Message::Text(msg.clone().into())).await {
                        error!("发送消息到币安失败: {}", e);
                        self.enqueue_pending_message(msg).await;
                        break;
                    }
                }
                
                // 检查连接生命周期
                _ = lifetime_check.tick() => {
                    if connection_start.elapsed() > Duration::from_secs(CONNECTION_LIFETIME_SECS) {
                        info!("⏰ 连接已达到生命周期限制，主动重连...");
                        break;
                    }
                }
            }
        }
        
        // 清理发送通道
        *self.binance_tx.lock().await = None;
    }
    
    /// 前端 WebSocket 服务器
    async fn run_frontend_server(&self) {
        let addr = format!("0.0.0.0:{}", LOCAL_SERVER_PORT);
        let listener = TcpListener::bind(&addr).await.expect(&format!("无法绑定端口 {}", LOCAL_SERVER_PORT));
        info!("📡 前端代理服务正在监听: ws://{}", addr);
        
        while let Ok((stream, addr)) = listener.accept().await {
            let client_id = self.client_id_counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let broadcast_rx = self.broadcast_tx.subscribe();
            let sub_tx = self.sub_tx.clone();
            
            tokio::spawn(async move {
                handle_frontend_connection(stream, addr, client_id, broadcast_rx, sub_tx).await;
            });
        }
    }
}

/// 通过 SOCKS5 代理连接 WebSocket
async fn connect_via_socks5_proxy(
    ws_url: &str,
    proxy_addr: &str,
) -> Result<WebSocketStream<MaybeTlsStream<TcpStream>>, Box<dyn std::error::Error + Send + Sync>> {
    use tokio_socks::tcp::Socks5Stream;
    use tokio_tungstenite::client_async_tls_with_config;
    
    let url = url::Url::parse(ws_url)?;
    let host = url.host_str().ok_or("无效的主机名")?;
    let port = url.port().unwrap_or(443);
    
    info!("通过 SOCKS5 代理 {} 连接 {}:{}...", proxy_addr, host, port);
    
    let socks_stream = Socks5Stream::connect(proxy_addr, (host, port)).await?;
    let tcp_stream = socks_stream.into_inner();
    
    let request = http::Request::builder()
        .method("GET")
        .uri(ws_url)
        .header("Host", host)
        .header("Upgrade", "websocket")
        .header("Connection", "Upgrade")
        .header("Sec-WebSocket-Key", tokio_tungstenite::tungstenite::handshake::client::generate_key())
        .header("Sec-WebSocket-Version", "13")
        .body(())?;
    
    let connector = tokio_tungstenite::Connector::Rustls(Arc::new(
        rustls::ClientConfig::builder()
            .with_root_certificates(rustls::RootCertStore {
                roots: webpki_roots::TLS_SERVER_ROOTS.iter().cloned().collect(),
            })
            .with_no_client_auth()
    ));
    
    let (ws_stream, _) = client_async_tls_with_config(
        request,
        tcp_stream,
        None,
        Some(connector),
    ).await?;
    
    Ok(ws_stream)
}

/// 处理前端连接
async fn handle_frontend_connection(
    stream: TcpStream,
    addr: std::net::SocketAddr,
    client_id: u64,
    mut broadcast_rx: broadcast::Receiver<(Option<u64>, FrontendResponse)>,
    sub_tx: mpsc::Sender<SubscriptionRequest>,
) {
    info!("📱 前端客户端 {} 连接 (ID: {})", addr, client_id);
    
    let ws_stream = match accept_async(stream).await {
        Ok(s) => s,
        Err(e) => {
            error!("WebSocket 握手失败 ({}): {}", addr, e);
            return;
        }
    };
    
    let (mut write, mut read) = ws_stream.split();
    
    loop {
        tokio::select! {
            // 接收前端消息
            msg = read.next() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        if let Ok(val) = serde_json::from_str::<Value>(&text) {
                            handle_frontend_message(client_id, val, &sub_tx).await;
                        }
                    }
                    Some(Ok(Message::Close(_))) | None => {
                        break;
                    }
                    _ => {}
                }
            }
            
            // 广播消息到前端
            msg = broadcast_rx.recv() => {
                match msg {
                    Ok((target_client, response)) => {
                        // 如果指定了目标客户端，检查是否匹配
                        if let Some(target) = target_client {
                            if target != client_id {
                                continue;
                            }
                        }
                        
                        if let Ok(json) = serde_json::to_string(&response) {
                            if let Err(e) = write.send(Message::Text(json.into())).await {
                                warn!("发送消息到前端失败 ({}): {}", addr, e);
                                break;
                            }
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        warn!("客户端 {} 丢失 {} 条消息", client_id, n);
                    }
                    Err(_) => break,
                }
            }
        }
    }
    
    // 客户端断开，通知订阅管理器
    let _ = sub_tx.send(SubscriptionRequest::ClientDisconnected { client_id }).await;
    info!("📱 前端客户端 {} 断开 (ID: {})", addr, client_id);
}

/// 解析并处理前端消息
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
                let streams: Vec<String> = streams
                    .iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect();
                debug!("客户端 {} 订阅: {:?}", client_id, streams);
                let _ = sub_tx.send(SubscriptionRequest::Subscribe {
                    client_id,
                    streams,
                    request_id,
                }).await;
            }
        }
        Some("UNSUBSCRIBE") => {
            if let Some(streams) = params {
                let streams: Vec<String> = streams
                    .iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect();
                debug!("客户端 {} 取消订阅: {:?}", client_id, streams);
                let _ = sub_tx.send(SubscriptionRequest::Unsubscribe {
                    client_id,
                    streams,
                    request_id,
                }).await;
            }
        }
        Some("LIST_SUBSCRIPTIONS") => {
            if let Some(id) = request_id {
                let _ = sub_tx.send(SubscriptionRequest::ListSubscriptions {
                    client_id,
                    request_id: id,
                }).await;
            }
        }
        _ => {
            debug!("客户端 {} 发送未知消息: {}", client_id, val);
        }
    }
}

// ============================================================================
// UserDataProxy - 私有数据流代理 (端口 6003)
// ============================================================================

use crate::api_client::ApiClient;
use crate::config::Config;
use crate::models::AccountState;

/// 私有数据流服务端口
const USER_DATA_PORT: u16 = 6003;
/// listenKey 续期间隔 (50分钟)
const LISTEN_KEY_KEEPALIVE_MINS: u64 = 50;

/// 私有数据流代理
pub struct UserDataProxy {
    api_client: Arc<ApiClient>,
    config: Arc<Config>,
    /// 广播通道
    broadcast_tx: broadcast::Sender<Value>,
    /// 当前 listenKey
    listen_key: Arc<RwLock<Option<String>>>,
    /// 账户状态缓存
    state: Arc<RwLock<Option<AccountState>>>,
}

impl UserDataProxy {
    pub fn new(api_client: Arc<ApiClient>, config: Arc<Config>) -> Self {
        let (broadcast_tx, _) = broadcast::channel(1000);
        
        Self {
            api_client,
            config,
            broadcast_tx,
            listen_key: Arc::new(RwLock::new(None)),
            state: Arc::new(RwLock::new(None)),
        }
    }
    
    pub async fn start(self: Arc<Self>) {
        info!("🚀 启动币安私有数据流代理服务 (端口 {})...", USER_DATA_PORT);
        
        // 启动 listenKey 管理器
        let proxy_clone = self.clone();
        tokio::spawn(async move {
            proxy_clone.run_listen_key_manager().await;
        });
        
        // 启动前端服务器
        let proxy_clone = self.clone();
        tokio::spawn(async move {
            proxy_clone.run_frontend_server().await;
        });
    }
    
    /// listenKey 管理器：获取、续期、维护 WebSocket 连接
    async fn run_listen_key_manager(&self) {
        loop {
            // 获取 listenKey
            match self.api_client.post_listen_key().await {
                Ok(key) => {
                    *self.listen_key.write().await = Some(key.clone());
                    
                    // 获取初始状态
                    info!("📥 正在获取账户初始快照...");
                    match tokio::join!(
                        self.api_client.get_account_information(),
                        self.api_client.get_open_orders()
                    ) {
                        (Ok(account), Ok(orders)) => {
                            let new_state = AccountState::from_snapshot(&account, &orders);
                            *self.state.write().await = Some(new_state);
                            info!("✅ 账户初始状态已缓存");
                        },
                        (Err(e), _) | (_, Err(e)) => {
                            warn!("⚠️ 获取账户初始快照失败: {}", e);
                        }
                    }

                    // 启动 WebSocket 连接和续期任务
                    let (ws_result, _) = tokio::join!(
                        self.run_user_data_connection(&key),
                        self.run_keepalive_task()
                    );
                    
                    // 连接断开，清理 listenKey
                    *self.listen_key.write().await = None;
                    
                    warn!("⚠️ 私有数据流连接断开，5秒后重连...");
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
                Err(e) => {
                    error!("❌ 获取 listenKey 失败: {}，30秒后重试...", e);
                    tokio::time::sleep(Duration::from_secs(30)).await;
                }
            }
        }
    }
    
    /// 续期任务
    async fn run_keepalive_task(&self) {
        let mut interval = interval(Duration::from_secs(LISTEN_KEY_KEEPALIVE_MINS * 60));
        
        loop {
            interval.tick().await;
            
            if self.listen_key.read().await.is_none() {
                break;
            }
            
            if let Err(e) = self.api_client.put_listen_key().await {
                warn!("⚠️ listenKey 续期失败: {}", e);
            }
        }
    }
    
    /// 维护与币安的私有数据 WebSocket 连接
    async fn run_user_data_connection(&self, listen_key: &str) {
        // 构建 WebSocket URL
        let direct_url = format!("{}/ws/{}", self.config.binance.direct_ws_base, listen_key);
        let proxy_url = format!("{}/ws/{}", self.config.binance.proxy_ws_base, listen_key);
        
        info!("🔗 连接私有数据流: {}...", &direct_url[..50.min(direct_url.len())]);
        
        // 尝试直连
        let ws_stream = match tokio::time::timeout(
            Duration::from_secs(10),
            connect_async(&direct_url)
        ).await {
            Ok(Ok((ws, _))) => {
                info!("✅ 私有数据流直连成功");
                ws
            }
            _ => {
                // 回退到代理
                info!("🔄 直连失败，尝试通过代理连接...");
                match connect_via_socks5_proxy(&proxy_url, &self.config.binance.socks5_proxy).await {
                    Ok(ws) => {
                        info!("✅ 私有数据流通过代理连接成功");
                        ws
                    }
                    Err(e) => {
                        error!("❌ 私有数据流连接失败: {}", e);
                        return;
                    }
                }
            }
        };
        
        let (mut write, mut read) = ws_stream.split();
        
        loop {
            tokio::select! {
                msg = read.next() => {
                    match msg {
                        Some(Ok(Message::Text(text))) => {
                            // [调试日志] 收到币安私有数据
                            if let Ok(data) = serde_json::from_str::<Value>(&text) {
                                let event_type = data.get("e").and_then(|s| s.as_str()).unwrap_or("unknown");
                                info!("[私有数据流] 收到币安数据: event={}", event_type);
                                
                                // 更新状态
                                if let Some(e) = data.get("e").and_then(|s| s.as_str()) {
                                    if e == "ACCOUNT_UPDATE" {
                                        if let Some(state) = self.state.write().await.as_mut() {
                                            state.update_from_account_update(&data);
                                        }
                                    } else if e == "ORDER_TRADE_UPDATE" {
                                        if let Some(state) = self.state.write().await.as_mut() {
                                            state.update_from_order_update(&data);
                                        }
                                    }
                                }

                                // 广播给所有前端
                                let _ = self.broadcast_tx.send(data);
                            }
                        }
                        Some(Ok(Message::Ping(payload))) => {
                            debug!("收到 Ping，回复 Pong");
                            if let Err(e) = write.send(Message::Pong(payload)).await {
                                error!("发送 Pong 失败: {}", e);
                                break;
                            }
                        }
                        Some(Ok(Message::Close(_))) => {
                            warn!("币安主动关闭私有数据流连接");
                            break;
                        }
                        Some(Err(e)) => {
                            error!("私有数据流 WebSocket 错误: {}", e);
                            break;
                        }
                        None => {
                            warn!("私有数据流连接已断开");
                            break;
                        }
                        _ => {}
                    }
                }
            }
        }
    }
    
    /// 前端 WebSocket 服务器
    async fn run_frontend_server(&self) {
        let addr = format!("0.0.0.0:{}", USER_DATA_PORT);
        let listener = TcpListener::bind(&addr).await.expect(&format!("无法绑定端口 {}", USER_DATA_PORT));
        info!("📡 私有数据代理服务正在监听: ws://{}", addr);
        
        while let Ok((stream, addr)) = listener.accept().await {
            let broadcast_rx = self.broadcast_tx.subscribe();
            
            let state = self.state.clone();
            tokio::spawn(async move {
                handle_user_data_frontend(stream, addr, broadcast_rx, state).await;
            });
        }
    }
}

/// 处理私有数据前端连接
async fn handle_user_data_frontend(
    stream: TcpStream,
    addr: std::net::SocketAddr,
    mut broadcast_rx: broadcast::Receiver<Value>,
    state: Arc<RwLock<Option<AccountState>>>,
) {
    info!("📱 私有数据客户端连接: {}", addr);
    
    let ws_stream = match accept_async(stream).await {
        Ok(s) => s,
        Err(e) => {
            error!("WebSocket 握手失败 ({}): {}", addr, e);
            return;
        }
    };
    
    let (mut write, mut read) = ws_stream.split();
    
    // 发送初始快照
    {
        let state_lock = state.read().await;
        if let Some(s) = state_lock.as_ref() {
            let snapshot_msg = json!({
                "e": "ACCOUNT_SNAPSHOT",
                "data": s
            });
            // [调试日志] 发送初始快照
            info!("[私有数据流] 发送初始快照给前端: {}", addr);
            if let Ok(json) = serde_json::to_string(&snapshot_msg) {
                 if let Err(e) = write.send(Message::Text(json.into())).await {
                      warn!("发送初始快照失败: {}", e);
                 } else {
                      info!("[私有数据流] 初始快照发送成功");
                 }
            }
        } else {
            warn!("[私有数据流] 状态为空，无法发送初始快照");
        }
    }
    
    loop {
        tokio::select! {
            // 接收前端消息（目前只处理订阅请求，但私有流是自动的）
            msg = read.next() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        debug!("收到前端消息: {}", text);
                        // 目前不需要处理，私有流自动推送
                    }
                    Some(Ok(Message::Close(_))) | None => {
                        break;
                    }
                    _ => {}
                }
            }
            
            // 广播私有数据到前端
            msg = broadcast_rx.recv() => {
                match msg {
                    Ok(data) => {
                        if let Ok(json) = serde_json::to_string(&data) {
                            if let Err(e) = write.send(Message::Text(json.into())).await {
                                warn!("发送私有数据到前端失败 ({}): {}", addr, e);
                                break;
                            }
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        warn!("私有数据客户端 {} 丢失 {} 条消息", addr, n);
                    }
                    Err(_) => break,
                }
            }
        }
    }
    
    info!("📱 私有数据客户端断开: {}", addr);
}
