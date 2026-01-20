use futures::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, mpsc, Mutex};
use tokio_tungstenite::{
    tungstenite::protocol::Message,
    accept_async,
    MaybeTlsStream,
    WebSocketStream,
};
use tracing::{error, info, warn, debug};

// --- 协议常量 ---
const TV_WS_URL: &str = "wss://data.tradingview.com/socket.io/websocket?type=chart";
const TV_ORIGIN: &str = "https://www.tradingview.com";
const PROXY_ADDR: &str = "127.0.0.1:1080";

// --- 数据结构 ---

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

// 消息类型常量，与前端 SyncClient 的 MessageType 枚举一致
const MESSAGE_TYPE_CUSTOM: u8 = 0x04;

/// 构建符合前端 SyncClient 期望的二进制协议消息
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

// --- TradingView 协议处理 ---

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
                    let content = parts[i+1];
                    let actual_content = if content.len() > len { &content[..len] } else { content };
                    if actual_content.starts_with("~h~") {
                        packets.push(TvPacket::Heartbeat(actual_content[3..].to_string()));
                    } else if let Ok(val) = serde_json::from_str::<Value>(actual_content) {
                        packets.push(TvPacket::Data(val));
                    }
                }
                i += 2;
            } else {
                if len_str.starts_with("~h~") {
                    packets.push(TvPacket::Heartbeat(len_str[3..].to_string()));
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

// --- 通过 SOCKS5 代理建立 TLS 连接 ---
async fn connect_via_socks5_proxy() -> Result<WebSocketStream<MaybeTlsStream<TcpStream>>, Box<dyn std::error::Error + Send + Sync>> {
    use tokio_socks::tcp::Socks5Stream;
    use tokio_tungstenite::client_async_tls_with_config;
    
    info!("通过 SOCKS5 代理 {} 连接 TradingView...", PROXY_ADDR);
    
    let target_host = "data.tradingview.com";
    let target_port = 443u16;
    
    let socks_stream = Socks5Stream::connect(PROXY_ADDR, (target_host, target_port)).await?;
    let tcp_stream = socks_stream.into_inner();
    
    let request = http::Request::builder()
        .method("GET")
        .uri(TV_WS_URL)
        .header("Host", target_host)
        .header("Origin", TV_ORIGIN)
        .header("User-Agent", "Mozilla/5.0")
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

    let (ws_stream, _) = client_async_tls_with_config(request, tcp_stream, None, Some(connector)).await?;
    info!("TradingView WebSocket 连接成功！");
    Ok(ws_stream)
}

// --- 代理核心逻辑 ---

pub struct TradingViewProxy {
    broadcast_tx: broadcast::Sender<FrontendMessage>,
    sub_tx: mpsc::Sender<String>,
    sub_rx: Arc<Mutex<mpsc::Receiver<String>>>,
}

impl TradingViewProxy {
    pub fn new() -> Self {
        let (tx, _) = broadcast::channel(1000);
        let (sub_tx, sub_rx) = mpsc::channel(100);
        Self {
            broadcast_tx: tx,
            sub_tx,
            sub_rx: Arc::new(Mutex::new(sub_rx)),
        }
    }

    pub async fn start(self: Arc<Self>) {
        info!("启动 TradingView 代理服务 (纯中转模式)...");
        
        let proxy_clone = self.clone();
        tokio::spawn(async move {
            proxy_clone.run_subscription_manager().await;
        });

        let proxy_clone = self.clone();
        tokio::spawn(async move {
            proxy_clone.run_frontend_server().await;
        });
    }

    /// [简化版] 每次收到订阅请求，都创建一个新的 TradingView 连接
    /// 适合个人项目，逻辑最简单
    async fn run_subscription_manager(&self) {
        let mut sub_rx = self.sub_rx.lock().await;

        while let Some(symbol) = sub_rx.recv().await {
            info!("[订阅] 收到请求: {}, 创建新的 TradingView 连接", symbol);
            
            let tx = self.broadcast_tx.clone();
            tokio::spawn(async move {
                Self::connect_and_stream(symbol, tx).await;
            });
        }
    }

    /// 建立连接并持续推送数据
    async fn connect_and_stream(symbol: String, broadcast_tx: broadcast::Sender<FrontendMessage>) {
        // 只连接一次，获取历史数据后持续推送增量更新
        // 如果连接断开，这个任务就结束（前端会重新发订阅）
        match connect_via_socks5_proxy().await {
            Ok(mut socket) => {
                info!("[{}] 连接成功，发送订阅请求...", symbol);
                
                // 认证
                socket.send(Message::Text(TvProtocol::format_packet(&json!({
                    "m": "set_auth_token",
                    "p": ["unauthorized_user_token"]
                })).into())).await.ok();

                // 创建 session 和订阅
                let session_id = format!("cs_{}", crate::utils::generate_random_string(12));
                let series_id = "ser_1";
                
                socket.send(Message::Text(TvProtocol::format_packet(&json!({
                    "m": "chart_create_session",
                    "p": [&session_id]
                })).into())).await.ok();
                
                socket.send(Message::Text(TvProtocol::format_packet(&json!({
                    "m": "resolve_symbol",
                    "p": [&session_id, series_id, format!("={}", json!({"symbol": &symbol}))]
                })).into())).await.ok();

                socket.send(Message::Text(TvProtocol::format_packet(&json!({
                    "m": "create_series",
                    "p": [&session_id, "$prices", "s1", series_id, "1", 300]
                })).into())).await.ok();

                // 消息循环
                while let Some(msg) = socket.next().await {
                    match msg {
                        Ok(Message::Text(text)) => {
                            for packet in TvProtocol::parse_packets(&text) {
                                match packet {
                                    TvPacket::Heartbeat(num) => {
                                        socket.send(Message::Text(TvProtocol::format_heartbeat(&num).into())).await.ok();
                                    }
                                    TvPacket::Data(val) => {
                                        Self::process_tv_data(&symbol, val, &broadcast_tx);
                                    }
                                }
                            }
                        }
                        Ok(Message::Close(_)) => {
                            info!("[{}] 连接关闭", symbol);
                            break;
                        }
                        Err(e) => {
                            error!("[{}] 连接错误: {}", symbol, e);
                            break;
                        }
                        _ => {}
                    }
                }
                info!("[{}] 连接任务结束", symbol);
            }
            Err(e) => {
                error!("[{}] 连接失败: {}", symbol, e);
            }
        }
    }

    fn process_tv_data(symbol: &str, val: Value, broadcast_tx: &broadcast::Sender<FrontendMessage>) {
        let m = val.get("m").and_then(|v| v.as_str());
        let p = val.get("p").and_then(|v| v.as_array());

        match (m, p) {
            (Some("timescale_update"), Some(p)) if p.len() >= 2 => {
                // 历史数据
                if let Some(prices) = p[1].get("$prices").and_then(|v| v.get("s")).and_then(|v| v.as_array()) {
                    let mut data = Vec::new();
                    for item in prices {
                        if let Some(v) = item.get("v").and_then(|v| v.as_array()) {
                            if v.len() >= 6 {
                                data.push(Kline {
                                    // TradingView 发送浮点数时间戳（秒），需要用 as_f64 解析
                                    time: v[0].as_f64().unwrap_or(0.0) as i64,
                                    open: v[1].as_f64().unwrap_or(0.0),
                                    high: v[2].as_f64().unwrap_or(0.0),
                                    low: v[3].as_f64().unwrap_or(0.0),
                                    close: v[4].as_f64().unwrap_or(0.0),
                                    volume: v[5].as_f64().unwrap_or(0.0),
                                });
                            }
                        }
                    }
                    if !data.is_empty() {
                        info!("[{}] 收到历史数据: {} 根 K 线", symbol, data.len());
                        let _ = broadcast_tx.send(FrontendMessage::History(HistoryPayload {
                            symbol: symbol.to_string(),
                            data,
                        }));
                    }
                }
            }
            (Some("du"), Some(p)) if p.len() >= 2 => {
                // 增量更新
                if let Some(prices) = p[1].get("$prices").and_then(|v| v.get("s")).and_then(|v| v.as_array()) {
                    for item in prices {
                        if let Some(v) = item.get("v").and_then(|v| v.as_array()) {
                            if v.len() >= 6 {
                                let kline = KlineUpdate {
                                    // TradingView 发送浮点数时间戳（秒），转为毫秒
                                    timestamp: (v[0].as_f64().unwrap_or(0.0) * 1000.0) as i64,
                                    open: v[1].as_f64().unwrap_or(0.0),
                                    high: v[2].as_f64().unwrap_or(0.0),
                                    low: v[3].as_f64().unwrap_or(0.0),
                                    close: v[4].as_f64().unwrap_or(0.0),
                                    volume: v[5].as_f64().unwrap_or(0.0),
                                };
                                let _ = broadcast_tx.send(FrontendMessage::UpdateLast(UpdateLastPayload {
                                    symbol: symbol.to_string(),
                                    kline,
                                }));
                            }
                        }
                    }
                }
            }
            _ => {}
        }
    }

    async fn run_frontend_server(&self) {
        let addr = "0.0.0.0:6001";
        let listener = TcpListener::bind(addr).await.expect("无法绑定到端口 6001");
        info!("前端 WS 服务监听: ws://{}", addr);

        while let Ok((stream, addr)) = listener.accept().await {
            let tx = self.broadcast_tx.clone();
            let sub_tx = self.sub_tx.clone();
            tokio::spawn(async move {
                handle_frontend_connection(stream, addr, tx, sub_tx).await;
            });
        }
    }
}

async fn handle_frontend_connection(
    stream: TcpStream,
    addr: std::net::SocketAddr,
    broadcast_tx: broadcast::Sender<FrontendMessage>,
    sub_tx: mpsc::Sender<String>,
) {
    info!("前端连接: {}", addr);
    let mut ws_stream = match accept_async(stream).await {
        Ok(s) => s,
        Err(e) => {
            error!("WS 握手失败 ({}): {}", addr, e);
            return;
        }
    };

    let mut rx = broadcast_tx.subscribe();

    loop {
        tokio::select! {
            msg = ws_stream.next() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        info!("收到前端消息 ({}): {}", addr, text);
                        if let Ok(val) = serde_json::from_str::<Value>(&text) {
                            if val.is_array() && val[0] == "addSubscriptions" {
                                if let Some(symbols) = val[1].get("symbols").and_then(|s| s.as_array()) {
                                    for sym in symbols {
                                        if let Some(s) = sym.as_str() {
                                            let _ = sub_tx.send(s.to_string()).await;
                                        }
                                    }
                                }
                            }
                        }
                    }
                    Some(Ok(Message::Close(_))) | None => {
                        info!("前端断开 ({})", addr);
                        break;
                    }
                    _ => {}
                }
            }
            Ok(msg) = rx.recv() => {
                match &msg {
                    FrontendMessage::History(p) => {
                        info!("-> 前端 ({}): 历史数据 {} ({} 根)", addr, p.symbol, p.data.len());
                    }
                    FrontendMessage::UpdateLast(_) => {}
                }
                let binary_msg = build_custom_message(&msg);
                if ws_stream.send(Message::Binary(binary_msg.into())).await.is_err() {
                    break;
                }
            }
        }
    }
}
