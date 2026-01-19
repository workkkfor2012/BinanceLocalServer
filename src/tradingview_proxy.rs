use futures::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashSet;
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
        // 参考 JS 实现，心跳通常也被包装在 ~m~ 帧中发送回服务器
        let msg = format!("~h~{}", num);
        format!("~m~{}~m~{}", msg.len(), msg)
    }

    fn parse_packets(raw: &str) -> Vec<TvPacket> {
        let mut packets = Vec::new();
        
        // 1. 先按 ~m~...~m~ 分割
        // 协议格式通常是 ~m~len~m~content
        let parts: Vec<&str> = raw.split("~m~").collect();
        let mut i = 1;
        while i < parts.len() {
            let len_str = parts[i];
            if let Ok(len) = len_str.parse::<usize>() {
                if i + 1 < parts.len() {
                    let content = parts[i+1];
                    // 检查 content 长度是否足够，或者是否包含更多内容
                    let actual_content = if content.len() > len {
                        &content[..len]
                    } else {
                        content
                    };

                    if actual_content.starts_with("~h~") {
                        packets.push(TvPacket::Heartbeat(actual_content[3..].to_string()));
                    } else if let Ok(val) = serde_json::from_str::<Value>(actual_content) {
                        packets.push(TvPacket::Data(val));
                    } else {
                        warn!("无法解析 TV 数据内容: {}", actual_content);
                    }
                }
                i += 2; // 跳过 len 和 content
            } else {
                // 如果不是数字，检查是否是独立的 ~h~ 包（有时服务器会发裸包）
                if len_str.starts_with("~h~") {
                    packets.push(TvPacket::Heartbeat(len_str[3..].to_string()));
                }
                i += 1;
            }
        }

        // 2. 后备方案：如果没有通过 ~m~ 分割出任何包，且以 ~h~ 开头
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
    
    info!("通过 SOCKS5 代理 {} 连接...", PROXY_ADDR);
    
    let target_host = "data.tradingview.com";
    let target_port = 443u16;
    
    let socks_stream = Socks5Stream::connect(
        PROXY_ADDR,
        (target_host, target_port),
    ).await?;
    
    info!("SOCKS5 隧道已建立，正在进行 TLS 握手...");
    
    let tcp_stream = socks_stream.into_inner();
    
    // 使用完整 URL 构建请求
    let request = http::Request::builder()
        .method("GET")
        .uri(TV_WS_URL)  // 使用完整的 wss:// URL
        .header("Host", target_host)
        .header("Origin", TV_ORIGIN)
        .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
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
    
    info!("WebSocket 连接成功建立！");
    Ok(ws_stream)
}

// --- 代理核心逻辑 ---

pub struct TradingViewProxy {
    broadcast_tx: broadcast::Sender<FrontendMessage>,
    sub_tx: mpsc::Sender<String>,
    sub_rx: Arc<Mutex<mpsc::Receiver<String>>>,
    subscribed_symbols: Arc<Mutex<HashSet<String>>>,
}

impl TradingViewProxy {
    pub fn new() -> Self {
        let (tx, _) = broadcast::channel(1000);
        let (sub_tx, sub_rx) = mpsc::channel(100);
        Self {
            broadcast_tx: tx,
            sub_tx,
            sub_rx: Arc::new(Mutex::new(sub_rx)),
            subscribed_symbols: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    pub async fn start(self: Arc<Self>) {
        info!("启动 TradingView 代理服务 (独立连接模式)...");
        
        let proxy_clone = self.clone();
        tokio::spawn(async move {
            proxy_clone.run_subscription_manager().await;
        });

        let proxy_clone = self.clone();
        tokio::spawn(async move {
            proxy_clone.run_frontend_server().await;
        });
    }

    async fn run_subscription_manager(&self) {
        let mut sub_rx = self.sub_rx.lock().await;

        // --- 自动订阅测试 ---
        let test_symbol = "BINANCE:BTCUSDT".to_string();
        info!("执行自动订阅测试: {}", test_symbol);
        {
            let mut subs = self.subscribed_symbols.lock().await;
            if !subs.contains(&test_symbol) {
                subs.insert(test_symbol.clone());
                let tx = self.broadcast_tx.clone();
                let sym_clone = test_symbol.clone();
                tokio::spawn(async move {
                    Self::connect_and_maintain_symbol(sym_clone, tx).await;
                });
            }
        }
        // ------------------

        while let Some(symbol) = sub_rx.recv().await {
            let mut subs = self.subscribed_symbols.lock().await;
            if subs.contains(&symbol) {
                // 如果已订阅，忽略
                continue;
            }
            subs.insert(symbol.clone());

            let tx = self.broadcast_tx.clone();
            let sym_clone = symbol.clone();
            info!("为符号 {} 启动新的 WebSocket 连接任务...", sym_clone);
            
            tokio::spawn(async move {
                Self::connect_and_maintain_symbol(sym_clone, tx).await;
            });
        }
    }

    // 静态方法，不再依赖 self，而是传入必要的参数 (symbol, broadcast_tx)
    async fn connect_and_maintain_symbol(symbol: String, broadcast_tx: broadcast::Sender<FrontendMessage>) {
        loop {
            info!("[{}] 正在建立独立连接: {}", symbol, TV_WS_URL);
            
            match connect_via_socks5_proxy().await {
                Ok(mut socket) => {
                    info!("[{}] WebSocket 连接成功。", symbol);
                    
                    socket.send(Message::Text(TvProtocol::format_packet(&json!({
                        "m": "set_auth_token",
                        "p": ["unauthorized_user_token"]
                    })).into())).await.ok();

                    let session_id = format!("cs_{}", crate::utils::generate_random_string(12));
                    let series_id = "ser_1"; 
                    
                    socket.send(Message::Text(TvProtocol::format_packet(&json!({
                        "m": "chart_create_session",
                        "p": [session_id]
                    })).into())).await.ok();
                    
                    socket.send(Message::Text(TvProtocol::format_packet(&json!({
                        "m": "resolve_symbol",
                        "p": [session_id, series_id, format!("={}", json!({"symbol": symbol}))]
                    })).into())).await.ok();

                    socket.send(Message::Text(TvProtocol::format_packet(&json!({
                        "m": "create_series",
                        "p": [session_id, "$prices", "s1", series_id, "1", 300]
                    })).into())).await.ok();

                    loop {
                        match socket.next().await {
                            Some(Ok(Message::Text(text))) => {
                                for packet in TvProtocol::parse_packets(&text) {
                                    match packet {
                                        TvPacket::Heartbeat(num) => {
                                            debug!("[{}] 收到心跳 ({}).", symbol, num);
                                            socket.send(Message::Text(TvProtocol::format_heartbeat(&num).into())).await.ok();
                                        }
                                        TvPacket::Data(val) => {
                                            Self::process_tv_data(&symbol, val, &broadcast_tx);
                                        }
                                    }
                                }
                            }
                            Some(Ok(Message::Close(_))) => break,
                            Some(Err(e)) => {
                                error!("[{}] WS 错误: {}", symbol, e);
                                break;
                            }
                            None => break,
                            _ => {}
                        }
                    }
                }
                Err(e) => {
                    error!("[{}] 连接失败: {}，5秒后重试...", symbol, e);
                    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                }
            }
        }
    }

    fn process_tv_data(symbol: &str, val: Value, broadcast_tx: &broadcast::Sender<FrontendMessage>) {
        let m = val.get("m").and_then(|v| v.as_str());
        let p = val.get("p").and_then(|v| v.as_array());

        if let Some(msg_type) = m {
            if msg_type != "du" && msg_type != "timescale_update" {
                // 仅调试非常规消息，避免刷屏
                debug!("[{}] 收到消息: type={}", symbol, msg_type);
            }
        }

        match (m, p) {
            (Some("timescale_update" | "du"), Some(p)) if p.len() >= 2 => {
                // p[1] 包含数据，不再需要通过 ID 查找 Symbol，直接使用闭包中的 symbol
                if let Some(prices) = p[1].get("$prices").and_then(|v| v.get("s")).and_then(|v| v.as_array()) {
                    if m == Some("timescale_update") {
                        let mut data = Vec::new();
                        for item in prices {
                            if let Some(v) = item.get("v").and_then(|v| v.as_array()) {
                                if v.len() >= 6 {
                                    data.push(Kline {
                                        time: v[0].as_i64().unwrap_or(0),
                                        open: v[1].as_f64().unwrap_or(0.0),
                                        high: v[2].as_f64().unwrap_or(0.0),
                                        low: v[3].as_f64().unwrap_or(0.0),
                                        close: v[4].as_f64().unwrap_or(0.0),
                                        volume: v[5].as_f64().unwrap_or(0.0),
                                    });
                                }
                            }
                        }
                        // 发送全量数据
                        if !data.is_empty() {
                            info!("[{}] 收到全量历史数据 ({} 条)", symbol, data.len());
                            let _ = broadcast_tx.send(FrontendMessage::History(HistoryPayload {
                                symbol: symbol.to_string(),
                                data,
                            }));
                        }
                    } else {
                        // 增量更新
                        for item in prices {
                            if let Some(v) = item.get("v").and_then(|v| v.as_array()) {
                                if v.len() >= 6 {
                                    let kline = KlineUpdate {
                                        timestamp: v[0].as_i64().unwrap_or(0) * 1000,
                                        open: v[1].as_f64().unwrap_or(0.0),
                                        high: v[2].as_f64().unwrap_or(0.0),
                                        low: v[3].as_f64().unwrap_or(0.0),
                                        close: v[4].as_f64().unwrap_or(0.0),
                                        volume: v[5].as_f64().unwrap_or(0.0),
                                    };
                                    info!("[{}] 收到最新K线: t={}, c={}", symbol, kline.timestamp, kline.close);
                                    let _ = broadcast_tx.send(FrontendMessage::UpdateLast(UpdateLastPayload {
                                        symbol: symbol.to_string(),
                                        kline,
                                    }));
                                }
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
        info!("前端 WS 转发服务正在监听: ws://{}", addr);

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
    info!("收到前端连接: {}", addr);
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
                    Some(Ok(Message::Close(_))) | None => break,
                    _ => {}
                }
            }
            Ok(msg) = rx.recv() => {
                if let Ok(json_msg) = serde_json::to_string(&msg) {
                    if let Err(e) = ws_stream.send(Message::Text(json_msg.into())).await {
                        warn!("发送消息到前端时出错 ({}): {}", addr, e);
                        break;
                    }
                }
            }
        }
    }
    info!("前端连接断开: {}", addr);
}
