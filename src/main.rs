// src/main.rs
mod api_client;
mod binance_proxy;
mod cache_manager;
mod config;
mod db_manager;
mod error;
mod models;
mod transformer;
mod utils;
mod web_server;
mod tradingview_proxy;

use crate::api_client::ApiClient;
use crate::cache_manager::CacheManager;
use crate::db_manager::DbManager;
use axum::{
    extract::Request,
    http::header,
    middleware::{self, Next},
    response::Response,
    routing::get,
    Router,
};
use futures::future::BoxFuture;
use std::fs;
use std::path::Path;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::time::{interval, Duration};
use tower::{Layer, Service};
use tower_http::cors::{Any, CorsLayer};
use tracing::{error, info, warn};
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

// --- (此处省略所有未改变的辅助函数: spawn_log_cleanup_task, log_requests, PrivateNetworkAccessLayer) ---
async fn spawn_log_cleanup_task() {
    info!("🧹 日志清理服务已启动，将每小时检查一次旧日志。");
    let mut interval = interval(Duration::from_secs(3600));
    loop {
        interval.tick().await;
        info!("执行预定的日志清理任务...");
        let result = tokio::task::spawn_blocking(move || {
            let log_dir = Path::new("./");
            let now = chrono::Local::now();
            let cutoff = now - chrono::Duration::hours(12);
            let mut deleted_count = 0;
            let entries = match fs::read_dir(log_dir) {
                Ok(entries) => entries,
                Err(e) => {
                    warn!("读取日志目录失败: {}", e);
                    return 0;
                }
            };
            for entry in entries.filter_map(Result::ok) {
                let path = entry.path();
                if path.is_file()
                    && path
                        .file_name()
                        .and_then(|s| s.to_str())
                        .map_or(false, |s| s.starts_with("start.log."))
                {
                    if let Ok(metadata) = entry.metadata() {
                        if let Ok(modified_time) = metadata.modified() {
                            let modified_time: chrono::DateTime<chrono::Local> =
                                modified_time.into();
                            if modified_time < cutoff {
                                match fs::remove_file(&path) {
                                    Ok(_) => {
                                        info!("已删除旧日志文件: {:?}", path);
                                        deleted_count += 1;
                                    }
                                    Err(e) => {
                                        warn!("删除旧日志文件 {:?} 失败: {}", path, e)
                                    }
                                }
                            }
                        }
                    }
                }
            }
            deleted_count
        })
        .await;
        match result {
            Ok(count) if count > 0 => {
                info!("日志清理完成，共删除了 {} 个旧日志文件。", count)
            }
            Ok(_) => info!("日志清理完成，没有需要删除的旧日志文件。"),
            Err(e) => error!("日志清理任务 panic: {}", e),
        }
    }
}
async fn log_requests(req: Request, next: Next) -> Response {
    next.run(req).await
}
#[derive(Clone)]
struct PrivateNetworkAccessLayer;
impl<S> Layer<S> for PrivateNetworkAccessLayer {
    type Service = PrivateNetworkAccessService<S>;
    fn layer(&self, inner: S) -> Self::Service {
        PrivateNetworkAccessService { inner }
    }
}
#[derive(Clone)]
struct PrivateNetworkAccessService<S> {
    inner: S,
}
impl<S, ReqBody> Service<axum::http::Request<ReqBody>> for PrivateNetworkAccessService<S>
where
    S: Service<axum::http::Request<ReqBody>, Response = Response> + Send + 'static,
    S::Future: Send + 'static,
    ReqBody: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = BoxFuture<'static, std::result::Result<Self::Response, Self::Error>>;
    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::result::Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }
    fn call(&mut self, req: axum::http::Request<ReqBody>) -> Self::Future {
        let future = self.inner.call(req);
        Box::pin(async move {
            let mut res: Response = future.await?;
            res.headers_mut().insert(
                "Access-Control-Allow-Private-Network",
                "true".parse().unwrap(),
            );
            Ok(res)
        })
    }
}

#[tokio::main]
async fn main() {
    // --- 日志初始化 (无变化) ---
    let file_appender = tracing_appender::rolling::hourly("./", "start.log");
    let (non_blocking_writer, _guard) = tracing_appender::non_blocking(file_appender);
    tracing_subscriber::registry()
        .with(fmt::layer().with_writer(non_blocking_writer).with_ansi(false))
        .with(fmt::layer().with_writer(std::io::stdout))
        .with(EnvFilter::from_default_env().add_directive("info".parse().unwrap()))
        .init();
    info!("程序启动，日志系统已初始化。");
    tokio::spawn(spawn_log_cleanup_task());

    // --- 1. 初始化依赖 (无变化) ---
    let api_client = Arc::new(ApiClient::new().expect("Failed to create API clients"));
    info!("API 客户端已初始化。");

    let db_manager = Arc::new(DbManager::new().await.expect("Failed to initialize DbManager"));
    info!("数据库管理器已初始化。");

    // --- 2. 注入依赖 (CacheManager::new 调用无变化) ---
    let cache_manager = Arc::new(CacheManager::new(
        api_client.clone(),
        db_manager.clone(),
    ));
    info!("数据服务已准备就绪。");
    
    // --- 3. 启动 TradingView 代理服务 ---
    let tv_proxy = Arc::new(tradingview_proxy::TradingViewProxy::new());
    let tv_proxy_start = tv_proxy.clone();
    tokio::spawn(async move {
        tv_proxy_start.start().await;
    });
    
    // --- 4. 启动 Binance WebSocket 代理服务 (K线, 端口 6002) ---
    let binance_proxy = Arc::new(binance_proxy::BinanceProxy::new());
    let binance_proxy_start = binance_proxy.clone();
    tokio::spawn(async move {
        binance_proxy_start.start().await;
    });
    
    // --- 5. 加载配置并启动私有数据流服务 (端口 6003) ---
    if let Some(config) = config::Config::load() {
        let config = Arc::new(config);
        
        // 创建带 API Key 的客户端
        match api_client::ApiClient::new_with_config(config.binance.clone()) {
            Ok(private_client) => {
                let private_client = Arc::new(private_client);
                let user_data_proxy = Arc::new(binance_proxy::UserDataProxy::new(
                    private_client,
                    config.clone(),
                ));
                let user_data_proxy_start = user_data_proxy.clone();
                tokio::spawn(async move {
                    user_data_proxy_start.start().await;
                });
                info!("✅ 私有数据流服务已启动 (端口 6003)");
            }
            Err(e) => {
                warn!("⚠️ 无法创建带 API Key 的客户端: {}", e);
            }
        }
    } else {
        info!("ℹ️ 配置文件未加载，私有数据流服务已禁用");
    }

    info!("✅ 所有服务已准备就绪。");

    // --- 3. 启动 Web 服务器 (无变化) ---
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers([
            header::CONTENT_TYPE,
            "Access-Control-Request-Private-Network".parse().unwrap(),
        ]);

    let app = Router::new()
        .route(
            "/api/account",
            get(web_server::proxy_account_handler),
        )
        .route(
            "/download/{symbol}/{interval}",
            get(web_server::proxy_kline_handler),
        )
        .route(
            "/download-json/{symbol}/{interval}",
            get(web_server::json_kline_handler),
        )
        .route(
            "/download-binary/{symbol}/{interval}",
            get(web_server::binary_kline_handler),
        )
        .route(
            "/test-download", 
            get(web_server::test_download_handler)
        )
        .route(
            "/test-download-binary",
            get(web_server::test_download_binary_handler),
        )
        .with_state(cache_manager)
        .layer(middleware::from_fn(log_requests))
        .layer(cors)
        .layer(PrivateNetworkAccessLayer);

    let addr = "127.0.0.1:30000";
    let listener = TcpListener::bind(addr).await.expect("Failed to bind");
    info!("🚀 服务正在监听 http://{}", addr);
    axum::serve(listener, app).await.unwrap();
}