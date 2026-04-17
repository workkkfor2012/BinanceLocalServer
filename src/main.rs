mod api_client;
mod binance_proxy;
mod cache_manager;
mod config;
mod db_manager;
mod error;
mod meta_service;
mod models;
mod news_service;
mod tradingview_proxy;
mod transformer;
mod utils;
mod web_server;

use crate::api_client::ApiClient;
use crate::cache_manager::CacheManager;
use crate::db_manager::DbManager;
use crate::meta_service::MetaService;
use crate::news_service::NewsService;
use axum::{
    extract::{ws::WebSocketUpgrade, Path as AxumPath, Request},
    http::header,
    middleware::{self, Next},
    response::Response,
    routing::get,
    Router,
};
use futures::future::BoxFuture;
use std::env;
use std::fs;
use std::path::Path;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::time::{interval, Duration};
use tower::{Layer, Service};
use tower_http::cors::{Any, CorsLayer};
use tracing::{error, info, warn};
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

async fn spawn_log_cleanup_task() {
    info!("log cleanup task started");
    let mut timer = interval(Duration::from_secs(3600));

    loop {
        timer.tick().await;
        let result = tokio::task::spawn_blocking(move || {
            let log_dir = Path::new("./");
            let cutoff = chrono::Local::now() - chrono::Duration::hours(12);
            let mut deleted_count = 0;

            let entries = match fs::read_dir(log_dir) {
                Ok(entries) => entries,
                Err(e) => {
                    warn!("failed to read log directory: {}", e);
                    return 0;
                }
            };

            for entry in entries.filter_map(Result::ok) {
                let path = entry.path();
                if path.is_file()
                    && path
                        .file_name()
                        .and_then(|s| s.to_str())
                        .is_some_and(|name| name.starts_with("start.log."))
                {
                    if let Ok(metadata) = entry.metadata() {
                        if let Ok(modified_time) = metadata.modified() {
                            let modified_time: chrono::DateTime<chrono::Local> =
                                modified_time.into();
                            if modified_time < cutoff {
                                match fs::remove_file(&path) {
                                    Ok(_) => {
                                        deleted_count += 1;
                                        info!("deleted old log file: {:?}", path);
                                    }
                                    Err(e) => {
                                        warn!("failed to delete old log file {:?}: {}", path, e)
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
            Ok(count) if count > 0 => info!("log cleanup completed, deleted {} files", count),
            Ok(_) => info!("log cleanup completed, nothing to delete"),
            Err(e) => error!("log cleanup task panic: {}", e),
        }
    }
}

async fn log_requests(req: Request, next: Next) -> Response {
    next.run(req).await
}

fn http_bind_addr() -> String {
    env::var("BINANCE_LOCAL_SERVER_HTTP_BIND")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "127.0.0.1:30000".to_string())
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
    let file_appender = tracing_appender::rolling::hourly("./", "start.log");
    let (non_blocking_writer, _guard) = tracing_appender::non_blocking(file_appender);
    tracing_subscriber::registry()
        .with(
            fmt::layer()
                .with_writer(non_blocking_writer)
                .with_ansi(false),
        )
        .with(fmt::layer().with_writer(std::io::stdout))
        .with(EnvFilter::from_default_env().add_directive("info".parse().unwrap()))
        .init();

    info!("starting BinanceLocalServer");
    tokio::spawn(spawn_log_cleanup_task());

    let runtime_config = config::Config::load().map(Arc::new);
    let api_client = Arc::new(
        runtime_config
            .as_ref()
            .map(|cfg| ApiClient::new_public_with_config(&cfg.binance))
            .unwrap_or_else(ApiClient::new)
            .expect("failed to create API clients"),
    );
    api_client
        .clone()
        .sync_server_time()
        .await
        .expect("failed to sync server time");
    api_client.clone().spawn_sync_loop();

    let db_manager = Arc::new(
        DbManager::new()
            .await
            .expect("failed to initialize DbManager"),
    );
    let cache_manager = Arc::new(CacheManager::new(api_client, db_manager));
    let meta_service = Arc::new(
        MetaService::new(runtime_config.as_deref()).expect("failed to initialize MetaService"),
    );
    meta_service.warm_caches().await;
    meta_service.clone().spawn_background_refresh();
    let news_service = Arc::new(
        NewsService::new(runtime_config.as_deref()).expect("failed to initialize NewsService"),
    );
    if let Err(error) = news_service.warm_important_cache().await {
        warn!("important news warmup failed: {}", error);
    }
    news_service.clone().spawn_background_refresh();

    let tv_proxy = Arc::new(tradingview_proxy::TradingViewProxy::new(
        runtime_config
            .as_ref()
            .map(|cfg| cfg.binance.ws_proxy_addr()),
    ));
    let tv_proxy_task = tv_proxy.clone();
    tokio::spawn(async move {
        tv_proxy_task.start().await;
    });

    let binance_proxy = Arc::new(binance_proxy::BinanceProxy::new(
        runtime_config.as_ref().map(|cfg| &cfg.binance),
    ));
    let binance_proxy_task = binance_proxy.clone();
    tokio::spawn(async move {
        binance_proxy_task.start().await;
    });

    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers([
            header::CONTENT_TYPE,
            "Access-Control-Request-Private-Network".parse().unwrap(),
        ]);

    let binance_ws_route = {
        let binance_proxy = binance_proxy.clone();
        move |ws: WebSocketUpgrade| {
            let binance_proxy = binance_proxy.clone();
            async move { binance_proxy.frontend_ws_upgrade(ws) }
        }
    };

    let tradingview_ws_route = {
        let tv_proxy = tv_proxy.clone();
        move |ws: WebSocketUpgrade| {
            let tv_proxy = tv_proxy.clone();
            async move { tv_proxy.frontend_ws_upgrade(ws) }
        }
    };

    let app = Router::new()
        .route("/ws/binance", get(binance_ws_route))
        .route("/ws/tradingview", get(tradingview_ws_route))
        .route(
            "/api/news/important",
            get({
                let news_service = news_service.clone();
                move || {
                    let news_service = news_service.clone();
                    async move { news_service.important_route().await }
                }
            }),
        )
        .route(
            "/api/news/symbol/{symbol}",
            get({
                let news_service = news_service.clone();
                move |AxumPath(symbol): AxumPath<String>| {
                    let news_service = news_service.clone();
                    async move { news_service.symbol_route(symbol).await }
                }
            }),
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
        .route("/test-download", get(web_server::test_download_handler))
        .route(
            "/test-download-binary",
            get(web_server::test_download_binary_handler),
        )
        .merge(
            Router::new()
                .route(
                    "/api/exchange-info",
                    get(meta_service::exchange_info_handler),
                )
                .route("/api/contracts", get(meta_service::contracts_handler))
                .route(
                    "/api/contracts/{symbol}",
                    get(meta_service::contract_handler),
                )
                .route("/meta/contracts", get(meta_service::contracts_handler))
                .route(
                    "/meta/top-turnover",
                    get(meta_service::top_turnover_handler),
                )
                .route("/meta/icon/{symbol}", get(meta_service::icon_handler))
                .with_state(meta_service),
        )
        .with_state(cache_manager)
        .layer(middleware::from_fn(log_requests))
        .layer(cors)
        .layer(PrivateNetworkAccessLayer);

    let addr = http_bind_addr();
    let listener = TcpListener::bind(&addr).await.expect("failed to bind");
    info!("HTTP server listening on http://{}", addr);
    info!("Binance WS mounted at ws://{}/ws/binance", addr);
    info!("TradingView WS mounted at ws://{}/ws/tradingview", addr);
    axum::serve(listener, app).await.unwrap();
}
