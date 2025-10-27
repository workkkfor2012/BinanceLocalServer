// src/main.rs
mod api_client;
mod cache_manager;
mod db_manager;
mod error;
mod models;
mod transformer;
mod utils;
mod web_server;

use crate::api_client::ApiClient;
use crate::cache_manager::{CacheManager, KLINE_CACHE_LIMIT};
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
use std::sync::Arc;
use tokio::net::TcpListener;
use tower::{Layer, Service};
use tower_http::cors::{Any, CorsLayer};
use tracing::{error, info};

async fn log_requests(req: Request, next: Next) -> Response {
    let method = req.method().clone();
    let uri = req.uri().clone();
    info!("⬅️ Received request: {} {}", method, uri);
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
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    info!("Starting K-line API proxy service...");

    let api_client = Arc::new(ApiClient::new().expect("Failed to create API clients"));
    info!("API clients initialized.");

    let db_manager = Arc::new(DbManager::new().await.expect("Failed to initialize DbManager"));
    info!("Database manager initialized.");

    info!("--- 📊 Database Cache Summary ---");
    match db_manager.get_db_summary().await {
        Ok(summary) => {
            if summary.is_empty() {
                info!("   Database is empty. Cache will be built on first request.");
            } else {
                let mut sorted_symbols: Vec<_> = summary.keys().cloned().collect();
                sorted_symbols.sort();

                for symbol in sorted_symbols {
                    if let Some(intervals) = summary.get(&symbol) {
                        let mut sorted_intervals = intervals.clone();
                        sorted_intervals.sort_by_key(|(interval_str, _)| {
                            utils::interval_to_milliseconds(interval_str).unwrap_or(i64::MAX)
                        });

                        let parts: Vec<String> = sorted_intervals
                            .iter()
                            .map(|(interval, count)| format!("{}: {}", interval, count))
                            .collect();
                        
                        info!("   - {:<15} | {}", symbol, parts.join(" | "));
                    }
                }
            }
        }
        Err(e) => {
            error!("   Failed to get database summary: {}", e);
        }
    }
    info!("------------------------------------");

    info!("🔥 Starting cache pre-warming from database...");
    let cache_manager = Arc::new(CacheManager::new(api_client.clone(), db_manager.clone()));
    
    let keys_to_warm = db_manager.get_all_cache_keys().await.unwrap_or_else(|e| {
        error!("Failed to get cache keys from DB: {}", e);
        vec![]
    });

    let total_keys = keys_to_warm.len();
    info!("Found {} unique (symbol, interval) keys to warm.", total_keys);

    for (i, (symbol, interval)) in keys_to_warm.into_iter().enumerate() {
        // --- 【核心修改】 ---
        match db_manager.get_latest_klines(&symbol, &interval, KLINE_CACHE_LIMIT).await {
            Ok(klines) if !klines.is_empty() => {
                // 在日志中加入 klines.len()
                info!(
                    "[{}/{}] Warming cache for {}/{}... ({} klines)",
                    i + 1,
                    total_keys,
                    symbol,
                    interval,
                    klines.len()
                );
                cache_manager.warm_up(&symbol, &interval, klines);
            }
            Ok(_) => info!(
                "[{}/{}] No data in DB for {}/{}, skipping.",
                i + 1, total_keys, symbol, interval
            ),
            Err(e) => error!(
                "[{}/{}] Failed to warm up cache for {}/{}: {}",
                i + 1, total_keys, symbol, interval, e
            ),
        }
    }
    info!("✅ Cache pre-warming finished.");

    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers([
            header::CONTENT_TYPE,
            "Access-Control-Request-Private-Network".parse().unwrap(),
        ]);
    info!("CORS middleware configured for PNA.");

    let app = Router::new()
        .route(
            "/download/{symbol}/{interval}",
            get(web_server::proxy_kline_handler),
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
        .with_state(cache_manager)
        .layer(middleware::from_fn(log_requests))
        .layer(cors)
        .layer(PrivateNetworkAccessLayer);

    let addr = "127.0.0.1:3000";
    let listener = TcpListener::bind(addr).await.expect("Failed to bind");
    info!("🚀 Server listening on http://{}", addr);
    info!("🌐 Now accessible from public websites due to PNA headers.");
    info!("---");
    info!("👉 JSON Test endpoint:   curl http://{}/test-download", addr);
    info!(
        "👉 Binary Test endpoint: curl http://{}/test-download-binary -o test.bin",
        addr
    );
    info!("---");
    info!(
        "👉 General JSON endpoint:   curl \"http://{}/download/ETHUSDT/5m?limit=10\"",
        addr
    );
    info!(
        "👉 General Binary endpoint: curl \"http://{}/download-binary/BTCUSDT/5m?limit=10\" -o klines.bin",
        addr
    );
    info!("---");

    axum::serve(listener, app).await.unwrap();
}