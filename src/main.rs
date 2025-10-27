// src/main.rs
mod api_client;
mod error;
mod models;
mod transformer;
mod web_server;

use crate::api_client::ApiClient;
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
use tracing::info;

/// 一个简单的中间件，用于打印每个收到的请求URL
async fn log_requests(req: Request, next: Next) -> Response {
    let method = req.method().clone();
    let uri = req.uri().clone();
    info!("⬅️ Received request: {} {}", method, uri);
    next.run(req).await
}

// --- 中间件用于添加 PNA 头部 (保持不变) ---
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

    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers([
            header::CONTENT_TYPE,
            "Access-Control-Request-Private-Network".parse().unwrap(),
        ]);
    info!("CORS middleware configured for PNA.");

    let app = Router::new()
        // 【路由语法修正】使用 {param} 替代 :param
        .route(
            "/download/{symbol}/{interval}",
            get(web_server::proxy_kline_handler),
        )
        .route(
            "/download-binary/{symbol}/{interval}",
            get(web_server::binary_kline_handler),
        )
        // 测试接口 (无参数，不受影响)
        .route("/test-download", get(web_server::test_download_handler))
        .route(
            "/test-download-binary",
            get(web_server::test_download_binary_handler),
        )
        .with_state(api_client)
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