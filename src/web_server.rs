// src/web_server.rs
use crate::api_client::ApiClient;
use crate::error::Result;
use crate::models::DownloadTask;
use crate::transformer; // <-- 引入 transformer 模块
use axum::{
    extract::{Path, Query, State},
    http::{header, HeaderMap}, // <-- 引入 HeaderMap
    response::IntoResponse,
    Json,
};
use serde::Deserialize;
use std::sync::Arc;
use tracing::info;

/// 定义用于接收查询参数的结构体
#[derive(Debug, Deserialize)]
pub struct KlineParams {
    #[serde(rename = "startTime")]
    pub start_time: Option<i64>,
    #[serde(rename = "endTime")]
    pub end_time: Option<i64>,
    pub limit: Option<usize>,
}

/// Axum Handler: 代理对K线API的请求 (返回 JSON)
/// (此函数保持不变)
pub async fn proxy_kline_handler(
    State(api_client): State<Arc<ApiClient>>,
    Path((symbol, interval)): Path<(String, String)>,
    Query(params): Query<KlineParams>,
) -> Result<impl IntoResponse> {
    info!(
        "Received JSON request for {}/{} with params: {:?}",
        symbol, interval, params
    );

    let task = DownloadTask {
        symbol,
        interval,
        start_time: params.start_time,
        end_time: params.end_time,
        limit: params.limit.unwrap_or(500),
    };

    let klines = api_client.download_continuous_klines(&task).await?;
    info!(
        "Responding with {} k-lines for JSON task: {:?}",
        klines.len(),
        task
    );

    Ok(Json(klines))
}

/// Axum Handler: 获取K线数据并以二进制格式返回
/// e.g., GET /download-binary/BTCUSDT/5m?limit=2000
pub async fn binary_kline_handler(
    State(api_client): State<Arc<ApiClient>>,
    Path((symbol, interval)): Path<(String, String)>,
    Query(params): Query<KlineParams>,
) -> Result<impl IntoResponse> {
    info!(
        "Received BINARY request for {}/{} with params: {:?}",
        symbol, interval, params
    );
    
    // 规范要求默认 2000, 且最大容量也是 2000
    let limit = params.limit.unwrap_or(2000).min(2000);

    let task = DownloadTask {
        symbol: symbol.clone(), 
        interval: interval.clone(),
        start_time: params.start_time,
        end_time: params.end_time,
        limit,
    };

    let klines = api_client.download_continuous_klines(&task).await?;

    info!(
        "Transforming {} k-lines to binary format for task: {:?}",
        klines.len(),
        task
    );
    let binary_blob = transformer::klines_to_binary_blob(&klines, &symbol, &interval)?;

    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        "application/octet-stream".parse().unwrap(),
    );

    Ok((headers, binary_blob))
}

/// Axum Handler: 一个简单的测试端点 (JSON)
/// e.g., GET /test-download
pub async fn test_download_handler(
    State(api_client): State<Arc<ApiClient>>,
) -> Result<impl IntoResponse> {
    info!("Received test download request for BTCUSDT/1m limit=5");

    let task = DownloadTask {
        symbol: "BTCUSDT".to_string(),
        interval: "1m".to_string(),
        start_time: None,
        end_time: None,
        limit: 5,
    };

    let klines = api_client.download_continuous_klines(&task).await?;
    info!(
        "Test download successful, responding with {} k-lines.",
        klines.len()
    );

    Ok(Json(klines))
}

/// 【新增】Axum Handler: 一个简单的测试端点，用于下载二进制 K 线
/// e.g., GET /test-download-binary
pub async fn test_download_binary_handler(
    State(api_client): State<Arc<ApiClient>>,
) -> Result<impl IntoResponse> {
    info!("Received test BINARY download request for BTCUSDT/5m limit=5");

    let symbol = "BTCUSDT".to_string();
    let interval = "5m".to_string(); // 使用一个二进制格式支持的周期

    let task = DownloadTask {
        symbol: symbol.clone(),
        interval: interval.clone(),
        start_time: None,
        end_time: None,
        limit: 5,
    };

    // 1. 获取K线数据
    let klines = api_client.download_continuous_klines(&task).await?;

    // 2. 转换为二进制
    info!(
        "Test binary transform successful with {} k-lines. Responding with binary blob.",
        klines.len()
    );
    let binary_blob = transformer::klines_to_binary_blob(&klines, &symbol, &interval)?;

    // 3. 构造二进制响应
    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        "application/octet-stream".parse().unwrap(),
    );

    Ok((headers, binary_blob))
}