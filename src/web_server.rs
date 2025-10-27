// src/web_server.rs
use crate::cache_manager::CacheManager;
use crate::error::Result;
use crate::models::DownloadTask;
use crate::transformer;
use axum::{
    extract::{Path, Query, State},
    http::{header, HeaderMap},
    response::IntoResponse,
    Json,
};
use serde::Deserialize;
use std::sync::Arc;
use tracing::info;

#[derive(Debug, Deserialize)]
pub struct KlineParams {
    // 这些参数目前由缓存逻辑处理，但保留结构以备将来使用
    #[serde(rename = "startTime")]
    pub _start_time: Option<i64>,
    #[serde(rename = "endTime")]
    pub _end_time: Option<i64>,
    pub _limit: Option<usize>,
}

/// Axum Handler: 代理对K线API的请求 (返回 JSON)
pub async fn proxy_kline_handler(
    State(cache_manager): State<Arc<CacheManager>>,
    Path((symbol, interval)): Path<(String, String)>,
    Query(_params): Query<KlineParams>, // params are ignored, cache manager handles logic
) -> Result<impl IntoResponse> {
    info!("Received JSON request for {}/{}", symbol, interval);

    let klines = cache_manager.get_klines(&symbol, &interval).await?;

    info!(
        "Responding with {} k-lines for JSON request: {}/{}",
        klines.len(),
        symbol,
        interval
    );

    Ok(Json(klines))
}

/// Axum Handler: 获取K线数据并以二进制格式返回
pub async fn binary_kline_handler(
    State(cache_manager): State<Arc<CacheManager>>,
    Path((symbol, interval)): Path<(String, String)>,
    Query(_params): Query<KlineParams>,
) -> Result<impl IntoResponse> {
    info!("Received BINARY request for {}/{}", symbol, interval);

    let klines = cache_manager
        .get_klines(&symbol, &interval)
        .await?;

    info!(
        "Transforming {} k-lines to binary format for: {}/{}",
        klines.len(),
        symbol,
        interval
    );
    let binary_blob = transformer::klines_to_binary_blob(&klines, &symbol, &interval)?;

    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        "application/octet-stream".parse().unwrap(),
    );

    Ok((headers, binary_blob))
}

// --- 测试端点现在从 CacheManager 中获取 ApiClient ---

/// Axum Handler: 一个简单的测试端点 (JSON)
pub async fn test_download_handler(
    State(cache_manager): State<Arc<CacheManager>>,
) -> Result<impl IntoResponse> {
    info!("Received test download request for BTCUSDT/1m limit=5");

    let task = DownloadTask {
        symbol: "BTCUSDT".to_string(),
        interval: "1m".to_string(),
        start_time: None,
        end_time: None,
        limit: 5,
    };

    // 直接使用 cache_manager 内部的 api_client，绕过缓存
    let klines = cache_manager
        .api_client
        .download_continuous_klines(&task)
        .await?;
    info!(
        "Test download successful, responding with {} k-lines.",
        klines.len()
    );

    Ok(Json(klines))
}

/// Axum Handler: 一个简单的测试端点，用于下载二进制 K 线
pub async fn test_download_binary_handler(
    State(cache_manager): State<Arc<CacheManager>>,
) -> Result<impl IntoResponse> {
    info!("Received test BINARY download request for BTCUSDT/5m limit=5");

    let symbol = "BTCUSDT".to_string();
    let interval = "5m".to_string();

    let task = DownloadTask {
        symbol: symbol.clone(),
        interval: interval.clone(),
        start_time: None,
        end_time: None,
        limit: 5,
    };

    let klines = cache_manager
        .api_client
        .download_continuous_klines(&task)
        .await?;

    info!(
        "Test binary transform successful with {} k-lines. Responding with binary blob.",
        klines.len()
    );
    let binary_blob = transformer::klines_to_binary_blob(&klines, &symbol, &interval)?;

    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        "application/octet-stream".parse().unwrap(),
    );

    Ok((headers, binary_blob))
}