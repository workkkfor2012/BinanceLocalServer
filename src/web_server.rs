// src/web_server.rs
use crate::cache_manager::CacheManager;
use crate::error::Result;
use crate::models::{DownloadTask, KlineJsonDto};
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

// 修改 KlineParams 来包含新的 'source' 字段
#[derive(Debug, Deserialize)]
pub struct KlineParams {
    pub source: Option<String>,
    // 其他参数保留，即使当前未使用
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
    Query(params): Query<KlineParams>,
) -> Result<impl IntoResponse> {
    // 默认行为是 'update'
    let source = params.source.unwrap_or_else(|| "update".to_string());

    let klines = cache_manager
        .get_klines(&symbol, &interval, &source)
        .await?;

    Ok(Json(klines))
}

/// Axum Handler: 获取 K 线数据并以符合前端要求的 JSON 格式返回 (秒级时间戳, 数值类型)
pub async fn json_kline_handler(
    State(cache_manager): State<Arc<CacheManager>>,
    Path((symbol, interval)): Path<(String, String)>,
    Query(params): Query<KlineParams>,
) -> Result<impl IntoResponse> {
    let source = params.source.unwrap_or_else(|| "update".to_string());

    let klines = cache_manager
        .get_klines(&symbol, &interval, &source)
        .await?;

    let json_data: Vec<KlineJsonDto> = klines.iter().map(KlineJsonDto::from).collect();

    Ok(Json(json_data))
}

/// Axum Handler: 获取K线数据并以二进制格式返回
pub async fn binary_kline_handler(
    State(cache_manager): State<Arc<CacheManager>>,
    Path((symbol, interval)): Path<(String, String)>,
    Query(params): Query<KlineParams>,
) -> Result<impl IntoResponse> {
    // 默认行为是 'update'
    let source = params.source.unwrap_or_else(|| "update".to_string());

    let klines = cache_manager
        .get_klines(&symbol, &interval, &source)
        .await?;

    let binary_blob = transformer::klines_to_binary_blob(&klines, &symbol, &interval)?;

    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        "application/octet-stream".parse().unwrap(),
    );

    Ok((headers, binary_blob))
}

// --- 测试端点保持不变，它们不参与两阶段加载逻辑 ---

/// Axum Handler: 一个简单的测试端点 (JSON)
pub async fn test_download_handler(
    State(cache_manager): State<Arc<CacheManager>>,
) -> Result<impl IntoResponse> {

    let task = DownloadTask {
        symbol: "BTCUSDT".to_string(),
        interval: "1m".to_string(),
        start_time: None,
        end_time: None,
        limit: 5,
    };

    let klines = cache_manager
        .api_client
        .download_continuous_klines(&task)
        .await?;

    Ok(Json(klines))
}

/// Axum Handler: 一个简单的测试端点，用于下载二进制 K 线
pub async fn test_download_binary_handler(
    State(cache_manager): State<Arc<CacheManager>>,
) -> Result<impl IntoResponse> {

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

    let binary_blob = transformer::klines_to_binary_blob(&klines, &symbol, &interval)?;

    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        "application/octet-stream".parse().unwrap(),
    );

    Ok((headers, binary_blob))
}