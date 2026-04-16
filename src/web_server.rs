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

#[derive(Debug, Deserialize)]
pub struct KlineParams {
    pub source: Option<String>,
    #[serde(rename = "startTime")]
    pub start_time: Option<i64>,
    #[serde(rename = "endTime")]
    pub end_time: Option<i64>,
    pub limit: Option<usize>,
}

pub async fn proxy_kline_handler(
    State(cache_manager): State<Arc<CacheManager>>,
    Path((symbol, interval)): Path<(String, String)>,
    Query(params): Query<KlineParams>,
) -> Result<impl IntoResponse> {
    let source = params.source.unwrap_or_else(|| "update".to_string());
    let klines = cache_manager
        .get_klines(
            &symbol,
            &interval,
            &source,
            params.start_time,
            params.end_time,
            params.limit,
        )
        .await?;
    Ok(Json(klines))
}

pub async fn json_kline_handler(
    State(cache_manager): State<Arc<CacheManager>>,
    Path((symbol, interval)): Path<(String, String)>,
    Query(params): Query<KlineParams>,
) -> Result<impl IntoResponse> {
    let source = params.source.unwrap_or_else(|| "update".to_string());
    let klines = cache_manager
        .get_klines(
            &symbol,
            &interval,
            &source,
            params.start_time,
            params.end_time,
            params.limit,
        )
        .await?;
    let json_data: Vec<KlineJsonDto> = klines.iter().map(KlineJsonDto::from).collect();
    Ok(Json(json_data))
}

pub async fn binary_kline_handler(
    State(cache_manager): State<Arc<CacheManager>>,
    Path((symbol, interval)): Path<(String, String)>,
    Query(params): Query<KlineParams>,
) -> Result<impl IntoResponse> {
    let source = params.source.unwrap_or_else(|| "update".to_string());
    let klines = cache_manager
        .get_klines(
            &symbol,
            &interval,
            &source,
            params.start_time,
            params.end_time,
            params.limit,
        )
        .await?;
    let binary_blob = transformer::klines_to_binary_blob(&klines, &symbol, &interval)?;

    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        "application/octet-stream".parse().unwrap(),
    );

    Ok((headers, binary_blob))
}

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
