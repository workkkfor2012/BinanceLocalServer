use crate::config::Config;
use crate::error::{AppError, Result};
use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use chrono::{SecondsFormat, TimeZone, Utc};
use reqwest::Client;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{
    sync::{Mutex, RwLock},
    time,
};
use tracing::{error, info, warn};

const DEFAULT_NEWS_PROXY_URL: &str = "http://127.0.0.1:17892";
const HTTP_TIMEOUT_SECS: u64 = 15;
const NEWS_FETCH_LIMIT: usize = 15;
const IMPORTANT_NEWS_REFRESH_INTERVAL: Duration = Duration::from_secs(10);
const SYMBOL_NEWS_CACHE_DURATION: Duration = Duration::from_secs(5 * 60);
const ODALY_IMPORTANT_URL: &str =
    "https://web-api.odaily.news/newsflash/page?page=1&size={limit}&isImport=true&groupId=0";
const ODALY_SYMBOL_URL: &str =
    "https://web-api.odaily.news/search/searchNewsflash?page=1&size={limit}&keywords={symbol}";
const BLOCKBEATS_IMPORTANT_URL: &str =
    "https://api.blockbeats.cn/v2/newsflash/list?page=1&limit={limit}&ios=1&end_time=&detective=-2";
const BLOCKBEATS_SYMBOL_URL: &str = "https://api.blockbeats.cn/v2/search/list?page=1&limit={limit}&title={symbol}&start_time=0&end_time=0&order=1&is_flash=1";
const SYMBOL_PLACEHOLDER: &str = "{symbol}";

#[derive(Debug, Deserialize)]
struct OdailyEnvelope {
    code: i64,
    data: Option<OdailyData>,
}

#[derive(Debug, Deserialize)]
struct OdailyData {
    #[serde(default)]
    list: Vec<OdailyItem>,
}

#[derive(Debug, Deserialize)]
struct OdailyItem {
    id: Value,
    #[serde(default)]
    title: String,
    #[serde(default)]
    description: String,
    #[serde(rename = "publishTimestamp")]
    publish_timestamp: Value,
    #[serde(rename = "newsUrl")]
    news_url: Option<String>,
}

#[derive(Debug, Deserialize)]
struct BlockbeatsEnvelope {
    code: i64,
    data: Option<BlockbeatsData>,
}

#[derive(Debug, Deserialize)]
struct BlockbeatsData {
    #[serde(default)]
    list: Vec<BlockbeatsItem>,
}

#[derive(Debug, Deserialize)]
struct BlockbeatsItem {
    id: Value,
    #[serde(default)]
    title: String,
    #[serde(default)]
    content: String,
    add_time: Value,
    url: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct NewsMessage {
    pub id: String,
    pub title: String,
    pub detail: String,
    pub time: String,
    pub url: Option<String>,
    #[serde(skip)]
    pub sort_ts_ms: i64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub struct NewsResponse {
    pub items: Vec<NewsMessage>,
}

#[derive(Clone, Debug)]
struct CacheEntry {
    created_at: Instant,
    data: NewsResponse,
}

impl CacheEntry {
    fn new(data: NewsResponse) -> Self {
        Self {
            created_at: Instant::now(),
            data,
        }
    }

    fn is_fresh(&self, ttl: Duration) -> bool {
        self.created_at.elapsed() < ttl
    }
}

pub struct NewsService {
    direct_client: Client,
    proxy_client: Option<Client>,
    important_cache: RwLock<Option<CacheEntry>>,
    symbol_cache: RwLock<HashMap<String, CacheEntry>>,
    important_refresh_lock: Mutex<()>,
}

impl NewsService {
    pub fn new(runtime_config: Option<&Config>) -> Result<Self> {
        let direct_client = build_http_client(None)?;
        let proxy_url = runtime_config
            .map(|config| config.binance.download_proxy_url().to_string())
            .unwrap_or_else(|| DEFAULT_NEWS_PROXY_URL.to_string());
        let proxy_client = Some(build_http_client(Some(&proxy_url))?);

        Ok(Self {
            direct_client,
            proxy_client,
            important_cache: RwLock::new(None),
            symbol_cache: RwLock::new(HashMap::new()),
            important_refresh_lock: Mutex::new(()),
        })
    }

    pub async fn warm_important_cache(&self) -> Result<()> {
        self.refresh_important_news_cache("startup").await
    }

    pub fn spawn_background_refresh(self: Arc<Self>) {
        tokio::spawn(async move {
            let mut interval = time::interval(IMPORTANT_NEWS_REFRESH_INTERVAL);
            interval.tick().await;

            loop {
                interval.tick().await;
                if let Err(error) = self.refresh_important_news_cache("interval").await {
                    warn!("important news interval refresh failed: {error}");
                }
            }
        });
    }

    pub async fn important_route(self: Arc<Self>) -> Response {
        match self.important_news_response().await {
            Ok(Some(response)) => (StatusCode::OK, Json(response)).into_response(),
            Ok(None) => error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "News data is not available yet, please try again shortly.",
            ),
            Err(error) => {
                error!("load important news failed: {error}");
                error_response(StatusCode::INTERNAL_SERVER_ERROR, &error.to_string())
            }
        }
    }

    pub async fn symbol_route(self: Arc<Self>, symbol: String) -> Response {
        match self.symbol_news_response(&symbol).await {
            Ok(response) => (StatusCode::OK, Json(response)).into_response(),
            Err(AppError::ApiLogic(message)) => error_response(StatusCode::BAD_REQUEST, &message),
            Err(error) => {
                error!(
                    "aggregate symbol news failed: symbol={} err={}",
                    symbol, error
                );
                error_response(StatusCode::INTERNAL_SERVER_ERROR, &error.to_string())
            }
        }
    }

    async fn important_news_response(&self) -> Result<Option<NewsResponse>> {
        if let Some(entry) = self.important_cache.read().await.as_ref() {
            return Ok(Some(entry.data.clone()));
        }

        if let Err(error) = self.refresh_important_news_cache("on_demand").await {
            warn!("important news on-demand refresh failed: {error}");
        }

        Ok(self
            .important_cache
            .read()
            .await
            .as_ref()
            .map(|entry| entry.data.clone()))
    }

    async fn symbol_news_response(&self, symbol: &str) -> Result<NewsResponse> {
        let normalized_symbol = normalize_symbol(symbol);
        if normalized_symbol.is_empty() {
            return Err(AppError::ApiLogic("symbol is required".to_string()));
        }

        let stale_entry = {
            let cache = self.symbol_cache.read().await;
            match cache.get(&normalized_symbol) {
                Some(entry) if entry.is_fresh(SYMBOL_NEWS_CACHE_DURATION) => {
                    return Ok(entry.data.clone());
                }
                Some(entry) => Some(entry.clone()),
                None => None,
            }
        };

        let response = match fetch_symbol_news_response(
            &self.direct_client,
            self.proxy_client.as_ref(),
            &normalized_symbol,
        )
        .await
        {
            Ok(response) => response,
            Err(error) => {
                if let Some(entry) = stale_entry {
                    warn!(
                        "symbol news refresh failed, fallback to stale cache: symbol={} err={}",
                        normalized_symbol, error
                    );
                    return Ok(entry.data);
                }
                return Err(error);
            }
        };

        self.symbol_cache
            .write()
            .await
            .insert(normalized_symbol, CacheEntry::new(response.clone()));

        Ok(response)
    }

    async fn refresh_important_news_cache(&self, reason: &str) -> Result<()> {
        let _guard = self.important_refresh_lock.lock().await;
        let (odaily_news, blockbeats_news) = tokio::join!(
            fetch_odaily_important_news(&self.direct_client, self.proxy_client.as_ref()),
            fetch_blockbeats_important_news(&self.direct_client, self.proxy_client.as_ref())
        );

        let merged = merge_news_lists(odaily_news, blockbeats_news);
        if merged.is_empty() {
            warn!("important news refresh returned empty payload: reason={reason}");
            return Ok(());
        }

        let count = merged.len();
        self.important_cache
            .write()
            .await
            .replace(CacheEntry::new(NewsResponse { items: merged }));
        info!("important news cache refreshed: reason={reason} items={count}");
        Ok(())
    }
}

fn error_response(status: StatusCode, message: &str) -> Response {
    (
        status,
        Json(serde_json::json!({
            "error": message,
            "items": Vec::<NewsMessage>::new(),
        })),
    )
        .into_response()
}

async fn fetch_symbol_news_response(
    direct_client: &Client,
    proxy_client: Option<&Client>,
    symbol: &str,
) -> Result<NewsResponse> {
    let (odaily_news, blockbeats_news) = tokio::join!(
        fetch_odaily_symbol_news(direct_client, proxy_client, symbol),
        fetch_blockbeats_symbol_news(direct_client, proxy_client, symbol)
    );

    Ok(NewsResponse {
        items: merge_news_lists(odaily_news, blockbeats_news),
    })
}

async fn fetch_odaily_important_news(
    direct_client: &Client,
    proxy_client: Option<&Client>,
) -> Vec<NewsMessage> {
    let url = ODALY_IMPORTANT_URL.replace("{limit}", &NEWS_FETCH_LIMIT.to_string());
    match fetch_json_with_fallback::<OdailyEnvelope>(direct_client, proxy_client, &url).await {
        Ok(payload) if payload.code == 200 => payload
            .data
            .map(|data| {
                data.list
                    .into_iter()
                    .map(transform_odaily_news_item)
                    .collect()
            })
            .unwrap_or_default(),
        Ok(_) => Vec::new(),
        Err(error) => {
            warn!("fetch odaily important news failed: {error}");
            Vec::new()
        }
    }
}

async fn fetch_odaily_symbol_news(
    direct_client: &Client,
    proxy_client: Option<&Client>,
    symbol: &str,
) -> Vec<NewsMessage> {
    let url = ODALY_SYMBOL_URL
        .replace("{limit}", &NEWS_FETCH_LIMIT.to_string())
        .replace(SYMBOL_PLACEHOLDER, symbol.to_ascii_lowercase().as_str());
    match fetch_json_with_fallback::<OdailyEnvelope>(direct_client, proxy_client, &url).await {
        Ok(payload) if payload.code == 200 => payload
            .data
            .map(|data| {
                data.list
                    .into_iter()
                    .map(transform_odaily_news_item)
                    .collect()
            })
            .unwrap_or_default(),
        Ok(_) => Vec::new(),
        Err(error) => {
            warn!(
                "fetch odaily symbol news failed: symbol={} err={}",
                symbol, error
            );
            Vec::new()
        }
    }
}

async fn fetch_blockbeats_important_news(
    direct_client: &Client,
    proxy_client: Option<&Client>,
) -> Vec<NewsMessage> {
    let url = BLOCKBEATS_IMPORTANT_URL.replace("{limit}", &NEWS_FETCH_LIMIT.to_string());
    match fetch_json_with_fallback::<BlockbeatsEnvelope>(direct_client, proxy_client, &url).await {
        Ok(payload) if payload.code == 0 => payload
            .data
            .map(|data| {
                data.list
                    .into_iter()
                    .map(transform_blockbeats_news_item)
                    .collect()
            })
            .unwrap_or_default(),
        Ok(_) => Vec::new(),
        Err(error) => {
            warn!("fetch blockbeats important news failed: {error}");
            Vec::new()
        }
    }
}

async fn fetch_blockbeats_symbol_news(
    direct_client: &Client,
    proxy_client: Option<&Client>,
    symbol: &str,
) -> Vec<NewsMessage> {
    let url = BLOCKBEATS_SYMBOL_URL
        .replace("{limit}", &NEWS_FETCH_LIMIT.to_string())
        .replace(SYMBOL_PLACEHOLDER, symbol);
    match fetch_json_with_fallback::<BlockbeatsEnvelope>(direct_client, proxy_client, &url).await {
        Ok(payload) if payload.code == 0 => payload
            .data
            .map(|data| {
                data.list
                    .into_iter()
                    .map(transform_blockbeats_news_item)
                    .collect()
            })
            .unwrap_or_default(),
        Ok(_) => Vec::new(),
        Err(error) => {
            warn!(
                "fetch blockbeats symbol news failed: symbol={} err={}",
                symbol, error
            );
            Vec::new()
        }
    }
}

async fn fetch_json_with_fallback<T: DeserializeOwned>(
    direct_client: &Client,
    proxy_client: Option<&Client>,
    url: &str,
) -> Result<T> {
    match fetch_json_once(direct_client, url).await {
        Ok(value) => Ok(value),
        Err(direct_error) => {
            let Some(proxy_client) = proxy_client else {
                return Err(direct_error);
            };
            warn!(
                "direct news fetch failed, retry with proxy: url={} err={}",
                url, direct_error
            );
            fetch_json_once(proxy_client, url).await
        }
    }
}

async fn fetch_json_once<T: DeserializeOwned>(client: &Client, url: &str) -> Result<T> {
    client
        .get(url)
        .send()
        .await
        .map_err(AppError::Reqwest)?
        .error_for_status()
        .map_err(AppError::Reqwest)?
        .json::<T>()
        .await
        .map_err(AppError::Reqwest)
}

fn build_http_client(proxy: Option<&str>) -> Result<Client> {
    let mut builder = reqwest::Client::builder()
        .timeout(Duration::from_secs(HTTP_TIMEOUT_SECS))
        .user_agent("Mozilla/5.0 BinanceLocalServer/1.0");
    if let Some(proxy_url) = proxy {
        builder = builder.proxy(reqwest::Proxy::all(proxy_url).map_err(AppError::Reqwest)?);
    }
    builder.build().map_err(AppError::Reqwest)
}

fn transform_odaily_news_item(item: OdailyItem) -> NewsMessage {
    let ts_ms = value_to_i64(&item.publish_timestamp);
    NewsMessage {
        id: format!("odaily-{}", value_to_string(&item.id)),
        title: item.title,
        detail: strip_html_tags(&item.description),
        time: timestamp_to_iso(ts_ms),
        url: item.news_url.filter(|value| !value.trim().is_empty()),
        sort_ts_ms: ts_ms,
    }
}

fn transform_blockbeats_news_item(item: BlockbeatsItem) -> NewsMessage {
    let ts_ms = value_to_i64(&item.add_time).saturating_mul(1000);
    NewsMessage {
        id: format!("blockbeats-{}", value_to_string(&item.id)),
        title: item.title,
        detail: strip_html_tags(&item.content),
        time: timestamp_to_iso(ts_ms),
        url: item.url.filter(|value| !value.trim().is_empty()),
        sort_ts_ms: ts_ms,
    }
}

fn timestamp_to_iso(ts_ms: i64) -> String {
    Utc.timestamp_millis_opt(ts_ms.max(0))
        .single()
        .unwrap_or_else(|| {
            Utc.timestamp_millis_opt(0)
                .single()
                .expect("epoch should exist")
        })
        .to_rfc3339_opts(SecondsFormat::Millis, true)
}

fn value_to_i64(value: &Value) -> i64 {
    match value {
        Value::Number(number) => number.as_i64().unwrap_or(0),
        Value::String(text) => text.trim().parse::<i64>().unwrap_or(0),
        _ => 0,
    }
}

fn value_to_string(value: &Value) -> String {
    match value {
        Value::String(text) => text.clone(),
        Value::Number(number) => number.to_string(),
        Value::Bool(flag) => flag.to_string(),
        Value::Null => String::new(),
        other => other.to_string(),
    }
}

fn strip_html_tags(input: &str) -> String {
    let mut output = String::with_capacity(input.len());
    let mut in_tag = false;
    for ch in input.chars() {
        match ch {
            '<' => in_tag = true,
            '>' => in_tag = false,
            _ if !in_tag => output.push(ch),
            _ => {}
        }
    }
    output
}

fn normalize_symbol(symbol: &str) -> String {
    symbol
        .trim()
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric() || *ch == '_' || *ch == '-')
        .collect::<String>()
        .to_ascii_uppercase()
}

fn merge_news_lists(mut left: Vec<NewsMessage>, mut right: Vec<NewsMessage>) -> Vec<NewsMessage> {
    left.append(&mut right);
    left.sort_by(|a, b| b.sort_ts_ms.cmp(&a.sort_ts_ms));
    dedupe_adjacent_titles(left)
}

fn dedupe_adjacent_titles(items: Vec<NewsMessage>) -> Vec<NewsMessage> {
    let mut deduped = Vec::with_capacity(items.len());
    let mut last_title: Option<String> = None;

    for item in items {
        let normalized = item.title.trim().to_ascii_lowercase();
        if last_title.as_deref() == Some(normalized.as_str()) {
            continue;
        }
        last_title = Some(normalized);
        deduped.push(item);
    }

    deduped
}

#[cfg(test)]
mod tests {
    use super::{dedupe_adjacent_titles, merge_news_lists, strip_html_tags, NewsMessage};

    fn item(id: &str, title: &str, ts: i64) -> NewsMessage {
        NewsMessage {
            id: id.to_string(),
            title: title.to_string(),
            detail: String::new(),
            time: String::new(),
            url: None,
            sort_ts_ms: ts,
        }
    }

    #[test]
    fn strip_html_tags_keeps_plain_text() {
        assert_eq!(strip_html_tags("hello <b>world</b>"), "hello world");
    }

    #[test]
    fn dedupe_adjacent_titles_matches_node_behavior() {
        let items = vec![
            item("1", "Hello", 3),
            item("2", " hello ", 2),
            item("3", "World", 1),
        ];
        let deduped = dedupe_adjacent_titles(items);
        assert_eq!(deduped.len(), 2);
        assert_eq!(deduped[0].id, "1");
        assert_eq!(deduped[1].id, "3");
    }

    #[test]
    fn merge_news_lists_sorts_descending_before_deduping() {
        let merged = merge_news_lists(
            vec![item("1", "Alpha", 1), item("2", "Same", 3)],
            vec![item("3", "same", 2), item("4", "Beta", 4)],
        );
        assert_eq!(
            merged
                .iter()
                .map(|item| item.id.as_str())
                .collect::<Vec<_>>(),
            vec!["4", "2", "1"]
        );
    }
}
