use crate::config::Config;
use crate::error::{AppError, Result};
use axum::{
    extract::{Path, Query, State},
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    path::PathBuf,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::{fs, sync::RwLock};
use tracing::{error, info, warn};

const DEFAULT_DOWNLOAD_PROXY_URL: &str = "http://127.0.0.1:17892";
const DEFAULT_GATEWAY_CONTRACTS_URL: &str = "http://127.0.0.1:40000/api/contracts";
const CONTRACTS_CACHE_DIR: &str = "./meta_cache";
const CONTRACTS_CACHE_FILE: &str = "contracts_cache.json";
const ICONS_CACHE_DIR: &str = "icons";
const CONTRACTS_CACHE_TTL_MS: i64 = 5 * 60 * 1000;
const DEFAULT_BINANCE_REST_BASE: &str = "https://fapi.binance.com";
const DEFAULT_TOP_TURNOVER_LIMIT: usize = 15;
const MAX_TOP_TURNOVER_LIMIT: usize = 50;
const TOP_TURNOVER_CANDIDATE_MULTIPLIER: usize = 4;
const TOP_TURNOVER_MAX_AGE_MS: i64 = 10 * 60 * 1_000;
const TOP_TURNOVER_VALIDATION_KLINE_LIMIT: usize = 12;
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LocalContractInfo {
    pub name: String,
    pub symbol: String,
    pub description: String,
    pub image_url: String,
    pub website_url: String,
    pub explorer_url: String,
    pub current_price: f64,
    pub market_cap: f64,
    pub rank: i64,
    pub volume24h: f64,
    pub price_change_percent: f64,
    pub turnover_rate: f64,
    pub circulating_supply: f64,
    pub max_supply: f64,
    pub total_supply: f64,
    pub highest_price: f64,
    pub highest_price_date: i64,
    pub highest_price_confirmed: bool,
    pub lowest_price: f64,
    pub lowest_price_date: i64,
    pub lowest_price_confirmed: bool,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TopTurnoverSymbolInfo {
    pub symbol: String,
    pub quote_volume: f64,
    pub last_price: f64,
    pub price_change_percent: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ContractsCacheSnapshot {
    fetched_at_ms: i64,
    items: Vec<LocalContractInfo>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct FuturesTicker24hrDto {
    symbol: String,
    quote_volume: String,
    last_price: String,
    price_change_percent: String,
    close_time: i64,
}

#[derive(Debug, Deserialize)]
pub struct TopTurnoverQuery {
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct IconCacheMeta {
    source_url: String,
    content_type: String,
}

#[derive(Debug, Clone)]
struct CachedIconPayload {
    bytes: Vec<u8>,
    content_type: String,
    source_url: String,
}

pub struct MetaService {
    upstream_client: reqwest::Client,
    gateway_client: reqwest::Client,
    gateway_contracts_url: String,
    binance_rest_base: String,
    cache_dir: PathBuf,
    icons_dir: PathBuf,
    contracts_cache: RwLock<Option<ContractsCacheSnapshot>>,
}

impl MetaService {
    pub fn new(runtime_config: Option<&Config>) -> Result<Self> {
        let proxy_url = runtime_config
            .map(|config| config.binance.download_proxy_url().to_string())
            .unwrap_or_else(|| DEFAULT_DOWNLOAD_PROXY_URL.to_string());
        let upstream_client = reqwest::Client::builder()
            .proxy(reqwest::Proxy::all(&proxy_url).map_err(AppError::Reqwest)?)
            .timeout(std::time::Duration::from_secs(15))
            .build()
            .map_err(AppError::Reqwest)?;
        let gateway_client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(5))
            .build()
            .map_err(AppError::Reqwest)?;
        let cache_dir = PathBuf::from(CONTRACTS_CACHE_DIR);
        let icons_dir = cache_dir.join(ICONS_CACHE_DIR);

        Ok(Self {
            upstream_client,
            gateway_client,
            gateway_contracts_url: DEFAULT_GATEWAY_CONTRACTS_URL.to_string(),
            binance_rest_base: runtime_config
                .map(|config| config.binance.proxy_rest_base().trim_end_matches('/').to_string())
                .filter(|value| !value.is_empty())
                .unwrap_or_else(|| DEFAULT_BINANCE_REST_BASE.to_string()),
            cache_dir,
            icons_dir,
            contracts_cache: RwLock::new(None),
        })
    }

    pub async fn get_contracts(&self) -> Result<Vec<LocalContractInfo>> {
        Ok(self.load_contracts_snapshot().await?.items)
    }

    pub async fn get_top_turnover_symbols(
        &self,
        limit: usize,
    ) -> Result<Vec<TopTurnoverSymbolInfo>> {
        let response = self
            .upstream_client
            .get(format!("{}/fapi/v1/ticker/24hr", self.binance_rest_base))
            .send()
            .await
            .map_err(AppError::Reqwest)?
            .error_for_status()
            .map_err(AppError::Reqwest)?;
        let mut candidates = response
            .json::<Vec<FuturesTicker24hrDto>>()
            .await
            .map_err(AppError::Reqwest)?
            .into_iter()
            .filter_map(|item| {
                let raw_symbol = item.symbol.trim();
                let symbol = normalize_symbol(raw_symbol);
                let quote_volume = item.quote_volume.parse::<f64>().ok()?;
                let last_price = item.last_price.parse::<f64>().unwrap_or(0.0);
                let price_change_percent = item.price_change_percent.parse::<f64>().unwrap_or(0.0);

                if raw_symbol.is_empty() {
                    error!("top-turnover ticker returned empty symbol");
                    return None;
                }

                if symbol != raw_symbol.to_ascii_uppercase() {
                    error!(
                        "top-turnover ticker returned malformed symbol: raw_symbol={} normalized_symbol={} quote_volume={} close_time={}",
                        raw_symbol,
                        symbol,
                        quote_volume,
                        item.close_time
                    );
                    return None;
                }

                if quote_volume <= 0.0 {
                    return None;
                }

                if symbol == "USDT" {
                    error!(
                        "top-turnover ticker returned unexpected bare quote asset symbol: raw_symbol={} quote_volume={} close_time={}",
                        raw_symbol,
                        quote_volume,
                        item.close_time
                    );
                    return None;
                }

                if !symbol.ends_with("USDT") {
                    return None;
                }

                if now_ms().saturating_sub(item.close_time) > TOP_TURNOVER_MAX_AGE_MS {
                    warn!(
                        "skip stale top-turnover ticker: symbol={} close_time={} age_ms={}",
                        symbol,
                        item.close_time,
                        now_ms().saturating_sub(item.close_time)
                    );
                    return None;
                }

                Some(TopTurnoverSymbolInfo {
                    symbol,
                    quote_volume,
                    last_price,
                    price_change_percent,
                })
            })
            .collect::<Vec<_>>();

        candidates.sort_by(|left, right| right.quote_volume.total_cmp(&left.quote_volume));
        let requested_limit = limit.clamp(1, MAX_TOP_TURNOVER_LIMIT);
        let candidate_limit = (requested_limit * TOP_TURNOVER_CANDIDATE_MULTIPLIER).min(candidates.len());
        let mut items = Vec::with_capacity(requested_limit);

        for candidate in candidates.into_iter().take(candidate_limit) {
            match self.has_recent_positive_turnover(&candidate.symbol).await {
                Ok(true) => {
                    items.push(candidate);
                }
                Ok(false) => {
                    warn!(
                        "skip zero-turnover top-turnover candidate: symbol={}",
                        candidate.symbol
                    );
                }
                Err(error) => {
                    warn!(
                        "skip top-turnover candidate on validation error: symbol={} err={}",
                        candidate.symbol, error
                    );
                }
            }

            if items.len() >= requested_limit {
                break;
            }
        }

        if items.len() < requested_limit {
            warn!(
                "top-turnover candidates were filtered by validation: requested={} actual={}",
                requested_limit,
                items.len()
            );
        }

        info!("top-turnover returned {} validated symbols", items.len());
        Ok(items)
    }

    pub async fn get_icon_response(&self, symbol: &str) -> Result<Option<Response>> {
        let normalized_symbol = normalize_symbol(symbol);

        if normalized_symbol.is_empty() {
            return Ok(None);
        }

        let contracts = self.load_contracts_snapshot().await?.items;
        let contract = contracts
            .into_iter()
            .find(|item| normalize_symbol(&item.symbol) == normalized_symbol);
        let Some(contract) = contract else {
            return Ok(None);
        };
        let source_url = contract.image_url.trim().to_string();

        if source_url.is_empty() {
            return Ok(None);
        }

        let cached_icon = self.read_cached_icon(&normalized_symbol).await;

        if let Some(icon) = cached_icon.as_ref() {
            if icon.source_url == source_url {
                return Ok(Some(build_icon_response(&icon.bytes, &icon.content_type)));
            }
        }

        match self
            .download_and_cache_icon(&normalized_symbol, &source_url)
            .await
        {
            Ok(icon) => Ok(Some(build_icon_response(&icon.bytes, &icon.content_type))),
            Err(error) => {
                if let Some(icon) = cached_icon {
                    warn!(
                        "icon refresh failed, fallback to stale cache: symbol={} err={}",
                        normalized_symbol, error
                    );
                    Ok(Some(build_icon_response(&icon.bytes, &icon.content_type)))
                } else {
                    Err(error)
                }
            }
        }
    }

    async fn load_contracts_snapshot(&self) -> Result<ContractsCacheSnapshot> {
        if let Some(snapshot) = self.read_memory_contracts_if_fresh().await {
            return Ok(snapshot);
        }

        match self.fetch_contracts_from_gateway().await {
            Ok(snapshot) => {
                self.store_contracts_snapshot(&snapshot).await?;
                Ok(snapshot)
            }
            Err(fetch_error) => {
                if let Some(snapshot) = self.read_memory_contracts_any().await {
                    warn!("contracts upstream refresh failed, reuse memory cache: {fetch_error}");
                    return Ok(snapshot);
                }

                if let Some(snapshot) = self.read_contracts_snapshot_from_disk().await? {
                    warn!("contracts upstream refresh failed, reuse disk cache: {fetch_error}");
                    self.contracts_cache.write().await.replace(snapshot.clone());
                    return Ok(snapshot);
                }

                Err(fetch_error)
            }
        }
    }

    async fn read_memory_contracts_if_fresh(&self) -> Option<ContractsCacheSnapshot> {
        let guard = self.contracts_cache.read().await;
        let snapshot = guard.clone()?;

        if now_ms().saturating_sub(snapshot.fetched_at_ms) <= CONTRACTS_CACHE_TTL_MS {
            return Some(snapshot);
        }

        None
    }

    async fn read_memory_contracts_any(&self) -> Option<ContractsCacheSnapshot> {
        self.contracts_cache.read().await.clone()
    }

    async fn fetch_contracts_from_gateway(&self) -> Result<ContractsCacheSnapshot> {
        let response = self
            .gateway_client
            .get(&self.gateway_contracts_url)
            .send()
            .await
            .map_err(AppError::Reqwest)?
            .error_for_status()
            .map_err(AppError::Reqwest)?;
        let items = response
            .json::<Vec<LocalContractInfo>>()
            .await
            .map_err(AppError::Reqwest)?;

        Ok(ContractsCacheSnapshot {
            fetched_at_ms: now_ms(),
            items,
        })
    }

    async fn store_contracts_snapshot(&self, snapshot: &ContractsCacheSnapshot) -> Result<()> {
        fs::create_dir_all(&self.cache_dir).await?;
        let body = serde_json::to_vec(snapshot)?;
        fs::write(self.contracts_cache_path(), body).await?;
        self.contracts_cache.write().await.replace(snapshot.clone());
        Ok(())
    }

    async fn read_contracts_snapshot_from_disk(&self) -> Result<Option<ContractsCacheSnapshot>> {
        let path = self.contracts_cache_path();

        if !fs::try_exists(&path).await? {
            return Ok(None);
        }

        let body = fs::read(&path).await?;
        let snapshot = serde_json::from_slice::<ContractsCacheSnapshot>(&body)?;

        Ok(Some(snapshot))
    }

    async fn read_cached_icon(&self, symbol: &str) -> Option<CachedIconPayload> {
        let body_path = self.icon_body_path(symbol);
        let meta_path = self.icon_meta_path(symbol);

        if !fs::try_exists(&body_path).await.ok()? || !fs::try_exists(&meta_path).await.ok()? {
            return None;
        }

        let body = fs::read(&body_path).await.ok()?;
        let meta =
            serde_json::from_slice::<IconCacheMeta>(&fs::read(&meta_path).await.ok()?).ok()?;

        Some(CachedIconPayload {
            bytes: body,
            content_type: meta.content_type,
            source_url: meta.source_url,
        })
    }

    async fn download_and_cache_icon(
        &self,
        symbol: &str,
        source_url: &str,
    ) -> Result<CachedIconPayload> {
        let response = self
            .upstream_client
            .get(source_url)
            .send()
            .await
            .map_err(AppError::Reqwest)?
            .error_for_status()
            .map_err(AppError::Reqwest)?;
        let content_type = response
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .filter(|value| !value.trim().is_empty())
            .unwrap_or("application/octet-stream")
            .to_string();
        let bytes = response.bytes().await.map_err(AppError::Reqwest)?.to_vec();

        if bytes.is_empty() {
            return Err(AppError::ApiLogic(format!(
                "empty icon response: symbol={symbol}"
            )));
        }

        fs::create_dir_all(&self.icons_dir).await?;
        fs::write(self.icon_body_path(symbol), &bytes).await?;
        fs::write(
            self.icon_meta_path(symbol),
            serde_json::to_vec(&IconCacheMeta {
                source_url: source_url.to_string(),
                content_type: content_type.clone(),
            })?,
        )
        .await?;

        Ok(CachedIconPayload {
            bytes,
            content_type,
            source_url: source_url.to_string(),
        })
    }

    fn contracts_cache_path(&self) -> PathBuf {
        self.cache_dir.join(CONTRACTS_CACHE_FILE)
    }

    fn icon_body_path(&self, symbol: &str) -> PathBuf {
        self.icons_dir.join(format!("{symbol}.body"))
    }

    fn icon_meta_path(&self, symbol: &str) -> PathBuf {
        self.icons_dir.join(format!("{symbol}.json"))
    }

    async fn has_recent_positive_turnover(&self, symbol: &str) -> Result<bool> {
        let url = format!(
            "{}/fapi/v1/klines?symbol={}&interval=5m&limit={}",
            self.binance_rest_base, symbol, TOP_TURNOVER_VALIDATION_KLINE_LIMIT
        );
        let response = self
            .upstream_client
            .get(url)
            .send()
            .await
            .map_err(AppError::Reqwest)?
            .error_for_status()
            .map_err(AppError::Reqwest)?;
        let rows = response
            .json::<Vec<Vec<Value>>>()
            .await
            .map_err(AppError::Reqwest)?;

        Ok(rows.iter().any(|row| has_positive_quote_turnover(row)))
    }
}

fn normalize_symbol(symbol: &str) -> String {
    symbol
        .trim()
        .chars()
        .filter(|char| char.is_ascii_alphanumeric() || *char == '_' || *char == '-')
        .collect::<String>()
        .to_ascii_uppercase()
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

fn has_positive_quote_turnover(row: &[Value]) -> bool {
    row.get(7)
        .and_then(Value::as_str)
        .and_then(|value| value.parse::<f64>().ok())
        .is_some_and(|value| value > 0.0)
}

fn build_icon_response(bytes: &[u8], content_type: &str) -> Response {
    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        content_type
            .parse()
            .unwrap_or_else(|_| header::HeaderValue::from_static("application/octet-stream")),
    );
    headers.insert(
        header::CACHE_CONTROL,
        header::HeaderValue::from_static("public, max-age=86400"),
    );

    (headers, bytes.to_vec()).into_response()
}

pub async fn contracts_handler(State(service): State<Arc<MetaService>>) -> impl IntoResponse {
    match service.get_contracts().await {
        Ok(items) => Json(items).into_response(),
        Err(error) => error.into_response(),
    }
}

pub async fn top_turnover_handler(
    State(service): State<Arc<MetaService>>,
    Query(query): Query<TopTurnoverQuery>,
) -> impl IntoResponse {
    let limit = query.limit.unwrap_or(DEFAULT_TOP_TURNOVER_LIMIT);

    match service.get_top_turnover_symbols(limit).await {
        Ok(items) => Json(items).into_response(),
        Err(error) => error.into_response(),
    }
}

pub async fn icon_handler(
    State(service): State<Arc<MetaService>>,
    Path(symbol): Path<String>,
) -> impl IntoResponse {
    match service.get_icon_response(&symbol).await {
        Ok(Some(response)) => response,
        Ok(None) => StatusCode::NOT_FOUND.into_response(),
        Err(error) => error.into_response(),
    }
}
