use crate::config::Config;
use crate::error::{AppError, Result};
use crate::utils::resolve_runtime_base_dir;
use axum::{
    extract::{Path, Query, State},
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use futures::stream::{self, StreamExt};
use serde::{de::DeserializeOwned, Deserialize, Deserializer, Serialize};
use serde_json::Value;
use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    fs,
    sync::{Mutex, RwLock},
    time,
};
use tracing::{info, warn};

const CONTRACTS_CACHE_DIR: &str = "meta_cache";
const CONTRACTS_CACHE_FILE: &str = "contracts_cache.json";
const EXCHANGE_INFO_CACHE_FILE: &str = "exchange_info_cache.json";
const ICONS_CACHE_DIR: &str = "icons";
const CONTRACTS_CACHE_TTL_MS: i64 = 5 * 60 * 1000;
const EXCHANGE_INFO_CACHE_TTL_MS: i64 = 5 * 60 * 1000;
const BACKGROUND_REFRESH_SECS: u64 = 300;
const DEFAULT_BINANCE_REST_BASE: &str = "https://fapi.binance.com";
const DEFAULT_TOP_TURNOVER_LIMIT: usize = 15;
const MAX_TOP_TURNOVER_LIMIT: usize = 50;
const TOP_TURNOVER_CANDIDATE_MULTIPLIER: usize = 4;
const TOP_TURNOVER_MAX_AGE_MS: i64 = 10 * 60 * 1_000;
const TOP_TURNOVER_VALIDATION_KLINE_LIMIT: usize = 12;
const BINANCE_DESCRIPTIONS_URL: &str =
    "https://bin.bnbstatic.com/api/i18n/-/web/cms/zh-CN/symbol-description";
const BINANCE_CONTRACT_DETAIL_URL: &str =
    "https://www.binance.com/bapi/composite/v1/public/marketing/tardingPair/detail";
const CONTRACT_DETAIL_CONCURRENCY: usize = 16;
const UNKNOWN_RANK: i64 = i64::MAX;

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

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ContractExchangeInfo {
    #[serde(rename = "tickSize")]
    pub tick_size: f64,
    #[serde(rename = "stepSize")]
    pub step_size: f64,
    pub limit_min: f64,
    pub limit_max: f64,
    pub market_min: f64,
    pub market_max: f64,
    #[serde(rename = "onboardDate")]
    pub onboard_date: i64,
    #[serde(rename = "liquidationFee")]
    pub liquidation_fee: f64,
    #[serde(rename = "marketTakeBound")]
    pub market_take_bound: f64,
    pub status: String,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ExchangeInfoCacheSnapshot {
    fetched_at_ms: i64,
    items: HashMap<String, ContractExchangeInfo>,
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

#[derive(Debug, Deserialize)]
pub struct ExchangeInfoQuery {
    pub symbol: Option<String>,
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

#[derive(Clone, Debug, Deserialize)]
struct ExchangeInfoResp {
    symbols: Vec<ExchangeInfoSymbol>,
}

#[derive(Clone, Debug, Deserialize)]
struct ExchangeInfoSymbol {
    symbol: String,
    status: String,
    #[serde(rename = "contractType")]
    contract_type: String,
    filters: Vec<ExchangeInfoFilter>,
    #[serde(rename = "onboardDate")]
    onboard_date: Option<i64>,
    #[serde(rename = "liquidationFee")]
    liquidation_fee: Option<String>,
    #[serde(rename = "marketTakeBound")]
    market_take_bound: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
struct ExchangeInfoFilter {
    #[serde(rename = "filterType")]
    filter_type: String,
    #[serde(rename = "tickSize")]
    tick_size: Option<String>,
    #[serde(rename = "stepSize")]
    step_size: Option<String>,
    #[serde(rename = "minQty")]
    min_qty: Option<String>,
    #[serde(rename = "maxQty")]
    max_qty: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ContractDetailEnvelope {
    success: bool,
    #[serde(default)]
    data: Vec<ContractDetailData>,
}

#[derive(Debug, Deserialize)]
struct ContractDetailData {
    #[serde(default, deserialize_with = "deserialize_string_or_default")]
    symbol: String,
    #[serde(default, deserialize_with = "deserialize_string_or_default")]
    alias: String,
    #[serde(default, deserialize_with = "deserialize_string_or_default")]
    url: String,
    #[serde(default, deserialize_with = "deserialize_string_or_default")]
    website: String,
    #[serde(default, deserialize_with = "deserialize_string_or_default")]
    explorer_urls: String,
    #[serde(
        rename = "explorerUrls",
        default,
        deserialize_with = "deserialize_string_or_default"
    )]
    explorer_urls_alias: String,
    #[serde(default)]
    mc: Value,
    #[serde(rename = "marketCap", default)]
    market_cap: Value,
    #[serde(default)]
    rk: Value,
    #[serde(rename = "rank", default)]
    rank: Value,
    #[serde(default)]
    v: Value,
    #[serde(rename = "volume", default)]
    volume: Value,
    #[serde(rename = "dayChange", default)]
    day_change: Value,
    #[serde(default)]
    cs: Value,
    #[serde(rename = "circulatingSupply", default)]
    circulating_supply: Value,
    #[serde(default)]
    ms: Value,
    #[serde(rename = "maxSupply", default)]
    max_supply: Value,
    #[serde(default)]
    ts: Value,
    #[serde(rename = "totalSupply", default)]
    total_supply: Value,
    #[serde(default)]
    athpu: Value,
    #[serde(rename = "allTimeHighPriceUsd", default)]
    all_time_high_price_usd: Value,
    #[serde(default)]
    athd: Value,
    #[serde(rename = "allTimeHighDate", default)]
    all_time_high_date: Value,
    #[serde(default)]
    athfc: bool,
    #[serde(rename = "allTimeHighFromCmc", default)]
    all_time_high_from_cmc: bool,
    #[serde(default)]
    atlpu: Value,
    #[serde(rename = "allTimeLowPriceUsd", default)]
    all_time_low_price_usd: Value,
    #[serde(default)]
    ald: Value,
    #[serde(rename = "allTimeLowDate", default)]
    all_time_low_date: Value,
    #[serde(default)]
    atlfc: bool,
    #[serde(rename = "allTimeLowFromCmc", default)]
    all_time_low_from_cmc: bool,
    #[serde(
        rename = "symbolPair",
        default,
        deserialize_with = "deserialize_string_or_default"
    )]
    symbol_pair: String,
    #[serde(rename = "details", default)]
    details: Vec<ContractDetailLocalized>,
}

#[derive(Debug, Deserialize, Clone)]
struct ContractDetailLocalized {
    #[serde(
        rename = "language",
        default,
        deserialize_with = "deserialize_string_or_default"
    )]
    language: String,
    #[serde(
        rename = "description",
        default,
        deserialize_with = "deserialize_string_or_default"
    )]
    description: String,
}

pub struct MetaService {
    upstream_client: reqwest::Client,
    binance_rest_base: String,
    cache_dir: PathBuf,
    icons_dir: PathBuf,
    contracts_cache: RwLock<Option<ContractsCacheSnapshot>>,
    exchange_info_cache: RwLock<Option<ExchangeInfoCacheSnapshot>>,
    contracts_refresh_lock: Mutex<()>,
    exchange_info_refresh_lock: Mutex<()>,
}

impl MetaService {
    pub fn new(runtime_config: Option<&Config>) -> Result<Self> {
        let proxy_url = runtime_config
            .map(|config| config.binance.non_download_rest_proxy_url())
            .filter(|value| !value.trim().is_empty());
        let mut upstream_builder = reqwest::Client::builder().timeout(Duration::from_secs(15));
        if let Some(proxy_url) = proxy_url.as_deref() {
            upstream_builder =
                upstream_builder.proxy(reqwest::Proxy::all(proxy_url).map_err(AppError::Reqwest)?);
        }
        let upstream_client = upstream_builder.build().map_err(AppError::Reqwest)?;

        let runtime_dir = resolve_runtime_base_dir();
        let cache_dir = runtime_dir.join(CONTRACTS_CACHE_DIR);
        let icons_dir = cache_dir.join(ICONS_CACHE_DIR);

        Ok(Self {
            upstream_client,
            binance_rest_base: runtime_config
                .map(|config| {
                    config
                        .binance
                        .proxy_rest_base()
                        .trim_end_matches('/')
                        .to_string()
                })
                .filter(|value| !value.is_empty())
                .unwrap_or_else(|| DEFAULT_BINANCE_REST_BASE.to_string()),
            cache_dir,
            icons_dir,
            contracts_cache: RwLock::new(None),
            exchange_info_cache: RwLock::new(None),
            contracts_refresh_lock: Mutex::new(()),
            exchange_info_refresh_lock: Mutex::new(()),
        })
    }

    pub async fn warm_caches(&self) {
        if let Ok(Some(snapshot)) = self.read_contracts_snapshot_from_disk().await {
            self.contracts_cache.write().await.replace(snapshot);
        }
        if let Ok(Some(snapshot)) = self.read_exchange_info_snapshot_from_disk().await {
            self.exchange_info_cache.write().await.replace(snapshot);
        }

        if let Err(error) = self.refresh_exchange_info("startup").await {
            warn!("exchange-info startup refresh failed: {}", error);
        }
        if let Err(error) = self.refresh_contracts("startup").await {
            warn!("contracts startup refresh failed: {}", error);
        }
    }

    pub fn spawn_background_refresh(self: Arc<Self>) {
        tokio::spawn(async move {
            let mut interval = time::interval(Duration::from_secs(BACKGROUND_REFRESH_SECS));
            interval.tick().await;
            loop {
                interval.tick().await;
                if let Err(error) = self.refresh_exchange_info("interval").await {
                    warn!("exchange-info interval refresh failed: {}", error);
                }
                if let Err(error) = self.refresh_contracts("interval").await {
                    warn!("contracts interval refresh failed: {}", error);
                }
            }
        });
    }

    pub async fn get_contracts(&self) -> Result<Vec<LocalContractInfo>> {
        Ok(self.ensure_contracts_snapshot().await?.items)
    }

    pub async fn get_contract(&self, symbol: &str) -> Result<Option<LocalContractInfo>> {
        let normalized = normalize_symbol(symbol);
        if normalized.is_empty() {
            return Ok(None);
        }
        let snapshot = self.ensure_contracts_snapshot().await?;
        Ok(snapshot
            .items
            .into_iter()
            .find(|item| normalize_symbol(&item.symbol) == normalized))
    }

    pub async fn get_exchange_info_map(&self) -> Result<HashMap<String, ContractExchangeInfo>> {
        Ok(self.ensure_exchange_info_snapshot().await?.items)
    }

    pub async fn get_exchange_info_item(
        &self,
        symbol: &str,
    ) -> Result<Option<ContractExchangeInfo>> {
        let normalized = normalize_symbol(symbol);
        if normalized.is_empty() {
            return Ok(None);
        }
        let snapshot = self.ensure_exchange_info_snapshot().await?;
        Ok(snapshot.items.get(&normalized).cloned())
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
                let symbol = item.symbol.trim().to_string();
                let quote_volume = item.quote_volume.parse::<f64>().ok()?;
                let last_price = item.last_price.parse::<f64>().unwrap_or(0.0);
                let price_change_percent = item.price_change_percent.parse::<f64>().unwrap_or(0.0);

                if symbol.is_empty() || quote_volume <= 0.0 || !symbol.ends_with("USDT") {
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
        let candidate_limit =
            (requested_limit * TOP_TURNOVER_CANDIDATE_MULTIPLIER).min(candidates.len());
        let mut items = Vec::with_capacity(requested_limit);

        for candidate in candidates.into_iter().take(candidate_limit) {
            match self.has_recent_positive_turnover(&candidate.symbol).await {
                Ok(true) => items.push(candidate),
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

        let contracts = self.ensure_contracts_snapshot().await?.items;
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

    async fn ensure_contracts_snapshot(&self) -> Result<ContractsCacheSnapshot> {
        if let Some(snapshot) = self.read_memory_contracts_if_fresh().await {
            return Ok(snapshot);
        }

        if let Some(snapshot) = self.read_memory_contracts_any().await {
            return Ok(snapshot);
        }

        if let Some(snapshot) = self.read_contracts_snapshot_from_disk().await? {
            self.contracts_cache.write().await.replace(snapshot.clone());
            return Ok(snapshot);
        }

        self.refresh_contracts("request_cold_start").await
    }

    async fn ensure_exchange_info_snapshot(&self) -> Result<ExchangeInfoCacheSnapshot> {
        if let Some(snapshot) = self.read_memory_exchange_info_if_fresh().await {
            return Ok(snapshot);
        }

        if let Some(snapshot) = self.read_memory_exchange_info_any().await {
            return Ok(snapshot);
        }

        if let Some(snapshot) = self.read_exchange_info_snapshot_from_disk().await? {
            self.exchange_info_cache
                .write()
                .await
                .replace(snapshot.clone());
            return Ok(snapshot);
        }

        self.refresh_exchange_info("request_cold_start").await
    }

    async fn refresh_contracts(&self, reason: &str) -> Result<ContractsCacheSnapshot> {
        let _guard = self.contracts_refresh_lock.lock().await;

        if let Some(snapshot) = self.read_memory_contracts_if_fresh().await {
            return Ok(snapshot);
        }

        let snapshot = self.fetch_contracts_from_upstream().await?;
        self.store_contracts_snapshot(&snapshot).await?;
        info!(
            "contracts cache refreshed: reason={} symbols={}",
            reason,
            snapshot.items.len()
        );
        Ok(snapshot)
    }

    async fn refresh_exchange_info(&self, reason: &str) -> Result<ExchangeInfoCacheSnapshot> {
        let _guard = self.exchange_info_refresh_lock.lock().await;

        if let Some(snapshot) = self.read_memory_exchange_info_if_fresh().await {
            return Ok(snapshot);
        }

        let snapshot = self.fetch_exchange_info_from_upstream().await?;
        self.store_exchange_info_snapshot(&snapshot).await?;
        info!(
            "exchange-info cache refreshed: reason={} symbols={}",
            reason,
            snapshot.items.len()
        );
        Ok(snapshot)
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

    async fn read_memory_exchange_info_if_fresh(&self) -> Option<ExchangeInfoCacheSnapshot> {
        let guard = self.exchange_info_cache.read().await;
        let snapshot = guard.clone()?;
        if now_ms().saturating_sub(snapshot.fetched_at_ms) <= EXCHANGE_INFO_CACHE_TTL_MS {
            return Some(snapshot);
        }
        None
    }

    async fn read_memory_exchange_info_any(&self) -> Option<ExchangeInfoCacheSnapshot> {
        self.exchange_info_cache.read().await.clone()
    }

    async fn fetch_contracts_from_upstream(&self) -> Result<ContractsCacheSnapshot> {
        let exchange_info = self.fetch_exchange_info_response().await?;
        let active_contract_symbols = extract_trading_symbols(&exchange_info);
        let descriptions = self
            .fetch_json::<HashMap<String, String>>(BINANCE_DESCRIPTIONS_URL)
            .await
            .map(build_description_map)
            .unwrap_or_default();

        let items = stream::iter(active_contract_symbols.into_iter().map(|symbol| {
            let descriptions = descriptions.clone();
            async move {
                match self.fetch_contract_detail(&symbol, &descriptions).await {
                    Ok(item) => item,
                    Err(error) => {
                        warn!(
                            "fetch contract detail failed: symbol={} err={}",
                            symbol, error
                        );
                        build_placeholder_contract_info(&symbol, &descriptions)
                    }
                }
            }
        }))
        .buffer_unordered(CONTRACT_DETAIL_CONCURRENCY)
        .collect::<Vec<_>>()
        .await;

        let mut items = items;
        items.sort_by(|left, right| {
            let rank_cmp = left.rank.cmp(&right.rank);
            if rank_cmp == std::cmp::Ordering::Equal {
                left.symbol.cmp(&right.symbol)
            } else {
                rank_cmp
            }
        });

        Ok(ContractsCacheSnapshot {
            fetched_at_ms: now_ms(),
            items,
        })
    }

    async fn fetch_exchange_info_from_upstream(&self) -> Result<ExchangeInfoCacheSnapshot> {
        let payload = self.fetch_exchange_info_response().await?;
        Ok(ExchangeInfoCacheSnapshot {
            fetched_at_ms: now_ms(),
            items: parse_contract_exchange_info(payload),
        })
    }

    async fn fetch_exchange_info_response(&self) -> Result<ExchangeInfoResp> {
        let url = format!("{}/fapi/v1/exchangeInfo", self.binance_rest_base);
        self.fetch_json(&url).await
    }

    async fn fetch_contract_detail(
        &self,
        symbol: &str,
        descriptions: &HashMap<String, String>,
    ) -> Result<LocalContractInfo> {
        let normalized_symbol = normalize_contract_symbol(symbol);
        let query_symbol = normalize_contract_detail_query_symbol(symbol);
        let url = format!("{BINANCE_CONTRACT_DETAIL_URL}?symbol={query_symbol}");
        let envelope = self.fetch_json::<ContractDetailEnvelope>(&url).await?;

        if !envelope.success {
            return Err(AppError::ApiLogic(format!(
                "contract detail api returned success=false for {normalized_symbol}"
            )));
        }

        let data = select_contract_detail(&envelope.data, &normalized_symbol, &query_symbol)
            .ok_or_else(|| {
                AppError::ApiLogic(format!(
                    "contract detail api returned empty data for {normalized_symbol}"
                ))
            })?;

        let market_cap = parse_num(&data.market_cap).max(parse_num(&data.mc));
        let volume24h = parse_num(&data.volume).max(parse_num(&data.v));
        let rank = parse_i64(&data.rank).max(parse_i64(&data.rk));
        let circulating_supply = parse_num(&data.circulating_supply).max(parse_num(&data.cs));
        let max_supply = parse_num(&data.max_supply).max(parse_num(&data.ms));
        let total_supply = parse_num(&data.total_supply).max(parse_num(&data.ts));
        let highest_price = parse_num(&data.all_time_high_price_usd).max(parse_num(&data.athpu));
        let highest_price_date = parse_i64(&data.all_time_high_date).max(parse_i64(&data.athd));
        let lowest_price = parse_num(&data.all_time_low_price_usd).max(parse_num(&data.atlpu));
        let lowest_price_date = parse_i64(&data.all_time_low_date).max(parse_i64(&data.ald));
        let turnover_rate = if market_cap > 0.0 {
            volume24h / market_cap * 100.0
        } else {
            0.0
        };
        let description = descriptions
            .get(&normalize_contract_description_key(symbol))
            .cloned()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| pick_contract_description(&data.details));
        let explorer_url = first_non_empty_text(&[&data.explorer_urls_alias, &data.explorer_urls])
            .split(',')
            .next()
            .unwrap_or_default()
            .trim()
            .to_string();

        Ok(LocalContractInfo {
            name: first_non_empty_text(&[&data.symbol, &data.alias, &query_symbol]),
            symbol: normalized_symbol,
            description,
            image_url: data.url.clone(),
            website_url: data.website.clone(),
            explorer_url,
            current_price: 0.0,
            market_cap,
            rank,
            volume24h,
            price_change_percent: parse_num(&data.day_change),
            turnover_rate,
            circulating_supply,
            max_supply,
            total_supply,
            highest_price,
            highest_price_date,
            highest_price_confirmed: data.all_time_high_from_cmc || data.athfc,
            lowest_price,
            lowest_price_date,
            lowest_price_confirmed: data.all_time_low_from_cmc || data.atlfc,
        })
    }

    async fn fetch_json<T: DeserializeOwned>(&self, url: &str) -> Result<T> {
        self.upstream_client
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

    async fn store_contracts_snapshot(&self, snapshot: &ContractsCacheSnapshot) -> Result<()> {
        fs::create_dir_all(&self.cache_dir).await?;
        let body = serde_json::to_vec(snapshot)?;
        fs::write(self.contracts_cache_path(), body).await?;
        self.contracts_cache.write().await.replace(snapshot.clone());
        Ok(())
    }

    async fn store_exchange_info_snapshot(
        &self,
        snapshot: &ExchangeInfoCacheSnapshot,
    ) -> Result<()> {
        fs::create_dir_all(&self.cache_dir).await?;
        let body = serde_json::to_vec(snapshot)?;
        fs::write(self.exchange_info_cache_path(), body).await?;
        self.exchange_info_cache
            .write()
            .await
            .replace(snapshot.clone());
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

    async fn read_exchange_info_snapshot_from_disk(
        &self,
    ) -> Result<Option<ExchangeInfoCacheSnapshot>> {
        let path = self.exchange_info_cache_path();
        if !fs::try_exists(&path).await? {
            return Ok(None);
        }
        let body = fs::read(&path).await?;
        let snapshot = serde_json::from_slice::<ExchangeInfoCacheSnapshot>(&body)?;
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

    fn exchange_info_cache_path(&self) -> PathBuf {
        self.cache_dir.join(EXCHANGE_INFO_CACHE_FILE)
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
        let rows = self.fetch_json::<Vec<Vec<Value>>>(&url).await?;
        Ok(rows.iter().any(|row| has_positive_quote_turnover(row)))
    }
}

fn deserialize_string_or_default<'de, D>(deserializer: D) -> std::result::Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    Ok(Option::<String>::deserialize(deserializer)?.unwrap_or_default())
}

fn build_description_map(raw: HashMap<String, String>) -> HashMap<String, String> {
    let mut descriptions = HashMap::new();
    for (key, value) in raw {
        if !key.starts_with("symbol_desc_") {
            continue;
        }
        let symbol = key
            .trim_start_matches("symbol_desc_")
            .trim()
            .to_ascii_uppercase();
        if symbol.is_empty() {
            continue;
        }
        descriptions.entry(symbol).or_insert(value);
    }
    descriptions
}

fn extract_trading_symbols(parsed: &ExchangeInfoResp) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut symbols = Vec::new();

    for row in &parsed.symbols {
        if !row.symbol.ends_with("USDT") {
            continue;
        }

        if row.status != "TRADING" {
            continue;
        }

        if row.contract_type != "PERPETUAL" && row.contract_type != "TRADIFI_PERPETUAL" {
            continue;
        }

        let normalized = normalize_contract_symbol(&row.symbol);
        if seen.insert(normalized.clone()) {
            symbols.push(normalized);
        }
    }

    symbols.sort_unstable();
    symbols
}

fn parse_contract_exchange_info(parsed: ExchangeInfoResp) -> HashMap<String, ContractExchangeInfo> {
    let mut map = HashMap::with_capacity(parsed.symbols.len());

    for row in parsed.symbols {
        if !row.symbol.ends_with("USDT") {
            continue;
        }

        if row.status != "TRADING" {
            continue;
        }

        if row.contract_type != "PERPETUAL" && row.contract_type != "TRADIFI_PERPETUAL" {
            continue;
        }

        let Some(price_filter) = row
            .filters
            .iter()
            .find(|item| item.filter_type == "PRICE_FILTER")
        else {
            continue;
        };
        let Some(lot_filter) = row
            .filters
            .iter()
            .find(|item| item.filter_type == "LOT_SIZE")
        else {
            continue;
        };
        let Some(market_lot_filter) = row
            .filters
            .iter()
            .find(|item| item.filter_type == "MARKET_LOT_SIZE")
            .or(Some(lot_filter))
        else {
            continue;
        };

        let limit_max = parse_optional_num(lot_filter.max_qty.as_deref()).unwrap_or(0.0);
        let market_max = parse_optional_num(market_lot_filter.max_qty.as_deref())
            .unwrap_or(limit_max)
            .min(limit_max);

        map.insert(
            normalize_symbol(&row.symbol),
            ContractExchangeInfo {
                tick_size: parse_optional_num(price_filter.tick_size.as_deref()).unwrap_or(0.0),
                step_size: parse_optional_num(lot_filter.step_size.as_deref()).unwrap_or(0.0),
                limit_min: parse_optional_num(lot_filter.min_qty.as_deref()).unwrap_or(0.0),
                limit_max,
                market_min: parse_optional_num(market_lot_filter.min_qty.as_deref()).unwrap_or(0.0),
                market_max,
                onboard_date: row.onboard_date.unwrap_or(0),
                liquidation_fee: parse_optional_num(row.liquidation_fee.as_deref()).unwrap_or(0.0),
                market_take_bound: parse_optional_num(row.market_take_bound.as_deref())
                    .unwrap_or(0.0),
                status: row.status,
            },
        );
    }

    map
}

fn normalize_symbol(symbol: &str) -> String {
    symbol
        .trim()
        .chars()
        .filter(|char| char.is_ascii_alphanumeric() || *char == '_' || *char == '-')
        .collect::<String>()
        .to_ascii_uppercase()
}

fn normalize_contract_symbol(symbol: &str) -> String {
    symbol.trim().to_ascii_uppercase()
}

fn normalize_contract_description_key(symbol: &str) -> String {
    normalize_contract_symbol(symbol)
        .trim_end_matches("USDT")
        .trim_start_matches(|char: char| char.is_ascii_digit())
        .to_string()
}

fn normalize_contract_detail_query_symbol(symbol: &str) -> String {
    normalize_contract_description_key(symbol)
}

fn parse_num(value: &Value) -> f64 {
    match value {
        Value::Number(v) => v.as_f64().unwrap_or(0.0),
        Value::String(v) => v.trim().parse::<f64>().unwrap_or(0.0),
        _ => 0.0,
    }
}

fn parse_i64(value: &Value) -> i64 {
    match value {
        Value::Number(v) => v.as_i64().unwrap_or(0),
        Value::String(v) => v.trim().parse::<i64>().unwrap_or(0),
        _ => 0,
    }
}

fn parse_optional_num(value: Option<&str>) -> Option<f64> {
    value.and_then(|text| text.trim().parse::<f64>().ok())
}

fn first_non_empty_text(values: &[&str]) -> String {
    values
        .iter()
        .map(|value| value.trim())
        .find(|value| !value.is_empty())
        .unwrap_or_default()
        .to_string()
}

fn pick_contract_description(details: &[ContractDetailLocalized]) -> String {
    let preferred = ["EN", "CN", "ZH", ""];
    for language in preferred {
        if let Some(detail) = details.iter().find(|detail| {
            let current = detail.language.trim().to_ascii_uppercase();
            !detail.description.trim().is_empty() && (language.is_empty() || current == language)
        }) {
            return detail.description.trim().to_string();
        }
    }

    details
        .iter()
        .find_map(|detail| {
            let description = detail.description.trim();
            (!description.is_empty()).then(|| description.to_string())
        })
        .unwrap_or_default()
}

fn select_contract_detail<'a>(
    items: &'a [ContractDetailData],
    trading_symbol: &str,
    query_symbol: &str,
) -> Option<&'a ContractDetailData> {
    items
        .iter()
        .find(|item| item.symbol_pair.eq_ignore_ascii_case(trading_symbol))
        .or_else(|| {
            let expected_pair = format!("{query_symbol}USDT");
            items
                .iter()
                .find(|item| item.symbol_pair.eq_ignore_ascii_case(&expected_pair))
        })
        .or_else(|| {
            items
                .iter()
                .find(|item| item.alias.eq_ignore_ascii_case(query_symbol))
        })
        .or_else(|| items.first())
}

fn build_placeholder_contract_info(
    symbol: &str,
    descriptions: &HashMap<String, String>,
) -> LocalContractInfo {
    let normalized_symbol = normalize_contract_symbol(symbol);
    let query_symbol = normalize_contract_detail_query_symbol(symbol);
    let description = descriptions
        .get(&normalize_contract_description_key(symbol))
        .cloned()
        .unwrap_or_default();

    LocalContractInfo {
        name: query_symbol.clone(),
        symbol: normalized_symbol,
        description,
        image_url: String::new(),
        website_url: String::new(),
        explorer_url: String::new(),
        current_price: 0.0,
        market_cap: 0.0,
        rank: UNKNOWN_RANK,
        volume24h: 0.0,
        price_change_percent: 0.0,
        turnover_rate: 0.0,
        circulating_supply: 0.0,
        max_supply: 0.0,
        total_supply: 0.0,
        highest_price: 0.0,
        highest_price_date: 0,
        highest_price_confirmed: false,
        lowest_price: 0.0,
        lowest_price_date: 0,
        lowest_price_confirmed: false,
    }
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

pub async fn contract_handler(
    State(service): State<Arc<MetaService>>,
    Path(symbol): Path<String>,
) -> impl IntoResponse {
    let normalized = normalize_symbol(&symbol);
    match service.get_contract(&symbol).await {
        Ok(item) => Json(serde_json::json!({
            "symbol": normalized,
            "data": item,
        }))
        .into_response(),
        Err(error) => error.into_response(),
    }
}

pub async fn exchange_info_handler(
    State(service): State<Arc<MetaService>>,
    Query(query): Query<ExchangeInfoQuery>,
) -> impl IntoResponse {
    match query.symbol {
        Some(symbol) => match service.get_exchange_info_item(&symbol).await {
            Ok(item) => Json(serde_json::json!({
                "symbol": normalize_symbol(&symbol),
                "data": item,
            }))
            .into_response(),
            Err(error) => error.into_response(),
        },
        None => match service.get_exchange_info_map().await {
            Ok(items) => Json(items).into_response(),
            Err(error) => error.into_response(),
        },
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
