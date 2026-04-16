use crate::config::BinanceConfig;
use crate::error::{AppError, Result};
use crate::models::{DownloadTask, Kline};
use reqwest::header::{HeaderMap, HeaderValue, USER_AGENT};
use reqwest::Client;
use serde_json::Value;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{debug, info, instrument, trace, warn};

const BINANCE_BASE_URL: &str = "https://fapi.binance.com";
const DEFAULT_DOWNLOAD_PROXY_URL: &str = "http://127.0.0.1:17892";
const DEFAULT_NON_DOWNLOAD_PROXY_URL: &str = "socks5h://127.0.0.1:1080";
const FALLBACK_RETRIES: u32 = 10;
const RETRY_DELAY_MS: u64 = 10;
const BINANCE_MAX_KLINE_LIMIT: usize = 1000;

static TIME_OFFSET: AtomicI64 = AtomicI64::new(0);

#[derive(Clone)]
pub struct ApiClient {
    download_client: Arc<Client>,
    rest_client: Arc<Client>,
}

impl ApiClient {
    fn build_clients(
        download_proxy_url: &str,
        non_download_proxy_url: &str,
    ) -> Result<(Client, Client)> {
        let mut download_headers = HeaderMap::new();
        download_headers.insert(
            USER_AGENT,
            HeaderValue::from_static(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Binance/1.54.19 Chrome/128.0.6613.186 Electron/32.3.0 Safari/537.36 (electron 1.54.19)",
            ),
        );
        download_headers.insert(
            "mclient-x-tag",
            HeaderValue::from_static("tfph2mpTPAuwxbiMHoQc"),
        );

        let download_client = Client::builder()
            .default_headers(download_headers)
            .proxy(reqwest::Proxy::all(download_proxy_url).map_err(AppError::Reqwest)?)
            .timeout(Duration::from_secs(10))
            .build()
            .map_err(AppError::Reqwest)?;

        let rest_client = Client::builder()
            .proxy(reqwest::Proxy::all(non_download_proxy_url).map_err(AppError::Reqwest)?)
            .timeout(Duration::from_secs(10))
            .build()
            .map_err(AppError::Reqwest)?;

        Ok((download_client, rest_client))
    }

    fn new_internal(download_proxy_url: &str, non_download_proxy_url: &str) -> Result<Self> {
        let (download_client, rest_client) =
            Self::build_clients(download_proxy_url, non_download_proxy_url)?;

        Ok(Self {
            download_client: Arc::new(download_client),
            rest_client: Arc::new(rest_client),
        })
    }

    pub fn new() -> Result<Self> {
        Self::new_internal(DEFAULT_DOWNLOAD_PROXY_URL, DEFAULT_NON_DOWNLOAD_PROXY_URL)
    }

    pub fn new_public_with_config(config: &BinanceConfig) -> Result<Self> {
        let non_download_proxy_url = config.non_download_rest_proxy_url();
        Self::new_internal(config.download_proxy_url(), &non_download_proxy_url)
    }

    #[instrument(skip(self))]
    pub async fn download_continuous_klines(&self, task: &DownloadTask) -> Result<Vec<Kline>> {
        let interval_ms = interval_to_milliseconds(&task.interval)?;
        let request_limit = task.limit.max(1);

        if request_limit <= BINANCE_MAX_KLINE_LIMIT {
            return self.download_klines_once(task).await;
        }

        if task.start_time.is_some() {
            self.download_klines_forward_chunked(task, interval_ms).await
        } else {
            self.download_klines_backward_chunked(task, interval_ms).await
        }
    }

    async fn download_klines_once(&self, task: &DownloadTask) -> Result<Vec<Kline>> {
        let mut last_error: Option<AppError> = None;

        for attempt in 1..=FALLBACK_RETRIES {
            match self
                .fetch_klines(&self.download_client, BINANCE_BASE_URL, task)
                .await
            {
                Ok(klines) => return Ok(klines),
                Err(err) => {
                    last_error = Some(err);
                    if attempt < FALLBACK_RETRIES {
                        sleep(Duration::from_millis(RETRY_DELAY_MS)).await;
                    }
                }
            }
        }

        Err(last_error.unwrap_or_else(|| {
            AppError::ApiLogic(format!(
                "failed to download klines for symbol={} interval={}",
                task.symbol, task.interval
            ))
        }))
    }

    async fn download_klines_forward_chunked(
        &self,
        task: &DownloadTask,
        interval_ms: i64,
    ) -> Result<Vec<Kline>> {
        let mut collected: Vec<Kline> = Vec::new();
        let mut next_start_time = task.start_time;
        let end_time = task.end_time;

        while collected.len() < task.limit {
            let remaining = task.limit - collected.len();
            let chunk_limit = remaining.min(BINANCE_MAX_KLINE_LIMIT);
            let chunk_task = DownloadTask {
                symbol: task.symbol.clone(),
                interval: task.interval.clone(),
                start_time: next_start_time,
                end_time,
                limit: chunk_limit,
            };
            let mut chunk = self.download_klines_once(&chunk_task).await?;

            if chunk.is_empty() {
                break;
            }

            if let Some(last_collected) = collected.last() {
                if let Some(first_chunk) = chunk.first() {
                    if first_chunk.open_time == last_collected.open_time {
                        chunk.remove(0);
                    }
                }
            }

            if chunk.is_empty() {
                break;
            }

            next_start_time = chunk
                .last()
                .map(|kline| kline.open_time.saturating_add(interval_ms));
            let fetched_len = chunk.len();
            collected.extend(chunk);

            if fetched_len < chunk_limit {
                break;
            }

            if let (Some(next_start), Some(chunk_end)) = (next_start_time, end_time) {
                if next_start > chunk_end {
                    break;
                }
            }
        }

        if collected.len() > task.limit {
            let overflow = collected.len() - task.limit;
            collected.drain(..overflow);
        }

        Ok(collected)
    }

    async fn download_klines_backward_chunked(
        &self,
        task: &DownloadTask,
        interval_ms: i64,
    ) -> Result<Vec<Kline>> {
        let mut collected: Vec<Kline> = Vec::new();
        let mut next_end_time = task.end_time;

        while collected.len() < task.limit {
            let remaining = task.limit - collected.len();
            let chunk_limit = remaining.min(BINANCE_MAX_KLINE_LIMIT);
            let chunk_task = DownloadTask {
                symbol: task.symbol.clone(),
                interval: task.interval.clone(),
                start_time: None,
                end_time: next_end_time,
                limit: chunk_limit,
            };
            let mut chunk = self.download_klines_once(&chunk_task).await?;

            if chunk.is_empty() {
                break;
            }

            if let Some(first_existing) = collected.first() {
                if let Some(last_chunk) = chunk.last() {
                    if last_chunk.open_time == first_existing.open_time {
                        chunk.pop();
                    }
                }
            }

            if chunk.is_empty() {
                break;
            }

            let fetched_len = chunk.len();
            next_end_time = chunk
                .first()
                .map(|kline| kline.open_time.saturating_sub(interval_ms));
            chunk.extend(collected);
            collected = chunk;

            if fetched_len < chunk_limit {
                break;
            }
        }

        if collected.len() > task.limit {
            let overflow = collected.len() - task.limit;
            collected.drain(..overflow);
        }

        Ok(collected)
    }

    async fn fetch_klines(
        &self,
        client: &Client,
        base_url: &str,
        task: &DownloadTask,
    ) -> Result<Vec<Kline>> {
        let mut attempts = Vec::with_capacity(3);
        attempts.push((
            "symbol_klines",
            self.fetch_symbol_klines(client, base_url, task).await,
        ));
        attempts.push((
            "continuous_perpetual",
            self.fetch_continuous_klines(client, base_url, task, "PERPETUAL")
                .await,
        ));
        attempts.push((
            "continuous_tradifi",
            self.fetch_continuous_klines(client, base_url, task, "TRADIFI_PERPETUAL")
                .await,
        ));

        let mut last_error = None;
        for (strategy, result) in attempts {
            match result {
                Ok(klines) => {
                    debug!(
                        "kline fetch succeeded via {} symbol={} interval={}",
                        strategy, task.symbol, task.interval
                    );
                    return Ok(klines);
                }
                Err(err) => {
                    debug!(
                        "kline fetch failed via {} symbol={} interval={} err={}",
                        strategy, task.symbol, task.interval, err
                    );
                    last_error = Some(err);
                }
            }
        }

        Err(last_error.unwrap_or_else(|| {
            AppError::ApiLogic(format!(
                "no kline strategy succeeded for symbol={} interval={}",
                task.symbol, task.interval
            ))
        }))
    }

    async fn fetch_symbol_klines(
        &self,
        client: &Client,
        base_url: &str,
        task: &DownloadTask,
    ) -> Result<Vec<Kline>> {
        let mut url_params = format!(
            "symbol={}&interval={}&limit={}",
            task.symbol, task.interval, task.limit
        );
        if let Some(start_time) = task.start_time {
            url_params.push_str(&format!("&startTime={}", start_time));
        }
        if let Some(end_time) = task.end_time {
            url_params.push_str(&format!("&endTime={}", end_time));
        }

        let url = format!("{}/fapi/v1/klines?{}", base_url, url_params);
        self.fetch_klines_from_url(client, &url, task).await
    }

    async fn fetch_continuous_klines(
        &self,
        client: &Client,
        base_url: &str,
        task: &DownloadTask,
        contract_type: &str,
    ) -> Result<Vec<Kline>> {
        let mut url_params = format!(
            "pair={}&contractType={}&interval={}&limit={}",
            task.symbol, contract_type, task.interval, task.limit
        );
        if let Some(start_time) = task.start_time {
            url_params.push_str(&format!("&startTime={}", start_time));
        }
        if let Some(end_time) = task.end_time {
            url_params.push_str(&format!("&endTime={}", end_time));
        }

        let url = format!("{}/fapi/v1/continuousKlines?{}", base_url, url_params);
        self.fetch_klines_from_url(client, &url, task).await
    }

    async fn fetch_klines_from_url(
        &self,
        client: &Client,
        url: &str,
        task: &DownloadTask,
    ) -> Result<Vec<Kline>> {
        let response = client.get(url).send().await?.error_for_status()?;
        let response_text = response.text().await?;
        let raw_klines: Vec<Vec<Value>> = serde_json::from_str(&response_text)?;

        if raw_klines.is_empty() {
            trace!(
                "API returned empty result for task: {:?}, url={}",
                task,
                url
            );
            return Ok(vec![]);
        }

        Ok(raw_klines
            .iter()
            .filter_map(|raw_kline_vec| Kline::from_raw_kline(raw_kline_vec))
            .collect())
    }

    pub async fn sync_server_time(&self) -> Result<()> {
        debug!("syncing server time through proxy");
        let url = format!("{}/fapi/v1/time", BINANCE_BASE_URL);
        let val: Value = self
            .rest_client
            .get(&url)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;

        let server_time = val["serverTime"]
            .as_i64()
            .ok_or_else(|| AppError::ApiLogic("missing serverTime".to_string()))?;
        let local_time = chrono::Utc::now().timestamp_millis();
        let offset = server_time - local_time;
        TIME_OFFSET.store(offset, Ordering::Relaxed);
        info!("server time synced, offset={}ms", offset);
        Ok(())
    }

    pub fn spawn_sync_loop(self: Arc<Self>) {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(3600));
            loop {
                interval.tick().await;
                if let Err(e) = self.sync_server_time().await {
                    warn!("failed to sync server time: {}", e);
                }
            }
        });
    }
}

fn interval_to_milliseconds(interval: &str) -> Result<i64> {
    let unit = interval
        .chars()
        .last()
        .ok_or_else(|| AppError::ApiLogic(format!("invalid interval: {}", interval)))?;
    let value = interval[..interval.len().saturating_sub(1)]
        .parse::<i64>()
        .map_err(|_| AppError::ApiLogic(format!("invalid interval value: {}", interval)))?;

    let millis = match unit {
        's' => value * 1_000,
        'm' => value * 60 * 1_000,
        'h' => value * 60 * 60 * 1_000,
        'd' => value * 24 * 60 * 60 * 1_000,
        'w' => value * 7 * 24 * 60 * 60 * 1_000,
        _ => {
            return Err(AppError::ApiLogic(format!(
                "unsupported interval unit: {}",
                interval
            )))
        }
    };

    Ok(millis)
}
