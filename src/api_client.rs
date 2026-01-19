// src/api_client.rs
use crate::error::{AppError, Result};
use crate::models::{DownloadTask, Kline};
use reqwest::header::{HeaderMap, HeaderValue, USER_AGENT};
use reqwest::Client;
use serde_json::Value;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use tokio::time::sleep;
use tracing::{info, trace, warn, instrument}; // 添加 instrument

// Constants
const MOKEX_BASE_URL: &str = "https://www.mokexapp.info";
const BINANCE_BASE_URL: &str = "https://fapi.binance.com";
const PROXY_URL: &str = "http://127.0.0.1:1080";
const FALLBACK_RETRIES: u32 = 10;
const RETRY_DELAY_MS: u64 = 10;

#[derive(Clone)]
pub struct ApiClient {
    mokex_client: Arc<Client>,
    binance_client: Arc<Client>,
}

impl ApiClient {
    pub fn new() -> Result<Self> {
        let mut mokex_headers = HeaderMap::new();
        mokex_headers.insert(
            USER_AGENT,
            HeaderValue::from_static("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Binance/1.54.19 Chrome/128.0.6613.186 Electron/32.3.0 Safari/537.36 (electron 1.54.19)")
        );
        mokex_headers.insert(
            "mclient-x-tag",
            HeaderValue::from_static("tfph2mpTPAuwxbiMHoQc"),
        );

        let mokex_client = Client::builder()
            .default_headers(mokex_headers)
            .timeout(Duration::from_secs(10)) // 添加超时，防止网络层永久卡死
            .build()
            .map_err(AppError::Reqwest)?;

        let proxy = reqwest::Proxy::all(PROXY_URL).map_err(AppError::Reqwest)?;
        let binance_client = Client::builder()
            .proxy(proxy)
            .timeout(Duration::from_secs(10)) // 添加超时
            .build()
            .map_err(AppError::Reqwest)?;

        Ok(Self {
            mokex_client: Arc::new(mokex_client),
            binance_client: Arc::new(binance_client),
        })
    }

    /// 使用 fallback 和 retry 逻辑下载K线
    #[instrument(skip(self))]
    pub async fn download_continuous_klines(&self, task: &DownloadTask) -> Result<Vec<Kline>> {
        let start_time = Instant::now();

        // 1. 首先尝试 Mokex
        let mokex_result = self
            .fetch_klines(&self.mokex_client, MOKEX_BASE_URL, task)
            .await;

        match mokex_result {
            Ok(klines) => {
                Ok(klines)
            }
            Err(e) => {
                warn!(
                    "⚠️ [DEBUG_API] Mokex 失败: {}. 切换到 Binance 重试.",
                    e
                );
                
                let mut last_error: Option<AppError> = None;

                for attempt in 1..=FALLBACK_RETRIES {

                    match self
                        .fetch_klines(&self.binance_client, BINANCE_BASE_URL, task)
                        .await
                    {
                        Ok(klines) => {
                            return Ok(klines);
                        }
                        Err(retry_err) => {
                            warn!(
                                "❌ [DEBUG_API] Binance 尝试 {} 失败: {}",
                                attempt, retry_err
                            );
                            last_error = Some(retry_err);

                            if attempt < FALLBACK_RETRIES {
                                sleep(Duration::from_millis(RETRY_DELAY_MS)).await;
                            }
                        }
                    }
                }

                warn!(
                    "⛔ [DEBUG_API] 所有重试均失败: {}/{}",
                     task.symbol, task.interval
                );
                Err(last_error.unwrap())
            }
        }
    }

    /// 实际执行API请求的私有方法
    async fn fetch_klines(
        &self,
        client: &Client,
        base_url: &str,
        task: &DownloadTask,
    ) -> Result<Vec<Kline>> {
        // --- 疑问与探讨点 ---
        // 这里手动拼接 URL 字符串，如果 symbol 包含特殊字符（除了中文，还有像 &、= 等），
        // 可能会导致 URL 解析错误。一个更健壮的做法是使用 reqwest 的查询参数构建器，
        // 它会自动处理 URL 编码。例如：
        // client.get(url)
        //       .query(&[("pair", &task.symbol), ("interval", &task.interval), ...])
        // 这样做会让代码更安全，不过当前 `format!` 的方式也能工作，因为 reqwest 内部会编码整个 URL。
        let mut url_params = format!(
            "pair={}&contractType=PERPETUAL&interval={}&limit={}",
            task.symbol, task.interval, task.limit
        );
        if let Some(start_time) = task.start_time {
            url_params.push_str(&format!("&startTime={}", start_time));
        }
        if let Some(end_time) = task.end_time {
            url_params.push_str(&format!("&endTime={}", end_time));
        }

        let url = format!("{}/fapi/v1/continuousKlines?{}", base_url, url_params);

        // --- 【核心修复】 ---
        // 移除了不安全的字符串切片 `&url[..60]`，直接打印完整的 URL。
        let response = client.get(&url).send().await?.error_for_status()?;
        let response_text = response.text().await?;

        let raw_klines: Vec<Vec<Value>> = serde_json::from_str(&response_text)?;

        if raw_klines.is_empty() {
            trace!("API returned empty result for task: {:?}", task);
            return Ok(vec![]);
        }

        let klines = raw_klines
            .iter()
            .filter_map(|raw_kline_vec| Kline::from_raw_kline(raw_kline_vec))
            .collect::<Vec<Kline>>();

        Ok(klines)
    }
}