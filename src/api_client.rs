// src/api_client.rs
use crate::config::BinanceConfig;
use crate::error::{AppError, Result};
use crate::models::{DownloadTask, Kline};
use reqwest::header::{HeaderMap, HeaderValue, USER_AGENT};
use reqwest::Client;
use serde::Deserialize;
use serde_json::Value;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use tokio::time::sleep;
use tracing::{debug, info, instrument, trace, warn};

// Constants
const MOKEX_BASE_URL: &str = "https://www.mokexapp.org";
const BINANCE_BASE_URL: &str = "https://fapi.binance.com";
const PROXY_URL: &str = "http://127.0.0.1:1080";
const FALLBACK_RETRIES: u32 = 10;
const RETRY_DELAY_MS: u64 = 10;

/// 全局时间偏移量（毫秒）：server_time - local_time
static TIME_OFFSET: AtomicI64 = AtomicI64::new(0);

/// 获取与币安服务器同步后的当前毫秒时间戳
pub fn get_synced_timestamp() -> i64 {
    chrono::Utc::now().timestamp_millis() + TIME_OFFSET.load(Ordering::Relaxed)
}

/// listenKey 响应结构
#[derive(Debug, Deserialize)]
pub struct ListenKeyResponse {
    #[serde(rename = "listenKey")]
    pub listen_key: String,
}

#[derive(Clone)]
pub struct ApiClient {
    mokex_client: Arc<Client>,
    binance_client: Arc<Client>,
    /// 币安配置（可选，用于私有 API）
    binance_config: Option<Arc<BinanceConfig>>,
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
            .timeout(Duration::from_secs(10))
            .build()
            .map_err(AppError::Reqwest)?;

        let proxy = reqwest::Proxy::all(PROXY_URL).map_err(AppError::Reqwest)?;
        let binance_client = Client::builder()
            .proxy(proxy)
            .timeout(Duration::from_secs(10))
            .build()
            .map_err(AppError::Reqwest)?;

        Ok(Self {
            mokex_client: Arc::new(mokex_client),
            binance_client: Arc::new(binance_client),
            binance_config: None,
        })
    }

    /// 创建带 API Key 配置的客户端
    pub fn new_with_config(config: BinanceConfig) -> Result<Self> {
        let mut client = Self::new()?;
        client.binance_config = Some(Arc::new(config));
        Ok(client)
    }

    /// 获取配置
    pub fn config(&self) -> Option<&BinanceConfig> {
        self.binance_config.as_ref().map(|c| c.as_ref())
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
                            last_error = Some(retry_err);

                            if attempt < FALLBACK_RETRIES {
                                sleep(Duration::from_millis(RETRY_DELAY_MS)).await;
                            }
                        }
                    }
                }

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

    // ========== listenKey API ==========

    /// 同步币安服务器时间，更新全局偏移量
    pub async fn sync_server_time(&self) -> Result<()> {
        debug!("🕒 正在同步币安服务器时间...");
        let url = format!("{}/fapi/v1/time", MOKEX_BASE_URL);
        
        // 1. 尝试通过 Mokex (直连)
        let resp = self.mokex_client.get(&url).send().await;
        let server_time = match resp {
            Ok(r) if r.status().is_success() => {
                let val: Value = r.json().await?;
                val["serverTime"].as_i64()
            }
            _ => {
                // 2. 尝试通过 Binance (代理)
                let url = format!("{}/fapi/v1/time", BINANCE_BASE_URL);
                let r = self.binance_client.get(&url).send().await?.error_for_status()?;
                let val: Value = r.json().await?;
                val["serverTime"].as_i64()
            }
        };

        if let Some(st) = server_time {
            let local_time = chrono::Utc::now().timestamp_millis();
            let offset = st - local_time;
            TIME_OFFSET.store(offset, Ordering::Relaxed);
            info!("✅ 已建立全局时间标准，当前偏移量: {}ms (同步自币安服务器)", offset);
            Ok(())
        } else {
            Err(AppError::ApiLogic("解析服务器时间失败".to_string()))
        }
    }

    /// 开启定时同步任务，每小时执行一次
    pub fn spawn_sync_loop(self: Arc<Self>) {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(3600));
            loop {
                interval.tick().await;
                if let Err(e) = self.sync_server_time().await {
                    warn!("❌ 定时同步服务器时间失败: {}", e);
                }
            }
        });
    }

    // ========== listenKey API ==========

    /// 创建 listenKey
    pub async fn post_listen_key(&self) -> Result<String> {
        let config = self.binance_config.as_ref()
            .ok_or_else(|| AppError::Config("API Key 未配置".to_string()))?;
        
        info!("📡 正在获取 listenKey...");
        
        // 构建签名参数
        let timestamp = get_synced_timestamp();
        let query = format!("timestamp={}&recvWindow=60000", timestamp);
        let signature = config.sign(&query);
        let full_query = format!("{}&signature={}", query, signature);
        
        // 首先尝试直连
        let url = format!("{}/fapi/v1/listenKey?{}", config.direct_rest_base, full_query);
        debug!("listenKey URL: {}", url);
        
        let response = self.mokex_client
            .post(&url)
            .header("X-MBX-APIKEY", &config.api_key)
            .send()
            .await;
        
        match response {
            Ok(resp) if resp.status().is_success() => {
                let data: ListenKeyResponse = resp.json().await?;
                info!("✅ listenKey 获取成功: {}...", &data.listen_key[..16.min(data.listen_key.len())]);
                return Ok(data.listen_key);
            }
            Ok(resp) => {
                let status = resp.status();
                let body = resp.text().await.unwrap_or_default();
                warn!("直连获取 listenKey 失败: {} - {}", status, body);
            }
            Err(e) => {
                warn!("直连获取 listenKey 失败: {}", e);
            }
        }
        
        // 回退到代理
        info!("🔄 尝试通过代理获取 listenKey...");
        let url = format!("{}/fapi/v1/listenKey?{}", config.proxy_rest_base, full_query);
        
        let response = self.binance_client
            .post(&url)
            .header("X-MBX-APIKEY", &config.api_key)
            .send()
            .await?
            .error_for_status()?;
        
        let data: ListenKeyResponse = response.json().await?;
        info!("✅ listenKey 通过代理获取成功: {}...", &data.listen_key[..16.min(data.listen_key.len())]);
        Ok(data.listen_key)
    }

    /// 续期 listenKey
    pub async fn put_listen_key(&self) -> Result<()> {
        let config = self.binance_config.as_ref()
            .ok_or_else(|| AppError::Config("API Key 未配置".to_string()))?;
        
        debug!("🔄 正在续期 listenKey...");
        
        let timestamp = get_synced_timestamp();
        let query = format!("timestamp={}&recvWindow=60000", timestamp);
        let signature = config.sign(&query);
        let full_query = format!("{}&signature={}", query, signature);
        
        // 首先尝试直连
        let url = format!("{}/fapi/v1/listenKey?{}", config.direct_rest_base, full_query);
        
        let response = self.mokex_client
            .put(&url)
            .header("X-MBX-APIKEY", &config.api_key)
            .send()
            .await;
        
        match response {
            Ok(resp) if resp.status().is_success() => {
                info!("✅ listenKey 续期成功");
                return Ok(());
            }
            Ok(resp) => {
                let status = resp.status();
                warn!("直连续期 listenKey 失败: {}", status);
            }
            Err(e) => {
                warn!("直连续期 listenKey 失败: {}", e);
            }
        }
        
        // 回退到代理
        let url = format!("{}/fapi/v1/listenKey?{}", config.proxy_rest_base, full_query);
        
        self.binance_client
            .put(&url)
            .header("X-MBX-APIKEY", &config.api_key)
            .send()
            .await?
            .error_for_status()?;
        
        info!("✅ listenKey 通过代理续期成功");
        Ok(())
    }

    /// 删除 listenKey
    pub async fn delete_listen_key(&self) -> Result<()> {
        let config = self.binance_config.as_ref()
            .ok_or_else(|| AppError::Config("API Key 未配置".to_string()))?;
        
        debug!("🗑️ 正在删除 listenKey...");
        
        let timestamp = get_synced_timestamp();
        let query = format!("timestamp={}&recvWindow=60000", timestamp);
        let signature = config.sign(&query);
        let full_query = format!("{}&signature={}", query, signature);
        
        let url = format!("{}/fapi/v1/listenKey?{}", config.direct_rest_base, full_query);
        
        let _ = self.mokex_client
            .delete(&url)
            .header("X-MBX-APIKEY", &config.api_key)
            .send()
            .await;
        
        info!("🗑️ listenKey 已删除");
        Ok(())
    }

    /// 转发账号请求 (fapi/v2/account)
    pub async fn forward_account_request(&self, query: &str, headers: HeaderMap) -> Result<String> {
        info!("▶️ 开始处理账号信息请求转发");
        
        // 1. 尝试直连 (Mokex)
        // 注意：这里我们使用传进来的 query，因为它已经包含了 signature
        let url = format!("{}/fapi/v2/account?{}", MOKEX_BASE_URL, query);
        debug!("尝试通过直连地址: {}", url);
        
        let mut req_builder = self.mokex_client.get(&url);
        // 转发特定的 Headers (主要是 API Key)
        for (k, v) in headers.iter() {
            req_builder = req_builder.header(k, v);
        }

        match req_builder.send().await {
            Ok(resp) if resp.status().is_success() => {
                 let text = resp.text().await?;
                 info!("✅ [直连成功] 已通过 Mokex 获取账号信息");
                 return Ok(text);
            }
            Ok(resp) => {
                 warn!("⚠️ [直连失败] Mokex 返回状态码: {}", resp.status());
            }
            Err(e) => {
                 warn!("⚠️ [直连失败] 请求错误: {}", e);
            }
        }

        // 2. 尝试代理 (Binance)
        info!("🔄 直连失败，尝试切换到代理通道 (Binance)...");
        let url = format!("{}/fapi/v2/account?{}", BINANCE_BASE_URL, query);
        let mut req_builder = self.binance_client.get(&url);
         for (k, v) in headers.iter() {
             req_builder = req_builder.header(k, v);
        }
        
        match req_builder.send().await {
             Ok(resp) => {
                 let status = resp.status();
                 if status.is_success() {
                     let text = resp.text().await?;
                     info!("✅ [代理成功] 已通过 Binance 代理获取账号信息");
                     Ok(text)
                 } else {
                     let err_text = resp.text().await.unwrap_or_default();
                     warn!("❌ [代理失败] Binance 返回状态码: {}, 响应: {}", status, err_text);
                     Err(AppError::ApiLogic(format!("Binance Proxy Error: Status {}, Body: {}", status, err_text)))
                 }
             }
             Err(e) => {
                 warn!("❌ [代理失败] 请求错误: {}", e);
                 Err(AppError::Reqwest(e))
             }
        }
    }

    /// 获取账户信息 (REST API)
    /// 返回原始 JSON Value
    pub async fn get_account_information(&self) -> Result<Value> {
        let config = self.binance_config.as_ref()
            .ok_or_else(|| AppError::Config("API Key 未配置".to_string()))?;

        let timestamp = get_synced_timestamp();
        let query = format!("timestamp={}&recvWindow=60000", timestamp);
        let signature = config.sign(&query);
        let full_query = format!("{}&signature={}", query, signature);

        // 使用 forward_account_request 复用逻辑? 
        // forward_account_request 是为了转发任意请求设计的，这里我们可以直接利用它的逻辑，
        // 或者简单点直接调它，但要注意它接收的是 headers。
        
        let mut headers = HeaderMap::new();
        headers.insert("X-MBX-APIKEY", HeaderValue::from_str(&config.api_key).unwrap());

        // 由于 forward_account_request 针对的是 /fapi/v2/account，这里正好复用
        let json_str = self.forward_account_request(&full_query, headers).await?;
        let val: Value = serde_json::from_str(&json_str)?;
        Ok(val)
    }

    /// 获取当前挂单 (REST API)
    pub async fn get_open_orders(&self) -> Result<Vec<Value>> {
        let config = self.binance_config.as_ref()
            .ok_or_else(|| AppError::Config("API Key 未配置".to_string()))?;

        let timestamp = get_synced_timestamp();
        let query = format!("timestamp={}&recvWindow=60000", timestamp);
        let signature = config.sign(&query);
        let full_query = format!("{}&signature={}", query, signature);

        // 这里不能复用 forward_account_request，因为那是硬编码了 /fapi/v2/account
        // 我们需要类似的逻辑但是针对 /fapi/v1/openOrders
        
        // 1. 直连
        let url = format!("{}/fapi/v1/openOrders?{}", MOKEX_BASE_URL, full_query);
        let resp = self.mokex_client.get(&url)
            .header("X-MBX-APIKEY", &config.api_key)
            .send().await;

        if let Ok(r) = resp {
             if r.status().is_success() {
                 let val: Vec<Value> = r.json().await?;
                 return Ok(val);
             }
        }

        // 2. 代理
        let url = format!("{}/fapi/v1/openOrders?{}", BINANCE_BASE_URL, full_query);
        let resp = self.binance_client.get(&url)
            .header("X-MBX-APIKEY", &config.api_key)
            .send().await?
            .error_for_status()?;
        
        let val: Vec<Value> = resp.json().await?;
        Ok(val)
    }
}