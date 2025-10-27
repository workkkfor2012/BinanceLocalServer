// src/cache_manager.rs
use crate::api_client::ApiClient;
use crate::error::{AppError, Result};
use crate::models::{DownloadTask, Kline};
use crate::utils;
use chrono::Utc;
use dashmap::DashMap;
use std::sync::Arc;
use tracing::{info, warn};

const KLINE_CACHE_LIMIT: usize = 3000;
const KLINE_FULL_FETCH_LIMIT: usize = 1500;

pub struct CacheManager {
    // 公开 api_client 字段，以便测试端点可以直接访问
    pub api_client: Arc<ApiClient>, 
    cache: DashMap<(String, String), Vec<Kline>>,
}

impl CacheManager {
    /// 创建一个新的 CacheManager 实例
    pub fn new(api_client: Arc<ApiClient>) -> Self {
        Self {
            api_client,
            cache: DashMap::new(),
        }
    }

    /// 获取K线数据，自动处理缓存逻辑
    pub async fn get_klines(&self, symbol: &str, interval: &str) -> Result<Vec<Kline>> {
        let cache_key = (symbol.to_string(), interval.to_string());

        // 1. 尝试从缓存中获取数据
        if let Some(mut cached_entry) = self.cache.get_mut(&cache_key) {
            let klines = cached_entry.value_mut();

            if let Some(last_kline) = klines.last() {
                let interval_ms = utils::interval_to_milliseconds(interval)?;
                let last_open_time = last_kline.open_time;
                let now_ms = Utc::now().timestamp_millis();
                
                // 2. 检查缓存是否会超过上限
                let missing_duration_ms = now_ms - last_open_time;
                // 至少需要1根来刷新当前K线
                let needed_klines = (missing_duration_ms / interval_ms).max(1) as usize;

                if klines.len() + needed_klines > KLINE_CACHE_LIMIT {
                    warn!(
                        "Cache invalidated for {:?}. Cached size: {}, needed: ~{}. Refetching.",
                        cache_key,
                        klines.len(),
                        needed_klines
                    );
                    // 释放锁，以便下面可以删除
                    drop(cached_entry); 
                    self.cache.remove(&cache_key);
                    // 跳转到下面的“缓存未命中”逻辑
                } else {
                    // 3. 执行增量更新 (包括刷新最后一根K线)
                    info!(
                        "Cache hit for {:?}. Fetching incremental update from openTime {}.",
                        cache_key, last_open_time
                    );
                    let task = DownloadTask {
                        symbol: symbol.to_string(),
                        interval: interval.to_string(),
                        // 使用最后一根K线的 open_time，以获取它的更新和之后的新K线
                        start_time: Some(last_open_time),
                        end_time: None,
                        limit: KLINE_FULL_FETCH_LIMIT, // 请求足够大的数量
                    };
                    
                    let new_klines = self.api_client.download_continuous_klines(&task).await?;

                    if !new_klines.is_empty() {
                        // 关键逻辑：先移除缓存中旧的、可能未完成的最后一根K线
                        klines.pop();
                        // 然后追加所有新的K线（包括已完成的最后一根和之后新增的）
                        klines.extend(new_klines);
                    }

                    // --- 【核心修正】 ---
                    // 1. 控制缓存本身的大小，防止无限增长
                    if klines.len() > KLINE_CACHE_LIMIT {
                        let overflow = klines.len() - KLINE_CACHE_LIMIT;
                        klines.drain(..overflow); // 从开头移除多余的旧数据
                    }

                    // 2. 准备返回给前端的数据：总是最新的 KLINE_FULL_FETCH_LIMIT 根
                    let start_index = klines.len().saturating_sub(KLINE_FULL_FETCH_LIMIT);
                    let response_klines = klines[start_index..].to_vec();
                    
                    return Ok(response_klines);
                }
            }
        }

        // 4. 缓存未命中或已失效，执行全量请求
        info!("Cache miss for {:?}. Performing full fetch.", cache_key);
        let task = DownloadTask {
            symbol: symbol.to_string(),
            interval: interval.to_string(),
            start_time: None,
            end_time: None,
            limit: KLINE_FULL_FETCH_LIMIT,
        };

        let fresh_klines = self.api_client.download_continuous_klines(&task).await?;
        if !fresh_klines.is_empty() {
            self.cache.insert(cache_key, fresh_klines.clone());
        }

        Ok(fresh_klines)
    }
}