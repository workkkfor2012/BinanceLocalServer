// src/cache_manager.rs
use crate::api_client::ApiClient;
use crate::error::{AppError, Result};
use crate::models::{DownloadTask, Kline};
use crate::utils;
use chrono::Utc;
use dashmap::DashMap;
use std::sync::Arc;
use tracing::{info, instrument, warn};

const KLINE_CACHE_LIMIT: usize = 3000;
const KLINE_FULL_FETCH_LIMIT: usize = 1500;

pub struct CacheManager {
    pub api_client: Arc<ApiClient>,
    cache: DashMap<(String, String), Vec<Kline>>,
}

impl CacheManager {
    pub fn new(api_client: Arc<ApiClient>) -> Self {
        Self {
            api_client,
            cache: DashMap::new(),
        }
    }

    #[instrument(skip(self), fields(symbol = %symbol, interval = %interval))]
    pub async fn get_klines(&self, symbol: &str, interval: &str) -> Result<Vec<Kline>> {
        let cache_key = (symbol.to_string(), interval.to_string());
        let mut last_open_time_for_update: Option<i64> = None;

        // --- 步骤 1: 【无await】检查缓存并决定是否需要增量更新 ---
        // 使用读锁（共享锁）来检查，避免不必要的写锁
        if let Some(cached_entry) = self.cache.get(&cache_key) {
            let klines = cached_entry.value();

            if let Some(last_kline) = klines.last() {
                let interval_ms = utils::interval_to_milliseconds(interval)?;
                let last_open_time = last_kline.open_time;
                let now_ms = Utc::now().timestamp_millis();

                let missing_duration_ms = now_ms - last_open_time;
                let needed_klines = (missing_duration_ms / interval_ms).max(1) as usize;

                if klines.len() + needed_klines > KLINE_CACHE_LIMIT {
                    warn!(
                        "Cache invalidated for {:?}. Cached size: {}, needed: ~{}. Refetching.",
                        cache_key,
                        klines.len(),
                        needed_klines
                    );
                    // 释放读锁后，再获取写权限来移除
                    drop(cached_entry);
                    self.cache.remove(&cache_key);
                } else {
                    info!(
                        "Cache hit for {:?}. Preparing incremental update from openTime {}.",
                        cache_key, last_open_time
                    );
                    // 【核心修正 A】: 只记录需要的数据，然后在这个 if 块结束时自动释放读锁
                    last_open_time_for_update = Some(last_open_time);
                }
            } else {
                // 缓存中是空数组，移除它并进行全量获取
                drop(cached_entry);
                self.cache.remove(&cache_key);
            }
        } // <--- cached_entry 在这里被丢弃，读锁被释放

        // --- 步骤 2: 【有await】如果需要，执行网络请求 (此时已没有任何锁) ---
        if let Some(start_time) = last_open_time_for_update {
            let task = DownloadTask {
                symbol: symbol.to_string(),
                interval: interval.to_string(),
                start_time: Some(start_time),
                end_time: None,
                limit: KLINE_FULL_FETCH_LIMIT,
            };

            // 【核心修正 B】: 在锁外执行 await，不会阻塞其他任务
            info!("🚀 Performing incremental network fetch for {:?}", cache_key);
            let new_klines = self.api_client.download_continuous_klines(&task).await?;
            info!("✅ Incremental fetch done for {:?}", cache_key);

            // --- 步骤 3: 【无await】重新获取写锁并更新缓存 ---
            if !new_klines.is_empty() {
                if let Some(mut entry) = self.cache.get_mut(&cache_key) {
                    let klines_in_cache = entry.value_mut();
                    klines_in_cache.pop();
                    klines_in_cache.extend(new_klines);
                    
                    if klines_in_cache.len() > KLINE_CACHE_LIMIT {
                        let overflow = klines_in_cache.len() - KLINE_CACHE_LIMIT;
                        klines_in_cache.drain(..overflow);
                    }
                } // <--- 写锁在这里被释放
            }
            
            // --- 步骤 4: 【无await】再次获取读锁，准备返回数据 ---
            if let Some(entry) = self.cache.get(&cache_key) {
                let klines = entry.value();
                let start_index = klines.len().saturating_sub(KLINE_FULL_FETCH_LIMIT);
                let response_klines = klines[start_index..].to_vec();
                return Ok(response_klines);
            }
        }
        
        // --- 步骤 5: 【有await】缓存未命中或已失效，执行全量请求 (无锁状态) ---
        info!("🌊 Performing full network fetch for {:?}", cache_key);
        let task = DownloadTask {
            symbol: symbol.to_string(),
            interval: interval.to_string(),
            start_time: None,
            end_time: None,
            limit: KLINE_FULL_FETCH_LIMIT,
        };

        let fresh_klines = self.api_client.download_continuous_klines(&task).await?;
        info!("✅ Full fetch done for {:?}", cache_key);
        
        if !fresh_klines.is_empty() {
            self.cache.insert(cache_key, fresh_klines.clone());
        }

        Ok(fresh_klines)
    }
}