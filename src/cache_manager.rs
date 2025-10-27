// src/cache_manager.rs
use crate::api_client::ApiClient;
use crate::db_manager::DbManager;
use crate::error::Result;
use crate::models::{DownloadTask, Kline};
use crate::utils;
use chrono::Utc;
use dashmap::DashMap;
use std::sync::Arc;
use tokio::task;
use tracing::{info, instrument, warn};

pub const KLINE_CACHE_LIMIT: usize = 3000;
pub const KLINE_FULL_FETCH_LIMIT: usize = 1500;

pub struct CacheManager {
    pub api_client: Arc<ApiClient>,
    pub db_manager: Arc<DbManager>,
    mem_cache: DashMap<(String, String), Vec<Kline>>,
}

impl CacheManager {
    pub fn new(api_client: Arc<ApiClient>, db_manager: Arc<DbManager>) -> Self {
        Self {
            api_client,
            db_manager,
            mem_cache: DashMap::new(),
        }
    }
    
    pub fn warm_up(&self, symbol: &str, interval: &str, klines: Vec<Kline>) {
        let cache_key = (symbol.to_string(), interval.to_string());
        self.mem_cache.insert(cache_key, klines);
    }

    #[instrument(skip(self), fields(symbol = %symbol, interval = %interval))]
    pub async fn get_klines(&self, symbol: &str, interval: &str) -> Result<Vec<Kline>> {
        let cache_key = (symbol.to_string(), interval.to_string());

        // --- 步骤 1: 检查缓存并决定下一步操作（无 .await） ---
        enum CacheAction {
            IncrementalUpdate(i64), // 需要增量更新，参数是 start_time
            FullFetch,             // 需要全量获取
            InvalidateAndFullFetch, // 数据太旧，需要先删除再全量获取
        }

        let action = { // 用一个块来限制锁的生命周期
            if let Some(entry) = self.mem_cache.get(&cache_key) {
                let klines = entry.value();
                if let Some(last_kline) = klines.last() {
                    let interval_ms = utils::interval_to_milliseconds(interval)?;
                    let now_ms = Utc::now().timestamp_millis();
                    
                    let too_old_threshold = interval_ms * KLINE_CACHE_LIMIT as i64;
                    if now_ms - last_kline.open_time > too_old_threshold {
                        CacheAction::InvalidateAndFullFetch
                    } else {
                        CacheAction::IncrementalUpdate(last_kline.open_time)
                    }
                } else { // 缓存中有空的 Vec，视为需要全量获取
                    CacheAction::FullFetch
                }
            } else { // 内存中完全没有
                CacheAction::FullFetch
            }
        }; // <-- 在这里，DashMap 的读锁 `entry` 被自动释放

        // --- 步骤 2: 执行异步操作（现在没有任何锁） ---
        match action {
            CacheAction::IncrementalUpdate(start_time) => {
                info!("✅ [CACHE] Memory hit for {:?}. Performing incremental update.", cache_key);
                self.perform_incremental_update(symbol, interval, start_time).await
            }
            CacheAction::InvalidateAndFullFetch => {
                warn!("-> [CACHE] Stale data for {:?} is too old. Invalidating and performing full fetch.", cache_key);
                self.mem_cache.remove(&cache_key); // 移除旧数据
                self.perform_full_fetch(symbol, interval).await
            }
            CacheAction::FullFetch => {
                info!("-> [CACHE] Memory miss for {:?}. Performing full fetch.", cache_key);
                self.perform_full_fetch(symbol, interval).await
            }
        }
    }
    
    async fn perform_full_fetch(&self, symbol: &str, interval: &str) -> Result<Vec<Kline>> {
        let task = DownloadTask {
            symbol: symbol.to_string(),
            interval: interval.to_string(),
            start_time: None, end_time: None, limit: KLINE_FULL_FETCH_LIMIT,
        };
        let fresh_klines = self.api_client.download_continuous_klines(&task).await?;

        if !fresh_klines.is_empty() {
            // 异步保存到数据库
            let db_manager = self.db_manager.clone();
            let klines_to_save = fresh_klines.clone();
            let symbol_clone = symbol.to_string();
            let interval_clone = interval.to_string();
            task::spawn(async move {
                info!("💾 [ASYNC] Persisting {} full-fetch klines to DB for {}/{}", klines_to_save.len(), symbol_clone, interval_clone);
                if let Err(e) = db_manager.save_klines(&symbol_clone, &interval_clone, &klines_to_save).await {
                    warn!("Failed to save full-fetch klines to DB: {}", e);
                }
            });
            // 重新获取锁，写入内存
            self.mem_cache.insert((symbol.to_string(), interval.to_string()), fresh_klines.clone());
        }
        Ok(fresh_klines)
    }

    async fn perform_incremental_update(&self, symbol: &str, interval: &str, start_time: i64) -> Result<Vec<Kline>> {
        let task = DownloadTask {
            symbol: symbol.to_string(),
            interval: interval.to_string(),
            start_time: Some(start_time), end_time: None, limit: KLINE_FULL_FETCH_LIMIT,
        };
        let new_klines = self.api_client.download_continuous_klines(&task).await?;
        
        let cache_key = (symbol.to_string(), interval.to_string());
        
        if !new_klines.is_empty() {
            // 异步保存到数据库
            let db_manager = self.db_manager.clone();
            let klines_to_save = new_klines.clone();
            let symbol_clone = symbol.to_string();
            let interval_clone = interval.to_string();
             task::spawn(async move {
                info!("💾 [ASYNC] Persisting {} incremental klines to DB for {}/{}", klines_to_save.len(), symbol_clone, interval_clone);
                if let Err(e) = db_manager.save_klines(&symbol_clone, &interval_clone, &klines_to_save).await {
                    warn!("Failed to save incremental klines to DB: {}", e);
                }
            });

            // 重新获取写锁，更新内存
            if let Some(mut entry) = self.mem_cache.get_mut(&cache_key) {
                let klines_in_cache = entry.value_mut();
                if klines_in_cache.last().map_or(false, |k| k.open_time == start_time) {
                    klines_in_cache.pop(); // 确保我们移除的是正确的K线
                }
                klines_in_cache.extend(new_klines);
                if klines_in_cache.len() > KLINE_CACHE_LIMIT {
                    let overflow = klines_in_cache.len() - KLINE_CACHE_LIMIT;
                    klines_in_cache.drain(..overflow);
                }
            } else {
                 // 极小概率下，缓存条目在读和写之间被移除了，我们干脆就插入新的
                 self.mem_cache.insert(cache_key.clone(), new_klines);
            }
        }

        // 最后，再次获取读锁并返回最新的数据切片
        if let Some(entry) = self.mem_cache.get(&cache_key) {
            let klines = entry.value();
            let start_index = klines.len().saturating_sub(KLINE_FULL_FETCH_LIMIT);
            return Ok(klines[start_index..].to_vec());
        }
        
        // 如果缓存条目真的不见了，返回空数组，下一次请求会触发全量更新
        warn!("Cache entry for {:?} disappeared. Returning empty vec.", cache_key);
        Ok(vec![])
    }
}