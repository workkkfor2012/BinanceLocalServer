// src/cache_manager.rs
use crate::api_client::ApiClient;
use crate::db_manager::DbManager;
use crate::error::Result;
use crate::models::{DownloadTask, Kline};
use crate::utils;
use chrono::Utc;
use std::sync::Arc;
use tokio::task;
use tracing::{info, instrument, warn};

pub const KLINE_CACHE_LIMIT: usize = 3000;
pub const KLINE_FULL_FETCH_LIMIT: usize = 1500;

// 它的职责不再是内存缓存，而是数据获取和更新的协调器
pub struct CacheManager {
    pub api_client: Arc<ApiClient>,
    pub db_manager: Arc<DbManager>,
}

impl CacheManager {
    pub fn new(api_client: Arc<ApiClient>, db_manager: Arc<DbManager>) -> Self {
        Self {
            api_client,
            db_manager,
        }
    }

    #[instrument(skip(self), fields(symbol = %symbol, interval = %interval))]
    pub async fn get_klines(&self, symbol: &str, interval: &str) -> Result<Vec<Kline>> {
        // --- 【核心修改 1】 ---
        // 从数据库读取时，就遵守 1500 条的限制
        let mut klines_from_db = self.db_manager
            .get_latest_klines(symbol, interval, KLINE_FULL_FETCH_LIMIT)
            .await?;

        // 2. 检查数据是否需要更新
        let needs_update = if let Some(last_kline) = klines_from_db.last() {
            let interval_ms = utils::interval_to_milliseconds(interval)?;
            let now_ms = Utc::now().timestamp_millis();
            now_ms - last_kline.open_time > interval_ms
        } else {
            true
        };

        if needs_update {
            info!("-> [DATA] DB data for {}/{} is missing or stale. Fetching from API.", symbol, interval);
            
            let start_time = klines_from_db.last().map(|k| k.open_time);
            
            let task = DownloadTask {
                symbol: symbol.to_string(),
                interval: interval.to_string(),
                start_time,
                end_time: None,
                limit: KLINE_FULL_FETCH_LIMIT,
            };

            let new_klines = self.api_client.download_continuous_klines(&task).await?;

            if !new_klines.is_empty() {
                // 异步保存到数据库
                let db_manager = self.db_manager.clone();
                let klines_to_save = new_klines.clone();
                let symbol_clone = symbol.to_string();
                let interval_clone = interval.to_string();
                task::spawn(async move {
                    info!("💾 [ASYNC] Persisting {} new klines to DB for {}/{}", klines_to_save.len(), symbol_clone, interval_clone);
                    if let Err(e) = db_manager.save_klines(&symbol_clone, &interval_clone, &klines_to_save).await {
                        warn!("Failed to save new klines to DB: {}", e);
                    }
                });

                // 合并新旧数据以立即返回给用户
                if let Some(last_db_kline) = klines_from_db.last() {
                     if let Some(first_new_kline) = new_klines.first() {
                         if last_db_kline.open_time == first_new_kline.open_time {
                             klines_from_db.pop();
                         }
                     }
                }
                klines_from_db.extend(new_klines);
            }
        } else {
            info!("✅ [DATA] DB hit for {}/{}. Serving directly.", symbol, interval);
        }

        // --- 【核心修改 2】 ---
        // 无论发生什么，在函数返回前，进行最终的长度截断，确保严格遵守约定
        if klines_from_db.len() > KLINE_FULL_FETCH_LIMIT {
            let overflow = klines_from_db.len() - KLINE_FULL_FETCH_LIMIT;
            // 从开头移除多余的旧数据，保留最新的
            klines_from_db.drain(..overflow);
        }

        Ok(klines_from_db)
    }
}