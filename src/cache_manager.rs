// src/cache_manager.rs
use crate::api_client::ApiClient;
use crate::db_manager::DbManager;
use crate::error::Result;
use crate::models::{DownloadTask, Kline};
use crate::utils; // <-- 引入 utils
use chrono::Utc; // <-- 引入 Utc 用于获取当前时间
use std::sync::Arc;
use tokio::task;
use tracing::{info, instrument, warn};

pub const KLINE_FULL_FETCH_LIMIT: usize = 1500;

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

    #[instrument(skip(self), fields(symbol = %symbol, interval = %interval, source = %source))]
    pub async fn get_klines(
        &self,
        symbol: &str,
        interval: &str,
        source: &str,
    ) -> Result<Vec<Kline>> {
        match source {
            "db_only" => self.get_klines_from_db_only(symbol, interval).await,
            _ => self.get_klines_with_update(symbol, interval).await,
        }
    }
    
    async fn get_klines_from_db_only(
        &self,
        symbol: &str,
        interval: &str,
    ) -> Result<Vec<Kline>> {
        info!("⚡️ [DB_ONLY] Serving {}/{} directly from database.", symbol, interval);
        self.db_manager
            .get_latest_klines(symbol, interval, KLINE_FULL_FETCH_LIMIT)
            .await
    }

    async fn get_klines_with_update(
        &self,
        symbol: &str,
        interval: &str,
    ) -> Result<Vec<Kline>> {
        info!("🔄 [UPDATE] Starting full data sync for {}/{}.", symbol, interval);

        // 1. 从DB读取现有数据
        let mut klines_from_db = self
            .db_manager
            .get_latest_klines(symbol, interval, KLINE_FULL_FETCH_LIMIT)
            .await?;
        
        // 2. 准备API下载任务的初始 start_time
        let mut start_time = klines_from_db.last().map(|k| k.open_time);

        // --- 【核心逻辑：主动重置判断】 ---
        if let Some(last_open_time) = start_time {
            if let Ok(interval_ms) = utils::interval_to_milliseconds(interval) {
                let current_time_ms = Utc::now().timestamp_millis();
                let time_gap_ms = current_time_ms - last_open_time;
                
                // 计算需要补齐多少根K线
                let candles_to_fetch = time_gap_ms / interval_ms;

                if candles_to_fetch > KLINE_FULL_FETCH_LIMIT as i64 {
                    warn!(
                        "⚠️ [CACHE_RESET] Data for {}/{} is too old (gap is {} candles > {}). Deleting local cache and performing a full fetch.",
                        symbol, interval, candles_to_fetch, KLINE_FULL_FETCH_LIMIT
                    );
                    // (a) 删除DB中的旧数据
                    self.db_manager.delete_klines_for_symbol_interval(symbol, interval).await?;
                    // (b) 重置任务为全量更新
                    start_time = None;
                    // (c) 清空内存中的旧数据
                    klines_from_db.clear();
                }
            }
        }
        
        // 3. 创建最终的下载任务
        let task = DownloadTask {
            symbol: symbol.to_string(),
            interval: interval.to_string(),
            start_time, // 可能是原始值，也可能被重置为 None
            end_time: None,
            limit: KLINE_FULL_FETCH_LIMIT,
        };

        // 4. (同步)从API获取新数据
        info!("-> [API_FETCH] Fetching new klines for {}/{} since {:?}.", symbol, interval, start_time);
        let new_klines = self.api_client.download_continuous_klines(&task).await?;

        if new_klines.is_empty() {
            info!("✅ [UPDATE] No new klines from API. Returning {} klines from DB.", klines_from_db.len());
            return Ok(klines_from_db);
        }
        
        info!("-> [API_FETCH] Fetched {} new klines for {}/{}.", new_klines.len(), symbol, interval);

        // 5. (后台)异步将新数据写入数据库
        let db_manager = self.db_manager.clone();
        let klines_to_save = new_klines.clone();
        let symbol_clone = symbol.to_string();
        let interval_clone = interval.to_string();
        task::spawn(async move {
            info!("💾 [ASYNC_DB] Spawning task to persist {} new klines for {}/{}", klines_to_save.len(), symbol_clone, interval_clone);
            if let Err(e) = db_manager.save_klines(&symbol_clone, &interval_clone, &klines_to_save).await {
                warn!("❌ [ASYNC_DB] Failed to save new klines to DB for {}/{}: {}", symbol_clone, interval_clone, e);
            } else {
                info!("✅ [ASYNC_DB] Successfully saved new klines for {}/{}", symbol_clone, interval_clone);
            }
        });

        // 6. (同步)在内存中合并新旧数据
        if let Some(last_db_kline) = klines_from_db.last() {
             if let Some(first_new_kline) = new_klines.first() {
                 if last_db_kline.open_time == first_new_kline.open_time {
                     klines_from_db.pop();
                 }
             }
        }
        klines_from_db.extend(new_klines);

        // 7. 确保返回的数据不超过限制
        if klines_from_db.len() > KLINE_FULL_FETCH_LIMIT {
            let overflow = klines_from_db.len() - KLINE_FULL_FETCH_LIMIT;
            klines_from_db.drain(..overflow);
        }
        
        info!("🚀 [UPDATE] Responding with {} merged klines for {}/{}.", klines_from_db.len(), symbol, interval);

        // 8. 返回合并后的最新数据
        Ok(klines_from_db)
    }
}