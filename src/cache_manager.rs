// src/cache_manager.rs
use crate::api_client::ApiClient;
use crate::db_manager::DbManager;
use crate::error::Result;
use crate::models::{DownloadTask, Kline};
use std::sync::Arc;
use tokio::task;
use tracing::{info, instrument, warn};

pub const KLINE_FULL_FETCH_LIMIT: usize = 1500;

// 职责变为：根据前端指令，协调数据源
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

    /// 方案一：只从数据库读取数据并立即返回
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

    /// 方案二：从API更新，内存合并，后台写库，返回新数据
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
        
        // 2. 准备API下载任务
        let start_time = klines_from_db.last().map(|k| k.open_time);
        let task = DownloadTask {
            symbol: symbol.to_string(),
            interval: interval.to_string(),
            start_time,
            end_time: None,
            limit: KLINE_FULL_FETCH_LIMIT,
        };

        // 3. (同步)从API获取新数据
        info!("-> [API_FETCH] Fetching new klines for {}/{} since {:?}.", symbol, interval, start_time);
        let new_klines = self.api_client.download_continuous_klines(&task).await?;

        if new_klines.is_empty() {
            info!("✅ [UPDATE] No new klines from API. Returning {} klines from DB.", klines_from_db.len());
            return Ok(klines_from_db);
        }
        
        info!("-> [API_FETCH] Fetched {} new klines for {}/{}.", new_klines.len(), symbol, interval);

        // 4. (后台)异步将新数据写入数据库
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

        // 5. (同步)在内存中合并新旧数据
        // 检查最后一个旧K线和第一个新K线是否有重叠（时间戳相同），如有则移除旧的那个
        if let Some(last_db_kline) = klines_from_db.last() {
             if let Some(first_new_kline) = new_klines.first() {
                 if last_db_kline.open_time == first_new_kline.open_time {
                     klines_from_db.pop();
                 }
             }
        }
        klines_from_db.extend(new_klines);

        // 6. 确保返回的数据不超过限制
        if klines_from_db.len() > KLINE_FULL_FETCH_LIMIT {
            let overflow = klines_from_db.len() - KLINE_FULL_FETCH_LIMIT;
            // 从开头移除多余的旧数据，保留最新的
            klines_from_db.drain(..overflow);
        }
        
        info!("🚀 [UPDATE] Responding with {} merged klines for {}/{}.", klines_from_db.len(), symbol, interval);

        // 7. 返回合并后的最新数据
        Ok(klines_from_db)
    }
}