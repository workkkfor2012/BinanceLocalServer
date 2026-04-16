use crate::api_client::ApiClient;
use crate::db_manager::DbManager;
use crate::error::Result;
use crate::models::{DownloadTask, Kline};
use crate::utils;
use chrono::Utc;
use std::sync::Arc;
use tokio::task;
use tracing::{instrument, warn};

pub const KLINE_FULL_FETCH_LIMIT: usize = 1500;
pub const KLINE_RESPONSE_MAX_LIMIT: usize = 3000;

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
        start_time: Option<i64>,
        end_time: Option<i64>,
        limit: Option<usize>,
    ) -> Result<Vec<Kline>> {
        let requested_limit = limit
            .unwrap_or(KLINE_FULL_FETCH_LIMIT)
            .clamp(1, KLINE_RESPONSE_MAX_LIMIT);

        match source {
            "db_only" => {
                self.get_klines_from_db_only(symbol, interval, requested_limit)
                    .await
            }
            _ => {
                self.get_klines_with_update(
                    symbol,
                    interval,
                    start_time,
                    end_time,
                    requested_limit,
                )
                .await
            }
        }
    }

    async fn get_klines_from_db_only(
        &self,
        symbol: &str,
        interval: &str,
        limit: usize,
    ) -> Result<Vec<Kline>> {
        self.db_manager.get_latest_klines(symbol, interval, limit).await
    }

    async fn get_klines_with_update(
        &self,
        symbol: &str,
        interval: &str,
        requested_start_time: Option<i64>,
        requested_end_time: Option<i64>,
        requested_limit: usize,
    ) -> Result<Vec<Kline>> {
        let mut klines_from_db = self
            .db_manager
            .get_latest_klines(symbol, interval, requested_limit)
            .await?;

        let mut start_time = requested_start_time.or_else(|| klines_from_db.last().map(|k| k.open_time));

        if let Some(last_open_time) = start_time {
            if let Ok(interval_ms) = utils::interval_to_milliseconds(interval) {
                let current_time_ms = Utc::now().timestamp_millis();
                let time_gap_ms = current_time_ms - last_open_time;
                let candles_to_fetch = time_gap_ms / interval_ms;

                if candles_to_fetch > requested_limit as i64 {
                    self.db_manager
                        .delete_klines_for_symbol_interval(symbol, interval)
                        .await?;
                    start_time = requested_start_time;
                    klines_from_db.clear();
                }
            }
        }

        let task = DownloadTask {
            symbol: symbol.to_string(),
            interval: interval.to_string(),
            start_time,
            end_time: requested_end_time,
            limit: requested_limit,
        };

        let new_klines = self.api_client.download_continuous_klines(&task).await?;

        if new_klines.is_empty() {
            return Ok(klines_from_db);
        }

        let db_manager = self.db_manager.clone();
        let klines_to_save = new_klines.clone();
        let symbol_clone = symbol.to_string();
        let interval_clone = interval.to_string();
        task::spawn(async move {
            if let Err(error) = db_manager
                .save_klines(&symbol_clone, &interval_clone, &klines_to_save)
                .await
            {
                warn!(
                    "failed to persist klines for {} {}: {}",
                    symbol_clone, interval_clone, error
                );
            }
        });

        if let Some(last_db_kline) = klines_from_db.last() {
            if let Some(first_new_kline) = new_klines.first() {
                if last_db_kline.open_time == first_new_kline.open_time {
                    klines_from_db.pop();
                }
            }
        }

        klines_from_db.extend(new_klines);

        if klines_from_db.len() > requested_limit {
            let overflow = klines_from_db.len() - requested_limit;
            klines_from_db.drain(..overflow);
        }

        Ok(klines_from_db)
    }
}
