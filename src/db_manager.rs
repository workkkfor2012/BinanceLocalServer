// src/db_manager.rs
use crate::error::Result;
use crate::models::Kline;
use std::collections::HashMap;
use tokio_rusqlite::{params, Connection};
use tracing::info; // <- 移除了未使用的 `warn`

const DB_PATH: &str = "kline_cache.db";
const PRUNE_TRIGGER_COUNT: i64 = 3000;
const PRUNE_KEEP_COUNT: i64 = 1500;

#[derive(Clone)]
pub struct DbManager {
    conn: Connection,
}

impl DbManager {
    pub async fn new() -> Result<Self> {
        let conn = Connection::open(DB_PATH).await?;
        info!("🗄️ Database connection opened at '{}'", DB_PATH);
        
        conn.call(|conn| {
            conn.pragma_update(None, "journal_mode", "WAL")?;
            conn.pragma_update(None, "synchronous", "NORMAL")?;
            Ok(())
        }).await?;
        info!("🗄️ WAL mode enabled for SQLite.");

        conn.call(|conn| {
            conn.execute(
                "CREATE TABLE IF NOT EXISTS klines (
                    symbol              TEXT NOT NULL,
                    interval            TEXT NOT NULL,
                    open_time           INTEGER NOT NULL,
                    open                TEXT NOT NULL,
                    high                TEXT NOT NULL,
                    low                 TEXT NOT NULL,
                    close               TEXT NOT NULL,
                    volume              TEXT NOT NULL,
                    close_time          INTEGER NOT NULL,
                    quote_asset_volume  TEXT NOT NULL,
                    number_of_trades    INTEGER NOT NULL,
                    taker_buy_base_asset_volume TEXT NOT NULL,
                    taker_buy_quote_asset_volume TEXT NOT NULL,
                    ignore              TEXT NOT NULL,
                    PRIMARY KEY (symbol, interval, open_time)
                )",
                [],
            )?;
            Ok(())
        }).await?;
        info!("🗄️ 'klines' table initialized.");

        Ok(Self { conn })
    }

    // --- 【已修复编译错误】 ---
    pub async fn delete_klines_for_symbol_interval(&self, symbol: &str, interval: &str) -> Result<()> {
        let symbol = symbol.to_string();
        let interval = interval.to_string();

        self.conn.call(move |conn| {
            let deleted_rows = conn.execute(
                "DELETE FROM klines WHERE symbol = ?1 AND interval = ?2",
                params![symbol, interval],
            )?;
            Ok(())
        }).await?;
        
        Ok(())
    }

    pub async fn save_klines(&self, symbol: &str, interval: &str, klines: &[Kline]) -> Result<()> {
        if klines.is_empty() {
            return Ok(());
        }

        let symbol_owned = symbol.to_string();
        let interval_owned = interval.to_string();
        let klines_owned = klines.to_vec();

        self.conn
            .call(move |conn| {
                let tx = conn.transaction()?;
                {
                    let mut stmt = tx.prepare_cached(
                        "INSERT OR REPLACE INTO klines (
                            symbol, interval, open_time, open, high, low, close, volume, close_time,
                            quote_asset_volume, number_of_trades, taker_buy_base_asset_volume,
                            taker_buy_quote_asset_volume, ignore
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    )?;

                    for kline in &klines_owned {
                        stmt.execute(params![
                            symbol_owned,
                            interval_owned,
                            kline.open_time,
                            kline.open, // <-- 【已修复数据损坏BUG】
                            kline.high,
                            kline.low,
                            kline.close,
                            kline.volume,
                            kline.close_time,
                            kline.quote_asset_volume,
                            kline.number_of_trades,
                            kline.taker_buy_base_asset_volume,
                            kline.taker_buy_quote_asset_volume,
                            kline.ignore,
                        ])?;
                    }
                }
                tx.commit()?;
                Ok(())
            })
            .await?;
        
        self.prune_klines_if_needed(symbol, interval).await?;
        Ok(())
    }
    
    async fn prune_klines_if_needed(&self, symbol: &str, interval: &str) -> Result<()> {
        let symbol = symbol.to_string();
        let interval = interval.to_string();

        self.conn.call(move |conn| {
            let count: i64 = conn.query_row(
                "SELECT COUNT(*) FROM klines WHERE symbol = ?1 AND interval = ?2",
                params![&symbol, &interval],
                |row| row.get(0),
            )?;

            if count > PRUNE_TRIGGER_COUNT {
                let deleted_rows = conn.execute(
                    "DELETE FROM klines WHERE symbol = ?1 AND interval = ?2 AND open_time IN (
                        SELECT open_time FROM klines WHERE symbol = ?1 AND interval = ?2 ORDER BY open_time ASC LIMIT ?3
                    )",
                    params![&symbol, &interval, (count - PRUNE_KEEP_COUNT)],
                )?;
            }
            Ok(())
        }).await?;

        Ok(())
    }

    pub async fn get_latest_klines(
        &self,
        symbol: &str,
        interval: &str,
        limit: usize,
    ) -> Result<Vec<Kline>> {
        let symbol = symbol.to_string();
        let interval = interval.to_string();

        let klines = self.conn.call(move |conn| {
            let mut stmt = conn.prepare_cached(
                "SELECT * FROM klines WHERE symbol = ?1 AND interval = ?2 ORDER BY open_time DESC LIMIT ?3",
            )?;
            let mut rows = stmt.query(params![symbol, interval, limit])?;
            let mut result_klines = Vec::new();

            while let Some(row) = rows.next()? {
                 result_klines.push(Kline {
                    open_time: row.get(2)?,
                    open: row.get(3)?,
                    high: row.get(4)?,
                    low: row.get(5)?,
                    close: row.get(6)?,
                    volume: row.get(7)?,
                    close_time: row.get(8)?,
                    quote_asset_volume: row.get(9)?,
                    number_of_trades: row.get(10)?,
                    taker_buy_base_asset_volume: row.get(11)?,
                    taker_buy_quote_asset_volume: row.get(12)?,
                    ignore: row.get(13)?,
                });
            }
            result_klines.reverse();
            Ok(result_klines)
        }).await?;
        Ok(klines)
    }

    pub async fn get_db_summary(&self) -> Result<HashMap<String, Vec<(String, i64)>>> {
        let summary_data = self
            .conn
            .call(|conn| {
                let mut stmt = conn.prepare_cached(
                    "SELECT symbol, interval, COUNT(*) FROM klines GROUP BY symbol, interval",
                )?;
                let mut rows = stmt.query([])?;
                let mut summary = HashMap::<String, Vec<(String, i64)>>::new();

                while let Some(row) = rows.next()? {
                    let symbol: String = row.get(0)?;
                    let interval: String = row.get(1)?;
                    let count: i64 = row.get(2)?;

                    summary.entry(symbol).or_default().push((interval, count));
                }
                Ok(summary)
            })
            .await?;
        Ok(summary_data)
    }

    pub async fn get_all_cache_keys(&self) -> Result<Vec<(String, String)>> {
        let keys = self.conn.call(|conn| {
            let mut stmt = conn.prepare_cached(
                "SELECT DISTINCT symbol, interval FROM klines",
            )?;
            let mut rows = stmt.query([])?;
            let mut result_keys = Vec::new();
            while let Some(row) = rows.next()? {
                result_keys.push((row.get(0)?, row.get(1)?));
            }
            Ok(result_keys)
        }).await?;
        Ok(keys)
    }
}