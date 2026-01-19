// src/models.rs
use serde::{Deserialize, Serialize};

/// K线数据结构，与币安API响应和数据库存储格式对应
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Kline {
    pub open_time: i64,
    pub open: String,
    pub high: String,
    pub low: String,
    pub close: String,
    pub volume: String,
    pub close_time: i64,
    pub quote_asset_volume: String,
    pub number_of_trades: i64,
    pub taker_buy_base_asset_volume: String,
    pub taker_buy_quote_asset_volume: String,
    pub ignore: String,
}

impl Kline {
    /// 从币安返回的原始JSON数组解析Kline
    pub fn from_raw_kline(raw: &[serde_json::Value]) -> Option<Self> {
        if raw.len() < 12 {
            return None;
        }
        Some(Self {
            open_time: raw[0].as_i64()?,
            open: raw[1].as_str()?.to_string(),
            high: raw[2].as_str()?.to_string(),
            low: raw[3].as_str()?.to_string(),
            close: raw[4].as_str()?.to_string(),
            volume: raw[5].as_str()?.to_string(),
            close_time: raw[6].as_i64()?,
            quote_asset_volume: raw[7].as_str()?.to_string(),
            number_of_trades: raw[8].as_i64()?,
            taker_buy_base_asset_volume: raw[9].as_str()?.to_string(),
            taker_buy_quote_asset_volume: raw[10].as_str()?.to_string(),
            ignore: raw[11].as_str()?.to_string(),
        })
    }
}

/// 下载任务的定义
#[derive(Debug, Clone)]
pub struct DownloadTask {
    pub symbol: String,
    pub interval: String,
    pub start_time: Option<i64>,
    pub end_time: Option<i64>,
    pub limit: usize,
}

/// 专门用于 JSON 接口的输出 DTO
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KlineJsonDto {
    pub time: i64,       // 秒级时间戳
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
}

impl From<&Kline> for KlineJsonDto {
    fn from(k: &Kline) -> Self {
        Self {
            time: k.open_time / 1000, // 转换为秒
            open: k.open.parse::<f64>().unwrap_or(0.0),
            high: k.high.parse::<f64>().unwrap_or(0.0),
            low: k.low.parse::<f64>().unwrap_or(0.0),
            close: k.close.parse::<f64>().unwrap_or(0.0),
            volume: k.volume.parse::<f64>().unwrap_or(0.0),
        }
    }
}
