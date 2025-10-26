// BinanceLocalServer/src/transformer.rs
use crate::error::{AppError, Result};
use crate::models::Kline;
use std::io::{Cursor, Write};
use tracing::warn;

// --- 常量定义，与 API 规范保持一致 ---
const MAX_KLINE_RECORDS: usize = 2000;
const NUM_FIELDS: usize = 9; // 前端 K_Line 有9个字段
const FIELD_BLOCK_SIZE: usize = MAX_KLINE_RECORDS * 8; // 2000 * f64/i64

// K线数据体: 9个字段 * 每个字段16000字节 = 144000
const KLINE_DATA_BODY_SIZE: usize = NUM_FIELDS * FIELD_BLOCK_SIZE;
// K线数据缓冲区: kline_count(4) + start_index(4) + K线数据体(144000) = 144008
const KLINE_DATA_BUFFER_SIZE: usize = 4 + 4 + KLINE_DATA_BODY_SIZE;
// 记录体: 品种周期字符串 (32) + K线数据缓冲区 (144008) = 144040
const RECORD_BODY_SIZE: usize = 32 + KLINE_DATA_BUFFER_SIZE;
// 全局头部 (8) + 记录体 (144040) = 144048
const TOTAL_BLOB_SIZE: usize = 8 + RECORD_BODY_SIZE;


/// 【已更新】将字符串形式的时间间隔映射到规范要求的 periodIndex
// ... (函数内容不变)
fn interval_to_period_index(interval: &str) -> Result<i32> {
    match interval {
        "1s" => Ok(-1),
        "1m" => Ok(0),
        "5m" => Ok(1),
        "30m" => Ok(2),
        "4h" => Ok(3),
        "1d" => Ok(4),
        "1w" => Ok(5),
        _ => Err(AppError::ApiLogic(format!(
            "Unsupported interval for binary format: '{}'. Supported values are: 1s, 1m, 5m, 30m, 4h, 1d, 1w.",
            interval
        ))),
    }
}

/// 将获取到的 K 线数据转换为前端需要的二进制 blob
pub fn klines_to_binary_blob(
    klines: &[Kline],
    symbol: &str,
    interval: &str,
) -> Result<Vec<u8>> {
    // 1. 初始化一个固定大小、填满0的字节缓冲区
    let mut buffer = vec![0u8; TOTAL_BLOB_SIZE];
    let mut writer = Cursor::new(&mut buffer[..]);

    // --- 写入全局头部 (8字节) ---
    writer.write_all(&1u32.to_le_bytes())?; // 记录数量，固定为1
    writer.write_all(&0u32.to_le_bytes())?; // 保留字段

    // --- 开始写入记录体 (从偏移量 8 开始) ---

    // A. 写入品种周期字符串 (32字节)
    let period_index = interval_to_period_index(interval)?;
    let symbol_period_str = format!("{}_{}", symbol, period_index);
    let mut symbol_bytes = [0u8; 32];
    let bytes_to_copy = symbol_period_str.as_bytes();
    let len = bytes_to_copy.len().min(32);
    symbol_bytes[..len].copy_from_slice(&bytes_to_copy[..len]);
    writer.write_all(&symbol_bytes)?;

    // B. 写入K线数据缓冲区头部 (8字节)
    let kline_count = klines.len() as u32;
    writer.write_all(&kline_count.to_le_bytes())?; // 实际K线数量
    writer.write_all(&0u32.to_le_bytes())?; // 起始索引，固定为0

    // C. 写入K线数据体 (144,000字节) - Struct of Arrays 布局
    let data_body_slice = &mut writer.into_inner()[48..];

    for (i, kline) in klines.iter().enumerate() {
        if i >= MAX_KLINE_RECORDS {
            warn!("More than {} klines provided, truncating.", MAX_KLINE_RECORDS);
            break;
        }

        let open = kline.open.parse::<f64>().unwrap_or(0.0);
        let high = kline.high.parse::<f64>().unwrap_or(0.0);
        let low = kline.low.parse::<f64>().unwrap_or(0.0);
        let close = kline.close.parse::<f64>().unwrap_or(0.0);
        let quote_volume = kline.quote_asset_volume.parse::<f64>().unwrap_or(0.0);
        let zero_f64 = 0.0f64;

        let offset = i * 8;

        // --- 【核心修正】按照前端 CycleArray 排序后的字段顺序写入 ---
        // 排序后: close, ext_多空比, ext_持仓量, ext_资金费率, high, low, open, timestamp, 成交额
        data_body_slice[0 * FIELD_BLOCK_SIZE + offset..0 * FIELD_BLOCK_SIZE + offset + 8].copy_from_slice(&close.to_le_bytes());
        data_body_slice[1 * FIELD_BLOCK_SIZE + offset..1 * FIELD_BLOCK_SIZE + offset + 8].copy_from_slice(&zero_f64.to_le_bytes());
        data_body_slice[2 * FIELD_BLOCK_SIZE + offset..2 * FIELD_BLOCK_SIZE + offset + 8].copy_from_slice(&zero_f64.to_le_bytes());
        data_body_slice[3 * FIELD_BLOCK_SIZE + offset..3 * FIELD_BLOCK_SIZE + offset + 8].copy_from_slice(&zero_f64.to_le_bytes());
        data_body_slice[4 * FIELD_BLOCK_SIZE + offset..4 * FIELD_BLOCK_SIZE + offset + 8].copy_from_slice(&high.to_le_bytes());
        data_body_slice[5 * FIELD_BLOCK_SIZE + offset..5 * FIELD_BLOCK_SIZE + offset + 8].copy_from_slice(&low.to_le_bytes());
        data_body_slice[6 * FIELD_BLOCK_SIZE + offset..6 * FIELD_BLOCK_SIZE + offset + 8].copy_from_slice(&open.to_le_bytes());
        data_body_slice[7 * FIELD_BLOCK_SIZE + offset..7 * FIELD_BLOCK_SIZE + offset + 8].copy_from_slice(&kline.open_time.to_le_bytes());
        data_body_slice[8 * FIELD_BLOCK_SIZE + offset..8 * FIELD_BLOCK_SIZE + offset + 8].copy_from_slice(&quote_volume.to_le_bytes());
    }

    Ok(buffer)
}