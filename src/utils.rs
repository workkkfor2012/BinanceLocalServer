// src/utils.rs
use crate::error::{AppError, Result};

/// 将K线的时间间隔字符串（如 "1m", "4h"）转换为毫秒数
pub fn interval_to_milliseconds(interval: &str) -> Result<i64> {
    let last_char = interval.chars().last().unwrap_or(' ');
    let num_part = &interval[..interval.len() - 1];
    let num: i64 = num_part.parse().map_err(|_| {
        AppError::ApiLogic(format!("Invalid interval format: {}", interval))
    })?;

    match last_char {
        'm' => Ok(num * 60 * 1000),
        'h' => Ok(num * 60 * 60 * 1000),
        'd' => Ok(num * 24 * 60 * 60 * 1000),
        'w' => Ok(num * 7 * 24 * 60 * 60 * 1000),
        's' => Ok(num * 1000),
        _ => Err(AppError::ApiLogic(format!(
            "Unsupported interval unit in: {}",
            interval
        ))),
    }
}

pub fn generate_random_string(len: usize) -> String {
    use rand::distributions::Alphanumeric;
    use rand::{thread_rng, Rng};
    thread_rng()
        .sample_iter(&Alphanumeric)
        .take(len)
        .map(char::from)
        .collect()
}