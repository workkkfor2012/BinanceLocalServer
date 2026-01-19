// src/config.rs
//! 配置管理模块
//!
//! 从 config.toml 加载应用配置，包括 API 密钥和端点设置。

use serde::Deserialize;
use std::fs;
use std::path::Path;
use tracing::{info, warn};

/// 应用配置
#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub binance: BinanceConfig,
}

/// 币安相关配置
#[derive(Debug, Clone, Deserialize)]
pub struct BinanceConfig {
    /// API Key
    pub api_key: String,
    /// API Secret
    pub api_secret: String,
    /// 直连 REST API 地址
    pub direct_rest_base: String,
    /// 直连 WebSocket 地址
    pub direct_ws_base: String,
    /// 代理 REST API 地址
    pub proxy_rest_base: String,
    /// 代理 WebSocket 地址
    pub proxy_ws_base: String,
    /// SOCKS5 代理地址
    pub socks5_proxy: String,
}

impl Config {
    /// 从配置文件加载配置
    pub fn load() -> Option<Self> {
        let config_path = Path::new("config.toml");
        
        if !config_path.exists() {
            warn!("⚠️ 配置文件 config.toml 不存在，私有数据流功能将被禁用");
            return None;
        }
        
        match fs::read_to_string(config_path) {
            Ok(content) => {
                match toml::from_str::<Config>(&content) {
                    Ok(config) => {
                        // 检查 API Key 是否已配置
                        if config.binance.api_key == "YOUR_API_KEY_HERE" 
                            || config.binance.api_key.is_empty() 
                        {
                            warn!("⚠️ API Key 未配置，私有数据流功能将被禁用");
                            return None;
                        }
                        
                        info!("✅ 配置文件加载成功");
                        info!("  - 直连 REST: {}", config.binance.direct_rest_base);
                        info!("  - 代理 REST: {}", config.binance.proxy_rest_base);
                        info!("  - API Key: {}...", &config.binance.api_key[..8.min(config.binance.api_key.len())]);
                        
                        Some(config)
                    }
                    Err(e) => {
                        warn!("❌ 配置文件解析失败: {}", e);
                        None
                    }
                }
            }
            Err(e) => {
                warn!("❌ 读取配置文件失败: {}", e);
                None
            }
        }
    }
}

impl BinanceConfig {
    /// 生成 HMAC-SHA256 签名
    pub fn sign(&self, message: &str) -> String {
        use hmac::{Hmac, Mac};
        use sha2::Sha256;
        
        type HmacSha256 = Hmac<Sha256>;
        
        let mut mac = HmacSha256::new_from_slice(self.api_secret.as_bytes())
            .expect("HMAC can take key of any size");
        mac.update(message.as_bytes());
        
        hex::encode(mac.finalize().into_bytes())
    }
}
