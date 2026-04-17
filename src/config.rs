use crate::utils::resolve_runtime_base_dir;
use serde::Deserialize;
use std::fs;
use std::path::Path;
use tracing::{info, warn};

const DEFAULT_DOWNLOAD_PROXY_URL: &str = "http://127.0.0.1:17892";
const DEFAULT_WS_PROXY_ADDR: &str = "127.0.0.1:1080";
const DEFAULT_REST_SOCKS5H_PROXY_URL: &str = "socks5h://127.0.0.1:1080";
const DEFAULT_PROXY_REST_BASE: &str = "https://fapi.binance.com";
const DEFAULT_PROXY_WS_BASE: &str = "wss://fstream.binance.com";

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub binance: BinanceConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BinanceConfig {
    pub proxy_rest_base: Option<String>,
    pub proxy_ws_base: Option<String>,
    pub rest_proxy_url: Option<String>,
    pub ws_socks5_proxy: Option<String>,
    pub socks5_proxy: Option<String>,
}

impl Config {
    pub fn load() -> Option<Self> {
        let base_dir = resolve_runtime_base_dir();
        let config_path_buf = base_dir.join("config.toml");
        let config_path = Path::new(&config_path_buf);
        if !config_path.exists() {
            warn!(
                "config.toml not found at {}, using built-in proxy defaults",
                config_path.display()
            );
            return None;
        }

        match fs::read_to_string(config_path) {
            Ok(content) => match toml::from_str::<Config>(&content) {
                Ok(config) => {
                    info!("config.toml loaded from {}", config_path.display());
                    info!(
                        "  - download proxy: {}",
                        config.binance.download_proxy_url()
                    );
                    info!(
                        "  - rest proxy: {}",
                        config.binance.non_download_rest_proxy_url()
                    );
                    info!("  - ws proxy: {}", config.binance.ws_proxy_addr());
                    info!("  - upstream rest: {}", config.binance.proxy_rest_base());
                    info!("  - upstream ws: {}", config.binance.proxy_public_ws_url());
                    Some(config)
                }
                Err(e) => {
                    warn!("failed to parse config.toml: {}", e);
                    None
                }
            },
            Err(e) => {
                warn!("failed to read config.toml: {}", e);
                None
            }
        }
    }
}

impl BinanceConfig {
    pub fn download_proxy_url(&self) -> &str {
        self.rest_proxy_url
            .as_deref()
            .filter(|value| !value.is_empty())
            .unwrap_or(DEFAULT_DOWNLOAD_PROXY_URL)
    }

    pub fn non_download_rest_proxy_url(&self) -> String {
        if let Some(addr) = self
            .ws_socks5_proxy
            .as_deref()
            .or(self.socks5_proxy.as_deref())
        {
            if addr.contains("://") {
                if addr.starts_with("socks5h://") {
                    return addr.to_string();
                }
                if addr.starts_with("socks5://") {
                    return addr.replacen("socks5://", "socks5h://", 1);
                }
                return addr.to_string();
            }
            return format!("socks5h://{}", addr);
        }

        DEFAULT_REST_SOCKS5H_PROXY_URL.to_string()
    }

    pub fn ws_proxy_addr(&self) -> &str {
        self.ws_socks5_proxy
            .as_deref()
            .or(self.socks5_proxy.as_deref())
            .unwrap_or(DEFAULT_WS_PROXY_ADDR)
    }

    pub fn proxy_rest_base(&self) -> &str {
        self.proxy_rest_base
            .as_deref()
            .filter(|value| !value.is_empty())
            .unwrap_or(DEFAULT_PROXY_REST_BASE)
    }

    pub fn proxy_public_ws_url(&self) -> String {
        format!(
            "{}/ws",
            self.proxy_ws_base
                .as_deref()
                .filter(|value| !value.is_empty())
                .unwrap_or(DEFAULT_PROXY_WS_BASE)
                .trim_end_matches('/')
        )
    }
}
