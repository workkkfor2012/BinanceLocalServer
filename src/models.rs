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
            volume: k.quote_asset_volume.parse::<f64>().unwrap_or(0.0),
        }
    }
}

// ===================================
// 账户状态相关模型
// ===================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountState {
    pub update_time: i64,
    /// 资产余额 (仅 USDT)
    pub balance: f64,
    /// 仓位信息 Symbol -> Position
    pub positions: std::collections::HashMap<String, Position>,
    /// 活动订单 Symbol -> { OrderId -> Order }
    pub orders: std::collections::HashMap<String, std::collections::HashMap<u64, Order>>,
}

impl AccountState {
    pub fn new() -> Self {
        Self {
            update_time: chrono::Utc::now().timestamp_millis(),
            balance: 0.0,
            positions: std::collections::HashMap::new(),
            orders: std::collections::HashMap::new(),
        }
    }

    /// 从 REST API 快照初始化
    pub fn from_snapshot(account: &serde_json::Value, open_orders: &[serde_json::Value]) -> Self {
        let mut state = Self::new();
        
        // 1. 解析余额 (assets)
        if let Some(assets) = account["assets"].as_array() {
            for asset in assets {
                if asset["asset"].as_str() == Some("USDT") {
                    state.balance = asset["crossWalletBalance"].as_str()
                        .and_then(|s| s.parse::<f64>().ok())
                        .unwrap_or(0.0);
                }
            }
        }

        // 2. 解析仓位 (positions)
        if let Some(positions) = account["positions"].as_array() {
            for p in positions {
                 let symbol = p["symbol"].as_str().unwrap_or("").to_string();
                 let position_amt = p["positionAmt"].as_str().and_then(|s| s.parse::<f64>().ok()).unwrap_or(0.0);
                 
                 // 只保存有持仓的
                 if position_amt.abs() > 0.0 {
                     let entry_price = p["entryPrice"].as_str().and_then(|s| s.parse::<f64>().ok()).unwrap_or(0.0);
                     let unrealized_pnl = p["unrealizedProfit"].as_str().and_then(|s| s.parse::<f64>().ok()).unwrap_or(0.0);
                     let side = if position_amt > 0.0 { "LONG" } else { "SHORT" }.to_string();

                     state.positions.insert(symbol.clone(), Position {
                         symbol,
                         side,
                         amount: position_amt.abs(),
                         entry_price,
                         unrealized_pnl,
                     });
                 }
            }
        }

        // 3. 解析订单
        for o in open_orders {
             let symbol = o["symbol"].as_str().unwrap_or("").to_string();
             let order_id = o["orderId"].as_u64().unwrap_or(0);
             let status = o["status"].as_str().unwrap_or("").to_string();
             
             let order = Order {
                 order_id,
                 symbol: symbol.clone(),
                 status,
                 side: o["side"].as_str().unwrap_or("").to_string(),
                 position_side: o["positionSide"].as_str().unwrap_or("BOTH").to_string(),
                 r#type: o["type"].as_str().unwrap_or("").to_string(),
                 price: o["price"].as_str().and_then(|s| s.parse::<f64>().ok()).unwrap_or(0.0),
                 original_quantity: o["origQty"].as_str().and_then(|s| s.parse::<f64>().ok()).unwrap_or(0.0),
                 executed_quantity: o["executedQty"].as_str().and_then(|s| s.parse::<f64>().ok()).unwrap_or(0.0),
                 stop_price: o["stopPrice"].as_str().and_then(|s| s.parse::<f64>().ok()).unwrap_or(0.0),
                 reduce_only: o["reduceOnly"].as_bool().unwrap_or(false),
             };

             state.orders.entry(symbol).or_default().insert(order_id, order);
        }

        state
    }

    /// 更新: ACCOUNT_UPDATE
    pub fn update_from_account_update(&mut self, event: &serde_json::Value) {
        self.update_time = event["E"].as_i64().unwrap_or(0);
        let data = &event["a"];

        // 更新余额
        if let Some(balances) = data["B"].as_array() {
            for b in balances {
                if b["a"].as_str() == Some("USDT") {
                    if let Some(cw) = b["cw"].as_str().and_then(|s| s.parse::<f64>().ok()) {
                        self.balance = cw;
                    }
                }
            }
        }

        // 更新仓位
        if let Some(positions) = data["P"].as_array() {
            for p in positions {
                let symbol = p["s"].as_str().unwrap_or("").to_string();
                let amount = p["pa"].as_str().and_then(|s| s.parse::<f64>().ok()).unwrap_or(0.0);
                let entry_price = p["ep"].as_str().and_then(|s| s.parse::<f64>().ok()).unwrap_or(0.0);
                let unrealized_pnl = p["up"].as_str().and_then(|s| s.parse::<f64>().ok()).unwrap_or(0.0);
                
                if amount.abs() == 0.0 {
                    self.positions.remove(&symbol);
                } else {
                    let side = if amount > 0.0 { "LONG" } else { "SHORT" }.to_string();
                    self.positions.insert(symbol.clone(), Position {
                        symbol,
                        side,
                        amount: amount.abs(),
                        entry_price,
                        unrealized_pnl,
                    });
                }
            }
        }
    }

    /// 更新: ORDER_TRADE_UPDATE
    pub fn update_from_order_update(&mut self, event: &serde_json::Value) {
        self.update_time = event["E"].as_i64().unwrap_or(0);
        let o = &event["o"];
        
        let symbol = o["s"].as_str().unwrap_or("").to_string();
        let order_id = o["i"].as_u64().unwrap_or(0);
        let status = o["X"].as_str().unwrap_or("").to_string();
        
        // 如果是完结状态，移除订单
        if matches!(status.as_str(), "FILLED" | "CANCELED" | "EXPIRED" | "REJECTED") {
            if let Some(orders) = self.orders.get_mut(&symbol) {
                orders.remove(&order_id);
            }
        } else {
            // 更新或新增
            let order = Order {
                order_id,
                symbol: symbol.clone(),
                status,
                side: o["S"].as_str().unwrap_or("").to_string(),
                position_side: o["ps"].as_str().unwrap_or("BOTH").to_string(),
                r#type: o["o"].as_str().unwrap_or("").to_string(),
                price: o["p"].as_str().and_then(|s| s.parse::<f64>().ok()).unwrap_or(0.0),
                original_quantity: o["q"].as_str().and_then(|s| s.parse::<f64>().ok()).unwrap_or(0.0),
                executed_quantity: o["z"].as_str().and_then(|s| s.parse::<f64>().ok()).unwrap_or(0.0),
                stop_price: o["sp"].as_str().and_then(|s| s.parse::<f64>().ok()).unwrap_or(0.0),
                reduce_only: o["R"].as_bool().unwrap_or(false),
            };
            self.orders.entry(symbol).or_default().insert(order_id, order);
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Position {
    pub symbol: String,
    pub side: String, // "LONG", "SHORT", "BOTH"
    pub amount: f64,
    pub entry_price: f64,
    pub unrealized_pnl: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Order {
    pub order_id: u64,
    pub symbol: String,
    pub status: String,
    pub side: String,
    pub position_side: String,
    pub r#type: String,
    pub price: f64,
    pub original_quantity: f64,
    pub executed_quantity: f64,
    pub stop_price: f64, // 触发价
    pub reduce_only: bool,
}
