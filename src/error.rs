// src/error.rs
use thiserror::Error;

#[derive(Error, Debug)]
pub enum AppError {
    #[error("Reqwest HTTP error: {0}")]
    Reqwest(#[from] reqwest::Error),

    #[error("Serde JSON error: {0}")]
    SerdeJson(#[from] serde_json::Error),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Join error: {0}")]
    Join(#[from] tokio::task::JoinError),

    #[error("API logic error: {0}")]
    ApiLogic(String),
}

pub type Result<T> = std::result::Result<T, AppError>;

// 为axum提供错误转换
impl axum::response::IntoResponse for AppError {
    fn into_response(self) -> axum::response::Response {
        let (status, error_message) = match self {
            // 当是 API 逻辑错误 (如无效参数) 时，返回 400 Bad Request
            AppError::ApiLogic(msg) => (axum::http::StatusCode::BAD_REQUEST, msg),
            
            // 上游 API 错误，返回 502 Bad Gateway
            AppError::Reqwest(e) => (
                axum::http::StatusCode::BAD_GATEWAY,
                format!("Upstream API error: {}", e),
            ),

            // 其他所有错误视为服务器内部错误，返回 500
            _ => (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                self.to_string(),
            ),
        };
        let body = axum::Json(serde_json::json!({ "error": error_message }));
        (status, body).into_response()
    }
}