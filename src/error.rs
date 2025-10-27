// src/error.rs
use thiserror::Error;
use tokio::task::JoinError; // 显式导入，避免歧义

#[derive(Error, Debug)]
pub enum AppError {
    #[error("Reqwest HTTP error: {0}")]
    Reqwest(#[from] reqwest::Error),

    #[error("Serde JSON error: {0}")]
    SerdeJson(#[from] serde_json::Error),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Join error: {0}")]
    Join(#[from] JoinError),

    #[error("Database error: {0}")]
    Database(#[from] tokio_rusqlite::Error), // <-- **核心修复**: 添加此行

    #[error("API logic error: {0}")]
    ApiLogic(String),
}

pub type Result<T> = std::result::Result<T, AppError>;

// 为axum提供错误转换
impl axum::response::IntoResponse for AppError {
    fn into_response(self) -> axum::response::Response {
        let (status, error_message) = match self {
            AppError::ApiLogic(msg) => (axum::http::StatusCode::BAD_REQUEST, msg),
            AppError::Reqwest(e) => (
                axum::http::StatusCode::BAD_GATEWAY,
                format!("Upstream API error: {}", e),
            ),
            _ => (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                self.to_string(),
            ),
        };
        let body = axum::Json(serde_json::json!({ "error": error_message }));
        (status, body).into_response()
    }
}