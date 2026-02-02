use axum::http::StatusCode;

/// Simple health check endpoint that returns OK
pub async fn healthz() -> (StatusCode, &'static str) {
    (StatusCode::OK, "OK")
}
