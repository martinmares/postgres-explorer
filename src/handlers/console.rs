use axum::extract::State;
use axum::response::Html;
use askama::Template;
use std::sync::Arc;

use crate::handlers::{build_ctx_with_endpoint, AppState};
use crate::templates::ConsoleTemplate;

pub async fn console(State(state): State<Arc<AppState>>) -> Html<String> {
    let tpl = ConsoleTemplate {
        ctx: build_ctx_with_endpoint(&state, None),
        title: "Dev Console | Postgres Explorer".to_string(),
    };

    Html(tpl.render().unwrap_or_else(|_| "Template error".to_string()))
}
