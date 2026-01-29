use axum::{
    extract::State,
    response::{Html, IntoResponse, Redirect, Response},
    Form,
    http::{StatusCode, HeaderMap},
};
use axum_extra::extract::CookieJar;
use askama::Template;
use serde::Deserialize;
use std::sync::Arc;

use crate::db::models::{CreateEndpoint, UpdateEndpoint};
use crate::handlers::{base_path_url, build_ctx, connect_pg, get_active_endpoint, set_active_endpoint_cookie, AppState};
use crate::templates::{EndpointsListTemplate, EndpointsTemplate};

#[derive(Deserialize)]
pub struct CreateEndpointForm {
    name: String,
    url: String,
    insecure: Option<String>,
    username: Option<String>,
    password: Option<String>,
    ssl_mode: Option<String>,
    search_path: Option<String>,
}

#[derive(Deserialize)]
pub struct UpdateEndpointForm {
    name: String,
    url: String,
    insecure: Option<String>,
    username: Option<String>,
    password: Option<String>,
    ssl_mode: Option<String>,
    search_path: Option<String>,
}

fn render_list(endpoints: Vec<crate::db::models::Endpoint>, active_id: i64) -> Result<Response, (StatusCode, String)> {
    let tpl = EndpointsListTemplate { endpoints, active_id };
    tpl.render()
        .map(Html)
        .map(|h| h.into_response())
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))
}

pub async fn list_endpoints(
    State(state): State<Arc<AppState>>,
    jar: CookieJar,
) -> Result<Response, (StatusCode, String)> {
    let endpoints = state
        .db
        .get_endpoints()
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let active = get_active_endpoint(&state, &jar).await;
    let ctx = build_ctx(&state);

    let tpl = EndpointsTemplate {
        ctx,
        endpoints,
        active_id: active.as_ref().map(|e| e.id).unwrap_or(-1),
    };

    tpl.render()
        .map(Html)
        .map(|h| h.into_response())
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))
}

pub async fn create_endpoint(
    State(state): State<Arc<AppState>>,
    jar: CookieJar,
    Form(form): Form<CreateEndpointForm>,
) -> Result<Response, (StatusCode, String)> {
    let create_endpoint = CreateEndpoint {
        name: form.name,
        url: form.url,
        insecure: form.insecure.is_some(),
        username: if form.username.as_ref().map(|s| s.is_empty()).unwrap_or(true) {
            None
        } else {
            form.username
        },
        password: if form.password.as_ref().map(|s| s.is_empty()).unwrap_or(true) {
            None
        } else {
            form.password
        },
        ssl_mode: if form.ssl_mode.as_ref().map(|s| s.is_empty()).unwrap_or(true) {
            None
        } else {
            form.ssl_mode
        },
        search_path: if form.search_path.as_ref().map(|s| s.is_empty()).unwrap_or(true) {
            None
        } else {
            form.search_path
        },
    };

    if let Err(e) = state.db.create_endpoint(create_endpoint).await {
        tracing::error!("Failed to create endpoint: {}", e);
        return Err((StatusCode::INTERNAL_SERVER_ERROR, e.to_string()));
    }

    let endpoints = state
        .db
        .get_endpoints()
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let active = get_active_endpoint(&state, &jar).await;

    render_list(endpoints, active.as_ref().map(|e| e.id).unwrap_or(-1))
}

pub async fn select_endpoint(
    State(state): State<Arc<AppState>>,
    jar: CookieJar,
    axum::extract::Path(id): axum::extract::Path<i64>,
) -> Result<Response, (StatusCode, String)> {
    let endpoint = state
        .db
        .get_endpoint(id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    if endpoint.is_none() {
        return Err((StatusCode::NOT_FOUND, "Endpoint not found".to_string()));
    }

    let jar = jar.add(set_active_endpoint_cookie(id));
    let target = base_path_url(&state, "/");
    Ok((jar, Redirect::to(&target)).into_response())
}

pub async fn update_endpoint(
    State(state): State<Arc<AppState>>,
    jar: CookieJar,
    axum::extract::Path(id): axum::extract::Path<i64>,
    Form(form): Form<UpdateEndpointForm>,
) -> Result<Response, (StatusCode, String)> {
    let update = UpdateEndpoint {
        name: Some(form.name),
        url: Some(form.url),
        insecure: Some(form.insecure.is_some()),
        username: if form.username.as_ref().map(|s| s.is_empty()).unwrap_or(true) {
            None
        } else {
            form.username
        },
        password: if form.password.as_ref().map(|s| s.is_empty()).unwrap_or(true) {
            None
        } else {
            form.password
        },
        ssl_mode: if form.ssl_mode.as_ref().map(|s| s.is_empty()).unwrap_or(true) {
            None
        } else {
            form.ssl_mode
        },
        search_path: if form.search_path.as_ref().map(|s| s.is_empty()).unwrap_or(true) {
            None
        } else {
            form.search_path
        },
    };

    state
        .db
        .update_endpoint(id, update)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    // Vrátíme HTMX redirect na /endpoints
    let mut headers = HeaderMap::new();
    headers.insert("HX-Redirect", base_path_url(&state, "/endpoints").parse().unwrap());
    Ok((StatusCode::OK, headers).into_response())
}

pub async fn delete_endpoint(
    State(state): State<Arc<AppState>>,
    jar: CookieJar,
    axum::extract::Path(id): axum::extract::Path<i64>,
) -> Result<Response, (StatusCode, String)> {
    state
        .db
        .delete_endpoint(id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    // Vrátíme HTMX redirect na /endpoints
    let mut headers = HeaderMap::new();
    headers.insert("HX-Redirect", base_path_url(&state, "/endpoints").parse().unwrap());
    Ok((StatusCode::OK, headers).into_response())
}

pub async fn test_endpoint(
    State(state): State<Arc<AppState>>,
    axum::extract::Path(id): axum::extract::Path<i64>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let endpoint = state
        .db
        .get_endpoint(id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let endpoint = match endpoint {
        Some(ep) => ep,
        None => return Err((StatusCode::NOT_FOUND, "Endpoint not found".to_string())),
    };

    match connect_pg(&state, &endpoint).await {
        Ok(pg) => {
            let version: Option<String> = sqlx::query_scalar("SELECT version()")
                .fetch_optional(&pg)
                .await
                .unwrap_or(None);
            let payload = serde_json::json!({
                "success": true,
                "message": "Connection OK",
                "version": version.unwrap_or_default()
            });
            Ok((StatusCode::OK, axum::Json(payload)))
        }
        Err(err) => {
            let payload = serde_json::json!({
                "success": false,
                "message": err.to_string()
            });
            Ok((StatusCode::OK, axum::Json(payload)))
        }
    }
}
