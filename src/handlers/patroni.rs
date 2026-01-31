use std::sync::Arc;
use askama::Template;
use axum::extract::State;
use axum::response::{Html, IntoResponse};
use axum::Json;
use axum::http::StatusCode;
use serde::{Deserialize, Serialize};

use crate::handlers::{build_ctx, AppState};
use crate::templates::PatroniTemplate;

#[derive(Debug, Serialize, Deserialize)]
pub struct PatroniClusterResponse {
    pub scope: String,
    pub members: Vec<PatroniMember>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PatroniMember {
    pub name: String,
    pub host: String,
    pub port: u16,
    pub role: String,
    pub state: String,
    pub timeline: u64,
    #[serde(default)]
    pub lag: Option<i64>,
}

#[derive(Debug, Serialize)]
pub struct PatroniNodeStatus {
    pub url: String,
    pub online: bool,
    pub error: Option<String>,
    pub cluster: Option<PatroniClusterResponse>,
}

pub async fn patroni_view(
    State(state): State<Arc<AppState>>,
) -> Result<Html<String>, (StatusCode, String)> {
    let ctx = build_ctx(&state);

    if state.patroni_urls.is_none() {
        return Err((
            StatusCode::NOT_FOUND,
            "Patroni monitoring is not enabled. Use --enable-patroni flag.".to_string(),
        ));
    }

    let tmpl = PatroniTemplate {
        ctx,
        title: "Patroni Cluster | Postgres Explorer".to_string(),
    };

    let html = tmpl.render()
        .map_err(|e: askama::Error| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Html(html))
}

pub async fn patroni_status(
    State(state): State<Arc<AppState>>,
) -> Result<Json<Vec<PatroniNodeStatus>>, (StatusCode, String)> {
    let urls = state.patroni_urls.as_ref().ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            "Patroni monitoring is not enabled".to_string(),
        )
    })?;

    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(5))
        .build()
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let mut statuses = Vec::new();

    for url in urls {
        let cluster_url = format!("{}/cluster", url.trim_end_matches('/'));

        match client.get(&cluster_url).send().await {
            Ok(response) => {
                if response.status().is_success() {
                    match response.json::<PatroniClusterResponse>().await {
                        Ok(cluster) => {
                            statuses.push(PatroniNodeStatus {
                                url: url.clone(),
                                online: true,
                                error: None,
                                cluster: Some(cluster),
                            });
                        }
                        Err(e) => {
                            statuses.push(PatroniNodeStatus {
                                url: url.clone(),
                                online: false,
                                error: Some(format!("JSON parse error: {}", e)),
                                cluster: None,
                            });
                        }
                    }
                } else {
                    statuses.push(PatroniNodeStatus {
                        url: url.clone(),
                        online: false,
                        error: Some(format!("HTTP {}", response.status())),
                        cluster: None,
                    });
                }
            }
            Err(e) => {
                statuses.push(PatroniNodeStatus {
                    url: url.clone(),
                    online: false,
                    error: Some(e.to_string()),
                    cluster: None,
                });
            }
        }
    }

    Ok(Json(statuses))
}
