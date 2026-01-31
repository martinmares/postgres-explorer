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
    #[serde(flatten)]
    pub extra: Option<serde_json::Value>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PatroniMember {
    pub name: String,
    pub host: String,
    pub port: u16,
    pub role: String,
    pub state: String,
    #[serde(default)]
    pub timeline: Option<u64>,
    #[serde(default, deserialize_with = "deserialize_lag")]
    pub lag: Option<i64>,
    #[serde(default)]
    pub tags: Option<PatroniTags>,
}

fn deserialize_lag<'de, D>(deserializer: D) -> Result<Option<i64>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::Deserialize;

    #[derive(Deserialize)]
    #[serde(untagged)]
    enum LagValue {
        Number(i64),
        String(String),
    }

    match Option::<LagValue>::deserialize(deserializer)? {
        Some(LagValue::Number(n)) => Ok(Some(n)),
        Some(LagValue::String(_)) => Ok(None), // "unknown" -> None
        None => Ok(None),
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PatroniTags {
    #[serde(default)]
    pub clonefrom: Option<bool>,
    #[serde(default)]
    pub noloadbalance: Option<bool>,
    #[serde(default)]
    pub nofailover: Option<bool>,
    #[serde(default)]
    pub nosync: Option<bool>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PatroniNodeInfo {
    pub state: String,
    pub role: String,
    #[serde(default)]
    pub server_version: Option<u32>,
    #[serde(default)]
    pub cluster_unlocked: Option<bool>,
    #[serde(default)]
    pub xlog: Option<XlogInfo>,
    #[serde(default)]
    pub timeline: Option<u64>,
    #[serde(default)]
    pub database_system_identifier: Option<String>,
    #[serde(default)]
    pub patroni: Option<PatroniInfo>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct XlogInfo {
    #[serde(default)]
    pub location: Option<u64>,
    #[serde(default)]
    pub received_location: Option<u64>,
    #[serde(default)]
    pub replayed_location: Option<u64>,
    #[serde(default)]
    pub paused: Option<bool>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PatroniInfo {
    pub version: String,
    #[serde(default)]
    pub scope: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TimelineHistoryEntry(pub u64, pub u64, pub String);

#[derive(Debug, Serialize, Deserialize)]
pub struct PatroniConfig {
    #[serde(default)]
    pub ttl: Option<u32>,
    #[serde(default)]
    pub loop_wait: Option<u32>,
    #[serde(default)]
    pub retry_timeout: Option<u32>,
    #[serde(default)]
    pub maximum_lag_on_failover: Option<u64>,
    #[serde(default)]
    pub synchronous_mode: Option<bool>,
    #[serde(default)]
    pub synchronous_mode_strict: Option<bool>,
    #[serde(default)]
    pub postgresql: Option<serde_json::Value>,
}

#[derive(Debug, Serialize, Clone)]
pub struct PatroniMemberExtended {
    pub name: String,
    pub host: String,
    pub port: u16,
    pub role: String,
    pub state: String,
    pub timeline: Option<u64>,
    pub lag: Option<i64>,
    pub pg_version: Option<String>,
    pub patroni_version: Option<String>,
    pub xlog_location: Option<u64>,
    pub xlog_received_location: Option<u64>,
    pub xlog_replayed_location: Option<u64>,
    pub wal_lag_bytes: Option<u64>,
    pub cluster_unlocked: Option<bool>,
    pub tags: Option<PatroniTags>,
}

#[derive(Debug, Serialize)]
pub struct PatroniNodeStatus {
    pub url: String,
    pub online: bool,
    pub error: Option<String>,
    pub cluster: Option<PatroniClusterResponse>,
    pub members_extended: Vec<PatroniMemberExtended>,
    pub history: Vec<TimelineHistoryEntry>,
    pub config: Option<PatroniConfig>,
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
                    let body_text = match response.text().await {
                        Ok(text) => text,
                        Err(e) => {
                            statuses.push(PatroniNodeStatus {
                                url: url.clone(),
                                online: false,
                                error: Some(format!("Failed to read response body: {}", e)),
                                cluster: None,
                                members_extended: Vec::new(),
                                history: Vec::new(),
                                config: None,
                            });
                            continue;
                        }
                    };

                    match serde_json::from_str::<PatroniClusterResponse>(&body_text) {
                        Ok(cluster) => {
                            // Fetch details from each member
                            let mut members_extended = Vec::new();

                            for member in &cluster.members {
                                let node_url = format!("http://{}:{}/patroni", member.host, 8008);

                                let mut extended = PatroniMemberExtended {
                                    name: member.name.clone(),
                                    host: member.host.clone(),
                                    port: member.port,
                                    role: member.role.clone(),
                                    state: member.state.clone(),
                                    timeline: member.timeline,
                                    lag: member.lag,
                                    pg_version: None,
                                    patroni_version: None,
                                    xlog_location: None,
                                    xlog_received_location: None,
                                    xlog_replayed_location: None,
                                    wal_lag_bytes: None,
                                    cluster_unlocked: None,
                                    tags: member.tags.clone(),
                                };

                                // Try to get node details
                                if let Ok(node_resp) = client.get(&node_url).send().await {
                                    if let Ok(node_info) = node_resp.json::<PatroniNodeInfo>().await {
                                        // Parse PG version (e.g. 160001 -> "16.0.1")
                                        if let Some(ver) = node_info.server_version {
                                            let major = ver / 10000;
                                            let minor = (ver / 100) % 100;
                                            let patch = ver % 100;
                                            extended.pg_version = Some(if patch > 0 {
                                                format!("{}.{}.{}", major, minor, patch)
                                            } else {
                                                format!("{}.{}", major, minor)
                                            });
                                        }

                                        if let Some(patroni) = node_info.patroni {
                                            extended.patroni_version = Some(patroni.version);
                                        }

                                        if let Some(xlog) = node_info.xlog {
                                            extended.xlog_location = xlog.location;
                                            extended.xlog_received_location = xlog.received_location;
                                            extended.xlog_replayed_location = xlog.replayed_location;
                                        }

                                        extended.cluster_unlocked = node_info.cluster_unlocked;
                                    }
                                }

                                members_extended.push(extended);
                            }

                            // Calculate WAL lag for replicas
                            if let Some(leader) = members_extended.iter().find(|m| {
                                let r = m.role.to_lowercase();
                                r == "leader" || r == "master" || r == "primary"
                            }) {
                                if let Some(leader_loc) = leader.xlog_location {
                                    for member in &mut members_extended {
                                        if member.role.to_lowercase() == "replica" || member.role.to_lowercase() == "standby" {
                                            if let Some(replica_loc) = member.xlog_replayed_location.or(member.xlog_received_location) {
                                                if leader_loc >= replica_loc {
                                                    member.wal_lag_bytes = Some(leader_loc - replica_loc);
                                                }
                                            }
                                        }
                                    }
                                }
                            }

                            // Fetch timeline history from first available node
                            let mut history = Vec::new();
                            for member in &cluster.members {
                                let history_url = format!("http://{}:{}/history", member.host, 8008);
                                if let Ok(hist_resp) = client.get(&history_url).send().await {
                                    if let Ok(hist) = hist_resp.json::<Vec<TimelineHistoryEntry>>().await {
                                        history = hist;
                                        break;
                                    }
                                }
                            }

                            // Fetch config from first available node
                            let mut config = None;
                            for member in &cluster.members {
                                let config_url = format!("http://{}:{}/config", member.host, 8008);
                                if let Ok(cfg_resp) = client.get(&config_url).send().await {
                                    if let Ok(cfg) = cfg_resp.json::<PatroniConfig>().await {
                                        config = Some(cfg);
                                        break;
                                    }
                                }
                            }

                            statuses.push(PatroniNodeStatus {
                                url: url.clone(),
                                online: true,
                                error: None,
                                cluster: Some(cluster),
                                members_extended,
                                history,
                                config,
                            });
                        }
                        Err(e) => {
                            tracing::warn!("Failed to parse /cluster response from {}: {}", url, e);
                            tracing::debug!("Response body: {}", body_text);
                            statuses.push(PatroniNodeStatus {
                                url: url.clone(),
                                online: false,
                                error: Some(format!("JSON parse error: {} (body: {})", e,
                                    if body_text.len() > 200 {
                                        format!("{}...", &body_text[..200])
                                    } else {
                                        body_text
                                    })),
                                cluster: None,
                                members_extended: Vec::new(),
                                history: Vec::new(),
                                config: None,
                            });
                        }
                    }
                } else {
                    statuses.push(PatroniNodeStatus {
                        url: url.clone(),
                        online: false,
                        error: Some(format!("HTTP {}", response.status())),
                        cluster: None,
                        members_extended: Vec::new(),
                        history: Vec::new(),
                        config: None,
                    });
                }
            }
            Err(e) => {
                statuses.push(PatroniNodeStatus {
                    url: url.clone(),
                    online: false,
                    error: Some(e.to_string()),
                    cluster: None,
                    members_extended: Vec::new(),
                    history: Vec::new(),
                    config: None,
                });
            }
        }
    }

    Ok(Json(statuses))
}
