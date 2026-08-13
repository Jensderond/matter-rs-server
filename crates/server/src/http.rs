use std::sync::Arc;

use axum::extract::State;
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};

use matter_rs_controller::api::Controller;

#[derive(Clone)]
pub struct AppState {
    pub controller: Arc<dyn Controller>,
    pub shutdown: tokio::sync::watch::Receiver<bool>,
}

pub fn build_router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health).fallback(method_not_allowed))
        .route("/ws", get(crate::ws::ws_upgrade))
        .with_state(state)
}

async fn health(State(state): State<AppState>) -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "version": env!("CARGO_PKG_VERSION"),
        "node_count": state.controller.node_count(),
    }))
}

async fn method_not_allowed() -> Response {
    (StatusCode::METHOD_NOT_ALLOWED, [(header::ALLOW, "GET")], "").into_response()
}
