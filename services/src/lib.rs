use async_trait::async_trait;
use axum::extract::{Path, State};
use axum::{
    Json, Router,
    routing::{delete, get, post},
};
use connection::node::port::NodeAddress;
use serde_json::{Value, json};
use state::node::{DefaultNodeState, NodeState};
use std::sync::Arc;

#[async_trait]
pub trait NodeService {
    async fn init(&self);
}

pub struct NodeHTTPService {
    address: NodeAddress,
    state: Arc<DefaultNodeState>,
}

impl NodeHTTPService {
    pub fn new(address: NodeAddress, state: Arc<DefaultNodeState>) -> Self {
        Self { address, state }
    }

    async fn get_handler(
        State(state): State<Arc<DefaultNodeState>>,
        Path(key): Path<String>,
    ) -> Json<Value> {
        // state.data().

        println!("Fetching key: {}", key);
        Json(json!({ "key": key, "value": "example_value" }))
    }

    async fn post_handler(
        State(state): State<Arc<DefaultNodeState>>,
        Path(key): Path<String>,
        Json(payload): Json<Value>,
    ) -> Json<Value> {
        println!("Setting key: {} to value: {}", key, payload);
        Json(json!({ "status": "success", "key": format!("/{}", key) }))
    }

    async fn delete_handler(
        State(state): State<Arc<DefaultNodeState>>,
        Path(key): Path<String>,
    ) -> Json<Value> {
        println!("Deleting key: {}", key);
        Json(json!({ "status": "deleted", "key": key }))
    }
}

#[async_trait]
impl NodeService for NodeHTTPService {
    async fn init(&self) {
        let app: Router = Router::new()
            .route("/{key}", get(Self::get_handler))
            .route("/{key}", post(Self::post_handler))
            .route("/{key}", delete(Self::delete_handler))
            .with_state(self.state.clone());

        let listener = tokio::net::TcpListener::bind(format!(
            "{}:{}",
            self.address.host(),
            self.address.port()
        ))
        .await
        .unwrap();

        axum::serve(listener, app).await.unwrap();
    }
}
