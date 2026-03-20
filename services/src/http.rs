use async_trait::async_trait;
use axum::extract::{Path, State};
use axum::{
    Json, Router,
    routing::{delete, get, post},
};
use connection::node::port::NodeAddress;
use serde::Deserialize;
use serde_json::{Value, json};
use state::node::{DefaultNodeState, NodeState};
use state::storage::{
    item::DefaultDataStateItem,
    state::{DataState, DefaultDataState},
};
use std::sync::Arc;

use crate::NodeService;

pub struct NodeHTTPService {
    address: NodeAddress,
    state: Arc<DefaultNodeState>,
}

#[derive(Deserialize)]
pub struct PostRequestPayload {
    value: String,
}

impl NodeHTTPService {
    pub fn new(address: NodeAddress, state: Arc<DefaultNodeState>) -> Self {
        Self { address, state }
    }

    async fn get_handler(
        State(state): State<Arc<dyn DataState + Send + Sync>>,
        Path(key): Path<String>,
    ) -> Json<Value> {
        match state.get(&key).await {
            Some(value) => Json(json!({ "key": key, "value": value.value() })),
            None => Json(json!({ "error": "Key not found" })),
        }
    }

    async fn post_handler(
        State(state): State<Arc<dyn DataState + Send + Sync>>,
        Path(key): Path<String>,
        Json(payload): Json<PostRequestPayload>,
    ) -> Json<Value> {
        state
            .store(Box::new(DefaultDataStateItem::new(
                key.clone(),
                payload.value.clone(),
            )))
            .await;

        Json(json!({"key": key, "value": payload.value}))
    }

    async fn delete_handler(
        State(state): State<Arc<dyn DataState + Send + Sync>>,
        Path(key): Path<String>,
    ) -> Json<Value> {
        println!("Deleting key: {}", key);
        Json(json!({ "status": "deleted", "key": key }))
    }
}

#[async_trait]
impl NodeService for NodeHTTPService {
    async fn init(&self) {
        let data = self
            .state
            .get_storage("default".to_string())
            .unwrap()
            .clone();

        let app: Router = Router::new()
            .route("/{key}", get(Self::get_handler))
            .route("/{key}", post(Self::post_handler))
            .route("/{key}", delete(Self::delete_handler))
            .with_state(data);

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
