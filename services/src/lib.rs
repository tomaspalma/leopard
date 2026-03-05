use async_trait::async_trait;
use axum::extract::Path;
use axum::{
    Json, Router,
    routing::{delete, get, post},
};
use serde_json::{Value, json};

#[async_trait]
pub trait NodeService {
    async fn init(&self);
}

pub struct NodeHTTPService {}

impl NodeHTTPService {
    pub fn new() -> Self {
        Self {}
    }

    async fn get_handler(Path(key): Path<String>) -> Json<Value> {
        println!("Fetching key: {}", key);
        // Logic to fetch from your actual storage goes here
        Json(json!({ "key": key, "value": "example_value" }))
    }

    // POST /key_name
    async fn post_handler(Path(key): Path<String>, Json(payload): Json<Value>) -> Json<Value> {
        println!("Setting key: {} to value: {}", key, payload);
        // Logic to save to your actual storage goes here
        Json(json!({ "status": "success", "key": format!("/{}", key) }))
    }

    // DELETE /key_name
    async fn delete_handler(Path(key): Path<String>) -> Json<Value> {
        println!("Deleting key: {}", key);
        // Logic to delete from storage goes here
        Json(json!({ "status": "deleted", "key": key }))
    }
}

#[async_trait]
impl NodeService for NodeHTTPService {
    async fn init(&self) {
        let app: Router = Router::new()
            .route("/{key}", get(Self::get_handler))
            .route("/{key}", post(Self::post_handler))
            .route("/{key}", delete(Self::delete_handler));

        let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
        axum::serve(listener, app).await.unwrap();
    }
}
