use async_trait::async_trait;
use axum::extract::{FromRef, MatchedPath, Path, Request, State};
use axum::http::Method;
use axum::middleware::{self, Next};
use axum::response::Response;
use axum::{
    Json, Router,
    routing::{delete, get, post},
};
use connection::node::port::NodeAddress;
use serde::Deserialize;
use serde_json::{Value, json};
use state::node::{DefaultNodeState, NodeState};
use state::storage::{item::DefaultDataStateItem, state::DataState};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, RwLock};
use tracing::info;

use crate::NodeService;

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;
pub type ActionCallback = Arc<dyn Fn(Option<axum::body::Bytes>) -> BoxFuture<'static, ()> + Send + Sync>;

#[derive(Clone)]
pub struct AfterRequestAction {
    pub method: Method,
    pub path: String,
    pub action: ActionCallback,
}

#[derive(Clone)]
struct AppState {
    data: Arc<dyn DataState + Send + Sync>,
    actions: Arc<Vec<AfterRequestAction>>,
}

impl FromRef<AppState> for Arc<dyn DataState + Send + Sync> {
    fn from_ref(input: &AppState) -> Self {
        input.data.clone()
    }
}

impl FromRef<AppState> for Arc<Vec<AfterRequestAction>> {
    fn from_ref(input: &AppState) -> Self {
        input.actions.clone()
    }
}

pub struct NodeHTTPService {
    address: NodeAddress,
    state: Arc<DefaultNodeState>,
    after_request_actions: RwLock<Vec<AfterRequestAction>>,
}

#[derive(Deserialize)]
pub struct PostRequestPayload {
    value: String,
}

impl NodeHTTPService {
    pub fn new(address: NodeAddress, state: Arc<DefaultNodeState>) -> Self {
        Self {
            address,
            state,
            after_request_actions: std::sync::RwLock::new(Vec::new()),
        }
    }

    pub fn add_after_request_action(&self, method: Method, path: &str, action: ActionCallback) {
        self.after_request_actions
            .write()
            .unwrap()
            .push(AfterRequestAction {
                method,
                path: path.to_string(),
                action,
            });
    }

    async fn after_request_middleware(
        State(actions): State<Arc<Vec<AfterRequestAction>>>,
        req: Request,
        next: Next,
    ) -> Response {
        let req_method = req.method().clone();
        let matched_path = req
            .extensions()
            .get::<MatchedPath>()
            .map(|m| m.as_str().to_string())
            .unwrap_or_else(|| req.uri().path().to_string());

        let (parts, body) = req.into_parts();
        let bytes = axum::body::to_bytes(body, usize::MAX).await.unwrap_or_default();
        let req = Request::from_parts(parts, axum::body::Body::from(bytes.clone()));

        let res = next.run(req).await;

        for action in actions.as_ref() {
            if action.method == req_method && action.path == matched_path {
                let action_bytes = if bytes.is_empty() { None } else { Some(bytes.clone()) };
                (action.action)(action_bytes).await;
            }
        }

        res
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
        State(_state): State<Arc<dyn DataState + Send + Sync>>,
        Path(key): Path<String>,
    ) -> Json<Value> {
        info!("Deleting key: {}", key);
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

        let actions = Arc::new(self.after_request_actions.read().unwrap().clone());
        let app_state = AppState { data, actions };

        let app: Router = Router::new()
            .route("/{key}", get(Self::get_handler))
            .route("/{key}", post(Self::post_handler))
            .route("/{key}", delete(Self::delete_handler))
            .layer(middleware::from_fn_with_state(
                app_state.clone(),
                Self::after_request_middleware,
            ))
            .with_state(app_state);

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
