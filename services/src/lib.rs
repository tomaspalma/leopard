pub mod http;

use async_trait::async_trait;

#[async_trait]
pub trait NodeService {
    async fn init(&self);
}
