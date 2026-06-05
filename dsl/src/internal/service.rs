#[derive(Clone)]
pub enum ServiceConfig {
    Http { port: u16 },
    Ws { port: u16 },
}

enum ServiceKind {
    Http,
    Ws,
}

/// Builder for a single node service (HTTP / WebSocket). Construct a kind with
/// [`ServiceBuilder::http`] or [`ServiceBuilder::ws`], set its fields, and hand
/// it to [`crate::NodeBuilder::service`]; the node builder resolves it into a
/// [`ServiceConfig`] via [`ServiceBuilder::build`] when the node is built.
pub struct ServiceBuilder {
    kind: ServiceKind,
    port: Option<u16>,
}

impl ServiceBuilder {
    pub fn http() -> Self {
        Self {
            kind: ServiceKind::Http,
            port: None,
        }
    }

    pub fn ws() -> Self {
        Self {
            kind: ServiceKind::Ws,
            port: None,
        }
    }

    pub fn port(mut self, port: u16) -> Self {
        self.port = Some(port);
        self
    }

    pub fn build(self) -> Result<ServiceConfig, String> {
        let port = self
            .port
            .ok_or_else(|| "service requires a port".to_string())?;
        Ok(match self.kind {
            ServiceKind::Http => ServiceConfig::Http { port },
            ServiceKind::Ws => ServiceConfig::Ws { port },
        })
    }
}
