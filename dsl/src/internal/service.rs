pub trait ServiceReceiver {
    fn push_service(&mut self, config: ServiceConfig);
}

#[derive(Clone)]
pub enum ServiceConfig {
    Http { port: u16 },
    Ws { port: u16 },
}

pub struct ServiceEntry<B: ServiceReceiver> {
    pub(crate) parent: B,
}

pub struct HttpServiceEntry<B: ServiceReceiver> {
    parent: B,
}

pub struct WsServiceEntry<B: ServiceReceiver> {
    parent: B,
}

impl<B: ServiceReceiver> ServiceEntry<B> {
    pub fn http(self) -> HttpServiceEntry<B> {
        HttpServiceEntry { parent: self.parent }
    }

    pub fn ws(self) -> WsServiceEntry<B> {
        WsServiceEntry { parent: self.parent }
    }
}

impl<B: ServiceReceiver> HttpServiceEntry<B> {
    pub fn port(mut self, port: u16) -> B {
        self.parent.push_service(ServiceConfig::Http { port });
        self.parent
    }
}

impl<B: ServiceReceiver> WsServiceEntry<B> {
    pub fn port(mut self, port: u16) -> B {
        self.parent.push_service(ServiceConfig::Ws { port });
        self.parent
    }
}
