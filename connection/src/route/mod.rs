use message::{DefaultMessageType, Message, MessageType};

pub trait Route {
    fn task(&self) -> Box<dyn RouteTask>;
}

pub trait RouteTask {}

pub trait RouterHandlerInfo {
    type MType: MessageType;
    type RStorage: RouteStorage;
}

pub trait RouteStorage {
    fn store(&self, route: Box<dyn Route>);
    fn get(&self, route: Box<dyn Route>) -> Option<Box<dyn Route>>; // explorar a ideia de ter um
    // id em vez de ser a route
    // toda
}

pub struct HashMapRouteStorage {}

impl RouteStorage for HashMapRouteStorage {
    fn store(&self, route: Box<dyn Route>) {}

    fn get(&self, route: Box<dyn Route>) -> Option<Box<dyn Route>> {
        None
    }
}

pub trait RouteHandler<MType, RStorage>
where
    MType: MessageType,
    RStorage: RouteStorage,
{
    fn handle(&self, message: Box<dyn Message<MType>>);
    fn add_route(&self, route: Box<dyn Route>, route_task: Box<dyn RouteTask>);
}

pub struct DefaultRouteHandler {}

impl DefaultRouteHandler {
    pub fn new() -> Self {
        Self {}
    }
}

impl RouteHandler<DefaultMessageType, HashMapRouteStorage> for DefaultRouteHandler {
    fn handle(&self, message: Box<dyn Message<DefaultMessageType>>) {
        println!("Handling route");
    }

    fn add_route(&self, route: Box<dyn Route>, route_task: Box<dyn RouteTask>) {}
}
