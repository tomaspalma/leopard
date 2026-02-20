use message::{DefaultMessageType, Message, MessageType};

pub trait Route {}

pub trait RouteTask {}

pub trait RouteHandler<MType>
where
    MType: MessageType,
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

impl RouteHandler<DefaultMessageType> for DefaultRouteHandler {
    fn handle(&self, message: Box<dyn Message<DefaultMessageType>>) {
        println!("Handling route");
    }

    fn add_route(&self, route: Box<dyn Route>, route_task: Box<dyn RouteTask>) {}
}
