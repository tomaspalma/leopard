use std::sync::Arc;

use async_trait::async_trait;
use connection::node::default::{
    DefaultNodeSocketTask, DefaultNodeSocketTaskMetadata, PeriodicDefaultNodeSocketTask,
};
use connection::node::port::NodeAddress;
use connection::request::handler::default::{TestMessage, TestMessageType};
use connection::route::default::{DefaultRouteHandler, HashMapRouteStorage};
use membership::{
    DefaultMembership, DefaultMembershipNeighbor, DefaultMembershipNeighborRepresentation,
};
use message::Message;
use protocol::{Protocol, ProtocolIDGenerator, deserializer::ProtocolDeserializer};
use runtime::time::TokioPeriodTimeUnit;
use state::node::DefaultNodeState;

pub struct DefaultMembershipProtocol {
    id: u64,
    deserializer: Arc<DefaultMembershipProtocolDeserializer>,
}

impl DefaultMembershipProtocol {
    pub fn new() -> Self {
        Self {
            id: ProtocolIDGenerator::generate(),
            deserializer: Arc::new(DefaultMembershipProtocolDeserializer::new()),
        }
    }
}

pub struct DefaultMembershipProtocolDeserializer {}

impl DefaultMembershipProtocolDeserializer {
    pub fn new() -> Self {
        Self {}
    }
}

impl ProtocolDeserializer for DefaultMembershipProtocolDeserializer {
    fn deserialize(&self, bytes: Vec<u8>) -> Arc<dyn Message + Send + Sync> {
        Arc::new(TestMessage::new(Arc::new(TestMessageType::new()), None))
    }
}

#[async_trait]
impl
    Protocol<
        DefaultNodeState,
        DefaultNodeSocketTask,
        DefaultNodeSocketTaskMetadata,
        DefaultMembershipNeighborRepresentation<DefaultMembershipNeighbor>,
        DefaultMembership,
        DefaultMembershipNeighbor,
        NodeAddress,
        NodeAddress,
        TokioPeriodTimeUnit,
        PeriodicDefaultNodeSocketTask,
        DefaultRouteHandler,
        HashMapRouteStorage,
    > for DefaultMembershipProtocol
{
    fn deserializer(&self) -> Arc<dyn ProtocolDeserializer> {
        self.deserializer.clone()
    }

    fn deserialize_message(&self, bytes: Vec<u8>) -> Arc<dyn Message> {
        self.deserializer().deserialize(bytes)
    }

    fn id(&self) -> u64 {
        self.id
    }
    async fn init(&mut self) {}
}
