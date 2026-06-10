pub mod deserializer;

use std::sync::Arc;

use async_trait::async_trait;

use message::Message;
use rand::RngExt;

use connection::node::{NodeSocketTaskMetadata, PeriodicNodeSocketTask, port::ConnectionInfo};
use connection::route::{RouteHandler, RouteStorage, RouteTask};
use membership::{Membership, MembershipNeighbor, MembershipNeighbors};
use runtime::time::PeriodTimeUnit;
use state::node::NodeState;

use crate::deserializer::ProtocolDeserializer;

/// Wire identifier assigned to each protocol. The discriminant is serialized
/// as the first 8 bytes of every message and forms part of the routing key
/// (`NodeSocketRouteId`), so values must stay stable across versions.
/// Keeping every ID in this enum makes the compiler reject duplicates (E0081).
/// 0 is reserved: deserializers treat a zero tag as "no protocol".
#[repr(u64)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ProtocolId {
    Riblt = 1,
    MerkleTree = 2,
    RbfRiblt = 3,
    RfRiblt = 4,
    Replication = 5,
}

pub struct ProtocolIDGenerator {}

impl ProtocolIDGenerator {
    pub fn generate() -> u64 {
        let mut rng = rand::rng();

        rng.random()
    }
}

#[async_trait]
pub trait Protocol<S, T, M, R, N, MN, CI, CV, PTU, PT, RHandler, RStorage>
where
    S: NodeState,
    T: RouteTask,
    M: NodeSocketTaskMetadata,
    R: MembershipNeighbors<MN>,
    N: Membership<R, MN>,
    MN: MembershipNeighbor + Send + Sync,
    CI: ConnectionInfo<CV>,
    CV: Sized,
    PTU: PeriodTimeUnit + Send + Sync,
    PT: PeriodicNodeSocketTask<PTU>,
    RHandler: RouteHandler + Send + Sync,
    RStorage: RouteStorage,
{
    fn deserializer(&self) -> Arc<dyn ProtocolDeserializer>;
    fn deserialize_message(&self, bytes: Vec<u8>) -> Arc<dyn Message>;

    fn id(&self) -> u64;
    async fn init(&mut self);
}
