/// The node is the main component of the replication engine
/// It works as a repository of algorithms and spawns them
/// - Network layer
/// - Storage layer
/// - Replication mechanism
trait Node {
    fn init(
        config: Config, 
        network: NetworkLayer,
        membership: MembershipLayer,
        replication: ReplicationLayer,
        storage: StorageLayer
    );
}
