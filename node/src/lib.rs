use protocol::Protocol;

/// The node is the main component of the replication engine
trait Node {
    fn init(
        config: Config, 
        protocol: Vec<Protocol>
    );
}
