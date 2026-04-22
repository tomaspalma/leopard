#[macro_export]
macro_rules! impl_protocol_message {
    ($message_ty:ty, $self_ident:ident, $body:block) => {
        impl $crate::Message for $message_ty {
            fn as_any(&self) -> &dyn std::any::Any {
                self
            }

            fn get_type(&self) -> std::sync::Arc<dyn $crate::MessageType + Send + Sync> {
                std::sync::Arc::new(self._type.clone())
            }

            fn content(&self) -> std::sync::Arc<Vec<u8>> {
                std::sync::Arc::new(vec![])
            }

            fn protocol(&self) -> Option<u64> {
                self.protocol_id
            }

            fn serialize(&self, protocol: Option<u64>, sender_port: u16) -> Result<Vec<u8>, ()> {
                let $self_ident = self;
                let body_bytes = $body;

                Ok($crate::build_message_packet(
                    protocol,
                    sender_port,
                    body_bytes,
                ))
            }
        }
    };
}