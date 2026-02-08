use thiserror::Error;

#[derive(Error, Debug)]
pub enum NodeInitError {
    #[error("IO error: {0}")]
    IO(#[from] std::io::Error),
    #[error("Socket error")]
    SocketDoesNotExist(),
}
