//! Wrapper for [`deadpool`] managed connection pool. This is a pool
//! that opens connections to a single server with a fixed
//! configuration. Unlike the [`deadpool-lapin`] crate, this allows
//! the use of TLS and EXTERNAL authetnication

use deadpool::{
    async_trait,
    managed::{self, Metrics},
};
use tracing::info;

use crate::connection::Opener;

/// Result of returning a connection to the pool
type RecycleResult = managed::RecycleResult<lapin::Error>;
/// Error returning the connection to the pool
type RecycleError = managed::RecycleError<lapin::Error>;

#[async_trait]
impl managed::Manager for Opener {
    type Type = lapin::Connection;
    type Error = lapin::Error;

    async fn create(&self) -> lapin::Result<Self::Type> {
        info!("Opening new connection");
        self.get_connection().await
    }

    // copypasta from https://github.com/bikeshedder/deadpool/blob/d7167eaf47ccaadabfb831ce3718cdebe51185ba/lapin/src/lib.rs#L91
    async fn recycle(&self, conn: &mut lapin::Connection, _metrics: &Metrics) -> RecycleResult {
        match conn.status().state() {
            lapin::ConnectionState::Connected => Ok(()),
            other_state => Err(RecycleError::Message(
                format!("lapin connection is in state: {:?}", other_state).into(),
            )),
        }
    }
}

/// Pool of `RabbitMQ` connections. Connections will be lazily
/// re-opened when closed as needed
pub type ConnectionPool = managed::Pool<Opener>;
