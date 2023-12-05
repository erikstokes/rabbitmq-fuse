use lapin_pool::ConnectionBuilder;
use miette::{IntoDiagnostic, Result};
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

#[tokio::test]
async fn main() -> Result<()> {
    // Enable logging based on the RUST_LOG environment variable
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::from_default_env())
        .init();

    let props =
        lapin::ConnectionProperties::default().with_connection_name("Test Connection".into());

    let opener = ConnectionBuilder::new("amqp://127.0.0.1:5672/%2f")
        .with_properties(props)
        .plain_auth("rabbit")
        .with_password("rabbitpw")
        // .with_ca_pem("../test_all/tls-gen/basic/result/ca_certificate.pem")
        .opener()?;

    let connection = opener.get_connection().await.into_diagnostic()?;
    assert!(connection.status().connected());
    tracing::info!("Connected!");
    let channel = connection.create_channel().await.into_diagnostic()?;
    assert!(channel.status().connected());
    tracing::info!(channel=?channel, "Got channel");
    Ok(())
}

#[cfg(feature = "deadpool")]
mod pool_tests {
    use super::*;
    use deadpool::managed::Manager;
    #[tokio::test]
    async fn pool() -> eyre::Result<()> {
        let props = lapin::ConnectionProperties::default()
            .with_connection_name("Pool Test Connection".into());

        let pool = ConnectionBuilder::new("amqp://127.0.0.1:5672/%2f")
            .with_properties(props)
            .plain_auth("rabbit")
            .with_password("rabbitpw")
            .pool()?
            .build()?;
        let conn = pool.get().await?;
        let chan = conn.create_channel().await?;
        assert!(chan.status().connected());
        std::mem::drop(conn);
        // when the connection is open, we get it back
        let conn = pool.get().await?;
        // if it's closed, we get a new one
        conn.close(0, "Closing test connection").await?;
        std::mem::drop(conn);
        let conn = pool.get().await?;
        let chan = conn.create_channel().await?;
        assert!(chan.status().connected());
        Ok(())
    }
}
