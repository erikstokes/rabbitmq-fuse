use lapin_pool::ConnectionBuilder;
use miette::{IntoDiagnostic, Result};
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

#[tokio::test]
async fn test_plain() -> Result<()> {
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

#[tokio::test]
async fn password_file() -> eyre::Result<()> {
    use std::io::Write;
    use tempfile::NamedTempFile;

    let props =
        lapin::ConnectionProperties::default().with_connection_name("Test Connection".into());

    let mut pw_file = NamedTempFile::new()?;
    pw_file.write_all("rabbitpw".as_bytes())?;

    let opener = ConnectionBuilder::new("amqp://127.0.0.1:5672/%2f")
        .with_properties(props)
        .plain_auth("rabbit")
        // The password is in a file so we dont't have to hard-code it.
        .with_password_file(pw_file.path())
        .opener()?;

    let connection = opener.get_connection().await?;
    assert!(connection.status().connected());
    tracing::info!("Connected!");
    let channel = connection.create_channel().await?;
    assert!(channel.status().connected());
    tracing::info!(channel=?channel, "Got channel");

    // The password file is assume to be a single line, trailing newlines are stripped

    let props =
        lapin::ConnectionProperties::default().with_connection_name("Test Connection".into());

    let mut pw_file = NamedTempFile::new()?;
    pw_file.write_all("guest\n".as_bytes())?;

    let _opener = ConnectionBuilder::new("amqp://127.0.0.1:5672/%2f")
        .with_properties(props)
        .plain_auth("guest")
        // The password is in a file so we dont't have to hard-code it.
        .with_password_file(pw_file.path())
        .opener()?;

    Ok(())
}

#[test]
fn mismatched_args() -> eyre::Result<()> {
    let props =
        lapin::ConnectionProperties::default().with_connection_name("Test Connection".into());

    let opener = ConnectionBuilder::new("amqp://127.0.0.1:5672/%2f?auth_mechanism=external")
        .with_properties(props)
        .plain_auth("rabbit")
        .with_password("rabbitpw")
        // .with_ca_pem("../test_all/tls-gen/basic/result/ca_certificate.pem")
        .opener()?;
    assert_eq!(
        opener.uri().query.auth_mechanism.unwrap(),
        lapin::auth::SASLMechanism::Plain
    );
    Ok(())
}

#[cfg(feature = "deadpool")]
mod pool_tests {
    use super::*;
    #[tokio::test]
    async fn test_plain_pool() -> eyre::Result<()> {
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
