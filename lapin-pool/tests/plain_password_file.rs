use std::io::Write;

use lapin_pool::ConnectionBuilder;
use tempfile::NamedTempFile;
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

#[tokio::test]
async fn main() -> eyre::Result<()> {
    // Enable logging based on the RUST_LOG environment variable
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::from_default_env())
        .init();

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
