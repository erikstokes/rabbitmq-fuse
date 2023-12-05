use assert_cmd::prelude::*;
use tempdir::TempDir; // Add methods on commands
                      // Used for writing assertions
use std::process::{Child, Command}; // Run programs

use miette::{IntoDiagnostic, Result};
use nix::sys::signal::{self, Signal};
use nix::unistd::Pid;

/// A `fusegate` mount command at the given mount
struct Mount {
    /// Subprocess running `fusegate`
    cmd: Child,
    /// Temporary mount direcory. This will be dropped when the mount
    /// it dropped, not when the child process completes.
    mount_dir: TempDir,
}

impl Mount {
    /// Spawn a subcommand running `cargo run -- mount_dir rabbit` mounting to
    /// `temp_dir`. `temp_dir` needs to live until the spawned process
    /// completes. It will connect to a rabbit server given by the environment
    /// variable `$RABBITMQ_URL`, or "amqp://127.0.0.1:5672" if that isn't
    /// set.
    #[rustfmt::skip]
    fn spawn() -> miette::Result<Self> {
        let amqp_url = std::env::var("RABBITMQ_URL").unwrap_or("amqp://127.0.0.1:5672".to_string());
        let mut mount = Command::cargo_bin("fusegate").into_diagnostic()?;
        let mount_dir = TempDir::new("fusegate").into_diagnostic()?;

        Ok(Self {
            cmd: mount
                .args([
                    mount_dir.path().to_str().expect("Invalid path"),
                    "rabbit",
                    // "--ca-cert",
                    // "../rabbitmq_ssl/tls-gen/basic/result/ca_certificate.pem",
                    // "--key",
                    // "../rabbitmq_ssl/tls-gen/basic/client/keycert.p12",
                    // "--password",
                    // "bunnies",
                    "--amqp-auth", "plain",
                    "--amqp-user", "rabbit",
                    "--amqp-password", "rabbitpw",
                    "--rabbit-addr", &amqp_url,
                ],
                )
                .spawn()
                .into_diagnostic()?,
            mount_dir,
        })
    }
}

impl AsRef<Child> for Mount {
    fn as_ref(&self) -> &Child {
        &self.cmd
    }
}

impl AsMut<Child> for Mount {
    fn as_mut(&mut self) -> &mut Child {
        &mut self.cmd
    }
}

#[test]
fn mount_test_sigterm() -> miette::Result<(), Box<dyn std::error::Error>> {
    let mut proc = Mount::spawn()?;
    std::thread::sleep(std::time::Duration::from_secs(2));
    println!("unmounting");
    signal::kill(
        Pid::from_raw(proc.as_ref().id().try_into()?),
        Signal::SIGTERM,
    )?;
    assert!(proc.as_mut().wait()?.success());
    Ok(())
}

#[test]
fn mount_test_sigint() -> miette::Result<(), Box<dyn std::error::Error>> {
    let mut proc = Mount::spawn()?;
    std::thread::sleep(std::time::Duration::from_secs(2));
    println!("unmounting");
    // Command::new("fusermout")
    //     .arg("-u")
    //     .arg(mount_dir.path().to_str().unwrap())
    //     .spawn()?.wait()?;
    // mount.assert().success();
    signal::kill(
        Pid::from_raw(proc.as_ref().id().try_into()?),
        Signal::SIGINT,
    )?;
    assert!(proc.as_mut().wait()?.success());
    Ok(())
}

#[test]
fn mkdir() -> miette::Result<(), Box<dyn std::error::Error>> {
    let mut proc = Mount::spawn()?;

    std::thread::sleep(std::time::Duration::from_secs(2));

    let target = proc.mount_dir.path().join("test");
    println!("creating {}", target.display());
    std::fs::create_dir(&target)?;

    assert!(target.is_dir());

    println!("unmounting");
    signal::kill(
        Pid::from_raw(proc.as_ref().id().try_into()?),
        Signal::SIGINT,
    )?;
    assert!(proc.as_mut().wait()?.success());
    Ok(())
}

#[test]
fn write() -> Result<(), Box<dyn std::error::Error>> {
    use std::io::Write;
    let mut proc = Mount::spawn()?;

    std::thread::sleep(std::time::Duration::from_secs(2));

    let target = proc.mount_dir.path().join("test");
    println!("creating {}", target.display());
    std::fs::create_dir(&target)?;

    assert!(target.is_dir());

    let mut fp = std::fs::File::create(target.join("file.txt"))?;
    assert!(fp.write(b"hello\n")? == 6);

    println!("unmounting");
    signal::kill(
        Pid::from_raw(proc.as_ref().id().try_into()?),
        Signal::SIGINT,
    )?;
    assert!(proc.as_mut().wait()?.success());
    Ok(())
}
