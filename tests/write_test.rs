use assert_cmd::prelude::*; // Add methods on commands
                            // Used for writing assertions
use std::process::Command; // Run programs

use nix::sys::signal::{self, Signal};
use nix::unistd::Pid;

#[test]
fn mount_test_sigterm() -> Result<(), Box<dyn std::error::Error>> {
    let mut mount = Command::cargo_bin("rabbitmq-fuse")?;
    let mount_dir = tempdir::TempDir::new("rabbit_fuse")?;

    let mut proc = mount
        .args([
            "--ca-cert",
            "../rabbitmq_ssl/tls-gen/basic/result/ca_certificate.pem",
            "--key",
            "../rabbitmq_ssl/tls-gen/basic/client/keycert.p12",
            "--password",
            "bunnies",
            mount_dir.path().to_str().unwrap(),
        ])
        .spawn()?;
    std::thread::sleep(std::time::Duration::from_secs(2));
    println!("unmounting");
    // Command::new("fusermout")
    //     .arg("-u")
    //     .arg(mount_dir.path().to_str().unwrap())
    //     .spawn()?.wait()?;
    // mount.assert().success();
    signal::kill(Pid::from_raw(proc.id().try_into()?), Signal::SIGTERM)?;
    assert!(proc.wait()?.success());
    Ok(())
}

#[test]
fn mount_test_sigint() -> Result<(), Box<dyn std::error::Error>> {
    let mut mount = Command::cargo_bin("rabbitmq-fuse")?;
    let mount_dir = tempdir::TempDir::new("rabbit_fuse")?;

    let mut proc = mount
        .args([
            "--ca-cert",
            "../rabbitmq_ssl/tls-gen/basic/result/ca_certificate.pem",
            "--key",
            "../rabbitmq_ssl/tls-gen/basic/client/keycert.p12",
            "--password",
            "bunnies",
            mount_dir.path().to_str().unwrap(),
        ])
        .spawn()?;
    std::thread::sleep(std::time::Duration::from_secs(2));
    println!("unmounting");
    // Command::new("fusermout")
    //     .arg("-u")
    //     .arg(mount_dir.path().to_str().unwrap())
    //     .spawn()?.wait()?;
    // mount.assert().success();
    signal::kill(Pid::from_raw(proc.id().try_into()?), Signal::SIGINT)?;
    assert!(proc.wait()?.success());
    Ok(())
}

#[test]
fn mkdir() -> Result<(), Box<dyn std::error::Error>> {
    let mut mount = Command::cargo_bin("rabbitmq-fuse")?;
    let mount_dir = tempdir::TempDir::new("rabbit_fuse")?;

    let mut proc = mount
        .args([
            "--ca-cert",
            "../rabbitmq_ssl/tls-gen/basic/result/ca_certificate.pem",
            "--key",
            "../rabbitmq_ssl/tls-gen/basic/client/keycert.p12",
            "--password",
            "bunnies",
            mount_dir.path().to_str().unwrap(),
        ])
        .spawn()?;

    std::thread::sleep(std::time::Duration::from_secs(2));

    let target = mount_dir.into_path().join("test");
    println!("creating {}", target.display());
    std::fs::create_dir(&target)?;

    assert!(target.is_dir());

    println!("unmounting");
    signal::kill(Pid::from_raw(proc.id().try_into()?), Signal::SIGINT)?;
    assert!(proc.wait()?.success());
    Ok(())
}

#[test]
fn write() -> Result<(), Box<dyn std::error::Error>> {
    use std::io::Write;
    let mut mount = Command::cargo_bin("rabbitmq-fuse")?;
    let mount_dir = tempdir::TempDir::new("rabbit_fuse")?;

    let mut proc = mount
        .args([
            "--ca-cert",
            "../rabbitmq_ssl/tls-gen/basic/result/ca_certificate.pem",
            "--key",
            "../rabbitmq_ssl/tls-gen/basic/client/keycert.p12",
            "--password",
            "bunnies",
            mount_dir.path().to_str().unwrap(),
        ])
        .spawn()?;

    std::thread::sleep(std::time::Duration::from_secs(2));

    let target = mount_dir.into_path().join("test");
    println!("creating {}", target.display());
    std::fs::create_dir(&target)?;

    assert!(target.is_dir());

    let mut fp = std::fs::File::create(target.join("file.txt"))?;
    assert!(fp.write(b"hello\n")? == 6);

    println!("unmounting");
    signal::kill(Pid::from_raw(proc.id().try_into()?), Signal::SIGINT)?;
    assert!(proc.wait()?.success());
    Ok(())
}
