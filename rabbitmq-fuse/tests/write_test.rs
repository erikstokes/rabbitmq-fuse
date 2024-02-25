use assert_cmd::prelude::*;
use nix::sys::statfs::FUSE_SUPER_MAGIC;
use std::fs::{FileTimes, OpenOptions};
use std::io::Read;
use tempfile::TempDir;
// Add methods on commands
// Used for writing assertions
use std::process::{Child, Command, Stdio}; // Run programs

use miette::Result;
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

#[rustfmt::skip]
const GLOBAL_OPTIONS: [&str; 2] = [
    // "--max-fuse-requests", "100",
    // "--fuse-write-buffer", "4096",
    "--open-timeout-ms", "1000",
];

#[rustfmt::skip]
const RABBIT_OPTIONS: [&str; 7] = [
    "--amqp-auth", "plain",
    "--amqp-user", "rabbit",
    "--amqp-password", "rabbitpw",
    "--immediate-connection",
];

impl Mount {
    /// Spawn a subcommand running `cargo run -- mount_dir rabbit` mounting to
    /// `temp_dir`. `temp_dir` needs to live until the spawned process
    /// completes. It will connect to a rabbit server given by the environment
    /// variable `$RABBITMQ_URL`, or "amqp://127.0.0.1:5672" if that isn't
    /// set.
    fn spawn<S>(endpoint: &str, options: &[S], endpoint_options: &[S]) -> eyre::Result<Self>
    where
        S: AsRef<str>,
    {
        let amqp_url = std::env::var("RABBITMQ_URL").unwrap_or("amqp://127.0.0.1:5672".to_string());
        let mut mount = Command::cargo_bin("fusegate")?;
        let mount_dir = TempDir::with_prefix("fusegate")?;

        let mut args = GLOBAL_OPTIONS.to_vec();
        let extra_opts: Vec<&str> = options.iter().map(|s| s.as_ref()).collect();
        args.extend_from_slice(&extra_opts);

        args.extend_from_slice(&[mount_dir.path().to_str().expect("Invalid path"), endpoint]);
        if endpoint == "rabbit" {
            args.extend_from_slice(&RABBIT_OPTIONS);
            args.extend_from_slice(&["--rabbit-addr", &amqp_url]);
        }
        let ep_opts: Vec<&str> = endpoint_options.iter().map(|s| s.as_ref()).collect();
        args.extend(&ep_opts);
        dbg!(&args);

        Ok(Self {
            cmd: mount.args(&args).stdout(Stdio::piped()).spawn()?,
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
    let mut proc = Mount::spawn::<&str>("rabbit", &[], &[])?;
    std::thread::sleep(std::time::Duration::from_secs(2));
    let meta = nix::sys::statfs::statfs(proc.mount_dir.path())?;
    assert_eq!(meta.filesystem_type(), FUSE_SUPER_MAGIC);
    println!("unmounting");
    signal::kill(
        Pid::from_raw(proc.as_ref().id().try_into()?),
        Signal::SIGTERM,
    )?;
    assert!(proc.as_mut().wait()?.success());
    let meta = nix::sys::statfs::statfs(proc.mount_dir.path())?;
    assert!(meta.filesystem_type() != FUSE_SUPER_MAGIC);
    Ok(())
}

#[test]
fn mount_test_sigint() -> miette::Result<(), Box<dyn std::error::Error>> {
    let mut proc = Mount::spawn::<&str>("rabbit", &[], &[])?;
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
    let mut proc = Mount::spawn::<&str>("rabbit", &[], &[])?;

    std::thread::sleep(std::time::Duration::from_secs(2));

    let target = proc.mount_dir.path().join("test");
    println!("creating {}", target.display());
    std::fs::create_dir(&target)?;

    assert!(target.is_dir());

    std::fs::remove_dir(&target)?;

    println!("unmounting");
    signal::kill(
        Pid::from_raw(proc.as_ref().id().try_into()?),
        Signal::SIGINT,
    )?;
    assert!(proc.as_mut().wait()?.success());
    Ok(())
}

#[test]
fn mk_rm_file() -> miette::Result<(), Box<dyn std::error::Error>> {
    use std::io::Write;
    let mut proc = Mount::spawn::<&str>("rabbit", &[], &[])?;

    std::thread::sleep(std::time::Duration::from_secs(2));

    let target = proc.mount_dir.path().join("test");
    println!("creating {}", target.display());
    std::fs::create_dir(&target)?;

    assert!(target.is_dir());

    let file = target.join("file.txt");

    let mut fp = std::fs::File::create(&file)?;

    // Can't remove a non-empty directory
    let fail = std::fs::remove_dir(&target);
    assert!(fail.is_err());

    std::fs::remove_file(file)?;
    // we can still write the open file
    assert_eq!(6, fp.write(b"hello\n")?);

    std::fs::remove_dir(&target)?;

    println!("unmounting");
    signal::kill(
        Pid::from_raw(proc.as_ref().id().try_into()?),
        Signal::SIGINT,
    )?;
    assert!(proc.as_mut().wait()?.success());
    Ok(())
}

#[test]
fn nested_mkdir() -> miette::Result<(), Box<dyn std::error::Error>> {
    let mut proc = Mount::spawn::<&str>("rabbit", &[], &[])?;

    std::thread::sleep(std::time::Duration::from_secs(2));

    let target = proc.mount_dir.path().join("test");
    println!("creating {}", target.display());
    std::fs::create_dir(&target)?;

    assert!(target.is_dir());
    // the second level dir should fail
    let target2 = target.join("test2");
    let ret = std::fs::create_dir(target2);
    let err = ret.unwrap_err();
    assert_eq!(err.raw_os_error(), libc::EINVAL.into());

    println!("unmounting");
    signal::kill(
        Pid::from_raw(proc.as_ref().id().try_into()?),
        Signal::SIGINT,
    )?;
    assert!(proc.as_mut().wait()?.success());
    Ok(())
}

#[test]
fn write_lines() -> Result<(), Box<dyn std::error::Error>> {
    use std::io::Write;
    let mut proc = Mount::spawn::<&str>("rabbit", &[], &[])?;

    std::thread::sleep(std::time::Duration::from_secs(2));

    let target = proc.mount_dir.path().join("test");
    println!("creating {}", target.display());
    std::fs::create_dir(&target)?;

    assert!(target.is_dir());

    let mut fp = std::fs::File::create(target.join("file.txt"))?;
    assert!(fp.write(b"hello\n")? == 6);

    // now write a path that doesn't exist to make sure it fails
    let target = proc.mount_dir.path().join("fake");
    println!("creating {}", target.display());
    std::fs::create_dir(&target)?;

    let mut fp = std::fs::File::create(target.join("file.txt"))?;
    assert!(fp.write(b"hello2\n")? == 7);
    // std::thread::sleep(std::time::Duration::from_secs(2));
    let ret = fp.sync_data();
    dbg!(&ret);
    assert!(ret.is_err());

    // write again and fail again, but this time, we catch the error
    // on close. Except rust drops it, but still, it at least
    // shoudln't panic
    assert!(fp.write(b"hello3\n")? == 7);
    std::mem::drop(fp);

    // re-open the file. It should still exist, but be empty. Sleep to
    // make sure the metadata isn't still cached
    std::thread::sleep(std::time::Duration::from_secs(2));
    let fp = std::fs::File::open(target.join("file.txt"))?;
    assert_eq!(fp.metadata()?.len(), 0);

    println!("unmounting");
    signal::kill(
        Pid::from_raw(proc.as_ref().id().try_into()?),
        Signal::SIGINT,
    )?;
    assert!(proc.as_mut().wait()?.success());
    Ok(())
}

#[test]
fn mv_file() -> Result<(), Box<dyn std::error::Error>> {
    use std::io::Write;
    let mut proc = Mount::spawn::<&str>("rabbit", &[], &[])?;
    std::thread::sleep(std::time::Duration::from_secs(2));

    let target = proc.mount_dir.path().join("test");
    println!("creating {}", target.display());
    std::fs::create_dir(&target)?;

    assert!(target.is_dir());
    let file = target.join("file.txt");
    let file2 = target.join("file2.txt");
    let mut fp = std::fs::File::create(&file)?;
    assert!(fp.write(b"hello\n")? == 6);
    fp.sync_all().unwrap();
    std::mem::drop(fp);
    std::fs::rename(file, &file2).unwrap();

    // // After moving, we can still write the open handle
    // assert!(fp.write(b"hello\n")? == 6);

    // // And we can open the new name and write to it too
    // let mut fp = OpenOptions::new().write(true).append(true).open(file2)?;
    // assert!(fp.write(b"hello\n")? == 6);

    println!("unmounting");
    signal::kill(
        Pid::from_raw(proc.as_ref().id().try_into()?),
        Signal::SIGINT,
    )?;
    assert!(proc.as_mut().wait()?.success());
    Ok(())
}

#[test]
fn read_file() -> Result<(), Box<dyn std::error::Error>> {
    use std::io::Write;
    let mut proc = Mount::spawn::<&str>("rabbit", &[], &[])?;
    std::thread::sleep(std::time::Duration::from_secs(2));

    let target = proc.mount_dir.path().join("test");
    println!("creating {}", target.display());
    std::fs::create_dir(&target)?;

    assert!(target.is_dir());
    let file = target.join("file.txt");
    let mut fp = OpenOptions::new()
        .write(true)
        .create(true)
        .read(true)
        .open(file)?;
    assert!(fp.write(b"hello\n")? == 6);
    fp.sync_all().unwrap();

    let mut data = vec![];
    assert_eq!(fp.read(&mut data)?, 0);
    assert!(data.is_empty());

    println!("unmounting");
    signal::kill(
        Pid::from_raw(proc.as_ref().id().try_into()?),
        Signal::SIGINT,
    )?;
    assert!(proc.as_mut().wait()?.success());
    Ok(())
}

#[test]
fn read_dir() -> Result<(), Box<dyn std::error::Error>> {
    use std::io::Write;
    let mut proc = Mount::spawn::<&str>("rabbit", &[], &[])?;
    std::thread::sleep(std::time::Duration::from_secs(2));

    let target = proc.mount_dir.path().join("test");
    println!("creating {}", target.display());
    std::fs::create_dir(&target)?;

    assert!(target.is_dir());
    let file = target.join("file.txt");
    let mut fp = OpenOptions::new()
        .write(true)
        .create(true)
        .read(true)
        .open(file)?;
    assert!(fp.write(b"hello\n")? == 6);
    fp.sync_all().unwrap();

    let dir = std::fs::read_dir(target)?;
    for ent in dir {
        assert!(ent?.file_type()?.is_file());
    }

    println!("unmounting");
    signal::kill(
        Pid::from_raw(proc.as_ref().id().try_into()?),
        Signal::SIGINT,
    )?;
    assert!(proc.as_mut().wait()?.success());
    Ok(())
}

#[test]
fn get_set_attr() -> Result<(), Box<dyn std::error::Error>> {
    let mut proc = Mount::spawn::<&str>("rabbit", &[], &[])?;
    std::thread::sleep(std::time::Duration::from_secs(2));

    let target = proc.mount_dir.path().join("test");
    println!("creating {}", target.display());
    std::fs::create_dir(&target)?;

    assert!(target.is_dir());
    let file = target.join("file.txt");
    let fp = std::fs::File::create(file)?;

    let now = std::time::SystemTime::now();
    let times = FileTimes::new().set_modified(now).set_accessed(now);
    fp.set_times(times)?;
    let attr = fp.metadata()?;
    assert_eq!(attr.modified()?, now);
    assert_eq!(attr.accessed()?, now);

    println!("unmounting");
    signal::kill(
        Pid::from_raw(proc.as_ref().id().try_into()?),
        Signal::SIGINT,
    )?;
    assert!(proc.as_mut().wait()?.success());
    Ok(())
}

#[test]
fn write_json() -> Result<(), Box<dyn std::error::Error>> {
    use std::io::Write;
    let mut proc = Mount::spawn::<&str>("rabbit", &[], &["--publish-in", "header"])?;

    std::thread::sleep(std::time::Duration::from_secs(2));

    let target = proc.mount_dir.path().join("test");
    println!("creating {}", target.display());
    std::fs::create_dir(&target)?;

    assert!(target.is_dir());

    let mut fp = std::fs::File::create(target.join("file.json"))?;
    assert!(fp.write(br#"{"stuff": {"a": 1, "b": "hello", "c": [123456789, "test"]}}"#)? != 0);
    assert!(fp.write(b"\n")? == 1);
    assert!(fp.sync_data().is_ok());
    // invalid json should fail to write. Parse failures are immediate
    assert!(fp.write(b"{can'tparseme\n").is_err());
    std::mem::drop(fp);
    println!("unmounting");
    signal::kill(
        Pid::from_raw(proc.as_ref().id().try_into()?),
        Signal::SIGINT,
    )?;
    assert!(proc.as_mut().wait()?.success());
    Ok(())
}

#[test]
fn write_stream() -> Result<(), Box<dyn std::error::Error>> {
    use std::io::Write;
    let mut proc = Mount::spawn::<&str>("stream", &[], &[])?;

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

#[test]
fn daemon_fail() -> eyre::Result<()> {
    let mut mount = Command::cargo_bin("fusegate")?;
    // The child process will error because this isn't a directory
    let mut proc = mount.args(["--daemon", "/dev/null", "stream"]).spawn()?;
    let ret = proc.wait()?;
    assert!(!ret.success());

    Ok(())
}

#[test]
fn daemon_spawn() -> Result<(), Box<dyn std::error::Error>> {
    use std::io::Read;
    use std::io::Write;

    let mut proc = Mount::spawn("stream", &["--daemon"], &[])?;
    let ret = proc.as_mut().wait()?;
    assert!(ret.success());
    let mut out = String::new();
    proc.as_mut()
        .stdout
        .take()
        .unwrap()
        .read_to_string(&mut out)?;
    let pid: i32 = out.strip_suffix('\n').unwrap().parse()?;
    let pid = nix::unistd::Pid::from_raw(pid);

    // the daemon should be running, try to write to it
    let target = proc.mount_dir.path().join("test");
    println!("creating {}", target.display());
    std::fs::create_dir(&target)?;

    assert!(target.is_dir());

    let mut fp = std::fs::File::create(target.join("file.txt"))?;
    assert!(fp.write(b"hello\n")? == 6);

    signal::kill(pid, Signal::SIGINT)?;
    Ok(())
}

#[test]
fn logfile() -> eyre::Result<()> {
    use std::io::Write;
    let logfile = tempfile::NamedTempFile::new()?;
    let mut mount = Mount::spawn(
        "stream",
        &[],
        &["--logfile", logfile.path().to_str().unwrap()],
    )?;
    std::thread::sleep(std::time::Duration::from_secs(2));

    let target = mount.mount_dir.path().join("test");
    println!("creating {}", target.display());
    std::fs::create_dir(&target)?;

    assert!(target.is_dir());

    let mut fp = std::fs::File::create(target.join("file.txt"))?;
    assert!(fp.write(b"hello\n")? == 6);

    signal::kill(
        Pid::from_raw(mount.as_ref().id().try_into()?),
        Signal::SIGINT,
    )?;
    let ret = mount.as_mut().wait()?;
    assert!(ret.success());
    // let logfile = logfile.persist("/tmp/logfile.log")?;
    let logfile = logfile.into_file();
    assert!(logfile.metadata()?.len() > 0);

    Ok(())
}
