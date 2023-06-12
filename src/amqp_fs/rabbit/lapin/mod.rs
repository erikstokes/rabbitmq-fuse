mod connection;
pub mod endpoint;

pub use endpoint::RabbitExchnage;
pub(crate) use endpoint::RabbitPublisher;

#[cfg(feature = "lapin-hack")]
fn lapin_has_hack() {
    #![allow(dead_code)]
    assert!(
        !lapin::has_hack(),
        "Requested patched lapin, but did not find the patch"
    );
}