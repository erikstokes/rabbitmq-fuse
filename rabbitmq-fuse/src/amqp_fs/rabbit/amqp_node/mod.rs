use async_trait::async_trait;

use crate::amqp_fs::publisher::Endpoint;
use crate::amqp_fs::publisher::Publisher;
use crate::cli::EndpointCommand;

use super::lapin::endpoint::RabbitExchnage;
use super::lapin::endpoint::RabbitPublisher;
use super::RabbitCommand;

/// Special endpoint for message that will be recieved by amqp-node
#[derive(Debug)]
pub struct NodeEndpoint(RabbitExchnage);

/// Publisher that will emit headers able to be consumed by node-amqp
/// endpoints. Otherwise this is the same is [`RabbitPublisher`]
#[derive(Debug)]
pub struct NodePublisher(RabbitPublisher);

#[async_trait]
impl Publisher for NodePublisher {
    async fn wait_for_confirms(&self) -> Result<(), crate::amqp_fs::descriptor::WriteError> {
        self.0.wait_for_confirms().await
    }

    async fn basic_publish(
        &self,
        line: &[u8],
        force_sync: bool,
    ) -> Result<usize, crate::amqp_fs::descriptor::WriteError> {
        super::lapin::endpoint::basic_publish::<headers::NodeFieldTable>(&self.0, line, force_sync)
            .await
    }
}

#[async_trait]
impl Endpoint for NodeEndpoint {
    type Publisher = NodePublisher;

    type Options = Command;

    async fn open(
        &self,
        path: &std::path::Path,
        flags: u32,
    ) -> Result<Self::Publisher, crate::amqp_fs::descriptor::WriteError> {
        self.0.open(path, flags).await.map(NodePublisher)
    }
}

/// Newtype wrapper to create a second rabbit command with the same
/// arguments
#[derive(Debug, clap::Args)]
pub struct Command {
    /// Options to control publishing to RabbitMQ
    #[clap(flatten)]
    args: RabbitCommand,
}

impl EndpointCommand for Command {
    type Endpoint = NodeEndpoint;

    fn as_endpoint(&self) -> miette::Result<NodeEndpoint> {
        self.args.as_endpoint().map(NodeEndpoint)
    }
}

#[cfg(feature = "lapin_endpoint")]
/// AMQP header type that will never emit the `B`, `u`, `i` or `_`
/// types since amqp-node can't read those
mod headers {
    use serde::{Deserialize, Serialize};

    use lapin::types::FieldArray;
    use lapin::types::{AMQPValue, ByteArray};
    use lapin_pool::lapin;
    use std::collections::BTreeMap;

    use crate::amqp_fs::rabbit::lapin::headers::amqp_value_hack::MyAMQPValue;
    use crate::amqp_fs::rabbit::message::AmqpHeaders;

    /// Wrapper type to expose headers to amqp-node without the disallowed types
    #[doc(hidden)]
    #[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
    #[repr(transparent)]
    pub struct AMQPNodeValue(MyAMQPValue);

    #[doc(hidden)]
    #[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
    pub(super) struct NodeFieldTable(pub(super) BTreeMap<lapin::types::ShortString, AMQPNodeValue>);

    #[doc(hidden)]
    #[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
    struct NodeFieldArray(Vec<AMQPNodeValue>);

    impl From<AMQPNodeValue> for AMQPValue {
        fn from(val: AMQPNodeValue) -> Self {
            match val.0 {
                // These are the types not supported by amqp-node, so
                // we upcast them
                MyAMQPValue::ShortShortUInt(val) => AMQPValue::LongInt(val.into()),
                MyAMQPValue::ShortUInt(val) => AMQPValue::LongInt(val.into()),
                MyAMQPValue::LongUInt(val) => AMQPValue::LongLongInt(val.into()),
                MyAMQPValue::ShortString(val) => AMQPValue::LongString(val.as_str().into()),

                MyAMQPValue::Boolean(val) => AMQPValue::Boolean(val),
                MyAMQPValue::ShortShortInt(val) => AMQPValue::ShortShortInt(val),
                MyAMQPValue::ShortInt(val) => AMQPValue::ShortInt(val),
                MyAMQPValue::LongInt(val) => AMQPValue::LongInt(val),
                MyAMQPValue::LongLongInt(val) => AMQPValue::LongLongInt(val),
                MyAMQPValue::Float(val) => AMQPValue::Float(val),
                MyAMQPValue::Double(val) => AMQPValue::Double(val),
                MyAMQPValue::DecimalValue(val) => AMQPValue::DecimalValue(val),
                MyAMQPValue::LongString(val) => AMQPValue::LongString(val),
                MyAMQPValue::MyFieldArray(val) => AMQPValue::FieldArray(val.into()),
                MyAMQPValue::Timestamp(val) => AMQPValue::Timestamp(val),
                MyAMQPValue::MyFieldTable(val) => AMQPValue::FieldTable(val.into()),
                MyAMQPValue::ByteArray(val) => AMQPValue::ByteArray(val),
                MyAMQPValue::Void => AMQPValue::Void,
            }
        }
    }

    impl<'a> AmqpHeaders<'a> for NodeFieldTable {
        fn insert_bytes(&mut self, key: &str, bytes: &[u8]) {
            let val = MyAMQPValue::ByteArray(ByteArray::from(bytes));
            self.0.insert(key.to_string().into(), AMQPNodeValue(val));
        }
    }

    impl From<NodeFieldTable> for lapin::types::FieldTable {
        fn from(tbl: NodeFieldTable) -> Self {
            // lapin's FieldTable's inner member is private, so we
            // can't just move our value into it, this is dumb but
            // just copy eveything, I guess
            let mut out = lapin::types::FieldTable::default();
            for item in tbl.0 {
                out.insert(item.0.clone(), item.1.clone().into());
            }
            out
        }
    }

    impl From<NodeFieldArray> for FieldArray {
        fn from(v: NodeFieldArray) -> Self {
            let mut out = FieldArray::default();
            for item in v.0 {
                out.push(item.into());
            }
            out
        }
    }
}
