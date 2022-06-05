use amq_protocol_types::{FieldTable, AMQPValue, ByteArray};
use bytes::Bytes;
use serde::{Deserialize, Serialize, Deserializer};
use serde_json::Value;
use lapin::types::*;

use std::collections::{btree_map, BTreeMap};

#[allow(unused_imports)]
use tracing::{debug, error, info, trace, warn};

use crate::amqp_fs::options::UnparsableStyle;

use super::{WriteOptions,LinePublishOptions, descriptor::{WriteError, ParsingError}, options::PublishStyle};

pub (in super) struct Message<'a> {
    bytes: &'a [u8],
    options: &'a LinePublishOptions,
}

impl<'a> Message<'a> {
    pub fn new(bytes: &'a [u8], options: &'a LinePublishOptions) -> Self {
        Self {bytes, options}
    }

    pub fn headers(&self) -> Result<FieldTable, ParsingError> {
        use std::str;
        match &self.options.publish_in {
            PublishStyle::Header => {
                match serde_json::from_slice::<MyFieldTable>(self.bytes){
                    Ok(my_headers) => {
                        trace!("my headers are {:?}", serde_json::to_string(&my_headers).unwrap());
                        let headers : FieldTable = my_headers.into();
                        Ok(headers)
                    },
                    Err(err) => {
                        error!("Failed to parse JSON line {}: {}",
                               String::from_utf8_lossy(self.bytes), err);
                        match self.options.handle_unparsable {
                            UnparsableStyle::Skip => {
                                warn!("Skipping unparsable message, but reporting success");
                                Err(ParsingError(self.bytes.len())) // A LIE!
                            },
                            UnparsableStyle::Error => {
                                error!("Returning error for unparsed line");
                                Err(ParsingError(0))
                            },
                            UnparsableStyle::Key => {
                                let mut headers = FieldTable::default();
                                let val = AMQPValue::ByteArray(ByteArray::from(self.bytes));
                                // The CLI parser requires this field if
                                // the style is set to "key", so unwrap is
                                // safe
                                headers.insert(
                                    self.options.parse_error_key
                                        .as_ref()
                                        .unwrap()
                                        .to_string()
                                        .into(), // Wow, that's a lot of conversions
                                    val);
                                Ok(headers)
                            }
                        }
                    }
                }
            }
            PublishStyle::Body => Ok(FieldTable::default())
        }
    }

    pub fn body(&self) -> Vec::<u8> {
        match &self.options.publish_in {
            PublishStyle::Header => Vec::<u8>::with_capacity(0),
            PublishStyle::Body => self.bytes.to_vec()
        }
    }
}

impl<'a> From<(&'a [u8], &'a LinePublishOptions)> for Message<'a> {
    fn from(arg: (&'a [u8], &'a LinePublishOptions)) -> Self {
        Self::new(arg.0, arg.1)
    }
}


#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
#[serde(untagged)]
// #[serde(remote="AMQPValue")]
pub enum MyAMQPValue {
    Boolean(Boolean),
    ShortShortInt(ShortShortInt),
    ShortShortUInt(ShortShortUInt),
    ShortInt(ShortInt),
    ShortUInt(ShortUInt),
    LongInt(LongInt),
    LongUInt(LongUInt),
    LongLongInt(LongLongInt),
    Float(Float),
    Double(Double),
    DecimalValue(DecimalValue),
    ShortString(ShortString),
    LongString(LongString),
    MyFieldArray(MyFieldArray),
    Timestamp(Timestamp),
    MyFieldTable(MyFieldTable),
    ByteArray(ByteArray),
    Void,
}

#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct MyFieldTable(BTreeMap<ShortString, MyAMQPValue>);


#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct MyFieldArray(Vec<MyAMQPValue>);

impl From<MyFieldTable> for FieldTable {
    fn from(tbl: MyFieldTable) -> Self {
        let mut out = FieldTable::default();
        for item in tbl.0.iter() {
            out.insert(item.0.clone(), item.1.clone().into())
        }
        out
    }
}

impl From<MyFieldArray> for FieldArray {
    fn from(v: MyFieldArray) -> Self {
        let mut out = FieldArray::default();
        for item in v.0.into_iter() {
            out.push(item.into());
        }
        out
    }
}

impl From<MyAMQPValue> for AMQPValue {
    fn from(val: MyAMQPValue) -> Self {
        match val {
            MyAMQPValue::Boolean(val) => AMQPValue::Boolean(val),
            MyAMQPValue::ShortShortInt(val) => AMQPValue::ShortShortInt(val),
            MyAMQPValue::ShortShortUInt(val) =>  AMQPValue::ShortShortUInt(val),
            MyAMQPValue::ShortInt(val)       =>  AMQPValue::ShortInt(val),
            MyAMQPValue::ShortUInt(val)      =>  AMQPValue::ShortUInt(val),
            MyAMQPValue::LongInt(val)        =>  AMQPValue::LongInt(val),
            MyAMQPValue::LongUInt(val)       =>  AMQPValue::LongUInt(val),
            MyAMQPValue::LongLongInt(val)    =>  AMQPValue::LongLongInt(val),
            MyAMQPValue::Float(val)          =>  AMQPValue::Float(val),
            MyAMQPValue::Double(val)         =>  AMQPValue::Double(val),
            MyAMQPValue::DecimalValue(val)   =>  AMQPValue::DecimalValue(val),
            MyAMQPValue::ShortString(val)    =>  AMQPValue::LongString(val.as_str().into()),
            MyAMQPValue::LongString(val)     =>  AMQPValue::LongString(val),
            MyAMQPValue::MyFieldArray(val)   =>  AMQPValue::FieldArray(val.into()),
            MyAMQPValue::Timestamp(val)      =>  AMQPValue::Timestamp(val),
            MyAMQPValue::MyFieldTable(val)     =>  AMQPValue::FieldTable(val.into()),
            MyAMQPValue::ByteArray(val)      =>  AMQPValue::ByteArray(val),
            MyAMQPValue::Void                =>  AMQPValue::Void
        }
    }
}

#[cfg(test)]
mod test {
    #[test]
    fn plain() -> Result<(), super::WriteError>{
        let line = b"hello world";
        let opts = super::LinePublishOptions{
            publish_in: super::PublishStyle::Body,
            .. super::LinePublishOptions::default()
        };
        let msg = super::Message::new(line, &opts);
        assert_eq!(msg.body(), line);
        let header = msg.headers()?;
        assert_eq!(header, super::FieldTable::default());
        Ok(())
    }
}
