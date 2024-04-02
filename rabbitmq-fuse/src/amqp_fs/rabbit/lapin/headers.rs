//! Reconstruction of [`lapin::types`](https://docs.rs/amq-protocol-types/7.1.2/amq_protocol_types/index.html) so we can modify the [`serde`] output

// The only function of this whole mess is to add the
// `#[serde(untagged)]` line to `AMQPValue` so that it loads json the
// way I want it to. Is there a cleaner way to do this?
#[cfg(feature = "lapin_endpoint")]
#[doc(hidden)]
pub(crate) mod amqp_value_hack {

    use lapin::types::{AMQPValue, ByteArray};
    use lapin::types::{
        Boolean, DecimalValue, Double, FieldArray, Float, LongInt, LongLongInt, LongUInt, ShortInt,
        ShortShortInt, ShortShortUInt, ShortString, ShortUInt, Timestamp,
    };
    use lapin_pool::lapin;
    use serde::{Deserialize, Serialize};
    use std::collections::BTreeMap;

    use crate::amqp_fs::rabbit::message::AmqpHeaders;

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
        LongString(String), // We read json, so everything is already UTF8
        MyFieldArray(MyFieldArray),
        Timestamp(Timestamp),
        MyFieldTable(MyFieldTable),
        ByteArray(ByteArray),
        Void,
    }

    #[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
    pub struct MyFieldTable(pub(super) BTreeMap<lapin::types::ShortString, MyAMQPValue>);

    #[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
    pub struct MyFieldArray(Vec<MyAMQPValue>);

    impl From<MyFieldTable> for lapin::types::FieldTable {
        fn from(tbl: MyFieldTable) -> Self {
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

    impl From<MyFieldArray> for FieldArray {
        fn from(v: MyFieldArray) -> Self {
            let mut out = FieldArray::default();
            for item in v.0 {
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
                MyAMQPValue::ShortShortUInt(val) => AMQPValue::ShortShortUInt(val),
                MyAMQPValue::ShortInt(val) => AMQPValue::ShortInt(val),
                MyAMQPValue::ShortUInt(val) => AMQPValue::ShortUInt(val),
                MyAMQPValue::LongInt(val) => AMQPValue::LongInt(val),
                MyAMQPValue::LongUInt(val) => AMQPValue::LongUInt(val),
                MyAMQPValue::LongLongInt(val) => AMQPValue::LongLongInt(val),
                MyAMQPValue::Float(val) => AMQPValue::Float(val),
                MyAMQPValue::Double(val) => AMQPValue::Double(val),
                MyAMQPValue::DecimalValue(val) => AMQPValue::DecimalValue(val),
                MyAMQPValue::ShortString(val) => AMQPValue::LongString(val.as_str().into()),
                MyAMQPValue::LongString(val) => AMQPValue::LongString(val.into_bytes().into()),
                MyAMQPValue::MyFieldArray(val) => AMQPValue::FieldArray(val.into()),
                MyAMQPValue::Timestamp(val) => AMQPValue::Timestamp(val),
                MyAMQPValue::MyFieldTable(val) => AMQPValue::FieldTable(val.into()),
                MyAMQPValue::ByteArray(val) => AMQPValue::ByteArray(val),
                MyAMQPValue::Void => AMQPValue::Void,
            }
        }
    }

    impl<'a> AmqpHeaders<'a> for MyFieldTable {
        fn insert_bytes(&mut self, key: &str, bytes: &[u8]) {
            let val = MyAMQPValue::ByteArray(ByteArray::from(bytes));
            self.0.insert(key.to_string().into(), val);
        }
    }
}

#[cfg(test)]
mod test {
    use super::amqp_value_hack::*;
    use lapin_pool::lapin::types::FieldTable;

    use crate::amqp_fs::{
        descriptor::WriteError,
        rabbit::{
            message::Message,
            options::{PublishStyle, RabbitMessageOptions},
        },
    };
    #[test]
    fn plain() -> Result<(), WriteError> {
        let line = b"hello world";
        let opts = RabbitMessageOptions {
            publish_in: PublishStyle::Body,
            ..RabbitMessageOptions::default()
        };
        let msg = Message::new(line, &opts);
        assert_eq!(msg.body(), line);
        let header: FieldTable = msg.headers::<MyFieldTable>()?.into();
        assert_eq!(header, lapin_pool::lapin::types::FieldTable::default());
        Ok(())
    }

    #[test]
    fn json_mixed() -> Result<(), WriteError> {
        use lapin_pool::lapin::types::AMQPValue;
        let line = br#"{"stuff": {"a": 1, "b": "hello", "c": [123456789, "test"]}}"#;
        let opts = RabbitMessageOptions {
            publish_in: PublishStyle::Header,
            ..RabbitMessageOptions::default()
        };
        let msg = Message::new(line, &opts);
        assert_eq!(msg.body(), b"");
        let header: FieldTable = msg.headers::<MyFieldTable>()?.into();

        // let value: serde_json::Value = serde_json::from_slice(line).unwrap();
        // let header_value = serde_json::to_value(header).unwrap();
        let map = header.inner();
        match &map["stuff"] {
            AMQPValue::FieldTable(val) => {
                assert_eq!(val.inner()["a"], AMQPValue::ShortShortInt(1));
                assert_eq!(val.inner()["b"], AMQPValue::LongString("hello".into()));
                if let AMQPValue::FieldArray(arr) = &val.inner()["c"] {
                    assert_eq!(arr.as_slice()[0], AMQPValue::LongInt(123_456_789));
                    assert_eq!(arr.as_slice()[1], AMQPValue::LongString("test".into()));
                } else {
                    panic!();
                }
            }
            _ => {
                panic!();
            }
        }
        Ok(())
    }

    #[test]
    fn json_long() -> Result<(), WriteError> {
        let line = br#"{"stuff": [{"_id": "629c007a47dce2709d945d07", "index": 0, "guid": "86601991-b705-4ff1-be4b-8364519b2427", "isActive": false, "balance": "$3,616.89", "picture": "http://placehold.it/32x32", "age": 31, "eyeColor": "blue", "name": "Bennett Hatfield", "gender": "male", "company": "ECOLIGHT", "email": "bennetthatfield@ecolight.com", "phone": "+1 (999) 408-2002", "address": "641 Coventry Road, Grenelefe, South Dakota, 3821", "about": "Laboris sunt voluptate eu consectetur irure sit tempor reprehenderit deserunt exercitation duis. Eu pariatur quis aute ea deserunt. Culpa in esse magna irure dolor officia ipsum consequat sint magna.\\r\\n", "registered": "2015-03-11T08:49:07 +04:00", "latitude": -78.399259, "longitude": 26.547197, "tags": ["eiusmod", "qui", "laborum", "Lorem", "consequat", "esse", "esse"], "friends": [{"id": 0, "name": "Rhoda Kelley"}, {"id": 1, "name": "Schmidt Stanton"}, {"id": 2, "name": "Fletcher Ballard"}], "greeting": "Hello, Bennett Hatfield! You have 8 unread messages.", "favoriteFruit": "strawberry"}, {"_id": "629c007a9acc7a89b0f85131", "index": 1, "guid": "a09b46a0-f443-44cc-9a12-42d812bf934b", "isActive": true, "balance": "$1,664.68", "picture": "http://placehold.it/32x32", "age": 22, "eyeColor": "brown", "name": "Josefina Barrera", "gender": "female", "company": "ERSUM", "email": "josefinabarrera@ersum.com", "phone": "+1 (852) 501-2520", "address": "318 Arlington Avenue, Santel, Palau, 7035", "about": "Do nisi cupidatat sint culpa incididunt sunt labore. Est quis et eiusmod dolor non sunt. Enim sunt eiusmod velit deserunt anim irure aute elit et proident ullamco proident nostrud veniam. Officia do ea velit dolor Lorem sit excepteur in. Incididunt exercitation minim fugiat proident irure cupidatat nulla laborum. Eiusmod ipsum minim do laborum fugiat velit.\\r\\n", "registered": "2020-12-15T10:49:53 +05:00", "latitude": -6.332824, "longitude": 134.008099, "tags": ["esse", "id", "Lorem", "dolor", "commodo", "enim", "labore"], "friends": [{"id": 0, "name": "Torres Sparks"}, {"id": 1, "name": "Lorena Buck"}, {"id": 2, "name": "Agnes Daniels"}], "greeting": "Hello, Josefina Barrera! You have 10 unread messages.", "favoriteFruit": "banana"}, {"_id": "629c007a5f86d8d03607b05b", "index": 2, "guid": "64e9a62e-9f42-4854-8e50-3599819bd50c", "isActive": false, "balance": "$2,285.18", "picture": "http://placehold.it/32x32", "age": 29, "eyeColor": "green", "name": "Josephine Robinson", "gender": "female", "company": "KINETICA", "email": "josephinerobinson@kinetica.com", "phone": "+1 (978) 495-2959", "address": "412 Amity Street, Keller, Kentucky, 7957", "about": "Laboris irure et commodo voluptate cillum id aute. Dolore minim ipsum non sint consequat nostrud non do eiusmod aliqua nisi commodo dolor irure. Aute nulla ullamco cupidatat enim est sit esse ipsum quis.\\r\\n", "registered": "2020-09-06T10:42:41 +04:00", "latitude": -3.00885, "longitude": 40.402674, "tags": ["quis", "qui", "qui", "occaecat", "ipsum", "elit", "magna"], "friends": [{"id": 0, "name": "Mccray Bonner"}, {"id": 1, "name": "Walsh Gardner"}, {"id": 2, "name": "Watts Mcmillan"}], "greeting": "Hello, Josephine Robinson! You have 3 unread messages.", "favoriteFruit": "banana"}, {"_id": "629c007a2a0badcaa3cd62eb", "index": 3, "guid": "2800e44c-79ab-4d03-9d18-c58fd90937f4", "isActive": true, "balance": "$3,132.60", "picture": "http://placehold.it/32x32", "age": 30, "eyeColor": "brown", "name": "Reeves Buckner", "gender": "male", "company": "PARAGONIA", "email": "reevesbuckner@paragonia.com", "phone": "+1 (831) 521-2736", "address": "879 Dunne Court, Wedgewood, New York, 5681", "about": "Dolor ad nisi sunt ad. Proident voluptate nisi excepteur aliquip nulla. Deserunt minim ipsum sunt occaecat culpa aute ea do irure non dolor Lorem. Id occaecat mollit occaecat laboris incididunt tempor dolore do nulla dolore laboris aliqua sit ut. Voluptate ut fugiat non id aliquip adipisicing excepteur deserunt est cupidatat reprehenderit eu. Magna enim esse adipisicing eu qui labore aliqua sunt. Reprehenderit Lorem eu non voluptate commodo veniam ipsum qui.\\r\\n", "registered": "2018-06-12T04:35:54 +04:00", "latitude": 89.905316, "longitude": -97.486195, "tags": ["Lorem", "laboris", "exercitation", "id", "ullamco", "excepteur", "ex"], "friends": [{"id": 0, "name": "Ruiz Hughes"}, {"id": 1, "name": "Taylor Macias"}, {"id": 2, "name": "Watson Clay"}], "greeting": "Hello, Reeves Buckner! You have 7 unread messages.", "favoriteFruit": "apple"}, {"_id": "629c007af785d4f8205a943c", "index": 4, "guid": "53ab89f4-91a0-4b52-8daa-78d303470d5c", "isActive": true, "balance": "$3,350.67", "picture": "http://placehold.it/32x32", "age": 31, "eyeColor": "brown", "name": "Maxine Thompson", "gender": "female", "company": "COMBOGENE", "email": "maxinethompson@combogene.com", "phone": "+1 (940) 552-2275", "address": "279 Times Placez, Hollins, Oregon, 5363", "about": "Irure elit quis consequat Lorem ipsum proident duis cillum quis laboris mollit incididunt deserunt. Reprehenderit elit anim incididunt qui eiusmod exercitation occaecat esse excepteur. Fugiat occaecat magna ullamco elit do in eiusmod amet consectetur duis. Pariatur fugiat quis fugiat reprehenderit duis nulla ea sit laborum enim est et. Id proident anim exercitation amet amet culpa.\\r\\n", "registered": "2021-03-21T03:47:30 +04:00", "latitude": 38.071513, "longitude": -178.204013, "tags": ["consequat", "reprehenderit", "laborum", "adipisicing", "irure", "ipsum", "eiusmod"], "friends": [{"id": 0, "name": "Suzanne Delaney"}, {"id": 1, "name": "Curry Keller"}, {"id": 2, "name": "Nora Holder"}], "greeting": "Hello, Maxine Thompson! You have 8 unread messages.", "favoriteFruit": "apple"}, {"_id": "629c007a3fe614c51328f4e9", "index": 5, "guid": "dc8a30fd-f43d-44ea-af80-aca229f0c726", "isActive": false, "balance": "$1,738.13", "picture": "http://placehold.it/32x32", "age": 23, "eyeColor": "green", "name": "Rice Glenn", "gender": "male", "company": "CEMENTION", "email": "riceglenn@cemention.com", "phone": "+1 (891) 407-3574", "address": "916 Prescott Place, Edneyville, North Dakota, 3579", "about": "Sit deserunt do sint veniam fugiat deserunt. Ea proident velit officia consectetur esse elit dolor. Irure est aute eu cillum sit proident occaecat enim ut laboris. Ex non ut eu fugiat ea nisi sit adipisicing nisi est aute. Voluptate pariatur sint velit cillum nulla ut exercitation eiusmod amet ea minim consectetur magna qui. Quis labore esse culpa laboris mollit laborum. Occaecat ad aliqua elit proident non.\\r\\n", "registered": "2020-09-12T12:15:59 +04:00", "latitude": -25.805296, "longitude": 179.76254, "tags": ["irure", "minim", "quis", "dolore", "reprehenderit", "nisi", "velit"], "friends": [{"id": 0, "name": "Pugh Stone"}, {"id": 1, "name": "Herman Carney"}, {"id": 2, "name": "Jennifer Cline"}], "greeting": "Hello, Rice Glenn! You have 3 unread messages.", "favoriteFruit": "apple"}]}"#;

        let opts = RabbitMessageOptions {
            publish_in: PublishStyle::Header,
            ..RabbitMessageOptions::default()
        };
        let msg = Message::new(line, &opts);
        assert_eq!(msg.body(), b"");
        let header: FieldTable = msg.headers::<MyFieldTable>()?.into();

        let header_val = serde_json::to_value(&header).unwrap();
        let field_map: lapin_pool::lapin::types::FieldTable = serde_json::from_slice(
            // Re-adds the type tags the json string
            serde_json::to_string(&header).unwrap().as_ref(),
        )
        .unwrap();
        let field_val = serde_json::to_value(field_map).unwrap();

        assert_eq!(header_val, field_val);

        Ok(())
    }

    #[test]
    #[should_panic]
    fn bad_json() {
        let line = b"{can'tparseme";

        let opts = RabbitMessageOptions {
            publish_in: PublishStyle::Header,
            ..RabbitMessageOptions::default()
        };
        let msg = Message::new(line, &opts);
        assert_eq!(msg.body(), b"");
        msg.headers::<MyFieldTable>().unwrap();
    }

    // Make sure lists of small ints don't turn into strings
    #[test]
    fn test_short_int_list() -> Result<(), WriteError> {
        use lapin_pool::lapin::types::AMQPValue;
        let line = br#"{"ints": [1,2,3, 255], "str": "A String"}"#;
        let opts = RabbitMessageOptions {
            publish_in: PublishStyle::Header,
            ..RabbitMessageOptions::default()
        };
        let msg = Message::new(line, &opts);
        assert_eq!(msg.body(), b"");
        let header: lapin_pool::lapin::types::FieldTable = msg.headers::<MyFieldTable>()?.into();

        // let value: serde_json::Value = serde_json::from_slice(line).unwrap();
        // let header_value = serde_json::to_value(header).unwrap();
        let map = header.inner();
        match &map["ints"] {
            AMQPValue::FieldArray(_) => {}
            _ => panic!("Found wrong type"),
        }
        Ok(())
    }
}
