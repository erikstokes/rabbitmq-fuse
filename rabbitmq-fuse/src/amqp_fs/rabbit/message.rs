//! Struct to form an AMQP message for publication from a line of
//! input

#[allow(unused_imports)]
use tracing::{debug, error, info, trace, warn};

use crate::amqp_fs::descriptor::ParsingError;

use super::options::{PublishStyle, RabbitMessageOptions, UnparsableStyle};

/// AMQP message
pub(super) struct Message<'a> {
    /// The raw bytes being prepared for publish
    bytes: &'a [u8],

    /// The options used to publish the message
    ///
    /// The key options that control the output are [`RabbitMessageOptions::publish_in`]
    /// and [`RabbitMessageOptions::handle_unparsable`]
    options: &'a RabbitMessageOptions,
}

/// RabbitMQ message headers
pub trait AmqpHeaders<'a>: Default + serde::Deserialize<'a> {
    /// Insert bytes into the headers at the given key
    fn insert_bytes(&mut self, key: &str, bytes: &[u8]);
}

impl<'a> Message<'a> {
    /// Create a new message
    pub fn new(bytes: &'a [u8], options: &'a RabbitMessageOptions) -> Self {
        Self { bytes, options }
    }

    /// The headers for the `RabbitMQ` message.
    ///
    /// If [`RabbitMessageOptions::publish_in`] was set to
    /// [`PublishStyle::Header`], this parses [`Message::bytes`] as a json
    /// string and creates AMQP message header from that.
    ///
    /// # Errors
    ///
    /// If [`PublishStyle::Header`] was set in the options, this may
    /// return a parsing error if
    /// [`RabbitMessageOptions::handle_unparsable`] is
    /// [`UnparsableStyle::Key`]. Errors can be returned if the bytes
    /// can't be parse as JSON
    ///
    /// - [`UnparsableStyle::Skip`]:  [`ParsingError`] holding the length of the line
    /// - [`UnparsableStyle::Error`]: [`ParsingError`] holding length 0
    /// - [`UnparsableStyle::Key`]:  Always succeeds
    ///
    /// # Panics
    /// Will panic if [`RabbitMessageOptions::handle_unparsable`] is
    /// [`UnparsableStyle::Key`] and  [`RabbitMessageOptions::parse_error_key`]
    /// is not a UTF8 string
    pub fn headers<Headers: AmqpHeaders<'a>>(&self) -> Result<Headers, ParsingError> {
        match &self.options.publish_in {
            PublishStyle::Header => {
                match serde_json::from_slice::<Headers>(self.bytes) {
                    Ok(headers) => Ok(headers),
                    Err(err) => {
                        error!(
                            "Failed to parse JSON line {}: {:?}",
                            String::from_utf8_lossy(self.bytes),
                            err
                        );
                        match self.options.handle_unparsable {
                            UnparsableStyle::Skip => {
                                warn!("Skipping unparsable message, but reporting success");
                                Err(ParsingError(self.bytes.len())) // A LIE!
                            }
                            UnparsableStyle::Error => {
                                error!("Returning error for unparsed line");
                                Err(ParsingError(0))
                            }
                            UnparsableStyle::Key => {
                                let mut headers = Headers::default();
                                // let val = amqp_value_hack::MyAMQPValue::ByteArray(ByteArray::from(self.bytes));
                                // The CLI parser requires this field if
                                // the style is set to "key", so unwrap is
                                // safe
                                #[allow(clippy::unwrap_in_result)]
                                headers.insert_bytes(
                                    self.options.parse_error_key.as_ref().unwrap(),
                                    // .to_string(),
                                    // .into(), // Wow, that's a lot of conversions
                                    self.bytes,
                                );
                                Ok(headers)
                            }
                        }
                    }
                }
            }
            PublishStyle::Body => Ok(Headers::default()),
        }
    }

    /// Body of the message.
    ///
    /// If [`RabbitMessageOptions::publish_in`] is [`PublishStyle::Header`]
    /// this returns an empty vector. Otherwise it returns same bytes
    /// used to create the message
    pub fn body(&self) -> &'a [u8] {
        match &self.options.publish_in {
            PublishStyle::Header => &[],
            PublishStyle::Body => self.bytes,
        }
    }
}

impl<'a> From<(&'a [u8], &'a RabbitMessageOptions)> for Message<'a> {
    fn from(arg: (&'a [u8], &'a RabbitMessageOptions)) -> Self {
        Self::new(arg.0, arg.1)
    }
}
