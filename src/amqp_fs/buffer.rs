//! Internal buffer of partially completed lines
use std::io::{self, BufRead, BufWriter, Write};

use tokio_util::codec::{AnyDelimiterCodec, Decoder, Encoder, AnyDelimiterCodecError};
use bytes::{Bytes, BufMut, BytesMut};

use super::WriteOptions;

pub(in super) struct Buffer {
    line_buf: AnyDelimiterCodec,
    byte_buf: BytesMut,
    max_bytes: usize,
}


impl Buffer {

    pub fn new(delimiters: &[u8], initial_capacity: usize, opts: &WriteOptions) -> Self {
        Self {
            line_buf: AnyDelimiterCodec::new_with_max_length(
                delimiters.to_vec(),
                vec![],
                (1 << 27)*128*2,
            ),
            byte_buf: BytesMut::with_capacity(initial_capacity),
            max_bytes: opts.max_buffer_bytes,
        }
    }

    /// Append a slice to the buffer. Calls
    /// [BytesMut::extend_from_slice]. Returns the number of bytes
    /// read
    pub fn extend(&mut self, extend: &[u8]) -> usize {
        let orig_len = self.byte_buf.len();
        self.byte_buf.extend_from_slice(extend);
        self.byte_buf.len() - orig_len
    }

    /// Truncate the buffer. Calls [BytesMut::truncate]
    pub fn truncate(&mut self, size: usize) {
        self.byte_buf.truncate(size);
    }

    /// Return a decoded line as in [tokio_util::codec::Decoder]
    pub fn decode(&mut self) -> Result<Option<Bytes>, AnyDelimiterCodecError> {
        self.line_buf.decode(&mut self.byte_buf)
    }

    pub fn is_full(&self) -> bool {
        self.max_bytes>0 && self.byte_buf.len() >= self.max_bytes
    }

    pub fn reserve(&mut self, size: usize) {
        self.byte_buf.reserve(size);
    }

}
