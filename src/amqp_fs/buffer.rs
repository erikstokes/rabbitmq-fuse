//! Internal buffer of partially completed lines
use bytes::{Bytes, BytesMut};
use tokio_util::codec::{AnyDelimiterCodec, AnyDelimiterCodecError, Decoder};

use super::options::WriteOptions;

/// Byte buffer that can split data into lines
pub(super) struct Buffer {
    /// Codec to split data into lines
    line_buf: AnyDelimiterCodec,

    /// Buffer to hold raw bytes
    byte_buf: BytesMut,

    /// Max buffer capacity
    max_bytes: usize,
}

impl Buffer {
    /// Splits data into "lines" using any member of the `delimiters`
    /// characters, in the style of
    /// [tokio_util::codec::AnyDelimiterCodec]. The buffer will
    /// allocate `inital_capacity` immediatly and will grow up to
    /// `opts.max_buffer_bytes` as needed. A value of 0 means unlimited size
    pub fn with_delimeter(delimiters: &[u8], initial_capacity: usize, opts: &WriteOptions) -> Self {
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

    /// Call [Buffer::with_delimeter] using '\n' as the default delimiter
    pub fn new(initial_capacity: usize, opts: &WriteOptions) -> Self {
        Buffer::with_delimeter(b"\n".as_ref(), initial_capacity, opts)
    }

    /// Append a slice to the buffer. Calls
    /// [BytesMut::extend_from_slice]. Returns the number of bytes
    /// read, which may be less than the length of `extend` (for
    /// example if the buffer is at capacity)
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
    ///
    /// Returned lines do not have the terminal \n
    pub fn decode(&mut self) -> Result<Option<Bytes>, AnyDelimiterCodecError> {
        self.line_buf.decode(&mut self.byte_buf)
    }

    /// Is the buffer full?
    ///
    /// If 0 max bytes was specificed in the options when the buffer
    /// was created, this function will always return true
    pub fn is_full(&self) -> bool {
        self.max_bytes > 0 && self.byte_buf.len() >= self.max_bytes
    }

    /// Allocate enough space to store `size` additional bytes
    pub fn reserve(&mut self, size: usize) {
        self.byte_buf.reserve(size);
    }
}

#[cfg(test)]
mod test {
    use super::WriteOptions;
    #[test]
    fn split_partial() {
        let mut buf = super::Buffer::new(8000, &WriteOptions::default());
        buf.extend(b"abc\n123");
        assert_eq!(buf.decode().unwrap().unwrap(), "abc");
        assert_eq!(buf.decode().unwrap(), None);
    }

    #[test]
    fn split_full() {
        let mut buf = super::Buffer::new(8000, &WriteOptions::default());
        buf.extend(b"abc\n123\n");
        assert_eq!(buf.decode().unwrap().unwrap(), "abc");
        assert_eq!(buf.decode().unwrap().unwrap(), "123");
    }

    #[test]
    fn is_full() {
        let opts = WriteOptions {
            max_buffer_bytes: 10,
            ..WriteOptions::default()
        };
        let mut buf = super::Buffer::new(8000, &opts);
        buf.extend(b"aaaaa"); // 5 bytes
        assert!(!buf.is_full());
        buf.extend(b"aaaaa"); // 5 more bytes = 10
        assert!(buf.is_full());
        buf.truncate(0);
        assert!(!buf.is_full());
    }

    #[test]
    fn truncate() {
        let mut buf = super::Buffer::new(
            8000,
            &WriteOptions {
                max_buffer_bytes: 10,
                ..WriteOptions::default()
            },
        );
        buf.extend(b"aaaaaaaaaa");
        assert!(buf.is_full());
        buf.truncate(0);
        assert!(!buf.is_full());
    }
}
