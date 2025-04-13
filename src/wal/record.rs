//! WAL record encoding/decoding.
//!
//! Record format:
//! ```text
//! ┌─────────────┬──────────┬───────────┬──────────────┐
//! │ checksum: u32│ length: u16│ type: u8  │ payload      │
//! │ (4 bytes)    │ (2 bytes)  │ (1 byte)  │ (length bytes)│
//! └─────────────┴──────────┴───────────┴──────────────┘
//! ```
//!
//! The WAL is divided into fixed-size blocks (default 32KB).
//! A record that doesn't fit in the remaining block space is split
//! across blocks using record types: Full, First, Middle, Last.

/// WAL block size: 32 KB.
pub const BLOCK_SIZE: usize = 32 * 1024;

/// Record header size: checksum(4) + length(2) + type(1) = 7 bytes.
pub const HEADER_SIZE: usize = 7;

/// Record types for fragmentation support.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum RecordType {
    /// Zero is reserved for pre-allocated file regions.
    Zero = 0,
    /// Complete record contained in one fragment.
    Full = 1,
    /// First fragment of a split record.
    First = 2,
    /// Middle fragment of a split record.
    Middle = 3,
    /// Last fragment of a split record.
    Last = 4,
}

impl RecordType {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::Zero),
            1 => Some(Self::Full),
            2 => Some(Self::First),
            3 => Some(Self::Middle),
            4 => Some(Self::Last),
            _ => None,
        }
    }
}

/// Encode a record header into a 7-byte buffer.
pub fn encode_header(
    buf: &mut [u8; HEADER_SIZE],
    checksum: u32,
    length: u16,
    record_type: RecordType,
) {
    buf[0..4].copy_from_slice(&checksum.to_le_bytes());
    buf[4..6].copy_from_slice(&length.to_le_bytes());
    buf[6] = record_type as u8;
}

/// Decode a record header from a 7-byte buffer.
/// Returns (checksum, length, record_type).
pub fn decode_header(buf: &[u8; HEADER_SIZE]) -> (u32, u16, RecordType) {
    let checksum = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]);
    let length = u16::from_le_bytes([buf[4], buf[5]]);
    let record_type = RecordType::from_u8(buf[6]).unwrap_or(RecordType::Zero);
    (checksum, length, record_type)
}
