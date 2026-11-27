//! SST file format constants and encoding helpers.
//!
//! The 48-byte footer contains two block handles, padding, and the file magic.
//! Each handle stores its offset and size as little-endian `u64`s.
//!
//! ```text
//! bytes   0..16             16..32         32..40    40..48
//! field   metaindex handle  index handle   padding   magic (u64 LE)
//! ```

use crate::error::{Error, Result, ResultExt};

/// Magic number identifying MMDB SST files.
pub const TABLE_MAGIC_V2: u64 = 0x4D4D_4442_5353_5402; // "MMDBSST" v2

/// Fixed footer size.
pub const FOOTER_SIZE: usize = 48;

/// Metaindex key for the range-deletion block.
pub const RANGE_DEL_BLOCK_NAME: &str = "rangedelblock";
/// Optional SST v2 metaindex entry containing the highest stored MVCC sequence (u64 LE).
pub const MAX_SEQUENCE_NAME: &str = "table.max_sequence";
/// Metaindex key storing the fixed prefix length used by the prefix bloom.
pub const PREFIX_FILTER_LEN_NAME: &str = "filter.prefix_len";
/// Required layout marker shared by whole-key and prefix Bloom payloads.
pub const FILTER_FORMAT_NAME: &str = "filter.format";
/// Cache-line-blocked XXH3-64 Bloom payload, eight probes per line.
pub const FILTER_FORMAT_BLOCKED_64B: u8 = 2;

/// A handle pointing to a block within an SST file.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BlockHandle {
    /// Offset of the block in the file.
    pub offset: u64,
    /// Size of the block data (not including the trailing checksum + type byte).
    pub size: u64,
}

impl BlockHandle {
    pub fn new(offset: u64, size: u64) -> Self {
        Self { offset, size }
    }

    /// Encode to bytes (fixed 16 bytes: offset u64 LE + size u64 LE).
    pub fn encode(&self) -> [u8; 16] {
        let mut buf = [0u8; 16];
        buf[0..8].copy_from_slice(&self.offset.to_le_bytes());
        buf[8..16].copy_from_slice(&self.size.to_le_bytes());
        buf
    }

    /// Decode from bytes. Returns an error if `buf` is shorter than 16 bytes.
    pub fn decode(buf: &[u8]) -> Result<Self> {
        if buf.len() < 16 {
            return Err(Error::corruption(format!(
                "BlockHandle::decode: need 16 bytes, got {}",
                buf.len()
            )));
        }
        let offset = u64::from_le_bytes(buf[0..8].try_into().unwrap());
        let size = u64::from_le_bytes(buf[8..16].try_into().unwrap());
        Ok(Self { offset, size })
    }
}

/// Each block ends with a one-byte compression type and a little-endian
/// four-byte CRC32 over the block data and type byte.
pub const BLOCK_TRAILER_SIZE: usize = 5; // type(1) + crc32(4)

/// Compression type for a block.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum CompressionType {
    None = 0,
    Lz4 = 2,
    Zstd = 3,
}

impl CompressionType {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::None),
            2 => Some(Self::Lz4),
            3 => Some(Self::Zstd),
            _ => None,
        }
    }
}

/// Encode an SST footer.
pub fn encode_footer(
    metaindex_handle: &BlockHandle,
    index_handle: &BlockHandle,
) -> [u8; FOOTER_SIZE] {
    let mut footer = [0u8; FOOTER_SIZE];
    let meta_enc = metaindex_handle.encode();
    let idx_enc = index_handle.encode();
    footer[0..16].copy_from_slice(&meta_enc);
    footer[16..32].copy_from_slice(&idx_enc);
    // bytes 32..40 are padding (zeros)
    footer[40..48].copy_from_slice(&TABLE_MAGIC_V2.to_le_bytes());
    footer
}

/// Decode an SST footer. Returns (metaindex_handle, index_handle).
pub fn decode_footer(data: &[u8; FOOTER_SIZE]) -> Result<(BlockHandle, BlockHandle)> {
    let magic = u64::from_le_bytes(data[40..48].try_into().unwrap());
    if magic != TABLE_MAGIC_V2 {
        return Err(Error::corruption(format!(
            "invalid SST magic: {:#x}",
            magic
        )));
    }
    let metaindex_handle = BlockHandle::decode(&data[0..16]).ctx()?;
    let index_handle = BlockHandle::decode(&data[16..32]).ctx()?;
    Ok((metaindex_handle, index_handle))
}

/// Encode an extended index value: BlockHandle + first_key.
fn encode_index_value(handle: &BlockHandle, first_key: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(20 + first_key.len());
    buf.extend_from_slice(&handle.encode());
    buf.extend_from_slice(&(first_key.len() as u32).to_le_bytes());
    buf.extend_from_slice(first_key);
    buf
}

/// Encode an extended index value with block properties:
/// `[BlockHandle(16)][first_key_len(4 LE)][first_key][num_props(2 LE)]`
/// followed by for each property:
/// `[name_len(2 LE)][name bytes][data_len(2 LE)][data bytes]`
pub fn encode_index_value_with_props(
    handle: &BlockHandle,
    first_key: &[u8],
    properties: &[(&str, &[u8])],
) -> Result<Vec<u8>> {
    let mut buf = encode_index_value(handle, first_key);
    let num_props = u16::try_from(properties.len())
        .map_err(|_| Error::invalid_argument("too many block properties"))
        .ctx()?;
    buf.extend_from_slice(&num_props.to_le_bytes());
    for (name, data) in properties {
        let name_len = u16::try_from(name.len())
            .map_err(|_| Error::invalid_argument("block property name too large"))?;
        let data_len = u16::try_from(data.len())
            .map_err(|_| Error::invalid_argument("block property data too large"))?;
        buf.extend_from_slice(&name_len.to_le_bytes());
        buf.extend_from_slice(name.as_bytes());
        buf.extend_from_slice(&data_len.to_le_bytes());
        buf.extend_from_slice(data);
    }
    Ok(buf)
}

/// Zero-allocation view of a decoded SST index value.
#[derive(Clone, Copy)]
pub struct DecodedIndexEntry<'a> {
    pub handle: BlockHandle,
    pub first_key: &'a [u8],
    props_raw: &'a [u8],
    num_props: u16,
}

impl<'a> DecodedIndexEntry<'a> {
    /// Evaluate a predicate over `(name, data)` block-property slices without allocating.
    /// Property framing was already validated by [`decode_index_entry`].
    #[inline]
    pub fn any_property(&self, mut pred: impl FnMut(&'a [u8], &'a [u8]) -> bool) -> bool {
        let mut offset = 0usize;
        let raw = self.props_raw;
        for _ in 0..self.num_props {
            let name_len = u16::from_le_bytes(raw[offset..offset + 2].try_into().unwrap()) as usize;
            offset += 2;
            let name = &raw[offset..offset + name_len];
            offset += name_len;
            let data_len = u16::from_le_bytes(raw[offset..offset + 2].try_into().unwrap()) as usize;
            offset += 2;
            let prop_data = &raw[offset..offset + data_len];
            offset += data_len;
            if pred(name, prop_data) {
                return true;
            }
        }
        false
    }
}

/// Validate and decode an extended index value without heap allocation.
pub fn decode_index_entry(data: &[u8]) -> Result<DecodedIndexEntry<'_>> {
    let handle = BlockHandle::decode(data).ctx()?;
    let first_key_len = data
        .get(16..20)
        .ok_or_else(|| Error::corruption("missing index first key"))?;
    let len = u32::from_le_bytes(first_key_len.try_into().unwrap()) as usize;
    let (first_key, rest) = data[20..]
        .split_at_checked(len)
        .ok_or_else(|| Error::corruption("truncated index first key"))?;
    if rest.len() < 2 {
        return Err(Error::corruption("truncated property count"));
    }
    let num_props = u16::from_le_bytes(rest[..2].try_into().unwrap());
    let props_raw = &rest[2..];
    let mut offset = 0usize;
    for _ in 0..num_props {
        if props_raw.len() < offset + 2 {
            return Err(Error::corruption("truncated block property name length"));
        }
        let name_len =
            u16::from_le_bytes(props_raw[offset..offset + 2].try_into().unwrap()) as usize;
        offset += 2;
        if props_raw.len() < offset + name_len {
            return Err(Error::corruption("truncated block property name"));
        }
        offset += name_len;
        if props_raw.len() < offset + 2 {
            return Err(Error::corruption("truncated block property data length"));
        }
        let data_len =
            u16::from_le_bytes(props_raw[offset..offset + 2].try_into().unwrap()) as usize;
        offset += 2;
        if props_raw.len() < offset + data_len {
            return Err(Error::corruption("truncated block property data"));
        }
        offset += data_len;
    }
    if offset != props_raw.len() {
        return Err(Error::corruption("trailing bytes after block properties"));
    }
    Ok(DecodedIndexEntry {
        handle,
        first_key,
        props_raw,
        num_props,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_block_handle_encode_decode() {
        let bh = BlockHandle::new(12345, 6789);
        let encoded = bh.encode();
        let decoded = BlockHandle::decode(&encoded).unwrap();
        assert_eq!(bh, decoded);
    }

    #[test]
    fn test_footer_encode_decode() {
        let meta = BlockHandle::new(100, 200);
        let idx = BlockHandle::new(300, 400);
        let footer = encode_footer(&meta, &idx);
        let (m2, i2) = decode_footer(&footer).unwrap();
        assert_eq!(meta, m2);
        assert_eq!(idx, i2);
    }

    #[test]
    fn test_footer_bad_magic() {
        let mut footer = [0u8; FOOTER_SIZE];
        footer[40..48].copy_from_slice(&0xDEADBEEF_u64.to_le_bytes());
        assert!(decode_footer(&footer).is_err());
    }
}
