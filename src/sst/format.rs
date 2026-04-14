//! SST file format constants and encoding helpers.
//!
//! Footer layout (48 bytes):
//! ```text
//! ┌───────────────────────────────┐
//! │ metaindex_handle (BlockHandle)│  (varint encoded, up to 20 bytes)
//! │ index_handle (BlockHandle)    │  (varint encoded, up to 20 bytes)
//! │ padding (to 40 bytes)         │
//! │ magic number (8 bytes)        │
//! └───────────────────────────────┘
//! ```

use ruc::*;

/// Magic number identifying MMDB SST files.
pub const TABLE_MAGIC: u64 = 0x4D4D_4442_5353_5400; // "MMDBSST\0"

/// Fixed footer size.
pub const FOOTER_SIZE: usize = 48;

/// Metaindex key for the range-deletion block.
pub const RANGE_DEL_BLOCK_NAME: &str = "rangedelblock";

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
            return Err(eg!("BlockHandle::decode: need 16 bytes, got {}", buf.len()));
        }
        let offset = u64::from_le_bytes(buf[0..8].try_into().unwrap());
        let size = u64::from_le_bytes(buf[8..16].try_into().unwrap());
        Ok(Self { offset, size })
    }
}

/// Block trailer appended after each block's data.
/// ```text
/// ┌──────────────┬──────────────┐
/// │ type: u8     │ crc32: u32   │
/// └──────────────┴──────────────┘
/// ```
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
    footer[40..48].copy_from_slice(&TABLE_MAGIC.to_le_bytes());
    footer
}

/// Decode an SST footer. Returns (metaindex_handle, index_handle).
pub fn decode_footer(data: &[u8; FOOTER_SIZE]) -> crate::error::Result<(BlockHandle, BlockHandle)> {
    let magic = u64::from_le_bytes(data[40..48].try_into().unwrap());
    if magic != TABLE_MAGIC {
        return Err(eg!(crate::error::Error::Corruption(format!(
            "invalid SST magic: {:#x}",
            magic
        ))));
    }
    let metaindex_handle = BlockHandle::decode(&data[0..16]).c(d!())?;
    let index_handle = BlockHandle::decode(&data[16..32]).c(d!())?;
    Ok((metaindex_handle, index_handle))
}

/// Decode an extended index value: BlockHandle + optional first_key.
/// Encode an extended index value: BlockHandle + first_key.
pub fn encode_index_value(handle: &BlockHandle, first_key: &[u8]) -> Vec<u8> {
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
) -> Vec<u8> {
    let mut buf = encode_index_value(handle, first_key);
    let num_props = properties.len() as u16;
    buf.extend_from_slice(&num_props.to_le_bytes());
    for (name, data) in properties {
        buf.extend_from_slice(&(name.len() as u16).to_le_bytes());
        buf.extend_from_slice(name.as_bytes());
        buf.extend_from_slice(&(data.len() as u16).to_le_bytes());
        buf.extend_from_slice(data);
    }
    buf
}

/// Decoded extended index value with optional block properties.
pub struct DecodedIndexValue<'a> {
    pub handle: BlockHandle,
    pub first_key: Option<&'a [u8]>,
    pub properties: Vec<(&'a [u8], &'a [u8])>,
}

/// Decode an extended index value that may contain block properties.
/// Compatible with old format (no properties appended).
pub fn decode_index_value_with_props(data: &[u8]) -> crate::error::Result<DecodedIndexValue<'_>> {
    let handle = BlockHandle::decode(data).c(d!())?;
    let mut first_key: Option<&[u8]> = None;
    let mut props = Vec::new();

    if data.len() <= 16 {
        return Ok(DecodedIndexValue {
            handle,
            first_key: None,
            properties: props,
        });
    }

    // Parse first_key
    let mut offset = 16;
    if data.len() >= offset + 4 {
        let fk_len = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;
        if fk_len > 0 && data.len() >= offset + fk_len {
            first_key = Some(&data[offset..offset + fk_len]);
            offset += fk_len;
        }
    }

    // Parse properties (if present)
    if data.len() >= offset + 2 {
        let num_props = u16::from_le_bytes(data[offset..offset + 2].try_into().unwrap()) as usize;
        offset += 2;
        for _ in 0..num_props {
            if data.len() < offset + 2 {
                break;
            }
            let name_len =
                u16::from_le_bytes(data[offset..offset + 2].try_into().unwrap()) as usize;
            offset += 2;
            if data.len() < offset + name_len {
                break;
            }
            let name = &data[offset..offset + name_len];
            offset += name_len;
            if data.len() < offset + 2 {
                break;
            }
            let data_len =
                u16::from_le_bytes(data[offset..offset + 2].try_into().unwrap()) as usize;
            offset += 2;
            if data.len() < offset + data_len {
                break;
            }
            let prop_data = &data[offset..offset + data_len];
            offset += data_len;
            props.push((name, prop_data));
        }
    }

    Ok(DecodedIndexValue {
        handle,
        first_key,
        properties: props,
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
