#![doc = include_str!("../README.md")]
#![deny(warnings)]
#![cfg_attr(test, allow(warnings))]
#![recursion_limit = "512"]

pub mod common;

pub mod basic;

pub use basic::mapx_raw::MapxRaw;

pub use common::{
    mmdb_flush, mmdb_get_base_dir, mmdb_get_custom_dir, mmdb_set_base_dir, RawBytes,
    RawKey, RawValue, GB, KB, MB, NULL,
};

#[cfg(feature = "extra_types")]
pub use {primitive_types_0_10, primitive_types_0_11, primitive_types_0_12};
