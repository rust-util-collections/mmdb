#![doc = include_str!("../README.md")]
#![deny(warnings)]
#![cfg_attr(test, allow(warnings))]
#![recursion_limit = "512"]

pub mod basic;
pub mod basic_multi_key;
pub mod common;

pub use basic::{mapx::Mapx, mapx_ord::MapxOrd, vecx::Vecx};
pub use common::{
    ende::{KeyDe, KeyEn, KeyEnDe, KeyEnDeOrdered, ValueDe, ValueEn, ValueEnDe},
    NULL,
};

pub use mmdb_core::{self, *};
