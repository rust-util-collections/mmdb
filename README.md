![GitHub top language](https://img.shields.io/github/languages/top/rust-util-collections/mmdb)
[![Rust](https://github.com/rust-util-collections/mmdb/actions/workflows/rust.yml/badge.svg)](https://github.com/rust-util-collections/mmdb/actions/workflows/rust.yml)
[![Minimum rustc version](https://img.shields.io/badge/rustc-1.70+-lightgray.svg)](https://github.com/rust-random/rand#rust-version-requirements)

# mmdb

mmdb is a 'std-collection-like' database.

This is a simplified version of [**vsdb**](https://crates.io/crates/vsdb), retaining only the most practical and stable parts.

Check [**here**](wrappers/README.md) for a detailed description.

### Crate List

|Name|Version|Doc|Path|Description|
|:-|:-|:-|:-|:-|
|[**mmdb**](wrappers)|[![](https://img.shields.io/crates/v/mmdb.svg)](https://crates.io/crates/mmdb)|[![](https://img.shields.io/badge/api-rustdoc-blue.svg)](https://docs.rs/mmdb)|`wrappers`|High-level APIs|
|[**mmdb_core**](core)|[![](https://img.shields.io/crates/v/mmdb_core.svg)](https://crates.io/crates/mmdb_core)|[![](https://img.shields.io/badge/api-rustdoc-blue.svg)](https://docs.rs/mmdb_core)|`core`|Low-level implementations|
|[**mmdb_slot_db**](utils/slot_db)|[![](https://img.shields.io/crates/v/mmdb_slot_db.svg)](https://crates.io/crates/mmdb_slot_db)|[![](https://img.shields.io/badge/api-rustdoc-blue.svg)](https://docs.rs/mmdb_slot_db)|`utils/slot_db`|A skip-list like timestamp DB|
|[**mmdb_trie_db**](utils/trie_db)|[![](https://img.shields.io/crates/v/mmdb_trie_db.svg)](https://crates.io/crates/mmdb_trie_db)|[![](https://img.shields.io/badge/api-rustdoc-blue.svg)](https://docs.rs/mmdb_trie_db)|`utils/trie_db`|MPT(trie) implementations|
