![GitHub top language](https://img.shields.io/github/languages/top/rust-util-collections/mmdb)
[![Rust](https://github.com/rust-util-collections/mmdb/actions/workflows/rust.yml/badge.svg)](https://github.com/rust-util-collections/mmdb/actions/workflows/rust.yml)
[![Minimum rustc version](https://img.shields.io/badge/rustc-1.70+-lightgray.svg)](https://github.com/rust-random/rand#rust-version-requirements)

# mmdb

mmdb is a 'std-collection-like' database.

Check [**here**](wrappers/README.md) for a detailed description.

### Crate List

|Name|Version|Doc|Path|Description|
|:-|:-|:-|:-|:-|
|[**mmdb**](wrappers)|[![](https://img.shields.io/crates/v/mmdb.svg)](https://crates.io/crates/mmdb)|[![](https://img.shields.io/badge/api-rustdoc-blue.svg)](https://docs.rs/mmdb)|`./wrappers`|High-level APIs|
|[**mmdb_core**](core)|[![](https://img.shields.io/crates/v/mmdb_core.svg)](https://crates.io/crates/mmdb_core)|[![](https://img.shields.io/badge/api-rustdoc-blue.svg)](https://docs.rs/mmdb_core)|`./core`|Low-level implementations|
|[**mmdb_trie_map**](utils/trie_map)|[![](https://img.shields.io/crates/v/mmdb_trie_map.svg)](https://crates.io/crates/mmdb_trie_map)|[![](https://img.shields.io/badge/api-rustdoc-blue.svg)](https://docs.rs/mmdb_trie_map)|`./utils/trie_map`|trie based structures with </br> limited version capabilities|
|[**mmdb_slot_db**](utils/slot_db)|[![](https://img.shields.io/crates/v/mmdb_slot_db.svg)](https://crates.io/crates/mmdb_slot_db)|[![](https://img.shields.io/badge/api-rustdoc-blue.svg)](https://docs.rs/mmdb_slot_db)|`./utils/slot_db`|A skip-list like timestamp DB|
