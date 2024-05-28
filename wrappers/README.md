![GitHub top language](https://img.shields.io/github/languages/top/rust-util-collections/mmdb)
[![Rust](https://github.com/rust-util-collections/mmdb/actions/workflows/rust.yml/badge.svg)](https://github.com/rust-util-collections/mmdb/actions/workflows/rust.yml)
[![Latest Version](https://img.shields.io/crates/v/mmdb.svg)](https://crates.io/crates/mmdb)
[![Rust Documentation](https://img.shields.io/badge/api-rustdoc-blue.svg)](https://docs.rs/mmdb)
[![Minimum rustc version](https://img.shields.io/badge/rustc-1.63+-lightgray.svg)](https://github.com/rust-random/rand#rust-version-requirements)

# mmdb

mmdb is a 'std-collection-like' database.

[**To view the change log check here**](https://github.com/rust-util-collections/mmdb/blob/master/CHANGELOG.md).

### Highlights

- Most APIs is similar as the coresponding data structures in the standard library
    - Use `Vecx` just like `Vec`
    - Use `Mapx` just like `HashMap`
    - Use `MapxOrd` just like `BTreeMap`
- ...

### Compilation features

- [ **DEFAULT** ] `rocks_backend`, use `rocksdb` as the backend database
  - Stable
  - C++ implementation, difficult to be compiled into a static binary
- `parity_backend`, use `parity-db` as the backend database
  - Experimental
  - Pure rust implementation, can be easily compiled into a static binary
- `bcs_codec`, use `bcs` as the codec
    - Faster running speed than json
    - Security reinforcement for blockchain scenarios
    - Created by the original 'Libre' project of Facebook
- `msgpack_codec`, use `rmp-serde` as the codec
    - Faster running speed than json
- `json_codec`, use `serde_json` as the codec
    - Better generality and compatibility
- `compress`, enable compression in the backend database
- `extra_types`, implement `VsMgmt` for some common extra types
  - For example: `H256` and `H160` of the `primitive-types` crate
- `vs`, enable all versioned APIs and the `Vs` procedural macro

### NOTE

- The serialized result of a mmdb instance can not be used as the basis for distributed consensus
  - The serialized result only contains some meta-information(storage paths, etc.)
  - These meta-information are likely to be different in different environments
  - The correct way is to read what you need from it, and then process the real content
