//! MANIFEST: tracks the set of SST files that make up each "Version" of the database.
//!
//! A MANIFEST file is a sequence of VersionEdit records, each describing
//! a delta (files added/removed) from the previous state.

pub mod version;
pub mod version_edit;
pub mod version_set;

pub use version::Version;
pub use version_edit::VersionEdit;
pub use version_set::VersionSet;
