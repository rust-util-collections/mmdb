//! Error types for MMDB.
//!
//! Every [`Error`] carries:
//!
//! - a typed [`ErrorKind`] for programmatic matching (`err.kind()`),
//! - a human-readable message,
//! - a **propagation trace**: the location where the error originated plus one
//!   `file:line:column` frame (with optional note) for every [`ResultExt::ctx`] /
//!   [`ResultExt::with_ctx`] hop it traveled through,
//! - an optional underlying `source` error, preserved as a live object and
//!   reachable through [`std::error::Error::source`] (so it can be downcast,
//!   e.g. to [`std::io::Error`]).
//!
//! Both `Display` and `Debug` render the *complete* chain — message, every
//! trace frame, and the source — so `unwrap()`/`expect()`/log output never
//! lose diagnostic information:
//!
//! ```text
//! Corruption: WAL 000012 CRC mismatch
//!     at src/wal/reader.rs:116:38
//!     at src/db.rs:410:36 -- refusing prefix recovery
//! caused by: unexpected end of file
//! ```
//!
//! `Error` is `Clone + Send + Sync`, so a single failure can be distributed
//! to multiple waiters (e.g. group-commit followers) without stringification.

use std::{error::Error as StdError, fmt, io, panic::Location, sync::Arc};

/// Result type used throughout MMDB.
pub type Result<T> = std::result::Result<T, Error>;

/// Classifies an [`Error`] for programmatic handling.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ErrorKind {
    /// An underlying I/O operation failed.
    Io,
    /// Stored data violates an integrity check (CRC mismatch, malformed
    /// block/record, inconsistent manifest, ...).
    Corruption,
    /// A caller-supplied argument or configuration value is invalid.
    InvalidArgument,
    /// The database has been closed; no further operations are possible.
    DbClosed,
    /// A background task (flush/compaction) failed earlier; writes are
    /// rejected until the database is reopened.
    Background,
}

impl ErrorKind {
    /// Stable string form of the kind.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Io => "I/O error",
            Self::Corruption => "Corruption",
            Self::InvalidArgument => "Invalid argument",
            Self::DbClosed => "DB is closed",
            Self::Background => "Background error",
        }
    }
}

impl fmt::Display for ErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// One hop in the propagation trace.
#[derive(Clone)]
struct Frame {
    location: &'static Location<'static>,
    note: Option<String>,
}

#[derive(Clone)]
struct Inner {
    kind: ErrorKind,
    msg: String,
    /// Trace frames, origin first, most recent hop last.
    trace: Vec<Frame>,
    source: Option<Arc<dyn StdError + Send + Sync + 'static>>,
}

/// The MMDB error type. See the [module documentation](self) for details.
#[derive(Clone)]
pub struct Error {
    inner: Box<Inner>,
}

impl Error {
    #[track_caller]
    fn new(kind: ErrorKind, msg: String) -> Self {
        Self::new_at(kind, msg, Location::caller())
    }

    fn new_at(kind: ErrorKind, msg: String, origin: &'static Location<'static>) -> Self {
        Self {
            inner: Box::new(Inner {
                kind,
                msg,
                trace: vec![Frame {
                    location: origin,
                    note: None,
                }],
                source: None,
            }),
        }
    }

    /// Create a [`ErrorKind::Corruption`] error.
    #[track_caller]
    pub fn corruption(msg: impl Into<String>) -> Self {
        Self::new(ErrorKind::Corruption, msg.into())
    }

    /// Create an [`ErrorKind::InvalidArgument`] error.
    #[track_caller]
    pub fn invalid_argument(msg: impl Into<String>) -> Self {
        Self::new(ErrorKind::InvalidArgument, msg.into())
    }

    /// Create an [`ErrorKind::Background`] error.
    #[track_caller]
    pub fn background(msg: impl Into<String>) -> Self {
        Self::new(ErrorKind::Background, msg.into())
    }

    /// Create an [`ErrorKind::DbClosed`] error.
    #[track_caller]
    pub fn db_closed() -> Self {
        Self::new(ErrorKind::DbClosed, ErrorKind::DbClosed.as_str().to_owned())
    }

    /// Create an [`ErrorKind::Io`] error, preserving `e` as the source.
    #[track_caller]
    pub fn io(e: io::Error) -> Self {
        Self::io_at(e, Location::caller())
    }

    fn io_at(e: io::Error, origin: &'static Location<'static>) -> Self {
        let mut err = Self::new_at(ErrorKind::Io, e.to_string(), origin);
        err.inner.source = Some(Arc::new(e));
        err
    }

    /// Attach an underlying source error (preserved for
    /// [`std::error::Error::source`] / downcasting).
    #[must_use]
    pub fn with_source(mut self, source: impl StdError + Send + Sync + 'static) -> Self {
        self.inner.source = Some(Arc::new(source));
        self
    }

    /// Append a trace frame at the caller's location, with an optional note.
    #[must_use]
    #[track_caller]
    pub fn context(self, note: impl Into<String>) -> Self {
        self.push_frame(Location::caller(), Some(note.into()))
    }

    fn push_frame(mut self, location: &'static Location<'static>, note: Option<String>) -> Self {
        self.inner.trace.push(Frame { location, note });
        self
    }

    /// The kind of this error.
    pub fn kind(&self) -> ErrorKind {
        self.inner.kind
    }

    /// The primary (origin) message.
    pub fn message(&self) -> &str {
        &self.inner.msg
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.inner.kind, self.inner.msg)?;
        for frame in &self.inner.trace {
            write!(f, "\n    at {}", frame.location)?;
            if let Some(note) = &frame.note {
                write!(f, " -- {note}")?;
            }
        }
        if let Some(source) = &self.inner.source {
            write!(f, "\ncaused by: {source}")?;
            let mut cause = source.source();
            while let Some(c) = cause {
                write!(f, "\ncaused by: {c}")?;
                cause = c.source();
            }
        }
        Ok(())
    }
}

impl fmt::Debug for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Render the full chain so `unwrap()`/`expect()` output is complete.
        fmt::Display::fmt(self, f)
    }
}

impl StdError for Error {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        self.inner
            .source
            .as_ref()
            .map(|s| &**s as &(dyn StdError + 'static))
    }
}

impl From<io::Error> for Error {
    /// Convert an [`io::Error`], recording the conversion site (e.g. the `?`
    /// operator's location) as the origin frame.
    #[track_caller]
    fn from(e: io::Error) -> Self {
        Self::io_at(e, Location::caller())
    }
}

/// Extension trait adding trace frames to `Result` values as they propagate.
///
/// Implemented for `Result<T, Error>` and `Result<T, io::Error>`; each call
/// records the caller's `file:line:column` in the error's trace.
pub trait ResultExt<T> {
    /// Append the caller's location to the error trace.
    fn ctx(self) -> Result<T>;

    /// Append the caller's location plus a note (evaluated lazily).
    fn with_ctx<S: Into<String>>(self, note: impl FnOnce() -> S) -> Result<T>;
}

impl<T> ResultExt<T> for Result<T> {
    #[track_caller]
    fn ctx(self) -> Result<T> {
        match self {
            Ok(v) => Ok(v),
            Err(e) => Err(e.push_frame(Location::caller(), None)),
        }
    }

    #[track_caller]
    fn with_ctx<S: Into<String>>(self, note: impl FnOnce() -> S) -> Result<T> {
        match self {
            Ok(v) => Ok(v),
            Err(e) => Err(e.push_frame(Location::caller(), Some(note().into()))),
        }
    }
}

impl<T> ResultExt<T> for std::result::Result<T, io::Error> {
    #[track_caller]
    fn ctx(self) -> Result<T> {
        match self {
            Ok(v) => Ok(v),
            Err(e) => Err(Error::io_at(e, Location::caller())),
        }
    }

    #[track_caller]
    fn with_ctx<S: Into<String>>(self, note: impl FnOnce() -> S) -> Result<T> {
        match self {
            Ok(v) => Ok(v),
            Err(e) => {
                let loc = Location::caller();
                Err(Error::io_at(e, loc).push_frame(loc, Some(note().into())))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn origin() -> Result<()> {
        Err(Error::corruption("bad magic"))
    }

    #[test]
    fn kind_matching_and_message() {
        let err = origin().unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Corruption);
        assert_eq!(err.message(), "bad magic");
    }

    #[test]
    fn trace_chain_renders_every_hop() {
        let err = origin()
            .ctx()
            .with_ctx(|| "while opening table 000012")
            .unwrap_err();
        let rendered = format!("{err}");
        assert!(rendered.starts_with("Corruption: bad magic"));
        // Origin frame + two hops, all pointing into this file.
        assert_eq!(rendered.matches("at src/error.rs:").count(), 3);
        assert!(rendered.contains("-- while opening table 000012"));
        // Debug output must carry the same full chain.
        assert_eq!(format!("{err:?}"), rendered);
    }

    #[test]
    fn io_source_is_preserved_and_downcastable() {
        let io_err = io::Error::new(io::ErrorKind::UnexpectedEof, "eof!");
        let err: Error = std::result::Result::<(), _>::Err(io_err).ctx().unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Io);
        let src = StdError::source(&err).expect("source preserved");
        let io_src = src.downcast_ref::<io::Error>().expect("downcast io");
        assert_eq!(io_src.kind(), io::ErrorKind::UnexpectedEof);
        assert!(format!("{err}").contains("caused by: eof!"));
    }

    #[test]
    fn question_mark_conversion_records_origin() {
        fn f() -> Result<()> {
            Err(io::Error::other("disk gone"))?;
            Ok(())
        }
        let err = f().unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Io);
        assert!(format!("{err}").contains("at src/error.rs:"));
    }

    #[test]
    fn clone_preserves_chain() {
        let err = origin().ctx().unwrap_err();
        let cloned = err.clone();
        assert_eq!(format!("{cloned}"), format!("{err}"));
        assert_eq!(cloned.kind(), ErrorKind::Corruption);
    }

    #[test]
    fn error_is_send_sync_clone() {
        fn assert_bounds<T: Send + Sync + Clone + 'static>() {}
        assert_bounds::<Error>();
    }

    #[test]
    fn manual_context_and_source() {
        let err = Error::invalid_argument("num_levels must be >= 2")
            .context("validating options")
            .with_source(io::Error::other("inner"));
        assert_eq!(err.kind(), ErrorKind::InvalidArgument);
        let rendered = format!("{err}");
        assert!(rendered.contains("-- validating options"));
        assert!(rendered.contains("caused by: inner"));
    }
}
