//! Publish a new local store only after its directory entries are durable.

use std::{
    fs::{self, File},
    io,
    path::Path,
};

/// Called with an absolute path, before opening LOCK or publishing CURRENT.
/// Establish each link before creating children, including when creators race.
pub(super) fn create_durable(
    path: &Path,
    sync: impl Fn(&File) -> io::Result<()>,
) -> io::Result<()> {
    let mut missing = Vec::new();
    let mut existing = path;
    while !existing.try_exists()? {
        missing.push(existing);
        existing = existing.parent().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                "store path has no existing ancestor",
            )
        })?;
    }

    // An interrupted mkdir can leave its last link visible but unconfirmed.
    // Earlier links were synced before descent. Resolve symlinks to confirm
    // the actual directory's link, including on a retry with an empty store.
    if let Some(parent) = fs::canonicalize(existing)?.parent() {
        match File::open(parent) {
            Ok(dir) => sync(&dir)?,
            // A caller can supply a directory under a search-only parent.
            // Its existing links must already be durable. For every new
            // link we open the parent for sync *before* attempting mkdir.
            Err(e) if e.kind() == io::ErrorKind::PermissionDenied => {}
            Err(e) => return Err(e),
        }
    }

    for child in missing.into_iter().rev() {
        let parent = File::open(child.parent().expect("absolute non-root path"))?;
        match fs::create_dir(child) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists && child.is_dir() => {}
            Err(e) => return Err(e),
        }
        sync(&parent)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;

    #[cfg(unix)]
    fn assert_directory(fd: &File, path: &Path) {
        use std::os::unix::fs::MetadataExt;
        let actual = fd.metadata().unwrap();
        let expected = fs::metadata(path).unwrap();
        assert_eq!(
            (actual.dev(), actual.ino()),
            (expected.dev(), expected.ino())
        );
    }

    #[test]
    fn retry_confirms_the_last_link_before_descending() {
        let temp = tempfile::tempdir().unwrap();
        let first = temp.path().join("one");
        let nested = first.join("two/store");
        let fail_after_first_mkdir = |fd: &File| {
            if first.exists() {
                #[cfg(unix)]
                assert_directory(fd, temp.path());
                Err(io::Error::other("directory sync failed"))
            } else {
                fd.sync_all()
            }
        };
        assert!(create_durable(&nested, fail_after_first_mkdir).is_err());
        assert!(first.is_dir());
        assert!(!first.join("two").exists());

        let calls = Cell::new(0);
        create_durable(&nested, |fd| {
            // The first retry sync is the parent of the visible 'one' entry.
            assert_eq!(first.join("two").exists(), calls.get() >= 1);
            assert_eq!(nested.exists(), calls.get() >= 2);
            #[cfg(unix)]
            assert_directory(fd, [temp.path(), &first, &first.join("two")][calls.get()]);
            calls.set(calls.get() + 1);
            fd.sync_all()
        })
        .unwrap();
        assert_eq!(calls.get(), 3);
        assert!(nested.is_dir());
    }

    #[test]
    fn confirms_a_directory_created_by_a_competing_opener() {
        let temp = tempfile::tempdir().unwrap();
        let store = temp.path().join("store");
        let calls = Cell::new(0);
        create_durable(&store, |fd| {
            if calls.get() == 0 {
                // Another creator wins mkdir after we discover the missing suffix.
                fs::create_dir(&store)?;
            } else {
                #[cfg(unix)]
                assert_directory(fd, temp.path());
            }
            calls.set(calls.get() + 1);
            fd.sync_all()
        })
        .unwrap();
        assert_eq!(calls.get(), 2);
    }
}
