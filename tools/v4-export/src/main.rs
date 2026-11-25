//! Copy the visible v4 rows into a new v5 store.

use std::{env, error::Error, fs, io, path::Path};

// Publication must preserve a destination created concurrently. Use only
// the atomic operation, even on a filesystem where it returns Unsupported.
#[cfg(not(target_os = "redox"))]
use renamest::rename_exclusive as publish;

#[cfg(target_os = "redox")]
fn publish(staging: &Path, dest: &Path) -> io::Result<()> {
    use rustix::{
        fs::{CWD, RenameFlags, renameat_with},
        io::Errno,
    };
    renameat_with(CWD, staging, CWD, dest, RenameFlags::NOREPLACE).map_err(|error| match error {
        Errno::NOSYS | Errno::OPNOTSUPP | Errno::INVAL => io::ErrorKind::Unsupported.into(),
        other => other.into(),
    })
}

fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<_> = env::args_os().skip(1).collect();
    if !(2..=4).contains(&args.len()) {
        return Err("usage: mmdb-v4-export SOURCE DEST [NUM_LEVELS] [PREFIX_LEN]".into());
    }
    let dest = Path::new(&args[1]);
    if dest.try_exists()? {
        return Err(io::Error::new(io::ErrorKind::AlreadyExists, "destination exists").into());
    }
    let parse = |i: usize, default: usize| -> Result<usize, Box<dyn Error>> {
        match args.get(i) {
            None => Ok(default),
            Some(arg) => Ok(arg.to_str().ok_or("invalid integer")?.parse()?),
        }
    };
    let levels = parse(2, 7)?;
    let prefix = parse(3, 0)?;
    let source_path = fs::canonicalize(&args[0])?;
    let parent = dest
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let parent = fs::canonicalize(parent)?;
    let dest = parent.join(dest.file_name().ok_or("invalid destination name")?);
    if parent.starts_with(&source_path) {
        return Err("the destination must be outside the source store".into());
    }
    let source = v4::DB::open_read_only_with_options(
        v4::DbOptions {
            num_levels: levels,
            prefix_len: prefix,
            ..Default::default()
        },
        &source_path,
    )?;
    let staging = tempfile::tempdir_in(&parent)?;
    let target = v5::DB::open(
        v5::DbOptions::default()
            .with_num_levels(levels.clamp(2, 16))
            .with_prefix_len(prefix),
        staging.path(),
    )?;
    let mut iter = source.iter_with_options(&v4::ReadOptions {
        fill_cache: false,
        ..Default::default()
    })?;
    let count = target.ingest_sorted(iter.by_ref())?;
    if let Some(error) = iter.error() {
        return Err(error.into());
    }
    drop(iter);
    source.close()?;
    target.close()?;
    drop(target);
    publish(staging.path(), &dest).map_err(|e| {
        if e.kind() == io::ErrorKind::Unsupported {
            io::Error::new(
                e.kind(),
                "export requires an exclusive directory rename on the destination filesystem",
            )
        } else {
            e
        }
    })?;
    let _ = staging.keep();
    #[cfg(unix)]
    fs::File::open(parent)?.sync_all()?;
    println!("Exported {count} rows to {}", dest.display());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn publication_preserves_existing_directories_and_can_be_retried() {
        let parent = tempfile::tempdir().unwrap();
        let staging = parent.path().join("staging");
        let dest = parent.path().join("dest");
        assert!(publish(&staging, &dest).is_err());
        assert!(!dest.exists());
        fs::create_dir(&staging).unwrap();
        fs::write(staging.join("exported"), b"new").unwrap();

        fs::create_dir(&dest).unwrap();
        assert!(publish(&staging, &dest).is_err());
        assert_eq!(dest.read_dir().unwrap().count(), 0);
        fs::write(dest.join("existing"), b"old").unwrap();
        assert!(publish(&staging, &dest).is_err());
        assert_eq!(fs::read(dest.join("existing")).unwrap(), b"old");

        fs::remove_dir_all(&dest).unwrap();
        if let Err(error) = publish(&staging, &dest) {
            assert_eq!(error.kind(), io::ErrorKind::Unsupported);
            assert!(
                !(cfg!(target_os = "linux") && env::var_os("CI").is_some()),
                "the Linux CI filesystem must support exclusive directory publication"
            );
            assert_eq!(
                publish(&staging, &dest).unwrap_err().kind(),
                io::ErrorKind::Unsupported
            );
            assert_eq!(fs::read(staging.join("exported")).unwrap(), b"new");
            assert!(!dest.exists());
            return;
        }
        assert_eq!(fs::read(dest.join("exported")).unwrap(), b"new");
        assert!(!staging.exists());
    }
}
