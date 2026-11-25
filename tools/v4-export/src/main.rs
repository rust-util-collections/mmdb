//! Copy the visible v4 rows into a new v5 store.

use std::{env, error::Error, fs, io, path::Path};

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
    let mut count = 0u64;
    for (key, value) in iter.by_ref() {
        target.put(&key, &value)?;
        count += 1;
    }
    if let Some(error) = iter.error() {
        return Err(error.into());
    }
    drop(iter);
    source.close()?;
    target.close()?;
    drop(target);
    // POSIX rename can replace an existing empty directory. Reserve the
    // destination name exclusively after both engines have closed.
    #[cfg(unix)]
    fs::create_dir(dest)?;
    fs::rename(staging.path(), dest)?;
    #[cfg(unix)]
    fs::File::open(parent)?.sync_all()?;
    println!("Exported {count} rows to {}", dest.display());
    Ok(())
}
