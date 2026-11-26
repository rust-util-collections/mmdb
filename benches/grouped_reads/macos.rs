use std::{fs, io, mem::MaybeUninit, os::fd::AsRawFd, path::Path, process, ptr};

pub(super) fn storage_reads() -> u64 {
    let mut usage = MaybeUninit::<libc::rusage_info_v2>::uninit();
    // SAFETY: the version selects the layout of the output buffer. Darwin's
    // rusage_info_t* parameter accepts the address of that struct.
    let status = unsafe {
        libc::proc_pid_rusage(
            process::id() as libc::pid_t,
            libc::RUSAGE_INFO_V2,
            usage.as_mut_ptr().cast(),
        )
    };
    assert_eq!(status, 0, "proc_pid_rusage: {}", io::Error::last_os_error());
    // SAFETY: proc_pid_rusage initialized the v2 output on success.
    unsafe { usage.assume_init() }.ri_diskio_bytesread
}

pub(super) fn evict_os_cache(files: &[fs::DirEntry]) {
    for entry in files {
        let path = entry.path();
        invalidate_sst(&path).unwrap_or_else(|e| panic!("cannot evict {}: {e}", path.display()));
    }
}

fn invalidate_sst(path: &Path) -> io::Result<()> {
    let file = fs::File::open(path)?;
    let len = usize::try_from(file.metadata()?.len()).map_err(io::Error::other)?;
    if len == 0 {
        return Ok(());
    }
    // SAFETY: the immutable fixture is open for reading. Nothing accesses the
    // mapping's bytes; it only identifies this file's pages for invalidation.
    let mapping = unsafe {
        libc::mmap(
            ptr::null_mut(),
            len,
            libc::PROT_READ,
            libc::MAP_SHARED,
            file.as_raw_fd(),
            0,
        )
    };
    if mapping == libc::MAP_FAILED {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: both calls use exactly the address and length returned by mmap.
    // Save any invalidation error before releasing the mapping.
    let invalidate = unsafe { libc::msync(mapping, len, libc::MS_INVALIDATE) };
    let error = (invalidate != 0).then(io::Error::last_os_error);
    // SAFETY: this is the mapping's only owner and its final use.
    if unsafe { libc::munmap(mapping, len) } != 0 {
        return Err(io::Error::last_os_error());
    }
    error.map_or(Ok(()), Err)
}
