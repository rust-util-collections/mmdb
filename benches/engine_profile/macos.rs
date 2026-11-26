use std::{io, mem::MaybeUninit, process};

pub(super) fn rss_kib(field: &str) -> Option<u64> {
    let bytes = match field {
        "VmRSS:" => {
            let mut info = MaybeUninit::<libc::proc_taskinfo>::uninit();
            let size = size_of::<libc::proc_taskinfo>() as libc::c_int;
            // SAFETY: PROC_PIDTASKINFO writes the specified task info struct.
            // Only read the result after confirming the complete byte count.
            let count = unsafe {
                libc::proc_pidinfo(
                    process::id() as libc::pid_t,
                    libc::PROC_PIDTASKINFO,
                    0,
                    info.as_mut_ptr().cast(),
                    size,
                )
            };
            assert_eq!(count, size, "proc_pidinfo: {}", io::Error::last_os_error());
            // SAFETY: proc_pidinfo just initialized the full struct.
            unsafe { info.assume_init() }.pti_resident_size
        }
        "VmHWM:" => {
            let mut usage = MaybeUninit::<libc::rusage>::uninit();
            // SAFETY: getrusage initializes the output struct on success.
            let status = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
            assert_eq!(status, 0, "getrusage: {}", io::Error::last_os_error());
            // SAFETY: getrusage succeeded. Darwin reports max RSS in bytes.
            unsafe { usage.assume_init() }.ru_maxrss.try_into().unwrap()
        }
        _ => return None,
    };
    Some(bytes / 1024)
}

pub(super) fn cpu_ticks() -> u64 {
    let mut times = MaybeUninit::<libc::tms>::uninit();
    // SAFETY: times initializes the output struct on success.
    let status = unsafe { libc::times(times.as_mut_ptr()) };
    assert_ne!(
        status,
        libc::clock_t::MAX,
        "times: {}",
        io::Error::last_os_error()
    );
    // SAFETY: times succeeded. Only the current process enters the sum.
    let times = unsafe { times.assume_init() };
    times.tms_utime + times.tms_stime
}
