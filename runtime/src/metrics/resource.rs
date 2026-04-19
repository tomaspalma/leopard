#[cfg(unix)]
use libc::{getrusage, rusage, RUSAGE_SELF};

#[derive(Clone, Copy, Debug)]
pub struct ProcessUsageSnapshot {
    pub cpu_seconds: f64,
    pub rss_bytes: u64,
}

#[cfg(unix)]
pub fn process_usage_snapshot() -> Option<ProcessUsageSnapshot> {
    let mut usage = rusage {
        ru_utime: libc::timeval {
            tv_sec: 0,
            tv_usec: 0,
        },
        ru_stime: libc::timeval {
            tv_sec: 0,
            tv_usec: 0,
        },
        ru_maxrss: 0,
        ru_ixrss: 0,
        ru_idrss: 0,
        ru_isrss: 0,
        ru_minflt: 0,
        ru_majflt: 0,
        ru_nswap: 0,
        ru_inblock: 0,
        ru_oublock: 0,
        ru_msgsnd: 0,
        ru_msgrcv: 0,
        ru_nsignals: 0,
        ru_nvcsw: 0,
        ru_nivcsw: 0,
    };

    let rc = unsafe { getrusage(RUSAGE_SELF, &mut usage as *mut rusage) };
    if rc != 0 {
        return None;
    }

    let user_seconds = usage.ru_utime.tv_sec as f64 + usage.ru_utime.tv_usec as f64 / 1_000_000.0;
    let sys_seconds = usage.ru_stime.tv_sec as f64 + usage.ru_stime.tv_usec as f64 / 1_000_000.0;

    #[cfg(target_os = "macos")]
    let rss_bytes = usage.ru_maxrss.max(0) as u64;

    #[cfg(not(target_os = "macos"))]
    let rss_bytes = (usage.ru_maxrss.max(0) as u64) * 1024;

    Some(ProcessUsageSnapshot {
        cpu_seconds: user_seconds + sys_seconds,
        rss_bytes,
    })
}

#[cfg(not(unix))]
pub fn process_usage_snapshot() -> Option<ProcessUsageSnapshot> {
    None
}
