#[cfg(unix)]
use libc::{RUSAGE_SELF, getrusage, rusage};
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Clone, Copy, Debug)]
pub struct ProcessUsageSnapshot {
    pub cpu_delta_seconds: f64,
    pub rss_bytes: u64,
}

#[cfg(unix)]
static LAST_CPU_BITS: AtomicU64 = AtomicU64::new(0);

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
    let cpu_total = user_seconds + sys_seconds;

    let prev_bits = LAST_CPU_BITS.swap(cpu_total.to_bits(), Ordering::Relaxed);
    let cpu_delta = (cpu_total - f64::from_bits(prev_bits)).max(0.0);

    // Peak resident set size. On macOS/other Unix, getrusage's ru_maxrss is
    // already the lifetime peak; on Linux we read VmHWM to match.
    #[cfg(target_os = "linux")]
    let rss_bytes = read_peak_rss_bytes().unwrap_or(0);

    #[cfg(target_os = "macos")]
    let rss_bytes = usage.ru_maxrss.max(0) as u64;

    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    let rss_bytes = (usage.ru_maxrss.max(0) as u64) * 1024;

    Some(ProcessUsageSnapshot {
        cpu_delta_seconds: cpu_delta,
        rss_bytes,
    })
}

// Peak resident set size (high-water mark over the process lifetime), so the
// measurement captures the maximum footprint during a run rather than whatever
// RSS happens to be at convergence. One process per run keeps the peak clean.
#[cfg(target_os = "linux")]
fn read_peak_rss_bytes() -> Option<u64> {
    let content = std::fs::read_to_string("/proc/self/status").ok()?;
    for line in content.lines() {
        if line.starts_with("VmHWM:") {
            let kb: u64 = line.split_whitespace().nth(1)?.parse().ok()?;
            return Some(kb * 1024);
        }
    }
    None
}

#[cfg(not(unix))]
pub fn process_usage_snapshot() -> Option<ProcessUsageSnapshot> {
    None
}
