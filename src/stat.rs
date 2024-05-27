use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

pub struct Stat {
    io_stat: IOStat,
    last_io_stat: IOStat,
}

struct IOStat {
    get: AtomicU64,
    put: AtomicU64,
    delete: AtomicU64,
}

impl Stat {
    pub fn new() -> Self {
        Self {
            io_stat: IOStat {
                get: AtomicU64::new(0),
                put: AtomicU64::new(0),
                delete: AtomicU64::new(0),
            },
            last_io_stat: IOStat {
                get: AtomicU64::new(0),
                put: AtomicU64::new(0),
                delete: AtomicU64::new(0),
            },
        }
    }

    pub fn calc_tps_and_output(&self, delta: Duration) -> String {
        let get = self.io_stat.get.load(Ordering::SeqCst);
        let put = self.io_stat.put.load(Ordering::SeqCst);
        let delete = self.io_stat.delete.load(Ordering::SeqCst);

        let delta_f64 = delta.as_secs_f64();

        let get_tps = (get - self.last_io_stat.get.load(Ordering::SeqCst)) as f64 / delta_f64;
        let put_tps = (put - self.last_io_stat.put.load(Ordering::SeqCst)) as f64 / delta_f64;
        let delete_tps =
            (delete - self.last_io_stat.delete.load(Ordering::SeqCst)) as f64 / delta_f64;

        // keep io stat snapshot
        self.last_io_stat.get.store(get, Ordering::SeqCst);
        self.last_io_stat.put.store(put, Ordering::SeqCst);
        self.last_io_stat.delete.store(delete, Ordering::SeqCst);

        return format!(
            "tps: [get={:.2}, put={:.2}, delete={:.2}]",
            get_tps, put_tps, delete_tps
        );
    }

    pub fn inc_put(&self, num: u64) {
        self.io_stat.put.fetch_add(num, Ordering::SeqCst);
    }

    pub fn inc_get(&self, num: u64) {
        self.io_stat.get.fetch_add(num, Ordering::SeqCst);
    }

    // pub fn inc_delete(&self, num: u32) {
    //     self.io_stat.delete.fetch_add(num, Ordering::SeqCst);
    // }
}
