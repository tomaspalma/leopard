use std::{
    cmp::max,
    f64::consts::LN_2,
    hash::{Hash, Hasher},
    marker::PhantomData,
    time::{Duration, Instant},
};

use bitvec::{bitvec, slice::BitSlice, vec::BitVec};
use std::collections::hash_map::DefaultHasher;

pub struct BloomFilter<T: ?Sized> {
    base: BitVec,
    seeds: [u64; 2],
    hashes: u64,
    _marker: PhantomData<T>,
    t_enc: Duration,
    t_dec: Duration,
}

impl<T> BloomFilter<T>
where
    T: ?Sized,
{
    #[inline]
    #[must_use]
    pub fn new(capacity: usize, fpr: f64) -> Self {
        assert!(
            (0.0..1.0).contains(&fpr) && fpr > 0.0,
            "false positive rate should be a ratio greater than 0.0"
        );

        let m = (-1.0f64 * capacity as f64 * fpr.ln() / (LN_2 * LN_2)).ceil() as usize;
        let k = (-1.0f64 * fpr.ln() / LN_2).ceil() as u64;

        Self {
            base: bitvec![0; max(m, 1)],
            seeds: [rand::random::<u64>(), rand::random::<u64>()],
            hashes: k,
            _marker: PhantomData,
            t_enc: Duration::from_secs(0),
            t_dec: Duration::from_secs(0),
        }
    }

    pub fn from_raw_parts(m: usize, k: u64) -> Self {
        assert!(m > 0 && k > 0, "m and k should be positive");

        Self {
            base: bitvec![0; max(m, 1)],
            seeds: [rand::random::<u64>(), rand::random::<u64>()],
            hashes: k,
            _marker: PhantomData,
            t_enc: Duration::from_secs(0),
            t_dec: Duration::from_secs(0),
        }
    }

    pub fn from_raw_parts_with_seeds(m: usize, k: u64, seeds: [u64; 2]) -> Self {
        assert!(m > 0 && k > 0, "m and k should be positive");
        Self {
            base: bitvec![0; m],
            seeds,
            hashes: k,
            _marker: PhantomData,
            t_enc: Duration::from_secs(0),
            t_dec: Duration::from_secs(0),
        }
    }

    pub fn from_bytes_with_seeds(m: usize, k: u64, seeds: [u64; 2], bytes: &[u8]) -> Self {
        let mut base = bitvec![0; m];

        for (byte_idx, byte) in bytes.iter().enumerate() {
            for bit_idx in 0..8 {
                let overall_idx = byte_idx * 8 + bit_idx;
                if overall_idx >= m {
                    break;
                }
                let is_set = (byte >> bit_idx) & 1 == 1;
                if is_set {
                    base.set(overall_idx, true);
                }
            }
        }

        Self {
            base,
            seeds,
            hashes: k,
            _marker: PhantomData,
            t_enc: Duration::from_secs(0),
            t_dec: Duration::from_secs(0),
        }
    }

    #[inline]
    pub fn bitslice(&self) -> &BitSlice {
        &self.base
    }

    #[inline]
    pub fn seeds(&self) -> [u64; 2] {
        self.seeds
    }

    pub fn hashes(&self) -> u64 {
        self.hashes
    }

    pub fn bit_len(&self) -> usize {
        self.base.len()
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut out = vec![0u8; self.base.len().div_ceil(8)];
        for i in 0..self.base.len() {
            if self.base[i] {
                out[i / 8] |= 1 << (i % 8);
            }
        }
        out
    }

    #[inline]
    pub fn t_enc(&self) -> Duration {
        self.t_enc
    }

    #[inline]
    pub fn t_dec(&self) -> Duration {
        self.t_dec
    }
}

impl<T> BloomFilter<T>
where
    T: ?Sized + Hash,
{
    fn hash_with_seed(value: &T, seed: u64) -> u64 {
        let mut hasher = DefaultHasher::new();
        seed.hash(&mut hasher);
        value.hash(&mut hasher);
        hasher.finish()
    }

    pub fn timed_contains(&mut self, value: &T) -> bool {
        let exec_time = Instant::now();
        let contains = self.contains(value);
        self.t_dec += exec_time.elapsed();
        contains
    }

    #[inline]
    pub fn contains(&self, value: &T) -> bool {
        let h = (
            Self::hash_with_seed(value, self.seeds[0]),
            Self::hash_with_seed(value, self.seeds[1]),
        );

        (0..self.hashes).all(|i| {
            let bit =
                usize::try_from(h.0.wrapping_add(i.wrapping_mul(h.1))).unwrap() % self.base.len();
            self.base[bit]
        })
    }

    #[inline]
    pub fn timed_insert(&mut self, value: &T) {
        let exec_time = Instant::now();
        self.insert(value);
        self.t_enc += exec_time.elapsed();
    }

    #[inline]
    pub fn insert(&mut self, value: &T) {
        let h = (
            Self::hash_with_seed(value, self.seeds[0]),
            Self::hash_with_seed(value, self.seeds[1]),
        );

        (0..self.hashes).for_each(|i| {
            let bit =
                usize::try_from(h.0.wrapping_add(i.wrapping_mul(h.1))).unwrap() % self.base.len();
            self.base.set(bit, true);
        });
    }
}
