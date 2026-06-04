use std::{
    cmp::max,
    collections::hash_map::DefaultHasher,
    f64::consts::LN_2,
    hash::{Hash, Hasher},
    marker::PhantomData,
    time::{Duration, Instant},
};

use bitvec::{bitvec, slice::BitSlice, vec::BitVec};

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
    fn hash_with_seed(seed: u64, value: &(impl Hash + ?Sized)) -> u64 {
        let mut hasher = DefaultHasher::new();
        seed.hash(&mut hasher);
        value.hash(&mut hasher);
        hasher.finish()
    }

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
            seeds: [0, 1],
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
            seeds: [0, 1],
            hashes: k,
            _marker: PhantomData,
            t_enc: Duration::from_secs(0),
            t_dec: Duration::from_secs(0),
        }
    }

    pub fn from_raw_parts_with_seeds(m: usize, k: u64, seeds: [u64; 2]) -> Self {
        assert!(m > 0 && k > 0, "m and k should be positive");

        Self {
            base: bitvec![0; max(m, 1)],
            seeds,
            hashes: k,
            _marker: PhantomData,
            t_enc: Duration::from_secs(0),
            t_dec: Duration::from_secs(0),
        }
    }

    pub fn from_raw_bits(m: usize, k: u64, bits: &[u8], seeds: [u64; 2]) -> Self {
        let mut base = bitvec![0; m];
        for (byte_idx, &byte) in bits.iter().enumerate() {
            for bit_idx in 0..8 {
                let global_idx = byte_idx * 8 + bit_idx;
                if global_idx >= m {
                    break;
                }
                base.set(global_idx, (byte >> bit_idx) & 1 == 1);
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
    pub fn timed_contains(&mut self, value: &T) -> bool {
        let exec_time = Instant::now();
        let contains = self.contains(value);
        self.t_dec += exec_time.elapsed();
        contains
    }

    #[inline]
    pub fn contains(&self, value: &T) -> bool {
        let h = (
            Self::hash_with_seed(self.seeds[0], value),
            Self::hash_with_seed(self.seeds[1], value),
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
            Self::hash_with_seed(self.seeds[0], value),
            Self::hash_with_seed(self.seeds[1], value),
        );

        (0..self.hashes).for_each(|i| {
            let bit =
                usize::try_from(h.0.wrapping_add(i.wrapping_mul(h.1))).unwrap() % self.base.len();
            self.base.set(bit, true);
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_membership_and_no_false_positives() {
        let mut bloom = BloomFilter::new(100, 0.01);

        assert!(!bloom.contains("1"));
        assert!(!bloom.contains("2"));

        bloom.insert("1");
        assert!(bloom.contains("1"));
    }

    #[test]
    fn test_cross_reconstruction() {
        let seeds = [42u64, 99u64];
        let mut sender = BloomFilter::<String>::from_raw_parts_with_seeds(128, 1, seeds);
        sender.insert(&"hello".to_string());
        sender.insert(&"world".to_string());

        let bits: Vec<u8> = (0..sender.bitslice().len())
            .step_by(8)
            .map(|start| {
                let end = (start + 8).min(sender.bitslice().len());
                (start..end).fold(0u8, |b, i| {
                    if sender.bitslice()[i] { b | (1 << (i - start)) } else { b }
                })
            })
            .collect();

        let receiver = BloomFilter::<String>::from_raw_bits(128, 1, &bits, seeds);
        assert!(receiver.contains(&"hello".to_string()));
        assert!(receiver.contains(&"world".to_string()));
        assert!(!receiver.contains(&"other".to_string()));
    }
}
