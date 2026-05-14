use bitvec::prelude::{BitVec, Lsb0};
use std::hash::{BuildHasher, Hash};

use super::builder::Scratch;
use super::hashing::{for_each_set_bit_u128_parts, standard_equation_w64, xor_words};
use super::params::Params;

#[derive(Debug, Clone)]
pub struct RibbonFilter<S> {
    params: Params,
    build_hasher: S,
    z: BitVec<u64, Lsb0>,
    stride_words: usize,
}

impl<S> RibbonFilter<S>
where
    S: BuildHasher + Clone,
{
    pub(super) fn new(params: Params, build_hasher: S, z: Vec<u64>) -> Self {
        let stride_words = params.fingerprint_words();
        let z = BitVec::<u64, Lsb0>::from_vec(z);
        Self { params, build_hasher, z, stride_words }
    }

    /// Reconstruct a filter from its serialized components.
    pub fn from_raw_parts(params: Params, build_hasher: S, z_raw: Vec<u64>) -> Self {
        Self::new(params, build_hasher, z_raw)
    }

    /// Return the raw backing array for serialization.
    pub fn z_raw(&self) -> &[u64] {
        self.z.as_raw_slice()
    }

    pub fn params(&self) -> Params {
        self.params
    }

    pub fn new_scratch(&self) -> Scratch {
        Scratch::new(self.stride_words)
    }

    pub fn contains<Q: Hash + ?Sized>(&self, key: &Q) -> bool {
        let mut scratch = self.new_scratch();
        self.contains_in(key, &mut scratch)
    }

    pub fn contains_in<Q: Hash + ?Sized>(&self, key: &Q, scratch: &mut Scratch) -> bool {
        debug_assert_eq!(scratch.fingerprint.len(), self.stride_words);
        debug_assert_eq!(scratch.acc.len(), self.stride_words);
        scratch.reset();

        let equation = standard_equation_w64(
            &self.build_hasher,
            key,
            self.params.seed,
            &self.params,
            &mut scratch.fingerprint,
        );

        for_each_set_bit_u128_parts(equation.coeff_lo, equation.coeff_hi, |offset| {
            let row_index = equation.start + offset;
            if row_index < self.params.m {
                let row = self.z_row(row_index);
                xor_words(&mut scratch.acc, row);
            }
        });

        scratch.acc == scratch.fingerprint
    }

    fn z_row(&self, row: usize) -> &[u64] {
        let start = row * self.stride_words;
        &self.z.as_raw_slice()[start..start + self.stride_words]
    }
}
