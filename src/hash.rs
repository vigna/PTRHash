use std::ops::BitXor;

use crate::{reduce::Reduce, Key};
use murmur2::murmur64a;

/// Strong type for 64bit hashes.
///
/// We want to limit what kind of operations we do on hashes.
/// In particular we only need:
/// - xor, for h(x) ^ h(k)
/// - reduce: h(x) -> [0, n)
/// - ord: h(x) < p1 * n
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Default)]
pub struct Hash(u64);

impl Hash {
    pub fn new(v: u64) -> Self {
        Hash(v)
    }
    pub fn get(&self) -> u64 {
        self.0
    }
    pub fn get_low(&self) -> u32 {
        self.0 as u32
    }
    pub fn get_high(&self) -> u32 {
        (self.0 >> 32) as u32
    }
    pub fn reduce<R: Reduce>(self, d: R) -> usize {
        d.reduce(self)
    }
}

impl BitXor for Hash {
    type Output = Self;

    fn bitxor(self, rhs: Self) -> Self::Output {
        Self(self.0 ^ rhs.0)
    }
}

pub trait Hasher {
    fn hash(x: &Key, seed: u64) -> Hash;
}

pub struct Murmur;

impl Hasher for Murmur {
    fn hash(x: &Key, seed: u64) -> Hash {
        Hash(murmur64a(
            // Pass the key as a byte slice.
            unsafe {
                std::slice::from_raw_parts(x as *const Key as *const u8, std::mem::size_of::<Key>())
            },
            seed,
        ))
    }
}

/// Xor the key and seed.
pub struct XorHash;

impl Hasher for XorHash {
    fn hash(x: &Key, seed: u64) -> Hash {
        Hash(*x ^ seed)
    }
}

/// Multiply the key by a mixing constant.
pub struct MulHash;

impl Hasher for MulHash {
    fn hash(x: &Key, _seed: u64) -> Hash {
        // Reuse the mixing constant from MurmurHash.
        const M_64: u64 = 0xc6a4a7935bd1e995;
        Hash(*x * M_64)
    }
}

/// Pass the key through unchanged.
pub struct NoHash;

impl Hasher for NoHash {
    fn hash(x: &Key, _seed: u64) -> Hash {
        Hash(*x)
    }
}