//! Apache DataSketches Theta sketches, as Iceberg's
//! `apache-datasketches-theta-v1` statistics blobs store them.
//!
//! A sketch keeps the 63-bit hashes of distinct values below a threshold
//! `theta`; their count divided by `theta` estimates the number of distinct
//! values. Hashing and the compact serialization follow DataSketches Java
//! (`MurmurHash3_x64_128` with seed 9001, serial version 3), so a sketch
//! Flow writes reads as an ordinary sketch in Java, Trino and Spark.

use anyhow::{Result, bail, ensure};

/// DataSketches' default update seed. Sketches of different seeds never mix.
pub const DEFAULT_SEED: u64 = 9001;

/// Nominal entries: Java's default `lgK = 12`.
pub const NOMINAL_ENTRIES: usize = 4096;

/// Theta at its maximum: every hash is retained (exact mode).
pub const MAX_THETA: u64 = i64::MAX as u64;

const SERIAL_VERSION: u8 = 3;
const COMPACT_FAMILY: u8 = 3;
const FLAG_READ_ONLY: u8 = 2;
const FLAG_EMPTY: u8 = 4;
const FLAG_COMPACT: u8 = 8;
const FLAG_ORDERED: u8 = 16;
const FLAG_SINGLE_ITEM: u8 = 32;

fn fmix64(mut k: u64) -> u64 {
    k ^= k >> 33;
    k = k.wrapping_mul(0xff51_afd7_ed55_8ccd);
    k ^= k >> 33;
    k = k.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    k ^ (k >> 33)
}

/// MurmurHash3 x64 128, as DataSketches Java computes it for byte arrays:
/// both halves start at `seed`, blocks and tail are little endian.
pub fn murmur3_x64_128(data: &[u8], seed: u64) -> (u64, u64) {
    const C1: u64 = 0x87c3_7b91_1142_53d5;
    const C2: u64 = 0x4cf5_ad43_2745_937f;
    let (mut h1, mut h2) = (seed, seed);
    let mut blocks = data.chunks_exact(16);
    for block in &mut blocks {
        let k1 = u64::from_le_bytes(block[..8].try_into().expect("8 bytes"));
        let k2 = u64::from_le_bytes(block[8..].try_into().expect("8 bytes"));
        h1 ^= k1.wrapping_mul(C1).rotate_left(31).wrapping_mul(C2);
        h1 = h1
            .rotate_left(27)
            .wrapping_add(h2)
            .wrapping_mul(5)
            .wrapping_add(0x52dc_e729);
        h2 ^= k2.wrapping_mul(C2).rotate_left(33).wrapping_mul(C1);
        h2 = h2
            .rotate_left(31)
            .wrapping_add(h1)
            .wrapping_mul(5)
            .wrapping_add(0x3849_5ab5);
    }
    let tail = blocks.remainder();
    let (mut k1, mut k2) = (0u64, 0u64);
    for (index, byte) in tail.iter().enumerate() {
        if index < 8 {
            k1 |= u64::from(*byte) << (8 * index);
        } else {
            k2 |= u64::from(*byte) << (8 * (index - 8));
        }
    }
    if tail.len() > 8 {
        h2 ^= k2.wrapping_mul(C2).rotate_left(33).wrapping_mul(C1);
    }
    if !tail.is_empty() {
        h1 ^= k1.wrapping_mul(C1).rotate_left(31).wrapping_mul(C2);
    }
    let length = data.len() as u64;
    h1 ^= length;
    h2 ^= length;
    h1 = h1.wrapping_add(h2);
    h2 = h2.wrapping_add(h1);
    h1 = fmix64(h1);
    h2 = fmix64(h2);
    h1 = h1.wrapping_add(h2);
    h2 = h2.wrapping_add(h1);
    (h1, h2)
}

/// The 16-bit hash of a seed that every serialized sketch carries.
pub fn seed_hash(seed: u64) -> u16 {
    (murmur3_x64_128(&seed.to_le_bytes(), 0).0 & 0xffff) as u16
}

/// A value's 63-bit Theta hash, or `None` for input Theta sketches skip:
/// empty input, and the reserved hash 0.
pub fn hash(value: &[u8]) -> Option<u64> {
    if value.is_empty() {
        return None;
    }
    let hash = murmur3_x64_128(value, DEFAULT_SEED).0 >> 1;
    (hash != 0).then_some(hash)
}

/// A Theta sketch over the default seed with [`NOMINAL_ENTRIES`] entries.
/// Updates collect hashes below `theta`; past twice the nominal size the
/// smallest are kept and `theta` lowers to the next, as Java's rebuild does.
/// Every hash below `theta` is retained, so estimates and unions agree with
/// Java's even where the retained count differs between rebuilds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ThetaSketch {
    theta: u64,
    hashes: Vec<u64>,
    sorted: bool,
    /// Whether any value was offered, including ones theta rejected.
    updated: bool,
}

impl Default for ThetaSketch {
    fn default() -> Self {
        Self {
            theta: MAX_THETA,
            hashes: Vec::new(),
            sorted: true,
            updated: false,
        }
    }
}

impl ThetaSketch {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn theta(&self) -> u64 {
        self.theta
    }

    /// Whether no value was ever offered.
    pub fn is_empty(&self) -> bool {
        !self.updated
    }

    /// Offer one value's serialized bytes.
    pub fn update(&mut self, value: &[u8]) {
        if let Some(hash) = hash(value) {
            self.update_hash(hash);
        }
    }

    /// Offer a hash from [`hash`].
    pub fn update_hash(&mut self, hash: u64) {
        self.updated = true;
        if hash == 0 || hash >= self.theta {
            return;
        }
        self.hashes.push(hash);
        self.sorted = false;
        if self.hashes.len() >= 2 * NOMINAL_ENTRIES {
            self.normalize();
        }
    }

    /// Sort, drop duplicates, and keep at most the nominal entries.
    fn normalize(&mut self) {
        self.sort();
        if self.hashes.len() > NOMINAL_ENTRIES {
            self.theta = self.hashes[NOMINAL_ENTRIES];
            self.hashes.truncate(NOMINAL_ENTRIES);
        }
    }

    fn sort(&mut self) {
        if !self.sorted {
            self.hashes.sort_unstable();
            self.hashes.dedup();
            self.sorted = true;
        }
    }

    /// Merge another sketch of the same seed into this one.
    pub fn union(&mut self, other: &ThetaSketch) {
        self.updated |= other.updated;
        self.theta = self.theta.min(other.theta);
        let theta = self.theta;
        self.hashes.retain(|hash| *hash < theta);
        self.hashes
            .extend(other.hashes.iter().copied().filter(|hash| *hash < theta));
        self.sorted = false;
        self.normalize();
    }

    /// Retained distinct hashes, after normalizing.
    pub fn retained(&mut self) -> usize {
        self.sort();
        self.hashes.len()
    }

    /// The estimated number of distinct values.
    pub fn estimate(&mut self) -> f64 {
        let retained = self.retained() as f64;
        if self.theta == MAX_THETA {
            return retained;
        }
        retained / (self.theta as f64 / MAX_THETA as f64)
    }

    /// The `ndv` blob property: the estimate truncated, as Iceberg writes it.
    pub fn ndv(&mut self) -> u64 {
        self.estimate() as u64
    }

    /// The compact, ordered serial-version-3 form Java reads with
    /// `CompactSketch.wrap`.
    pub fn serialize(&mut self) -> Vec<u8> {
        self.sort();
        let seed = seed_hash(DEFAULT_SEED).to_le_bytes();
        let exact = self.theta == MAX_THETA;
        if self.hashes.is_empty() && exact {
            // Java writes an empty sketch without a seed hash.
            return vec![
                1,
                SERIAL_VERSION,
                COMPACT_FAMILY,
                0,
                0,
                FLAG_READ_ONLY | FLAG_EMPTY | FLAG_COMPACT | FLAG_ORDERED,
                0,
                0,
            ];
        }
        let flags = FLAG_READ_ONLY | FLAG_COMPACT | FLAG_ORDERED;
        if self.hashes.len() == 1 && exact {
            let mut bytes = vec![
                1,
                SERIAL_VERSION,
                COMPACT_FAMILY,
                0,
                0,
                flags | FLAG_SINGLE_ITEM,
                seed[0],
                seed[1],
            ];
            bytes.extend(self.hashes[0].to_le_bytes());
            return bytes;
        }
        let preamble = if exact { 2 } else { 3 };
        let mut bytes = Vec::with_capacity(8 * (preamble + self.hashes.len()));
        bytes.extend([
            preamble as u8,
            SERIAL_VERSION,
            COMPACT_FAMILY,
            0,
            0,
            flags,
            seed[0],
            seed[1],
        ]);
        bytes.extend((self.hashes.len() as u32).to_le_bytes());
        bytes.extend(1.0f32.to_le_bytes());
        if !exact {
            bytes.extend(self.theta.to_le_bytes());
        }
        for hash in &self.hashes {
            bytes.extend(hash.to_le_bytes());
        }
        bytes
    }

    /// Read a compact serial-version-3 sketch of the default seed.
    pub fn deserialize(bytes: &[u8]) -> Result<Self> {
        ensure!(
            bytes.len() >= 8,
            "theta sketch is shorter than its preamble"
        );
        let preamble = usize::from(bytes[0] & 0x3f);
        ensure!(
            bytes[1] == SERIAL_VERSION && bytes[2] == COMPACT_FAMILY,
            "not a compact serial-version-3 theta sketch"
        );
        let flags = bytes[5];
        ensure!(
            flags & 1 == 0,
            "big-endian theta sketches are not supported"
        );
        ensure!(
            flags & FLAG_COMPACT != 0,
            "theta sketch is not in compact form"
        );
        let stored_seed = u16::from_le_bytes([bytes[6], bytes[7]]);
        if flags & FLAG_EMPTY != 0 {
            return Ok(Self {
                updated: true,
                ..Self::default()
            });
        }
        ensure!(
            stored_seed == seed_hash(DEFAULT_SEED),
            "theta sketch uses another seed"
        );
        let long = |offset: usize| -> Result<u64> {
            bytes
                .get(offset..offset + 8)
                .map(|slice| u64::from_le_bytes(slice.try_into().expect("8 bytes")))
                .ok_or_else(|| anyhow::anyhow!("theta sketch is truncated"))
        };
        let (theta, count, start) = match preamble {
            1 => {
                ensure!(
                    flags & FLAG_SINGLE_ITEM != 0 || bytes.len() >= 16,
                    "theta sketch has no entries"
                );
                (MAX_THETA, 1, 8)
            }
            2 | 3 => {
                ensure!(bytes.len() >= 16, "theta sketch is truncated");
                let count = u32::from_le_bytes(bytes[8..12].try_into().expect("4 bytes")) as usize;
                if preamble == 2 {
                    (MAX_THETA, count, 16)
                } else {
                    (long(16)?, count, 24)
                }
            }
            other => bail!("unsupported theta preamble of {other} longs"),
        };
        ensure!(
            theta > 0 && theta <= MAX_THETA,
            "theta sketch threshold is out of range"
        );
        let mut hashes = Vec::with_capacity(count);
        for index in 0..count {
            let hash = long(start + 8 * index)?;
            ensure!(
                hash > 0 && hash < theta,
                "theta sketch entry is out of range"
            );
            hashes.push(hash);
        }
        // Keep every entry: Java retains more than the nominal size between
        // rebuilds, and the estimate depends on all of them.
        let mut sketch = Self {
            theta,
            hashes,
            sorted: false,
            updated: true,
        };
        sketch.sort();
        Ok(sketch)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn murmur_matches_the_reference_implementation() {
        let mut data = Vec::new();
        for length in 0..70u8 {
            for seed in [0u64, 9001, 0xdead_beef] {
                let (h1, h2) = murmur3_x64_128(&data, seed);
                let reference =
                    murmur3::murmur3_x64_128(&mut std::io::Cursor::new(&data), seed as u32)
                        .unwrap();
                assert_eq!(
                    (h1, h2),
                    (reference as u64, (reference >> 64) as u64),
                    "length {length} seed {seed}"
                );
            }
            data.push(length.wrapping_mul(37).wrapping_add(11));
        }
    }

    #[test]
    fn the_default_seed_hash_is_javas() {
        assert_eq!(seed_hash(DEFAULT_SEED), 0x93cc);
    }

    #[test]
    fn empty_and_single_item_sketches_use_javas_short_forms() {
        let mut empty = ThetaSketch::new();
        assert_eq!(empty.serialize(), [1, 3, 3, 0, 0, 0x1e, 0, 0]);
        assert!(
            ThetaSketch::deserialize(&empty.serialize())
                .unwrap()
                .hashes
                .is_empty()
        );
        let mut single = ThetaSketch::new();
        single.update(b"a");
        single.update(b"a");
        let bytes = single.serialize();
        assert_eq!(&bytes[..8], &[1, 3, 3, 0, 0, 0x3a, 0xcc, 0x93]);
        assert_eq!(bytes.len(), 16);
        let mut read = ThetaSketch::deserialize(&bytes).unwrap();
        assert_eq!(read.ndv(), 1);
        // Empty values are not counted, as in Java.
        let mut skipped = ThetaSketch::new();
        skipped.update(b"");
        assert_eq!(skipped.retained(), 0);
    }

    #[test]
    fn exact_sketches_count_distinct_values_and_round_trip() {
        let mut sketch = ThetaSketch::new();
        for value in 0..1000u64 {
            sketch.update(&(value % 700).to_le_bytes());
        }
        assert_eq!(sketch.ndv(), 700);
        let bytes = sketch.serialize();
        assert_eq!(bytes[0], 2);
        assert_eq!(&bytes[12..16], &[0, 0, 0x80, 0x3f]);
        assert_eq!(bytes.len(), 16 + 8 * 700);
        let mut read = ThetaSketch::deserialize(&bytes).unwrap();
        assert_eq!(read.ndv(), 700);
        assert_eq!(read.serialize(), bytes);
    }

    #[test]
    fn estimates_stay_within_three_standard_errors() {
        // Relative standard error of a Theta sketch is about 1 / sqrt(k).
        let bound = 3.0 / (NOMINAL_ENTRIES as f64).sqrt();
        for distinct in [10_000u64, 1_000_000] {
            let mut sketch = ThetaSketch::new();
            for value in 0..distinct {
                sketch.update(&value.to_le_bytes());
            }
            let estimate = sketch.estimate();
            let error = (estimate - distinct as f64).abs() / distinct as f64;
            assert!(error < bound, "{distinct}: estimate {estimate}");
            let bytes = sketch.serialize();
            assert_eq!(bytes[0], 3, "estimation mode stores theta");
            assert_eq!(
                ThetaSketch::deserialize(&bytes).unwrap().estimate(),
                estimate
            );
        }
    }

    #[test]
    fn unions_count_shared_values_once() {
        let (mut left, mut right, mut all) =
            (ThetaSketch::new(), ThetaSketch::new(), ThetaSketch::new());
        for value in 0..300_000u64 {
            let bytes = value.to_le_bytes();
            if value < 200_000 {
                left.update(&bytes);
            }
            if value >= 100_000 {
                right.update(&bytes);
            }
            all.update(&bytes);
        }
        left.union(&right);
        let bound = 3.0 / (NOMINAL_ENTRIES as f64).sqrt();
        let error = (left.estimate() - 300_000.0).abs() / 300_000.0;
        assert!(error < bound, "union estimate {}", left.estimate());
        // A union of exact sketches is exact.
        let (mut a, mut b) = (ThetaSketch::new(), ThetaSketch::new());
        a.update(b"x");
        a.update(b"y");
        b.update(b"y");
        b.update(b"z");
        a.union(&b);
        assert_eq!(a.ndv(), 3);
        let mut empty = ThetaSketch::new();
        empty.union(&ThetaSketch::new());
        assert!(empty.is_empty());
    }

    #[test]
    fn foreign_seeds_and_corrupt_sketches_are_rejected() {
        let mut sketch = ThetaSketch::new();
        sketch.update(b"a");
        sketch.update(b"b");
        let mut bytes = sketch.serialize();
        assert!(ThetaSketch::deserialize(&bytes[..20]).is_err());
        bytes[6] ^= 1;
        assert!(ThetaSketch::deserialize(&bytes).is_err());
    }
}
