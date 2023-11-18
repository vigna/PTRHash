//! Construct a PtrHash instance on 10^11 keys in memory.
//! Using 6 threads this takes around 90 minutes.
//!
//! NOTE: This requires somewhere between 32 and 64GB of memory.
use std::hint::black_box;
use std::io::stderr;

use bitvec::bitvec;
use dsi_progress_logger::ProgressLog;
use dsi_progress_logger::ProgressLogger;
use ptr_hash::{hash::*, tiny_ef::TinyEf, PtrHash, PtrHashParams};
use rayon::prelude::*;
use sux::bits::BitFieldVec;
use sux::traits::BitFieldSlice;

fn main() {
    stderrlog::new().verbosity(2).init().unwrap();
    let n = 1_000_000_000;
    let n_query = 1 << 27;
    let range = 0..n as u64;
    let keys = range.clone().into_par_iter();
    let ptr_hash = PtrHash::<_, TinyEf, Murmur2_64, _>::new_from_par_iter(
        n,
        keys.clone(),
        PtrHashParams {
            c: 10.,
            // ~10GB of keys per shard.
            keys_per_shard: 1 << 29,
            shard_to_disk: false,
            ..Default::default()
        },
    );

    let mut pl = ProgressLogger::default();
    pl.start("");
    let values = BitFieldVec::<usize>::new((n - 1).ilog2() as usize + 1, n as usize);
    for key in 0..n_query {
        let idx = ptr_hash.index_minimal(&key);
        black_box(values.get(idx as usize));
    }
    pl.done_with_count(n_query as usize);
}
