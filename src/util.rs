use super::*;
use rand::{thread_rng, Rng};
use rayon::prelude::*;
use rdst::RadixSort;
use std::{hint::black_box, time::SystemTime};

pub fn log_duration(name: &str, start: Instant) -> Instant {
    eprintln!(
        "{}",
        format!("{name:>12}: {:>13.2?}s", start.elapsed().as_secs_f32()).bold()
    );
    Instant::now()
}

pub fn generate_keys(n: usize) -> Vec<Key> {
    let start = Instant::now();
    let keys = loop {
        let start = Instant::now();
        let keys: Vec<_> = (0..n)
            .into_par_iter()
            .map_init(thread_rng, |rng, _| rng.gen())
            .collect();
        let start = log_duration("┌   gen keys", start);
        let mut keys2: Vec<_> = keys.par_iter().copied().collect();
        let start = log_duration("├      clone", start);
        keys2.radix_sort_unstable();
        let start = log_duration("├       sort", start);
        let distinct = keys2.par_windows(2).all(|w| w[0] < w[1]);
        log_duration("├ duplicates", start);
        if distinct {
            break keys;
        }
        eprintln!("DUPLICATE KEYS GENERATED");
    };
    log_duration("generatekeys", start);
    keys
}

#[must_use]
pub fn bench_index(loops: usize, keys: &Vec<u64>, index: impl Fn(&Key) -> usize) -> f32 {
    let start = SystemTime::now();
    let mut sum = 0;
    for _ in 0..loops {
        for key in keys {
            sum += index(key);
        }
    }
    black_box(sum);
    start.elapsed().unwrap().as_nanos() as f32 / (loops * keys.len()) as f32
}

#[must_use]
pub fn time<F>(loops: usize, keys: &[u64], f: F) -> f32
where
    F: Fn() -> usize,
{
    let start = SystemTime::now();
    for _ in 0..loops {
        black_box(f());
    }
    start.elapsed().unwrap().as_nanos() as f32 / (loops * keys.len()) as f32
}