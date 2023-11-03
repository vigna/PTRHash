#![allow(dead_code)]

use crate::types::{BucketIdx, BucketSlice};

use super::*;
use bitvec::{slice::BitSlice, vec::BitVec};

impl<F: Packed, Rm: Reduce, Rn: Reduce, Hx: Hasher, const T: bool, const PT: bool>
    PTHash<F, Rm, Rn, Hx, T, PT>
{
    pub fn displace(
        &self,
        hashes: &[Hash],
        starts: &BucketVec<usize>,
        bucket_order: &[BucketIdx],
        pilots: &mut BucketVec<u8>,
        taken: &mut Vec<BitVec>,
    ) -> bool {
        let kmax = 256;

        // Reset output-memory.
        pilots.clear();
        pilots.resize(self.b_total, 0);
        for taken in taken.iter_mut() {
            taken.clear();
            taken.resize(self.s, false);
        }
        taken.resize(self.num_parts, bitvec![0; self.s]);

        let mut total_displacements = 0;

        for part in 0..self.num_parts {
            let (ok, cnt) = self.displace_part(
                part,
                hashes,
                &starts[part * self.b..(part + 1) * self.b],
                &bucket_order[part * self.b..(part + 1) * self.b],
                &mut pilots[part * self.b..(part + 1) * self.b],
                &mut taken[part],
            );
            total_displacements += cnt;
            if !ok {
                return false;
            }
        }

        // Clear the last \r line.
        eprint!("\x1b[K");
        let max = pilots.iter().copied().max().unwrap();
        assert!(
            (max as Pilot) < kmax,
            "Max pilot found is {max} which is not less than {kmax}"
        );

        let sum_pilots = pilots.iter().map(|&k| k as Pilot).sum::<Pilot>();

        eprintln!(
            "  displ./bkt: {:>14.3}",
            total_displacements as f32 / self.b_total as f32
        );
        eprintln!(
            "   avg pilot: {:>14.3}",
            sum_pilots as f32 / self.b_total as f32
        );
        true
    }

    fn displace_part(
        &self,
        part: usize,
        hashes: &[Hash],
        starts: &BucketSlice<usize>,
        bucket_order: &[BucketIdx],
        pilots: &mut [u8],
        taken: &mut BitSlice,
    ) -> (bool, usize) {
        let kmax = 256;

        let mut slots = vec![BucketIdx::NONE; self.s];
        let bucket_len = |b: BucketIdx| starts[b + 1] - starts[b];

        let max_bucket_len = bucket_len(bucket_order[0]);

        let mut stack = vec![];

        let positions = |b: BucketIdx, p: Pilot| unsafe {
            let hp = self.hash_pilot(p);
            hashes
                .get_unchecked(starts[b]..starts[b + 1])
                .iter()
                .map(move |&hx| self.position_in_part_hp(hx, hp))
        };
        let mut duplicate_positions = {
            let mut positions_tmp = vec![0; max_bucket_len];
            move |b: BucketIdx, p: Pilot| {
                positions_tmp.clear();
                positions(b, p).collect_into(&mut positions_tmp);
                positions_tmp.sort_unstable();
                !positions_tmp.partition_dedup().1.is_empty()
            }
        };

        let mut recent = [BucketIdx::NONE; 4];
        let mut total_displacements = 0;

        // TODO: Permute the buckets by bucket_order up-front to make memory access linear afterwards.
        for (i, &b) in bucket_order.iter().enumerate() {
            // Check for duplicate hashes inside bucket.
            let bucket = &hashes[starts[b]..starts[b + 1]];
            if bucket.is_empty() {
                pilots[b] = 0;
                continue;
            }
            let b_len = bucket.len();

            let mut displacements = 0usize;

            stack.push(b);
            recent.fill(BucketIdx::NONE);
            let mut recent_idx = 0;
            recent[0] = b;

            'b: while let Some(b) = stack.pop() {
                if displacements > self.s && displacements.is_power_of_two() {
                    eprintln!(
                        "part {part:>6} bucket {:>5.2}%  chain: {displacements:>9}",
                        100. * (part * self.b + i) as f32 / self.b_total as f32,
                    );
                    if displacements >= 10 * self.s {
                        panic!(
                            "\
Too many displacements. Aborting!
Possible causes:
- Too many elements in part.
- Not enough empty slots => lower alpha.
- Not enough buckets     => increase c.
- Not enough entropy     => fix algorithm.
"
                        );
                    }
                }

                // Check for a solution without collisions.

                let bucket = unsafe { &hashes.get_unchecked(starts[b]..starts[b + 1]) };
                let b_positions = |hp: Hash| {
                    bucket
                        .iter()
                        .map(move |&hx| self.position_in_part_hp(hx, hp))
                };

                // Hot-path for when there are no collisions, which is most of the buckets.
                if let Some((p, hp)) = self.find_pilot(kmax, bucket, taken) {
                    pilots[b] = p as u8;
                    for p in b_positions(hp) {
                        unsafe {
                            // Taken is already filled by fine_pilot.
                            *slots.get_unchecked_mut(p) = b;
                        }
                    }
                    continue 'b;
                }

                let p = pilots[b] as Pilot + 1;
                // (worst colliding bucket size, p)
                let mut best = (usize::MAX, u64::MAX);

                if best.0 != 0 {
                    'p: for delta in 0u64..kmax {
                        let p = (p + delta) % kmax;
                        let hp = self.hash_pilot(p);
                        let mut collision_score = 0;
                        for p in b_positions(hp) {
                            let s = unsafe { *slots.get_unchecked(p) };
                            // Heavily penalize recently moved buckets.
                            let new_score = if s.is_none() {
                                continue;
                            } else if recent.contains(&s) {
                                continue 'p;
                            } else {
                                bucket_len(s).pow(2)
                            };
                            collision_score += new_score;
                            if collision_score >= best.0 {
                                continue 'p;
                            }
                        }
                        // This check takes 2% of times even though it almost
                        // always passes. Can we delay it to filling of the
                        // positions table, and backtrack if needed.
                        if !duplicate_positions(b, p) {
                            best = (collision_score, p);
                            // Since we already checked for a collision-free solution,
                            // the next best is a single collision of size b_len.
                            if collision_score == b_len * b_len {
                                break;
                            }
                        }
                    }
                }

                let (_collision_score, p) = best;
                pilots[b] = p as u8;
                let hp = self.hash_pilot(p);

                // Drop the collisions and set the new pilot.
                for p in b_positions(hp) {
                    // THIS IS A HOT INSTRUCTION.
                    let b2 = slots[p];
                    if b2.is_some() {
                        // FIXME: This assertion still fails from time to time but it really shouldn't.
                        assert!(b2 != b);
                        // DROP BUCKET b
                        // eprintln!("{i:>8}/{:>8} Drop bucket {b2:>8}", self.n);
                        stack.push(b2);
                        displacements += 1;
                        for p2 in positions(b2, pilots[b2] as Pilot) {
                            unsafe {
                                *slots.get_unchecked_mut(p2) = BucketIdx::NONE;
                                taken.set_unchecked(p2, false);
                            }
                        }
                    }
                    // eprintln!("Set slot {:>8} to {:>8}", p, b);
                    unsafe {
                        *slots.get_unchecked_mut(p) = b;
                        taken.set_unchecked(p, true);
                    }
                }

                recent_idx += 1;
                recent_idx %= 4;
                recent[recent_idx] = b;
            }
            total_displacements += displacements;
            if i % (1 << 14) == 0 {
                eprint!(
                    "part {part:>6} bucket {:>5.2}%\r",
                    100. * (part * self.b + i) as f32 / self.b_total as f32,
                );
            }
        }
        (true, total_displacements)
    }
}
