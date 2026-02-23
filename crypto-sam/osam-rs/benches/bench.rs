extern crate criterion;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use oram::{
    path_oram::{
        PathOram, DEFAULT_BLOCKS_PER_BUCKET, DEFAULT_POSITIONS_PER_BLOCK,
        DEFAULT_STASH_OVERFLOW_SIZE,
    },
    Address, BlockSize, BlockValue, BucketSize, RecursionCutoff, StashSize,
};
use rand::{rngs::OsRng, rngs::StdRng, SeedableRng};
use std::time::Duration;

const BLOCK_SIZE: BlockSize = 64;
const CAPACITIES_TO_BENCHMARK: [Address; 6] =
    [1 << 21, 1 << 22, 1 << 23, 1 << 24, 1 << 25, 1 << 26];

const BUCKET_SIZE: BucketSize = DEFAULT_BLOCKS_PER_BUCKET;
const POSITIONS_PER_BLOCK: BlockSize = DEFAULT_POSITIONS_PER_BLOCK;
const INITIAL_STASH_OVERFLOW_SIZE: StashSize = DEFAULT_STASH_OVERFLOW_SIZE;
const RECURSION_CUTOFF: RecursionCutoff = u64::MAX;

struct DeterministicOram {
    oram: PathOram<BlockValue<BLOCK_SIZE>, BUCKET_SIZE, POSITIONS_PER_BLOCK>,
    eviction_counter: u64,
}

impl DeterministicOram {
    fn new(capacity: Address) -> Self {
        let mut rng = StdRng::seed_from_u64(0);

        let oram = PathOram::new_with_parameters(
            capacity,
            &mut rng,
            INITIAL_STASH_OVERFLOW_SIZE,
            RECURSION_CUTOFF,
        )
        .expect("Failed to initialize PathOram");

        Self {
            oram,
            eviction_counter: 0,
        }
    }

    fn write(&mut self, address: Address, value: BlockValue<BLOCK_SIZE>) {
        let mut rng = OsRng;

        let height = self.oram.height();
        let num_leaves = 1 << height;

        let rev_idx = self.eviction_counter.reverse_bits() >> (64 - height);
        let leaf_id = (1 << height) + rev_idx;

        self.oram
            .deterministic_access(address, |_| value, leaf_id, &mut rng)
            .expect("ORAM write failed");

        self.eviction_counter = (self.eviction_counter + 1) % num_leaves;
    }

    fn read(&mut self, address: Address) {
        let mut rng = OsRng;

        let height = self.oram.height();
        let num_leaves = 1 << height;

        let rev_idx = self.eviction_counter.reverse_bits() >> (64 - height);
        let leaf_id = (1 << height) + rev_idx;

        self.oram
            .deterministic_access(address, |x| *x, leaf_id, &mut rng)
            .expect("ORAM read failed");

        self.eviction_counter = (self.eviction_counter + 1) % num_leaves;
    }
}

fn benchmark_deterministic_read(c: &mut Criterion) {
    let mut group = c.benchmark_group("DeterministicOram::read");

    for &capacity in &CAPACITIES_TO_BENCHMARK {
        group.bench_with_input(
            BenchmarkId::new("capacity", capacity),
            &capacity,
            |b, &cap| {
                let mut oram = DeterministicOram::new(cap);

                b.iter(|| {
                    oram.read(0);
                })
            },
        );
    }
    group.finish();
}

fn benchmark_deterministic_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("DeterministicOram::write");

    for &capacity in &CAPACITIES_TO_BENCHMARK {
        group.bench_with_input(
            BenchmarkId::new("capacity", capacity),
            &capacity,
            |b, &cap| {
                let mut oram = DeterministicOram::new(cap);
                let val = BlockValue::default();

                b.iter(|| {
                    oram.write(0, val);
                })
            },
        );
    }
    group.finish();
}

criterion_group!(
    name = benches;
    config = Criterion::default()
        .warm_up_time(Duration::from_secs(5))
        .measurement_time(Duration::from_secs(15))
        .sample_size(10);
    targets = benchmark_deterministic_read, benchmark_deterministic_write
);

criterion_main!(benches);
