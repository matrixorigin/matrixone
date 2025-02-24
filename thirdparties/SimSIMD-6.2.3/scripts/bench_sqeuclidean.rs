use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use simsimd::SpatialSimilarity as SimSIMD;

mod bench_native;

const DIMENSIONS: usize = 1536;

pub fn l2sq_benchmark(c: &mut Criterion) {
    let inputs: (Vec<f32>, Vec<f32>) = (
        bench_native::generate_random_vector(DIMENSIONS),
        bench_native::generate_random_vector(DIMENSIONS),
    );

    let mut group = c.benchmark_group("SIMD SqEuclidean");

    for i in 0..=5 {
        group.bench_with_input(BenchmarkId::new("SimSIMD", i), &i, |b, _| {
            b.iter(|| SimSIMD::sqeuclidean(&inputs.0, &inputs.1))
        });
        group.bench_with_input(BenchmarkId::new("Rust Procedural", i), &i, |b, _| {
            b.iter(|| bench_native::baseline_l2sq_procedural(&inputs.0, &inputs.1))
        });
        group.bench_with_input(BenchmarkId::new("Rust Functional", i), &i, |b, _| {
            b.iter(|| bench_native::baseline_l2sq_functional(&inputs.0, &inputs.1))
        });
        group.bench_with_input(BenchmarkId::new("Rust Unrolled", i), &i, |b, _| {
            b.iter(|| bench_native::baseline_l2sq_unrolled(&inputs.0, &inputs.1))
        });
    }
}

criterion_group!(benches, l2sq_benchmark);
criterion_main!(benches);
