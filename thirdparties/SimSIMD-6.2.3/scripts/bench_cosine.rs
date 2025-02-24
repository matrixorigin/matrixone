use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use simsimd::SpatialSimilarity as SimSIMD;

mod native;

const DIMENSIONS: usize = 1536;

pub fn cos_benchmark(c: &mut Criterion) {
    let inputs: (Vec<f32>, Vec<f32>) = (
        native::generate_random_vector(DIMENSIONS),
        native::generate_random_vector(DIMENSIONS),
    );

    let mut group = c.benchmark_group("SIMD Cosine");

    for i in 0..=5 {
        group.bench_with_input(BenchmarkId::new("SimSIMD", i), &i, |b, _| {
            b.iter(|| SimSIMD::cosine(&inputs.0, &inputs.1))
        });
        group.bench_with_input(BenchmarkId::new("Rust Procedural", i), &i, |b, _| {
            b.iter(|| native::baseline_cos_procedural(&inputs.0, &inputs.1))
        });
        group.bench_with_input(BenchmarkId::new("Rust Functional", i), &i, |b, _| {
            b.iter(|| native::baseline_cos_functional(&inputs.0, &inputs.1))
        });
        group.bench_with_input(BenchmarkId::new("Rust Unrolled", i), &i, |b, _| {
            b.iter(|| native::baseline_cos_unrolled(&inputs.0, &inputs.1))
        });
    }
}

criterion_group!(benches, cos_benchmark);
criterion_main!(benches);
