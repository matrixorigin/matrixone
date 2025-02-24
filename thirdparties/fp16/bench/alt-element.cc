#include <benchmark/benchmark.h>

#include <fp16.h>

static inline uint16_t next_xorshift16(uint16_t x) {
	x ^= x >> 8;
	x ^= x << 9;
	x ^= x >> 5;
	return x;
}

static inline uint32_t next_xorshift32(uint32_t x) {
	x ^= x >> 13;
	x ^= x << 17;
	x ^= x >> 5;
	return x;
}


static void fp16_alt_to_fp32_bits(benchmark::State& state) {
	uint16_t fp16 = UINT16_C(0x7C00);
	while (state.KeepRunning()) {
		uint32_t fp32 = fp16_alt_to_fp32_bits(fp16);

		fp16 = next_xorshift16(fp16);
		benchmark::DoNotOptimize(fp32);
	}
}
BENCHMARK(fp16_alt_to_fp32_bits);

static void fp16_alt_to_fp32_value(benchmark::State& state) {
	uint16_t fp16 = UINT16_C(0x7C00);
	while (state.KeepRunning()) {
		float fp32 = fp16_alt_to_fp32_value(fp16);

		fp16 = next_xorshift16(fp16);
		benchmark::DoNotOptimize(fp32);
	}
}
BENCHMARK(fp16_alt_to_fp32_value);

static void fp16_alt_from_fp32_value(benchmark::State& state) {
	uint32_t fp32 = UINT32_C(0x7F800000);
	while (state.KeepRunning()) {
		uint16_t fp16 = fp16_alt_from_fp32_value(fp32_from_bits(fp32));

		fp32 = next_xorshift32(fp32);
		benchmark::DoNotOptimize(fp16);
	}
}
BENCHMARK(fp16_alt_from_fp32_value);

BENCHMARK_MAIN();
