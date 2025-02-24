#include <benchmark/benchmark.h>

#include <fp16.h>

#ifdef FP16_COMPARATIVE_BENCHMARKS
	#include <third-party/THHalf.h>
	#include <third-party/npy-halffloat.h>
	#include <third-party/eigen-half.h>
	#include <third-party/float16-compressor.h>
	#include <third-party/half.hpp>
#endif

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


/* Conversion from IEEE FP16 to IEEE FP32 */

static void fp16_ieee_to_fp32_bits(benchmark::State& state) {
	uint16_t fp16 = UINT16_C(0x7C00);
	while (state.KeepRunning()) {
		uint32_t fp32 = fp16_ieee_to_fp32_bits(fp16);

		fp16 = next_xorshift16(fp16);
		benchmark::DoNotOptimize(fp32);
	}
}
BENCHMARK(fp16_ieee_to_fp32_bits);

static void fp16_ieee_to_fp32_value(benchmark::State& state) {
	uint16_t fp16 = UINT16_C(0x7C00);
	while (state.KeepRunning()) {
		float fp32 = fp16_ieee_to_fp32_value(fp16);

		fp16 = next_xorshift16(fp16);
		benchmark::DoNotOptimize(fp32);
	}
}
BENCHMARK(fp16_ieee_to_fp32_value);

#ifdef FP16_COMPARATIVE_BENCHMARKS
	static void TH_halfbits2float(benchmark::State& state) {
		uint16_t fp16 = UINT16_C(0x7C00);
		while (state.KeepRunning()) {
			float fp32;
			TH_halfbits2float(&fp16, &fp32);

			fp16 = next_xorshift16(fp16);
			benchmark::DoNotOptimize(fp32);
		}
	}
	BENCHMARK(TH_halfbits2float);

	static void npy_halfbits_to_floatbits(benchmark::State& state) {
		uint16_t fp16 = UINT16_C(0x7C00);
		while (state.KeepRunning()) {
			uint32_t fp32 = npy_halfbits_to_floatbits(fp16);

			fp16 = next_xorshift16(fp16);
			benchmark::DoNotOptimize(fp32);
		}
	}
	BENCHMARK(npy_halfbits_to_floatbits);

	static void Eigen_half_to_float(benchmark::State& state) {
		uint16_t fp16 = UINT16_C(0x7C00);
		while (state.KeepRunning()) {
			float fp32 =
				Eigen::half_impl::half_to_float(
					Eigen::half_impl::raw_uint16_to_half(fp16));

			fp16 = next_xorshift16(fp16);
			benchmark::DoNotOptimize(fp32);
		}
	}
	BENCHMARK(Eigen_half_to_float);

	static void Float16Compressor_decompress(benchmark::State& state) {
		uint16_t fp16 = UINT16_C(0x7C00);
		while (state.KeepRunning()) {
			float fp32 = Float16Compressor::decompress(fp16);

			fp16 = next_xorshift16(fp16);
			benchmark::DoNotOptimize(fp32);
		}
	}
	BENCHMARK(Float16Compressor_decompress);

	static void half_float_detail_half2float_table(benchmark::State& state) {
		uint16_t fp16 = UINT16_C(0x7C00);
		while (state.KeepRunning()) {
			float fp32 =
				half_float::detail::half2float_impl(fp16,
					half_float::detail::true_type());

			fp16 = next_xorshift16(fp16);
			benchmark::DoNotOptimize(fp32);
		}
	}
	BENCHMARK(half_float_detail_half2float_table);

	static void half_float_detail_half2float_branch(benchmark::State& state) {
		uint16_t fp16 = UINT16_C(0x7C00);
		while (state.KeepRunning()) {
			float fp32 =
				half_float::detail::half2float_impl(fp16,
					half_float::detail::false_type());

			fp16 = next_xorshift16(fp16);
			benchmark::DoNotOptimize(fp32);
		}
	}
	BENCHMARK(half_float_detail_half2float_branch);
#endif

/* Conversion from IEEE FP32 to IEEE FP16 */

static void fp16_ieee_from_fp32_value(benchmark::State& state) {
	uint32_t fp32 = UINT32_C(0x7F800000);
	while (state.KeepRunning()) {
		uint16_t fp16 = fp16_ieee_from_fp32_value(fp32_from_bits(fp32));

		fp32 = next_xorshift32(fp32);
		benchmark::DoNotOptimize(fp16);
	}
}
BENCHMARK(fp16_ieee_from_fp32_value);

#ifdef FP16_COMPARATIVE_BENCHMARKS
	static void TH_float2halfbits(benchmark::State& state) {
		uint32_t fp32 = UINT32_C(0x7F800000);
		while (state.KeepRunning()) {
			uint16_t fp16;
			float fp32_value = fp32_from_bits(fp32);
			TH_float2halfbits(&fp32_value, &fp16);

			fp32 = next_xorshift32(fp32);
			benchmark::DoNotOptimize(fp16);
		}
	}
	BENCHMARK(TH_float2halfbits);

	static void npy_floatbits_to_halfbits(benchmark::State& state) {
		uint32_t fp32 = UINT32_C(0x7F800000);
		while (state.KeepRunning()) {
			uint16_t fp16 = npy_floatbits_to_halfbits(fp32);

			fp32 = next_xorshift32(fp32);
			benchmark::DoNotOptimize(fp16);
		}
	}
	BENCHMARK(npy_floatbits_to_halfbits);

	static void Eigen_float_to_half_rtne(benchmark::State& state) {
		uint32_t fp32 = UINT32_C(0x7F800000);
		while (state.KeepRunning()) {
			Eigen::half_impl::__half fp16 =
				Eigen::half_impl::float_to_half_rtne(
					fp32_from_bits(fp32));

			fp32 = next_xorshift32(fp32);
			benchmark::DoNotOptimize(fp16);
		}
	}
	BENCHMARK(Eigen_float_to_half_rtne);

	static void Float16Compressor_compress(benchmark::State& state) {
		uint32_t fp32 = UINT32_C(0x7F800000);
		while (state.KeepRunning()) {
			uint16_t fp16 = Float16Compressor::compress(fp32_from_bits(fp32));

			fp32 = next_xorshift32(fp32);
			benchmark::DoNotOptimize(fp16);
		}
	}
	BENCHMARK(Float16Compressor_compress);

	static void half_float_detail_float2half_table(benchmark::State& state) {
		uint32_t fp32 = UINT32_C(0x7F800000);
		while (state.KeepRunning()) {
			uint16_t fp16 =
				half_float::detail::float2half_impl<std::round_to_nearest>(
					fp32_from_bits(fp32),
						half_float::detail::true_type());

			fp32 = next_xorshift32(fp32);
			benchmark::DoNotOptimize(fp16);
		}
	}
	BENCHMARK(half_float_detail_float2half_table);

	static void half_float_detail_float2half_branch(benchmark::State& state) {
		uint32_t fp32 = UINT32_C(0x7F800000);
		while (state.KeepRunning()) {
			uint16_t fp16 =
				half_float::detail::float2half_impl<std::round_to_nearest>(
					fp32_from_bits(fp32),
						half_float::detail::false_type());

			fp32 = next_xorshift32(fp32);
			benchmark::DoNotOptimize(fp16);
		}
	}
	BENCHMARK(half_float_detail_float2half_branch);
#endif

BENCHMARK_MAIN();
