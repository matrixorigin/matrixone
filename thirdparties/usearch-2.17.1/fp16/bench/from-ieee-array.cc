#include <benchmark/benchmark.h>

#include <fp16.h>

#include <vector>
#include <random>
#include <chrono>
#include <functional>
#include <algorithm>

#ifdef FP16_COMPARATIVE_BENCHMARKS
	#include <third-party/THHalf.h>
	#include <third-party/npy-halffloat.h>
	#include <third-party/eigen-half.h>
	#include <third-party/float16-compressor.h>
	#include <third-party/half.hpp>
#endif


static void fp16_ieee_to_fp32_bits(benchmark::State& state) {
	const uint_fast32_t seed = std::chrono::system_clock::now().time_since_epoch().count();
	auto rng = std::bind(std::uniform_real_distribution<float>(-1.0f, 1.0f), std::mt19937(seed));

	std::vector<uint16_t> fp16(state.range(0));
	std::vector<uint32_t> fp32(state.range(0));
	std::generate(fp16.begin(), fp16.end(),
		[&rng]{ return fp16_ieee_from_fp32_value(rng()); });

	while (state.KeepRunning()) {
		uint16_t* input = fp16.data();
		benchmark::DoNotOptimize(input);

		uint32_t* output = fp32.data();
		const size_t n = state.range(0);
		for (size_t i = 0; i < n; i++) {
			output[i] = fp16_ieee_to_fp32_bits(input[i]);
		}

		benchmark::DoNotOptimize(output);
	}
	state.SetItemsProcessed(int64_t(state.iterations()) * int64_t(state.range(0)));
}
BENCHMARK(fp16_ieee_to_fp32_bits)->RangeMultiplier(2)->Range(1<<10, 64<<20);

static void fp16_ieee_to_fp32_value(benchmark::State& state) {
	const uint_fast32_t seed = std::chrono::system_clock::now().time_since_epoch().count();
	auto rng = std::bind(std::uniform_real_distribution<float>(-1.0f, 1.0f), std::mt19937(seed));

	std::vector<uint16_t> fp16(state.range(0));
	std::vector<float> fp32(state.range(0));
	std::generate(fp16.begin(), fp16.end(),
		[&rng]{ return fp16_ieee_from_fp32_value(rng()); });

	while (state.KeepRunning()) {
		uint16_t* input = fp16.data();
		benchmark::DoNotOptimize(input);

		float* output = fp32.data();
		const size_t n = state.range(0);
		for (size_t i = 0; i < n; i++) {
			output[i] = fp16_ieee_to_fp32_value(input[i]);
		}

		benchmark::DoNotOptimize(output);
	}
	state.SetItemsProcessed(int64_t(state.iterations()) * int64_t(state.range(0)));
}
BENCHMARK(fp16_ieee_to_fp32_value)->RangeMultiplier(2)->Range(1<<10, 64<<20);

#ifdef FP16_COMPARATIVE_BENCHMARKS
	static void TH_halfbits2float(benchmark::State& state) {
		const uint_fast32_t seed = std::chrono::system_clock::now().time_since_epoch().count();
		auto rng = std::bind(std::uniform_real_distribution<float>(-1.0f, 1.0f), std::mt19937(seed));

		std::vector<uint16_t> fp16(state.range(0));
		std::vector<float> fp32(state.range(0));
		std::generate(fp16.begin(), fp16.end(),
			[&rng]{ return fp16_ieee_from_fp32_value(rng()); });

		while (state.KeepRunning()) {
			uint16_t* input = fp16.data();
			benchmark::DoNotOptimize(input);

			float* output = fp32.data();
			const size_t n = state.range(0);
			for (size_t i = 0; i < n; i++) {
				TH_halfbits2float(&input[i], &output[i]);
			}

			benchmark::DoNotOptimize(output);
		}
		state.SetItemsProcessed(int64_t(state.iterations()) * int64_t(state.range(0)));
	}
	BENCHMARK(TH_halfbits2float)->RangeMultiplier(2)->Range(1<<10, 64<<20);

	static void npy_halfbits_to_floatbits(benchmark::State& state) {
		const uint_fast32_t seed = std::chrono::system_clock::now().time_since_epoch().count();
		auto rng = std::bind(std::uniform_real_distribution<float>(-1.0f, 1.0f), std::mt19937(seed));

		std::vector<uint16_t> fp16(state.range(0));
		std::vector<uint32_t> fp32(state.range(0));
		std::generate(fp16.begin(), fp16.end(),
			[&rng]{ return fp16_ieee_from_fp32_value(rng()); });

		while (state.KeepRunning()) {
			uint16_t* input = fp16.data();
			benchmark::DoNotOptimize(input);

			uint32_t* output = fp32.data();
			const size_t n = state.range(0);
			for (size_t i = 0; i < n; i++) {
				output[i] = npy_halfbits_to_floatbits(input[i]);
			}

			benchmark::DoNotOptimize(output);
		}
		state.SetItemsProcessed(int64_t(state.iterations()) * int64_t(state.range(0)));
	}
	BENCHMARK(npy_halfbits_to_floatbits)->RangeMultiplier(2)->Range(1<<10, 64<<20);

	static void Eigen_half_to_float(benchmark::State& state) {
		const uint_fast32_t seed = std::chrono::system_clock::now().time_since_epoch().count();
		auto rng = std::bind(std::uniform_real_distribution<float>(-1.0f, 1.0f), std::mt19937(seed));

		std::vector<uint16_t> fp16(state.range(0));
		std::vector<float> fp32(state.range(0));
		std::generate(fp16.begin(), fp16.end(),
			[&rng]{ return fp16_ieee_from_fp32_value(rng()); });

		while (state.KeepRunning()) {
			uint16_t* input = fp16.data();
			benchmark::DoNotOptimize(input);

			float* output = fp32.data();
			const size_t n = state.range(0);
			for (size_t i = 0; i < n; i++) {
				output[i] =
					Eigen::half_impl::half_to_float(
						Eigen::half_impl::raw_uint16_to_half(input[i]));
			}

			benchmark::DoNotOptimize(output);
		}
		state.SetItemsProcessed(int64_t(state.iterations()) * int64_t(state.range(0)));
	}
	BENCHMARK(Eigen_half_to_float)->RangeMultiplier(2)->Range(1<<10, 64<<20);

	static void Float16Compressor_decompress(benchmark::State& state) {
		const uint_fast32_t seed = std::chrono::system_clock::now().time_since_epoch().count();
		auto rng = std::bind(std::uniform_real_distribution<float>(-1.0f, 1.0f), std::mt19937(seed));

		std::vector<uint16_t> fp16(state.range(0));
		std::vector<float> fp32(state.range(0));
		std::generate(fp16.begin(), fp16.end(),
			[&rng]{ return fp16_ieee_from_fp32_value(rng()); });

		while (state.KeepRunning()) {
			uint16_t* input = fp16.data();
			benchmark::DoNotOptimize(input);

			float* output = fp32.data();
			const size_t n = state.range(0);
			for (size_t i = 0; i < n; i++) {
				output[i] = Float16Compressor::decompress(input[i]);
			}

			benchmark::DoNotOptimize(output);
		}
		state.SetItemsProcessed(int64_t(state.iterations()) * int64_t(state.range(0)));
	}
	BENCHMARK(Float16Compressor_decompress)->RangeMultiplier(2)->Range(1<<10, 64<<20);

	static void half_float_detail_half2float_table(benchmark::State& state) {
		const uint_fast32_t seed = std::chrono::system_clock::now().time_since_epoch().count();
		auto rng = std::bind(std::uniform_real_distribution<float>(-1.0f, 1.0f), std::mt19937(seed));

		std::vector<uint16_t> fp16(state.range(0));
		std::vector<float> fp32(state.range(0));
		std::generate(fp16.begin(), fp16.end(),
			[&rng]{ return fp16_ieee_from_fp32_value(rng()); });

		while (state.KeepRunning()) {
			uint16_t* input = fp16.data();
			benchmark::DoNotOptimize(input);

			float* output = fp32.data();
			const size_t n = state.range(0);
			for (size_t i = 0; i < n; i++) {
				output[i] = half_float::detail::half2float_impl(input[i],
					half_float::detail::true_type());
			}

			benchmark::DoNotOptimize(output);
		}
		state.SetItemsProcessed(int64_t(state.iterations()) * int64_t(state.range(0)));
	}
	BENCHMARK(half_float_detail_half2float_table)->RangeMultiplier(2)->Range(1<<10, 64<<20);

	static void half_float_detail_half2float_branch(benchmark::State& state) {
		const uint_fast32_t seed = std::chrono::system_clock::now().time_since_epoch().count();
		auto rng = std::bind(std::uniform_real_distribution<float>(-1.0f, 1.0f), std::mt19937(seed));

		std::vector<uint16_t> fp16(state.range(0));
		std::vector<float> fp32(state.range(0));
		std::generate(fp16.begin(), fp16.end(),
			[&rng]{ return fp16_ieee_from_fp32_value(rng()); });

		while (state.KeepRunning()) {
			uint16_t* input = fp16.data();
			benchmark::DoNotOptimize(input);

			float* output = fp32.data();
			const size_t n = state.range(0);
			for (size_t i = 0; i < n; i++) {
				output[i] = half_float::detail::half2float_impl(input[i],
					half_float::detail::false_type());
			}

			benchmark::DoNotOptimize(output);
		}
		state.SetItemsProcessed(int64_t(state.iterations()) * int64_t(state.range(0)));
	}
	BENCHMARK(half_float_detail_half2float_branch)->RangeMultiplier(2)->Range(1<<10, 64<<20);
#endif

BENCHMARK_MAIN();
