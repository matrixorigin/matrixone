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


static void fp16_ieee_from_fp32_value(benchmark::State& state) {
	const uint_fast32_t seed = std::chrono::system_clock::now().time_since_epoch().count();
	auto rng = std::bind(std::uniform_real_distribution<float>(-1.0f, 1.0f), std::mt19937(seed));

	std::vector<float> fp32(state.range(0));
	std::vector<uint16_t> fp16(state.range(0));
	std::generate(fp32.begin(), fp32.end(), std::ref(rng));

	while (state.KeepRunning()) {
		float* input = fp32.data();
		benchmark::DoNotOptimize(input);

		uint16_t* output = fp16.data();
		const size_t n = state.range(0);
		for (size_t i = 0; i < n; i++) {
			output[i] = fp16_ieee_from_fp32_value(input[i]);
		}

		benchmark::DoNotOptimize(output);
	}
	state.SetItemsProcessed(int64_t(state.iterations()) * int64_t(state.range(0)));
}
BENCHMARK(fp16_ieee_from_fp32_value)->RangeMultiplier(2)->Range(1<<10, 64<<20);

#ifdef FP16_COMPARATIVE_BENCHMARKS
	static void TH_float2halfbits(benchmark::State& state) {
		const uint_fast32_t seed = std::chrono::system_clock::now().time_since_epoch().count();
		auto rng = std::bind(std::uniform_real_distribution<float>(-1.0f, 1.0f), std::mt19937(seed));

		std::vector<float> fp32(state.range(0));
		std::vector<uint16_t> fp16(state.range(0));
		std::generate(fp32.begin(), fp32.end(), std::ref(rng));

		while (state.KeepRunning()) {
			float* input = fp32.data();
			benchmark::DoNotOptimize(input);

			uint16_t* output = fp16.data();
			const size_t n = state.range(0);
			for (size_t i = 0; i < n; i++) {
				TH_float2halfbits(&input[i], &output[i]);
			}

			benchmark::DoNotOptimize(output);
		}
		state.SetItemsProcessed(int64_t(state.iterations()) * int64_t(state.range(0)));
	}
	BENCHMARK(TH_float2halfbits)->RangeMultiplier(2)->Range(1<<10, 64<<20);

	static void npy_floatbits_to_halfbits(benchmark::State& state) {
		const uint_fast32_t seed = std::chrono::system_clock::now().time_since_epoch().count();
		auto rng = std::bind(std::uniform_real_distribution<float>(-1.0f, 1.0f), std::mt19937(seed));

		std::vector<float> fp32(state.range(0));
		std::vector<uint16_t> fp16(state.range(0));
		std::generate(fp32.begin(), fp32.end(), std::ref(rng));

		while (state.KeepRunning()) {
			float* input = fp32.data();
			benchmark::DoNotOptimize(input);

			uint16_t* output = fp16.data();
			const size_t n = state.range(0);
			for (size_t i = 0; i < n; i++) {
				output[i] = npy_floatbits_to_halfbits(fp32_to_bits(input[i]));
			}

			benchmark::DoNotOptimize(output);
		}
		state.SetItemsProcessed(int64_t(state.iterations()) * int64_t(state.range(0)));
	}
	BENCHMARK(npy_floatbits_to_halfbits)->RangeMultiplier(2)->Range(1<<10, 64<<20);

	static void Eigen_float_to_half_rtne(benchmark::State& state) {
		const uint_fast32_t seed = std::chrono::system_clock::now().time_since_epoch().count();
		auto rng = std::bind(std::uniform_real_distribution<float>(-1.0f, 1.0f), std::mt19937(seed));

		std::vector<float> fp32(state.range(0));
		std::vector<uint16_t> fp16(state.range(0));
		std::generate(fp32.begin(), fp32.end(), std::ref(rng));

		while (state.KeepRunning()) {
			float* input = fp32.data();
			benchmark::DoNotOptimize(input);

			uint16_t* output = fp16.data();
			const size_t n = state.range(0);
			for (size_t i = 0; i < n; i++) {
				output[i] = Eigen::half_impl::float_to_half_rtne(input[i]).x;
			}

			benchmark::DoNotOptimize(output);
		}
		state.SetItemsProcessed(int64_t(state.iterations()) * int64_t(state.range(0)));
	}
	BENCHMARK(Eigen_float_to_half_rtne)->RangeMultiplier(2)->Range(1<<10, 64<<20);

	static void Float16Compressor_compress(benchmark::State& state) {
		const uint_fast32_t seed = std::chrono::system_clock::now().time_since_epoch().count();
		auto rng = std::bind(std::uniform_real_distribution<float>(-1.0f, 1.0f), std::mt19937(seed));

		std::vector<float> fp32(state.range(0));
		std::vector<uint16_t> fp16(state.range(0));
		std::generate(fp32.begin(), fp32.end(), std::ref(rng));

		while (state.KeepRunning()) {
			float* input = fp32.data();
			benchmark::DoNotOptimize(input);

			uint16_t* output = fp16.data();
			const size_t n = state.range(0);
			for (size_t i = 0; i < n; i++) {
				output[i] = Float16Compressor::compress(input[i]);
			}

			benchmark::DoNotOptimize(output);
		}
		state.SetItemsProcessed(int64_t(state.iterations()) * int64_t(state.range(0)));
	}
	BENCHMARK(Float16Compressor_compress)->RangeMultiplier(2)->Range(1<<10, 64<<20);

	static void half_float_detail_float2half_table(benchmark::State& state) {
		const uint_fast32_t seed = std::chrono::system_clock::now().time_since_epoch().count();
		auto rng = std::bind(std::uniform_real_distribution<float>(-1.0f, 1.0f), std::mt19937(seed));

		std::vector<float> fp32(state.range(0));
		std::vector<uint16_t> fp16(state.range(0));
		std::generate(fp32.begin(), fp32.end(), std::ref(rng));

		while (state.KeepRunning()) {
			float* input = fp32.data();
			benchmark::DoNotOptimize(input);

			uint16_t* output = fp16.data();
			const size_t n = state.range(0);
			for (size_t i = 0; i < n; i++) {
				output[i] =
					half_float::detail::float2half_impl<std::round_to_nearest>(
						input[i], half_float::detail::true_type());
			}

			benchmark::DoNotOptimize(output);
		}
		state.SetItemsProcessed(int64_t(state.iterations()) * int64_t(state.range(0)));
	}
	BENCHMARK(half_float_detail_float2half_table)->RangeMultiplier(2)->Range(1<<10, 64<<20);

	static void half_float_detail_float2half_branch(benchmark::State& state) {
		const uint_fast32_t seed = std::chrono::system_clock::now().time_since_epoch().count();
		auto rng = std::bind(std::uniform_real_distribution<float>(-1.0f, 1.0f), std::mt19937(seed));

		std::vector<float> fp32(state.range(0));
		std::vector<uint16_t> fp16(state.range(0));
		std::generate(fp32.begin(), fp32.end(), std::ref(rng));

		while (state.KeepRunning()) {
			float* input = fp32.data();
			benchmark::DoNotOptimize(input);

			uint16_t* output = fp16.data();
			const size_t n = state.range(0);
			for (size_t i = 0; i < n; i++) {
				output[i] =
					half_float::detail::float2half_impl<std::round_to_nearest>(
						input[i], half_float::detail::false_type());
			}

			benchmark::DoNotOptimize(output);
		}
		state.SetItemsProcessed(int64_t(state.iterations()) * int64_t(state.range(0)));
	}
	BENCHMARK(half_float_detail_float2half_branch)->RangeMultiplier(2)->Range(1<<10, 64<<20);
#endif

BENCHMARK_MAIN();
