#include <benchmark/benchmark.h>

#include <fp16.h>

#include <vector>
#include <random>
#include <chrono>
#include <functional>
#include <algorithm>


static void fp16_alt_from_fp32_value(benchmark::State& state) {
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
			output[i] = fp16_alt_from_fp32_value(input[i]);
		}

		benchmark::DoNotOptimize(output);
	}
	state.SetItemsProcessed(int64_t(state.iterations()) * int64_t(state.range(0)));
}
BENCHMARK(fp16_alt_from_fp32_value)->RangeMultiplier(2)->Range(1<<10, 64<<20);

BENCHMARK_MAIN();
