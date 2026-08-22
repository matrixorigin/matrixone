// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "device_memory.hpp"
#include "test_framework.hpp"

#include <atomic>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <string>
#include <thread>
#include <vector>

using matrixone::device_memory_governor;
using matrixone::path_bytes;
using matrixone::required_path_bytes;

namespace {

int current_device() {
    int d = 0;
    cudaGetDevice(&d);
    return d;
}

// Every test must leave the ledger at zero: the counters are process-global, so
// a leaked claim would silently shrink the budget for every later test.
void require_ledger_empty(const char* where) {
    if (device_memory_governor::reserved_bytes(current_device()) != 0) {
        TEST_ERROR("ledger not empty after " << where);
    }
    ASSERT_EQ(size_t(0), device_memory_governor::reserved_bytes(current_device()));
}

size_t free_now() {
    size_t f = 0, t = 0;
    cudaMemGetInfo(&f, &t);
    return f;
}

}  // namespace

TEST(DeviceMemoryGovernor, ZeroDemandIsRefused) {
    // 0 must not be overloaded to mean "unknown demand, admit anyway": that
    // silently skips admission for a caller that failed to size its allocation.
    // A caller with nothing to allocate must not reserve at all.
    ASSERT_THROW(device_memory_governor::reserve(0, "test"), std::runtime_error);
    require_ledger_empty("refused zero-demand reserve");
}

TEST(DeviceMemoryGovernor, ClaimIsVisibleThenReleasedOnScopeExit) {
    const size_t want = 1u << 20;
    {
        auto claim = device_memory_governor::reserve(want, "test");
        ASSERT_EQ(want, device_memory_governor::reserved_bytes(current_device()));
    }
    require_ledger_empty("scope exit");
}

TEST(DeviceMemoryGovernor, ExplicitReleaseIsIdempotent) {
    const size_t want = 1u << 20;
    auto claim = device_memory_governor::reserve(want, "test");
    claim.release();
    require_ledger_empty("first release");
    claim.release();  // must not underflow the counter
    require_ledger_empty("second release");
}

TEST(DeviceMemoryGovernor, RefusesWhatDoesNotFitAndClaimsNothing) {
    // A demand far beyond the card must be refused, and a refused admission
    // must leave no residue -- a leaked claim would refuse every later load.
    bool threw = false;
    try {
        auto claim = device_memory_governor::reserve(free_now() * 4 + (1u << 30), "test");
    } catch (const std::exception& e) {
        threw = true;
        ASSERT_TRUE(std::string(e.what()).find("bytes of VRAM") != std::string::npos);
    }
    ASSERT_TRUE(threw);  // an impossible demand must be refused
    require_ledger_empty("refused admission");
}

TEST(DeviceMemoryGovernor, SecondClaimSeesTheFirst) {
    // The whole point: an in-flight claim is invisible to cudaMemGetInfo, so it
    // must be visible through the ledger instead. Claim ~all of the budget,
    // then verify a second claim of the same size is refused.
    const size_t budget = free_now() / 10 * 6;
    const size_t half   = budget / 2 + (1u << 20);  // just over half the budget

    auto first = device_memory_governor::reserve(half, "first");
    ASSERT_EQ(half, device_memory_governor::reserved_bytes(current_device()));

    bool refused = false;
    try {
        auto second = device_memory_governor::reserve(half, "second");
    } catch (const std::exception&) {
        refused = true;
    }
    ASSERT_TRUE(refused);  // two claims of >half the budget must not both pass

    first.release();
    require_ledger_empty("after releasing the first");

    // Once released, the headroom must be reusable.
    {
        auto third = device_memory_governor::reserve(half, "third");
        ASSERT_EQ(half, device_memory_governor::reserved_bytes(current_device()));
    }
    require_ledger_empty("after third");
}

TEST(DeviceMemoryGovernor, ConcurrentClaimsNeverExceedTheBudget) {
    // The check-and-claim is a CAS, so N threads racing for a budget that fits
    // only K of them must admit at most K. Without the CAS every thread reads
    // the same ledger value and they all pass.
    const size_t budget = free_now() / 10 * 6;
    const int    kThreads = 16;
    const size_t each   = budget / 4 + (1u << 20);   // only 3 can fit
    const size_t cap    = budget;

    std::atomic<int>  admitted{0};
    std::atomic<size_t> peak{0};
    std::vector<std::thread> ts;
    std::vector<device_memory_governor::reservation> claims(kThreads);

    for (int i = 0; i < kThreads; ++i) {
        ts.emplace_back([&, i] {
            try {
                claims[i] = device_memory_governor::reserve(each, "race");
                admitted.fetch_add(1);
                size_t cur = device_memory_governor::reserved_bytes(current_device());
                size_t p   = peak.load();
                while (cur > p && !peak.compare_exchange_weak(p, cur)) {}
            } catch (const std::exception&) {
                // refused: expected for the losers
            }
        });
    }
    for (auto& t : ts) t.join();

    ASSERT_TRUE(admitted.load() >= 1);  // at least one claim must win
    ASSERT_TRUE(admitted.load() <= 3);  // no more than the budget allows
    ASSERT_TRUE(peak.load() <= cap);  // the ledger must never exceed the budget

    for (auto& c : claims) c.release();
    require_ledger_empty("concurrent claims");
}

TEST(PathBytes, ReportsFileAndDirectorySizes) {
    namespace fs = std::filesystem;
    auto dir = fs::temp_directory_path() / "mo_path_bytes_test";
    fs::remove_all(dir);
    fs::create_directories(dir);

    auto write_n = [](const fs::path& p, size_t n) {
        std::ofstream f(p, std::ios::binary);
        std::string blob(n, 'x');
        f.write(blob.data(), static_cast<std::streamsize>(blob.size()));
    };

    write_n(dir / "a.bin", 1000);
    write_n(dir / "b.bin", 2000);

    ASSERT_EQ(size_t(1000), path_bytes((dir / "a.bin").string()));
    ASSERT_EQ(size_t(3000), path_bytes(dir.string()));
    // Unknown paths report 0, which reserve() reads as "do not guess".
    ASSERT_EQ(size_t(0), path_bytes((dir / "missing.bin").string()));
    ASSERT_EQ(size_t(0), path_bytes(""));

    fs::remove_all(dir);
}

TEST(RequiredPathBytes, ThrowsOnMissingEmptyOrUnreadable) {
    namespace fs = std::filesystem;
    auto dir = fs::temp_directory_path() / "mo_required_path_bytes_test";
    fs::remove_all(dir);
    fs::create_directories(dir);

    // A real artifact reports its size, same as path_bytes.
    {
        std::ofstream f(dir / "ok.bin", std::ios::binary);
        std::string blob(1234, 'x');
        f.write(blob.data(), static_cast<std::streamsize>(blob.size()));
    }
    ASSERT_EQ(size_t(1234), required_path_bytes((dir / "ok.bin").string(), "test"));

    // Missing, empty, and invalid must all be REFUSED rather than silently
    // admitted with no claim -- at a load site the artifact was just unpacked,
    // so a zero size is a defect, not "unknown demand".
    ASSERT_THROW(required_path_bytes((dir / "missing.bin").string(), "test"), std::runtime_error);

    { std::ofstream f(dir / "empty.bin", std::ios::binary); }  // zero-length
    ASSERT_THROW(required_path_bytes((dir / "empty.bin").string(), "test"), std::runtime_error);

    ASSERT_THROW(required_path_bytes("", "test"), std::runtime_error);

    // The non-strict query keeps reporting 0 for the same inputs.
    ASSERT_EQ(size_t(0), path_bytes((dir / "missing.bin").string()));

    fs::remove_all(dir);
}
