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

#include "host_memory.hpp"
#include "ivf_pq.hpp"
#include "test_framework.hpp"

#include <atomic>
#include <cstdint>
#include <thread>
#include <vector>

using matrixone::host_available_t;
using matrixone::host_memory_governor;

namespace {

// Every test must leave the ledger at zero: the counter is process-global, so a
// leaked claim would silently shrink the budget for every later test.
void require_ledger_empty(const char* where) {
    if (host_memory_governor::reserved_bytes() != 0) {
        REPORT_FAILURE(std::string("host ledger not empty after ") + where);
    }
}

host_available_t measured(uint64_t bytes) {
    host_available_t a;
    a.bytes    = bytes;
    a.measured = true;
    return a;
}

host_available_t unmeasured() { return host_available_t(); }

constexpr uint64_t kGiB = 1ULL << 30;

}  // namespace

TEST(HostMemoryGovernor, BudgetIsSeventyFivePercent) {
    ASSERT_EQ(host_memory_governor::budget_bytes(100), (size_t)75);
    ASSERT_EQ(host_memory_governor::budget_bytes(16 * kGiB), (size_t)(16 * kGiB / 100 * 75));
    ASSERT_EQ(host_memory_governor::budget_bytes(0), (size_t)0);
}

TEST(HostMemoryGovernor, ZeroByteClaimIsRefused) {
    // A zero demand means the caller could not size its allocation. Admitting
    // it would overload one value with "nothing to allocate" and "I do not
    // know", and only the first is legitimate -- such a caller must not reserve.
    ASSERT_THROW(host_memory_governor::reserve_against(measured(16 * kGiB), 0, "zero"),
                 std::runtime_error);
    require_ledger_empty("a refused zero claim");
}

TEST(HostMemoryGovernor, UnmeasuredAvailabilityAdmitsWithoutClaiming) {
    // Where availability cannot be read the capacity model already falls back
    // to the device bound. Refusing here instead would break builds that work
    // today on hosts with no readable /proc or cgroup files.
    auto claim = host_memory_governor::reserve_against(unmeasured(), 8 * kGiB, "unmeasured");
    ASSERT_EQ(claim.bytes(), (size_t)0);
    require_ledger_empty("an unmeasured claim");
}

TEST(HostMemoryGovernor, ExhaustedHostIsRefusedNotIgnored) {
    // avail == 0 WITH measured == true is a host genuinely out of memory. It
    // must refuse -- treating it like an unreadable host would disable the
    // bound exactly when it is needed.
    ASSERT_THROW(host_memory_governor::reserve_against(measured(0), 1, "exhausted"),
                 std::runtime_error);
    require_ledger_empty("a refused exhausted claim");
}

TEST(HostMemoryGovernor, SecondClaimSeesTheFirst) {
    // The whole point: an in-flight claim is invisible to the availability
    // reading, so it must be visible through the ledger instead.
    const uint64_t avail  = 16 * kGiB;
    const size_t   budget = host_memory_governor::budget_bytes(avail);
    const size_t   half   = budget / 2 + (1u << 20);  // just over half the budget

    auto first = host_memory_governor::reserve_against(measured(avail), half, "first");
    ASSERT_EQ(host_memory_governor::reserved_bytes(), half);

    bool refused = false;
    try {
        auto second = host_memory_governor::reserve_against(measured(avail), half, "second");
    } catch (const std::exception&) {
        refused = true;
    }
    ASSERT_TRUE(refused);  // two claims of >half the budget must not both pass
    ASSERT_EQ(host_memory_governor::reserved_bytes(), half);  // the loser claimed nothing

    first.release();
    require_ledger_empty("releasing the first");

    // Once released, the headroom must be reusable.
    {
        auto third = host_memory_governor::reserve_against(measured(avail), half, "third");
        ASSERT_EQ(host_memory_governor::reserved_bytes(), half);
    }
    require_ledger_empty("third going out of scope");
}

TEST(HostMemoryGovernor, ExplicitReleaseIsIdempotent) {
    const uint64_t avail = 16 * kGiB;
    {
        auto claim = host_memory_governor::reserve_against(measured(avail), 1 * kGiB, "idem");
        ASSERT_EQ(host_memory_governor::reserved_bytes(), (size_t)(1 * kGiB));
        claim.release();
        require_ledger_empty("an explicit release");
        claim.release();  // second release must not underflow the ledger
        require_ledger_empty("a second explicit release");
    }
    // ...nor must the destructor, after both explicit releases.
    require_ledger_empty("the destructor of an already-released claim");
}

TEST(HostMemoryGovernor, MovedFromClaimReleasesOnlyOnce) {
    const uint64_t avail = 16 * kGiB;
    {
        auto a = host_memory_governor::reserve_against(measured(avail), 1 * kGiB, "move");
        ASSERT_EQ(host_memory_governor::reserved_bytes(), (size_t)(1 * kGiB));
        auto b = std::move(a);
        // The move must transfer the claim, not duplicate or drop it: the
        // ledger is unchanged and `a` no longer owns anything.
        ASSERT_EQ(host_memory_governor::reserved_bytes(), (size_t)(1 * kGiB));
        ASSERT_EQ(a.bytes(), (size_t)0);
        ASSERT_EQ(b.bytes(), (size_t)(1 * kGiB));
    }
    require_ledger_empty("a moved-from claim and its new owner going out of scope");
}

TEST(HostMemoryGovernor, RefusalLeavesTheLedgerUntouched) {
    const uint64_t avail  = 16 * kGiB;
    const size_t   budget = host_memory_governor::budget_bytes(avail);
    ASSERT_THROW(host_memory_governor::reserve_against(measured(avail), budget + 1, "toobig"),
                 std::runtime_error);
    require_ledger_empty("a refused oversized claim");
}

TEST(HostMemoryGovernor, ConcurrentClaimsNeverExceedTheBudget) {
    // The check-and-claim is a CAS, so N threads racing for a budget that fits
    // only K of them must admit at most K. Without the CAS every thread reads
    // the same ledger value and they all pass.
    const uint64_t avail    = 16 * kGiB;
    const size_t   budget   = host_memory_governor::budget_bytes(avail);
    const int      kThreads = 16;
    const size_t   each     = budget / 4 + (1u << 20);  // only 3 can fit

    std::atomic<int>                                admitted{0};
    std::atomic<size_t>                             peak{0};
    std::vector<std::thread>                        ts;
    std::vector<host_memory_governor::reservation>  claims(kThreads);

    for (int i = 0; i < kThreads; ++i) {
        ts.emplace_back([&, i] {
            try {
                claims[i] = host_memory_governor::reserve_against(measured(avail), each, "race");
                admitted.fetch_add(1);
                size_t cur = host_memory_governor::reserved_bytes();
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
    ASSERT_TRUE(peak.load() <= budget); // the ledger must never exceed the budget

    for (auto& c : claims) c.release();
    require_ledger_empty("concurrent claims");
}

// reserve() is the production entry point and samples the live host. It cannot
// assert an amount -- the machine's real availability is not knowable from here
// -- so it asserts the contract that a claim round-trips through the ledger.
TEST(HostMemoryGovernor, LiveReserveRoundTrips) {
    require_ledger_empty("the start of the live test");
    {
        auto claim = host_memory_governor::reserve(1u << 20, "live");  // 1 MiB
        ASSERT_EQ(host_memory_governor::reserved_bytes(), claim.bytes());
    }
    require_ledger_empty("the live claim going out of scope");
}

// --- the WIRING, not just the ledger ------------------------------------------
//
// The tests above prove the governor's rules. These prove the build path is
// actually wired to it, which is a separate failure: a claim silently dropped
// from allocate_host_capacity leaves every rule above passing and every index
// admitted, and the Go tests that used to cover the equivalent claim were
// removed when the claim moved into C++.

// A construction must leave the ledger where it found it. The claim covers the
// capacity buffers only until they are allocated; holding it past the
// constructor would charge a concurrent build for the whole life of this index.
TEST(HostMemoryGovernor, IndexConstructionReleasesItsClaim) {
    require_ledger_empty("the start of the construction test");

    ivf_pq_build_params_t bp = ivf_pq_build_params_default();
    bp.n_lists               = 4;
    std::vector<int> devices = {0};
    {
        matrixone::gpu_ivf_pq_t<float, float> index(/*total_count=*/1000, /*dimension=*/32,
                                         DistanceType_L2Expanded, bp, devices, 1,
                                         DistributionMode_SINGLE_GPU);
        // Still inside the index's lifetime: the buffers exist and are visible in
        // the availability reading, so the LEDGER must already be clear.
        ASSERT_EQ(host_memory_governor::reserved_bytes(), (size_t)0);
    }
    require_ledger_empty("the index going out of scope");
}

// ...and the claim must actually be consulted. With the ledger already holding
// all but a sliver of the budget, a construction that needs more than the sliver
// has to be refused -- if the claim were dropped, this would silently succeed.
TEST(HostMemoryGovernor, IndexConstructionIsRefusedWhenTheLedgerIsFull) {
    require_ledger_empty("the start of the refusal test");

    const matrixone::host_available_t avail = matrixone::host_available_bytes();
    if (!avail.measured) {
        TEST_LOG("skipped: host availability is unreadable here, so admission is "
                 "disabled by design and there is nothing to refuse");
        return;
    }
    const size_t budget = host_memory_governor::budget_bytes(
        static_cast<size_t>(avail.bytes));
    const size_t sliver = 16u << 20;  // 16 MiB
    if (budget <= sliver) {
        TEST_LOG("skipped: host budget is smaller than the sliver this test leaves free");
        return;
    }

    // dim 128 f32 + 8 bytes of id per row = 520 B/row; 100k rows is ~52 MB, well
    // clear of the sliver, so the refusal is not a rounding accident.
    const uint64_t rows = 100000;
    const uint32_t dim  = 128;
    ASSERT_TRUE((size_t)rows * (dim * sizeof(float) + sizeof(int64_t)) > sliver);

    auto hog = host_memory_governor::reserve_against(avail, budget - sliver, "hog");
    ASSERT_EQ(host_memory_governor::reserved_bytes(), budget - sliver);

    ivf_pq_build_params_t bp = ivf_pq_build_params_default();
    bp.n_lists               = 4;
    std::vector<int> devices = {0};

    bool refused = false;
    try {
        matrixone::gpu_ivf_pq_t<float, float> index(rows, dim, DistanceType_L2Expanded, bp, devices, 1,
                                         DistributionMode_SINGLE_GPU);
    } catch (const std::exception&) {
        refused = true;
    }
    ASSERT_TRUE(refused);

    // A refused construction must claim nothing: only the hog is on the ledger.
    ASSERT_EQ(host_memory_governor::reserved_bytes(), budget - sliver);
    hog.release();
    require_ledger_empty("releasing the hog");
}
