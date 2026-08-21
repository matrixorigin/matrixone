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

// Per-index device cost models (index_cost.hpp).
//
// A cost object is a value type built from the index shape -- no index, no
// worker pool, nothing allocated -- so capacity can be estimated before anything
// exists to estimate it for. These tests pin the per-row formulas, the peak rule
// for builds with a k-means phase, that the device survey restores the caller's
// device, and the constraint that is easiest to get wrong: the answer must be
// taken ONCE, before anything has been allocated.

#include "index_cost.hpp"
#include "test_framework.hpp"

#include <cuda_runtime.h>
#include <vector>

using matrixone::cagra_cost;
using matrixone::ivf_flat_cost;
using matrixone::ivf_pq_cost;
using matrixone::ivf_pq_trainset_cost;

namespace {

int current_device() {
    int d = -1;
    cudaGetDevice(&d);
    return d;
}

}  // namespace

TEST(IndexCost, PerRowFormulasAreIndexSpecific) {
    // IVF-PQ keeps only the codes -- m=4 at 8 bits = 4 bytes -- plus the int64
    // payload cuVS stores beside each. The vectors are streamed, not resident.
    ASSERT_EQ((size_t)12, ivf_pq_cost(128, 4, 8, 4).bytes_per_row());

    // CAGRA keeps the raw vectors resident (search reads them while walking the
    // graph): dim*4 = 512, plus the intermediate kNN graph at an id and a
    // distance per edge, 64*8 = 512.
    ASSERT_EQ((size_t)1024, cagra_cost(128, 4, 64).bytes_per_row());

    // IVF-Flat keeps the FULL vectors in its lists -- no codebook -- plus the
    // payload. That is why it is far larger on device than IVF-PQ over the same
    // data: 512 + 8 against 12.
    ASSERT_EQ((size_t)520, ivf_flat_cost(128, 4).bytes_per_row());
}

TEST(IndexCost, NarrowStorageMakesTheTrainsetBigger) {
    // The counter-intuitive one. cuVS materialises the trainset as float32
    // whatever the storage type, and for a non-float T ALSO stages a copy in T.
    // So f16 costs 4+2 per component against f32's 4 -- a narrower type costs
    // MORE training memory, not less. Sizing this with the storage width alone
    // under-counts f16 by a third.
    ASSERT_EQ((size_t)512, ivf_pq_cost(128, 4, 8, 4).trainset_bytes_per_row());
    ASSERT_EQ((size_t)768, ivf_pq_cost(128, 4, 8, 2).trainset_bytes_per_row());
    ASSERT_EQ((size_t)640, ivf_pq_cost(128, 4, 8, 1).trainset_bytes_per_row());
}

TEST(IndexCost, BuildPeakIsMaxNotSum) {
    // The trainset block closes before the list data is allocated, so the two
    // never coexist: charging the sum would refuse builds that fit.
    const uint64_t rows = 1000;

    // Codes-dominant: 1000*12 = 12000 against a trainset of 1000*0.1*512 = 51200.
    // Training wins here, and charging only the codes -- which is what the Go
    // model used to do -- would under-claim by 39200 bytes.
    const ivf_pq_cost training_dominant(128, 4, 8, 4, 0.1);
    ASSERT_EQ((size_t)51200, training_dominant.build_peak_bytes(rows));

    // A tiny fraction flips it: 1000*0.001*512 = 512, under the 12000 of codes.
    const ivf_pq_cost index_dominant(128, 4, 8, 4, 0.001);
    ASSERT_EQ((size_t)12000, index_dominant.build_peak_bytes(rows));

    // CAGRA has no separate staging phase -- it allocates the dataset and graph
    // it keeps -- so its peak is just the resident footprint.
    ASSERT_EQ((size_t)1024 * rows, cagra_cost(128, 4, 64).build_peak_bytes(rows));
}

TEST(IndexCost, RowsFittingSurveysTheDeviceAndRestoresIt) {
    const int before = current_device();
    std::vector<int> devices = {0};

    size_t per_row = 0, min_free = 0;
    int min_dev = -1;
    const ivf_pq_cost cost(128, 4, 8, 4);
    const int64_t rows = cost.rows_fitting(devices.data(), (int)devices.size(),
                                           DistributionMode_SINGLE_GPU, "test",
                                           &per_row, &min_dev, &min_free);

    ASSERT_EQ((size_t)12, per_row);
    ASSERT_TRUE(rows > 0);
    ASSERT_TRUE(min_free > 0);
    ASSERT_EQ(0, min_dev);

    // The survey binds each device to read its free memory. The caller's device
    // must come back: this runs on a thread that did not ask to be moved, and for
    // capacity planning that is a Go goroutine's OS thread.
    ASSERT_EQ(before, current_device());
}

TEST(IndexCost, AliasedDeviceIdsAreSurveyedOnce) {
    // Under gpu_multi_simulation the device list is [0,0,0,0]. Querying one card
    // four times is not a survey of four cards, and treating it as one would
    // over-commit that card by 4x in SHARDED.
    std::vector<int> aliased = {0, 0, 0, 0};
    std::vector<int> single = {0};
    const cagra_cost cost(128, 4, 64);

    const int64_t a = cost.rows_fitting(aliased.data(), (int)aliased.size(),
                                        DistributionMode_SHARDED, "test");
    const int64_t s = cost.rows_fitting(single.data(), (int)single.size(),
                                        DistributionMode_SHARDED, "test");
    ASSERT_EQ(s, a);
}

TEST(IndexCost, ZeroDevicesAndZeroCostAnswerZero) {
    const ivf_pq_cost cost(128, 4, 8, 4);
    ASSERT_EQ((int64_t)0, cost.rows_fitting(nullptr, 0, DistributionMode_SINGLE_GPU, "test"));
    // A zero-dimension index has no per-row cost to divide by.
    const cagra_cost empty(0, 4, 0);
    ASSERT_TRUE(empty.bytes_per_row() > 0);  // graph term still counts
}

TEST(IndexCost, AnswerMustBeTakenBeforeAnythingIsAllocated) {
    std::vector<int> devices = {0};
    const ivf_pq_cost cost(128, 4, 8, 4);

    size_t free_before = 0, free_after = 0;
    const int64_t rows_before = cost.rows_fitting(devices.data(), 1, DistributionMode_SINGLE_GPU,
                                                  "test", nullptr, nullptr, &free_before);
    ASSERT_TRUE(rows_before > 0);

    // Take a real bite out of device memory, standing in for a sub-index that has
    // already been built.
    void* hog = nullptr;
    if (cudaMalloc(&hog, (size_t)512 << 20) != cudaSuccess) {
        cudaGetLastError();
        TEST_LOG("skipping: could not reserve 512 MiB to simulate a built sub-index");
        return;
    }
    const int64_t rows_after = cost.rows_fitting(devices.data(), 1, DistributionMode_SINGLE_GPU,
                                                 "test", nullptr, nullptr, &free_after);
    cudaFree(hog);

    // THIS is why capacity is decided once, at the beginning. Ask again after a
    // sub-index has been built and the answer is smaller, because the budget is a
    // fraction of what is free NOW. Sizing each sub-index from its own answer
    // would make every successive one smaller instead of sharing one capacity.
    ASSERT_TRUE(free_after < free_before);
    ASSERT_TRUE(rows_after < rows_before);
}
