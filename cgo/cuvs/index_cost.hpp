/*
 * Copyright 2026 Matrix Origin
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <vector>

#include <cuda_runtime.h>

#include "cuvs_types.h"
#include "device_memory.hpp"
#include "helper.h"

// ---------------------------------------------------------------------------
// Per-index DEVICE cost models.
//
// One class per index type, because what a row costs IS per index: CAGRA keeps
// the vectors resident and adds a graph, IVF-PQ keeps only codes and streams the
// vectors, IVF-Flat keeps the full vectors in its lists. Each class owns its own
// formula and nothing else has a copy -- a planner and an allocator holding
// different ideas of what a row costs is not a theoretical risk, it already
// happened once when Go carried a second copy of these expressions.
//
// A cost object needs NO index and no worker pool. Estimating capacity by
// standing up a worker pool -- threads, streams, RAFT handles -- just to read
// cudaMemGetInfo is far more machinery than the question deserves, and it forces
// a chicken-and-egg where the index has to exist before it can be sized. These
// objects are plain value types: construct one from the index shape, ask it.
//
// They bind each device themselves and put the caller's device back. The caller
// is typically a Go goroutine's OS thread that did not ask to be moved.
// ---------------------------------------------------------------------------
namespace matrixone {

// index_cost_base holds the part that is identical for every index: survey the
// devices, take the smallest answer, restore the caller's device. Only
// bytes_per_row() differs per index, and that is what the subclasses supply.
class index_cost_base {
public:
    virtual ~index_cost_base() = default;

    // Bytes ONE row of this index costs on the device.
    virtual size_t bytes_per_row() const = 0;

    // build_peak_bytes: the device demand of building `rows` rows.
    //
    // The default is the resident footprint, which is right for an index whose
    // build has no separate staging phase (CAGRA allocates the dataset and graph
    // it keeps). The IVF families override it: their k-means trainset is a
    // second allocation that never coexists with the list data, so their demand
    // is max(train, index) -- the sum would refuse builds that fit, and the
    // resident side alone under-claims a training-dominant build.
    virtual size_t build_peak_bytes(uint64_t rows) const {
        return static_cast<size_t>(rows) * bytes_per_row();
    }

    // Fraction of free VRAM this index may size against and claim, as a percentage.
    //
    // Per index, because the algorithms differ in how much of their peak the
    // per-row cost above accounts for. An index whose cost covers its whole
    // build can safely use more of the card; one whose peak includes phases cuVS
    // manages internally needs the difference held back. A single global number
    // would have to be the strictest of them, and would cost the others capacity
    // they could use.
    //
    // 75% is the default: it suits an index whose bytes_per_row covers what the
    // build actually allocates, leaving the remainder for the one large
    // allocation to land in, plus concurrent queries and kernel scratch.
    //
    // Capacity sizing (rows_fitting) and the admission that follows both read this
    // same value, so within C++ a build is sized and admitted against one fraction.
    //
    // That is NOT an invariant across the whole system. The Go-side gates get the
    // fraction from gpu_index_budget_percent, which maps index type NAMES to
    // classes by hand -- a class that overrides this and is not listed there falls
    // back to the strictest known value, which over-refuses rather than
    // over-admits, but is still a different number from its own. A subclass that
    // forgot to override at all is how the trainset probe came to size at 75%
    // while the build claimed at 65%.
    // Derived, not restated: device_memory_governor is where the default lives, and
    // a second literal here would be a number two headers can disagree about.
    static constexpr size_t kDefaultBudgetPercent =
        matrixone::device_memory_governor::kBudgetPercent;
    virtual size_t budget_percent() const { return kDefaultBudgetPercent; }

    // kShardAlignRows mirrors index_base.hpp's split: rows_per_shard is rounded
    // DOWN to a multiple of 32 (bitset words are sliced per shard), and the LAST
    // shard absorbs the remainder -- so the last shard is the biggest one.
    static constexpr int64_t kShardAlignRows = 32;

    // sharded_aggregate turns "rows one card can hold" into a table-wide capacity
    // that survives that split.
    //
    // min_rows * distinct does NOT: with 4 cards holding 1001 rows each it
    // advertises 4004, which splits as (4004/4)&~31 = 992 three times and 1028 on
    // the last card -- 27 rows more than the card the figure was derived from.
    // The build then sizes its claim from the real shard and is refused at exactly
    // the capacity this function advertised. CAGRA's 128-row training minimum does
    // not catch it either, since 992 clears that comfortably.
    //
    // Aligning FIRST makes every shard equal: (m*N)/N == m and m is already a
    // multiple of 32, so the last shard takes no remainder and no card is handed
    // more than min_rows.
    static int64_t sharded_aggregate(int64_t min_rows, int distinct) {
        const int64_t aligned = min_rows & ~(kShardAlignRows - 1);
        // Below one aligned shard there is no split that fits, but returning 0
        // would read as "not measured" and silently disable the VRAM bound
        // instead of refusing. Leave the unaligned figure and let the per-shard
        // k-means check reject it with a message that says why.
        if (aligned <= 0) return min_rows * distinct;
        return aligned * distinct;
    }

    // rows_fitting: how many rows fit across a device set.
    //
    // Sized from the SMALLEST participating card, because heterogeneous free
    // VRAM is supported and SHARDED cuts equal shards: sizing every shard for a
    // 40 GiB card means the 8 GiB one OOMs the moment its shard lands. Aliased
    // ids are skipped -- under gpu_multi_simulation the list is [0,0,0,0] and
    // querying one card four times is not a survey of four cards. SHARDED then
    // scales by the distinct card count, since one index is spread across them;
    // SINGLE_GPU and REPLICATED must each hold the whole thing.
    //
    // ASK ONCE, before anything has been allocated. A later call runs after
    // earlier sub-indexes have taken their memory, sees less free, and would
    // shrink every successive sub-index instead of all of them sharing one
    // capacity.
    int64_t rows_fitting(const int* device_ids, int num_devices, int dist_mode,
                         const char* who, size_t* out_per_row = nullptr,
                         int* out_min_device = nullptr, size_t* out_min_free = nullptr) const {
        const size_t per_row = bytes_per_row();
        if (out_per_row) *out_per_row = per_row;
        if (num_devices <= 0 || per_row == 0) return 0;

        // Put the caller's device back on every path: this runs on a thread that
        // did not ask to be moved, and a stray current-device leaks into whatever
        // that thread does next.
        int prev_device = device_ids[0];
        cudaGetDevice(&prev_device);

        int64_t min_rows = 0;
        size_t  min_free = 0;
        int     min_dev  = device_ids[0];
        int     distinct = 0;
        try {
            for (int i = 0; i < num_devices; ++i) {
                bool seen = false;
                for (int j = 0; j < i; ++j) {
                    if (device_ids[j] == device_ids[i]) { seen = true; break; }
                }
                if (seen) continue;

                const cudaError_t serr = cudaSetDevice(device_ids[i]);
                if (serr != cudaSuccess) {
                    // CONSUME the error before leaving. A failed CUDA call latches
                    // its status, and the next cudaPeekAtLastError anywhere reports
                    // it as though it belonged to unrelated code.
                    cudaGetLastError();
                    throw std::runtime_error(std::string(who) + ": cannot select device " +
                                             std::to_string(device_ids[i]) + ": " +
                                             cudaGetErrorString(serr));
                }
                size_t free_bytes = 0;
                const int64_t rows = rows_fitting_gpu_mem(per_row, who, &free_bytes,
                                                          this->budget_percent());
                if (distinct == 0 || rows < min_rows) {
                    min_rows = rows;
                    min_free = free_bytes;
                    min_dev  = device_ids[i];
                }
                ++distinct;
            }
        } catch (...) {
            cudaSetDevice(prev_device);
            cudaGetLastError();
            throw;
        }
        cudaSetDevice(prev_device);

        if (out_min_device) *out_min_device = min_dev;
        if (out_min_free) *out_min_free = min_free;
        if (dist_mode == DistributionMode_SHARDED && distinct > 1) {
            return sharded_aggregate(min_rows, distinct);
        }
        return min_rows;
    }
};

// cagra_cost: CAGRA keeps the raw vectors device-resident -- search walks the
// graph and reads them -- so the dataset is a permanent cost, not a build-time
// one. On top of it sits the intermediate kNN graph NN-Descent builds, holding a
// neighbour id and a distance per edge.
class cagra_cost final : public index_cost_base {
public:
    cagra_cost(size_t dim, size_t elem_size, size_t intermediate_graph_degree)
        : dim_(dim), elem_size_(elem_size),
          igd_(intermediate_graph_degree == 0 ? 128 : intermediate_graph_degree) {}

    size_t bytes_per_row() const override { return dim_ * elem_size_ + igd_ * 8; }

private:
    size_t dim_, elem_size_, igd_;
};

// ivf_pq_cost: IVF-PQ streams the dataset from the host and keeps only the PQ
// codes plus their int64 payload, so the vectors are NOT a resident cost. A
// search reaches every list, so the whole index stays resident and sub-index
// rotation does not shrink the sum.
//
// This figure is the RESIDENT index, not the build peak: extend also holds
// cuVS-managed workspace, which the vendor sizes internally and does not expose.
// At 87.5M / dim 768 / f16 / m=192 the resident index is 17.6 GB against a ~24 GB
// peak on an L40S, so the workspace runs about 0.36x the modelled figure. That is
// what budget_percent() below holds back for, rather than trying to model a
// vendor internal from outside -- see the 65% there.
class ivf_pq_cost final : public index_cost_base {
public:
    ivf_pq_cost(size_t dim, size_t m, size_t bits_per_code, size_t elem_size,
                double kmeans_trainset_fraction = 0.0)
        : dim_(dim), m_(m == 0 ? calculate_pq_dim(dim) : m),
          bits_(bits_per_code == 0 ? 8 : bits_per_code), elem_size_(elem_size),
          frac_(kmeans_trainset_fraction > 0.0 && kmeans_trainset_fraction <= 1.0
                    ? kmeans_trainset_fraction
                    : 0.5) {}  // cuVS default when unset

    size_t bytes_per_row() const override { return (m_ * bits_ + 7) / 8 + 8; }

    // 65%, deliberately below the default. bytes_per_row here is the PQ codes plus
    // their payloads -- the index that stays resident -- while the extend phase
    // also holds cuVS-managed workspace that the vendor sizes internally and does
    // not expose. Modelling that from outside would be guesswork tied to a cuVS
    // version; reserving for it is stable and needs no such coupling.
    //
    // The reserve is sized from a real build rather than a rule of thumb: at
    // 87.5M / dim 768 / f16 / m=192 the resident index is 17.6 GB against a ~24 GB
    // peak on an L40S, so the workspace runs about 0.36x the modelled figure.
    // Holding back 35% covers that with margin at the capacity boundary, where a
    // build sized to fill the budget is exactly where it would otherwise bite.
    //
    // CAGRA takes the 75% default because cagra_cost already charges both halves
    // of its peak explicitly -- the resident dataset and the intermediate kNN
    // graph -- so there is no comparable unmodelled term to hold back for.
    static constexpr size_t kBudgetPercent = 65;
    size_t budget_percent() const override { return kBudgetPercent; }

    // trainset_bytes_per_row: cuVS materialises the k-means trainset as float32
    // whatever the storage type, and for a non-float T also allocates a
    // trainset_tmp in T and converts (ivf_pq_build.cuh:1288-1307). Both are live
    // at the peak, so a NARROWER storage type costs MORE here, not less: f16 is
    // 4+2=6 bytes against f32's 4.
    size_t trainset_bytes_per_row() const {
        return dim_ * (elem_size_ == 4 ? 4 : 4 + elem_size_);
    }

    // build_peak_bytes: PEAK of the two phases, not their sum. The trainset block
    // closes (ivf_pq_build.cuh:1369) before detail::extend allocates the list
    // data (:1374), so they never coexist -- charging the sum would refuse builds
    // that fit, and charging only the codes under-claims a training-dominant one.
    size_t build_peak_bytes(uint64_t rows) const override {
        const size_t index_bytes = static_cast<size_t>(rows) * bytes_per_row();
        const size_t train_bytes =
            static_cast<size_t>(static_cast<double>(rows) * frac_) * trainset_bytes_per_row();
        return index_bytes > train_bytes ? index_bytes : train_bytes;
    }

    // calculate_pq_dim mirrors cuVS's default pq_dim selection, used when m is 0.
    // Sizing against the wrong value is not a rounding error: at dim 768 cuVS
    // picks 384, twice the 192 the wiki_all template configures, so a default-m
    // build needs twice the device memory a configured one does.
    static size_t calculate_pq_dim(size_t dim) {
        if (dim >= 128) dim /= 2;
        if (size_t rounded = dim & ~static_cast<size_t>(31); rounded > 0) return rounded;
        size_t r = 1;
        while ((r << 1) <= dim) r <<= 1;
        return r;
    }

private:
    size_t dim_, m_, bits_, elem_size_;
    double frac_;
};

// ivf_flat_cost: IVF-Flat keeps the FULL vectors in its lists -- it has no
// codebook -- plus the int64 payload beside each. That is why an IVF-Flat index
// is far larger on device than an IVF-PQ one over the same data.
class ivf_flat_cost final : public index_cost_base {
public:
    ivf_flat_cost(size_t dim, size_t elem_size, double kmeans_trainset_fraction = 0.0)
        : dim_(dim), elem_size_(elem_size),
          frac_(kmeans_trainset_fraction > 0.0 && kmeans_trainset_fraction <= 1.0
                    ? kmeans_trainset_fraction
                    : 0.5) {}

    size_t bytes_per_row() const override { return dim_ * elem_size_ + 8; }

    // Same trainset shape as IVF-PQ: float32, plus a copy in T for a narrow type.
    size_t trainset_bytes_per_row() const {
        return dim_ * (elem_size_ == 4 ? 4 : 4 + elem_size_);
    }

    size_t build_peak_bytes(uint64_t rows) const override {
        const size_t index_bytes = static_cast<size_t>(rows) * bytes_per_row();
        const size_t train_bytes =
            static_cast<size_t>(static_cast<double>(rows) * frac_) * trainset_bytes_per_row();
        return index_bytes > train_bytes ? index_bytes : train_bytes;
    }

private:
    size_t dim_, elem_size_;
    double frac_;
};

// The trainset bounds, expressed as cost objects so they go through the same
// device survey as the index bounds. They are separate objects rather than a
// second method because the trainset is a DIFFERENT per-row cost answering the
// same question -- "how many rows fit" -- and never coexists with the index
// data, so the demand is max(train, index), not their sum.
class ivf_pq_trainset_cost final : public index_cost_base {
public:
    ivf_pq_trainset_cost(size_t dim, size_t m, size_t bits_per_code, size_t elem_size)
        : cost_(dim, m, bits_per_code, elem_size) {}
    size_t bytes_per_row() const override { return cost_.trainset_bytes_per_row(); }

    // IVF-PQ's OWN fraction, not the base default. This is a different per-row
    // cost for the SAME index, sized against the same card and later claimed by
    // the same build -- so inheriting 75% here while ivf_pq_cost claims at 65%
    // let the trainset probe plan a training set the build then refused. At
    // 45 GiB free / dim 768 / f16 that is ~33.75 GiB planned against a ~29.25 GiB
    // ceiling: the plan succeeds and then deterministically refuses itself before
    // cuVS is ever called.
    //
    // Delegated rather than restated so it cannot drift from the index cost the
    // way it just did.
    size_t budget_percent() const override { return cost_.budget_percent(); }

private:
    ivf_pq_cost cost_;
};


}  // namespace matrixone
