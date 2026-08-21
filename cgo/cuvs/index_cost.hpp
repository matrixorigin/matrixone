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
                const int64_t rows = rows_fitting_gpu_mem(per_row, who, &free_bytes);
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
            return min_rows * distinct;
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
// KNOWN GAP, measured: the real peak during extend runs ~30-40% above this for
// cuVS workspace not folded in. At 87.5M / dim 768 / f16 / m=192 the tar is
// 17.6 GB but the device peak is ~24 GB (measured on an L40S). The 60% budget on
// top absorbs it on every workload measured so far, but a build tuned to exactly
// the advertised ceiling will OOM before the check says it should. See
// ivfpq_train_extend.md.
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

private:
    ivf_pq_cost cost_;
};


}  // namespace matrixone
