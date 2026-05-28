/* 
 * Copyright 2021 Matrix Origin
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include "cuvs_types.h"

#ifdef __cplusplus
#include <raft/core/resources.hpp>
#include <raft/core/host_mdspan.hpp>
#include <cuvs/distance/distance.hpp>
#include <vector>
#include <string>
#include <cstdint>
#include <algorithm>
#include <limits>
#include <utility>
#include <cuda_fp16.h>

namespace matrixone {

/**
 * @brief Helper to check if a raft handle has SNMG resources initialized.
 */
bool is_snmg_handle(const raft::resources& res);

/**
 * @brief Initialize NCCL communicators for a multi-GPU resource container.
 */
void init_mg_comms(raft::resources& mg_res, const std::vector<int>& devices);

/**
 * @brief Inject a raw NCCL communicator into raft::resources.
 * This is a wrapper to avoid multiple definitions of std_comms.hpp.
 */
void inject_nccl_comm(raft::resources* res, void* nccl_comm, int size, int rank);

/**
 * @brief Save a host matrix to a file in MODF format.
 */
void save_host_matrix(const std::string& filename, raft::host_matrix_view<const float, int64_t, raft::row_major> view);

/**
 * @brief Helper to set an error message in a C-compatible way.
 *
 * noexcept: callable from inside any `catch` block (including the
 * sweep added at every extern "C" boundary). If string construction
 * throws bad_alloc, the slot is set to a static fallback message
 * rather than re-throwing.
 */
void set_errmsg(void* errmsg, const char* context, const char* message) noexcept;

/**
 * @brief "HH:MM:SS.mmm" wall-clock timestamp used as a prefix for log lines.
 */
std::string get_timestamp();

/**
 * @brief Write `msg` to stderr prefixed with "[ERROR <timestamp>] ".
 * cuvs_worker.hpp's variadic log_err() formats its arguments into `msg`
 * and delegates here so the sink (timestamp + stream) lives in one place.
 */
void log_err(const std::string& msg);

/**
 * @brief Get raft resources for the given device (thread-local, one per device per thread).
 * Also calls cudaSetDevice(device_id) to ensure the calling thread is on the right device.
 */
const raft::resources& get_raft_resources(int device_id = 0);

/**
 * @brief Returns the next GPU device ID in round-robin order across all visible devices.
 * The device count is queried once and cached.
 */
int get_next_device_id();

/**
 * @brief Convert distance type from C enum to cuVS enum.
 */
cuvs::distance::DistanceType convert_distance_type(distance_type_t metric_c);

/**
 * @brief Performs float to half conversion on device.
 */
void convert_f32_to_f16_on_device(const raft::resources& res, const float* src, half* dst, uint64_t total_elements);

/**
 * @brief Performs half to float conversion on device.
 */
void convert_f16_to_f32_on_device(const raft::resources& res, const half* src, float* dst, uint64_t total_elements);

/**
 * @brief Host-side fp32→fp16 cast (uses F16C / AVX when available, scalar
 * __float2half_rn otherwise). Bit-identical to the device-side raft::copy
 * cast that compiles to mdspan_copy_kernel<__half>, so recall is preserved.
 *
 * Used to fold the per-search fp32→fp16 cast into the host-side query buffer
 * fill so the H2D copy moves half as many bytes and the GPU never runs the
 * mdspan_copy_kernel<__half> dispatch.
 */
void cast_float_to_half_host(const float* __restrict__ src,
                             half* __restrict__ dst, size_t n);

/**
 * @brief Sentinel placed in cuvs_task_result_t::result by the SHARDED async
 * search path. search_wait() in each index family detects this type, fans out
 * over the shard job_ids, and runs the k-way merge on the caller's thread —
 * so the worker's main_thread_ is never on the search hot path.
 *
 * See plan: .claude/plans/effervescent-hatching-dewdrop.md
 */
struct composite_search_pending_t {
    std::vector<uint64_t> shard_ids;
    uint64_t              num_queries;
    uint32_t              limit;
};

/**
 * @brief CPU k-way merge of per-shard top-k results into a single top-k per
 * query. Replaces the byte-identical merge_sharded_results() that previously
 * lived in cagra.hpp / ivf_flat.hpp / ivf_pq.hpp.
 *
 * SearchResult must expose `.neighbors` (vector<int64_t>) and `.distances`
 * (vector<float>) sized num_queries * limit. -1 sentinels in `neighbors`
 * indicate empty slots and are skipped. Output is dense top-`limit` per query,
 * sorted by ascending distance; trailing slots are padded with (-1, FLT_MAX).
 */
template <typename SearchResult>
SearchResult cpu_topk_merge_sharded(const std::vector<SearchResult>& shard_results,
                                    uint64_t num_queries, uint32_t limit) {
    SearchResult global_res;
    global_res.neighbors.resize(num_queries * limit);
    global_res.distances.resize(num_queries * limit);

    std::vector<std::pair<float, int64_t>> candidates;
    candidates.reserve(shard_results.size() * limit);

    for (uint64_t q = 0; q < num_queries; ++q) {
        candidates.clear();
        for (size_t s = 0; s < shard_results.size(); ++s) {
            const auto& sr = shard_results[s];
            for (uint32_t k = 0; k < limit; ++k) {
                int64_t id = sr.neighbors[q * limit + k];
                if (id != -1LL) {
                    candidates.emplace_back(sr.distances[q * limit + k], id);
                }
            }
        }

        uint32_t to_sort = std::min<uint32_t>(limit, static_cast<uint32_t>(candidates.size()));
        std::partial_sort(candidates.begin(), candidates.begin() + to_sort, candidates.end());

        for (uint32_t k = 0; k < limit; ++k) {
            if (k < to_sort) {
                global_res.neighbors[q * limit + k] = candidates[k].second;
                global_res.distances[q * limit + k] = candidates[k].first;
            } else {
                global_res.neighbors[q * limit + k] = -1LL;
                global_res.distances[q * limit + k] = std::numeric_limits<float>::max();
            }
        }
    }
    return global_res;
}

} // namespace matrixone
#endif

// C-compatible wrappers if needed
#ifdef __cplusplus
extern "C" {
#endif
    int gpu_get_device_count();
    void gpu_get_device_list(int* devices, int count);
    int gpu_get_next_device_id();
    void gpu_convert_f32_to_f16(const float* src, void* dst, uint64_t total_elements, int device_id, void* errmsg);
// Pinned memory management
void* gpu_alloc_pinned(uint64_t size, void* errmsg);
void gpu_free_pinned(void* ptr, void* errmsg);

#ifdef __cplusplus
}
#endif
