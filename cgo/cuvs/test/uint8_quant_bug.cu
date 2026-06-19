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

// ---------------------------------------------------------------------------
// STANDALONE repro for the uint8 quantization recall collapse.
//
// Isolates whether the uint8 quantization recall collapse (seen at 1M scale:
// int8 ~0.83, uint8 ~0.24) is a cuVS-layer bug or mo Go plumbing. It builds
// int8 vs uint8 indexes purely through the C++ cuVS wrapper (no mo storage/CDC)
// from the SAME signed dataset and the SAME scalar quantizer, for BOTH source
// element types:
//
//   * f32 source  (gpu_ivf_pq_t<float, T>)  via search_float (auto query quant)
//   * f16 source  (gpu_ivf_pq_t<half,  T>)  via quantize_query + native search
//
// transform<uint8> differs from transform<int8> only by a monotonic +128 shift
// (asserted: 0 mismatches), which is L2-invariant -- so cuVS uint8 recall MUST
// match int8 unless cuVS mishandles uint8 ivf_pq datasets.
//
// Build/run as its OWN executable (does not run the whole test suite):
//   make uint8_quant_bug && ./uint8_quant_bug

#include "cuvs_worker.hpp"
#include "ivf_pq.hpp"
#include "helper.h"
#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <vector>
#include <set>
#include <algorithm>
#include <utility>
#include <cuda_fp16.h>

using namespace matrixone;

namespace {

struct RecallData {
    uint32_t dim; uint64_t count; uint64_t nq;
    std::vector<float> base, queries;
    std::vector<int64_t> ids;
    std::vector<std::vector<int64_t>> gt;
};

RecallData make_signed_recall_data(uint32_t dim, uint64_t count, uint64_t nq, uint32_t k) {
    RecallData d; d.dim = dim; d.count = count; d.nq = nq;
    d.base.resize(count * dim); d.queries.resize(nq * dim); d.ids.resize(count);
    srand(1234);
    auto sgn = []() { return ((float)rand() / RAND_MAX) * 2.0f - 1.0f; }; // [-1,1], zero-centered
    for (uint64_t i = 0; i < count; ++i) {
        for (uint32_t j = 0; j < dim; ++j) d.base[i * dim + j] = sgn();
        d.ids[i] = (int64_t)i;
    }
    for (uint64_t q = 0; q < nq; ++q)
        for (uint32_t j = 0; j < dim; ++j) d.queries[q * dim + j] = sgn();
    d.gt.resize(nq);
    for (uint64_t q = 0; q < nq; ++q) {
        std::vector<std::pair<float, int64_t>> dist(count);
        for (uint64_t i = 0; i < count; ++i) {
            float s = 0;
            for (uint32_t j = 0; j < dim; ++j) { float df = d.queries[q * dim + j] - d.base[i * dim + j]; s += df * df; }
            dist[i] = {s, (int64_t)i};
        }
        std::partial_sort(dist.begin(), dist.begin() + k, dist.end());
        d.gt[q].resize(k);
        for (uint32_t r = 0; r < k; ++r) d.gt[q][r] = dist[r].second;
    }
    return d;
}

double recall_at_k(const ivf_pq_search_result_t& res, const RecallData& d, uint32_t k) {
    size_t hit = 0, tot = 0;
    for (uint64_t q = 0; q < d.nq; ++q) {
        std::set<int64_t> gt(d.gt[q].begin(), d.gt[q].end());
        for (uint32_t r = 0; r < k; ++r) {
            int64_t n = res.neighbors[q * k + r];
            if (gt.count(n)) ++hit;
        }
        tot += k;
    }
    return (double)hit / (double)tot;
}

// --- f32 source path: search_float auto-quantizes the float queries. ---------
template <typename T>
double f32_quantize_recall(RecallData& d, uint32_t k) {
    std::vector<int> devices = {0};
    ivf_pq_build_params_t bp = ivf_pq_build_params_default();
    bp.n_lists = 64;
    gpu_ivf_pq_t<float, T> index(d.count, d.dim, DistanceType_L2Expanded, bp, devices, 1, DistributionMode_SINGLE_GPU);
    index.start();
    index.add_chunk_quantize(d.base.data(), d.count, -1, d.ids.data());
    index.build();
    ivf_pq_search_params_t sp = ivf_pq_search_params_default();
    sp.n_probes = 64;
    auto res = index.search_float(d.queries.data(), d.nq, d.dim, k, sp);
    double r = recall_at_k(res, d, k);
    index.destroy();
    return r;
}

// --- f16 source path: quantize_query then native T search. -------------------
template <typename T>
double f16_quantize_recall(RecallData& d, uint32_t k) {
    std::vector<half> base_h(d.base.size()), query_h(d.queries.size());
    for (size_t i = 0; i < d.base.size(); ++i)   base_h[i]  = __float2half(d.base[i]);
    for (size_t i = 0; i < d.queries.size(); ++i) query_h[i] = __float2half(d.queries[i]);

    std::vector<int> devices = {0};
    ivf_pq_build_params_t bp = ivf_pq_build_params_default();
    bp.n_lists = 64;
    gpu_ivf_pq_t<half, T> index(d.count, d.dim, DistanceType_L2Expanded, bp, devices, 1, DistributionMode_SINGLE_GPU);
    index.start();
    index.add_chunk_quantize(base_h.data(), d.count, -1, d.ids.data());
    index.build();

    std::vector<T> qcodes(d.nq * d.dim);
    index.quantize_query(query_h.data(), d.nq, qcodes.data());
    ivf_pq_search_params_t sp = ivf_pq_search_params_default();
    sp.n_probes = 64;
    auto res = index.search(qcodes.data(), d.nq, d.dim, k, sp);
    double r = recall_at_k(res, d, k);
    index.destroy();
    return r;
}

// Quantize one query through both int8 and uint8 quantizers (same source) and
// count dims where uint8 != int8 + 128. 0 => monotonic, L2-invariant shift.
template <typename B>
int plus128_mismatches(RecallData& d, const std::vector<B>& base_b, const std::vector<B>& query_b) {
    std::vector<int> devices = {0};
    ivf_pq_build_params_t bp = ivf_pq_build_params_default(); bp.n_lists = 64;
    gpu_ivf_pq_t<B, int8_t>  qi(d.count, d.dim, DistanceType_L2Expanded, bp, devices, 1, DistributionMode_SINGLE_GPU);
    qi.start(); qi.add_chunk_quantize(base_b.data(), d.count, -1, d.ids.data()); qi.build();
    gpu_ivf_pq_t<B, uint8_t> qu(d.count, d.dim, DistanceType_L2Expanded, bp, devices, 1, DistributionMode_SINGLE_GPU);
    qu.start(); qu.add_chunk_quantize(base_b.data(), d.count, -1, d.ids.data()); qu.build();
    std::vector<int8_t> ci(d.dim); std::vector<uint8_t> cu(d.dim);
    qi.quantize_query(query_b.data(), 1, ci.data());
    qu.quantize_query(query_b.data(), 1, cu.data());
    int mism = 0;
    for (uint32_t j = 0; j < d.dim; ++j) if ((int)cu[j] != (int)ci[j] + 128) ++mism;
    qi.destroy(); qu.destroy();
    return mism;
}

} // namespace

int main() {
    const uint32_t dim = 32, k = 10;
    const uint64_t count = 4000, nq = 200;
    RecallData d = make_signed_recall_data(dim, count, nq, k);

    int failures = 0;

    // ---- f32 source -------------------------------------------------------
    {
        double r_i8 = f32_quantize_recall<int8_t>(d, k);
        double r_u8 = f32_quantize_recall<uint8_t>(d, k);
        int mism = plus128_mismatches<float>(d, d.base, d.queries);

        printf("\n=== f32 source ===\n");
        printf("[f32] +128 mismatches: %d / %u dims (0 => uint8==int8+128, L2-invariant)\n", mism, dim);
        printf("[f32] f32->int8  recall@%u = %.4f\n", k, r_i8);
        printf("[f32] f32->uint8 recall@%u = %.4f\n", k, r_u8);
        // Data-driven verdict: with 0 mismatches the codes are identical up to a
        // constant shift, so a large recall gap can only come from cuVS's uint8
        // dataset handling. A small gap => the collapse does NOT reproduce here.
        printf("[f32] VERDICT: %s (gap=%.4f, mism=%d)\n",
               (mism == 0 && r_i8 - r_u8 > 0.20) ? "uint8 COLLAPSE reproduced => cuVS uint8 bug"
                                                 : "no collapse at this scale (uint8 ~= int8)",
               r_i8 - r_u8, mism);
        if (!(r_i8 > 0.5)) { printf("[f32] FAIL: int8 recall too low (%.4f)\n", r_i8); ++failures; }
        if (mism != 0)     { printf("[f32] FAIL: %d +128 mismatches\n", mism); ++failures; }
    }

    // ---- f16 source -------------------------------------------------------
    {
        std::vector<half> base_h(d.base.size()), query_h(d.queries.size());
        for (size_t i = 0; i < d.base.size(); ++i)   base_h[i]  = __float2half(d.base[i]);
        for (size_t i = 0; i < d.queries.size(); ++i) query_h[i] = __float2half(d.queries[i]);

        double r_i8 = f16_quantize_recall<int8_t>(d, k);
        double r_u8 = f16_quantize_recall<uint8_t>(d, k);
        int mism = plus128_mismatches<half>(d, base_h, query_h);

        printf("\n=== f16 source ===\n");
        printf("[f16] +128 mismatches: %d / %u dims (0 => uint8==int8+128, L2-invariant)\n", mism, dim);
        printf("[f16] f16->int8  recall@%u = %.4f\n", k, r_i8);
        printf("[f16] f16->uint8 recall@%u = %.4f\n", k, r_u8);
        printf("[f16] VERDICT: %s (gap=%.4f, mism=%d)\n",
               (mism == 0 && r_i8 - r_u8 > 0.20) ? "uint8 COLLAPSE reproduced => cuVS uint8 bug (f16 source)"
                                                 : "no collapse at this scale (uint8 ~= int8)",
               r_i8 - r_u8, mism);
        if (!(r_i8 > 0.5)) { printf("[f16] FAIL: int8 recall too low (%.4f)\n", r_i8); ++failures; }
        if (mism != 0)     { printf("[f16] FAIL: %d +128 mismatches\n", mism); ++failures; }
    }

    printf("\n%s (%d failure(s))\n", failures == 0 ? "PASSED" : "FAILED", failures);
    return failures == 0 ? 0 : 1;
}
