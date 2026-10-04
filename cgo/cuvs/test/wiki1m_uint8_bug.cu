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
// REAL-DATASET repro for the uint8 quantization recall collapse, at 1M scale.
//
// The synthetic 4k repro (uint8_quant_bug.cu) did NOT reproduce the collapse,
// so this drives the SAME pure-C++ cuVS path (no mo Go plumbing) over the real
// wiki_all_1M dataset (1M x 768) with its published ground truth — the exact
// data/scale where the collapse (int8 ~0.83, uint8 ~0.24) was first seen.
//
// For each source element type it builds int8 and uint8 ivf_pq indexes from the
// SAME float base via the SAME scalar quantizer, then grades recall@k against
// the dataset's ground-truth neighbors. transform<uint8> differs from
// transform<int8> only by a monotonic +128 shift (asserted: 0 mismatches), so
// uint8 recall MUST match int8 unless cuVS mishandles uint8 ivf_pq datasets.
//
// Build/run as its own executable:
//   make wiki1m_uint8_bug && ./wiki1m_uint8_bug [base.fbin] [queries.fbin] [gt.ibin]
// Env knobs: MOQ=#queries (default 1000), MOK=k (default 10),
//            MO_NLISTS (default 1024), MO_NPROBES (default 64), MO_F16=1.

#include "cuvs_worker.hpp"
#include "ivf_pq.hpp"
#include "helper.h"
#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <cstring>
#include <string>
#include <vector>
#include <set>
#include <stdexcept>
#include <numeric>
#include <cuda_fp16.h>

using namespace matrixone;

namespace {

const char* kBase    = "../../../vector_benchmark/wiki_all_1M/base.1M.fbin";
const char* kQueries = "../../../vector_benchmark/wiki_all_1M/queries.fbin";
const char* kGt      = "../../../vector_benchmark/wiki_all_1M/groundtruth.1M.neighbors.ibin";

uint64_t env_u64(const char* k, uint64_t def) {
    const char* v = getenv(k);
    return v ? strtoull(v, nullptr, 10) : def;
}

// .fbin: int32 n, int32 dim, then row-major float32[n*dim].
// max_rows<=0 loads all; otherwise only the first max_rows rows.
std::vector<float> load_fbin(const std::string& path, uint64_t& n, uint32_t& dim, int64_t max_rows = -1) {
    FILE* f = fopen(path.c_str(), "rb");
    if (!f) throw std::runtime_error("cannot open " + path);
    int32_t hn = 0, hd = 0;
    if (fread(&hn, 4, 1, f) != 1 || fread(&hd, 4, 1, f) != 1) { fclose(f); throw std::runtime_error("bad header " + path); }
    n = (uint64_t)hn; dim = (uint32_t)hd;
    if (max_rows > 0 && (uint64_t)max_rows < n) n = (uint64_t)max_rows;
    std::vector<float> data(n * dim);
    size_t got = fread(data.data(), sizeof(float), n * dim, f);
    fclose(f);
    if (got != n * dim) throw std::runtime_error("short read " + path);
    return data;
}

// .ibin: int32 n, int32 k, then row-major int32[n*k]. Returns first `keep` cols.
std::vector<std::vector<int64_t>> load_ibin_gt(const std::string& path, uint64_t nq, uint32_t keep) {
    FILE* f = fopen(path.c_str(), "rb");
    if (!f) throw std::runtime_error("cannot open " + path);
    int32_t hn = 0, hk = 0;
    if (fread(&hn, 4, 1, f) != 1 || fread(&hk, 4, 1, f) != 1) { fclose(f); throw std::runtime_error("bad header " + path); }
    uint32_t k = (uint32_t)hk;
    if (keep > k) throw std::runtime_error("gt k too small");
    std::vector<int32_t> row(k);
    std::vector<std::vector<int64_t>> gt(nq);
    for (uint64_t q = 0; q < nq; ++q) {
        if (fread(row.data(), 4, k, f) != k) { fclose(f); throw std::runtime_error("short gt read"); }
        gt[q].resize(keep);
        for (uint32_t r = 0; r < keep; ++r) gt[q][r] = (int64_t)row[r];
    }
    fclose(f);
    return gt;
}

double recall_at_k(const ivf_pq_search_result_t& res,
                   const std::vector<std::vector<int64_t>>& gt, uint64_t nq, uint32_t k) {
    size_t hit = 0, tot = 0;
    for (uint64_t q = 0; q < nq; ++q) {
        std::set<int64_t> g(gt[q].begin(), gt[q].end());
        for (uint32_t r = 0; r < k; ++r)
            if (g.count(res.neighbors[q * k + r])) ++hit;
        tot += k;
    }
    return (double)hit / (double)tot;
}

struct Cfg { uint32_t n_lists, n_probes, m, bits; };

inline void apply_cfg(ivf_pq_build_params_t& bp, const Cfg& c) {
    bp.n_lists = c.n_lists;
    if (c.m) bp.m = c.m;
    if (c.bits) bp.bits_per_code = c.bits;
}

template <typename T>
double f32_recall(const std::vector<float>& base, uint64_t count, uint32_t dim,
                  const std::vector<float>& queries, uint64_t nq,
                  const std::vector<std::vector<int64_t>>& gt, uint32_t k, const Cfg& c) {
    std::vector<int64_t> ids(count);
    std::iota(ids.begin(), ids.end(), (int64_t)0);  // row index == gt id space

    std::vector<int> devices = {0};
    ivf_pq_build_params_t bp = ivf_pq_build_params_default();
    apply_cfg(bp, c);
    gpu_ivf_pq_t<float, T> index(count, dim, DistanceType_L2Expanded, bp, devices, 1, DistributionMode_SINGLE_GPU);
    index.start();
    index.add_chunk_quantize(base.data(), count, -1, ids.data());
    index.build();
    ivf_pq_search_params_t sp = ivf_pq_search_params_default();
    sp.n_probes = c.n_probes;
    auto res = index.search_quantize(queries.data(), nq, dim, k, sp);
    double r = recall_at_k(res, gt, nq, k);
    index.destroy();
    return r;
}

// Same as f32_recall but exercises the MO persist->load cycle: build, save_dir,
// destroy, reload a fresh index via load_dir, then search. This is what
// mo-service does (Pack/Unpack -> save_dir/load_dir). If uint8 recall is fine in
// f32_recall but collapses here, the bug is in cuVS serialize/deserialize of a
// uint8-built ivf_pq index (the quantizer.bin is byte-identical for int8/uint8).
template <typename T>
double f32_recall_saveload(const std::vector<float>& base, uint64_t count, uint32_t dim,
                           const std::vector<float>& queries, uint64_t nq,
                           const std::vector<std::vector<int64_t>>& gt, uint32_t k, const Cfg& c,
                           const std::string& dir) {
    std::vector<int64_t> ids(count);
    std::iota(ids.begin(), ids.end(), (int64_t)0);

    std::vector<int> devices = {0};
    ivf_pq_build_params_t bp = ivf_pq_build_params_default();
    apply_cfg(bp, c);
    {
        gpu_ivf_pq_t<float, T> index(count, dim, DistanceType_L2Expanded, bp, devices, 1, DistributionMode_SINGLE_GPU);
        index.start();
        index.add_chunk_quantize(base.data(), count, -1, ids.data());
        index.build();
        index.save_dir(dir);   // writes index.bin + quantizer.bin + manifest
        index.destroy();
    }
    // Fresh index, load from disk exactly like LoadIndex/Unpack does.
    gpu_ivf_pq_t<float, T> reloaded(count, dim, DistanceType_L2Expanded, bp, devices, 1, DistributionMode_SINGLE_GPU);
    reloaded.start();
    reloaded.load_dir(dir, DistributionMode_SINGLE_GPU);
    ivf_pq_search_params_t sp = ivf_pq_search_params_default();
    sp.n_probes = c.n_probes;
    auto res = reloaded.search_quantize(queries.data(), nq, dim, k, sp);
    double r = recall_at_k(res, gt, nq, k);
    reloaded.destroy();
    return r;
}

template <typename T>
double f16_recall(const std::vector<half>& base_h, uint64_t count, uint32_t dim,
                  const std::vector<half>& query_h, uint64_t nq,
                  const std::vector<std::vector<int64_t>>& gt, uint32_t k, const Cfg& c) {
    std::vector<int64_t> ids(count);
    std::iota(ids.begin(), ids.end(), (int64_t)0);

    std::vector<int> devices = {0};
    ivf_pq_build_params_t bp = ivf_pq_build_params_default();
    apply_cfg(bp, c);
    gpu_ivf_pq_t<half, T> index(count, dim, DistanceType_L2Expanded, bp, devices, 1, DistributionMode_SINGLE_GPU);
    index.start();
    index.add_chunk_quantize(base_h.data(), count, -1, ids.data());
    index.build();

    std::vector<T> qcodes(nq * dim);
    index.quantize_query(query_h.data(), nq, qcodes.data());
    ivf_pq_search_params_t sp = ivf_pq_search_params_default();
    sp.n_probes = c.n_probes;
    auto res = index.search(qcodes.data(), nq, dim, k, sp);
    double r = recall_at_k(res, gt, nq, k);
    index.destroy();
    return r;
}

// Count dims where uint8 query code != int8 query code + 128, using the same
// B source quantizer trained on the same base. 0 => monotonic L2-invariant.
template <typename B>
int plus128_mismatches(const std::vector<B>& base_b, const std::vector<B>& query_b,
                       uint64_t count, uint32_t dim, const Cfg& c) {
    std::vector<int64_t> ids(count);
    std::iota(ids.begin(), ids.end(), (int64_t)0);
    std::vector<int> devices = {0};
    ivf_pq_build_params_t bp = ivf_pq_build_params_default(); apply_cfg(bp, c);
    gpu_ivf_pq_t<B, int8_t>  qi(count, dim, DistanceType_L2Expanded, bp, devices, 1, DistributionMode_SINGLE_GPU);
    qi.start(); qi.add_chunk_quantize(base_b.data(), count, -1, ids.data()); qi.build();
    gpu_ivf_pq_t<B, uint8_t> qu(count, dim, DistanceType_L2Expanded, bp, devices, 1, DistributionMode_SINGLE_GPU);
    qu.start(); qu.add_chunk_quantize(base_b.data(), count, -1, ids.data()); qu.build();
    std::vector<int8_t> ci(dim); std::vector<uint8_t> cu(dim);
    qi.quantize_query(query_b.data(), 1, ci.data());
    qu.quantize_query(query_b.data(), 1, cu.data());
    int mism = 0;
    for (uint32_t j = 0; j < dim; ++j) if ((int)cu[j] != (int)ci[j] + 128) ++mism;
    qi.destroy(); qu.destroy();
    return mism;
}

} // namespace

int main(int argc, char** argv) {
    const char* base_path = argc > 1 ? argv[1] : kBase;
    const char* qry_path  = argc > 2 ? argv[2] : kQueries;
    const char* gt_path   = argc > 3 ? argv[3] : kGt;

    const uint32_t k   = (uint32_t)env_u64("MOK", 10);
    const uint64_t nq  = env_u64("MOQ", 1000);
    Cfg cfg{ (uint32_t)env_u64("MO_NLISTS", 1024), (uint32_t)env_u64("MO_NPROBES", 64),
             (uint32_t)env_u64("MO_M", 0), (uint32_t)env_u64("MO_BITS", 0) };  // m/bits 0 => cuVS default
    const bool do_f16  = env_u64("MO_F16", 0) != 0;

    printf("Loading base %s ...\n", base_path);
    uint64_t count = 0; uint32_t dim = 0;
    std::vector<float> base = load_fbin(base_path, count, dim, -1);  // FULL 1M (gt requires it)
    printf("  base: count=%lu dim=%u\n", (unsigned long)count, dim);

    uint64_t qn_file = 0; uint32_t qdim = 0;
    std::vector<float> queries = load_fbin(qry_path, qn_file, qdim, (int64_t)nq);
    if (qdim != dim) { printf("dim mismatch base=%u query=%u\n", dim, qdim); return 2; }
    uint64_t use_nq = qn_file;
    printf("  queries: nq=%lu dim=%u\n", (unsigned long)use_nq, qdim);

    auto gt = load_ibin_gt(gt_path, use_nq, k);
    printf("  gt loaded for %lu queries, k=%u\n", (unsigned long)use_nq, k);
    printf("  cfg: n_lists=%u n_probes=%u m=%u bits=%u (0=cuVS default)\n\n",
           cfg.n_lists, cfg.n_probes, cfg.m, cfg.bits);

    const bool do_saveload = env_u64("MO_SAVELOAD", 0) != 0;
    int failures = 0;

    // ---- f32 source -------------------------------------------------------
    {
        double r_i8 = f32_recall<int8_t>(base, count, dim, queries, use_nq, gt, k, cfg);
        double r_u8 = f32_recall<uint8_t>(base, count, dim, queries, use_nq, gt, k, cfg);
        int mism = plus128_mismatches<float>(base, queries, count, dim, cfg);

        printf("\n=== f32 source (wiki_all_1M) ===\n");
        printf("[f32] +128 mismatches: %d / %u dims (0 => uint8==int8+128, L2-invariant)\n", mism, dim);
        printf("[f32] f32->int8  recall@%u = %.4f\n", k, r_i8);
        printf("[f32] f32->uint8 recall@%u = %.4f\n", k, r_u8);
        printf("[f32] VERDICT: %s (gap=%.4f, mism=%d)\n",
               (mism == 0 && r_i8 - r_u8 > 0.20) ? "uint8 COLLAPSE reproduced => cuVS uint8 bug"
                                                 : "no collapse (uint8 ~= int8)",
               r_i8 - r_u8, mism);
        if (mism != 0) { printf("[f32] FAIL: %d +128 mismatches\n", mism); ++failures; }

        // The real mo path: build -> save_dir -> load_dir -> search.
        if (do_saveload) {
            double sl_i8 = f32_recall_saveload<int8_t>(base, count, dim, queries, use_nq, gt, k, cfg, "/tmp/ivfpq_sl_i8");
            double sl_u8 = f32_recall_saveload<uint8_t>(base, count, dim, queries, use_nq, gt, k, cfg, "/tmp/ivfpq_sl_u8");
            printf("[f32+saveload] f32->int8  recall@%u = %.4f (in-proc %.4f)\n", k, sl_i8, r_i8);
            printf("[f32+saveload] f32->uint8 recall@%u = %.4f (in-proc %.4f)\n", k, sl_u8, r_u8);
            printf("[f32+saveload] VERDICT: %s\n",
                   (sl_i8 - sl_u8 > 0.20)
                       ? "uint8 COLLAPSE after save/load => cuVS serialize/deserialize bug for uint8 ivf_pq"
                       : "uint8 survives save/load (~= int8)");
        }
    }

    // ---- f16 source (optional; converts the 3GB float base -> half) -------
    if (do_f16) {
        std::vector<half> base_h(base.size()), query_h(queries.size());
        for (size_t i = 0; i < base.size(); ++i)    base_h[i]  = __float2half(base[i]);
        for (size_t i = 0; i < queries.size(); ++i) query_h[i] = __float2half(queries[i]);

        double r_i8 = f16_recall<int8_t>(base_h, count, dim, query_h, use_nq, gt, k, cfg);
        double r_u8 = f16_recall<uint8_t>(base_h, count, dim, query_h, use_nq, gt, k, cfg);
        int mism = plus128_mismatches<half>(base_h, query_h, count, dim, cfg);

        printf("\n=== f16 source (wiki_all_1M) ===\n");
        printf("[f16] +128 mismatches: %d / %u dims (0 => uint8==int8+128, L2-invariant)\n", mism, dim);
        printf("[f16] f16->int8  recall@%u = %.4f\n", k, r_i8);
        printf("[f16] f16->uint8 recall@%u = %.4f\n", k, r_u8);
        printf("[f16] VERDICT: %s (gap=%.4f, mism=%d)\n",
               (mism == 0 && r_i8 - r_u8 > 0.20) ? "uint8 COLLAPSE reproduced => cuVS uint8 bug (f16 source)"
                                                 : "no collapse (uint8 ~= int8)",
               r_i8 - r_u8, mism);
        if (mism != 0) { printf("[f16] FAIL: %d +128 mismatches\n", mism); ++failures; }
    }

    printf("\n%s (%d hard failure(s); inspect recall gaps above)\n",
           failures == 0 ? "DONE" : "FAILED", failures);
    return failures == 0 ? 0 : 1;
}
