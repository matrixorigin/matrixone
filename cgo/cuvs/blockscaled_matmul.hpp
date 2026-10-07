// Copyright 2026 Matrix Origin
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

#pragma once

// Block-scaled dot-product matmul on cuBLASLt for vecf8 (MXFP8) and vecf4 (NVFP4) cells,
// and a plain matmul for vecf32, vecf16, vecbf16, vecint8 and vecuint8 rows (float formats:
// CUBLAS_COMPUTE_32F, fp32 accumulation, no TF32; integer formats: CUBLAS_COMPUTE_32I).
//
// A cell is the vecblock.go layout: a 12-byte header (version, format, reserved[2],
// dim uint32 LE, global float32 LE), one scale byte per block (E8M0 per 32 elements for
// MXFP8, UE4M3 per 16 for NVFP4), then the packed elements (one E4M3 byte each, or two
// E2M1 nibbles per byte with element 2i in the low nibble).
//
// The dataset tile is operand A and the queries are operand B: D = A^T B (TN), D column
// major (rows x queries). cuBLASLt needs:
//   - rows padded to a multiple of 128 (a 1-row A computes wrong results), with zero
//     elements in the padding rows; the queries are padded the same way;
//   - K padded to a multiple of 32, with zero elements;
//   - scales in the tiled layout: 128-row x 4-block tiles of 512 bytes,
//     off = ((r/128)*(Sp/4) + s/4)*512 + (r%32)*16 + ((r%128)/32)*4 + s%4.
// The scores are computed on the device for a metric: inner product, cosine distance or
// squared L2 distance. bsmm_row_stats_kernel computes each row's squared norm from the
// packed tile (the values the matmul multiplies) and the uint8 element sums;
// bsmm_fixup_kernel turns the dot products into rank scores, the negated distance (largest
// is nearest): dot, -(1 - clamp(dot / sqrt(|x|^2 |q|^2), -1, 1)) or
// -max(0, |x|^2 + |q|^2 - 2 dot). run copies the rank scores of every row; run_topk
// selects the k best rows per query with cuvs::selection::select_k and copies only those
// back.

#include "device_memory.hpp"

#include <cub/block/block_reduce.cuh>
#include <cublasLt.h>
#include <cuda_bf16.h>
#include <cuda_fp16.h>
#include <cuda_fp4.h>
#include <cuda_fp8.h>
#include <cuda_runtime.h>
#include <cuvs/selection/select_k.hpp>
#include <raft/core/device_mdspan.hpp>
#include <raft/core/resource/cuda_stream.hpp>
#include <raft/core/resources.hpp>

#include <algorithm>
#include <atomic>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <string>
#include <vector>

namespace matrixone {

namespace {

// Metrics of the rank scores.
constexpr int kMetricInnerProduct = 0;
constexpr int kMetricCosine = 1;
constexpr int kMetricL2sq = 2;


// bsmm_scale_off is the offset of block s of row r in the tiled scale layout.
__device__ inline uint64_t bsmm_scale_off(uint64_t r, uint64_t s, uint64_t Sp) {
    const uint64_t tile = (r / 128) * (Sp / 4) + s / 4;
    const uint64_t rr = r % 128;
    return tile * 512 + (rr % 32) * 16 + (rr / 32) * 4 + s % 4;
}

// bsmm_elem is element k of packed row r of a float format, before the row's global scale:
// the code times its block scale for MXFP8/NVFP4, the value for F32/F16/BF16.
__device__ inline double bsmm_elem(int format, const uint8_t* row, const uint8_t* scale,
                                   uint64_t r, uint64_t k, uint64_t Sp) {
    switch (format) {
    case 1: { // MXFP8: E4M3 element, E8M0 scale per 32
        __nv_fp8_e4m3 e;
        e.__x = row[k];
        return double(float(e)) * ldexp(1.0, int(scale[bsmm_scale_off(r, k / 32, Sp)]) - 127);
    }
    case 2: { // NVFP4: E2M1 nibble (element 2i low), UE4M3 scale per 16
        __nv_fp4_e2m1 e;
        e.__x = (k & 1) ? (row[k / 2] >> 4) : (row[k / 2] & 0x0f);
        __nv_fp8_e4m3 sc;
        sc.__x = scale[bsmm_scale_off(r, k / 16, Sp)];
        return double(float(e)) * double(float(sc));
    }
    case 3: // F32
        return double(reinterpret_cast<const float*>(row)[k]);
    case 4: // F16
        return double(__half2float(reinterpret_cast<const __half*>(row)[k]));
    case 7: // BF16
        return double(__bfloat162float(reinterpret_cast<const __nv_bfloat16*>(row)[k]));
    }
    return 0;
}

// bsmm_fixup_kernel turns the raw matmul output d (query major, M rows per query) into
// rank scores in place, the negated distance of the metric (largest is nearest). For float
// formats the inner product is d * float(g_row * g_query) in float, as cuBLASLt applies
// alpha = G_a * G_b to a GEMM with per-tensor global scales; cosine and l2sq take the dot
// product d * g_row * g_query in double, with the squared norms. Integer formats take the
// int32 sums with the uint8
// shift correction. The rank is rounded once to float; NaN and padding rows r >= n are
// -Inf. The cosine similarity is clamped to [-1, 1] and the squared L2 to >= 0, so no
// distance is negative. A zero vector has cosine distance 1.
//
// WARNING: near-zero recomputation is POISON. DO NOT add an exact recomputation of
// near-zero distances (sum (x - q)^2 per pair), a CPU re-score, or any other second scoring
// of a row here or in the caller. Cosine and l2sq are the GEMM expansion and carry fp32
// rounding relative to the squared norms near 0; that is the result contract, by decision
// of cpegeric. An exact near-zero pass was implemented, measured 7x slower (1.37 s -> 10.1
// s, 1M x 768, 128 queries) and removed: see "GPU cosine and squared L2 are the GEMM
// expansion" under Decisions in docs/design/20260930-low-precision-vector-storage.md.
__global__ void bsmm_fixup_kernel(float* d, int kind, int metric, uint64_t M, uint64_t n,
                                  uint64_t nq, const float* g_row, const float* g_query,
                                  const int64_t* sum_row, const int64_t* sum_query,
                                  int64_t base, const double* norm_row,
                                  const double* norm_query) {
    const uint64_t total = M * nq;
    for (uint64_t i = uint64_t(blockIdx.x) * blockDim.x + threadIdx.x; i < total;
         i += uint64_t(gridDim.x) * blockDim.x) {
        const uint64_t q = i / M, r = i % M;
        float v;
        if (r >= n) {
            v = -INFINITY;
        } else {
            double dot;
            if (kind == 0) {
                dot = metric == kMetricInnerProduct
                          ? double(__fmul_rn(d[i], __fmul_rn(g_row[r], g_query[q])))
                          : double(d[i]) * double(g_row[r]) * double(g_query[q]);
            } else {
                int64_t s = int64_t(__float_as_int(d[i]));
                if (kind == 2) s += 128 * (sum_row[r] + sum_query[q]) + base;
                dot = double(s);
            }
            double rank = dot;
            if (metric == kMetricCosine) {
                const double den = sqrt(norm_row[r] * norm_query[q]);
                rank = den > 0 ? -(1.0 - fmin(1.0, fmax(-1.0, dot / den))) : -1.0;
            } else if (metric == kMetricL2sq) {
                rank = -fmax(0.0, norm_row[r] + norm_query[q] - 2.0 * dot);
            }
            v = float(rank);
        }
        if (isnan(v)) v = -INFINITY;
        d[i] = v;
    }
}

// bsmm_row_stats_kernel computes, for each of the n packed rows, the squared norm of the
// values the matmul multiplies (elements times block scales times the row's global scale, in
// double; exact integers for int8/uint8) into norm, and for uint8 the sum of the shifted
// elements x' = x - 128 into sum. kLanes threads take a row (1, or a warp of 32), and the
// grid loops over the rows; format is a compile-time constant. With shift (cosine and l2sq on
// F32, BF16 and MXFP8) a row whose squared norm without its global scale is outside
// [2^-60, 2^60] is then scaled by a power of two 2^-k, |x| near 2^k, so its fp32 matmul with
// a query neither overflows nor underflows, and its global scale multiplied by 2^k: F32 and
// BF16 elements are scaled, MXFP8 block scale exponents shifted, and a block shifted below
// the E8M0 range zeroed. The format constants are those of blockscaled_matmul.
template <int kLanes, int format>
__global__ void bsmm_row_stats_kernel(uint8_t* elem, uint8_t* scale, float* global, uint64_t n,
                                      uint64_t row_bytes, uint64_t dim, uint64_t Sp, double* norm,
                                      int64_t* sum, bool rescale) {
    const uint64_t thread = uint64_t(blockIdx.x) * blockDim.x + threadIdx.x;
    const uint64_t lane = thread % kLanes;
    const uint64_t groups = uint64_t(gridDim.x) * blockDim.x / kLanes;
    for (uint64_t r = thread / kLanes; r < n; r += groups) {
        uint8_t* row = elem + r * row_bytes;
        double acc = 0;
        long long iacc = 0, isum = 0;
        for (uint64_t k = lane; k < dim; k += kLanes) {
            switch (format) {
            case 5: { // I8
                const long long v = reinterpret_cast<const int8_t*>(row)[k];
                iacc += v * v;
                break;
            }
            case 6: { // U8, stored shifted: x' = x - 128
                const long long x = reinterpret_cast<const int8_t*>(row)[k];
                iacc += (x + 128) * (x + 128);
                isum += x;
                break;
            }
            default: {
                const double v = bsmm_elem(format, row, scale, r, k, Sp);
                acc += v * v;
            }
            }
        }
        if (kLanes > 1) {
            for (int o = kLanes / 2; o > 0; o >>= 1) {
                acc += __shfl_down_sync(0xffffffff, acc, o);
                iacc += __shfl_down_sync(0xffffffff, iacc, o);
                isum += __shfl_down_sync(0xffffffff, isum, o);
            }
        }
        int k = 0;
        if (lane == 0) {
            if (format == 5 || format == 6) {
                norm[r] = double(iacc);
                if (sum != nullptr) sum[r] = isum;
            } else {
                const double g = double(global[r]);
                norm[r] = acc * g * g;
                if (rescale && acc > 0 && (acc < 0x1p-60 || acc > 0x1p60)) {
                    k = max(-126, min(127, int(lrint(log2(acc) / 2))));
                }
            }
        }
        if (kLanes > 1) k = __shfl_sync(0xffffffff, k, 0);
        if (k == 0) continue;
        switch (format) {
        case 1: // MXFP8: shift the E8M0 exponents
            for (uint64_t s = lane; s < (dim + 31) / 32; s += kLanes) {
                const uint64_t off = bsmm_scale_off(r, s, Sp);
                int e = int(scale[off]) - k;
                if (e < 0) {
                    for (uint64_t i = s * 32; i < min(dim, s * 32 + 32); i++) row[i] = 0;
                    e = 0;
                }
                scale[off] = uint8_t(min(e, 254));
            }
            break;
        case 3: { // F32
            float* p = reinterpret_cast<float*>(row);
            for (uint64_t i = lane; i < dim; i += kLanes) p[i] = ldexpf(p[i], -k);
            break;
        }
        case 7: { // BF16
            __nv_bfloat16* p = reinterpret_cast<__nv_bfloat16*>(row);
            for (uint64_t i = lane; i < dim; i += kLanes) {
                p[i] = __float2bfloat16(ldexpf(__bfloat162float(p[i]), -k));
            }
            break;
        }
        }
        if (lane == 0) global[r] = ldexpf(global[r], k);
    }
}

// bsmm_ties_kernel sets tied[q] when query q has more real rows (r < n) at its k-th score
// than select_k kept; one block per query.
template <int kThreads>
__global__ void bsmm_ties_kernel(const float* d, uint64_t M, uint64_t n, const float* top_val,
                                 const int* top_idx, int k, uint8_t* tied) {
    using reduce_f = cub::BlockReduce<float, kThreads>;
    using reduce_i = cub::BlockReduce<unsigned long long, kThreads>;
    __shared__ union {
        typename reduce_f::TempStorage f;
        typename reduce_i::TempStorage i;
    } tmp;
    __shared__ float kth;
    const uint64_t q = blockIdx.x;
    const float* tv = top_val + q * k;
    const int* ti = top_idx + q * k;

    float lo = INFINITY;
    for (int j = threadIdx.x; j < k; j += kThreads) lo = fminf(lo, tv[j]);
    lo = reduce_f(tmp.f).Reduce(lo, [](float a, float b) { return fminf(a, b); });
    if (threadIdx.x == 0) kth = lo;
    __syncthreads();

    unsigned long long sel = 0;
    for (int j = threadIdx.x; j < k; j += kThreads) {
        if (tv[j] == kth && ti[j] >= 0 && uint64_t(ti[j]) < n) sel++;
    }
    sel = reduce_i(tmp.i).Sum(sel);
    __syncthreads();
    unsigned long long all = 0;
    const float* col = d + q * M;
    for (uint64_t r = threadIdx.x; r < n; r += kThreads) {
        if (col[r] == kth) all++;
    }
    all = reduce_i(tmp.i).Sum(all);
    if (threadIdx.x == 0) tied[q] = all > sel ? 1 : 0;
}

} // namespace

class blockscaled_matmul {
public:
    static constexpr int kFormatMXFP8 = 1;
    static constexpr int kFormatNVFP4 = 2;
    // Plain formats: rows are raw float32 (vecf32), IEEE half (vecf16), bfloat16 (vecbf16),
    // int8 (vecint8) or uint8 (vecuint8) vectors, with no header, scales or global. int8 runs on the integer
    // tensor cores with exact int32 sums; uint8 is shifted to int8 (x - 128) and corrected
    // with the row and query sums: x.q = x'.q' + 128 (sum x' + sum q') + 128^2 dim.
    static constexpr int kFormatF32 = 3;
    static constexpr int kFormatF16 = 4;
    static constexpr int kFormatI8 = 5;
    static constexpr int kFormatU8 = 6;
    static constexpr int kFormatBF16 = 7;
    static constexpr size_t kHeader = 12;
    static constexpr size_t kWorkspace = size_t(32) << 20;
    // Metrics: the scores are the negated distances (largest is nearest).
    static constexpr int kInnerProduct = kMetricInnerProduct;
    static constexpr int kCosine = kMetricCosine;
    static constexpr int kL2sq = kMetricL2sq;
    // kMinComputeMajor is the supported GPU baseline: compute capability 10.0 or newer
    // (Blackwell, sm_100 / sm_120), which the MXFP8 and NVFP4 block-scaled modes need.
    // The engine runs only on such devices, for every format.
    static constexpr int kMinComputeMajor = 10;

    static bool meets_baseline(int compute_major) { return compute_major >= kMinComputeMajor; }

    // eligible_devices returns the visible devices meeting the baseline, queried once per
    // process.
    static const std::vector<int>& eligible_devices() {
        static const std::vector<int> devices = [] {
            std::vector<int> out;
            int n = 0;
            if (cudaGetDeviceCount(&n) != cudaSuccess) {
                cudaGetLastError();
                return out;
            }
            for (int d = 0; d < n; d++) {
                int major = 0;
                if (cudaDeviceGetAttribute(&major, cudaDevAttrComputeCapabilityMajor, d) ==
                        cudaSuccess &&
                    meets_baseline(major)) {
                    out.push_back(d);
                }
            }
            cudaGetLastError();
            return out;
        }();
        return devices;
    }

    // next_device returns an eligible device round-robin, or -1 when there is none.
    static int next_device() {
        static std::atomic<uint64_t> next{0};
        const std::vector<int>& devices = eligible_devices();
        if (devices.empty()) return -1;
        return devices[next.fetch_add(1) % devices.size()];
    }

    // host_bytes returns the host memory an engine of this shape allocates: the tile
    // staging and score copy, per-row and per-query globals, and the query packing buffers
    // used while it is constructed.
    static uint64_t host_bytes(int format, uint32_t dim, uint32_t nq, uint64_t max_rows) {
        const shape sh = shape::of(format, dim);
        const uint64_t M = roundup(max_rows, 128), nq_pad = roundup(nq, 128);
        return M * sh.row_bytes + M * sh.Sp + M * nq * sizeof(float) + M * sizeof(float) +
               nq * sizeof(float) + nq_pad * (sh.row_bytes + sh.Sp);
    }

    // topk > 0 sizes the buffers of run_topk; 0 allows run only. metric is kInnerProduct,
    // kCosine or kL2sq.
    blockscaled_matmul(int device_id, int format, uint32_t dim, uint32_t nq,
                       const uint8_t* query_cells, uint64_t max_rows, uint32_t topk = 0,
                       int metric = kInnerProduct)
        : device_id_(device_id) {
        if (format < kFormatMXFP8 || format > kFormatBF16 || dim == 0 || nq == 0 ||
            max_rows == 0) {
            throw std::invalid_argument(
                "blockscaled_matmul: invalid format, dimension, query count or tile size");
        }
        if (metric < kInnerProduct || metric > kL2sq) {
            throw std::invalid_argument("blockscaled_matmul: invalid metric");
        }
        metric_ = metric;
        format_ = format;
        dim_ = dim;
        const shape sh = shape::of(format, dim);
        K_ = sh.K;
        block_ = sh.block;
        nscale_ = sh.nscale;
        elem_bytes_ = sh.elem_bytes;
        cell_bytes_ = sh.cell_bytes;
        Sp_ = sh.Sp;
        row_bytes_ = sh.row_bytes;
        nq_ = nq;
        nq_pad_ = roundup(nq, 128);
        max_rows_ = roundup(max_rows, 128);
        topk_ = std::min<uint64_t>(topk, max_rows_);

        try {
            init(query_cells);
        } catch (...) {
            cleanup();
            throw;
        }
    }

    ~blockscaled_matmul() { cleanup(); }

    blockscaled_matmul(const blockscaled_matmul&) = delete;
    blockscaled_matmul& operator=(const blockscaled_matmul&) = delete;

    uint64_t max_rows() const { return max_rows_; }

    // run scores n cells packed back to back (n <= max_rows) into n * nq row-major rank
    // scores (NaN as -Inf).
    void run(const uint8_t* cells, uint64_t n, float* scores) {
        if (n == 0) return;
        const size_t M = matmul(cells, n);
        fixup(M, n);
        // D is column major M x nq_pad; the first nq columns hold the queries
        check(cudaMemcpy2DAsync(h_d_.data(), n * sizeof(float), d_d_, M * sizeof(float),
                                n * sizeof(float), nq_, cudaMemcpyDeviceToHost, stream_),
              "cudaMemcpy2DAsync");
        check(cudaStreamSynchronize(stream_), "cudaStreamSynchronize");
        // h_d is query major (nq x n); scores are row major
        for (size_t r = 0; r < n; r++) {
            for (size_t q = 0; q < nq_; q++) scores[r * nq_ + q] = h_d_[q * n + r];
        }
    }

    // run_topk scores n cells like run and keeps, per query, the topk best rows of the tile
    // (NaN as -Inf). top_scores and top_rows receive nq x topk entries, query major; slots
    // past min(topk, n) hold row -1. A query with more rows tied at its topk-th score than
    // were kept gets tied[q] = 1 and its n scores in full_scores[q * n, (q + 1) * n);
    // full_scores is untouched for the other queries.
    void run_topk(const uint8_t* cells, uint64_t n, float* top_scores, int32_t* top_rows,
                  float* full_scores, uint8_t* tied) {
        if (topk_ == 0) {
            throw std::invalid_argument("blockscaled_matmul: engine created without topk");
        }
        if (n == 0) return;
        const size_t M = matmul(cells, n);
        float* d = static_cast<float*>(d_d_);
        fixup(M, n);

        const int64_t kk = int64_t(std::min<uint64_t>(topk_, M));
        float* tv = static_cast<float*>(d_top_val_);
        int* ti = static_cast<int*>(d_top_idx_);
        cuvs::selection::select_k(
            res_, raft::make_device_matrix_view<const float, int64_t>(d, int64_t(nq_), int64_t(M)),
            std::nullopt, raft::make_device_matrix_view<float, int64_t>(tv, int64_t(nq_), kk),
            raft::make_device_matrix_view<int, int64_t>(ti, int64_t(nq_), kk), false);
        bsmm_ties_kernel<256><<<unsigned(nq_), 256, 0, stream_>>>(
            d, M, n, tv, ti, int(kk), static_cast<uint8_t*>(d_tied_));
        check(cudaGetLastError(), "bsmm_ties_kernel");

        check(cudaMemcpy2DAsync(top_scores, topk_ * sizeof(float), tv, kk * sizeof(float),
                                kk * sizeof(float), nq_, cudaMemcpyDeviceToHost, stream_),
              "cudaMemcpy2DAsync");
        check(cudaMemcpy2DAsync(top_rows, topk_ * sizeof(int32_t), ti, kk * sizeof(int),
                                kk * sizeof(int), nq_, cudaMemcpyDeviceToHost, stream_),
              "cudaMemcpy2DAsync");
        check(cudaMemcpyAsync(tied, d_tied_, nq_, cudaMemcpyDeviceToHost, stream_),
              "cudaMemcpyAsync");
        check(cudaStreamSynchronize(stream_), "cudaStreamSynchronize");

        for (size_t q = 0; q < nq_; q++) {
            int32_t* rows = top_rows + q * topk_;
            for (uint64_t j = 0; j < topk_; j++) {
                if (int64_t(j) >= kk || rows[j] < 0 || uint64_t(rows[j]) >= n) rows[j] = -1;
            }
            if (tied[q] != 0) {
                check(cudaMemcpyAsync(full_scores + q * n, d + q * M, n * sizeof(float),
                                      cudaMemcpyDeviceToHost, stream_),
                      "cudaMemcpyAsync");
            }
        }
        check(cudaStreamSynchronize(stream_), "cudaStreamSynchronize");
    }

private:
    // shape is the cell and padded-row layout of a format and dimension.
    struct shape {
        size_t K = 0, block = 0, nscale = 0, elem_bytes = 0, cell_bytes = 0, Sp = 0,
               row_bytes = 0;

        static shape of(int format, uint32_t dim) {
            shape sh;
            sh.K = roundup(dim, 32);
            if (plain(format)) {
                const size_t esize = format == kFormatF32                           ? 4
                                     : format == kFormatF16 || format == kFormatBF16 ? 2
                                                                                     : 1;
                sh.elem_bytes = size_t(dim) * esize;
                sh.cell_bytes = sh.elem_bytes;
                sh.row_bytes = sh.K * esize;
            } else {
                sh.block = format == kFormatMXFP8 ? 32 : 16;
                sh.nscale = (dim + sh.block - 1) / sh.block;
                sh.elem_bytes = format == kFormatMXFP8 ? dim : (dim + 1) / 2;
                sh.cell_bytes = kHeader + sh.nscale + sh.elem_bytes;
                sh.Sp = roundup(sh.K / sh.block, 4);
                sh.row_bytes = format == kFormatMXFP8 ? sh.K : sh.K / 2;
            }
            return sh;
        }
    };

    // matmul packs and uploads n cells and enqueues D = A^T B on the stream; it returns the
    // padded row count M.
    size_t matmul(const uint8_t* cells, uint64_t n) {
        if (n > max_rows_) {
            throw std::invalid_argument("blockscaled_matmul: tile exceeds the row capacity");
        }
        // the smallest row bucket holding n: its algorithm was found at construction
        size_t b = 0;
        while (algos_[b].first < n) ++b;
        const size_t M = algos_[b].first;
        std::fill(h_a_.begin(), h_a_.begin() + M * row_bytes_, 0);
        std::fill(h_sa_.begin(), h_sa_.begin() + M * Sp_, 0);
        pack_rows(cells, n, h_a_.data(), h_sa_.data(), g_row_.data());

        check(cudaSetDevice(device_id_), "cudaSetDevice");
        check(cudaMemcpyAsync(d_a_, h_a_.data(), M * row_bytes_, cudaMemcpyHostToDevice, stream_),
              "cudaMemcpyAsync");
        check(cudaMemcpyAsync(d_sa_, h_sa_.data(), M * Sp_, cudaMemcpyHostToDevice, stream_),
              "cudaMemcpyAsync");
        check(cudaMemcpyAsync(d_g_row_, g_row_.data(), n * sizeof(float), cudaMemcpyHostToDevice,
                              stream_),
              "cudaMemcpyAsync");
        row_stats(d_a_, d_sa_, d_g_row_, n, d_norm_row_, d_sum_row_);

        cudaDataType_t et = format_ == kFormatMXFP8   ? CUDA_R_8F_E4M3
                            : format_ == kFormatNVFP4 ? CUDA_R_4F_E2M1
                            : format_ == kFormatF16   ? CUDA_R_16F
                            : format_ == kFormatBF16  ? CUDA_R_16BF
                            : integer(format_)        ? CUDA_R_8I
                                                      : CUDA_R_32F;
        cudaDataType_t dt = integer(format_) ? CUDA_R_32I : CUDA_R_32F;
        layout la(et, K_, M), lb(et, K_, nq_pad_), lc(dt, M, nq_pad_);
        float alpha = 1.0f, beta = 0.0f;
        int32_t ialpha = 1, ibeta = 0;
        const void* pa = integer(format_) ? static_cast<const void*>(&ialpha) : &alpha;
        const void* pb = integer(format_) ? static_cast<const void*>(&ibeta) : &beta;
        check_lt(cublasLtMatmul(lt_, desc_, pa, d_a_, la.h, d_b_, lb.h, pb, d_d_, lc.h,
                                d_d_, lc.h, &algos_[b].second, d_work_, kWorkspace, stream_),
                 "cublasLtMatmul");
        return M;
    }

    // needs_stats reports whether the scores use the row and query statistics: the squared
    // norms of a distance metric, the element sums of uint8.
    bool needs_stats() const { return metric_ != kInnerProduct || format_ == kFormatU8; }

    // rescalable reports whether rows are rescaled before the matmul: cosine and l2sq on a
    // format without a bounded range.
    bool rescalable() const {
        return metric_ != kInnerProduct &&
               (format_ == kFormatMXFP8 || format_ == kFormatF32 || format_ == kFormatBF16);
    }

    // row_stats enqueues the squared norms and uint8 sums of n packed rows.
    void row_stats(const void* elem, const void* scale, const void* global, uint64_t n,
                   void* norm, void* sum) {
        if (!needs_stats() || n == 0) return;
        // cosine and l2sq rows of a format without a bounded range are rescaled in place
        const bool rescale = rescalable();
        auto* e = static_cast<uint8_t*>(const_cast<void*>(elem));
        auto* sc = static_cast<uint8_t*>(const_cast<void*>(scale));
        auto* g = static_cast<float*>(const_cast<void*>(global));
        // a thread per row up to 128 elements, a warp per row above; the grid loops over rows
        const int lanes = dim_ <= 128 ? 1 : 32;
        const uint64_t blocks =
            std::min<uint64_t>((n * lanes + 255) / 256, uint64_t(sm_count_) * 8);
        auto launch = [&](auto kernel) {
            kernel<<<unsigned(blocks), 256, 0, stream_>>>(e, sc, g, n, row_bytes_, dim_, Sp_,
                                                          static_cast<double*>(norm),
                                                          static_cast<int64_t*>(sum), rescale);
        };
        switch (format_ * 2 + (lanes == 1 ? 0 : 1)) {
#define BSMM_ROW_STATS(f)                                    \
    case (f) * 2:                                            \
        launch(bsmm_row_stats_kernel<1, (f)>);               \
        break;                                               \
    case (f) * 2 + 1:                                        \
        launch(bsmm_row_stats_kernel<32, (f)>);              \
        break;
            BSMM_ROW_STATS(kFormatMXFP8)
            BSMM_ROW_STATS(kFormatNVFP4)
            BSMM_ROW_STATS(kFormatF32)
            BSMM_ROW_STATS(kFormatF16)
            BSMM_ROW_STATS(kFormatI8)
            BSMM_ROW_STATS(kFormatU8)
            BSMM_ROW_STATS(kFormatBF16)
#undef BSMM_ROW_STATS
        }
        check(cudaGetLastError(), "bsmm_row_stats_kernel");
    }

    // fixup enqueues the rank scores of the matmul output of an M-row tile holding n rows.
    void fixup(size_t M, uint64_t n) {
        const int kind = format_ == kFormatU8 ? 2 : format_ == kFormatI8 ? 1 : 0;
        const int64_t base = format_ == kFormatU8 ? int64_t(128) * 128 * dim_ : 0;
        const uint64_t total = uint64_t(M) * nq_;
        const unsigned blocks = unsigned(std::min<uint64_t>((total + 255) / 256, 65535));
        bsmm_fixup_kernel<<<blocks, 256, 0, stream_>>>(
            static_cast<float*>(d_d_), kind, metric_, M, n, nq_,
            static_cast<const float*>(d_g_row_), static_cast<const float*>(d_g_query_),
            static_cast<const int64_t*>(d_sum_row_), static_cast<const int64_t*>(d_sum_query_),
            base, static_cast<const double*>(d_norm_row_),
            static_cast<const double*>(d_norm_query_));
        check(cudaGetLastError(), "bsmm_fixup_kernel");
    }

    void init(const uint8_t* query_cells) {
        h_a_.assign(max_rows_ * row_bytes_, 0);
        h_sa_.assign(max_rows_ * Sp_, 0);
        h_d_.resize(max_rows_ * nq_);
        g_row_.resize(max_rows_);
        g_query_.resize(nq_);
        std::vector<uint8_t> h_b(nq_pad_ * row_bytes_, 0), h_sb(nq_pad_ * Sp_, 0);
        pack_rows(query_cells, nq_, h_b.data(), h_sb.data(), g_query_.data());

        check(cudaSetDevice(device_id_), "cudaSetDevice");
        // every device buffer is a 256-byte aligned slice of one allocation
        struct slice {
            void** ptr;
            size_t bytes;
        };
        std::vector<slice> slices = {
            {&d_a_, max_rows_ * row_bytes_},
            {&d_sa_, max_rows_ * Sp_},
            {&d_b_, nq_pad_ * row_bytes_},
            {&d_sb_, nq_pad_ * Sp_},
            {&d_d_, max_rows_ * nq_pad_ * sizeof(float)},
            {&d_work_, kWorkspace},
            // per-row and per-query globals, uint8 sums and squared norms
            {&d_g_row_, max_rows_ * sizeof(float)},
            {&d_sum_row_, max_rows_ * sizeof(int64_t)},
            {&d_norm_row_, max_rows_ * sizeof(double)},
            {&d_g_query_, nq_ * sizeof(float)},
            {&d_sum_query_, nq_ * sizeof(int64_t)},
            {&d_norm_query_, nq_ * sizeof(double)},
        };
        if (topk_ != 0) {
            slices.insert(slices.end(), {{&d_top_val_, nq_ * topk_ * sizeof(float)},
                                         {&d_top_idx_, nq_ * topk_ * sizeof(int)},
                                         {&d_tied_, nq_}});
        }
        size_t device_bytes = 0;
        for (const slice& sl : slices) device_bytes += roundup(sl.bytes, 256);
        {
            // the claim covers the window until the buffers are resident
            auto claim = device_memory_governor::reserve_on(device_id_, device_bytes,
                                                            "blockscaled_matmul");
            d_arena_ = device_alloc(device_bytes);
            auto* next = static_cast<uint8_t*>(d_arena_);
            for (const slice& sl : slices) {
                *sl.ptr = next;
                next += roundup(sl.bytes, 256);
            }
        }
        check(cudaStreamCreateWithFlags(&stream_, cudaStreamNonBlocking), "cudaStreamCreate");
        check(cudaDeviceGetAttribute(&sm_count_, cudaDevAttrMultiProcessorCount, device_id_),
              "cudaDeviceGetAttribute");
        // on stream_, ahead of the kernels that read and rescale the queries
        check(cudaMemcpyAsync(d_b_, h_b.data(), h_b.size(), cudaMemcpyHostToDevice, stream_),
              "cudaMemcpyAsync");
        check(cudaMemcpyAsync(d_sb_, h_sb.data(), h_sb.size(), cudaMemcpyHostToDevice, stream_),
              "cudaMemcpyAsync");
        check(cudaMemcpyAsync(d_g_query_, g_query_.data(), nq_ * sizeof(float),
                              cudaMemcpyHostToDevice, stream_),
              "cudaMemcpyAsync");
        // the queries' squared norms and uint8 sums, from the packed query matrix
        row_stats(d_b_, d_sb_, d_g_query_, nq_, d_norm_query_, d_sum_query_);
        check(cudaStreamSynchronize(stream_), "cudaStreamSynchronize");
        if (topk_ != 0) {
            raft::resource::set_cuda_stream(res_, rmm::cuda_stream_view(stream_));
        }

        check_lt(cublasLtCreate(&lt_), "cublasLtCreate");
        check_lt(integer(format_)
                     ? cublasLtMatmulDescCreate(&desc_, CUBLAS_COMPUTE_32I, CUDA_R_32I)
                     : cublasLtMatmulDescCreate(&desc_, CUBLAS_COMPUTE_32F, CUDA_R_32F),
                 "cublasLtMatmulDescCreate");
        cublasOperation_t ta = CUBLAS_OP_T, tb = CUBLAS_OP_N;
        set_attr(CUBLASLT_MATMUL_DESC_TRANSA, &ta, sizeof(ta));
        set_attr(CUBLASLT_MATMUL_DESC_TRANSB, &tb, sizeof(tb));
        if (!plain(format_)) {
            cublasLtMatmulMatrixScale_t mode = format_ == kFormatMXFP8
                                                   ? CUBLASLT_MATMUL_MATRIX_SCALE_VEC32_UE8M0
                                                   : CUBLASLT_MATMUL_MATRIX_SCALE_VEC16_UE4M3;
            set_attr(CUBLASLT_MATMUL_DESC_A_SCALE_MODE, &mode, sizeof(mode));
            set_attr(CUBLASLT_MATMUL_DESC_B_SCALE_MODE, &mode, sizeof(mode));
            set_attr(CUBLASLT_MATMUL_DESC_A_SCALE_POINTER, &d_sa_, sizeof(d_sa_));
            set_attr(CUBLASLT_MATMUL_DESC_B_SCALE_POINTER, &d_sb_, sizeof(d_sb_));
        }
        size_t wsz = kWorkspace;
        check_lt(cublasLtMatmulPreferenceCreate(&pref_), "cublasLtMatmulPreferenceCreate");
        check_lt(cublasLtMatmulPreferenceSetAttribute(
                     pref_, CUBLASLT_MATMUL_PREF_MAX_WORKSPACE_BYTES, &wsz, sizeof(wsz)),
                 "cublasLtMatmulPreferenceSetAttribute");

        // a tile of n rows runs as the smallest bucket 128 * 2^i (or max_rows) holding n;
        // every bucket needs a cuBLASLt algorithm, found here so a shape the device cannot
        // run fails at construction
        cudaDataType_t et = format_ == kFormatMXFP8   ? CUDA_R_8F_E4M3
                            : format_ == kFormatNVFP4 ? CUDA_R_4F_E2M1
                            : format_ == kFormatF16   ? CUDA_R_16F
                            : format_ == kFormatBF16  ? CUDA_R_16BF
                            : integer(format_)        ? CUDA_R_8I
                                                      : CUDA_R_32F;
        cudaDataType_t dt = integer(format_) ? CUDA_R_32I : CUDA_R_32F;
        for (size_t M = 128;; M = std::min(M * 2, max_rows_)) {
            layout la(et, K_, M), lb(et, K_, nq_pad_), lc(dt, M, nq_pad_);
            cublasLtMatmulHeuristicResult_t heur{};
            int nres = 0;
            check_lt(cublasLtMatmulAlgoGetHeuristic(lt_, desc_, la.h, lb.h, lc.h, lc.h, pref_, 1,
                                                    &heur, &nres),
                     "cublasLtMatmulAlgoGetHeuristic");
            if (nres == 0) {
                throw std::runtime_error("blockscaled_matmul: no cuBLASLt algorithm for " +
                                        std::to_string(M) + " x " + std::to_string(K_) +
                                        " x " + std::to_string(nq_pad_));
            }
            algos_.emplace_back(M, heur.algo);
            if (M == max_rows_) break;
        }
    }

    void cleanup() {
        cudaSetDevice(device_id_);
        if (pref_ != nullptr) cublasLtMatmulPreferenceDestroy(pref_);
        if (desc_ != nullptr) cublasLtMatmulDescDestroy(desc_);
        if (lt_ != nullptr) cublasLtDestroy(lt_);
        if (stream_ != nullptr) cudaStreamDestroy(stream_);
        if (d_arena_ != nullptr) cudaFree(d_arena_);
        d_arena_ = nullptr;
        pref_ = nullptr;
        desc_ = nullptr;
        lt_ = nullptr;
        stream_ = nullptr;
        d_a_ = d_sa_ = d_b_ = d_sb_ = d_d_ = d_work_ = nullptr;
        d_g_row_ = d_sum_row_ = d_norm_row_ = d_g_query_ = d_sum_query_ = d_norm_query_ =
            d_top_val_ = d_top_idx_ = d_tied_ = nullptr;
    }

    struct layout {
        cublasLtMatrixLayout_t h = nullptr;
        layout(cudaDataType_t t, uint64_t rows, uint64_t cols) {
            check_lt(cublasLtMatrixLayoutCreate(&h, t, rows, cols, rows),
                     "cublasLtMatrixLayoutCreate");
        }
        ~layout() {
            if (h != nullptr) cublasLtMatrixLayoutDestroy(h);
        }
    };

    static size_t roundup(size_t v, size_t m) { return (v + m - 1) / m * m; }

    static bool plain(int format) { return format >= kFormatF32; }
    static bool integer(int format) { return format == kFormatI8 || format == kFormatU8; }

    static void check(cudaError_t e, const char* what) {
        if (e != cudaSuccess) {
            cudaGetLastError();
            throw std::runtime_error(std::string(what) + ": " + cudaGetErrorString(e));
        }
    }

    static void check_lt(cublasStatus_t s, const char* what) {
        if (s != CUBLAS_STATUS_SUCCESS) {
            throw std::runtime_error(std::string(what) + ": cuBLASLt status " +
                                     std::to_string(int(s)));
        }
    }

    void set_attr(cublasLtMatmulDescAttributes_t attr, const void* v, size_t sz) {
        check_lt(cublasLtMatmulDescSetAttribute(desc_, attr, v, sz),
                 "cublasLtMatmulDescSetAttribute");
    }

    static void* device_alloc(size_t bytes) {
        void* p = nullptr;
        if (bytes == 0) return nullptr;
        check(cudaMalloc(&p, bytes), "cudaMalloc");
        return p;
    }

    size_t scale_off(size_t r, size_t s) const {
        size_t tile = (r / 128) * (Sp_ / 4) + s / 4;
        size_t rr = r % 128;
        return tile * 512 + (rr % 32) * 16 + (rr / 32) * 4 + s % 4;
    }

    // pack_rows writes n cells as padded element rows and tiled scales into zeroed buffers.
    void pack_rows(const uint8_t* cells, size_t n, uint8_t* elem, uint8_t* scale,
                   float* global) const {
        if (format_ == kFormatU8) {
            // x' = x - 128 as int8 is x ^ 0x80; padding stays 0 (x' = 0)
            for (size_t r = 0; r < n; r++) {
                global[r] = 1.0f;
                const uint8_t* src = cells + r * cell_bytes_;
                uint8_t* dst = elem + r * row_bytes_;
                for (size_t k = 0; k < elem_bytes_; k++) dst[k] = src[k] ^ 0x80;
            }
            return;
        }
        if (plain(format_)) {
            for (size_t r = 0; r < n; r++) {
                global[r] = 1.0f;
                std::memcpy(elem + r * row_bytes_, cells + r * cell_bytes_, elem_bytes_);
            }
            return;
        }
        for (size_t r = 0; r < n; r++) {
            const uint8_t* cell = cells + r * cell_bytes_;
            const uint8_t* sc = cell + kHeader;
            std::memcpy(&global[r], cell + 8, sizeof(float));
            for (size_t s = 0; s < nscale_; s++) {
                scale[scale_off(r, s)] = sc[s];
            }
            std::memcpy(elem + r * row_bytes_, sc + nscale_, elem_bytes_);
        }
    }

    int device_id_;
    int sm_count_ = 1;
    int format_ = 0;
    int metric_ = kInnerProduct;
    size_t dim_ = 0, block_ = 0, nscale_ = 0, elem_bytes_ = 0, cell_bytes_ = 0;
    size_t K_ = 0, Sp_ = 0, row_bytes_ = 0;
    size_t nq_ = 0, nq_pad_ = 0, max_rows_ = 0, topk_ = 0;
    cudaStream_t stream_ = nullptr;
    cublasLtHandle_t lt_ = nullptr;
    cublasLtMatmulDesc_t desc_ = nullptr;
    cublasLtMatmulPreference_t pref_ = nullptr;
    void *d_a_ = nullptr, *d_sa_ = nullptr, *d_b_ = nullptr, *d_sb_ = nullptr, *d_d_ = nullptr,
         *d_work_ = nullptr;
    void *d_g_row_ = nullptr, *d_sum_row_ = nullptr, *d_norm_row_ = nullptr,
         *d_g_query_ = nullptr, *d_sum_query_ = nullptr, *d_norm_query_ = nullptr,
         *d_top_val_ = nullptr, *d_top_idx_ = nullptr, *d_tied_ = nullptr;
    // the one device allocation the buffers below are slices of
    void* d_arena_ = nullptr;
    raft::resources res_;
    // row bucket -> cuBLASLt algorithm, ascending
    std::vector<std::pair<size_t, cublasLtMatmulAlgo_t>> algos_;
    std::vector<uint8_t> h_a_, h_sa_;
    std::vector<float> h_d_, g_row_, g_query_;
};

} // namespace matrixone
