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
// run applies the per-vector global scales on the host when the scores are copied out:
// scores[r * nq + q] = g_r * g_q * D[r, q]. run_topk applies them on the device, selects
// the k best rows per query with cuvs::selection::select_k and copies only those back.

#include "device_memory.hpp"

#include <cub/block/block_reduce.cuh>
#include <cublasLt.h>
#include <cuda_runtime.h>
#include <cuvs/selection/select_k.hpp>
#include <raft/core/device_mdspan.hpp>
#include <raft/core/resource/cuda_stream.hpp>
#include <raft/core/resources.hpp>

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <string>
#include <vector>

namespace matrixone {

namespace {

// bsmm_fixup_kernel turns the raw matmul output d (query major, M rows per query) into
// final scores in place: global scales in double (float formats), int32 sums with the
// uint8 shift correction (integer formats), NaN and padding rows r >= n as -Inf.
__global__ void bsmm_fixup_kernel(float* d, int kind, uint64_t M, uint64_t n, uint64_t nq,
                                  const float* g_row, const float* g_query,
                                  const int64_t* sum_row, const int64_t* sum_query,
                                  int64_t base) {
    const uint64_t total = M * nq;
    for (uint64_t i = uint64_t(blockIdx.x) * blockDim.x + threadIdx.x; i < total;
         i += uint64_t(gridDim.x) * blockDim.x) {
        const uint64_t q = i / M, r = i % M;
        float v;
        if (r >= n) {
            v = -INFINITY;
        } else if (kind == 0) {
            v = float(double(d[i]) * double(g_row[r]) * double(g_query[q]));
        } else {
            int64_t dot = int64_t(__float_as_int(d[i]));
            if (kind == 2) dot += 128 * (sum_row[r] + sum_query[q]) + base;
            v = float(dot);
        }
        if (isnan(v)) v = -INFINITY;
        d[i] = v;
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

    // topk > 0 sizes the buffers of run_topk; 0 allows run only.
    blockscaled_matmul(int device_id, int format, uint32_t dim, uint32_t nq,
                       const uint8_t* query_cells, uint64_t max_rows, uint32_t topk = 0)
        : device_id_(device_id) {
        if (format < kFormatMXFP8 || format > kFormatBF16 || dim == 0 || nq == 0 ||
            max_rows == 0) {
            throw std::invalid_argument(
                "blockscaled_matmul: invalid format, dimension, query count or tile size");
        }
        format_ = format;
        dim_ = dim;
        K_ = roundup(dim, 32);
        if (plain(format)) {
            const size_t esize = format == kFormatF32                           ? 4
                                 : format == kFormatF16 || format == kFormatBF16 ? 2
                                                                                 : 1;
            elem_bytes_ = size_t(dim) * esize;
            cell_bytes_ = elem_bytes_;
            row_bytes_ = K_ * esize;
        } else {
            block_ = format == kFormatMXFP8 ? 32 : 16;
            nscale_ = (dim + block_ - 1) / block_;
            elem_bytes_ = format == kFormatMXFP8 ? dim : (dim + 1) / 2;
            cell_bytes_ = kHeader + nscale_ + elem_bytes_;
            Sp_ = roundup(K_ / block_, 4);
            row_bytes_ = format == kFormatMXFP8 ? K_ : K_ / 2;
        }
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

    // run scores n cells packed back to back (n <= max_rows) into n * nq row-major scores.
    void run(const uint8_t* cells, uint64_t n, float* scores) {
        if (n == 0) return;
        const size_t M = matmul(cells, n);
        // D is column major M x nq_pad; the first nq columns hold the queries
        check(cudaMemcpy2DAsync(h_d_.data(), n * sizeof(float), d_d_, M * sizeof(float),
                                n * sizeof(float), nq_, cudaMemcpyDeviceToHost, stream_),
              "cudaMemcpy2DAsync");
        check(cudaStreamSynchronize(stream_), "cudaStreamSynchronize");

        // h_d is query major (nq x n); write row major with the global scales applied in
        // double, rounded once
        if (integer(format_)) {
            // h_d holds int32 sums; uint8 adds the shift correction
            const int64_t base = format_ == kFormatU8 ? int64_t(128) * 128 * dim_ : 0;
            for (size_t r = 0; r < n; r++) {
                for (size_t q = 0; q < nq_; q++) {
                    int32_t v;
                    std::memcpy(&v, &h_d_[q * n + r], sizeof(v));
                    int64_t dot = int64_t(v);
                    if (format_ == kFormatU8) dot += 128 * (sum_row_[r] + sum_query_[q]) + base;
                    scores[r * nq_ + q] = float(dot);
                }
            }
            return;
        }
        for (size_t r = 0; r < n; r++) {
            for (size_t q = 0; q < nq_; q++) {
                scores[r * nq_ + q] =
                    float(double(h_d_[q * n + r]) * double(g_row_[r]) * double(g_query_[q]));
            }
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
        check(cudaMemcpyAsync(d_g_row_, g_row_.data(), n * sizeof(float), cudaMemcpyHostToDevice,
                              stream_),
              "cudaMemcpyAsync");
        if (format_ == kFormatU8) {
            check(cudaMemcpyAsync(d_sum_row_, sum_row_.data(), n * sizeof(int64_t),
                                  cudaMemcpyHostToDevice, stream_),
                  "cudaMemcpyAsync");
        }
        const int kind = format_ == kFormatU8 ? 2 : format_ == kFormatI8 ? 1 : 0;
        const int64_t base = format_ == kFormatU8 ? int64_t(128) * 128 * dim_ : 0;
        const uint64_t total = uint64_t(M) * nq_;
        const unsigned blocks = unsigned(std::min<uint64_t>((total + 255) / 256, 65535));
        bsmm_fixup_kernel<<<blocks, 256, 0, stream_>>>(
            d, kind, M, n, nq_, static_cast<const float*>(d_g_row_),
            static_cast<const float*>(d_g_query_), static_cast<const int64_t*>(d_sum_row_),
            static_cast<const int64_t*>(d_sum_query_), base);
        check(cudaGetLastError(), "bsmm_fixup_kernel");

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
    // matmul packs and uploads n cells and enqueues D = A^T B on the stream; it returns the
    // padded row count M.
    size_t matmul(const uint8_t* cells, uint64_t n) {
        if (n > max_rows_) {
            throw std::invalid_argument("blockscaled_matmul: tile exceeds the row capacity");
        }
        const size_t M = roundup(n, 128);
        std::fill(h_a_.begin(), h_a_.begin() + M * row_bytes_, 0);
        std::fill(h_sa_.begin(), h_sa_.begin() + M * Sp_, 0);
        pack_rows(cells, n, h_a_.data(), h_sa_.data(), g_row_.data(), sum_row_.data());

        check(cudaSetDevice(device_id_), "cudaSetDevice");
        check(cudaMemcpyAsync(d_a_, h_a_.data(), M * row_bytes_, cudaMemcpyHostToDevice, stream_),
              "cudaMemcpyAsync");
        check(cudaMemcpyAsync(d_sa_, h_sa_.data(), M * Sp_, cudaMemcpyHostToDevice, stream_),
              "cudaMemcpyAsync");

        cudaDataType_t et = format_ == kFormatMXFP8   ? CUDA_R_8F_E4M3
                            : format_ == kFormatNVFP4 ? CUDA_R_4F_E2M1
                            : format_ == kFormatF16   ? CUDA_R_16F
                            : format_ == kFormatBF16  ? CUDA_R_16BF
                            : integer(format_)        ? CUDA_R_8I
                                                      : CUDA_R_32F;
        cudaDataType_t dt = integer(format_) ? CUDA_R_32I : CUDA_R_32F;
        layout la(et, K_, M), lb(et, K_, nq_pad_), lc(dt, M, nq_pad_);
        cublasLtMatmulHeuristicResult_t heur{};
        int nres = 0;
        check_lt(cublasLtMatmulAlgoGetHeuristic(lt_, desc_, la.h, lb.h, lc.h, lc.h, pref_, 1,
                                                &heur, &nres),
                 "cublasLtMatmulAlgoGetHeuristic");
        if (nres == 0) {
            throw std::runtime_error("blockscaled_matmul: no cuBLASLt algorithm for this shape");
        }
        float alpha = 1.0f, beta = 0.0f;
        int32_t ialpha = 1, ibeta = 0;
        const void* pa = integer(format_) ? static_cast<const void*>(&ialpha) : &alpha;
        const void* pb = integer(format_) ? static_cast<const void*>(&ibeta) : &beta;
        check_lt(cublasLtMatmul(lt_, desc_, pa, d_a_, la.h, d_b_, lb.h, pb, d_d_, lc.h,
                                d_d_, lc.h, &heur.algo, d_work_, kWorkspace, stream_),
                 "cublasLtMatmul");
        return M;
    }

    void init(const uint8_t* query_cells) {
        h_a_.assign(max_rows_ * row_bytes_, 0);
        h_sa_.assign(max_rows_ * Sp_, 0);
        h_d_.resize(max_rows_ * nq_);
        g_row_.resize(max_rows_);
        g_query_.resize(nq_);
        sum_row_.resize(max_rows_);
        sum_query_.resize(nq_);
        std::vector<uint8_t> h_b(nq_pad_ * row_bytes_, 0), h_sb(nq_pad_ * Sp_, 0);
        pack_rows(query_cells, nq_, h_b.data(), h_sb.data(), g_query_.data(), sum_query_.data());

        check(cudaSetDevice(device_id_), "cudaSetDevice");
        const size_t topk_bytes = topk_ == 0 ? 0
                                             : max_rows_ * (sizeof(float) + sizeof(int64_t)) +
                                                   nq_ * (sizeof(float) + sizeof(int64_t)) +
                                                   nq_ * topk_ * (sizeof(float) + sizeof(int)) +
                                                   nq_;
        const size_t device_bytes = max_rows_ * row_bytes_ + max_rows_ * Sp_ +
                                    nq_pad_ * row_bytes_ + nq_pad_ * Sp_ +
                                    max_rows_ * nq_pad_ * sizeof(float) + kWorkspace + topk_bytes;
        {
            // the claim covers the window until the buffers are resident
            auto claim = device_memory_governor::reserve_on(device_id_, device_bytes,
                                                            "blockscaled_matmul");
            d_a_ = device_alloc(max_rows_ * row_bytes_);
            d_sa_ = device_alloc(max_rows_ * Sp_);
            d_b_ = device_alloc(nq_pad_ * row_bytes_);
            d_sb_ = device_alloc(nq_pad_ * Sp_);
            d_d_ = device_alloc(max_rows_ * nq_pad_ * sizeof(float));
            d_work_ = device_alloc(kWorkspace);
            if (topk_ != 0) {
                d_g_row_ = device_alloc(max_rows_ * sizeof(float));
                d_sum_row_ = device_alloc(max_rows_ * sizeof(int64_t));
                d_g_query_ = device_alloc(nq_ * sizeof(float));
                d_sum_query_ = device_alloc(nq_ * sizeof(int64_t));
                d_top_val_ = device_alloc(nq_ * topk_ * sizeof(float));
                d_top_idx_ = device_alloc(nq_ * topk_ * sizeof(int));
                d_tied_ = device_alloc(nq_);
            }
        }
        check(cudaStreamCreateWithFlags(&stream_, cudaStreamNonBlocking), "cudaStreamCreate");
        check(cudaMemcpy(d_b_, h_b.data(), h_b.size(), cudaMemcpyHostToDevice), "cudaMemcpy");
        check(cudaMemcpy(d_sb_, h_sb.data(), h_sb.size(), cudaMemcpyHostToDevice), "cudaMemcpy");
        if (topk_ != 0) {
            check(cudaMemcpy(d_g_query_, g_query_.data(), nq_ * sizeof(float),
                             cudaMemcpyHostToDevice),
                  "cudaMemcpy");
            check(cudaMemcpy(d_sum_query_, sum_query_.data(), nq_ * sizeof(int64_t),
                             cudaMemcpyHostToDevice),
                  "cudaMemcpy");
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
    }

    void cleanup() {
        cudaSetDevice(device_id_);
        if (pref_ != nullptr) cublasLtMatmulPreferenceDestroy(pref_);
        if (desc_ != nullptr) cublasLtMatmulDescDestroy(desc_);
        if (lt_ != nullptr) cublasLtDestroy(lt_);
        if (stream_ != nullptr) cudaStreamDestroy(stream_);
        for (void* p : {d_a_, d_sa_, d_b_, d_sb_, d_d_, d_work_, d_g_row_, d_sum_row_, d_g_query_,
                        d_sum_query_, d_top_val_, d_top_idx_, d_tied_}) {
            if (p != nullptr) cudaFree(p);
        }
        pref_ = nullptr;
        desc_ = nullptr;
        lt_ = nullptr;
        stream_ = nullptr;
        d_a_ = d_sa_ = d_b_ = d_sb_ = d_d_ = d_work_ = nullptr;
        d_g_row_ = d_sum_row_ = d_g_query_ = d_sum_query_ = d_top_val_ = d_top_idx_ = d_tied_ =
            nullptr;
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
                   float* global, int64_t* sum) const {
        if (format_ == kFormatU8) {
            // x' = x - 128 as int8 is x ^ 0x80; padding stays 0 (x' = 0)
            for (size_t r = 0; r < n; r++) {
                global[r] = 1.0f;
                const uint8_t* src = cells + r * cell_bytes_;
                uint8_t* dst = elem + r * row_bytes_;
                int64_t acc = 0;
                for (size_t k = 0; k < elem_bytes_; k++) {
                    dst[k] = src[k] ^ 0x80;
                    acc += int64_t(src[k]) - 128;
                }
                sum[r] = acc;
            }
            return;
        }
        if (plain(format_)) {
            for (size_t r = 0; r < n; r++) {
                global[r] = 1.0f;
                sum[r] = 0;
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
    int format_ = 0;
    size_t dim_ = 0, block_ = 0, nscale_ = 0, elem_bytes_ = 0, cell_bytes_ = 0;
    size_t K_ = 0, Sp_ = 0, row_bytes_ = 0;
    size_t nq_ = 0, nq_pad_ = 0, max_rows_ = 0, topk_ = 0;
    cudaStream_t stream_ = nullptr;
    cublasLtHandle_t lt_ = nullptr;
    cublasLtMatmulDesc_t desc_ = nullptr;
    cublasLtMatmulPreference_t pref_ = nullptr;
    void *d_a_ = nullptr, *d_sa_ = nullptr, *d_b_ = nullptr, *d_sb_ = nullptr, *d_d_ = nullptr,
         *d_work_ = nullptr;
    void *d_g_row_ = nullptr, *d_sum_row_ = nullptr, *d_g_query_ = nullptr,
         *d_sum_query_ = nullptr, *d_top_val_ = nullptr, *d_top_idx_ = nullptr,
         *d_tied_ = nullptr;
    raft::resources res_;
    std::vector<uint8_t> h_a_, h_sa_;
    std::vector<float> h_d_, g_row_, g_query_;
    std::vector<int64_t> sum_row_, sum_query_;
};

} // namespace matrixone
