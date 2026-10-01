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
// and a plain matmul for vecf32 and vecf16 rows (CUBLAS_COMPUTE_32F: fp32 accumulation,
// no TF32).
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
// The per-vector global scales are applied on the host when the scores are copied out:
// scores[r * nq + q] = g_r * g_q * D[r, q].

#include "device_memory.hpp"

#include <cublasLt.h>
#include <cuda_runtime.h>

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <string>
#include <vector>

namespace matrixone {

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

    blockscaled_matmul(int device_id, int format, uint32_t dim, uint32_t nq,
                       const uint8_t* query_cells, uint64_t max_rows)
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

private:
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
        const size_t device_bytes = max_rows_ * row_bytes_ + max_rows_ * Sp_ +
                                    nq_pad_ * row_bytes_ + nq_pad_ * Sp_ +
                                    max_rows_ * nq_pad_ * sizeof(float) + kWorkspace;
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
        }
        check(cudaStreamCreateWithFlags(&stream_, cudaStreamNonBlocking), "cudaStreamCreate");
        check(cudaMemcpy(d_b_, h_b.data(), h_b.size(), cudaMemcpyHostToDevice), "cudaMemcpy");
        check(cudaMemcpy(d_sb_, h_sb.data(), h_sb.size(), cudaMemcpyHostToDevice), "cudaMemcpy");

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
        for (void* p : {d_a_, d_sa_, d_b_, d_sb_, d_d_, d_work_}) {
            if (p != nullptr) cudaFree(p);
        }
        pref_ = nullptr;
        desc_ = nullptr;
        lt_ = nullptr;
        stream_ = nullptr;
        d_a_ = d_sa_ = d_b_ = d_sb_ = d_d_ = d_work_ = nullptr;
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
    size_t nq_ = 0, nq_pad_ = 0, max_rows_ = 0;
    cudaStream_t stream_ = nullptr;
    cublasLtHandle_t lt_ = nullptr;
    cublasLtMatmulDesc_t desc_ = nullptr;
    cublasLtMatmulPreference_t pref_ = nullptr;
    void *d_a_ = nullptr, *d_sa_ = nullptr, *d_b_ = nullptr, *d_sb_ = nullptr, *d_d_ = nullptr,
         *d_work_ = nullptr;
    std::vector<uint8_t> h_a_, h_sa_;
    std::vector<float> h_d_, g_row_, g_query_;
    std::vector<int64_t> sum_row_, sum_query_;
};

} // namespace matrixone
