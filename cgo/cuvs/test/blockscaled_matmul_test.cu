/*
 * Copyright 2026 Matrix Origin
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

#include "blockscaled_matmul.hpp"
#include "blockscaled_matmul_c.h"
#include "test_framework.hpp"

#include <cublasLt.h>
#include <cuda_bf16.h>
#include <cuda_fp16.h>
#include <cuda_fp4.h>
#include <cuda_fp8.h>

#include <algorithm>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <random>
#include <vector>

using namespace matrixone;

namespace {

double e4m3(uint8_t c) {
    __nv_fp8_e4m3 v;
    v.__x = c;
    return double(float(v));
}

double e2m1(uint8_t c) {
    __nv_fp4_e2m1 v;
    v.__x = c & 0xf;
    return double(float(v));
}

// make_cells builds n random cells in the vecblock.go layout and their dequantized values.
std::vector<uint8_t> make_cells(int format, uint32_t dim, size_t n, std::mt19937& rng,
                                std::vector<double>& values) {
    const size_t block = format == GPU_BLOCKSCALED_MXFP8 ? 32 : 16;
    const size_t nscale = (dim + block - 1) / block;
    const size_t elem_bytes = format == GPU_BLOCKSCALED_MXFP8 ? dim : (dim + 1) / 2;
    const size_t cell_bytes = 12 + nscale + elem_bytes;
    std::vector<uint8_t> cells(n * cell_bytes, 0);
    values.assign(n * dim, 0);
    for (size_t r = 0; r < n; r++) {
        uint8_t* cell = cells.data() + r * cell_bytes;
        cell[0] = 1;
        cell[1] = uint8_t(format);
        std::memcpy(cell + 4, &dim, 4);
        float g = format == GPU_BLOCKSCALED_MXFP8 ? 1.0f : float(0.25 + (rng() % 1000) / 500.0);
        std::memcpy(cell + 8, &g, 4);
        uint8_t* sc = cell + 12;
        uint8_t* el = sc + nscale;
        for (size_t s = 0; s < nscale; s++) {
            sc[s] = format == GPU_BLOCKSCALED_MXFP8 ? uint8_t(127 - 4 + rng() % 9)
                                                    : uint8_t(0x30 + rng() % 0x19);
        }
        for (uint32_t k = 0; k < dim; k++) {
            double scale;
            double v;
            if (format == GPU_BLOCKSCALED_MXFP8) {
                uint8_t c;
                do {
                    c = uint8_t(rng());
                } while ((c & 0x7f) == 0x7f);
                el[k] = c;
                v = e4m3(c);
                scale = std::ldexp(1.0, int(sc[k / 32]) - 127);
            } else {
                uint8_t c = uint8_t(rng() & 0xf);
                el[k / 2] |= k % 2 == 0 ? c : uint8_t(c << 4);
                v = e2m1(c);
                scale = e4m3(sc[k / 16]);
            }
            values[r * dim + k] = double(g) * scale * v;
        }
    }
    return cells;
}

void check_engine(int format, uint32_t dim, size_t rows, uint32_t nq, uint64_t max_rows) {
    std::mt19937 rng(dim * 131 + rows * 7 + nq);
    std::vector<double> qv, rv;
    std::vector<uint8_t> queries = make_cells(format, dim, nq, rng, qv);
    std::vector<uint8_t> cells = make_cells(format, dim, rows, rng, rv);
    const size_t cell_bytes = cells.size() / rows;

    blockscaled_matmul e(0, format, dim, nq, queries.data(), max_rows);
    ASSERT_EQ(e.max_rows() % 128, uint64_t(0));
    std::vector<float> scores(rows * nq);
    // tiles of at most max_rows, so a tile is reused across calls
    for (size_t off = 0; off < rows; off += e.max_rows()) {
        size_t n = std::min<size_t>(e.max_rows(), rows - off);
        e.run(cells.data() + off * cell_bytes, n, scores.data() + off * nq);
    }
    for (size_t r = 0; r < rows; r++) {
        for (uint32_t q = 0; q < nq; q++) {
            double ref = 0, mag = 0;
            for (uint32_t k = 0; k < dim; k++) {
                ref += rv[r * dim + k] * qv[q * dim + k];
                mag += std::fabs(rv[r * dim + k] * qv[q * dim + k]);
            }
            double got = scores[r * nq + q];
            ASSERT_TRUE(std::fabs(got - ref) <= 1e-5 * std::max(1.0, mag));
        }
    }
}

// check_plain scores raw vectors of a plain format against a double-precision reference.
void check_plain(int format, uint32_t dim, size_t rows, uint32_t nq, uint64_t max_rows) {
    std::mt19937 rng(format * 977 + dim * 131 + rows * 7 + nq);
    const size_t esize = format == GPU_BLOCKSCALED_F32 ? 4
                         : format == GPU_BLOCKSCALED_F16 || format == GPU_BLOCKSCALED_BF16 ? 2
                                                                                          : 1;
    auto make = [&](size_t n, std::vector<double>& vals) {
        std::vector<uint8_t> cells(n * dim * esize);
        vals.resize(n * dim);
        std::normal_distribution<float> nd(0.0f, 1.0f);
        for (size_t i = 0; i < n * dim; i++) {
            if (format == GPU_BLOCKSCALED_F32) {
                float v = nd(rng);
                std::memcpy(&cells[i * 4], &v, 4);
                vals[i] = v;
            } else if (format == GPU_BLOCKSCALED_F16) {
                __half h = __float2half(nd(rng));
                std::memcpy(&cells[i * 2], &h, 2);
                vals[i] = double(__half2float(h));
            } else if (format == GPU_BLOCKSCALED_BF16) {
                __nv_bfloat16 h = __float2bfloat16(nd(rng));
                std::memcpy(&cells[i * 2], &h, 2);
                vals[i] = double(__bfloat162float(h));
            } else if (format == GPU_BLOCKSCALED_I8) {
                int8_t v = int8_t(int(rng() % 256) - 128);
                cells[i] = uint8_t(v);
                vals[i] = v;
            } else {
                uint8_t v = uint8_t(rng() % 256);
                cells[i] = v;
                vals[i] = v;
            }
        }
        return cells;
    };
    std::vector<double> qv, rv;
    std::vector<uint8_t> queries = make(nq, qv), cells = make(rows, rv);
    const size_t cell_bytes = dim * esize;
    blockscaled_matmul e(0, format, dim, nq, queries.data(), max_rows);
    std::vector<float> scores(rows * nq);
    for (size_t off = 0; off < rows; off += e.max_rows()) {
        size_t n = std::min<size_t>(e.max_rows(), rows - off);
        e.run(cells.data() + off * cell_bytes, n, scores.data() + off * nq);
    }
    const bool exact = format == GPU_BLOCKSCALED_I8 || format == GPU_BLOCKSCALED_U8;
    for (size_t r = 0; r < rows; r++) {
        for (uint32_t q = 0; q < nq; q++) {
            double ref = 0, mag = 0;
            for (uint32_t k = 0; k < dim; k++) {
                ref += rv[r * dim + k] * qv[q * dim + k];
                mag += std::fabs(rv[r * dim + k] * qv[q * dim + k]);
            }
            double got = scores[r * nq + q];
            if (exact) {
                ASSERT_TRUE(got == ref);
            } else {
                ASSERT_TRUE(std::fabs(got - ref) <= 1e-5 * std::max(1.0, mag));
            }
        }
    }
}

// check_topk compares run_topk with the full scores of run over tiles of rows cells: every
// kept row carries its exact score, every row above the k-th score is kept, and a query
// whose rows tied at the k-th score were not all kept is flagged with its full scores.
void check_topk(int format, uint32_t dim, const std::vector<uint8_t>& queries,
                const std::vector<uint8_t>& cells, size_t rows, uint32_t nq, uint32_t k,
                uint64_t max_rows, bool expect_tied,
                int metric = blockscaled_matmul::kInnerProduct) {
    const size_t cell_bytes = cells.size() / rows;
    blockscaled_matmul e(0, format, dim, nq, queries.data(), max_rows, k, metric);
    const size_t kk = std::min<uint64_t>(k, e.max_rows());
    bool saw_tied = false;
    for (size_t off = 0; off < rows; off += e.max_rows()) {
        const size_t n = std::min<size_t>(e.max_rows(), rows - off);
        const uint8_t* tile = cells.data() + off * cell_bytes;
        std::vector<float> scores(n * nq), top(nq * kk), full(nq * n, -1.0f);
        std::vector<int32_t> top_rows(nq * kk);
        std::vector<uint8_t> tied(nq);
        e.run(tile, n, scores.data());
        e.run_topk(tile, n, top.data(), top_rows.data(), full.data(), tied.data());
        for (uint32_t q = 0; q < nq; q++) {
            std::vector<float> col(n);
            for (size_t r = 0; r < n; r++) {
                col[r] = std::isnan(scores[r * nq + q]) ? -INFINITY : scores[r * nq + q];
            }
            std::vector<float> sorted = col;
            std::sort(sorted.begin(), sorted.end(), std::greater<float>());
            const size_t want = std::min(kk, n);
            const float kth = sorted[want - 1];
            std::vector<bool> kept(n, false);
            size_t got = 0, kept_at_kth = 0;
            for (size_t j = 0; j < kk; j++) {
                const int32_t r = top_rows[q * kk + j];
                if (r < 0) continue;
                ASSERT_TRUE(size_t(r) < n);
                ASSERT_TRUE(!kept[r]);
                kept[r] = true;
                got++;
                ASSERT_TRUE(top[q * kk + j] == col[r]);
                if (col[r] == kth) kept_at_kth++;
            }
            ASSERT_EQ(got, want);
            size_t all_at_kth = 0;
            for (size_t r = 0; r < n; r++) {
                if (col[r] > kth) ASSERT_TRUE(kept[r]);
                if (col[r] == kth) all_at_kth++;
            }
            ASSERT_EQ(tied[q] != 0, all_at_kth > kept_at_kth);
            if (tied[q] != 0) {
                saw_tied = true;
                for (size_t r = 0; r < n; r++) ASSERT_TRUE(full[q * n + r] == col[r]);
            }
        }
    }
    ASSERT_EQ(saw_tied, expect_tied);
}

// small_int_cells builds n raw int8 or uint8 vectors with values in {0, 1, 2}, so scores tie.
std::vector<uint8_t> small_int_cells(uint32_t dim, size_t n, std::mt19937& rng) {
    std::vector<uint8_t> cells(n * dim);
    for (auto& c : cells) c = uint8_t(rng() % 3);
    return cells;
}

// make_plain builds n random raw vectors of a plain format and their values.
std::vector<uint8_t> make_plain(int format, uint32_t dim, size_t n, std::mt19937& rng,
                                std::vector<double>& vals) {
    const size_t esize = format == GPU_BLOCKSCALED_F32 ? 4
                         : format == GPU_BLOCKSCALED_F16 || format == GPU_BLOCKSCALED_BF16 ? 2
                                                                                          : 1;
    std::vector<uint8_t> cells(n * dim * esize);
    vals.resize(n * dim);
    std::normal_distribution<float> nd(0.0f, 1.0f);
    for (size_t i = 0; i < n * dim; i++) {
        if (format == GPU_BLOCKSCALED_F32) {
            float v = nd(rng);
            std::memcpy(&cells[i * 4], &v, 4);
            vals[i] = v;
        } else if (format == GPU_BLOCKSCALED_F16) {
            __half h = __float2half(nd(rng));
            std::memcpy(&cells[i * 2], &h, 2);
            vals[i] = double(__half2float(h));
        } else if (format == GPU_BLOCKSCALED_BF16) {
            __nv_bfloat16 h = __float2bfloat16(nd(rng));
            std::memcpy(&cells[i * 2], &h, 2);
            vals[i] = double(__bfloat162float(h));
        } else if (format == GPU_BLOCKSCALED_I8) {
            int8_t v = int8_t(int(rng() % 256) - 128);
            cells[i] = uint8_t(v);
            vals[i] = v;
        } else {
            uint8_t v = uint8_t(rng() % 256);
            cells[i] = v;
            vals[i] = v;
        }
    }
    return cells;
}

// zero_vector makes cell r of a format all-zero elements (a block-scaled cell keeps its header
// and scales) and its values zero.
void zero_vector(int format, uint32_t dim, std::vector<uint8_t>& cells, size_t cell_bytes,
                 size_t r, std::vector<double>& vals) {
    size_t from = 0;
    if (format == GPU_BLOCKSCALED_MXFP8 || format == GPU_BLOCKSCALED_NVFP4) {
        const size_t block = format == GPU_BLOCKSCALED_MXFP8 ? 32 : 16;
        from = 12 + (dim + block - 1) / block;
    }
    std::fill(cells.begin() + r * cell_bytes + from, cells.begin() + (r + 1) * cell_bytes, 0);
    std::fill(vals.begin() + r * dim, vals.begin() + (r + 1) * dim, 0.0);
}

// check_metric compares run's rank scores for a distance metric with a double-precision
// reference computed directly: -(1 - x.q / (|x||q|)) (a zero vector has distance 1) or
// -sum (x - q)^2. Row 0 is a zero vector and row 1 equals query 0.
void check_metric(int format, uint32_t dim, size_t rows, uint32_t nq, uint64_t max_rows,
                  int metric) {
    std::mt19937 rng(format * 31 + dim * 7 + rows + metric);
    const bool block = format == GPU_BLOCKSCALED_MXFP8 || format == GPU_BLOCKSCALED_NVFP4;
    std::vector<double> qv, rv;
    std::vector<uint8_t> queries = block ? make_cells(format, dim, nq, rng, qv)
                                         : make_plain(format, dim, nq, rng, qv);
    std::vector<uint8_t> cells = block ? make_cells(format, dim, rows, rng, rv)
                                       : make_plain(format, dim, rows, rng, rv);
    const size_t cell_bytes = cells.size() / rows, qbytes = queries.size() / nq;
    zero_vector(format, dim, cells, cell_bytes, 0, rv);
    std::memcpy(cells.data() + cell_bytes, queries.data(), qbytes);
    std::copy(qv.begin(), qv.begin() + dim, rv.begin() + dim);

    blockscaled_matmul e(0, format, dim, nq, queries.data(), max_rows, 0, metric);
    std::vector<float> scores(rows * nq);
    for (size_t off = 0; off < rows; off += e.max_rows()) {
        size_t n = std::min<size_t>(e.max_rows(), rows - off);
        e.run(cells.data() + off * cell_bytes, n, scores.data() + off * nq);
    }
    for (size_t r = 0; r < rows; r++) {
        for (uint32_t q = 0; q < nq; q++) {
            double dot = 0, nr = 0, nqq = 0, l2 = 0;
            for (uint32_t k = 0; k < dim; k++) {
                const double x = rv[r * dim + k], y = qv[q * dim + k];
                dot += x * y;
                nr += x * x;
                nqq += y * y;
                l2 += (x - y) * (x - y);
            }
            const double got = scores[r * nq + q];
            if (metric == blockscaled_matmul::kCosine) {
                const double ref = nr > 0 && nqq > 0 ? -(1 - dot / std::sqrt(nr * nqq)) : -1.0;
                ASSERT_TRUE(std::fabs(got - ref) <= 1e-5);
            } else {
                // the expansion |x|^2 + |q|^2 - 2 x.q loses digits relative to the norms
                ASSERT_TRUE(std::fabs(got + l2) <= 1e-5 * std::max(1.0, nr + nqq));
                ASSERT_TRUE(got <= 0);
            }
        }
    }
    // row 1 equals query 0: distance 0 up to the rounding relative to its squared norm
    double n0 = 0;
    for (uint32_t k = 0; k < dim; k++) n0 += qv[k] * qv[k];
    ASSERT_TRUE(std::fabs(scores[1 * nq + 0]) <= 1e-5 * std::max(1.0, 2 * n0));
}


// nvidia_gemm computes D = alpha * X Q^T with cuBLASLt's block-scaled GEMM on operands in
// NVIDIA's layout, as a framework calls it outside MO: x (rows) and q (nq) row major, K
// elements per row (E4M3 bytes, or E2M1 two per byte with element 2i in the low nibble),
// sx and sq row-major block scales (UE8M0 per 32 or UE4M3 per 16), and the per-tensor
// global scales folded into alpha. out receives rows x nq scores, row major.
void nvidia_gemm(int format, const std::vector<uint8_t>& x, const std::vector<uint8_t>& sx,
                 size_t rows, const std::vector<uint8_t>& q, const std::vector<uint8_t>& sq,
                 size_t nq, size_t K, float alpha, std::vector<float>& out) {
    const bool fp8 = format == GPU_BLOCKSCALED_MXFP8;
    const size_t block = fp8 ? 32 : 16, row_bytes = fp8 ? K : K / 2, S = K / block;
    const size_t M = (rows + 127) / 128 * 128, N = (nq + 127) / 128 * 128;
    const size_t Sp = (S + 3) / 4 * 4;
    // the scale tensor of an operand: 128 x 4 tiles, rows (r % 32, r / 32 % 4) interleaved
    auto tiled = [&](const std::vector<uint8_t>& s, size_t n, size_t padded) {
        std::vector<uint8_t> t(padded * Sp, 0);
        for (size_t r = 0; r < n; r++) {
            for (size_t j = 0; j < S; j++) {
                t[((r / 128) * (Sp / 4) + j / 4) * 512 + (r % 32) * 16 + (r % 128) / 32 * 4 +
                  j % 4] = s[r * S + j];
            }
        }
        return t;
    };
    std::vector<uint8_t> hx(M * row_bytes, 0), hq(N * row_bytes, 0);
    std::copy(x.begin(), x.end(), hx.begin());
    std::copy(q.begin(), q.end(), hq.begin());
    std::vector<uint8_t> hsx = tiled(sx, rows, M), hsq = tiled(sq, nq, N);

    void *d_x, *d_q, *d_sx, *d_sq, *d_d, *d_w;
    const size_t ws = 32 << 20;
    ASSERT_TRUE(cudaMalloc(&d_x, hx.size()) == cudaSuccess);
    ASSERT_TRUE(cudaMalloc(&d_q, hq.size()) == cudaSuccess);
    ASSERT_TRUE(cudaMalloc(&d_sx, hsx.size()) == cudaSuccess);
    ASSERT_TRUE(cudaMalloc(&d_sq, hsq.size()) == cudaSuccess);
    ASSERT_TRUE(cudaMalloc(&d_d, M * N * sizeof(float)) == cudaSuccess);
    ASSERT_TRUE(cudaMalloc(&d_w, ws) == cudaSuccess);
    cudaMemcpy(d_x, hx.data(), hx.size(), cudaMemcpyHostToDevice);
    cudaMemcpy(d_q, hq.data(), hq.size(), cudaMemcpyHostToDevice);
    cudaMemcpy(d_sx, hsx.data(), hsx.size(), cudaMemcpyHostToDevice);
    cudaMemcpy(d_sq, hsq.data(), hsq.size(), cudaMemcpyHostToDevice);

    cublasLtHandle_t lt;
    cublasLtMatmulDesc_t desc;
    cublasLtMatrixLayout_t la, lb, lc;
    cublasLtMatmulPreference_t pref;
    ASSERT_TRUE(cublasLtCreate(&lt) == CUBLAS_STATUS_SUCCESS);
    ASSERT_TRUE(cublasLtMatmulDescCreate(&desc, CUBLAS_COMPUTE_32F, CUDA_R_32F) ==
                CUBLAS_STATUS_SUCCESS);
    cublasOperation_t ta = CUBLAS_OP_T, tb = CUBLAS_OP_N;
    cublasLtMatmulMatrixScale_t mode =
        fp8 ? CUBLASLT_MATMUL_MATRIX_SCALE_VEC32_UE8M0 : CUBLASLT_MATMUL_MATRIX_SCALE_VEC16_UE4M3;
    cublasLtMatmulDescSetAttribute(desc, CUBLASLT_MATMUL_DESC_TRANSA, &ta, sizeof(ta));
    cublasLtMatmulDescSetAttribute(desc, CUBLASLT_MATMUL_DESC_TRANSB, &tb, sizeof(tb));
    cublasLtMatmulDescSetAttribute(desc, CUBLASLT_MATMUL_DESC_A_SCALE_MODE, &mode, sizeof(mode));
    cublasLtMatmulDescSetAttribute(desc, CUBLASLT_MATMUL_DESC_B_SCALE_MODE, &mode, sizeof(mode));
    cublasLtMatmulDescSetAttribute(desc, CUBLASLT_MATMUL_DESC_A_SCALE_POINTER, &d_sx, sizeof(d_sx));
    cublasLtMatmulDescSetAttribute(desc, CUBLASLT_MATMUL_DESC_B_SCALE_POINTER, &d_sq, sizeof(d_sq));
    const cudaDataType_t et = fp8 ? CUDA_R_8F_E4M3 : CUDA_R_4F_E2M1;
    cublasLtMatrixLayoutCreate(&la, et, K, M, K);
    cublasLtMatrixLayoutCreate(&lb, et, K, N, K);
    cublasLtMatrixLayoutCreate(&lc, CUDA_R_32F, M, N, M);
    cublasLtMatmulPreferenceCreate(&pref);
    cublasLtMatmulPreferenceSetAttribute(pref, CUBLASLT_MATMUL_PREF_MAX_WORKSPACE_BYTES, &ws,
                                         sizeof(ws));
    cublasLtMatmulHeuristicResult_t heur{};
    int nres = 0;
    ASSERT_TRUE(cublasLtMatmulAlgoGetHeuristic(lt, desc, la, lb, lc, lc, pref, 1, &heur, &nres) ==
                    CUBLAS_STATUS_SUCCESS &&
                nres == 1);
    const float beta = 0.0f;
    ASSERT_TRUE(cublasLtMatmul(lt, desc, &alpha, d_x, la, d_q, lb, &beta, d_d, lc, d_d, lc,
                               &heur.algo, d_w, ws, nullptr) == CUBLAS_STATUS_SUCCESS);
    std::vector<float> d(M * N);
    ASSERT_TRUE(cudaMemcpy(d.data(), d_d, d.size() * sizeof(float), cudaMemcpyDeviceToHost) ==
                cudaSuccess);
    out.assign(rows * nq, 0);
    for (size_t r = 0; r < rows; r++) {
        for (size_t j = 0; j < nq; j++) out[r * nq + j] = d[r + j * M];
    }
    cublasLtMatmulPreferenceDestroy(pref);
    cublasLtMatrixLayoutDestroy(la);
    cublasLtMatrixLayoutDestroy(lb);
    cublasLtMatrixLayoutDestroy(lc);
    cublasLtMatmulDescDestroy(desc);
    cublasLtDestroy(lt);
    for (void* p : {d_x, d_q, d_sx, d_sq, d_d, d_w}) cudaFree(p);
}

// check_matches_nvidia quantizes nothing: it draws element codes, block scales and one
// global scale per operand in NVIDIA's layout, scores them with nvidia_gemm, and scores the
// same bytes as MO cells (every row carrying the operand's global scale, as vecblock JSON or
// vecblock_binary stores it) with the engine. The scores agree to the float32 rounding of the
// two runs, and so do the top-10 rows of each query. At the GEMM shape of the engine's tile
// (same_shape), where cuBLASLt runs the same algorithm, MXFP8 (global scales 1) is
// bit-identical, and NVFP4 is within 2 ulp: the engine applies the global scales in double
// and rounds once, NVIDIA's alpha is float(G_a * G_b) applied in float.
void check_matches_nvidia(int format, size_t K, size_t rows, size_t nq, bool same_shape = false) {
    std::mt19937 rng(uint32_t(format * 1009 + K + rows));
    const bool fp8 = format == GPU_BLOCKSCALED_MXFP8;
    const size_t block = fp8 ? 32 : 16, row_bytes = fp8 ? K : K / 2, S = K / block;
    const float gx = fp8 ? 1.0f : 0.0123f, gq = fp8 ? 1.0f : 3.7f;
    auto operand = [&](size_t n, std::vector<uint8_t>& el, std::vector<uint8_t>& sc,
                       std::vector<double>& vals, float g) {
        el.assign(n * row_bytes, 0);
        sc.resize(n * S);
        vals.assign(n * K, 0);
        for (auto& s : sc) s = fp8 ? uint8_t(127 - 3 + rng() % 7) : uint8_t(0x30 + rng() % 0x19);
        for (size_t r = 0; r < n; r++) {
            for (size_t k = 0; k < K; k++) {
                double v, scale;
                if (fp8) {
                    uint8_t c;
                    do {
                        c = uint8_t(rng());
                    } while ((c & 0x7f) == 0x7f);
                    el[r * row_bytes + k] = c;
                    v = e4m3(c);
                    scale = std::ldexp(1.0, int(sc[r * S + k / 32]) - 127);
                } else {
                    uint8_t c = uint8_t(rng() & 0xf);
                    el[r * row_bytes + k / 2] |= k % 2 == 0 ? c : uint8_t(c << 4);
                    v = e2m1(c);
                    scale = e4m3(sc[r * S + k / 16]);
                }
                vals[r * K + k] = double(g) * scale * v;
            }
        }
    };
    // the same bytes as cells: header, the row's block scales, its elements
    auto cells = [&](size_t n, const std::vector<uint8_t>& el, const std::vector<uint8_t>& sc,
                     float g) {
        const size_t cell_bytes = 12 + S + row_bytes;
        std::vector<uint8_t> out(n * cell_bytes, 0);
        const uint32_t dim = uint32_t(K);
        for (size_t r = 0; r < n; r++) {
            uint8_t* c = out.data() + r * cell_bytes;
            c[0] = 1;
            c[1] = uint8_t(format);
            std::memcpy(c + 4, &dim, 4);
            std::memcpy(c + 8, &g, 4);
            std::memcpy(c + 12, sc.data() + r * S, S);
            std::memcpy(c + 12 + S, el.data() + r * row_bytes, row_bytes);
        }
        return out;
    };
    std::vector<uint8_t> xe, xs, qe, qs;
    std::vector<double> xv, qv;
    operand(rows, xe, xs, xv, gx);
    operand(nq, qe, qs, qv, gq);

    std::vector<float> nv;
    nvidia_gemm(format, xe, xs, rows, qe, qs, nq, K, gx * gq, nv);
    ASSERT_EQ(nv.size(), rows * nq);

    std::vector<uint8_t> qcells = cells(nq, qe, qs, gq), xcells = cells(rows, xe, xs, gx);
    blockscaled_matmul e(0, format, uint32_t(K), uint32_t(nq), qcells.data(), 512);
    std::vector<float> mo(rows * nq);
    const size_t cell_bytes = xcells.size() / rows;
    for (size_t off = 0; off < rows; off += e.max_rows()) {
        size_t n = std::min<size_t>(e.max_rows(), rows - off);
        e.run(xcells.data() + off * cell_bytes, n, mo.data() + off * nq);
    }

    size_t identical = 0;
    int64_t max_ulps = 0;
    for (size_t r = 0; r < rows; r++) {
        for (size_t j = 0; j < nq; j++) {
            double mag = 0;
            for (size_t k = 0; k < K; k++) mag += std::fabs(xv[r * K + k] * qv[j * K + k]);
            const float a = nv[r * nq + j], b = mo[r * nq + j];
            ASSERT_TRUE(std::fabs(double(a) - double(b)) <= 1e-6 * std::max(mag, 1e-30));
            identical += a == b;
            int32_t ia, ib;
            std::memcpy(&ia, &a, 4);
            std::memcpy(&ib, &b, 4);
            if ((ia < 0) == (ib < 0)) max_ulps = std::max<int64_t>(max_ulps, std::llabs(int64_t(ia) - ib));
        }
    }
    if (same_shape) {
        if (fp8) ASSERT_EQ(identical, rows * nq);
        ASSERT_LE(max_ulps, 2);
    }
    // the same rows lead every query, unless the 10th and 11th scores are within rounding
    for (size_t j = 0; j < nq; j++) {
        std::vector<size_t> on(rows), om(rows);
        for (size_t r = 0; r < rows; r++) on[r] = om[r] = r;
        auto by = [&](const std::vector<float>& s) {
            return [&s, j, nq](size_t a, size_t b) { return s[a * nq + j] > s[b * nq + j]; };
        };
        std::sort(on.begin(), on.end(), by(nv));
        std::sort(om.begin(), om.end(), by(mo));
        const float gap = nv[on[9] * nq + j] - nv[on[10] * nq + j];
        if (gap > 1e-5f * std::fabs(nv[on[9] * nq + j])) {
            std::vector<size_t> tn(on.begin(), on.begin() + 10), tm(om.begin(), om.begin() + 10);
            std::sort(tn.begin(), tn.end());
            std::sort(tm.begin(), tm.end());
            ASSERT_TRUE(tn == tm);
        }
    }
    printf("    %s K=%zu rows=%zu nq=%zu: %zu of %zu scores bit-identical to the NVIDIA call, "
           "largest difference %lld ulp\n",
           fp8 ? "MXFP8" : "NVFP4", K, rows, nq, identical, rows * nq, (long long)max_ulps);
}


// check_magnitude scores a query of magnitude 2^e (elements 2^e times {1, 1.5, 2}) against
// rows equal to the query, to twice it, to minus it and a zero vector, in a format holding
// those values,
// and compares cosine and l2sq with a double reference over the stored values: every
// distance non-negative, the cosine distance at most 2 and to 1e-6, the squared L2 distance to 1e-5 of |x|^2 + |q|^2, and a distance beyond
// the float range as +Inf.
void check_magnitude(int format, uint32_t dim, int e) {
    const bool mx = format == GPU_BLOCKSCALED_MXFP8, nv = format == GPU_BLOCKSCALED_NVFP4;
    const size_t esize = format == GPU_BLOCKSCALED_F32 ? 4 : 2;
    const double pattern[3] = {1, 1.5, 2};
    const double factor[5] = {1, 1, 2, -1, 0}; // query, then rows: equal, twice, minus, zero
    std::vector<std::vector<uint8_t>> cells(5);
    std::vector<std::vector<double>> vals(5, std::vector<double>(dim));
    for (int v = 0; v < 5; v++) {
        for (uint32_t k = 0; k < dim; k++) vals[v][k] = std::ldexp(factor[v] * pattern[k % 3], e);
        if (mx || nv) {
            const size_t block = mx ? 32 : 16, nscale = (dim + block - 1) / block;
            const size_t elem_bytes = mx ? dim : (dim + 1) / 2;
            std::vector<uint8_t> c(12 + nscale + elem_bytes, 0);
            c[0] = 1;
            c[1] = uint8_t(format);
            std::memcpy(&c[4], &dim, 4);
            // MXFP8: scale 2^e and E4M3 codes; NVFP4: global 2^e, scale 1 and E2M1 codes
            const float g = mx ? 1.0f : std::ldexp(1.0f, e);
            std::memcpy(&c[8], &g, 4);
            for (size_t s = 0; s < nscale; s++) c[12 + s] = mx ? uint8_t(127 + e) : 0x38;
            for (uint32_t k = 0; k < dim; k++) {
                const double x = factor[v] * pattern[k % 3];
                if (mx) {
                    __nv_fp8_e4m3 q(static_cast<float>(x));
                    c[12 + nscale + k] = q.__x;
                } else {
                    __nv_fp4_e2m1 q(static_cast<float>(x));
                    c[12 + nscale + k / 2] |= k % 2 == 0 ? (q.__x & 0xf) : uint8_t(q.__x << 4);
                }
            }
            cells[v] = c;
        } else {
            cells[v].resize(dim * esize);
            for (uint32_t k = 0; k < dim; k++) {
                const float x = float(vals[v][k]);
                if (format == GPU_BLOCKSCALED_F32) {
                    std::memcpy(&cells[v][k * 4], &x, 4);
                } else if (format == GPU_BLOCKSCALED_BF16) {
                    __nv_bfloat16 h = __float2bfloat16(x);
                    std::memcpy(&cells[v][k * 2], &h, 2);
                    vals[v][k] = double(__bfloat162float(h));
                } else {
                    __half h = __float2half(x);
                    std::memcpy(&cells[v][k * 2], &h, 2);
                    vals[v][k] = double(__half2float(h));
                }
            }
        }
    }
    std::vector<uint8_t> rows;
    for (int v = 1; v < 5; v++) rows.insert(rows.end(), cells[v].begin(), cells[v].end());
    for (int metric : {blockscaled_matmul::kCosine, blockscaled_matmul::kL2sq}) {
        blockscaled_matmul eng(0, format, dim, 1, cells[0].data(), 128, 0, metric);
        std::vector<float> scores(4);
        eng.run(rows.data(), 4, scores.data());
        double nq = 0;
        for (uint32_t k = 0; k < dim; k++) nq += vals[0][k] * vals[0][k];
        for (int r = 0; r < 4; r++) {
            double nr = 0, l2 = 0, dot = 0;
            for (uint32_t k = 0; k < dim; k++) {
                const double x = vals[r + 1][k], y = vals[0][k];
                nr += x * x;
                dot += x * y;
                l2 += (x - y) * (x - y);
            }
            const double got = -double(scores[r]);
            // a distance is never negative; a cosine distance is at most 2
            ASSERT_TRUE(got >= 0);
            if (metric == blockscaled_matmul::kCosine) {
                ASSERT_TRUE(got <= 2);
                const double want = nr > 0 && nq > 0 ? 1 - dot / std::sqrt(nr * nq) : 1.0;
                if (!(std::fabs(got - want) <= 1e-6)) {
                    printf("    format %d dim %u 2^%d cosine row %d: %g, want %g\n", format, dim, e, r, got, want);
                }
                ASSERT_TRUE(std::fabs(got - want) <= 1e-6);
            } else if (std::isinf(float(l2))) {
                ASSERT_TRUE(std::isinf(got));
            } else {
                const double tol = std::max(1e-5 * (nr + nq), 1.5e-45);
                if (!(std::fabs(got - l2) <= tol)) {
                    printf("    format %d dim %u 2^%d l2sq row %d: %g, want %g\n", format, dim, e, r, got, l2);
                }
                ASSERT_TRUE(std::fabs(got - l2) <= tol);
            }
        }
    }
}


} // namespace

TEST(BlockScaledMatmulTest, DistanceMetricsMatchReference) {
    for (int metric : {blockscaled_matmul::kCosine, blockscaled_matmul::kL2sq}) {
        for (int format : {GPU_BLOCKSCALED_MXFP8, GPU_BLOCKSCALED_NVFP4, GPU_BLOCKSCALED_F32,
                           GPU_BLOCKSCALED_F16, GPU_BLOCKSCALED_BF16, GPU_BLOCKSCALED_I8,
                           GPU_BLOCKSCALED_U8}) {
            // 300 rows over 128-row tiles: two full tiles and a partial one
            check_metric(format, 96, 300, 5, 128, metric);
            check_metric(format, 32, 7, 2, 512, metric);
            check_metric(format, 200, 300, 3, 128, metric);
        }
    }
}

// Cosine and l2sq hold at magnitudes whose fp32 products overflow or underflow: the rows are
// rescaled by powers of two before the matmul.
TEST(BlockScaledMatmulTest, DistanceMetricsAtExtremeMagnitudes) {
    // dimensions up to 128 take a thread per row in the row statistics, larger a warp
    for (uint32_t dim : {15u, 16u, 17u, 33u, 96u, 129u, 768u}) {
        for (int e : {120, 100, 0, -100, -140}) {
            check_magnitude(GPU_BLOCKSCALED_F32, dim, e);
            check_magnitude(GPU_BLOCKSCALED_BF16, dim, e);
            check_magnitude(GPU_BLOCKSCALED_NVFP4, dim, e);
            if (e >= -120) check_magnitude(GPU_BLOCKSCALED_MXFP8, dim, e);
        }
        check_magnitude(GPU_BLOCKSCALED_F16, dim, 0);
        check_magnitude(GPU_BLOCKSCALED_F16, dim, -10);
    }
}

TEST(BlockScaledMatmulTest, DistanceMetricTopK) {
    std::mt19937 rng(23);
    std::vector<double> qv, rv;
    for (int metric : {blockscaled_matmul::kCosine, blockscaled_matmul::kL2sq}) {
        std::vector<uint8_t> queries = make_cells(GPU_BLOCKSCALED_NVFP4, 96, 3, rng, qv);
        std::vector<uint8_t> cells = make_cells(GPU_BLOCKSCALED_NVFP4, 96, 300, rng, rv);
        check_topk(GPU_BLOCKSCALED_NVFP4, 96, queries, cells, 300, 3, 10, 256, false, metric);
        std::vector<uint8_t> iq = small_int_cells(4, 2, rng);
        iq[0] = iq[4] = 1;
        std::vector<uint8_t> ic = small_int_cells(4, 1000, rng);
        check_topk(GPU_BLOCKSCALED_U8, 4, iq, ic, 1000, 2, 5, 512, true, metric);
    }
}

TEST(BlockScaledMatmulTest, TopKMatchesFullScores) {
    std::mt19937 rng(11);
    std::vector<double> qv, rv;
    for (int format : {GPU_BLOCKSCALED_MXFP8, GPU_BLOCKSCALED_NVFP4}) {
        for (size_t rows : {size_t(5), size_t(300)}) {
            std::vector<uint8_t> queries = make_cells(format, 96, 3, rng, qv);
            std::vector<uint8_t> cells = make_cells(format, 96, rows, rng, rv);
            check_topk(format, 96, queries, cells, rows, 3, 10, 256, false);
        }
    }
    {
        std::vector<uint8_t> queries(3 * 64 * 4), cells(300 * 64 * 4);
        std::normal_distribution<float> nd(0.0f, 1.0f);
        for (size_t i = 0; i < queries.size() / 4; i++) {
            float v = nd(rng);
            std::memcpy(&queries[i * 4], &v, 4);
        }
        for (size_t i = 0; i < cells.size() / 4; i++) {
            float v = nd(rng);
            std::memcpy(&cells[i * 4], &v, 4);
        }
        check_topk(GPU_BLOCKSCALED_F32, 64, queries, cells, 300, 3, 7, 128, false);
    }
    // values in {0, 1, 2} over 4 dimensions: many rows share the k-th score
    for (int format : {GPU_BLOCKSCALED_I8, GPU_BLOCKSCALED_U8}) {
        std::vector<uint8_t> queries = small_int_cells(4, 2, rng);
        queries[0] = queries[4] = 1;
        std::vector<uint8_t> cells = small_int_cells(4, 1000, rng);
        check_topk(format, 4, queries, cells, 1000, 2, 5, 512, true);
    }
    // k above 256: select_k takes the radix path instead of the warp sort
    {
        std::vector<uint8_t> queries(3 * 64 * 4), cells(1000 * 64 * 4);
        std::normal_distribution<float> nd(0.0f, 1.0f);
        for (size_t i = 0; i < queries.size() / 4; i++) {
            float v = nd(rng);
            std::memcpy(&queries[i * 4], &v, 4);
        }
        for (size_t i = 0; i < cells.size() / 4; i++) {
            float v = nd(rng);
            std::memcpy(&cells[i * 4], &v, 4);
        }
        check_topk(GPU_BLOCKSCALED_F32, 64, queries, cells, 1000, 3, 300, 512, false);
        std::vector<uint8_t> iq = small_int_cells(4, 2, rng);
        iq[0] = iq[4] = 1;
        std::vector<uint8_t> ic = small_int_cells(4, 2000, rng);
        check_topk(GPU_BLOCKSCALED_I8, 4, iq, ic, 2000, 2, 300, 1024, true);
    }
    // k larger than the tile keeps every row
    std::vector<uint8_t> queries = small_int_cells(8, 1, rng);
    std::vector<uint8_t> cells = small_int_cells(8, 50, rng);
    check_topk(GPU_BLOCKSCALED_I8, 8, queries, cells, 50, 1, 500, 128, false);
}

// With one global scale per operand, MO's cells hold exactly NVIDIA's NVFP4 / MXFP8 operands,
// and vector_matmul's engine scores them as cuBLASLt does outside MO: bit for bit at the same
// GEMM shape.
TEST(BlockScaledMatmulTest, MatchesNvidiaBlockScaledGemm) {
    for (int format : {GPU_BLOCKSCALED_NVFP4, GPU_BLOCKSCALED_MXFP8}) {
        check_matches_nvidia(format, 768, 300, 5);
        check_matches_nvidia(format, 1024, 1000, 16);
        // one 512-row tile: the shape of the engine's GEMM
        check_matches_nvidia(format, 1024, 512, 16, true);
    }
}

TEST(BlockScaledMatmulTest, MXFP8MatchesReference) {
    for (uint32_t dim : {4u, 32u, 100u, 768u}) {
        for (size_t rows : {size_t(1), size_t(127), size_t(300)}) {
            for (uint32_t nq : {1u, 3u}) {
                check_engine(GPU_BLOCKSCALED_MXFP8, dim, rows, nq, 128);
            }
        }
    }
}

TEST(BlockScaledMatmulTest, NVFP4MatchesReference) {
    for (uint32_t dim : {4u, 16u, 33u, 768u}) {
        for (size_t rows : {size_t(1), size_t(129), size_t(300)}) {
            for (uint32_t nq : {1u, 3u}) {
                check_engine(GPU_BLOCKSCALED_NVFP4, dim, rows, nq, 256);
            }
        }
    }
}

TEST(BlockScaledMatmulTest, PlainFormatsMatchReference) {
    for (int format : {GPU_BLOCKSCALED_F32, GPU_BLOCKSCALED_F16, GPU_BLOCKSCALED_BF16,
                       GPU_BLOCKSCALED_I8, GPU_BLOCKSCALED_U8}) {
        for (uint32_t dim : {4u, 33u, 768u}) {
            for (size_t rows : {size_t(1), size_t(129), size_t(300)}) {
                for (uint32_t nq : {1u, 3u}) {
                    check_plain(format, dim, rows, nq, 256);
                }
            }
        }
    }
}

TEST(BlockScaledMatmulTest, DeviceBaselineAndHostBytes) {
    // Volta, Ampere, Ada and Hopper are below the baseline; Blackwell meets it
    for (int major : {7, 8, 9}) ASSERT_TRUE(!blockscaled_matmul::meets_baseline(major));
    for (int major : {10, 11, 12}) ASSERT_TRUE(blockscaled_matmul::meets_baseline(major));
    int n = 0;
    ASSERT_EQ(cudaGetDeviceCount(&n), cudaSuccess);
    size_t want = 0;
    for (int d = 0; d < n; d++) {
        int major = 0;
        ASSERT_EQ(cudaDeviceGetAttribute(&major, cudaDevAttrComputeCapabilityMajor, d), cudaSuccess);
        if (blockscaled_matmul::meets_baseline(major)) want++;
    }
    ASSERT_EQ(blockscaled_matmul::eligible_devices().size(), want);
    ASSERT_EQ(gpu_blockscaled_matmul_device_count(), int(want));

    // MXFP8, dim 768, 3 queries, 256 rows: K 768, 24 scales padded to 24, 128 padded queries
    const uint64_t mx = 256 * 768 + 256 * 24 + 256 * 3 * 4 + 256 * 4 + 3 * 4 + 128 * (768 + 24);
    ASSERT_EQ(blockscaled_matmul::host_bytes(GPU_BLOCKSCALED_MXFP8, 768, 3, 200), mx);
    ASSERT_EQ(gpu_blockscaled_matmul_host_bytes(GPU_BLOCKSCALED_MXFP8, 768, 3, 200), mx);
    // F32, dim 33: K 64, 4-byte elements, no scales
    const uint64_t f32 = 128 * 256 + 128 * 2 * 4 + 128 * 4 + 2 * 4 + 128 * 256;
    ASSERT_EQ(blockscaled_matmul::host_bytes(GPU_BLOCKSCALED_F32, 33, 2, 1), f32);
}

TEST(BlockScaledMatmulTest, CWrapper) {
    std::mt19937 rng(7);
    std::vector<double> qv, rv;
    std::vector<uint8_t> queries = make_cells(GPU_BLOCKSCALED_MXFP8, 64, 2, rng, qv);
    std::vector<uint8_t> cells = make_cells(GPU_BLOCKSCALED_MXFP8, 64, 5, rng, rv);
    char* err = nullptr;
    gpu_blockscaled_matmul_c e =
        gpu_blockscaled_matmul_new(GPU_BLOCKSCALED_MXFP8, 64, 2, queries.data(), 10, 3, GPU_BLOCKSCALED_METRIC_INNER_PRODUCT, &err);
    ASSERT_TRUE(e != nullptr);
    ASSERT_TRUE(err == nullptr);
    ASSERT_EQ(gpu_blockscaled_matmul_max_rows(e), uint64_t(128));
    std::vector<float> scores(5 * 2);
    gpu_blockscaled_matmul_run(e, cells.data(), 5, scores.data(), &err);
    ASSERT_TRUE(err == nullptr);
    gpu_blockscaled_matmul_run(e, cells.data(), 129, scores.data(), &err);
    ASSERT_TRUE(err != nullptr);
    free(err);
    err = nullptr;
    std::vector<float> top(2 * 3), full(2 * 5);
    std::vector<int32_t> rows(2 * 3);
    std::vector<uint8_t> tied(2);
    gpu_blockscaled_matmul_run_topk(e, cells.data(), 5, top.data(), rows.data(), full.data(),
                                    tied.data(), &err);
    ASSERT_TRUE(err == nullptr);
    gpu_blockscaled_matmul_destroy(e);

    err = nullptr;
    e = gpu_blockscaled_matmul_new(GPU_BLOCKSCALED_MXFP8, 64, 2, queries.data(), 10, 0, GPU_BLOCKSCALED_METRIC_INNER_PRODUCT, &err);
    ASSERT_TRUE(e != nullptr);
    gpu_blockscaled_matmul_run_topk(e, cells.data(), 5, top.data(), rows.data(), full.data(),
                                    tied.data(), &err);
    ASSERT_TRUE(err != nullptr);
    free(err);
    gpu_blockscaled_matmul_destroy(e);

    err = nullptr;
    ASSERT_TRUE(gpu_blockscaled_matmul_new(8, 64, 2, queries.data(), 10, 0,
                                           GPU_BLOCKSCALED_METRIC_INNER_PRODUCT, &err) == nullptr);
    ASSERT_TRUE(err != nullptr);
    free(err);

    // an unknown metric
    err = nullptr;
    ASSERT_TRUE(gpu_blockscaled_matmul_new(GPU_BLOCKSCALED_MXFP8, 64, 2, queries.data(), 10, 0, 3,
                                           &err) == nullptr);
    ASSERT_TRUE(err != nullptr);
    free(err);
}

thread_local bool current_test_failed = false;

int main() { return RUN_ALL_TESTS(); }
