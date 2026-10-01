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

#include <cuda_bf16.h>
#include <cuda_fp16.h>
#include <cuda_fp4.h>
#include <cuda_fp8.h>

#include <cmath>
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

} // namespace

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

TEST(BlockScaledMatmulTest, CWrapper) {
    std::mt19937 rng(7);
    std::vector<double> qv, rv;
    std::vector<uint8_t> queries = make_cells(GPU_BLOCKSCALED_MXFP8, 64, 2, rng, qv);
    std::vector<uint8_t> cells = make_cells(GPU_BLOCKSCALED_MXFP8, 64, 5, rng, rv);
    char* err = nullptr;
    gpu_blockscaled_matmul_c e =
        gpu_blockscaled_matmul_new(GPU_BLOCKSCALED_MXFP8, 64, 2, queries.data(), 10, &err);
    ASSERT_TRUE(e != nullptr);
    ASSERT_TRUE(err == nullptr);
    ASSERT_EQ(gpu_blockscaled_matmul_max_rows(e), uint64_t(128));
    std::vector<float> scores(5 * 2);
    gpu_blockscaled_matmul_run(e, cells.data(), 5, scores.data(), &err);
    ASSERT_TRUE(err == nullptr);
    gpu_blockscaled_matmul_run(e, cells.data(), 129, scores.data(), &err);
    ASSERT_TRUE(err != nullptr);
    free(err);
    gpu_blockscaled_matmul_destroy(e);

    err = nullptr;
    ASSERT_TRUE(gpu_blockscaled_matmul_new(8, 64, 2, queries.data(), 10, &err) == nullptr);
    ASSERT_TRUE(err != nullptr);
    free(err);
}

thread_local bool current_test_failed = false;

int main() { return RUN_ALL_TESTS(); }
