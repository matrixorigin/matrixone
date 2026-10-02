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

#ifndef BLOCKSCALED_MATMUL_C_H
#define BLOCKSCALED_MATMUL_C_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define GPU_BLOCKSCALED_MXFP8 1
#define GPU_BLOCKSCALED_NVFP4 2
/* Plain formats: cells are raw vectors: float32 (vecf32), IEEE half (vecf16), int8
 * (vecint8), uint8 (vecuint8) or bfloat16 (vecbf16). */
#define GPU_BLOCKSCALED_F32 3
#define GPU_BLOCKSCALED_F16 4
#define GPU_BLOCKSCALED_I8 5
#define GPU_BLOCKSCALED_U8 6
#define GPU_BLOCKSCALED_BF16 7

typedef void* gpu_blockscaled_matmul_c;

/**
 * @brief Creates a cuBLASLt block-scaled matmul engine for vecf8/vecf4 cells.
 *
 * The device is selected round-robin across the visible devices.
 *
 * @param format GPU_BLOCKSCALED_MXFP8 (vecf8), GPU_BLOCKSCALED_NVFP4 (vecf4) or
 *               or a plain format GPU_BLOCKSCALED_F32/_F16/_I8/_U8/_BF16.
 * @param dim Vector dimension.
 * @param nq Number of query cells.
 * @param query_cells nq cells packed back to back.
 * @param max_rows Tile row capacity.
 * @param topk Hits per query kept by gpu_blockscaled_matmul_run_topk; 0 disables it.
 * @param errmsg Pointer to store an error message, if any.
 * @return The engine, or NULL with errmsg set.
 */
gpu_blockscaled_matmul_c gpu_blockscaled_matmul_new(int format, uint32_t dim, uint32_t nq,
                                                    const uint8_t* query_cells, uint64_t max_rows,
                                                    uint32_t topk, void* errmsg);

/** @brief Returns the tile row capacity (max_rows rounded up to 128). */
uint64_t gpu_blockscaled_matmul_max_rows(gpu_blockscaled_matmul_c e);

/**
 * @brief Scores n cells packed back to back (n <= max rows) against the queries.
 *
 * scores receives n * nq floats, row major: scores[r * nq + q] is the dot product of
 * cell r and query q.
 */
void gpu_blockscaled_matmul_run(gpu_blockscaled_matmul_c e, const uint8_t* cells, uint64_t n,
                                float* scores, void* errmsg);

/**
 * @brief Scores n cells like gpu_blockscaled_matmul_run and keeps the topk best rows per
 * query on the device (NaN scores as -Inf).
 *
 * top_scores and top_rows receive nq * topk entries, query major: entry q * topk + j is a
 * kept row of query q (an index into the n cells) and its score; unused slots hold row -1.
 * When query q has more rows tied at its topk-th score than were kept, tied[q] is 1 and
 * full_scores[q * n, (q + 1) * n) receives all n scores of query q; otherwise tied[q] is 0
 * and that range of full_scores is not written.
 */
void gpu_blockscaled_matmul_run_topk(gpu_blockscaled_matmul_c e, const uint8_t* cells, uint64_t n,
                                     float* top_scores, int32_t* top_rows, float* full_scores,
                                     uint8_t* tied, void* errmsg);

void gpu_blockscaled_matmul_destroy(gpu_blockscaled_matmul_c e);

#ifdef __cplusplus
}
#endif

#endif
