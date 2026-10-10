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

#include "blockscaled_matmul_c.h"
#include "blockscaled_matmul.hpp"
#include "helper.h"

#include <exception>
#include <stdexcept>

extern "C" {

int gpu_blockscaled_matmul_device_count(void) {
    return int(matrixone::blockscaled_matmul::eligible_devices().size());
}

uint64_t gpu_blockscaled_matmul_host_bytes(int format, uint32_t dim, uint32_t nq, uint64_t max_rows) {
    return matrixone::blockscaled_matmul::host_bytes(format, dim, nq, max_rows);
}

gpu_blockscaled_matmul_c gpu_blockscaled_matmul_new(int format, uint32_t dim, uint32_t nq,
                                                    const uint8_t* query_cells, uint64_t max_rows,
                                                    uint32_t topk, int metric, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        int device_id = matrixone::blockscaled_matmul::next_device();
        if (device_id < 0) {
            throw std::runtime_error(
                "no visible GPU with compute capability 10.0 or newer for block-scaled matmul");
        }
        return new matrixone::blockscaled_matmul(device_id, format, dim, nq, query_cells, max_rows,
                                                 topk, metric);
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, "Error in gpu_blockscaled_matmul_new", e.what());
    } catch (...) {
        matrixone::set_errmsg(errmsg, "Error in gpu_blockscaled_matmul_new", "unknown C++ exception");
    }
    return nullptr;
}

uint64_t gpu_blockscaled_matmul_max_rows(gpu_blockscaled_matmul_c e) {
    return static_cast<matrixone::blockscaled_matmul*>(e)->max_rows();
}

void gpu_blockscaled_matmul_run(gpu_blockscaled_matmul_c e, const uint8_t* cells, uint64_t n,
                                float* scores, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        static_cast<matrixone::blockscaled_matmul*>(e)->run(cells, n, scores);
    } catch (const std::exception& ex) {
        matrixone::set_errmsg(errmsg, "Error in gpu_blockscaled_matmul_run", ex.what());
    } catch (...) {
        matrixone::set_errmsg(errmsg, "Error in gpu_blockscaled_matmul_run", "unknown C++ exception");
    }
}

void gpu_blockscaled_matmul_run_topk(gpu_blockscaled_matmul_c e, const uint8_t* cells, uint64_t n,
                                     float* top_scores, int32_t* top_rows, float* full_scores,
                                     uint8_t* tied, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        static_cast<matrixone::blockscaled_matmul*>(e)->run_topk(cells, n, top_scores, top_rows,
                                                                  full_scores, tied);
    } catch (const std::exception& ex) {
        matrixone::set_errmsg(errmsg, "Error in gpu_blockscaled_matmul_run_topk", ex.what());
    } catch (...) {
        matrixone::set_errmsg(errmsg, "Error in gpu_blockscaled_matmul_run_topk",
                              "unknown C++ exception");
    }
}

void gpu_blockscaled_matmul_destroy(gpu_blockscaled_matmul_c e) {
    delete static_cast<matrixone::blockscaled_matmul*>(e);
}

} // extern "C"
