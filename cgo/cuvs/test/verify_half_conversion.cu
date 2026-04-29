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

#include <iostream>
#include <vector>
#include <random>
#include <cmath>
#include <cuda_fp16.h>
#include <raft/core/resources.hpp>
#include <raft/core/device_mdarray.hpp>
#include <raft/core/host_mdarray.hpp>
#include <raft/core/copy.cuh>
#include "../helper.h"
#include "test_framework.hpp"

// Host conversion logic from benchmark_cuvs.cu
static std::vector<half> host_convert(const std::vector<float>& src) {
    std::vector<half> dst(src.size());
    for(size_t i = 0; i < src.size(); ++i) {
        dst[i] = __float2half(src[i]);
    }
    return dst;
}

// --- TEST 1: raft::copy (two-step) ---
TEST(HalfConversionTest, RaftCopy) {
    raft::resources res;
    const size_t n_elements = 1024;
    
    // 1. Generate random float data
    std::vector<float> h_src(n_elements);
    std::mt19937 gen(42);
    std::uniform_real_distribution<float> dis(-1.0, 1.0);
    for (size_t i = 0; i < n_elements; ++i) {
        h_src[i] = dis(gen);
    }

    // 2. Host conversion
    std::vector<half> h_dst_host = host_convert(h_src);

    auto d_src_f = raft::make_device_vector<float, int64_t>(res, n_elements);
    auto d_dst_h = raft::make_device_vector<half, int64_t>(res, n_elements);

    raft::copy(d_src_f.data_handle(), h_src.data(), n_elements, raft::resource::get_cuda_stream(res));
    raft::resource::sync_stream(res);

    raft::copy(res, d_dst_h.view(), d_src_f.view());
    raft::resource::sync_stream(res);

    std::vector<half> h_dst_device(n_elements);
    raft::copy(h_dst_device.data(), d_dst_h.data_handle(), n_elements, raft::resource::get_cuda_stream(res));
    raft::resource::sync_stream(res);

    for (size_t i = 0; i < n_elements; ++i) {
        ASSERT_EQ(*reinterpret_cast<unsigned short*>(&h_dst_host[i]), *reinterpret_cast<unsigned short*>(&h_dst_device[i]));
    }
}

// --- TEST 2: convert_f32_to_f16_on_device ---
TEST(HalfConversionTest, OnDeviceKernel) {
    raft::resources res;
    const size_t n_elements = 1024;
    
    // 1. Generate random float data
    std::vector<float> h_src(n_elements);
    std::mt19937 gen(42);
    std::uniform_real_distribution<float> dis(-1.0, 1.0);
    for (size_t i = 0; i < n_elements; ++i) {
        h_src[i] = dis(gen);
    }

    // 2. Host conversion
    std::vector<half> h_dst_host = host_convert(h_src);

    auto d_src_f = raft::make_device_vector<float, int64_t>(res, n_elements);
    auto d_dst_h = raft::make_device_vector<half, int64_t>(res, n_elements);

    raft::copy(d_src_f.data_handle(), h_src.data(), n_elements, raft::resource::get_cuda_stream(res));
    raft::resource::sync_stream(res);

    matrixone::convert_f32_to_f16_on_device(res, d_src_f.data_handle(), d_dst_h.data_handle(), n_elements);
    raft::resource::sync_stream(res);

    std::vector<half> h_dst_device(n_elements);
    raft::copy(h_dst_device.data(), d_dst_h.data_handle(), n_elements, raft::resource::get_cuda_stream(res));
    raft::resource::sync_stream(res);

    for (size_t i = 0; i < n_elements; ++i) {
        ASSERT_EQ(*reinterpret_cast<unsigned short*>(&h_dst_host[i]), *reinterpret_cast<unsigned short*>(&h_dst_device[i]));
    }
}

// --- TEST 3: gpu_convert_f32_to_f16 (C interface) ---
TEST(HalfConversionTest, CInterface) {
    const size_t n_elements = 1024;
    
    // 1. Generate random float data
    std::vector<float> h_src(n_elements);
    std::mt19937 gen(42);
    std::uniform_real_distribution<float> dis(-1.0, 1.0);
    for (size_t i = 0; i < n_elements; ++i) {
        h_src[i] = dis(gen);
    }

    // 2. Host conversion
    std::vector<half> h_dst_host = host_convert(h_src);

    std::vector<half> h_dst_device(n_elements);
    char* errmsg = nullptr;
    gpu_convert_f32_to_f16(h_src.data(), h_dst_device.data(), n_elements, 0, &errmsg);
    
    ASSERT_TRUE(errmsg == nullptr);

    for (size_t i = 0; i < n_elements; ++i) {
        ASSERT_EQ(*reinterpret_cast<unsigned short*>(&h_dst_host[i]), *reinterpret_cast<unsigned short*>(&h_dst_device[i]));
    }
}
