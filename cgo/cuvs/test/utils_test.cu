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

#include "utils.hpp"
#include "test_framework.hpp"
#include <raft/core/resources.hpp>
#include <cstdio>
#include <vector>

using namespace matrixone;

TEST(UtilsTest, SaveLoadHostMatrix) {
    const std::string filename = "test_host_matrix.modf";
    const int64_t count = 10;
    const int64_t dimension = 4;

    auto matrix = raft::make_host_matrix<float, int64_t>(count, dimension);
    for (int64_t i = 0; i < count * dimension; ++i) {
        matrix.data_handle()[i] = static_cast<float>(i);
    }

    // Save
    ASSERT_NO_THROW(save_host_matrix(filename, matrix.view()));

    // Load
    auto loaded_matrix = load_host_matrix<float>(filename);

    // Verify
    ASSERT_EQ(loaded_matrix.extent(0), count);
    ASSERT_EQ(loaded_matrix.extent(1), dimension);

    for (int64_t i = 0; i < count * dimension; ++i) {
        ASSERT_EQ(loaded_matrix.data_handle()[i], static_cast<float>(i));
    }

    std::remove(filename.c_str());
}

TEST(UtilsTest, SaveLoadDeviceMatrix) {
    raft::resources res;
    const std::string filename = "test_device_matrix.modf";
    const int64_t count = 5;
    const int64_t dimension = 3;

    auto matrix = raft::make_device_matrix<float, int64_t>(res, count, dimension);
    std::vector<float> host_data(count * dimension);
    for (size_t i = 0; i < host_data.size(); ++i) {
        host_data[i] = static_cast<float>(i) * 1.1f;
    }
    raft::copy(matrix.data_handle(), host_data.data(), host_data.size(), raft::resource::get_cuda_stream(res));
    raft::resource::sync_stream(res);

    // Save
    ASSERT_NO_THROW(save_device_matrix(res, filename, matrix.view()));

    // Load
    auto loaded_matrix = load_device_matrix<float>(res, filename);

    // Verify
    ASSERT_EQ(loaded_matrix.extent(0), count);
    ASSERT_EQ(loaded_matrix.extent(1), dimension);

    std::vector<float> loaded_host_data(count * dimension);
    raft::copy(loaded_host_data.data(), loaded_matrix.data_handle(), loaded_host_data.size(), raft::resource::get_cuda_stream(res));
    raft::resource::sync_stream(res);

    for (size_t i = 0; i < host_data.size(); ++i) {
        ASSERT_EQ(loaded_host_data[i], host_data[i]);
    }

    std::remove(filename.c_str());
}

TEST(UtilsTest, SaveLoadDeviceMatrixOverload) {
    raft::resources res;
    const std::string filename = "test_device_matrix_overload.modf";
    const int64_t count = 3;
    const int64_t dimension = 2;

    auto matrix = raft::make_device_matrix<float, int64_t>(res, count, dimension);
    std::vector<float> host_data = {1.0f, 2.0f, 3.0f, 4.0f, 5.0f, 6.0f};
    raft::copy(matrix.data_handle(), host_data.data(), host_data.size(), raft::resource::get_cuda_stream(res));
    raft::resource::sync_stream(res);

    // Save
    save_device_matrix(res, filename, matrix.view());

    // Load using overload
    uint64_t loaded_count = 0;
    uint64_t loaded_dimension = 0;
    // We must initialize device_matrix with some dimensions if we want to declare it, 
    // but the overload will re-assign it. 
    // Actually, the simplest is to just use the returned value or if we must use the overload reference:
    auto loaded_matrix = raft::make_device_matrix<float, int64_t>(res, 0, 0);
    load_device_matrix<float>(res, filename, loaded_matrix, loaded_count, loaded_dimension);

    // Verify
    ASSERT_EQ(loaded_count, (uint64_t)count);
    ASSERT_EQ(loaded_dimension, (uint64_t)dimension);
    ASSERT_EQ(loaded_matrix.extent(0), count);
    ASSERT_EQ(loaded_matrix.extent(1), dimension);

    std::vector<float> loaded_host_data(count * dimension);
    raft::copy(loaded_host_data.data(), loaded_matrix.data_handle(), loaded_host_data.size(), raft::resource::get_cuda_stream(res));
    raft::resource::sync_stream(res);

    for (size_t i = 0; i < host_data.size(); ++i) {
        ASSERT_EQ(loaded_host_data[i], host_data[i]);
    }

    std::remove(filename.c_str());
}

TEST(UtilsTest, LoadWithQuantization) {
    raft::resources res;
    const std::string filename = "test_quantization.modf";
    const int64_t count = 100;
    const int64_t dimension = 8;

    // 1. Create and save float data
    auto matrix = raft::make_device_matrix<float, int64_t>(res, count, dimension);
    std::vector<float> host_data(count * dimension);
    for (size_t i = 0; i < host_data.size(); ++i) {
        // Values between -1.0 and 1.0 to make quantization meaningful
        host_data[i] = static_cast<float>(i % 100) / 50.0f - 1.0f;
    }
    raft::copy(matrix.data_handle(), host_data.data(), host_data.size(), raft::resource::get_cuda_stream(res));
    raft::resource::sync_stream(res);
    save_device_matrix(res, filename, matrix.view());

    // 2. Load as int8_t (should trigger quantization)
    auto quantized_matrix = load_device_matrix<int8_t>(res, filename);

    // 3. Verify metadata
    ASSERT_EQ(quantized_matrix.extent(0), count);
    ASSERT_EQ(quantized_matrix.extent(1), dimension);

    // 4. Basic check that data is loaded
    std::vector<int8_t> result_host(count * dimension);
    raft::copy(result_host.data(), quantized_matrix.data_handle(), result_host.size(), raft::resource::get_cuda_stream(res));
    raft::resource::sync_stream(res);

    // We don't check exact values as quantization is lossy, but it should not be all zeros if input wasn't
    bool non_zero = false;
    for (auto v : result_host) if (v != 0) non_zero = true;
    ASSERT_TRUE(non_zero);

    std::remove(filename.c_str());
}

TEST(UtilsTest, FloatToHalfConversion) {
    raft::resources res;
    const std::string filename = "test_f32_to_f16.modf";
    const int64_t count = 10;
    const int64_t dimension = 4;

    // 1. Save float data
    auto matrix = raft::make_device_matrix<float, int64_t>(res, count, dimension);
    std::vector<float> host_data(count * dimension);
    for (size_t i = 0; i < host_data.size(); ++i) host_data[i] = static_cast<float>(i);
    raft::copy(matrix.data_handle(), host_data.data(), host_data.size(), raft::resource::get_cuda_stream(res));
    save_device_matrix(res, filename, matrix.view());

    // 2. Load as half (should trigger conversion)
    auto half_matrix = load_device_matrix<half>(res, filename);

    // 3. Verify
    ASSERT_EQ(half_matrix.extent(0), count);
    ASSERT_EQ(half_matrix.extent(1), dimension);

    std::vector<half> result_host(count * dimension);
    raft::copy(result_host.data(), half_matrix.data_handle(), result_host.size(), raft::resource::get_cuda_stream(res));
    raft::resource::sync_stream(res);

    for (size_t i = 0; i < host_data.size(); ++i) {
        ASSERT_EQ(static_cast<float>(result_host[i]), host_data[i]);
    }

    std::remove(filename.c_str());
}

TEST(UtilsTest, HalfToUint8Quantization) {
    raft::resources res;
    const std::string filename = "test_f16_to_u8.modf";
    const int64_t count = 100;
    const int64_t dimension = 8;

    // 1. Save half data
    auto matrix = raft::make_host_matrix<half, int64_t>(count, dimension);
    for (size_t i = 0; i < count * dimension; ++i) {
        matrix.data_handle()[i] = static_cast<half>(static_cast<float>(i % 100) / 100.0f);
    }
    save_host_matrix(filename, matrix.view());

    // 2. Load as uint8_t (should trigger quantization from half)
    auto u8_matrix = load_device_matrix<uint8_t>(res, filename);

    // 3. Verify
    ASSERT_EQ(u8_matrix.extent(0), count);
    ASSERT_EQ(u8_matrix.extent(1), dimension);

    std::vector<uint8_t> result_host(count * dimension);
    raft::copy(result_host.data(), u8_matrix.data_handle(), result_host.size(), raft::resource::get_cuda_stream(res));
    raft::resource::sync_stream(res);

    bool non_zero = false;
    for (auto v : result_host) if (v != 0) non_zero = true;
    ASSERT_TRUE(non_zero);

    std::remove(filename.c_str());
}

TEST(UtilsTest, LoadInvalidMagic) {
    const std::string filename = "invalid_magic.modf";
    std::ofstream file(filename, std::ios::binary);
    file.write("NOTM", 4);
    file.close();

    ASSERT_THROW(load_host_matrix<float>(filename), std::runtime_error);

    std::remove(filename.c_str());
}

TEST(UtilsTest, LoadTypeSizeMismatch) {
    const std::string filename = "size_mismatch.modf";
    file_header_t header;
    std::memcpy(header.magic, "MODF", 4);
    header.count = 1;
    header.dimension = 1;
    header.data_type_size = 8; // Double size

    std::ofstream file(filename, std::ios::binary);
    file.write(reinterpret_cast<char*>(&header), sizeof(file_header_t));
    file.close();

    // Try to load as float (size 4) should throw
    ASSERT_THROW(load_host_matrix<float>(filename), std::runtime_error);

    std::remove(filename.c_str());
}
