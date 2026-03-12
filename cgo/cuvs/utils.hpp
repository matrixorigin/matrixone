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

#pragma once

#include <raft/core/device_mdarray.hpp>
#include <raft/core/host_mdarray.hpp>
#include <raft/core/resources.hpp>
#include <raft/core/copy.cuh>
#include <fstream>
#include <string>
#include <vector>
#include <stdexcept>
#include <cstdint>
#include <cstring>

namespace matrixone {

#pragma pack(push, 1)
struct file_header_t {
    char magic[4];              // "MODF"
    uint64_t count;             // 8 bytes
    uint64_t dimension;         // 8 bytes
    uint32_t data_type_size;    // 4 bytes
};
#pragma pack(pop)

/**
 * @brief Reads a binary file into a CUDA device matrix.
 * 
 * File format:
 * header: [4 byte magic = "MODF"][8 byte count][8 byte dimension][4 byte data_type_size]
 * content: flattened vector with total size count * dimension * data_type_size
 * 
 * @tparam T Data type of the elements.
 * @param res RAFT resources handle.
 * @param filename Path to the input file.
 * @return raft::device_matrix<T, int64_t> The loaded device matrix.
 */
template <typename T>
auto load_device_matrix(const raft::resources& res, const std::string& filename) {
    std::ifstream file(filename, std::ios::binary);
    if (!file.is_open()) {
        throw std::runtime_error("Failed to open file: " + filename);
    }

    file_header_t header;
    file.read(reinterpret_cast<char*>(&header), sizeof(file_header_t));
    if (file.gcount() != sizeof(file_header_t)) {
        throw std::runtime_error("Failed to read header from: " + filename);
    }

    if (std::string(header.magic, 4) != "MODF") {
        throw std::runtime_error("Invalid magic number in file: " + filename);
    }

    if (header.data_type_size != sizeof(T)) {
        throw std::runtime_error("Data type size mismatch in file: " + filename + 
                                 " (expected " + std::to_string(sizeof(T)) + 
                                 ", found " + std::to_string(header.data_type_size) + ")");
    }

    auto matrix = raft::make_device_matrix<T, int64_t>(res, static_cast<int64_t>(header.count), static_cast<int64_t>(header.dimension));

    size_t total_elements = header.count * header.dimension;
    if (total_elements > 0) {
        // Read data into host buffer first
        std::vector<T> host_data(total_elements);
        file.read(reinterpret_cast<char*>(host_data.data()), total_elements * sizeof(T));
        if (file.gcount() != static_cast<std::streamsize>(total_elements * sizeof(T))) {
            throw std::runtime_error("Failed to read data content from: " + filename);
        }

        // Copy host buffer to device
        raft::copy(matrix.data_handle(), host_data.data(), total_elements, raft::resource::get_cuda_stream(res));
        raft::resource::sync_stream(res);
    }

    return matrix;
}

/**
 * @brief Reads a binary file into a CUDA device matrix.
 * 
 * @tparam T Data type of the elements.
 * @param res RAFT resources handle.
 * @param filename Path to the input file.
 * @param out_matrix Output device matrix to be populated.
 * @param out_count Output parameter for the number of vectors.
 * @param out_dimension Output parameter for the dimension.
 */
template <typename T>
void load_device_matrix(const raft::resources& res, const std::string& filename, raft::device_matrix<T, int64_t>& out_matrix, uint64_t& out_count, uint64_t& out_dimension) {
    std::ifstream file(filename, std::ios::binary);
    if (!file.is_open()) {
        throw std::runtime_error("Failed to open file: " + filename);
    }

    file_header_t header;
    file.read(reinterpret_cast<char*>(&header), sizeof(file_header_t));
    if (file.gcount() != sizeof(file_header_t)) {
        throw std::runtime_error("Failed to read header from: " + filename);
    }

    if (std::string(header.magic, 4) != "MODF") {
        throw std::runtime_error("Invalid magic number in file: " + filename);
    }

    if (header.data_type_size != sizeof(T)) {
        throw std::runtime_error("Data type size mismatch in file: " + filename + 
                                 " (expected " + std::to_string(sizeof(T)) + 
                                 ", found " + std::to_string(header.data_type_size) + ")");
    }

    out_count = header.count;
    out_dimension = header.dimension;
    out_matrix = raft::make_device_matrix<T, int64_t>(res, static_cast<int64_t>(out_count), static_cast<int64_t>(out_dimension));

    size_t total_elements = out_count * out_dimension;
    if (total_elements > 0) {
        // Read data into host buffer first
        std::vector<T> host_data(total_elements);
        file.read(reinterpret_cast<char*>(host_data.data()), total_elements * sizeof(T));
        if (file.gcount() != static_cast<std::streamsize>(total_elements * sizeof(T))) {
            throw std::runtime_error("Failed to read data content from: " + filename);
        }

        // Copy host buffer to device
        raft::copy(out_matrix.data_handle(), host_data.data(), total_elements, raft::resource::get_cuda_stream(res));
        raft::resource::sync_stream(res);
    }
}

/**
 * @brief Reads a binary file into a CUDA host matrix.
 * 
 * @tparam T Data type of the elements.
 * @param filename Path to the input file.
 * @return raft::host_matrix<T, int64_t> The loaded host matrix.
 */
template <typename T>
auto load_host_matrix(const std::string& filename) {
    std::ifstream file(filename, std::ios::binary);
    if (!file.is_open()) {
        throw std::runtime_error("Failed to open file: " + filename);
    }

    file_header_t header;
    file.read(reinterpret_cast<char*>(&header), sizeof(file_header_t));
    if (file.gcount() != sizeof(file_header_t)) {
        throw std::runtime_error("Failed to read header from: " + filename);
    }

    if (std::string(header.magic, 4) != "MODF") {
        throw std::runtime_error("Invalid magic number in file: " + filename);
    }

    if (header.data_type_size != sizeof(T)) {
        throw std::runtime_error("Data type size mismatch in file: " + filename + 
                                 " (expected " + std::to_string(sizeof(T)) + 
                                 ", found " + std::to_string(header.data_type_size) + ")");
    }

    auto matrix = raft::make_host_matrix<T, int64_t>(static_cast<int64_t>(header.count), static_cast<int64_t>(header.dimension));

    size_t total_elements = header.count * header.dimension;
    if (total_elements > 0) {
        file.read(reinterpret_cast<char*>(matrix.data_handle()), total_elements * sizeof(T));
        if (file.gcount() != static_cast<std::streamsize>(total_elements * sizeof(T))) {
            throw std::runtime_error("Failed to read data content from: " + filename);
        }
    }

    return matrix;
}

/**
 * @brief Reads a binary file into a host vector.
 * 
 * @tparam T Data type of the elements.
 * @param filename Path to the input file.
 * @param out_data Output vector to be populated.
 * @param out_count Output parameter for the number of vectors.
 * @param out_dimension Output parameter for the dimension.
 */
template <typename T>
void load_host_matrix(const std::string& filename, std::vector<T>& out_data, uint64_t& out_count, uint64_t& out_dimension) {
    std::ifstream file(filename, std::ios::binary);
    if (!file.is_open()) {
        throw std::runtime_error("Failed to open file: " + filename);
    }

    file_header_t header;
    file.read(reinterpret_cast<char*>(&header), sizeof(file_header_t));
    if (file.gcount() != sizeof(file_header_t)) {
        throw std::runtime_error("Failed to read header from: " + filename);
    }

    if (std::string(header.magic, 4) != "MODF") {
        throw std::runtime_error("Invalid magic number in file: " + filename);
    }

    if (header.data_type_size != sizeof(T)) {
        throw std::runtime_error("Data type size mismatch in file: " + filename + 
                                 " (expected " + std::to_string(sizeof(T)) + 
                                 ", found " + std::to_string(header.data_type_size) + ")");
    }

    out_count = header.count;
    out_dimension = header.dimension;
    out_data.resize(header.count * header.dimension);

    if (!out_data.empty()) {
        file.read(reinterpret_cast<char*>(out_data.data()), out_data.size() * sizeof(T));
        if (file.gcount() != static_cast<std::streamsize>(out_data.size() * sizeof(T))) {
            throw std::runtime_error("Failed to read data content from: " + filename);
        }
    }
}

/**
 * @brief Saves a CUDA device matrix to a binary file in the "MODF" format.
 */
template <typename T, typename Layout, typename IndexT>
void save_device_matrix(const raft::resources& res, const std::string& filename, 
                        raft::device_matrix_view<T, IndexT, Layout> matrix) {
    std::ofstream file(filename, std::ios::binary);
    if (!file.is_open()) {
        throw std::runtime_error("Failed to open file for writing: " + filename);
    }

    file_header_t header;
    std::memcpy(header.magic, "MODF", 4);
    header.count = static_cast<uint64_t>(matrix.extent(0));
    header.dimension = static_cast<uint64_t>(matrix.extent(1));
    header.data_type_size = sizeof(T);

    file.write(reinterpret_cast<const char*>(&header), sizeof(file_header_t));

    size_t total_elements = header.count * header.dimension;
    if (total_elements > 0) {
        std::vector<std::remove_const_t<T>> host_data(total_elements);
        raft::copy(host_data.data(), matrix.data_handle(), total_elements, raft::resource::get_cuda_stream(res));
        raft::resource::sync_stream(res);
        file.write(reinterpret_cast<const char*>(host_data.data()), total_elements * sizeof(T));
    }
}

/**
 * @brief Saves a host matrix to a binary file in the "MODF" format.
 */
template <typename T, typename Layout, typename IndexT>
void save_host_matrix(const std::string& filename, 
                      raft::host_matrix_view<T, IndexT, Layout> matrix) {
    std::ofstream file(filename, std::ios::binary);
    if (!file.is_open()) {
        throw std::runtime_error("Failed to open file for writing: " + filename);
    }

    file_header_t header;
    std::memcpy(header.magic, "MODF", 4);
    header.count = static_cast<uint64_t>(matrix.extent(0));
    header.dimension = static_cast<uint64_t>(matrix.extent(1));
    header.data_type_size = sizeof(T);

    file.write(reinterpret_cast<const char*>(&header), sizeof(file_header_t));

    size_t total_elements = header.count * header.dimension;
    if (total_elements > 0) {
        file.write(reinterpret_cast<const char*>(matrix.data_handle()), total_elements * sizeof(T));
    }
}

} // namespace matrixone
