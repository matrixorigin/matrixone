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
#include <cuvs/preprocessing/quantize/scalar.hpp>
#include <fstream>
#include <string>
#include <vector>
#include <stdexcept>
#include <cstdint>
#include <cstring>
#include <algorithm>
#include <type_traits>
#include <cuda_fp16.h>

namespace matrixone {

#pragma pack(push, 1)
struct file_header_t {
    char magic[4];              // "MODF"
    uint64_t count;             // 8 bytes
    uint64_t dimension;         // 8 bytes
    uint32_t data_type_size;    // 4 bytes
};
#pragma pack(pop)

namespace detail {

static constexpr int64_t DEFAULT_CHUNK_SIZE = 16384;

/**
 * @brief Internal helper to perform chunked quantization or conversion from datafile to a raw pointer.
 * 
 * @tparam S Source type (type in file)
 * @tparam T Target type (requested type)
 * @tparam DoQuantize Whether to use cuVS scalar quantization (only for target size 1)
 * @param res RAFT resources handle.
 * @param filename Path to the input file.
 * @param header Parsed file header.
 * @param out_ptr Destination pointer (can be host or device memory).
 * @param is_device_ptr Whether the destination pointer is in device memory.
 */
template <typename S, typename T, bool DoQuantize>
void load_matrix_chunked_ptr(const raft::resources& res, const std::string& filename, const file_header_t& header, T* out_ptr, bool is_device_ptr) {
    int64_t n_rows = static_cast<int64_t>(header.count);
    int64_t n_cols = static_cast<int64_t>(header.dimension);
    
    if (n_rows == 0 || n_cols == 0) return;

    std::ifstream file(filename, std::ios::binary);
    file.seekg(sizeof(file_header_t));

    // 1. If quantization requested, train quantizer on subset (up to 500 samples)
    std::unique_ptr<cuvs::preprocessing::quantize::scalar::quantizer<S>> quantizer_ptr;

    if constexpr (DoQuantize) {
        int64_t n_train = std::min(n_rows, static_cast<int64_t>(500));
        std::vector<S> train_host(n_train * n_cols);
        file.read(reinterpret_cast<char*>(train_host.data()), train_host.size() * sizeof(S));
        
        auto train_device = raft::make_device_matrix<S, int64_t>(res, n_train, n_cols);
        raft::copy(train_device.data_handle(), train_host.data(), train_host.size(), raft::resource::get_cuda_stream(res));
        
        cuvs::preprocessing::quantize::scalar::params q_params;
        auto train_view = raft::make_device_matrix_view<const S, int64_t>(train_device.data_handle(), n_train, n_cols);
        quantizer_ptr = std::make_unique<cuvs::preprocessing::quantize::scalar::quantizer<S>>(
            cuvs::preprocessing::quantize::scalar::train(res, q_params, train_view));
        file.seekg(sizeof(file_header_t)); // Reset to beginning of data
    }

    // 2. Transform in chunks
    std::vector<S> chunk_host;
    auto chunk_device_src = raft::make_device_matrix<S, int64_t>(res, DEFAULT_CHUNK_SIZE, n_cols);
    
    std::unique_ptr<raft::device_matrix<int8_t, int64_t>> chunk_device_int8;
    if constexpr (DoQuantize) {
        chunk_device_int8 = std::make_unique<raft::device_matrix<int8_t, int64_t>>(
            raft::make_device_matrix<int8_t, int64_t>(res, DEFAULT_CHUNK_SIZE, n_cols));
    }

    for (int64_t row_offset = 0; row_offset < n_rows; row_offset += DEFAULT_CHUNK_SIZE) {
        int64_t current_chunk_rows = std::min(DEFAULT_CHUNK_SIZE, n_rows - row_offset);
        size_t total_chunk_elements = current_chunk_rows * n_cols;
        
        chunk_host.resize(total_chunk_elements);
        file.read(reinterpret_cast<char*>(chunk_host.data()), total_chunk_elements * sizeof(S));
        
        raft::copy(chunk_device_src.data_handle(), chunk_host.data(), total_chunk_elements, raft::resource::get_cuda_stream(res));
        
        auto current_chunk_src_view = raft::make_device_matrix_view<const S, int64_t>(
            chunk_device_src.data_handle(), current_chunk_rows, n_cols);

        if constexpr (DoQuantize) {
            auto current_chunk_int8_view = raft::make_device_matrix_view<int8_t, int64_t>(
                chunk_device_int8->data_handle(), current_chunk_rows, n_cols);

            cuvs::preprocessing::quantize::scalar::transform(res, *quantizer_ptr, current_chunk_src_view, current_chunk_int8_view);
            
            if (is_device_ptr) {
                auto out_chunk_view = raft::make_device_matrix_view<T, int64_t>(out_ptr + (row_offset * n_cols), current_chunk_rows, n_cols);
                raft::copy(res, out_chunk_view, current_chunk_int8_view);
            } else {
                auto out_chunk_view = raft::make_host_matrix_view<T, int64_t>(out_ptr + (row_offset * n_cols), current_chunk_rows, n_cols);
                raft::copy(res, out_chunk_view, current_chunk_int8_view);
            }
        } else {
            if (is_device_ptr) {
                auto out_chunk_view = raft::make_device_matrix_view<T, int64_t>(out_ptr + (row_offset * n_cols), current_chunk_rows, n_cols);
                raft::copy(res, out_chunk_view, current_chunk_src_view);
            } else {
                auto out_chunk_view = raft::make_host_matrix_view<T, int64_t>(out_ptr + (row_offset * n_cols), current_chunk_rows, n_cols);
                raft::copy(res, out_chunk_view, current_chunk_src_view);
            }
        }
    }
    raft::resource::sync_stream(res);
}

/**
 * @brief Internal helper to read a binary file into a raw pointer using chunking.
 */
template <typename S>
void load_matrix_raw_ptr(const raft::resources& res, const std::string& filename, const file_header_t& header, S* out_ptr, bool is_device_ptr) {
    int64_t n_rows = static_cast<int64_t>(header.count);
    int64_t n_cols = static_cast<int64_t>(header.dimension);

    if (n_rows == 0 || n_cols == 0) return;

    std::ifstream file(filename, std::ios::binary);
    file.seekg(sizeof(file_header_t));

    if (!is_device_ptr) {
        // Direct read into host memory
        file.read(reinterpret_cast<char*>(out_ptr), n_rows * n_cols * sizeof(S));
        if (file.gcount() != static_cast<std::streamsize>(n_rows * n_cols * sizeof(S))) {
            throw std::runtime_error("Failed to read data content from: " + filename);
        }
    } else {
        // Chunked read and copy to device
        std::vector<S> chunk_host;
        for (int64_t row_offset = 0; row_offset < n_rows; row_offset += DEFAULT_CHUNK_SIZE) {
            int64_t current_chunk_rows = std::min(DEFAULT_CHUNK_SIZE, n_rows - row_offset);
            size_t total_chunk_elements = current_chunk_rows * n_cols;
            
            chunk_host.resize(total_chunk_elements);
            file.read(reinterpret_cast<char*>(chunk_host.data()), total_chunk_elements * sizeof(S));
            if (file.gcount() != static_cast<std::streamsize>(total_chunk_elements * sizeof(S))) {
                throw std::runtime_error("Failed to read data content from: " + filename);
            }

            raft::copy(out_ptr + (row_offset * n_cols), chunk_host.data(), total_chunk_elements, raft::resource::get_cuda_stream(res));
        }
        raft::resource::sync_stream(res);
    }
}

} // namespace detail

/**
 * @brief Reads a binary file into a CUDA device matrix.
 */
template <typename T>
auto load_device_matrix(const raft::resources& res, const std::string& filename) {
    std::ifstream file(filename, std::ios::binary);
    if (!file.is_open()) throw std::runtime_error("Failed to open file: " + filename);

    file_header_t header;
    file.read(reinterpret_cast<char*>(&header), sizeof(file_header_t));
    if (std::string(header.magic, 4) != "MODF") throw std::runtime_error("Invalid magic: " + filename);

    auto matrix = raft::make_device_matrix<T, int64_t>(res, static_cast<int64_t>(header.count), static_cast<int64_t>(header.dimension));
    if (header.data_type_size == sizeof(T)) {
        detail::load_matrix_raw_ptr<T>(res, filename, header, matrix.data_handle(), true);
    } else if (header.data_type_size == 4) {
        if constexpr (sizeof(T) == 2) {
            detail::load_matrix_chunked_ptr<float, T, false>(res, filename, header, matrix.data_handle(), true);
        } else if constexpr (sizeof(T) == 1) {
            detail::load_matrix_chunked_ptr<float, T, true>(res, filename, header, matrix.data_handle(), true);
        } else {
            throw std::runtime_error("Unsupported conversion from float to requested size");
        }
    } else if (header.data_type_size == 2) {
        if constexpr (sizeof(T) == 1) {
            detail::load_matrix_chunked_ptr<half, T, true>(res, filename, header, matrix.data_handle(), true);
        } else if constexpr (sizeof(T) == 4) {
            detail::load_matrix_chunked_ptr<half, T, false>(res, filename, header, matrix.data_handle(), true);
        } else {
            throw std::runtime_error("Unsupported conversion from half to requested size");
        }
    } else {
        throw std::runtime_error("Type size mismatch and conversion not supported for source size: " + std::to_string(header.data_type_size));
    }
    return matrix;
}

/**
 * @brief Reads a binary file into a CUDA device matrix (overload).
 */
template <typename T>
void load_device_matrix(const raft::resources& res, const std::string& filename, raft::device_matrix<T, int64_t>& out_matrix, uint64_t& out_count, uint64_t& out_dimension) {
    out_matrix = load_device_matrix<T>(res, filename);
    out_count = static_cast<uint64_t>(out_matrix.extent(0));
    out_dimension = static_cast<uint64_t>(out_matrix.extent(1));
}

/**
 * @brief Reads a binary file into a CUDA host matrix.
 */
template <typename T>
auto load_host_matrix(const std::string& filename) {
    raft::resources res; 
    std::ifstream file(filename, std::ios::binary);
    if (!file.is_open()) throw std::runtime_error("Failed to open file: " + filename);

    file_header_t header;
    file.read(reinterpret_cast<char*>(&header), sizeof(file_header_t));
    if (std::string(header.magic, 4) != "MODF") throw std::runtime_error("Invalid magic: " + filename);

    auto matrix = raft::make_host_matrix<T, int64_t>(static_cast<int64_t>(header.count), static_cast<int64_t>(header.dimension));
    if (header.data_type_size == sizeof(T)) {
        detail::load_matrix_raw_ptr<T>(res, filename, header, matrix.data_handle(), false);
    } else {
        if (header.data_type_size == 4) {
            if constexpr (sizeof(T) == 2) {
                detail::load_matrix_chunked_ptr<float, T, false>(res, filename, header, matrix.data_handle(), false);
            } else if constexpr (sizeof(T) == 1) {
                detail::load_matrix_chunked_ptr<float, T, true>(res, filename, header, matrix.data_handle(), false);
            } else {
                throw std::runtime_error("Unsupported conversion from float to requested size");
            }
        } else if (header.data_type_size == 2) {
            if constexpr (sizeof(T) == 1) {
                detail::load_matrix_chunked_ptr<half, T, true>(res, filename, header, matrix.data_handle(), false);
            } else if constexpr (sizeof(T) == 4) {
                detail::load_matrix_chunked_ptr<half, T, false>(res, filename, header, matrix.data_handle(), false);
            } else {
                throw std::runtime_error("Unsupported conversion from half to requested size");
            }
        } else {
            throw std::runtime_error("Unsupported conversion for host matrix");
        }
    }
    return matrix;
}

/**
 * @brief Reads a binary file into a host vector.
 */
template <typename T>
void load_host_matrix(const std::string& filename, std::vector<T>& out_data, uint64_t& out_count, uint64_t& out_dimension) {
    raft::resources res; 
    std::ifstream file(filename, std::ios::binary);
    if (!file.is_open()) throw std::runtime_error("Failed to open file: " + filename);

    file_header_t header;
    file.read(reinterpret_cast<char*>(&header), sizeof(file_header_t));
    if (std::string(header.magic, 4) != "MODF") throw std::runtime_error("Invalid magic: " + filename);

    out_count = header.count;
    out_dimension = header.dimension;
    out_data.resize(out_count * out_dimension);

    if (header.data_type_size == sizeof(T)) {
        detail::load_matrix_raw_ptr<T>(res, filename, header, out_data.data(), false);
    } else {
        if (header.data_type_size == 4) {
            if constexpr (sizeof(T) == 2) {
                detail::load_matrix_chunked_ptr<float, T, false>(res, filename, header, out_data.data(), false);
            } else if constexpr (sizeof(T) == 1) {
                detail::load_matrix_chunked_ptr<float, T, true>(res, filename, header, out_data.data(), false);
            } else {
                throw std::runtime_error("Unsupported conversion from float to requested size");
            }
        } else if (header.data_type_size == 2) {
            if constexpr (sizeof(T) == 1) {
                detail::load_matrix_chunked_ptr<half, T, true>(res, filename, header, out_data.data(), false);
            } else if constexpr (sizeof(T) == 4) {
                detail::load_matrix_chunked_ptr<half, T, false>(res, filename, header, out_data.data(), false);
            } else {
                throw std::runtime_error("Unsupported conversion from half to requested size");
            }
        } else {
            throw std::runtime_error("Unsupported conversion for host matrix");
        }
    }
}

/**
 * @brief Saves a CUDA device matrix to a binary file in the "MODF" format using chunking.
 */
template <typename T, typename Layout, typename IndexT>
void save_device_matrix(const raft::resources& res, const std::string& filename, 
                        raft::device_matrix_view<T, IndexT, Layout> matrix) {
    std::ofstream file(filename, std::ios::binary);
    if (!file.is_open()) throw std::runtime_error("Failed to open file for writing: " + filename);

    file_header_t header;
    std::memcpy(header.magic, "MODF", 4);
    header.count = static_cast<uint64_t>(matrix.extent(0));
    header.dimension = static_cast<uint64_t>(matrix.extent(1));
    header.data_type_size = sizeof(std::remove_const_t<T>);
    file.write(reinterpret_cast<const char*>(&header), sizeof(file_header_t));

    int64_t n_rows = static_cast<int64_t>(header.count);
    int64_t n_cols = static_cast<int64_t>(header.dimension);
    std::vector<std::remove_const_t<T>> chunk_host;

    for (int64_t row_offset = 0; row_offset < n_rows; row_offset += detail::DEFAULT_CHUNK_SIZE) {
        int64_t current_chunk_rows = std::min(detail::DEFAULT_CHUNK_SIZE, n_rows - row_offset);
        size_t total_chunk_elements = current_chunk_rows * n_cols;
        chunk_host.resize(total_chunk_elements);
        
        auto src_chunk_view = raft::make_device_matrix_view<const T, int64_t>(matrix.data_handle() + (row_offset * n_cols), current_chunk_rows, n_cols);
        auto host_chunk_view = raft::make_host_matrix_view<std::remove_const_t<T>, int64_t>(chunk_host.data(), current_chunk_rows, n_cols);
        
        raft::copy(res, host_chunk_view, src_chunk_view);
        raft::resource::sync_stream(res);
        file.write(reinterpret_cast<const char*>(chunk_host.data()), total_chunk_elements * sizeof(std::remove_const_t<T>));
    }
}

/**
 * @brief Saves a host matrix to a binary file in the "MODF" format.
 */
template <typename T, typename Layout, typename IndexT>
void save_host_matrix(const std::string& filename, 
                      raft::host_matrix_view<T, IndexT, Layout> matrix) {
    std::ofstream file(filename, std::ios::binary);
    if (!file.is_open()) throw std::runtime_error("Failed to open file for writing: " + filename);

    file_header_t header;
    std::memcpy(header.magic, "MODF", 4);
    header.count = static_cast<uint64_t>(matrix.extent(0));
    header.dimension = static_cast<uint64_t>(matrix.extent(1));
    header.data_type_size = sizeof(std::remove_const_t<T>);
    file.write(reinterpret_cast<const char*>(&header), sizeof(file_header_t));

    if (matrix.size() > 0) {
        file.write(reinterpret_cast<const char*>(matrix.data_handle()), matrix.size() * sizeof(std::remove_const_t<T>));
    }
}

} // namespace matrixone
