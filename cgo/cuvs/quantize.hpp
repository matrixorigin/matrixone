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
#include <raft/linalg/unary_op.cuh>
#include <cuvs/preprocessing/quantize/scalar.hpp>
#include <fstream>
#include <string>
#include <vector>
#include <stdexcept>
#include <cstdint>
#include <cstring>
#include <cmath>
#include <limits>
#include <algorithm>
#include <type_traits>
#include <cuda_fp16.h>

namespace matrixone {

/**
 * @brief Helper to manage cuVS scalar quantizer lifecycle and operations.
 * 
 * @tparam S Source type (float, half, double)
 */
template <typename S>
class scalar_quantizer_t {
public:
    using quantizer_type = cuvs::preprocessing::quantize::scalar::quantizer<S>;

    scalar_quantizer_t() = default;

    /**
     * @brief Constructor that initializes the quantizer with specific min and max values.
     */
    scalar_quantizer_t(S min, S max)
        : quantizer_(std::make_unique<quantizer_type>(quantizer_type{min, max})) {}

    /**
     * @brief Trains the quantizer on a device matrix.
     */
    void train(const raft::resources& res, raft::device_matrix_view<const S, int64_t> train_view) {
        cuvs::preprocessing::quantize::scalar::params q_params;
        quantizer_ = std::make_unique<quantizer_type>(
            cuvs::preprocessing::quantize::scalar::train(res, q_params, train_view));
        raft::resource::sync_stream(res);
    }

    /**
     * @brief Sets the quantizer range manually.
     */
    void set_quantizer(S min, S max) {
        quantizer_ = std::make_unique<quantizer_type>(quantizer_type{min, max});
    }

    /**
     * @brief Transforms a chunk of data into quantized 8-bit integers.
     * 
     * @tparam T Target type (int8_t or uint8_t)
     * @param res RAFT resources handle.
     * @param src_view Source data view on device.
     * @param out_ptr Destination pointer (host or device).
     * @param is_device_ptr Whether out_ptr is in device memory.
     */
    template <typename T>
    void transform(const raft::resources& res, raft::device_matrix_view<const S, int64_t> src_view, T* out_ptr, bool is_device_ptr) {
        if (!quantizer_) throw std::runtime_error("Quantizer not trained");
        static_assert(sizeof(T) == 1, "Quantization target must be 1-byte");

        int64_t n_rows = src_view.extent(0);
        int64_t n_cols = src_view.extent(1);

        if (is_device_ptr) {
            if constexpr (std::is_same_v<T, int8_t>) {
                auto out_view = raft::make_device_matrix_view<int8_t, int64_t>(out_ptr, n_rows, n_cols);
                cuvs::preprocessing::quantize::scalar::transform(res, *quantizer_, src_view, out_view);
            } else {
                // T is uint8_t. cuVS scalar transform only emits int8 [-128,127];
                // map it to uint8 [0,255] with a MONOTONIC +128 shift, NOT a raw
                // cast (raft::copy would value-cast and wrap negatives: -1->255,
                // -128->128, scrambling the L2 ordering for signed/zero-centered
                // data). The shift is L2-invariant — base and query both pass through
                // here, so the constant cancels in (a-b) — so uint8 recall matches int8.
                auto chunk_device_int8 = raft::make_device_matrix<int8_t, int64_t>(res, n_rows, n_cols);
                cuvs::preprocessing::quantize::scalar::transform(res, *quantizer_, src_view, chunk_device_int8.view());
                raft::linalg::unaryOp(
                    out_ptr, chunk_device_int8.data_handle(), n_rows * n_cols,
                    [] __device__(int8_t v) { return static_cast<uint8_t>(static_cast<int>(v) + 128); },
                    raft::resource::get_cuda_stream(res));
            }
        } else {
            // For host pointers, transform into a temporary device int8 buffer first.
            auto tmp_dev = raft::make_device_matrix<int8_t, int64_t>(res, n_rows, n_cols);
            cuvs::preprocessing::quantize::scalar::transform(res, *quantizer_, src_view, tmp_dev.view());
            if constexpr (std::is_same_v<T, uint8_t>) {
                // Monotonic int8->uint8 (+128) on device, then copy to host — see
                // the device path above for why a raw cast is wrong.
                auto tmp_u8 = raft::make_device_matrix<uint8_t, int64_t>(res, n_rows, n_cols);
                raft::linalg::unaryOp(
                    tmp_u8.data_handle(), tmp_dev.data_handle(), n_rows * n_cols,
                    [] __device__(int8_t v) { return static_cast<uint8_t>(static_cast<int>(v) + 128); },
                    raft::resource::get_cuda_stream(res));
                auto out_view = raft::make_host_matrix_view<uint8_t, int64_t>(out_ptr, n_rows, n_cols);
                raft::copy(res, out_view, tmp_u8.view());
            } else {
                auto out_view = raft::make_host_matrix_view<T, int64_t>(out_ptr, n_rows, n_cols);
                raft::copy(res, out_view, tmp_dev.view());
            }
            raft::resource::sync_stream(res);
        }
    }

    /**
     * @brief Host (CPU) equivalent of transform(): quantizes a chunk of
     * SOURCE-typed (S) elements into 1-byte T entirely on the CPU.
     *
     * Scalar quantization is a pure per-element affine map from the trained
     * [min_, max_] range, so once the quantizer is trained no GPU is needed.
     * This is a bit-for-bit port of cuVS' device quantize_op
     * (cuvs/preprocessing/quantize/detail/scalar.cuh): the scale/offset are
     * computed in `double` (the op's default TempT), the inner clamp uses the
     * source-type comparison, ties round via lroundf, and uint8 storage applies
     * the same monotonic +128 shift as the device path. Producing identical
     * bytes to transform() keeps a CPU-built base consistent with a
     * GPU-quantized query at search time.
     *
     * @tparam T Target storage type (int8_t or uint8_t).
     * @param src        Source elements, row-major, n_elements long.
     * @param out        Destination (host), n_elements long.
     * @param n_elements Number of scalar elements (rows * dimension).
     */
    template <typename T>
    void transform_host(const S* src, T* out, size_t n_elements) const {
        if (!quantizer_) throw std::runtime_error("Quantizer not trained");
        static_assert(sizeof(T) == 1, "Quantization target must be 1-byte");

        // cuVS maps the float interval onto the signed range [-128, 127];
        // uint8 is the same int8 result shifted by +128 (see transform()).
        constexpr int q_type_min = std::numeric_limits<int8_t>::min(); // -128
        constexpr int q_type_max = std::numeric_limits<int8_t>::max(); //  127

        const double dmin = static_cast<double>(quantizer_->min_);
        const double dmax = static_cast<double>(quantizer_->max_);
        const double scalar = (dmax > dmin)
            ? (static_cast<double>(q_type_max - q_type_min) / (dmax - dmin))
            : 1.0;
        const double offset = static_cast<double>(q_type_min) - dmin * scalar;

        // fp_lt() compares in the source type's float domain (half is cast to
        // float; float compares natively) — replicate with a float compare.
        const float fmin = static_cast<float>(quantizer_->min_);
        const float fmax = static_cast<float>(quantizer_->max_);

        for (size_t i = 0; i < n_elements; ++i) {
            const float xf = static_cast<float>(src[i]);
            int8_t q;
            if (!(fmin < xf)) {
                q = static_cast<int8_t>(q_type_min);
            } else if (!(xf < fmax)) {
                q = static_cast<int8_t>(q_type_max);
            } else {
                q = static_cast<int8_t>(
                    std::lroundf(static_cast<float>(scalar * static_cast<double>(src[i]) + offset)));
            }
            if constexpr (std::is_same_v<T, uint8_t>) {
                out[i] = static_cast<uint8_t>(static_cast<int>(q) + 128);
            } else {
                out[i] = static_cast<T>(q);
            }
        }
    }

    bool is_trained() const { return quantizer_ != nullptr; }
    void reset() { quantizer_.reset(); }

    /**
     * @brief Gets the minimum value of the quantizer range.
     */
    S min() const {
        if (!quantizer_) throw std::runtime_error("Quantizer not trained");
        return quantizer_->min_;
    }

    /**
     * @brief Gets the maximum value of the quantizer range.
     */
    S max() const {
        if (!quantizer_) throw std::runtime_error("Quantizer not trained");
        return quantizer_->max_;
    }

    /**
     * @brief Serializes the quantizer state to an output stream.
     */
    void serialize(std::ostream& os) const {
        if (!quantizer_) throw std::runtime_error("Quantizer not trained");
        os.write(reinterpret_cast<const char*>(&quantizer_->min_), sizeof(S));
        os.write(reinterpret_cast<const char*>(&quantizer_->max_), sizeof(S));
    }

    /**
     * @brief Deserializes the quantizer state from an input stream.
     */
    void deserialize(std::istream& is) {
        S params[2];
        is.read(reinterpret_cast<char*>(params), 2 * sizeof(S));
        if (is.gcount() != static_cast<std::streamsize>(2 * sizeof(S))) {
            throw std::runtime_error("Failed to read quantizer parameters from stream");
        }
        quantizer_ = std::make_unique<quantizer_type>(quantizer_type{params[0], params[1]});
    }

    /**
     * @brief Saves the quantizer state to a file.
     */
    void save_to_file(const std::string& filename) const {
        std::ofstream os(filename, std::ios::binary);
        if (!os.is_open()) throw std::runtime_error("Failed to open file for writing: " + filename);
        serialize(os);
    }

    /**
     * @brief Loads the quantizer state from a file.
     */
    void load_from_file(const std::string& filename) {
        std::ifstream is(filename, std::ios::binary);
        if (!is.is_open()) throw std::runtime_error("Failed to open file for reading: " + filename);
        deserialize(is);
    }

private:
    std::unique_ptr<quantizer_type> quantizer_;
};

} // namespace matrixone
