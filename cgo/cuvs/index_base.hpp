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

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#pragma GCC diagnostic ignored "-Wmissing-field-initializers"
#include <raft/core/bitset.cuh>
#include <raft/core/copy.cuh>
#include <raft/core/resources.hpp>
#include <raft/core/device_mdspan.hpp>
#include <thrust/fill.h>
#pragma GCC diagnostic pop

#include "cuvs_types.h"
#include "cuvs_worker.hpp"
#include "quantize.hpp"
#include "json.hpp"
#include <cuvs/distance/distance.hpp>
#include <vector>
#include <string>
#include <memory>
#include <shared_mutex>
#include <algorithm>
#include <atomic>
#include <fstream>
#include <unordered_map>
#include <sys/stat.h>
#include <cerrno>

namespace matrixone {

using ::distance_type_t;
using ::quantization_t;
using ::distribution_mode_t;

/**
 * @brief Base class for GPU-based indices.
 */
template <typename T, typename BuildParams, typename IdT = int64_t>
class gpu_index_base_t {
public:
    uint32_t dimension = 0;
    uint32_t count = 0;
    distance_type_t metric;
    BuildParams build_params;
    std::vector<int> devices_;
    std::vector<T> flattened_host_dataset;
    std::vector<IdT> host_ids;
    distribution_mode_t dist_mode;

    std::unique_ptr<cuvs_worker_t> worker;
    mutable std::shared_mutex mutex_;
    bool is_loaded_ = false;
    int build_device_id_ = 0;
    // Use shared_ptr<void> to keep various RAFT resources alive
    std::shared_ptr<void> dataset_device_ptr_; // Keep device memory alive

    // For REPLICATED mode: keep local resources alive for every device
    std::map<int, std::shared_ptr<void>> replicated_indices_;
    std::map<int, std::shared_ptr<void>> replicated_datasets_;

    // Soft-delete bitset: 1 = valid, 0 = deleted (uint32_t matches raft::core::bitset<uint32_t>)
    std::vector<uint32_t> deleted_bitset_;
    uint64_t deleted_count_ = 0;
    std::atomic<uint64_t> bitset_version_{0};

    struct device_bitset_cache_t {
        std::shared_ptr<void> ptr;
        uint64_t version = 0;
        std::mutex mutex;
    };
    // Protects access to the map itself
    std::mutex device_bitsets_mutex_;
    std::map<int, std::shared_ptr<device_bitset_cache_t>> device_deleted_bitsets_;

    // Reverse map from external ID to internal position (populated when host_ids are used)
    std::unordered_map<IdT, uint64_t> id_to_index_;

    gpu_index_base_t() = default;
    virtual ~gpu_index_base_t() {
        destroy();
    }
    
    // Helper to get or create a device-specific bitset cache info
    std::shared_ptr<device_bitset_cache_t> get_device_bitset_info(int dev_id) {
        std::lock_guard<std::mutex> lock(device_bitsets_mutex_);
        auto it = device_deleted_bitsets_.find(dev_id);
        if (it == device_deleted_bitsets_.end()) {
            auto info = std::make_shared<device_bitset_cache_t>();
            device_deleted_bitsets_[dev_id] = info;
            return info;
        }
        return it->second;
    }

    // Helper to sync host bitset to device if stale. Should be called within search.
    void sync_device_bitset(int dev_id, raft::resources const& res) {
        auto info = get_device_bitset_info(dev_id);
        uint64_t current_ver = bitset_version_.load();
        
        if (info->version < current_ver || !info->ptr) {
            std::lock_guard<std::mutex> lock(info->mutex);
            // Double-check after acquiring lock
            if (info->version < current_ver || !info->ptr) {
                // We need a read lock on the main mutex to safely read deleted_bitset_
                std::shared_lock<std::shared_mutex> base_lock(mutex_);
                
                using bs_t = raft::core::bitset<uint32_t, int64_t>;
                auto* bs = new bs_t(res, static_cast<int64_t>(current_offset_));
                uint32_t n_words = static_cast<uint32_t>((current_offset_ + 31) / 32);
                
                if (deleted_bitset_.size() < n_words) {
                    // This shouldn't happen if init/delete are used correctly, but for safety:
                    thrust::fill_n(raft::resource::get_thrust_policy(res), bs->data(), static_cast<int64_t>(n_words), ~0U);
                }
                
                raft::copy(res,
                    raft::make_device_vector_view<uint32_t, int64_t>(bs->data(), static_cast<int64_t>(std::min<size_t>(n_words, deleted_bitset_.size()))),
                    raft::make_host_vector_view<const uint32_t, int64_t>(deleted_bitset_.data(), static_cast<int64_t>(std::min<size_t>(n_words, deleted_bitset_.size()))));
                
                info->ptr = std::shared_ptr<void>(bs, [](void* p){ delete static_cast<bs_t*>(p); });
                info->version = current_ver;
            }
        }
    }

    void set_ids(const IdT* ids, uint64_t count_vectors, uint64_t offset = 0) {
        if (!ids) return;
        if (this->host_ids.size() < offset + count_vectors) {
            this->host_ids.resize(offset + count_vectors);
        }
        std::copy(ids, ids + count_vectors, this->host_ids.begin() + offset);
        for (uint64_t i = 0; i < count_vectors; ++i) {
            this->id_to_index_[ids[i]] = offset + i;
        }
    }

    virtual void start() {}
    virtual void build() {}

    // Common management methods
    virtual void destroy() {
        if (worker) worker->stop();
    }

    void set_per_thread_device(bool enable) {
        if (worker) worker->set_per_thread_device(enable);
    }

    void set_use_batching(bool enable) {
        if (worker) worker->set_use_batching(enable);
    }

    uint32_t cap() const { return count; }
    uint32_t len() const { return static_cast<uint32_t>(current_offset_); }

    void add_chunk(const T* chunk_data, uint64_t chunk_count, int64_t offset = -1, const IdT* ids = nullptr) {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        if (is_loaded_) throw std::runtime_error("Cannot add chunk to built index");

        uint64_t target_offset;
        if (offset == -1) {
            target_offset = current_offset_;
            current_offset_ += static_cast<uint32_t>(chunk_count);
        } else {
            target_offset = static_cast<uint64_t>(offset);
            if (target_offset + chunk_count > current_offset_) {
                current_offset_ = target_offset + chunk_count;
            }
        }
        if (current_offset_ > count) count = current_offset_;

        size_t required_elements = (size_t)current_offset_ * dimension;
        if (flattened_host_dataset.size() < required_elements) {
            flattened_host_dataset.resize(required_elements);
        }

        std::copy(chunk_data, chunk_data + chunk_count * dimension, flattened_host_dataset.begin() + (target_offset * dimension));

        if (ids) {
            if (host_ids.size() < current_offset_) {
                host_ids.resize(current_offset_);
            }
            std::copy(ids, ids + chunk_count, host_ids.begin() + target_offset);
            for (uint64_t i = 0; i < chunk_count; ++i) {
                id_to_index_[ids[i]] = target_offset + i;
            }
        }
    }

    // Initialize (or reset) the deleted bitset after index build.
    // All positions are marked valid (1). Must be called after is_loaded_ = true.
    void init_deleted_bitset() {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        uint64_t n_bits = current_offset_;
        uint64_t n_words = (n_bits + 31) / 32;
        // Only initialize if not already set or if size changed significantly
        if (deleted_bitset_.size() < n_words) {
            std::vector<uint32_t> new_bitset(n_words, ~0U);
            if (!deleted_bitset_.empty()) {
                std::copy(deleted_bitset_.begin(), deleted_bitset_.end(), new_bitset.begin());
            }
            deleted_bitset_ = std::move(new_bitset);
        }
        // Increment version to force GPU syncs if they exist
        bitset_version_.fetch_add(1);
        
        std::lock_guard<std::mutex> ds_lock(device_bitsets_mutex_);
        device_deleted_bitsets_.clear();
    }

    // Soft-delete by external ID (or internal position if no custom IDs).
    void delete_id(IdT id) {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        uint64_t pos;
        if (!host_ids.empty()) {
            auto it = id_to_index_.find(id);
            if (it == id_to_index_.end()) return; // not found
            pos = it->second;
        } else {
            pos = static_cast<uint64_t>(id);
        }
        if (pos >= current_offset_) return;

        // Ensure bitset is large enough (lazy allocation)
        uint64_t n_words = (current_offset_ + 31) / 32;
        if (deleted_bitset_.size() < n_words) {
            deleted_bitset_.resize(n_words, ~0U);
        }

        uint32_t word = static_cast<uint32_t>(pos / 32);
        uint32_t bit  = static_cast<uint32_t>(pos % 32);
        if ((deleted_bitset_[word] >> bit) & 1U) {
            deleted_bitset_[word] &= ~(1U << bit); // clear bit: mark deleted
            ++deleted_count_;
            bitset_version_.fetch_add(1);
        }
    }

    void add_chunk_float(const float* chunk_data, uint64_t chunk_count, int64_t offset = -1, const IdT* ids = nullptr) {
        uint64_t job_id = worker->submit_main(
            [this, chunk_data, chunk_count, offset, ids](raft_handle_wrapper_t& handle) -> std::any {
                auto res = handle.get_raft_resources();
                
                // If quantization is needed (T is 1-byte)
                if constexpr (sizeof(T) == 1) {
                    auto queries_host_view = raft::make_host_matrix_view<const float, int64_t>(chunk_data, chunk_count, dimension);
                    auto queries_device = raft::make_device_matrix<float, int64_t>(*res, chunk_count, dimension);
                    raft::copy(*res, queries_device.view(), queries_host_view);

                    auto chunk_device_target = raft::make_device_matrix<T, int64_t>(*res, chunk_count, dimension);
                    
                    if (!quantizer_.is_trained()) {
                        int64_t n_train = std::min((int64_t)chunk_count, (int64_t)1000);
                        auto train_view = raft::make_device_matrix_view<const float, int64_t>(queries_device.data_handle(), n_train, dimension);
                        quantizer_.train(*res, train_view);
                    }
                    quantizer_.template transform<T>(*res, queries_device.view(), chunk_device_target.data_handle(), true);
                    
                    std::vector<T> chunk_host_target(chunk_count * dimension);
                    raft::copy(*res, raft::make_host_matrix_view<T, int64_t>(chunk_host_target.data(), chunk_count, dimension), chunk_device_target.view());
                    handle.sync();

                    std::unique_lock<std::shared_mutex> lock(mutex_);
                    uint64_t target_offset;
                    if (offset == -1) {
                        target_offset = current_offset_;
                        current_offset_ += static_cast<uint32_t>(chunk_count);
                    } else {
                        target_offset = static_cast<uint64_t>(offset);
                        if (target_offset + chunk_count > current_offset_) {
                            current_offset_ = target_offset + chunk_count;
                        }
                    }
                    if (current_offset_ > count) count = current_offset_;

                    size_t required_elements = (size_t)current_offset_ * dimension;
                    if (flattened_host_dataset.size() < required_elements) {
                        flattened_host_dataset.resize(required_elements);
                    }
                    std::copy(chunk_host_target.begin(), chunk_host_target.end(), flattened_host_dataset.begin() + (target_offset * dimension));
                    
                    if (ids) {
                        if (host_ids.size() < current_offset_) {
                            host_ids.resize(current_offset_);
                        }
                        std::copy(ids, ids + chunk_count, host_ids.begin() + target_offset);
                    }
                } else {
                    std::unique_lock<std::shared_mutex> lock(mutex_);
                    uint64_t target_offset;
                    if (offset == -1) {
                        target_offset = current_offset_;
                        current_offset_ += static_cast<uint32_t>(chunk_count);
                    } else {
                        target_offset = static_cast<uint64_t>(offset);
                        if (target_offset + chunk_count > current_offset_) {
                            current_offset_ = target_offset + chunk_count;
                        }
                    }
                    if (current_offset_ > count) count = current_offset_;

                    size_t required_elements = (size_t)current_offset_ * dimension;
                    if (flattened_host_dataset.size() < required_elements) {
                        flattened_host_dataset.resize(required_elements);
                    }
                    std::copy(chunk_data, chunk_data + chunk_count * dimension, flattened_host_dataset.begin() + (target_offset * dimension));
                    
                    if (ids) {
                        this->set_ids(ids, chunk_count, target_offset);
                    }
                }
                return std::any();
            }
        );
        worker->wait(job_id).get();
    }

    void train_quantizer(const float* train_data, uint64_t n_samples) {
        uint64_t job_id = worker->submit_main(
            [this, train_data, n_samples](raft_handle_wrapper_t& handle) -> std::any {
                auto res = handle.get_raft_resources();
                auto train_host_view = raft::make_host_matrix_view<const float, int64_t>(train_data, n_samples, dimension);
                auto train_device = raft::make_device_matrix<float, int64_t>(*res, n_samples, dimension);
                raft::copy(*res, train_device.view(), train_host_view);
                quantizer_.train(*res, train_device.view());
                handle.sync();
                return std::any();
            }
        );
        worker->wait(job_id).get();
    }

    void train_quantizer_if_needed() {
        if constexpr (sizeof(T) == 1) {
            if (!quantizer_.is_trained() && !flattened_host_dataset.empty()) {
                uint64_t n_train = std::min(static_cast<uint64_t>(500), static_cast<uint64_t>(count));
                if (n_train == 0) return;
                std::vector<float> train_data(n_train * dimension);
                for (size_t i = 0; i < n_train * dimension; ++i) {
                    train_data[i] = static_cast<float>(flattened_host_dataset[i]);
                }
                train_quantizer(train_data.data(), n_train);
            }
        }
    }

    void set_quantizer(float min, float max) {
        quantizer_.set_quantizer(min, max);
    }

    void get_quantizer(float* min, float* max) {
        *min = quantizer_.min();
        *max = quantizer_.max();
    }

    const IdT* get_host_ids() const {
        return host_ids.empty() ? nullptr : host_ids.data();
    }

    void save_ids(const std::string& filename) const {
        std::ofstream os(filename, std::ios::binary);
        if (!os) throw std::runtime_error("Failed to open file for saving IDs: " + filename);
        uint64_t size = host_ids.size();
        os.write(reinterpret_cast<const char*>(&size), sizeof(size));
        if (size > 0) {
            os.write(reinterpret_cast<const char*>(host_ids.data()), size * sizeof(IdT));
        }
    }

    void load_ids(const std::string& filename) {
        std::ifstream is(filename, std::ios::binary);
        if (!is) throw std::runtime_error("Failed to open file for loading IDs: " + filename);
        uint64_t size;
        is.read(reinterpret_cast<char*>(&size), sizeof(size));
        this->host_ids.clear();
        this->id_to_index_.clear();
        if (size > 0) {
            std::vector<IdT> temp_ids(size);
            is.read(reinterpret_cast<char*>(temp_ids.data()), size * sizeof(IdT));
            this->set_ids(temp_ids.data(), size);
        }
    }

    // Returns a string name for the template element type T.
    std::string element_type_name() const {
        if constexpr (std::is_same_v<T, float>) return "float32";
        else if constexpr (sizeof(T) == 2)      return "float16";
        else if constexpr (std::is_same_v<T, int8_t>) return "int8";
        else return "uint8";
    }

    // Creates a directory and all its parents. Ignores EEXIST at each level.
    static void ensure_dir(const std::string& dir) {
        for (size_t pos = 1; pos <= dir.size(); ++pos) {
            if (pos == dir.size() || dir[pos] == '/') {
                std::string partial = dir.substr(0, pos);
                if (partial.empty() || partial == ".") continue;
                if (::mkdir(partial.c_str(), 0755) != 0 && errno != EEXIST) {
                    throw std::runtime_error("Failed to create directory: " + partial +
                                             " (errno=" + std::to_string(errno) + ")");
                }
            }
        }
    }

    // Writes the soft-delete bitset to {dir}/bitset.bin.
    // Format: [uint64 n_bits][uint64 n_words][uint64 deleted_count][uint32 words...]
    void save_bitset(const std::string& dir) const {
        std::string filename = dir + "/bitset.bin";
        std::ofstream os(filename, std::ios::binary);
        if (!os) throw std::runtime_error("Failed to open bitset file for writing: " + filename);
        uint64_t n_bits  = current_offset_;
        uint64_t n_words = deleted_bitset_.size();
        os.write(reinterpret_cast<const char*>(&n_bits),       sizeof(n_bits));
        os.write(reinterpret_cast<const char*>(&n_words),      sizeof(n_words));
        os.write(reinterpret_cast<const char*>(&deleted_count_), sizeof(deleted_count_));
        if (n_words > 0) {
            os.write(reinterpret_cast<const char*>(deleted_bitset_.data()),
                     n_words * sizeof(uint32_t));
        }
    }

    // Restores the soft-delete bitset from a file written by save_bitset().
    void load_bitset_from_file(const std::string& filename) {
        std::ifstream is(filename, std::ios::binary);
        if (!is) throw std::runtime_error("Failed to open bitset file for reading: " + filename);
        uint64_t n_bits = 0, n_words = 0;
        is.read(reinterpret_cast<char*>(&n_bits),       sizeof(n_bits));
        is.read(reinterpret_cast<char*>(&n_words),      sizeof(n_words));
        is.read(reinterpret_cast<char*>(&deleted_count_), sizeof(deleted_count_));
        deleted_bitset_.resize(n_words);
        if (n_words > 0) {
            is.read(reinterpret_cast<char*>(deleted_bitset_.data()),
                    n_words * sizeof(uint32_t));
        }
        bitset_version_.fetch_add(1);
        std::lock_guard<std::mutex> ds_lock(device_bitsets_mutex_);
        device_deleted_bitsets_.clear();
    }

    // -------------------------------------------------------------------------
    // Manifest helpers — shared by save_dir / load_dir in all derived classes
    // -------------------------------------------------------------------------

    struct manifest_data_t {
        std::string raw;          // full manifest.json content
        std::string comp_json;    // "components" sub-object
        bool has_ids       = false;
        bool has_quantizer = false;
        bool has_bitset    = false;
    };

    // Saves ids, quantizer, and bitset (when present) to dir.
    // Returns comp_entry strings for each saved file.
    std::vector<std::string> save_common_components(const std::string& dir) const {
        bool has_ids       = !this->host_ids.empty();
        bool has_quantizer = this->quantizer_.is_trained();
        bool has_bitset    = !this->deleted_bitset_.empty();

        if (has_ids)       this->save_ids(dir + "/ids.bin");
        if (has_quantizer) this->quantizer_.save_to_file(dir + "/quantizer.bin");
        if (has_bitset)    this->save_bitset(dir);

        std::vector<std::string> entries;
        if (has_ids)       entries.push_back("    \"ids\": \"ids.bin\"");
        if (has_quantizer) entries.push_back("    \"quantizer\": \"quantizer.bin\"");
        if (has_bitset)    entries.push_back("    \"bitset\": \"bitset.bin\"");
        return entries;
    }

    // Returns the JSON component entry for sharded index files.
    std::string shards_comp_entry() const {
        std::string s = "    \"shards\": [";
        for (int i = 0; i < static_cast<int>(this->devices_.size()); ++i) {
            s += "\"shard_" + std::to_string(i) + ".bin\"";
            if (i + 1 < static_cast<int>(this->devices_.size())) s += ", ";
        }
        s += "]";
        return s;
    }

    // Writes manifest.json to dir.
    // build_params_json: inner key:value lines for the "build_params" object.
    // comp_entries: per-component JSON lines for the "components" object.
    void write_manifest(const std::string& dir, const std::string& index_type,
                        const std::string& build_params_json,
                        const std::vector<std::string>& comp_entries) const {
        bool has_ids       = !this->host_ids.empty();
        bool has_quantizer = this->quantizer_.is_trained();
        bool has_bitset    = !this->deleted_bitset_.empty();

        std::ofstream mf(dir + "/manifest.json");
        if (!mf) throw std::runtime_error("Failed to create manifest.json in: " + dir);

        mf << "{\n";
        mf << "  \"schema_version\": 1,\n";
        mf << "  \"index_type\": \""    << index_type                    << "\",\n";
        mf << "  \"element_type\": \""  << this->element_type_name()     << "\",\n";
        mf << "  \"dimension\": "       << this->dimension               << ",\n";
        mf << "  \"metric\": "          << static_cast<int>(this->metric) << ",\n";
        mf << "  \"dist_mode\": "       << static_cast<int>(this->dist_mode) << ",\n";
        mf << "  \"capacity\": "        << this->count                   << ",\n";
        mf << "  \"length\": "          << this->current_offset_         << ",\n";
        mf << "  \"has_ids\": "         << (has_ids       ? "true" : "false") << ",\n";
        mf << "  \"has_quantizer\": "   << (has_quantizer ? "true" : "false") << ",\n";
        mf << "  \"has_bitset\": "      << (has_bitset    ? "true" : "false") << ",\n";
        mf << "  \"deleted_count\": "   << this->deleted_count_          << ",\n";
        mf << "  \"bitset_version\": "  << this->bitset_version_.load()  << ",\n";
        mf << "  \"devices\": [";
        for (size_t i = 0; i < this->devices_.size(); ++i) {
            mf << this->devices_[i];
            if (i + 1 < this->devices_.size()) mf << ", ";
        }
        mf << "],\n";
        mf << "  \"build_params\": {\n" << build_params_json << "\n  },\n";
        mf << "  \"components\": {\n";
        for (size_t i = 0; i < comp_entries.size(); ++i) {
            mf << comp_entries[i];
            if (i + 1 < comp_entries.size()) mf << ",";
            mf << "\n";
        }
        mf << "  }\n}\n";
    }

    // Reads manifest.json from dir, validates schema and index_type,
    // restores common index fields, and returns parsed manifest data.
    manifest_data_t read_manifest(const std::string& dir, const std::string& expected_type) {
        std::ifstream mf(dir + "/manifest.json");
        if (!mf) throw std::runtime_error("Failed to open manifest.json in: " + dir);
        std::string raw((std::istreambuf_iterator<char>(mf)),
                         std::istreambuf_iterator<char>());

        int64_t schema_ver = json_int(raw, "schema_version");
        if (schema_ver != 1)
            throw std::runtime_error("Unsupported manifest schema_version: " +
                                     std::to_string(schema_ver));
        std::string idx_type = json_value(raw, "index_type");
        if (idx_type != expected_type)
            throw std::runtime_error("manifest index_type is '" + idx_type +
                                     "', expected '" + expected_type + "'");

        this->dimension       = static_cast<uint32_t>(json_int(raw, "dimension"));
        this->count           = static_cast<uint32_t>(json_int(raw, "capacity"));
        this->current_offset_ = static_cast<uint64_t>(json_int(raw, "length"));
        this->metric          = static_cast<distance_type_t>(json_int(raw, "metric"));
        this->dist_mode       = static_cast<distribution_mode_t>(json_int(raw, "dist_mode"));
        this->deleted_count_  = static_cast<uint64_t>(json_int(raw, "deleted_count"));

        manifest_data_t m;
        m.raw           = raw;
        m.comp_json     = json_object(raw, "components");
        m.has_ids       = json_bool(raw, "has_ids");
        m.has_quantizer = json_bool(raw, "has_quantizer");
        m.has_bitset    = json_bool(raw, "has_bitset");
        return m;
    }

    // Loads ids, quantizer, and bitset from dir using the parsed manifest data.
    void load_common_components(const std::string& dir, const manifest_data_t& m) {
        if (m.has_ids) {
            this->load_ids(dir + "/" + json_value(m.comp_json, "ids"));
        }
        if (m.has_quantizer) {
            this->quantizer_.load_from_file(dir + "/" + json_value(m.comp_json, "quantizer"));
        }
        if (m.has_bitset) {
            this->load_bitset_from_file(dir + "/" + json_value(m.comp_json, "bitset"));
        }
    }

    virtual std::string info() const {
        std::string json = "{";
        json += "\"element_size\": " + std::to_string(sizeof(T)) + ", ";
        json += "\"dimension\": " + std::to_string(dimension) + ", ";
        json += "\"metric\": " + std::to_string((int)metric) + ", ";
        json += "\"status\": \"" + std::string(is_loaded_ ? "Loaded" : "Empty") + "\", ";
        json += "\"capacity\": " + std::to_string(count) + ", ";
        json += "\"current_length\": " + std::to_string(current_offset_) + ", ";
        json += "\"dist_mode\": " + std::to_string((int)dist_mode) + ", ";
        json += "\"has_ids\": " + std::string(host_ids.empty() ? "false" : "true") + ", ";
        json += "\"devices\": [";
        for (size_t i = 0; i < devices_.size(); ++i) {
            json += std::to_string(devices_[i]) + (i == devices_.size() - 1 ? "" : ", ");
        }
        json += "]";
        return json; // Caller will close the object or add more fields
    }

protected:
    scalar_quantizer_t<float> quantizer_;
    uint64_t current_offset_ = 0;
    // Serializes concurrent extend() calls. Held across GPU work and count update so that
    // set_ids() offsets always match the GPU execution order. Does NOT block searches.
    std::mutex extend_mutex_;
};

} // namespace matrixone
