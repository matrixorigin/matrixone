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

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <stdexcept>
#include <string>
#include <vector>

#ifdef _OPENMP
#include <omp.h>
#endif

namespace matrixone {

// =============================================================================
// Filter data + CPU predicate evaluation for pre-filtered vector search.
//
// This header is standalone — no raft/cuvs/cudf dependencies. It owns:
//   * FilterStore                — host-resident columnar storage for INCLUDE columns
//   * Predicate types + parser   — PredOp / PredOpType + parse_preds(json)
//   * eval_filter_bitmap_cpu     — OpenMP word-at-a-time bitmap producer
//
// The output bitmap layout is identical to raft::core::bitset<uint32_t, int64_t>:
// one uint32_t word per 32 rows, LSB-first. A bit value of 1 means the row
// passes all predicates (i.e. is "alive").  Callers AND this with the delete
// bitmap downstream.
// =============================================================================

// -----------------------------------------------------------------------------
// Column metadata
// -----------------------------------------------------------------------------

enum class FilterColType : uint8_t {
    INT32   = 0,
    INT64   = 1,
    FLOAT32 = 2,
    FLOAT64 = 3,
    UINT64  = 4,  // VARCHAR stored as 64-bit hash
};

inline uint32_t filter_col_elem_size(FilterColType t) {
    switch (t) {
        case FilterColType::INT32:   return 4;
        case FilterColType::INT64:   return 8;
        case FilterColType::FLOAT32: return 4;
        case FilterColType::FLOAT64: return 8;
        case FilterColType::UINT64:  return 8;
    }
    throw std::invalid_argument("unknown FilterColType");
}

struct FilterColMeta {
    std::string   name;
    FilterColType type;
    uint32_t      elem_size;  // derived from type; cached for hot path
};

// -----------------------------------------------------------------------------
// FilterStore — host-resident columnar buffer
//
// One flat byte buffer per column, row-major (count * elem_size bytes).
// Lifetime: populated by set_filter_columns() + add_chunk() before build,
// retained for the lifetime of the index, read at search time.
// -----------------------------------------------------------------------------

struct FilterStore {
    std::vector<FilterColMeta>        columns;
    std::vector<std::vector<uint8_t>> data;  // data[c] has count*elem_size bytes
    uint64_t count    = 0;                   // rows currently populated
    uint64_t capacity = 0;                   // rows pre-allocated

    void init(std::vector<FilterColMeta> cols, uint64_t cap) {
        columns = std::move(cols);
        for (auto& m : columns) m.elem_size = filter_col_elem_size(m.type);
        data.assign(columns.size(), {});
        for (size_t c = 0; c < columns.size(); ++c) {
            data[c].resize(cap * columns[c].elem_size);
        }
        count    = 0;
        capacity = cap;
    }

    // Appends nrows values for column col_idx. Grows the backing buffer if needed.
    // Caller owns the encoding of `src` (must be nrows * elem_size bytes).
    // count is advanced by nrows only on the first column written per batch;
    // subsequent columns in the same batch must match that row count.
    // Simplification: we track count per-column in col_counts_ and expose the
    // minimum as `count` — so all columns stay in lockstep.
    void add_chunk(uint32_t col_idx, const void* src, uint64_t nrows) {
        if (col_idx >= columns.size()) {
            throw std::out_of_range("add_chunk: col_idx out of range");
        }
        if (col_counts_.size() != columns.size()) {
            col_counts_.assign(columns.size(), 0);
        }
        uint64_t off_bytes = col_counts_[col_idx] * columns[col_idx].elem_size;
        uint64_t add_bytes = nrows * columns[col_idx].elem_size;
        if (off_bytes + add_bytes > data[col_idx].size()) {
            data[col_idx].resize(off_bytes + add_bytes);
        }
        std::memcpy(data[col_idx].data() + off_bytes, src, add_bytes);
        col_counts_[col_idx] += nrows;

        // count = min(col_counts_[*]) — the number of rows complete across all cols.
        uint64_t min_cnt = col_counts_[0];
        for (size_t c = 1; c < col_counts_.size(); ++c) {
            if (col_counts_[c] < min_cnt) min_cnt = col_counts_[c];
        }
        count = min_cnt;
        if (count > capacity) capacity = count;
    }

    bool empty() const { return columns.empty() || count == 0; }

    // Raw pointer to row `row` of column `col_idx`. No bounds check in the hot path.
    const void* row_ptr(uint32_t col_idx, uint64_t row) const {
        return data[col_idx].data() + row * columns[col_idx].elem_size;
    }

    // -------------------------------------------------------------------------
    // Serialization — filter_data.bin
    //
    // [header — 24 bytes]
    //   uint32  magic    = 0x54_4C_49_46 ('FILT' little-endian)
    //   uint32  version  = 1
    //   uint32  ncols
    //   uint64  nrows
    //   uint32  reserved = 0
    //
    // [column descriptors — ncols entries]
    //   uint8   type_tag
    //   uint8   name_len
    //   char[]  name (name_len bytes, no null terminator)
    //
    // [column data — ncols contiguous blocks]
    //   column c: nrows * elem_size(type_tag_c) bytes, row-major
    // -------------------------------------------------------------------------

    static constexpr uint32_t kMagic   = 0x544C4946u;  // 'FILT' (LE)
    static constexpr uint32_t kVersion = 1;

    void save(const std::string& path) const {
        std::ofstream os(path, std::ios::binary);
        if (!os) throw std::runtime_error("FilterStore::save: cannot open " + path);

        uint32_t magic    = kMagic;
        uint32_t version  = kVersion;
        uint32_t ncols    = static_cast<uint32_t>(columns.size());
        uint64_t nrows    = count;
        uint32_t reserved = 0;
        os.write(reinterpret_cast<const char*>(&magic),    sizeof(magic));
        os.write(reinterpret_cast<const char*>(&version),  sizeof(version));
        os.write(reinterpret_cast<const char*>(&ncols),    sizeof(ncols));
        os.write(reinterpret_cast<const char*>(&nrows),    sizeof(nrows));
        os.write(reinterpret_cast<const char*>(&reserved), sizeof(reserved));

        for (const auto& m : columns) {
            uint8_t tag = static_cast<uint8_t>(m.type);
            if (m.name.size() > 255) {
                throw std::runtime_error("FilterStore::save: column name too long");
            }
            uint8_t name_len = static_cast<uint8_t>(m.name.size());
            os.write(reinterpret_cast<const char*>(&tag),      1);
            os.write(reinterpret_cast<const char*>(&name_len), 1);
            os.write(m.name.data(), name_len);
        }

        for (size_t c = 0; c < columns.size(); ++c) {
            uint64_t nbytes = nrows * columns[c].elem_size;
            if (nbytes > 0) {
                os.write(reinterpret_cast<const char*>(data[c].data()),
                         static_cast<std::streamsize>(nbytes));
            }
        }
    }

    void load(const std::string& path) {
        std::ifstream is(path, std::ios::binary);
        if (!is) throw std::runtime_error("FilterStore::load: cannot open " + path);

        uint32_t magic, version, ncols, reserved;
        uint64_t nrows;
        is.read(reinterpret_cast<char*>(&magic),    sizeof(magic));
        is.read(reinterpret_cast<char*>(&version),  sizeof(version));
        is.read(reinterpret_cast<char*>(&ncols),    sizeof(ncols));
        is.read(reinterpret_cast<char*>(&nrows),    sizeof(nrows));
        is.read(reinterpret_cast<char*>(&reserved), sizeof(reserved));
        if (!is) throw std::runtime_error("FilterStore::load: short header in " + path);
        if (magic != kMagic) {
            throw std::runtime_error("FilterStore::load: bad magic in " + path);
        }
        if (version != kVersion) {
            throw std::runtime_error("FilterStore::load: unsupported version " +
                                     std::to_string(version));
        }

        columns.clear();
        columns.reserve(ncols);
        for (uint32_t c = 0; c < ncols; ++c) {
            uint8_t tag = 0, name_len = 0;
            is.read(reinterpret_cast<char*>(&tag),      1);
            is.read(reinterpret_cast<char*>(&name_len), 1);
            std::string name(name_len, '\0');
            if (name_len) is.read(name.data(), name_len);
            if (!is) throw std::runtime_error("FilterStore::load: short descriptor");
            FilterColMeta m;
            m.name      = std::move(name);
            m.type      = static_cast<FilterColType>(tag);
            m.elem_size = filter_col_elem_size(m.type);
            columns.push_back(std::move(m));
        }

        data.assign(ncols, {});
        for (uint32_t c = 0; c < ncols; ++c) {
            uint64_t nbytes = nrows * columns[c].elem_size;
            data[c].resize(nbytes);
            if (nbytes > 0) {
                is.read(reinterpret_cast<char*>(data[c].data()),
                        static_cast<std::streamsize>(nbytes));
            }
            if (!is) {
                throw std::runtime_error("FilterStore::load: short column data");
            }
        }

        count    = nrows;
        capacity = nrows;
        col_counts_.assign(ncols, nrows);
    }

private:
    std::vector<uint64_t> col_counts_;  // per-column row count during ingest
};

// -----------------------------------------------------------------------------
// Predicate types
// -----------------------------------------------------------------------------

enum class PredOpType : uint8_t {
    EQ      = 0,
    NE      = 1,
    LT      = 2,
    LE      = 3,
    GT      = 4,
    GE      = 5,
    BETWEEN = 6,
    IN      = 7,
};

// Union-ish scalar value. Both fields are populated from JSON; the reader
// picks i64 for integer types and f64 for floating types. No std::variant
// to keep this header nvcc-friendly without extended lambdas.
struct PredValue {
    int64_t  i64 = 0;
    uint64_t u64 = 0;
    double   f64 = 0.0;
};

struct PredOp {
    uint32_t               col_idx = 0;
    PredOpType             op      = PredOpType::EQ;
    PredValue              val;                      // used by EQ/NE/LT/LE/GT/GE
    PredValue              lo;                       // BETWEEN
    PredValue              hi;                       // BETWEEN
    std::vector<PredValue> in_vals;                  // IN
};

// -----------------------------------------------------------------------------
// JSON predicate parser
//
// Accepts a minimal subset — exactly what the SQL layer emits:
//
//   [
//     {"col": 0, "op": ">=",      "val": 5.0},
//     {"col": 2, "op": "in",      "vals": [100, 200, 300]},
//     {"col": 3, "op": "between", "lo": 1.0, "hi": 9.9}
//   ]
//
// Numbers may be int or float literals.  Strings are not supported (VARCHAR
// columns must be pre-hashed to UINT64 in the SQL layer).
// -----------------------------------------------------------------------------

namespace detail {

inline void skip_ws(const std::string& s, size_t& i) {
    while (i < s.size() && (s[i]==' '||s[i]=='\t'||s[i]=='\n'||s[i]=='\r')) ++i;
}

inline bool expect(const std::string& s, size_t& i, char c) {
    skip_ws(s, i);
    if (i >= s.size() || s[i] != c) return false;
    ++i;
    return true;
}

inline bool parse_string(const std::string& s, size_t& i, std::string& out) {
    skip_ws(s, i);
    if (i >= s.size() || s[i] != '"') return false;
    ++i;
    size_t start = i;
    while (i < s.size() && s[i] != '"') {
        if (s[i] == '\\' && i + 1 < s.size()) i += 2;
        else ++i;
    }
    if (i >= s.size()) return false;
    out = s.substr(start, i - start);
    ++i;  // past closing quote
    return true;
}

inline bool parse_number(const std::string& s, size_t& i, PredValue& out, bool& is_float) {
    skip_ws(s, i);
    size_t start = i;
    if (i < s.size() && (s[i] == '-' || s[i] == '+')) ++i;
    bool has_digit = false, has_dot = false, has_exp = false;
    while (i < s.size()) {
        char c = s[i];
        if (c >= '0' && c <= '9') { has_digit = true; ++i; }
        else if (c == '.' && !has_dot) { has_dot = true; ++i; }
        else if ((c == 'e' || c == 'E') && !has_exp) { has_exp = true; ++i;
            if (i < s.size() && (s[i] == '+' || s[i] == '-')) ++i; }
        else break;
    }
    if (!has_digit) return false;
    std::string tok = s.substr(start, i - start);
    is_float = has_dot || has_exp;
    try {
        if (is_float) {
            out.f64 = std::stod(tok);
            out.i64 = static_cast<int64_t>(out.f64);
            out.u64 = static_cast<uint64_t>(out.f64);
        } else {
            out.i64 = std::stoll(tok);
            out.u64 = static_cast<uint64_t>(out.i64);
            out.f64 = static_cast<double>(out.i64);
        }
    } catch (...) { return false; }
    return true;
}

inline PredOpType op_from_string(const std::string& s) {
    if (s == "=" || s == "==" || s == "eq")  return PredOpType::EQ;
    if (s == "!=" || s == "<>" || s == "ne") return PredOpType::NE;
    if (s == "<"  || s == "lt")              return PredOpType::LT;
    if (s == "<=" || s == "le")              return PredOpType::LE;
    if (s == ">"  || s == "gt")              return PredOpType::GT;
    if (s == ">=" || s == "ge")              return PredOpType::GE;
    if (s == "between")                      return PredOpType::BETWEEN;
    if (s == "in")                           return PredOpType::IN;
    throw std::runtime_error("parse_preds: unknown op '" + s + "'");
}

// Reads a single predicate object {...}, consuming whitespace before/after.
inline PredOp parse_one_pred(const std::string& s, size_t& i) {
    if (!expect(s, i, '{')) throw std::runtime_error("parse_preds: expected '{'");

    PredOp p;
    bool has_col = false, has_op = false;

    while (true) {
        skip_ws(s, i);
        if (i < s.size() && s[i] == '}') { ++i; break; }

        std::string key;
        if (!parse_string(s, i, key)) throw std::runtime_error("parse_preds: expected key");
        if (!expect(s, i, ':'))       throw std::runtime_error("parse_preds: expected ':'");

        skip_ws(s, i);
        if (key == "col") {
            PredValue tmp; bool is_flt = false;
            if (!parse_number(s, i, tmp, is_flt))
                throw std::runtime_error("parse_preds: bad col index");
            p.col_idx = static_cast<uint32_t>(tmp.i64);
            has_col = true;
        } else if (key == "op") {
            std::string op_str;
            if (!parse_string(s, i, op_str))
                throw std::runtime_error("parse_preds: bad op");
            p.op = op_from_string(op_str);
            has_op = true;
        } else if (key == "val" || key == "lo" || key == "hi") {
            PredValue v; bool is_flt = false;
            if (!parse_number(s, i, v, is_flt))
                throw std::runtime_error("parse_preds: bad value for " + key);
            if (key == "val") p.val = v;
            else if (key == "lo") p.lo = v;
            else p.hi = v;
        } else if (key == "vals") {
            if (!expect(s, i, '['))
                throw std::runtime_error("parse_preds: expected '[' for vals");
            while (true) {
                skip_ws(s, i);
                if (i < s.size() && s[i] == ']') { ++i; break; }
                PredValue v; bool is_flt = false;
                if (!parse_number(s, i, v, is_flt))
                    throw std::runtime_error("parse_preds: bad element in vals");
                p.in_vals.push_back(v);
                skip_ws(s, i);
                if (i < s.size() && s[i] == ',') { ++i; continue; }
            }
        } else {
            // Skip unknown key's value (string or number).
            skip_ws(s, i);
            if (i < s.size() && s[i] == '"') {
                std::string dummy;
                parse_string(s, i, dummy);
            } else {
                PredValue dummy; bool dummy_flt = false;
                parse_number(s, i, dummy, dummy_flt);
            }
        }

        skip_ws(s, i);
        if (i < s.size() && s[i] == ',') { ++i; continue; }
    }

    if (!has_col || !has_op)
        throw std::runtime_error("parse_preds: predicate missing col or op");
    if (p.op == PredOpType::IN && p.in_vals.empty())
        throw std::runtime_error("parse_preds: IN requires non-empty vals");
    return p;
}

}  // namespace detail

// Parses column metadata JSON emitted by the SQL layer:
//   [{"name":"price","type":2},{"name":"cat","type":1}]
// where `type` is a FilterColType enum value (0=int32, 1=int64, 2=float32,
// 3=float64, 4=uint64). Unknown keys are ignored.
inline std::vector<FilterColMeta> parse_filter_col_meta(const std::string& json) {
    std::vector<FilterColMeta> out;
    size_t i = 0;
    detail::skip_ws(json, i);
    if (i >= json.size()) return out;
    if (!detail::expect(json, i, '['))
        throw std::runtime_error("parse_filter_col_meta: expected '['");
    while (true) {
        detail::skip_ws(json, i);
        if (i < json.size() && json[i] == ']') { ++i; break; }
        if (!detail::expect(json, i, '{'))
            throw std::runtime_error("parse_filter_col_meta: expected '{'");

        FilterColMeta m;
        bool has_name = false, has_type = false;
        while (true) {
            detail::skip_ws(json, i);
            if (i < json.size() && json[i] == '}') { ++i; break; }

            std::string key;
            if (!detail::parse_string(json, i, key))
                throw std::runtime_error("parse_filter_col_meta: expected key");
            if (!detail::expect(json, i, ':'))
                throw std::runtime_error("parse_filter_col_meta: expected ':'");

            detail::skip_ws(json, i);
            if (key == "name") {
                if (!detail::parse_string(json, i, m.name))
                    throw std::runtime_error("parse_filter_col_meta: bad name");
                has_name = true;
            } else if (key == "type") {
                PredValue v; bool is_flt = false;
                if (!detail::parse_number(json, i, v, is_flt))
                    throw std::runtime_error("parse_filter_col_meta: bad type");
                if (v.i64 < 0 || v.i64 > 4)
                    throw std::runtime_error("parse_filter_col_meta: type out of range");
                m.type = static_cast<FilterColType>(v.i64);
                has_type = true;
            } else {
                // Skip unknown value (string or number).
                detail::skip_ws(json, i);
                if (i < json.size() && json[i] == '"') {
                    std::string dummy;
                    detail::parse_string(json, i, dummy);
                } else {
                    PredValue dummy; bool dummy_flt = false;
                    detail::parse_number(json, i, dummy, dummy_flt);
                }
            }
            detail::skip_ws(json, i);
            if (i < json.size() && json[i] == ',') { ++i; continue; }
        }
        if (!has_name || !has_type)
            throw std::runtime_error("parse_filter_col_meta: missing name or type");
        m.elem_size = filter_col_elem_size(m.type);
        out.push_back(std::move(m));

        detail::skip_ws(json, i);
        if (i < json.size() && json[i] == ',') { ++i; continue; }
    }
    return out;
}

inline std::vector<PredOp> parse_preds(const std::string& json) {
    std::vector<PredOp> out;
    size_t i = 0;
    detail::skip_ws(json, i);
    if (i >= json.size()) return out;  // empty input = no predicates
    if (!detail::expect(json, i, '['))
        throw std::runtime_error("parse_preds: expected top-level '['");
    while (true) {
        detail::skip_ws(json, i);
        if (i < json.size() && json[i] == ']') { ++i; break; }
        out.push_back(detail::parse_one_pred(json, i));
        detail::skip_ws(json, i);
        if (i < json.size() && json[i] == ',') { ++i; continue; }
    }
    return out;
}

// -----------------------------------------------------------------------------
// Typed scalar comparison helpers
// -----------------------------------------------------------------------------

namespace detail {

template <typename T> inline T pred_value_as(const PredValue& v);
template <> inline int32_t  pred_value_as<int32_t>(const PredValue& v)  { return static_cast<int32_t>(v.i64); }
template <> inline int64_t  pred_value_as<int64_t>(const PredValue& v)  { return v.i64; }
template <> inline uint64_t pred_value_as<uint64_t>(const PredValue& v) { return v.u64; }
template <> inline float    pred_value_as<float>(const PredValue& v)    { return static_cast<float>(v.f64); }
template <> inline double   pred_value_as<double>(const PredValue& v)   { return v.f64; }

// Per-predicate 32-row evaluator. Dispatches once on (column type, op);
// the inner row loop is tight, branchless, and fixed-stride so the compiler
// can unroll + auto-vectorize (SSE/AVX2 on x86, NEON on aarch64).
//
// Returns a uint32_t where bit k is 1 iff row (base_row + k) satisfies `p`.
// Bits at positions >= rows_in_word are left as 0, so the caller can AND
// predicate masks together and the unused tail bits stay zero.
template <typename T>
inline uint32_t eval_pred_word_typed(const T* col,
                                     uint64_t base_row,
                                     uint32_t rows_in_word,
                                     const PredOp& p) {
    uint32_t bits = 0;
    const T vv = pred_value_as<T>(p.val);
    switch (p.op) {
        case PredOpType::EQ:
            for (uint32_t k = 0; k < rows_in_word; ++k)
                bits |= static_cast<uint32_t>(col[base_row + k] == vv) << k;
            return bits;
        case PredOpType::NE:
            for (uint32_t k = 0; k < rows_in_word; ++k)
                bits |= static_cast<uint32_t>(col[base_row + k] != vv) << k;
            return bits;
        case PredOpType::LT:
            for (uint32_t k = 0; k < rows_in_word; ++k)
                bits |= static_cast<uint32_t>(col[base_row + k] <  vv) << k;
            return bits;
        case PredOpType::LE:
            for (uint32_t k = 0; k < rows_in_word; ++k)
                bits |= static_cast<uint32_t>(col[base_row + k] <= vv) << k;
            return bits;
        case PredOpType::GT:
            for (uint32_t k = 0; k < rows_in_word; ++k)
                bits |= static_cast<uint32_t>(col[base_row + k] >  vv) << k;
            return bits;
        case PredOpType::GE:
            for (uint32_t k = 0; k < rows_in_word; ++k)
                bits |= static_cast<uint32_t>(col[base_row + k] >= vv) << k;
            return bits;
        case PredOpType::BETWEEN: {
            const T lo = pred_value_as<T>(p.lo);
            const T hi = pred_value_as<T>(p.hi);
            for (uint32_t k = 0; k < rows_in_word; ++k) {
                T x = col[base_row + k];
                bits |= static_cast<uint32_t>(x >= lo && x <= hi) << k;
            }
            return bits;
        }
        case PredOpType::IN: {
            // Branchless across in_vals — no early break so the row loop
            // stays straight-line. Expect small in_vals lists in practice.
            for (uint32_t k = 0; k < rows_in_word; ++k) {
                T x = col[base_row + k];
                uint32_t hit = 0;
                for (const auto& iv : p.in_vals) {
                    hit |= static_cast<uint32_t>(x == pred_value_as<T>(iv));
                }
                bits |= (hit != 0 ? 1u : 0u) << k;
            }
            return bits;
        }
    }
    return 0;
}

inline uint32_t eval_pred_word(const FilterStore& fs, const PredOp& p,
                               uint64_t base_row, uint32_t rows_in_word) {
    const void* col_base = fs.data[p.col_idx].data();
    switch (fs.columns[p.col_idx].type) {
        case FilterColType::INT32:
            return eval_pred_word_typed<int32_t>(
                reinterpret_cast<const int32_t*>(col_base), base_row, rows_in_word, p);
        case FilterColType::INT64:
            return eval_pred_word_typed<int64_t>(
                reinterpret_cast<const int64_t*>(col_base), base_row, rows_in_word, p);
        case FilterColType::FLOAT32:
            return eval_pred_word_typed<float>(
                reinterpret_cast<const float*>(col_base), base_row, rows_in_word, p);
        case FilterColType::FLOAT64:
            return eval_pred_word_typed<double>(
                reinterpret_cast<const double*>(col_base), base_row, rows_in_word, p);
        case FilterColType::UINT64:
            return eval_pred_word_typed<uint64_t>(
                reinterpret_cast<const uint64_t*>(col_base), base_row, rows_in_word, p);
    }
    return 0;
}

}  // namespace detail

// -----------------------------------------------------------------------------
// eval_filter_bitmap_cpu
//
// Produces a packed uint32_t bitmap where bit i = 1 means the i-th row in the
// [start_row, start_row + num_rows) window passes all predicates. Empty preds
// → all rows pass (all-ones, with trailing tail bits zeroed to avoid matching
// phantom rows past num_rows).
// -----------------------------------------------------------------------------

inline std::vector<uint32_t>
eval_filter_bitmap_cpu(const FilterStore& fs,
                       const std::vector<PredOp>& preds,
                       uint64_t start_row,
                       uint64_t num_rows) {
    uint64_t nwords = (num_rows + 31) / 32;
    std::vector<uint32_t> mask(nwords, 0);

    if (preds.empty()) {
        std::fill(mask.begin(), mask.end(), 0xFFFFFFFFu);
        uint32_t tail = static_cast<uint32_t>(num_rows & 31);
        if (tail && nwords > 0) mask.back() = (1u << tail) - 1u;
        return mask;
    }

    // Each iteration owns one uint32_t word (32 consecutive rows). No atomics,
    // no false sharing across threads (each thread writes to its own word).
    // Per-predicate (type, op) dispatch happens once per word outside the row
    // loop, so the inner 32-row loop is tight and auto-vectorizable.
    uint64_t full_words = num_rows / 32;
    #pragma omp parallel for schedule(static)
    for (int64_t w = 0; w < static_cast<int64_t>(full_words); ++w) {
        uint64_t base = static_cast<uint64_t>(w) * 32;
        uint32_t bits = 0xFFFFFFFFu;
        for (const auto& p : preds) {
            bits &= detail::eval_pred_word(fs, p, start_row + base, 32);
        }
        mask[w] = bits;
    }

    // Tail word (fewer than 32 live rows). Each predicate mask has bit k = 0
    // for k >= tail, so starting from all-ones and ANDing naturally leaves
    // those positions zero in the result. Runs at most once.
    if (full_words < nwords) {
        uint64_t base = full_words * 32;
        uint32_t tail = static_cast<uint32_t>(num_rows - base);
        uint32_t bits = 0xFFFFFFFFu;
        for (const auto& p : preds) {
            bits &= detail::eval_pred_word(fs, p, start_row + base, tail);
        }
        mask[full_words] = bits;
    }
    return mask;
}

// Convenience overload — parses JSON and evaluates in one call.
inline std::vector<uint32_t>
eval_filter_bitmap_cpu(const FilterStore& fs,
                       const std::string& preds_json,
                       uint64_t start_row,
                       uint64_t num_rows) {
    auto preds = parse_preds(preds_json);
    return eval_filter_bitmap_cpu(fs, preds, start_row, num_rows);
}

}  // namespace matrixone
