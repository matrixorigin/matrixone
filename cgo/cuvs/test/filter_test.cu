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

#include "filter.hpp"
#include "index_base.hpp"
#include "test_framework.hpp"

#include <sys/stat.h>
#include <sys/types.h>

#include <atomic>
#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <fstream>
#include <random>
#include <string>
#include <unistd.h>

using namespace matrixone;

namespace {

// Returns 1 iff bit `row` is set in `mask` (LSB-first within each word).
inline uint32_t get_bit(const std::vector<uint32_t>& mask, uint64_t row) {
    return (mask[row / 32] >> (row % 32)) & 1u;
}

// Makes a temp file path unique to this process + a counter.
std::string tmp_path(const std::string& tag) {
    static std::atomic<int> ctr{0};
    return "/tmp/mo_filter_test_" + std::to_string(::getpid()) + "_" +
           tag + "_" + std::to_string(ctr.fetch_add(1)) + ".bin";
}

FilterStore make_store_i32_f32(uint64_t nrows) {
    FilterStore fs;
    fs.init({{"price", FilterColType::FLOAT32, 0},
             {"cat",   FilterColType::INT32,   0}}, nrows);
    std::vector<float> prices(nrows);
    std::vector<int32_t> cats(nrows);
    for (uint64_t i = 0; i < nrows; ++i) {
        prices[i] = static_cast<float>(i);       // 0.0, 1.0, 2.0, ...
        cats[i]   = static_cast<int32_t>(i % 5); // cycles 0..4
    }
    fs.add_chunk(0, prices.data(), nullptr, nrows);
    fs.add_chunk(1, cats.data(), nullptr, nrows);
    return fs;
}

}  // namespace

// =============================================================================
// FilterStore: init / add_chunk / row_ptr
// =============================================================================

TEST(FilterStoreTest, InitSetsMetadataAndSizes) {
    FilterStore fs;
    fs.init({{"a", FilterColType::INT64,   0},
             {"b", FilterColType::FLOAT32, 0}}, 100);

    ASSERT_EQ(fs.columns.size(), static_cast<size_t>(2));
    ASSERT_EQ(fs.columns[0].elem_size, 8u);
    ASSERT_EQ(fs.columns[1].elem_size, 4u);
    ASSERT_EQ(fs.capacity, 100u);
    ASSERT_EQ(fs.count,    0u);
    ASSERT_TRUE(fs.empty());
}

TEST(FilterStoreTest, AddChunkAdvancesCountInLockstep) {
    FilterStore fs;
    fs.init({{"a", FilterColType::INT32, 0},
             {"b", FilterColType::INT64, 0}}, 10);

    std::vector<int32_t> a{1, 2, 3};
    std::vector<int64_t> b{10, 20, 30};

    fs.add_chunk(0, a.data(), nullptr, 3);
    // Only col 0 has 3 rows; col 1 still empty → count = min = 0.
    ASSERT_EQ(fs.count, 0u);

    fs.add_chunk(1, b.data(), nullptr, 3);
    ASSERT_EQ(fs.count, 3u);

    ASSERT_EQ(*reinterpret_cast<const int32_t*>(fs.row_ptr(0, 0)), 1);
    ASSERT_EQ(*reinterpret_cast<const int32_t*>(fs.row_ptr(0, 2)), 3);
    ASSERT_EQ(*reinterpret_cast<const int64_t*>(fs.row_ptr(1, 0)), 10);
    ASSERT_EQ(*reinterpret_cast<const int64_t*>(fs.row_ptr(1, 2)), 30);
}

TEST(FilterStoreTest, AddChunkGrowsBuffers) {
    FilterStore fs;
    fs.init({{"a", FilterColType::INT32, 0}}, 2);  // undersized capacity

    std::vector<int32_t> a{100, 200, 300, 400, 500};
    fs.add_chunk(0, a.data(), nullptr, 5);  // forces resize from 2 → 5
    ASSERT_EQ(fs.count, 5u);
    ASSERT_EQ(*reinterpret_cast<const int32_t*>(fs.row_ptr(0, 4)), 500);
}

TEST(FilterStoreTest, AddChunkRejectsBadColIdx) {
    FilterStore fs;
    fs.init({{"a", FilterColType::INT32, 0}}, 4);
    int32_t v = 0;
    ASSERT_THROW(fs.add_chunk(5, &v, nullptr, 1), std::out_of_range);
}

// =============================================================================
// FilterStore: save / load roundtrip
// =============================================================================

TEST(FilterStoreTest, SaveLoadRoundtrip) {
    FilterStore src;
    src.init({{"price",    FilterColType::FLOAT64, 0},
              {"cat_hash", FilterColType::UINT64,  0},
              {"qty",      FilterColType::INT32,   0}}, 7);

    std::vector<double>   prices{1.5, 2.5, 3.5, 4.5, 5.5, 6.5, 7.5};
    std::vector<uint64_t> hashes{0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77};
    std::vector<int32_t>  qtys{-3, -2, -1, 0, 1, 2, 3};
    src.add_chunk(0, prices.data(), nullptr, 7);
    src.add_chunk(1, hashes.data(), nullptr, 7);
    src.add_chunk(2, qtys.data(), nullptr, 7);

    auto path = tmp_path("roundtrip");
    src.save(path);

    FilterStore dst;
    dst.load(path);
    ::unlink(path.c_str());

    ASSERT_EQ(dst.columns.size(), src.columns.size());
    ASSERT_EQ(dst.count, src.count);
    ASSERT_EQ(dst.columns[0].name, std::string("price"));
    ASSERT_EQ(dst.columns[1].name, std::string("cat_hash"));
    ASSERT_EQ(dst.columns[2].name, std::string("qty"));
    ASSERT_EQ(static_cast<int>(dst.columns[0].type),
              static_cast<int>(FilterColType::FLOAT64));
    ASSERT_EQ(dst.columns[0].elem_size, 8u);

    for (uint64_t i = 0; i < 7; ++i) {
        ASSERT_EQ(*reinterpret_cast<const double*>(dst.row_ptr(0, i)), prices[i]);
        ASSERT_EQ(*reinterpret_cast<const uint64_t*>(dst.row_ptr(1, i)), hashes[i]);
        ASSERT_EQ(*reinterpret_cast<const int32_t*>(dst.row_ptr(2, i)),  qtys[i]);
    }
}

TEST(FilterStoreTest, LoadRejectsBadMagic) {
    auto path = tmp_path("badmagic");
    {
        std::ofstream os(path, std::ios::binary);
        uint32_t junk = 0xDEADBEEF;
        os.write(reinterpret_cast<const char*>(&junk), sizeof(junk));
        // rest left empty
    }
    FilterStore dst;
    ASSERT_THROW(dst.load(path), std::runtime_error);
    ::unlink(path.c_str());
}

// =============================================================================
// parse_preds
// =============================================================================

TEST(ParsePredsTest, EmptyInputReturnsEmpty) {
    auto v = parse_preds("");
    ASSERT_EQ(v.size(), 0u);
    auto v2 = parse_preds("   \n\t  ");
    ASSERT_EQ(v2.size(), 0u);
    auto v3 = parse_preds("[]");
    ASSERT_EQ(v3.size(), 0u);
}

TEST(ParsePredsTest, AllOpsParseCorrectly) {
    std::string json =
        "["
        " {\"col\": 0, \"op\": \">=\", \"val\": 5.0},"
        " {\"col\": 1, \"op\": \"=\",  \"val\": 10},"
        " {\"col\": 2, \"op\": \"in\", \"vals\": [100, 200, 300]},"
        " {\"col\": 3, \"op\": \"between\", \"lo\": 1.5, \"hi\": 9.5},"
        " {\"col\": 4, \"op\": \"!=\", \"val\": -7}"
        "]";
    auto v = parse_preds(json);
    ASSERT_EQ(v.size(), 5u);

    ASSERT_EQ(v[0].col_idx, 0u);
    ASSERT_EQ(static_cast<int>(v[0].op), static_cast<int>(PredOpType::GE));
    ASSERT_EQ(v[0].val.f64, 5.0);

    ASSERT_EQ(static_cast<int>(v[1].op), static_cast<int>(PredOpType::EQ));
    ASSERT_EQ(v[1].val.i64, 10);

    ASSERT_EQ(static_cast<int>(v[2].op), static_cast<int>(PredOpType::IN));
    ASSERT_EQ(v[2].in_vals.size(), 3u);
    ASSERT_EQ(v[2].in_vals[0].i64, 100);
    ASSERT_EQ(v[2].in_vals[2].i64, 300);

    ASSERT_EQ(static_cast<int>(v[3].op), static_cast<int>(PredOpType::BETWEEN));
    ASSERT_EQ(v[3].lo.f64, 1.5);
    ASSERT_EQ(v[3].hi.f64, 9.5);

    ASSERT_EQ(static_cast<int>(v[4].op), static_cast<int>(PredOpType::NE));
    ASSERT_EQ(v[4].val.i64, -7);
}

TEST(ParsePredsTest, MalformedInputsThrow) {
    ASSERT_THROW(parse_preds("[{}]"),                           std::runtime_error);
    ASSERT_THROW(parse_preds("[{\"col\":0}]"),                  std::runtime_error);
    ASSERT_THROW(parse_preds("[{\"op\":\"eq\",\"val\":1}]"),    std::runtime_error);
    ASSERT_THROW(parse_preds("[{\"col\":0,\"op\":\"xx\"}]"),    std::runtime_error);
    ASSERT_THROW(parse_preds("[{\"col\":0,\"op\":\"in\",\"vals\":[]}]"),
                 std::runtime_error);
    ASSERT_THROW(parse_preds("{\"col\":0}"),                    std::runtime_error);
}

TEST(ParsePredsTest, IgnoresUnknownKeys) {
    std::string json = "[{\"col\":1,\"op\":\"=\",\"val\":3,\"comment\":\"hi\"}]";
    auto v = parse_preds(json);
    ASSERT_EQ(v.size(), 1u);
    ASSERT_EQ(v[0].col_idx, 1u);
}

// =============================================================================
// parse_filter_col_meta
// =============================================================================

TEST(ParseFilterColMetaTest, EmptyInputReturnsEmpty) {
    ASSERT_EQ(parse_filter_col_meta("").size(),     0u);
    ASSERT_EQ(parse_filter_col_meta("  ").size(),   0u);
    ASSERT_EQ(parse_filter_col_meta("[]").size(),   0u);
}

TEST(ParseFilterColMetaTest, ParsesAllTypes) {
    std::string json =
        "["
        " {\"name\":\"a\",\"type\":0},"
        " {\"name\":\"b\",\"type\":1},"
        " {\"name\":\"c\",\"type\":2},"
        " {\"name\":\"d\",\"type\":3},"
        " {\"name\":\"e\",\"type\":4}"
        "]";
    auto v = parse_filter_col_meta(json);
    ASSERT_EQ(v.size(), 5u);
    ASSERT_EQ(v[0].name, std::string("a"));
    ASSERT_EQ(static_cast<int>(v[0].type), static_cast<int>(FilterColType::INT32));
    ASSERT_EQ(v[0].elem_size, 4u);
    ASSERT_EQ(static_cast<int>(v[1].type), static_cast<int>(FilterColType::INT64));
    ASSERT_EQ(v[1].elem_size, 8u);
    ASSERT_EQ(static_cast<int>(v[2].type), static_cast<int>(FilterColType::FLOAT32));
    ASSERT_EQ(static_cast<int>(v[3].type), static_cast<int>(FilterColType::FLOAT64));
    ASSERT_EQ(static_cast<int>(v[4].type), static_cast<int>(FilterColType::UINT64));
}

TEST(ParseFilterColMetaTest, MalformedInputsThrow) {
    ASSERT_THROW(parse_filter_col_meta("[{}]"),                         std::runtime_error);
    ASSERT_THROW(parse_filter_col_meta("[{\"name\":\"x\"}]"),           std::runtime_error);
    ASSERT_THROW(parse_filter_col_meta("[{\"type\":0}]"),               std::runtime_error);
    ASSERT_THROW(parse_filter_col_meta("[{\"name\":\"x\",\"type\":5}]"),std::runtime_error);
    ASSERT_THROW(parse_filter_col_meta("[{\"name\":\"x\",\"type\":-1}]"),
                 std::runtime_error);
    ASSERT_THROW(parse_filter_col_meta("{\"name\":\"x\"}"),             std::runtime_error);
}

// =============================================================================
// eval_filter_bitmap_cpu — basic / all types / all ops
// =============================================================================

TEST(EvalFilterBitmapTest, EmptyPredsAllOnesWithTailMasked) {
    FilterStore fs = make_store_i32_f32(70);
    auto mask = eval_filter_bitmap_cpu(fs, std::vector<PredOp>{}, 0, 70);

    // nwords = ceil(70/32) = 3
    ASSERT_EQ(mask.size(), 3u);
    ASSERT_EQ(mask[0], 0xFFFFFFFFu);
    ASSERT_EQ(mask[1], 0xFFFFFFFFu);
    // Last word: 70%32 = 6 → only lowest 6 bits set.
    ASSERT_EQ(mask[2], (1u << 6) - 1u);
    // Rows past num_rows must be 0.
    for (uint64_t r = 70; r < 96; ++r) ASSERT_EQ(get_bit(mask, r), 0u);
}

TEST(EvalFilterBitmapTest, FloatGELowerBound) {
    FilterStore fs = make_store_i32_f32(100);
    auto mask = eval_filter_bitmap_cpu(fs,
        "[{\"col\":0,\"op\":\">=\",\"val\":50.0}]", 0, 100);

    for (uint64_t i = 0; i < 100; ++i) {
        uint32_t want = (i >= 50) ? 1u : 0u;
        ASSERT_EQ(get_bit(mask, i), want);
    }
}

TEST(EvalFilterBitmapTest, Int32EqualsPredicate) {
    FilterStore fs = make_store_i32_f32(50);
    auto mask = eval_filter_bitmap_cpu(fs,
        "[{\"col\":1,\"op\":\"=\",\"val\":3}]", 0, 50);

    for (uint64_t i = 0; i < 50; ++i) {
        uint32_t want = ((i % 5) == 3) ? 1u : 0u;
        ASSERT_EQ(get_bit(mask, i), want);
    }
}

TEST(EvalFilterBitmapTest, AndOfMultiplePredicates) {
    FilterStore fs = make_store_i32_f32(200);
    auto mask = eval_filter_bitmap_cpu(fs,
        "[{\"col\":0,\"op\":\"<\",\"val\":100.0},"
        " {\"col\":1,\"op\":\"=\",\"val\":2}]",
        0, 200);

    for (uint64_t i = 0; i < 200; ++i) {
        bool pass = (static_cast<float>(i) < 100.0f) && ((i % 5) == 2);
        ASSERT_EQ(get_bit(mask, i), pass ? 1u : 0u);
    }
}

TEST(EvalFilterBitmapTest, BetweenInclusive) {
    FilterStore fs = make_store_i32_f32(100);
    auto mask = eval_filter_bitmap_cpu(fs,
        "[{\"col\":0,\"op\":\"between\",\"lo\":10.0,\"hi\":20.0}]",
        0, 100);

    for (uint64_t i = 0; i < 100; ++i) {
        float v = static_cast<float>(i);
        bool pass = v >= 10.0f && v <= 20.0f;  // 11 rows: 10..20
        ASSERT_EQ(get_bit(mask, i), pass ? 1u : 0u);
    }
}

TEST(EvalFilterBitmapTest, InList) {
    FilterStore fs = make_store_i32_f32(40);
    auto mask = eval_filter_bitmap_cpu(fs,
        "[{\"col\":1,\"op\":\"in\",\"vals\":[0, 4]}]", 0, 40);

    for (uint64_t i = 0; i < 40; ++i) {
        bool pass = ((i % 5) == 0) || ((i % 5) == 4);
        ASSERT_EQ(get_bit(mask, i), pass ? 1u : 0u);
    }
}

TEST(EvalFilterBitmapTest, ShardSliceWithStartRow) {
    // One big column of 0..199; evaluate a 50-row window starting at row 100.
    FilterStore fs;
    fs.init({{"v", FilterColType::INT64, 0}}, 200);
    std::vector<int64_t> v(200);
    for (uint64_t i = 0; i < 200; ++i) v[i] = static_cast<int64_t>(i);
    fs.add_chunk(0, v.data(), nullptr, 200);

    auto mask = eval_filter_bitmap_cpu(fs,
        "[{\"col\":0,\"op\":\">=\",\"val\":120}]",
        /*start_row=*/100, /*num_rows=*/50);

    // Local row r corresponds to global row 100+r. Bit should be 1 when >= 120,
    // i.e. local r >= 20.
    ASSERT_EQ(mask.size(), 2u);
    for (uint64_t r = 0; r < 50; ++r) {
        uint32_t want = (r >= 20) ? 1u : 0u;
        ASSERT_EQ(get_bit(mask, r), want);
    }
    // Bits past num_rows=50 must stay 0.
    for (uint64_t r = 50; r < 64; ++r) ASSERT_EQ(get_bit(mask, r), 0u);
}

TEST(EvalFilterBitmapTest, Uint64EqFromHash) {
    FilterStore fs;
    fs.init({{"h", FilterColType::UINT64, 0}}, 8);
    std::vector<uint64_t> h{1ULL<<40, 2ULL<<40, 3ULL<<40, 4ULL<<40,
                            5ULL<<40, 6ULL<<40, 7ULL<<40, 8ULL<<40};
    fs.add_chunk(0, h.data(), nullptr, 8);

    // 3<<40 = 3298534883328
    auto mask = eval_filter_bitmap_cpu(fs,
        "[{\"col\":0,\"op\":\"=\",\"val\":3298534883328}]", 0, 8);

    for (uint64_t i = 0; i < 8; ++i) {
        uint32_t want = (i == 2) ? 1u : 0u;
        ASSERT_EQ(get_bit(mask, i), want);
    }
}

TEST(EvalFilterBitmapTest, Float64Comparison) {
    FilterStore fs;
    fs.init({{"d", FilterColType::FLOAT64, 0}}, 4);
    std::vector<double> d{-1.5, 0.0, 1.5, 3.0};
    fs.add_chunk(0, d.data(), nullptr, 4);

    auto mask = eval_filter_bitmap_cpu(fs,
        "[{\"col\":0,\"op\":\">\",\"val\":0.0}]", 0, 4);

    ASSERT_EQ(get_bit(mask, 0), 0u);
    ASSERT_EQ(get_bit(mask, 1), 0u);
    ASSERT_EQ(get_bit(mask, 2), 1u);
    ASSERT_EQ(get_bit(mask, 3), 1u);
}

// Rows that hit the tail word but past num_rows must never be set.
TEST(EvalFilterBitmapTest, TailBitsZeroedEvenWhenAllMatch) {
    FilterStore fs;
    fs.init({{"v", FilterColType::INT32, 0}}, 5);
    std::vector<int32_t> v{7, 7, 7, 7, 7};
    fs.add_chunk(0, v.data(), nullptr, 5);

    auto mask = eval_filter_bitmap_cpu(fs,
        "[{\"col\":0,\"op\":\"=\",\"val\":7}]", 0, 5);

    ASSERT_EQ(mask.size(), 1u);
    ASSERT_EQ(mask[0], 0b11111u);  // exactly 5 low bits
}

// =============================================================================
// HostIdsView — predicates on the reserved __mo_pk_host_id virtual column.
// These verify that a PredOp with col_idx == kHostIdColIdx is evaluated
// against the passed-in host_ids buffer rather than a FilterStore column.
// The Go planner emits col=-1 which wraps to kHostIdColIdx (0xFFFFFFFFu)
// via static_cast<uint32_t>(i64) inside parse_one_pred; these tests use
// the wire form ("col":-1) to exercise that path end-to-end.
// =============================================================================

TEST(EvalFilterBitmapTest, HostIdsVirtualColumn_SentinelWiresMatch) {
    // Sanity: the Go sentinel -1 lands on the C++ kHostIdColIdx constant.
    ASSERT_EQ(kHostIdColIdx, static_cast<uint32_t>(-1));
}

TEST(EvalFilterBitmapTest, HostIdsVirtualColumn_EqOnPK) {
    // host_ids = [10, 20, 30, 40, 50]; filter id = 30 → only row 2 passes.
    std::vector<int64_t> host_ids{10, 20, 30, 40, 50};
    FilterStore fs;  // no FilterStore columns needed for a pure-PK predicate
    HostIdsView hv{host_ids.data(), FilterColType::INT64, host_ids.size()};

    auto mask = eval_filter_bitmap_cpu(fs,
        "[{\"col\":-1,\"op\":\"=\",\"val\":30}]",
        /*start_row=*/0, /*num_rows=*/host_ids.size(), hv);

    for (uint64_t i = 0; i < host_ids.size(); ++i) {
        uint32_t want = (host_ids[i] == 30) ? 1u : 0u;
        ASSERT_EQ(get_bit(mask, i), want);
    }
}

TEST(EvalFilterBitmapTest, HostIdsVirtualColumn_InList) {
    // id IN (20, 50) on host_ids [10,20,30,40,50] → rows 1 and 4.
    std::vector<int64_t> host_ids{10, 20, 30, 40, 50};
    FilterStore fs;
    HostIdsView hv{host_ids.data(), FilterColType::INT64, host_ids.size()};

    auto mask = eval_filter_bitmap_cpu(fs,
        "[{\"col\":-1,\"op\":\"in\",\"vals\":[20, 50]}]",
        0, host_ids.size(), hv);

    for (uint64_t i = 0; i < host_ids.size(); ++i) {
        bool pass = (host_ids[i] == 20) || (host_ids[i] == 50);
        ASSERT_EQ(get_bit(mask, i), pass ? 1u : 0u);
    }
}

TEST(EvalFilterBitmapTest, HostIdsVirtualColumn_RangeAndBetween) {
    // host_ids = [100..139]; id >= 120 AND id < 135 → rows 20..34 (15 rows).
    constexpr uint64_t N = 40;
    std::vector<int64_t> host_ids(N);
    for (uint64_t i = 0; i < N; ++i) host_ids[i] = static_cast<int64_t>(100 + i);
    FilterStore fs;
    HostIdsView hv{host_ids.data(), FilterColType::INT64, host_ids.size()};

    auto mask = eval_filter_bitmap_cpu(fs,
        "[{\"col\":-1,\"op\":\">=\",\"val\":120},"
        " {\"col\":-1,\"op\":\"<\",\"val\":135}]",
        0, N, hv);

    for (uint64_t i = 0; i < N; ++i) {
        int64_t id = host_ids[i];
        uint32_t want = (id >= 120 && id < 135) ? 1u : 0u;
        ASSERT_EQ(get_bit(mask, i), want);
    }

    // BETWEEN variant: same effective window [120, 134].
    auto mask_bw = eval_filter_bitmap_cpu(fs,
        "[{\"col\":-1,\"op\":\"between\",\"lo\":120,\"hi\":134}]",
        0, N, hv);
    for (uint64_t i = 0; i < N; ++i) {
        int64_t id = host_ids[i];
        uint32_t want = (id >= 120 && id <= 134) ? 1u : 0u;
        ASSERT_EQ(get_bit(mask_bw, i), want);
    }
}

TEST(EvalFilterBitmapTest, HostIdsVirtualColumn_IsNullNonNull) {
    // PKs are non-nullable. IS_NULL → no row passes; IS_NOT_NULL → all rows
    // within the window pass (tail bits past num_rows stay zero).
    std::vector<int64_t> host_ids{1, 2, 3, 4, 5};
    FilterStore fs;
    HostIdsView hv{host_ids.data(), FilterColType::INT64, host_ids.size()};

    auto mask_null = eval_filter_bitmap_cpu(fs,
        "[{\"col\":-1,\"op\":\"is_null\"}]", 0, 5, hv);
    ASSERT_EQ(mask_null.size(), 1u);
    ASSERT_EQ(mask_null[0], 0u);

    auto mask_nn = eval_filter_bitmap_cpu(fs,
        "[{\"col\":-1,\"op\":\"is_not_null\"}]", 0, 5, hv);
    ASSERT_EQ(mask_nn.size(), 1u);
    ASSERT_EQ(mask_nn[0], 0b11111u);  // 5 low bits set, rest zeroed
}

TEST(EvalFilterBitmapTest, HostIdsVirtualColumn_EmptyViewPassesThrough) {
    // When the index has sequential IDs (host_ids empty), any PK predicate
    // that sneaks through must behave as "all match" — the planner's
    // residual filter on the scan is still authoritative.
    FilterStore fs;
    HostIdsView hv;  // default-constructed: data=nullptr, count=0
    auto mask = eval_filter_bitmap_cpu(fs,
        "[{\"col\":-1,\"op\":\"=\",\"val\":42}]",
        0, /*num_rows=*/10, hv);

    ASSERT_EQ(mask.size(), 1u);
    // Expect 10 low bits set; high bits zero.
    ASSERT_EQ(mask[0], (1u << 10) - 1u);
}

TEST(EvalFilterBitmapTest, HostIdsVirtualColumn_AndWithFilterStoreColumn) {
    // Combine a PK predicate with a FilterStore-column predicate. Mirrors
    // the common MO case: SELECT ... WHERE id >= 30 AND cat = 2.
    // FilterStore: cat cycles 0..4 over 40 rows; host_ids[i] = 10 + i.
    FilterStore fs = make_store_i32_f32(40);
    std::vector<int64_t> host_ids(40);
    for (uint64_t i = 0; i < 40; ++i) host_ids[i] = static_cast<int64_t>(10 + i);
    HostIdsView hv{host_ids.data(), FilterColType::INT64, host_ids.size()};

    // id >= 30  AND  cat = 2.  host_ids[i] = 10+i, so id>=30 ⇔ i>=20.
    // cat = (i % 5), so cat=2 ⇔ i%5==2.
    auto mask = eval_filter_bitmap_cpu(fs,
        "[{\"col\":-1,\"op\":\">=\",\"val\":30},"
        " {\"col\":1, \"op\":\"=\", \"val\":2}]",
        0, 40, hv);

    for (uint64_t i = 0; i < 40; ++i) {
        bool pass = (i >= 20) && ((i % 5) == 2);
        ASSERT_EQ(get_bit(mask, i), pass ? 1u : 0u);
    }
}

TEST(EvalFilterBitmapTest, HostIdsVirtualColumn_ShardSliceStartRow) {
    // SHARDED mode passes start_row = shard_offset and num_rows = shard_sz;
    // host_ids is the GLOBAL pointer, addressed as hv.data[start_row + base + k]
    // inside eval_pred_word. This test simulates a second shard starting at
    // row 64 (word-aligned) with 32 rows.
    constexpr uint64_t GLOBAL_N = 96;
    std::vector<int64_t> host_ids(GLOBAL_N);
    for (uint64_t i = 0; i < GLOBAL_N; ++i) host_ids[i] = static_cast<int64_t>(i);
    FilterStore fs;
    HostIdsView hv{host_ids.data(), FilterColType::INT64, host_ids.size()};

    // Window [64, 96): filter id >= 80. Local row r corresponds to global
    // row 64 + r; bit should be 1 when (64 + r) >= 80, i.e. r >= 16.
    auto mask = eval_filter_bitmap_cpu(fs,
        "[{\"col\":-1,\"op\":\">=\",\"val\":80}]",
        /*start_row=*/64, /*num_rows=*/32, hv);

    ASSERT_EQ(mask.size(), 1u);
    for (uint64_t r = 0; r < 32; ++r) {
        uint32_t want = (r >= 16) ? 1u : 0u;
        ASSERT_EQ(get_bit(mask, r), want);
    }
}

// =============================================================================
// Large parallel eval — sanity-check OpenMP path produces same result.
// =============================================================================

TEST(EvalFilterBitmapTest, LargeParallelMatchesSerialReference) {
    constexpr uint64_t N = 100000;
    FilterStore fs;
    fs.init({{"v", FilterColType::INT64, 0}}, N);
    std::vector<int64_t> v(N);
    std::mt19937_64 rng(42);
    for (uint64_t i = 0; i < N; ++i) v[i] = static_cast<int64_t>(rng() % 1000);
    fs.add_chunk(0, v.data(), nullptr, N);

    auto mask = eval_filter_bitmap_cpu(fs,
        "[{\"col\":0,\"op\":\"between\",\"lo\":200,\"hi\":700}]", 0, N);

    // Verify against a straight serial reference.
    uint64_t matches = 0;
    for (uint64_t i = 0; i < N; ++i) {
        bool want = v[i] >= 200 && v[i] <= 700;
        uint32_t got = get_bit(mask, i);
        ASSERT_EQ(got, want ? 1u : 0u);
        if (want) ++matches;
    }
    ASSERT_GE(matches, 10001u);  // sanity: we expect roughly half to match
}

// =============================================================================
// gpu_index_base_t — filter ingest + persistence (no GPU / no worker)
//
// We exercise the CPU-only paths: set_filter_columns, add_filter_chunk,
// save_common_components, load_common_components, read_manifest, write_manifest.
// =============================================================================

namespace {

// Minimal derived index used as a stand-in for the real index types. We never
// call start()/build()/search() — only the filter ingest + persistence methods.
struct test_index_t : public gpu_index_base_t<float, int, int64_t> {
    test_index_t() {
        // Populate the fields write_manifest reads so the file is valid JSON.
        this->dimension = 4;
        this->metric    = static_cast<distance_type_t>(0);
        this->dist_mode = static_cast<distribution_mode_t>(0);
        this->devices_  = {0};
    }
    void set_current_offset(uint64_t v) { this->current_offset_ = v; }
};

std::string make_tmp_dir(const std::string& tag) {
    std::string path = "/tmp/mo_filter_test_dir_" + std::to_string(::getpid()) +
                       "_" + tag;
    // Best-effort cleanup from prior runs.
    std::string rm = "rm -rf " + path;
    ::system(rm.c_str());
    gpu_index_base_t<float, int, int64_t>::ensure_dir(path);
    return path;
}

}  // namespace

TEST(IndexBaseFilterTest, SetFilterColumnsPopulatesStore) {
    test_index_t idx;
    idx.set_filter_columns(
        "[{\"name\":\"price\",\"type\":2},{\"name\":\"cat\",\"type\":1}]",
        /*total_count=*/32);

    ASSERT_EQ(idx.filter_host_.columns.size(), 2u);
    ASSERT_EQ(idx.filter_host_.columns[0].name, std::string("price"));
    ASSERT_EQ(static_cast<int>(idx.filter_host_.columns[0].type),
              static_cast<int>(FilterColType::FLOAT32));
    ASSERT_EQ(idx.filter_host_.columns[1].name, std::string("cat"));
    ASSERT_EQ(static_cast<int>(idx.filter_host_.columns[1].type),
              static_cast<int>(FilterColType::INT64));
    ASSERT_EQ(idx.filter_host_.capacity, 32u);
    ASSERT_EQ(idx.filter_host_.count,    0u);
}

TEST(IndexBaseFilterTest, AddFilterChunkAccumulatesLockstep) {
    test_index_t idx;
    idx.set_filter_columns(
        "[{\"name\":\"a\",\"type\":0},{\"name\":\"b\",\"type\":1}]", 10);

    std::vector<int32_t> a{1, 2, 3};
    std::vector<int64_t> b{10, 20, 30};
    idx.add_filter_chunk(0, a.data(), nullptr, 3);
    ASSERT_EQ(idx.filter_host_.count, 0u);  // col 0 ahead, col 1 empty
    idx.add_filter_chunk(1, b.data(), nullptr, 3);
    ASSERT_EQ(idx.filter_host_.count, 3u);

    std::vector<int32_t> a2{4, 5};
    std::vector<int64_t> b2{40, 50};
    idx.add_filter_chunk(0, a2.data(), nullptr, 2);
    idx.add_filter_chunk(1, b2.data(), nullptr, 2);
    ASSERT_EQ(idx.filter_host_.count, 5u);
    ASSERT_EQ(*reinterpret_cast<const int32_t*>(idx.filter_host_.row_ptr(0, 4)), 5);
    ASSERT_EQ(*reinterpret_cast<const int64_t*>(idx.filter_host_.row_ptr(1, 4)), 50);
}

TEST(IndexBaseFilterTest, IngestThrowsAfterIsLoaded) {
    test_index_t idx;
    idx.set_filter_columns("[{\"name\":\"a\",\"type\":0}]", 4);
    idx.is_loaded_ = true;  // simulate post-build

    int32_t v = 7;
    ASSERT_THROW(idx.add_filter_chunk(0, &v, nullptr, 1), std::runtime_error);
    ASSERT_THROW(
        idx.set_filter_columns("[{\"name\":\"b\",\"type\":1}]", 8),
        std::runtime_error);
}

TEST(IndexBaseFilterTest, ManifestSaveLoadRoundtripIncludesFilter) {
    auto dir = make_tmp_dir("manifest");

    {
        test_index_t src;
        src.set_filter_columns(
            "[{\"name\":\"price\",\"type\":2},{\"name\":\"cat\",\"type\":1}]",
            /*total_count=*/5);

        std::vector<float>   prices{1.0f, 2.0f, 3.0f, 4.0f, 5.0f};
        std::vector<int64_t> cats{100, 200, 300, 400, 500};
        src.add_filter_chunk(0, prices.data(), nullptr, 5);
        src.add_filter_chunk(1, cats.data(), nullptr, 5);

        // Pretend build is done so the manifest is sensible.
        src.count = 5;
        src.set_current_offset(5);

        auto entries = src.save_common_components(dir);
        src.write_manifest(dir, "test_index", "", entries);
    }

    test_index_t dst;
    auto m = dst.read_manifest(dir, "test_index");
    ASSERT_TRUE(m.has_filter);
    ASSERT_FALSE(m.has_ids);
    ASSERT_FALSE(m.has_bitset);

    dst.load_common_components(dir, m);
    ASSERT_EQ(dst.filter_host_.columns.size(), 2u);
    ASSERT_EQ(dst.filter_host_.count, 5u);
    ASSERT_EQ(dst.filter_host_.columns[0].name, std::string("price"));
    ASSERT_EQ(*reinterpret_cast<const float*>(dst.filter_host_.row_ptr(0, 4)), 5.0f);
    ASSERT_EQ(*reinterpret_cast<const int64_t*>(dst.filter_host_.row_ptr(1, 2)), 300);

    std::string rm = "rm -rf " + dir;
    ::system(rm.c_str());
}

TEST(IndexBaseFilterTest, ManifestOmitsFilterWhenEmpty) {
    auto dir = make_tmp_dir("nofilter");
    {
        test_index_t src;
        src.count = 0;
        src.set_current_offset(0);
        auto entries = src.save_common_components(dir);
        src.write_manifest(dir, "test_index", "", entries);
    }

    test_index_t dst;
    auto m = dst.read_manifest(dir, "test_index");
    ASSERT_FALSE(m.has_filter);

    // load_common_components on a manifest without filter must be a no-op for filter.
    dst.load_common_components(dir, m);
    ASSERT_TRUE(dst.filter_host_.empty());

    std::string rm = "rm -rf " + dir;
    ::system(rm.c_str());
}
