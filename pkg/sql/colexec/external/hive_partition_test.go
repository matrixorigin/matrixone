// Copyright 2024 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package external

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- ParseHivePartitionSegment tests ---

func TestParseHivePartitionSegment_Valid(t *testing.T) {
	seg, isHive, err := ParseHivePartitionSegment("year=2024")
	require.NoError(t, err)
	assert.True(t, isHive)
	assert.Equal(t, "year", seg.Key)
	assert.Equal(t, "2024", seg.Value)
}

func TestParseHivePartitionSegment_EmptyValue(t *testing.T) {
	seg, isHive, err := ParseHivePartitionSegment("year=")
	require.NoError(t, err)
	assert.True(t, isHive)
	assert.Equal(t, "year", seg.Key)
	assert.Equal(t, "", seg.Value)
}

func TestParseHivePartitionSegment_DecodesPercentValue(t *testing.T) {
	seg, isHive, err := ParseHivePartitionSegment("country=US%2FCA")
	require.NoError(t, err)
	assert.True(t, isHive)
	assert.Equal(t, "country", seg.Key)
	assert.Equal(t, "US/CA", seg.Value)
}

func TestParseHivePartitionSegment_DecodesOnce(t *testing.T) {
	seg, isHive, err := ParseHivePartitionSegment("country=US%252FCA")
	require.NoError(t, err)
	assert.True(t, isHive)
	assert.Equal(t, "US%2FCA", seg.Value)
}

func TestParseHivePartitionSegment_DecodesSpaceAndUnicode(t *testing.T) {
	seg, isHive, err := ParseHivePartitionSegment("city=New%20York")
	require.NoError(t, err)
	assert.True(t, isHive)
	assert.Equal(t, "New York", seg.Value)

	seg, isHive, err = ParseHivePartitionSegment("city=%E4%B8%8A%E6%B5%B7")
	require.NoError(t, err)
	assert.True(t, isHive)
	assert.Equal(t, "上海", seg.Value)
}

func TestParseHivePartitionSegment_NotPartition(t *testing.T) {
	_, isHive, err := ParseHivePartitionSegment("data.parquet")
	require.NoError(t, err)
	assert.False(t, isHive)
}

func TestParseHivePartitionSegment_StartsWithEquals(t *testing.T) {
	_, isHive, err := ParseHivePartitionSegment("=value")
	require.NoError(t, err)
	assert.False(t, isHive)
}

func TestParseHivePartitionSegment_InvalidPercentLiteral(t *testing.T) {
	_, isHive, err := ParseHivePartitionSegment("country=US%ZZ")
	assert.True(t, isHive)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid URL escape")
	assert.Contains(t, err.Error(), "US%ZZ")
}

func TestParseHivePartitionSegment_DefaultPartition(t *testing.T) {
	seg, isHive, err := ParseHivePartitionSegment("year=__HIVE_DEFAULT_PARTITION__")
	require.NoError(t, err)
	assert.True(t, isHive)
	assert.Equal(t, "__HIVE_DEFAULT_PARTITION__", seg.Value)
}

func TestParseHivePartitionSegment_RejectsUnsafeNames(t *testing.T) {
	cases := []string{
		"year=..",
		"year=.",
		"ye-ar=2024",
		"year=2024/05",
		"year=2024\\05",
		"year=2024\n",
		"year=2024\x00",
	}
	for _, tc := range cases {
		_, isHive, err := ParseHivePartitionSegment(tc)
		require.True(t, isHive, tc)
		require.Error(t, err, tc)
	}
}

// --- ExtractPartitionValues tests ---

func TestExtractPartitionValues_SingleLevel(t *testing.T) {
	vals, err := ExtractPartitionValues(
		"/warehouse/data/year=2024/file.parquet",
		"/warehouse/data",
		[]string{"year"},
	)
	require.NoError(t, err)
	assert.Equal(t, "2024", vals["year"])
}

func TestExtractPartitionValues_MultiLevel(t *testing.T) {
	vals, err := ExtractPartitionValues(
		"/warehouse/data/year=2024/month=05/file.parquet",
		"/warehouse/data",
		[]string{"year", "month"},
	)
	require.NoError(t, err)
	assert.Equal(t, "2024", vals["year"])
	assert.Equal(t, "05", vals["month"])
}

func TestExtractPartitionValues_NormalizePath(t *testing.T) {
	tests := []struct {
		filePath string
		basePath string
	}{
		{"warehouse/data/year=2025/f.parquet", "warehouse/data"},
		{"/warehouse/data/year=2025/f.parquet", "/warehouse/data"},
		{"warehouse/data/year=2025/f.parquet", "/warehouse/data"},
		{"/warehouse/data/year=2025/f.parquet", "warehouse/data"},
		{"  /warehouse/data/year=2025/f.parquet  ", "  warehouse/data  "},
	}
	for _, tt := range tests {
		vals, err := ExtractPartitionValues(tt.filePath, tt.basePath, []string{"year"})
		require.NoError(t, err, "filePath=%q basePath=%q", tt.filePath, tt.basePath)
		assert.Equal(t, "2025", vals["year"], "filePath=%q basePath=%q", tt.filePath, tt.basePath)
	}
}

func TestExtractPartitionValues_PrefixCollision(t *testing.T) {
	_, err := ExtractPartitionValues(
		"/warehouse/data2/year=2025/f.parquet",
		"/warehouse/data",
		[]string{"year"},
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not under base path")
}

// --- IsHiddenFile tests ---

func TestIsHiddenFile(t *testing.T) {
	assert.True(t, IsHiddenFile(".hidden"))
	assert.True(t, IsHiddenFile("_SUCCESS"))
	assert.True(t, IsHiddenFile("_metadata"))
	assert.False(t, IsHiddenFile("year=2024"))
	assert.False(t, IsHiddenFile("data.parquet"))
	assert.False(t, IsHiddenFile(""))
}

// --- IsParquetFile tests ---

func TestIsParquetFile(t *testing.T) {
	assert.True(t, IsParquetFile("data.parquet"))
	assert.True(t, IsParquetFile("data.snappy.parquet"))
	assert.True(t, IsParquetFile("DATA.PARQUET"))
	assert.False(t, IsParquetFile("data.csv"))
	assert.False(t, IsParquetFile("data.parquet.crc"))
	assert.False(t, IsParquetFile(""))
}

// --- matchPartitionValue tests ---

func TestMatchPartitionValue_IntMatch(t *testing.T) {
	ct := tree.HivePartColType{Id: int32(types.T_int32)}
	assert.Equal(t, MatchTrue, matchPartitionValue("2024", []string{"2024"}, ct))
	assert.Equal(t, MatchFalse, matchPartitionValue("2024", []string{"2025"}, ct))
	assert.Equal(t, MatchTrue, matchPartitionValue("2024", []string{"2023", "2024"}, ct))
}

func TestMatchPartitionValue_IntOverflow(t *testing.T) {
	ct := tree.HivePartColType{Id: int32(types.T_int8)}
	assert.Equal(t, MatchUnknown, matchPartitionValue("999", []string{"999"}, ct))
}

func TestMatchPartitionValue_IntParseError(t *testing.T) {
	ct := tree.HivePartColType{Id: int32(types.T_int32)}
	assert.Equal(t, MatchUnknown, matchPartitionValue("abc", []string{"123"}, ct))
}

func TestMatchPartitionValue_UintMatch(t *testing.T) {
	ct := tree.HivePartColType{Id: int32(types.T_uint32)}
	assert.Equal(t, MatchTrue, matchPartitionValue("100", []string{"100"}, ct))
	assert.Equal(t, MatchFalse, matchPartitionValue("100", []string{"200"}, ct))
}

func TestMatchPartitionValue_VarcharExact(t *testing.T) {
	ct := tree.HivePartColType{Id: int32(types.T_varchar)}
	assert.Equal(t, MatchTrue, matchPartitionValue("US", []string{"US"}, ct))
	assert.Equal(t, MatchUnknown, matchPartitionValue("us", []string{"US"}, ct))
}

func TestMatchPartitionValue_UnknownTypes(t *testing.T) {
	unknownTypes := []types.T{
		types.T_bool, types.T_bit, types.T_float32, types.T_float64,
		types.T_decimal64, types.T_decimal128, types.T_date, types.T_time,
		types.T_datetime, types.T_timestamp, types.T_json, types.T_uuid,
		types.T_binary, types.T_varbinary, types.T_blob, types.T_enum,
		types.T_array_float32, types.T_array_float64,
	}
	for _, typ := range unknownTypes {
		ct := tree.HivePartColType{Id: int32(typ)}
		assert.Equal(t, MatchUnknown, matchPartitionValue("val", []string{"val"}, ct),
			"type %v should return MatchUnknown", typ)
	}
}

func TestMatchPartitionValue_TAny(t *testing.T) {
	ct := tree.HivePartColType{Id: int32(types.T_any)}
	assert.Equal(t, MatchUnknown, matchPartitionValue("2024", []string{"2024"}, ct))
}

func TestMatchPartitionValue_ZeroPaddedInt(t *testing.T) {
	ct := tree.HivePartColType{Id: int32(types.T_int32)}
	assert.Equal(t, MatchTrue, matchPartitionValue("01", []string{"1"}, ct))
	assert.Equal(t, MatchTrue, matchPartitionValue("007", []string{"7"}, ct))
}

func TestMatchPartitionValue_ZeroPaddedVarcharConservative(t *testing.T) {
	ct := tree.HivePartColType{Id: int32(types.T_varchar)}
	assert.Equal(t, MatchTrue, matchPartitionValue("01", []string{"01"}, ct))
	assert.Equal(t, MatchUnknown, matchPartitionValue("01", []string{"1"}, ct),
		"varchar partitions keep string semantics; a mismatch is not pruned away")
}

func TestMatchPartitionCompare_IntRange(t *testing.T) {
	ct := tree.HivePartColType{Id: int32(types.T_int32)}
	assert.Equal(t, MatchTrue, matchPartitionCompare("-5", []string{"-10"}, ct, PartOpGt))
	assert.Equal(t, MatchFalse, matchPartitionCompare("-20", []string{"-10"}, ct, PartOpGt))
	assert.Equal(t, MatchTrue, matchPartitionCompare("0", []string{"0"}, ct, PartOpGe))
	assert.Equal(t, MatchTrue, matchPartitionCompare("01", []string{"2"}, ct, PartOpLt))
}

func TestMatchPartitionCompare_UintRange(t *testing.T) {
	ct := tree.HivePartColType{Id: int32(types.T_uint32)}
	assert.Equal(t, MatchTrue, matchPartitionCompare("0", []string{"0"}, ct, PartOpGe))
	assert.Equal(t, MatchFalse, matchPartitionCompare("0", []string{"1"}, ct, PartOpGe))
	assert.Equal(t, MatchUnknown, matchPartitionCompare("1", []string{"-1"}, ct, PartOpGt))
}

func TestMatchPartitionCompare_OverflowAndStringUnknown(t *testing.T) {
	int8Type := tree.HivePartColType{Id: int32(types.T_int8)}
	assert.Equal(t, MatchUnknown, matchPartitionCompare("128", []string{"1"}, int8Type, PartOpGt))
	assert.Equal(t, MatchUnknown, matchPartitionCompare("1", []string{"128"}, int8Type, PartOpGt))

	strType := tree.HivePartColType{Id: int32(types.T_varchar)}
	assert.Equal(t, MatchUnknown, matchPartitionCompare("b", []string{"a"}, strType, PartOpGt))
}

func TestMatchPartitionRange_UnsupportedTypesUnknown(t *testing.T) {
	unsupportedTypes := []types.T{
		types.T_bool, types.T_bit, types.T_float32, types.T_float64,
		types.T_decimal64, types.T_decimal128, types.T_date, types.T_time,
		types.T_datetime, types.T_timestamp, types.T_json, types.T_uuid,
		types.T_binary, types.T_varbinary, types.T_blob, types.T_enum,
		types.T_array_float32, types.T_array_float64,
	}
	for _, typ := range unsupportedTypes {
		ct := tree.HivePartColType{Id: int32(typ)}
		assert.Equal(t, MatchUnknown, matchPartitionRange("1", []string{"0", "2"}, ct),
			"type %v should return MatchUnknown", typ)
	}
}

func TestMatchPartitionNull(t *testing.T) {
	assert.Equal(t, MatchTrue, matchPartitionNull(HiveDefaultPartition, true))
	assert.Equal(t, MatchFalse, matchPartitionNull("2024", true))
	assert.Equal(t, MatchFalse, matchPartitionNull(HiveDefaultPartition, false))
	assert.Equal(t, MatchTrue, matchPartitionNull("2024", false))
}

// --- DiscoverHivePartitions tests ---

func mockListDir(dirs map[string][]fileservice.DirEntry) ListDirFunc {
	return func(ctx context.Context, prefix string) iter.Seq2[*fileservice.DirEntry, error] {
		return func(yield func(*fileservice.DirEntry, error) bool) {
			entries := dirs[prefix]
			for i := range entries {
				if !yield(&entries[i], nil) {
					return
				}
			}
		}
	}
}

func TestDiscoverHivePartitions_SingleLevel(t *testing.T) {
	dirs := map[string][]fileservice.DirEntry{
		"/data": {
			{Name: "year=2024", IsDir: true},
			{Name: "year=2025", IsDir: true},
			{Name: "_SUCCESS", IsDir: false},
		},
		"/data/year=2024": {
			{Name: "part-0000.parquet", IsDir: false, Size: 1000},
			{Name: ".hidden", IsDir: false, Size: 100},
		},
		"/data/year=2025": {
			{Name: "part-0000.parquet", IsDir: false, Size: 2000},
		},
	}

	result, err := DiscoverHivePartitions(
		context.Background(),
		mockListDir(dirs),
		"/data",
		[]string{"year"},
		[]tree.HivePartColType{{Id: int32(types.T_int32)}},
		nil,
	)
	require.NoError(t, err)
	assert.Equal(t, 2, result.PartitionCount)
	assert.Equal(t, 2, len(result.Files))
	assert.Equal(t, 2, result.DiscoveredFiles)
	assert.Equal(t, 2, result.PrunedFiles)
	assert.Equal(t, int64(3000), result.DiscoveredBytes)
	assert.Equal(t, int64(3000), result.PrunedBytes)
	assert.Equal(t, int64(1000), result.Files[0].FileSize)
}

func TestDiscoverHivePartitions_MultiLevel(t *testing.T) {
	dirs := map[string][]fileservice.DirEntry{
		"/data": {
			{Name: "year=2024", IsDir: true},
		},
		"/data/year=2024": {
			{Name: "month=01", IsDir: true},
			{Name: "month=02", IsDir: true},
		},
		"/data/year=2024/month=01": {
			{Name: "data.parquet", IsDir: false, Size: 500},
		},
		"/data/year=2024/month=02": {
			{Name: "data.parquet", IsDir: false, Size: 600},
		},
	}

	result, err := DiscoverHivePartitions(
		context.Background(),
		mockListDir(dirs),
		"/data",
		[]string{"year", "month"},
		[]tree.HivePartColType{
			{Id: int32(types.T_int32)},
			{Id: int32(types.T_int32)},
		},
		nil,
	)
	require.NoError(t, err)
	assert.Equal(t, 3, result.PartitionCount) // year=2024 + month=01 + month=02
	assert.Equal(t, 2, len(result.Files))
}

func TestDiscoverHivePartitions_WithPredicate(t *testing.T) {
	dirs := map[string][]fileservice.DirEntry{
		"/data": {
			{Name: "year=2024", IsDir: true},
			{Name: "year=2025", IsDir: true},
			{Name: "year=2026", IsDir: true},
		},
		"/data/year=2025": {
			{Name: "data.parquet", IsDir: false, Size: 1000},
		},
	}

	result, err := DiscoverHivePartitions(
		context.Background(),
		mockListDir(dirs),
		"/data",
		[]string{"year"},
		[]tree.HivePartColType{{Id: int32(types.T_int32)}},
		[]PartitionPredicate{{ColName: "year", Op: PartOpEq, Values: []string{"2025"}}},
	)
	require.NoError(t, err)
	assert.Equal(t, 1, result.PartitionCount)
	assert.Equal(t, 2, result.PrunedCount)
	assert.Equal(t, 1, len(result.Files))
	assert.Equal(t, 2, result.ListCalls) // root dir + year=2025 file listing
}

func TestDiscoverHivePartitions_SkipsHidden(t *testing.T) {
	dirs := map[string][]fileservice.DirEntry{
		"/data": {
			{Name: "year=2024", IsDir: true},
			{Name: ".metadata", IsDir: true},
			{Name: "_temp", IsDir: true},
		},
		"/data/year=2024": {
			{Name: "data.parquet", IsDir: false, Size: 100},
			{Name: "_SUCCESS", IsDir: false, Size: 0},
			{Name: ".crc", IsDir: false, Size: 10},
		},
	}

	result, err := DiscoverHivePartitions(
		context.Background(),
		mockListDir(dirs),
		"/data",
		[]string{"year"},
		[]tree.HivePartColType{{Id: int32(types.T_int32)}},
		nil,
	)
	require.NoError(t, err)
	assert.Equal(t, 1, len(result.Files))
}

func TestDiscoverHivePartitions_NormalizePath(t *testing.T) {
	dirs := map[string][]fileservice.DirEntry{
		"/warehouse/data": {
			{Name: "year=2024", IsDir: true},
		},
		"/warehouse/data/year=2024": {
			{Name: "f.parquet", IsDir: false, Size: 100},
		},
	}

	result, err := DiscoverHivePartitions(
		context.Background(),
		mockListDir(dirs),
		"warehouse/data/",
		[]string{"year"},
		[]tree.HivePartColType{{Id: int32(types.T_int32)}},
		nil,
	)
	require.NoError(t, err)
	assert.Equal(t, 1, len(result.Files))
}

func TestDiscoverHivePartitions_NilColTypes(t *testing.T) {
	dirs := map[string][]fileservice.DirEntry{
		"/data": {
			{Name: "year=2024", IsDir: true},
			{Name: "year=2025", IsDir: true},
		},
		"/data/year=2024": {
			{Name: "f.parquet", IsDir: false, Size: 100},
		},
		"/data/year=2025": {
			{Name: "f.parquet", IsDir: false, Size: 200},
		},
	}

	// nil colTypes means old JSON — should still discover all (no pruning possible)
	result, err := DiscoverHivePartitions(
		context.Background(),
		mockListDir(dirs),
		"/data",
		[]string{"year"},
		nil, // triggers T_any fallback
		[]PartitionPredicate{{ColName: "year", Op: PartOpEq, Values: []string{"2024"}}},
	)
	require.NoError(t, err)
	// T_any → MatchUnknown → no pruning, all partitions kept
	assert.Equal(t, 2, result.PartitionCount)
	assert.Equal(t, 0, result.PrunedCount)
	assert.Equal(t, 2, len(result.Files))
}

func TestDiscoverHivePartitions_DecodedPercentInDirName(t *testing.T) {
	dirs := map[string][]fileservice.DirEntry{
		"/data": {
			{Name: "country=US%2FCA", IsDir: true},
		},
		"/data/country=US%2FCA": {
			{Name: "f.parquet", IsDir: false, Size: 100},
		},
	}

	result, err := DiscoverHivePartitions(
		context.Background(),
		mockListDir(dirs),
		"/data",
		[]string{"country"},
		[]tree.HivePartColType{{Id: int32(types.T_varchar)}},
		nil,
	)
	require.NoError(t, err)
	require.Len(t, result.Files, 1)
	assert.Equal(t, "/data/country=US%2FCA/f.parquet", result.Files[0].FilePath)
	assert.Equal(t, 1, result.PartitionCount)
}

func TestDiscoverHivePartitions_RejectsUnsafeDirName(t *testing.T) {
	dirs := map[string][]fileservice.DirEntry{
		"/data": {
			{Name: "year=..", IsDir: true},
		},
	}

	_, err := DiscoverHivePartitions(
		context.Background(),
		mockListDir(dirs),
		"/data",
		[]string{"year"},
		[]tree.HivePartColType{{Id: int32(types.T_varchar)}},
		nil,
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "path traversal")
}

func TestDiscoverHivePartitions_INPredicate(t *testing.T) {
	dirs := map[string][]fileservice.DirEntry{
		"/data": {
			{Name: "year=2023", IsDir: true},
			{Name: "year=2024", IsDir: true},
			{Name: "year=2025", IsDir: true},
		},
		"/data/year=2024": {
			{Name: "f.parquet", IsDir: false, Size: 100},
		},
		"/data/year=2025": {
			{Name: "f.parquet", IsDir: false, Size: 200},
		},
	}

	result, err := DiscoverHivePartitions(
		context.Background(),
		mockListDir(dirs),
		"/data",
		[]string{"year"},
		[]tree.HivePartColType{{Id: int32(types.T_int32)}},
		[]PartitionPredicate{{ColName: "year", Op: PartOpIn, Values: []string{"2024", "2025"}}},
	)
	require.NoError(t, err)
	assert.Equal(t, 2, result.PartitionCount)
	assert.Equal(t, 1, result.PrunedCount)
	assert.Equal(t, 2, len(result.Files))
	assert.Equal(t, 3, result.ListCalls) // root + year=2024 files + year=2025 files
}

func TestDiscoverHivePartitions_KeyMismatchSkipped(t *testing.T) {
	dirs := map[string][]fileservice.DirEntry{
		"/data": {
			{Name: "year=2024", IsDir: true},
			{Name: "country=US", IsDir: true}, // wrong key for level 0
		},
		"/data/year=2024": {
			{Name: "f.parquet", IsDir: false, Size: 100},
		},
	}

	result, err := DiscoverHivePartitions(
		context.Background(),
		mockListDir(dirs),
		"/data",
		[]string{"year"},
		[]tree.HivePartColType{{Id: int32(types.T_int32)}},
		nil,
	)
	require.NoError(t, err)
	assert.Equal(t, 1, result.PartitionCount)
	assert.Equal(t, 1, len(result.Files))
}

func TestDiscoverHivePartitions_ListCallLimit(t *testing.T) {
	// Generate enough partitions at two levels to exceed maxListCalls (10000).
	// Level 0: 200 year partitions, Level 1: 200 month partitions each.
	// This requires 1 (root) + 200 (year dirs) = 201 List calls before we hit month level.
	// To trigger the limit efficiently, use a mock that always returns entries
	// forcing recursion well beyond the limit.
	entries := make([]fileservice.DirEntry, 200)
	for i := range entries {
		entries[i] = fileservice.DirEntry{Name: fmt.Sprintf("year=%d", i), IsDir: true}
	}
	monthEntries := make([]fileservice.DirEntry, 200)
	for i := range monthEntries {
		monthEntries[i] = fileservice.DirEntry{Name: fmt.Sprintf("month=%d", i), IsDir: true}
	}
	fileEntries := []fileservice.DirEntry{{Name: "f.parquet", IsDir: false, Size: 10}}

	listDir := func(ctx context.Context, prefix string) iter.Seq2[*fileservice.DirEntry, error] {
		return func(yield func(*fileservice.DirEntry, error) bool) {
			var items []fileservice.DirEntry
			if strings.Count(prefix, "/") <= 1 {
				items = entries
			} else if strings.Contains(prefix, "month=") {
				items = fileEntries
			} else {
				items = monthEntries
			}
			for i := range items {
				if !yield(&items[i], nil) {
					return
				}
			}
		}
	}

	_, err := DiscoverHivePartitions(
		context.Background(),
		listDir,
		"/data",
		[]string{"year", "month"},
		[]tree.HivePartColType{
			{Id: int32(types.T_int32)},
			{Id: int32(types.T_int32)},
		},
		nil,
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "List calls")
}

func TestDiscoverHivePartitions_WorkerErrorCancelsOtherWorkers(t *testing.T) {
	wantErr := errors.New("boom")
	slowStarted := make(chan struct{})
	slowCanceled := make(chan struct{})
	var closeSlowStarted sync.Once
	var closeSlowCanceled sync.Once

	listDir := func(ctx context.Context, prefix string) iter.Seq2[*fileservice.DirEntry, error] {
		return func(yield func(*fileservice.DirEntry, error) bool) {
			switch prefix {
			case "/data":
				yield(&fileservice.DirEntry{Name: "year=2024", IsDir: true}, nil)
				yield(&fileservice.DirEntry{Name: "year=2025", IsDir: true}, nil)
			case "/data/year=2024":
				select {
				case <-slowStarted:
				case <-time.After(time.Second):
					yield(nil, errors.New("slow worker did not start"))
					return
				}
				yield(nil, wantErr)
			case "/data/year=2025":
				closeSlowStarted.Do(func() { close(slowStarted) })
				<-ctx.Done()
				closeSlowCanceled.Do(func() { close(slowCanceled) })
				yield(nil, context.Cause(ctx))
			}
		}
	}

	_, err := DiscoverHivePartitionsWithPruneExpr(
		context.Background(),
		listDir,
		"/data",
		[]string{"year"},
		[]tree.HivePartColType{{Id: int32(types.T_int32)}},
		nil,
		&DiscoverOptions{ListConcurrency: 2},
	)

	require.Error(t, err)
	assert.ErrorIs(t, err, wantErr)
	select {
	case <-slowCanceled:
	case <-time.After(time.Second):
		t.Fatal("slow worker was not canceled after peer worker failed")
	}
}

func TestDiscoverHivePartitions_ContextCancellationStopsWorkers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	allStarted := make(chan struct{})
	var closeAllStarted sync.Once
	var canceledWorkers int32
	var startedWorkers int32

	go func() {
		<-allStarted
		cancel()
	}()

	listDir := func(ctx context.Context, prefix string) iter.Seq2[*fileservice.DirEntry, error] {
		return func(yield func(*fileservice.DirEntry, error) bool) {
			switch prefix {
			case "/data":
				yield(&fileservice.DirEntry{Name: "year=2024", IsDir: true}, nil)
				yield(&fileservice.DirEntry{Name: "year=2025", IsDir: true}, nil)
			default:
				if atomic.AddInt32(&startedWorkers, 1) == 2 {
					closeAllStarted.Do(func() { close(allStarted) })
				}
				<-ctx.Done()
				atomic.AddInt32(&canceledWorkers, 1)
				yield(nil, context.Cause(ctx))
			}
		}
	}

	_, err := DiscoverHivePartitionsWithPruneExpr(
		ctx,
		listDir,
		"/data",
		[]string{"year"},
		[]tree.HivePartColType{{Id: int32(types.T_int32)}},
		nil,
		&DiscoverOptions{ListConcurrency: 2},
	)

	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, int32(2), atomic.LoadInt32(&canceledWorkers))
}

// ---------------------------------------------------------------------------
// ClassifyFilters tests
// ---------------------------------------------------------------------------

func makeTableDef(cols ...string) *plan.TableDef {
	td := &plan.TableDef{Cols: make([]*plan.ColDef, len(cols))}
	for i, name := range cols {
		td.Cols[i] = &plan.ColDef{Name: name}
	}
	return td
}

func makeColExpr(colPos int32, name string) *plan.Expr {
	return &plan.Expr{
		Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: colPos, Name: name}},
	}
}

func makeLitInt64(val int64) *plan.Expr {
	return &plan.Expr{
		Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I64Val{I64Val: val}}},
	}
}

func makeLitString(val string) *plan.Expr {
	return &plan.Expr{
		Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Sval{Sval: val}}},
	}
}

func makeEqExpr(left, right *plan.Expr) *plan.Expr {
	return &plan.Expr{
		Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{Obj: function.EqualFunctionEncodedID},
			Args: []*plan.Expr{left, right},
		}},
	}
}

func makeInExpr(col *plan.Expr, vals ...*plan.Expr) *plan.Expr {
	return &plan.Expr{
		Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{Obj: function.InFunctionEncodedID},
			Args: []*plan.Expr{
				col,
				{Expr: &plan.Expr_List{List: &plan.ExprList{List: vals}}},
			},
		}},
	}
}

func makeBetweenExpr(col, low, high *plan.Expr) *plan.Expr {
	return &plan.Expr{
		Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{Obj: int64(function.BETWEEN) << 32},
			Args: []*plan.Expr{col, low, high},
		}},
	}
}

func makeFuncExpr(fid int32, args ...*plan.Expr) *plan.Expr {
	return &plan.Expr{
		Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{Obj: int64(fid) << 32},
			Args: args,
		}},
	}
}

func makeAndExpr(args ...*plan.Expr) *plan.Expr {
	return makeFuncExpr(function.AND, args...)
}

func makeOrExpr(args ...*plan.Expr) *plan.Expr {
	return makeFuncExpr(function.OR, args...)
}

func makeUnaryMinusExpr(arg *plan.Expr) *plan.Expr {
	return makeFuncExpr(function.UNARY_MINUS, arg)
}

func requirePartitionAtom(t *testing.T, expr PartitionPruneExpr) *PartitionAtom {
	t.Helper()
	atom, ok := expr.(*PartitionAtom)
	require.True(t, ok, "expected PartitionAtom, got %T", expr)
	return atom
}

func TestClassifyFilters_Basic(t *testing.T) {
	td := makeTableDef("year", "amount", "account", "__mo_filepath")
	partColSet := map[string]bool{"year": true}

	yearEq := makeEqExpr(makeColExpr(0, "year"), makeLitInt64(2025))
	amountGt := &plan.Expr{
		Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{Obj: int64(function.GREAT_THAN) << 32},
			Args: []*plan.Expr{makeColExpr(1, "amount"), makeLitInt64(100)},
		}},
	}
	fpFilter := makeEqExpr(makeColExpr(3, "__mo_filepath"), makeLitString("/path"))

	partF, fpF, rowF := ClassifyFilters(td, []*plan.Expr{yearEq, amountGt, fpFilter}, partColSet)

	assert.Equal(t, 1, len(partF), "year filter should be in partitionFilters")
	assert.Equal(t, 1, len(fpF), "__mo_filepath filter should be in filePathFilters")
	assert.Equal(t, 2, len(rowF), "year+amount should be in rowFilters (year duplicated for safety)")
	assert.Same(t, yearEq, partF[0])
	assert.Same(t, fpFilter, fpF[0])
}

func TestClassifyFilters_PartitionFilterDuplicated(t *testing.T) {
	td := makeTableDef("year", "data")
	partColSet := map[string]bool{"year": true}

	yearEq := makeEqExpr(makeColExpr(0, "year"), makeLitInt64(2025))
	partF, _, rowF := ClassifyFilters(td, []*plan.Expr{yearEq}, partColSet)

	assert.Equal(t, 1, len(partF))
	assert.Equal(t, 1, len(rowF))
	assert.Same(t, partF[0], rowF[0], "partition filter must appear in both lists")
}

func TestClassifyFilters_AccountNamedPartCol(t *testing.T) {
	td := makeTableDef("account", "data")
	partColSet := map[string]bool{"account": true}

	acctEq := makeEqExpr(makeColExpr(0, "account"), makeLitString("tenant1"))
	partF, fpF, _ := ClassifyFilters(td, []*plan.Expr{acctEq}, partColSet)

	assert.Equal(t, 1, len(partF), "account as partition col goes to partitionFilters")
	assert.Equal(t, 0, len(fpF), "should NOT go to filePathFilters")
}

func TestClassifyFilters_AccountIdSubstring(t *testing.T) {
	td := makeTableDef("account_id", "data")
	partColSet := map[string]bool{}

	acctIdEq := makeEqExpr(makeColExpr(0, "account_id"), makeLitString("123"))
	_, fpF, rowF := ClassifyFilters(td, []*plan.Expr{acctIdEq}, partColSet)

	assert.Equal(t, 0, len(fpF), "account_id must NOT be mistaken for filepath filter")
	assert.Equal(t, 1, len(rowF))
}

func TestClassifyFilters_MixedReference(t *testing.T) {
	td := makeTableDef("year", "amount")
	partColSet := map[string]bool{"year": true}

	mixed := &plan.Expr{
		Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{Obj: int64(function.GREAT_THAN) << 32},
			Args: []*plan.Expr{makeColExpr(0, "year"), makeColExpr(1, "amount")},
		}},
	}
	partF, fpF, rowF := ClassifyFilters(td, []*plan.Expr{mixed}, partColSet)

	assert.Equal(t, 0, len(partF))
	assert.Equal(t, 0, len(fpF))
	assert.Equal(t, 1, len(rowF), "mixed reference goes to rowFilters")
}

func TestClassifyFilters_MoFilepathCol(t *testing.T) {
	td := makeTableDef("year", catalog.ExternalFilePath)
	partColSet := map[string]bool{"year": true}

	fpExpr := makeEqExpr(makeColExpr(1, catalog.ExternalFilePath), makeLitString("x"))
	_, fpF, _ := ClassifyFilters(td, []*plan.Expr{fpExpr}, partColSet)

	assert.Equal(t, 1, len(fpF))
}

func TestClassifyFilters_NoColumnRefs(t *testing.T) {
	td := makeTableDef("year")
	partColSet := map[string]bool{"year": true}

	constExpr := makeLitInt64(42)
	_, _, rowF := ClassifyFilters(td, []*plan.Expr{constExpr}, partColSet)

	assert.Equal(t, 1, len(rowF), "constant expression goes to rowFilters")
}

func TestClassifyFilters_FunctionWrappedPartitionColumnConservative(t *testing.T) {
	td := makeTableDef("year", "data")
	partColSet := map[string]bool{"year": true}

	wrappedYear := &plan.Expr{
		Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{Obj: 123, ObjName: "cast"},
			Args: []*plan.Expr{makeColExpr(0, "year")},
		}},
	}
	filter := makeEqExpr(wrappedYear, makeLitInt64(2024))
	partF, fpF, rowF := ClassifyFilters(td, []*plan.Expr{filter}, partColSet)

	require.Len(t, partF, 1, "Expr_F wrappers must still expose the partition column")
	assert.Empty(t, fpF)
	require.Len(t, rowF, 1, "row-level filtering remains the correctness backstop")
	assert.Empty(t, ExtractPartitionPredicatesFromExprs(td, partF, partColSet),
		"CAST/function-wrapped partition columns are not structurally pruned")
}

// TestClassifyFilters_AccountAsPhysicalCol guards against classifying a
// physical column literally named "account" as a filepath pseudo column.
// The CSV-only per-batch "account" virtual column does not exist on Hive /
// Parquet external tables; treating it as such would cause row filters on a
// real column to be silently dropped and evaluated against garbage path
// synthesis.
func TestClassifyFilters_AccountAsPhysicalCol(t *testing.T) {
	td := makeTableDef("account", "amount")
	partColSet := map[string]bool{} // "account" is NOT a partition column

	acctEq := makeEqExpr(makeColExpr(0, "account"), makeLitString("tenant1"))
	partF, fpF, rowF := ClassifyFilters(td, []*plan.Expr{acctEq}, partColSet)

	assert.Equal(t, 0, len(partF))
	assert.Equal(t, 0, len(fpF), "physical column 'account' must NOT be classified as filepath filter")
	assert.Equal(t, 1, len(rowF), "must be evaluated as a normal row filter")
}

// TestClassifyFilters_OrFilepathAndLiteral documents the exact scenario that
// motivated the compile-side fpFilters → rowFilters propagation fix:
// ClassifyFilters routes OR(__mo_filepath LIKE ..., const) to fpFilters
// because both operands' col refs are a subset of filePathColSet (the
// literal contributes no refs). But FilterFileList's judgeContainColname is
// stricter — it rejects OR branches that don't reference a filepath column
// — so the filter comes back unconsumed and must be appended to rowFilters
// by the caller. This test pins the classification half of the contract so
// future ClassifyFilters changes don't silently break the invariant.
func TestClassifyFilters_OrFilepathAndLiteral(t *testing.T) {
	td := makeTableDef("year", catalog.ExternalFilePath)
	partColSet := map[string]bool{"year": true}

	// OR(__mo_filepath = 'x', false-literal) — no columns on the right arm.
	orExpr := &plan.Expr{
		Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{Obj: int64(function.OR) << 32, ObjName: "or"},
			Args: []*plan.Expr{
				makeEqExpr(makeColExpr(1, catalog.ExternalFilePath), makeLitString("x")),
				{Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Bval{Bval: false}}}},
			},
		}},
	}
	_, fpF, _ := ClassifyFilters(td, []*plan.Expr{orExpr}, partColSet)
	assert.Equal(t, 1, len(fpF),
		"OR(filepath, literal) must go to fpFilters; the compile-side caller is responsible for "+
			"re-appending it to rowFilters if FilterFileList refuses to consume it")
}

// TestFilterFileList_LeavesUnconsumedOrFilterInNode locks the exact side-effect
// contract that compile.getHivePartitionFileList depends on: when FilterFileList
// is handed an OR(filepath, literal) filter, its judgeContainColname check
// rejects it (OR branches must each reference a filepath col), and the rejected
// filter is written back via node.FilterList. compile.go appends tmpNode.FilterList
// onto rowFilters so the runtime still evaluates the predicate; without that
// append, the filter is silently dropped. If a future change has FilterFileList
// consume such filters, or uses a different side-effect pattern (e.g. returning
// leftover filters), this test goes red and the compile side must be audited.
func TestFilterFileList_LeavesUnconsumedOrFilterInNode(t *testing.T) {
	proc := testutil.NewProc(t)

	td := makeTableDef("year", catalog.ExternalFilePath)
	orExpr := &plan.Expr{
		Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{Obj: int64(function.OR) << 32, ObjName: "or"},
			Args: []*plan.Expr{
				makeEqExpr(makeColExpr(1, catalog.ExternalFilePath), makeLitString("x")),
				{Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Bval{Bval: false}}}},
			},
		}},
	}
	tmpNode := &plan.Node{
		TableDef:   td,
		FilterList: []*plan.Expr{orExpr},
	}
	fileList := []string{"/warehouse/data/year=2024/f.parquet"}
	fileSize := []int64{123}

	outFileList, outFileSize, err := FilterFileList(proc.Ctx, tmpNode, proc, fileList, fileSize)
	require.NoError(t, err)

	// judgeContainColname rejected the OR, so filterList in filterByAccountAndFilename
	// was empty and the function short-circuited at line 368-370 — fileList / fileSize
	// come back unchanged.
	assert.Equal(t, fileList, outFileList)
	assert.Equal(t, fileSize, outFileSize)

	// And tmpNode.FilterList must still hold the unconsumed predicate. This is
	// what compile.getHivePartitionFileList `append`s back onto rowFilters.
	require.Equal(t, 1, len(tmpNode.FilterList),
		"unconsumed OR(filepath, literal) filter must remain in tmpNode.FilterList")
	assert.Same(t, orExpr, tmpNode.FilterList[0],
		"tmpNode.FilterList must hold the exact expression for compile.go to re-append")
}

// ---------------------------------------------------------------------------
// collectBareColNames tests
// ---------------------------------------------------------------------------

func TestCollectBareColNames_ColPos(t *testing.T) {
	td := makeTableDef("year", "month")
	expr := makeColExpr(0, "catalog_returns.year")
	names := collectBareColNames(td, expr)
	assert.True(t, names["year"], "should resolve via ColPos, not col.Name")
	assert.False(t, names["catalog_returns.year"])
}

func TestCollectBareColNames_FallbackStrip(t *testing.T) {
	td := makeTableDef("year")
	expr := makeColExpr(99, "t.month")
	names := collectBareColNames(td, expr)
	assert.True(t, names["month"], "fallback should strip table prefix")
}

func TestCollectBareColNames_Nested(t *testing.T) {
	td := makeTableDef("year", "month")
	expr := &plan.Expr{
		Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{Obj: int64(function.GREAT_THAN) << 32},
			Args: []*plan.Expr{makeColExpr(0, "year"), makeColExpr(1, "month")},
		}},
	}
	names := collectBareColNames(td, expr)
	assert.True(t, names["year"])
	assert.True(t, names["month"])
}

// ---------------------------------------------------------------------------
// ExtractPartitionPredicatesFromExprs tests
// ---------------------------------------------------------------------------

func TestExtractPartitionPredicates_Eq(t *testing.T) {
	td := makeTableDef("year", "data")
	partColSet := map[string]bool{"year": true}

	yearEq := makeEqExpr(makeColExpr(0, "year"), makeLitInt64(2025))
	preds := ExtractPartitionPredicatesFromExprs(td, []*plan.Expr{yearEq}, partColSet)

	require.Equal(t, 1, len(preds))
	assert.Equal(t, "year", preds[0].ColName)
	assert.Equal(t, PartOpEq, preds[0].Op)
	assert.Equal(t, []string{"2025"}, preds[0].Values)
}

func TestExtractPartitionPredicates_EqReversed(t *testing.T) {
	td := makeTableDef("year", "data")
	partColSet := map[string]bool{"year": true}

	reversed := makeEqExpr(makeLitInt64(2025), makeColExpr(0, "year"))
	preds := ExtractPartitionPredicatesFromExprs(td, []*plan.Expr{reversed}, partColSet)

	require.Equal(t, 1, len(preds))
	assert.Equal(t, "year", preds[0].ColName)
	assert.Equal(t, []string{"2025"}, preds[0].Values)
}

func TestExtractPartitionPredicates_In(t *testing.T) {
	td := makeTableDef("year", "data")
	partColSet := map[string]bool{"year": true}

	inExpr := makeInExpr(makeColExpr(0, "year"), makeLitInt64(2024), makeLitInt64(2025))
	preds := ExtractPartitionPredicatesFromExprs(td, []*plan.Expr{inExpr}, partColSet)

	require.Equal(t, 1, len(preds))
	assert.Equal(t, "year", preds[0].ColName)
	assert.Equal(t, PartOpIn, preds[0].Op)
	assert.Equal(t, []string{"2024", "2025"}, preds[0].Values)
}

func TestExtractPartitionPredicates_InWithStrings(t *testing.T) {
	td := makeTableDef("country", "data")
	partColSet := map[string]bool{"country": true}

	inExpr := makeInExpr(makeColExpr(0, "country"), makeLitString("US"), makeLitString("CA"))
	preds := ExtractPartitionPredicatesFromExprs(td, []*plan.Expr{inExpr}, partColSet)

	require.Equal(t, 1, len(preds))
	assert.Equal(t, []string{"US", "CA"}, preds[0].Values)
}

func TestExtractPartitionPredicates_Between(t *testing.T) {
	td := makeTableDef("year", "data")
	partColSet := map[string]bool{"year": true}

	betweenExpr := makeBetweenExpr(makeColExpr(0, "year"), makeLitInt64(2020), makeLitInt64(2025))
	preds := ExtractPartitionPredicatesFromExprs(td, []*plan.Expr{betweenExpr}, partColSet)

	require.Equal(t, 1, len(preds))
	assert.Equal(t, "year", preds[0].ColName)
	assert.Equal(t, PartOpBetween, preds[0].Op)
	assert.Equal(t, []string{"2020", "2025"}, preds[0].Values)
}

func TestExtractPartitionPredicates_NonStructurable(t *testing.T) {
	td := makeTableDef("year", "data")
	partColSet := map[string]bool{"year": true}

	gtExpr := &plan.Expr{
		Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{Obj: int64(function.GREAT_THAN) << 32},
			Args: []*plan.Expr{makeColExpr(0, "year"), makeLitInt64(2024)},
		}},
	}
	preds := ExtractPartitionPredicatesFromExprs(td, []*plan.Expr{gtExpr}, partColSet)
	assert.Equal(t, 0, len(preds), "non EQ/IN should be silently skipped")
}

func TestExtractPartitionPredicates_RejectsCast(t *testing.T) {
	td := makeTableDef("year", "data")
	partColSet := map[string]bool{"year": true}

	castExpr := &plan.Expr{
		Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{Obj: 123},
			Args: []*plan.Expr{makeLitInt64(2025)},
		}},
	}
	eqWithCast := makeEqExpr(makeColExpr(0, "year"), castExpr)
	preds := ExtractPartitionPredicatesFromExprs(td, []*plan.Expr{eqWithCast}, partColSet)
	assert.Equal(t, 0, len(preds), "Expr_F on constant side should be rejected")
}

func TestExtractPartitionPredicates_NonPartCol(t *testing.T) {
	td := makeTableDef("year", "data")
	partColSet := map[string]bool{"year": true}

	dataEq := makeEqExpr(makeColExpr(1, "data"), makeLitString("foo"))
	preds := ExtractPartitionPredicatesFromExprs(td, []*plan.Expr{dataEq}, partColSet)
	assert.Equal(t, 0, len(preds), "non-partition col should not produce predicate")
}

func TestExtractPartitionPredicates_NullLiteral(t *testing.T) {
	td := makeTableDef("year", "data")
	partColSet := map[string]bool{"year": true}

	nullLit := &plan.Expr{
		Expr: &plan.Expr_Lit{Lit: &plan.Literal{Isnull: true}},
	}
	eqNull := makeEqExpr(makeColExpr(0, "year"), nullLit)
	preds := ExtractPartitionPredicatesFromExprs(td, []*plan.Expr{eqNull}, partColSet)
	assert.Equal(t, 0, len(preds), "NULL literal should be rejected")
}

func TestExtractPartitionPredicates_InWithNonLiteral(t *testing.T) {
	td := makeTableDef("year", "data")
	partColSet := map[string]bool{"year": true}

	castInList := &plan.Expr{
		Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{Obj: 123},
			Args: []*plan.Expr{makeLitInt64(2025)},
		}},
	}
	inExpr := makeInExpr(makeColExpr(0, "year"), makeLitInt64(2024), castInList)
	preds := ExtractPartitionPredicatesFromExprs(td, []*plan.Expr{inExpr}, partColSet)
	assert.Equal(t, 0, len(preds), "IN list with non-literal item should be rejected entirely")
}

func TestExtractPartitionPredicates_InVec(t *testing.T) {
	td := makeTableDef("year", "data")
	partColSet := map[string]bool{"year": true}

	// Simulate a folded Expr_Vec (what constant-fold produces from IN list)
	vec := vector.NewVec(types.T_int32.ToType())
	proc := testutil.NewProc(t)
	mp := proc.Mp()
	require.NoError(t, vector.AppendFixed(vec, int32(2024), false, mp))
	require.NoError(t, vector.AppendFixed(vec, int32(2025), false, mp))
	data, err := vec.MarshalBinary()
	require.NoError(t, err)
	vec.Free(mp)

	vecExpr := &plan.Expr{
		Typ:  plan.Type{Id: int32(types.T_int32)},
		Expr: &plan.Expr_Vec{Vec: &plan.LiteralVec{Len: 2, Data: data}},
	}
	colExpr := makeColExpr(0, "year")
	colExpr.Typ = plan.Type{Id: int32(types.T_int32)}
	inExpr := &plan.Expr{
		Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{Obj: function.InFunctionEncodedID},
			Args: []*plan.Expr{colExpr, vecExpr},
		}},
	}
	preds := ExtractPartitionPredicatesFromExprs(td, []*plan.Expr{inExpr}, partColSet)
	require.Equal(t, 1, len(preds))
	assert.Equal(t, PartOpIn, preds[0].Op)
	assert.Equal(t, []string{"2024", "2025"}, preds[0].Values)
}

func TestExtractPartitionPredicates_InVecLengthMismatch(t *testing.T) {
	td := makeTableDef("year", "data")
	partColSet := map[string]bool{"year": true}

	vec := vector.NewVec(types.T_int32.ToType())
	proc := testutil.NewProc(t)
	mp := proc.Mp()
	require.NoError(t, vector.AppendFixed(vec, int32(2024), false, mp))
	require.NoError(t, vector.AppendFixed(vec, int32(2025), false, mp))
	data, err := vec.MarshalBinary()
	require.NoError(t, err)
	vec.Free(mp)

	vecExpr := &plan.Expr{
		Typ:  plan.Type{Id: int32(types.T_int32)},
		Expr: &plan.Expr_Vec{Vec: &plan.LiteralVec{Len: 3, Data: data}},
	}
	colExpr := makeColExpr(0, "year")
	colExpr.Typ = plan.Type{Id: int32(types.T_int32)}
	inExpr := &plan.Expr{
		Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{Obj: function.InFunctionEncodedID},
			Args: []*plan.Expr{colExpr, vecExpr},
		}},
	}
	preds := ExtractPartitionPredicatesFromExprs(td, []*plan.Expr{inExpr}, partColSet)
	assert.Empty(t, preds, "LiteralVec length mismatch must disable partition pruning")
}

func TestExtractPartitionPredicates_InVecVarchar(t *testing.T) {
	td := makeTableDef("country", "data")
	partColSet := map[string]bool{"country": true}

	vec := vector.NewVec(types.T_varchar.ToType())
	proc := testutil.NewProc(t)
	mp := proc.Mp()
	require.NoError(t, vector.AppendBytes(vec, []byte("US"), false, mp))
	require.NoError(t, vector.AppendBytes(vec, []byte("CN"), false, mp))
	data, err := vec.MarshalBinary()
	require.NoError(t, err)
	vec.Free(mp)

	vecExpr := &plan.Expr{
		Typ:  plan.Type{Id: int32(types.T_varchar)},
		Expr: &plan.Expr_Vec{Vec: &plan.LiteralVec{Len: 2, Data: data}},
	}
	colExpr := makeColExpr(0, "country")
	colExpr.Typ = plan.Type{Id: int32(types.T_varchar)}
	inExpr := &plan.Expr{
		Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{Obj: function.InFunctionEncodedID},
			Args: []*plan.Expr{colExpr, vecExpr},
		}},
	}
	preds := ExtractPartitionPredicatesFromExprs(td, []*plan.Expr{inExpr}, partColSet)
	require.Equal(t, 1, len(preds))
	assert.Equal(t, PartOpIn, preds[0].Op)
	assert.Equal(t, []string{"US", "CN"}, preds[0].Values)
}

func TestExtractPartitionPruneExpr_RangeComparisons(t *testing.T) {
	td := makeTableDef("year", "data")
	partColSet := map[string]bool{"year": true}
	cases := []struct {
		name string
		expr *plan.Expr
		op   PartitionOp
	}{
		{
			name: "greater than",
			expr: makeFuncExpr(function.GREAT_THAN, makeColExpr(0, "year"), makeLitInt64(2020)),
			op:   PartOpGt,
		},
		{
			name: "greater equal",
			expr: makeFuncExpr(function.GREAT_EQUAL, makeColExpr(0, "year"), makeLitInt64(2020)),
			op:   PartOpGe,
		},
		{
			name: "less than",
			expr: makeFuncExpr(function.LESS_THAN, makeColExpr(0, "year"), makeLitInt64(2025)),
			op:   PartOpLt,
		},
		{
			name: "less equal",
			expr: makeFuncExpr(function.LESS_EQUAL, makeColExpr(0, "year"), makeLitInt64(2025)),
			op:   PartOpLe,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pruneExpr := ExtractPartitionPruneExprFromExprs(td, []*plan.Expr{tc.expr}, partColSet)
			atom := requirePartitionAtom(t, pruneExpr)
			assert.Equal(t, "year", atom.ColName)
			assert.Equal(t, tc.op, atom.Op)
		})
	}
}

func TestExtractPartitionPruneExpr_ReversedRangeComparisons(t *testing.T) {
	td := makeTableDef("year", "data")
	partColSet := map[string]bool{"year": true}

	pruneExpr := ExtractPartitionPruneExprFromExprs(td, []*plan.Expr{
		makeFuncExpr(function.LESS_THAN, makeLitInt64(2020), makeColExpr(0, "year")),
	}, partColSet)
	atom := requirePartitionAtom(t, pruneExpr)
	assert.Equal(t, PartOpGt, atom.Op)
	assert.Equal(t, []string{"2020"}, atom.Values)

	pruneExpr = ExtractPartitionPruneExprFromExprs(td, []*plan.Expr{
		makeFuncExpr(function.GREAT_EQUAL, makeLitInt64(2025), makeColExpr(0, "year")),
	}, partColSet)
	atom = requirePartitionAtom(t, pruneExpr)
	assert.Equal(t, PartOpLe, atom.Op)
	assert.Equal(t, []string{"2025"}, atom.Values)
}

func TestExtractPartitionPruneExpr_AndOrTree(t *testing.T) {
	td := makeTableDef("year", "month")
	partColSet := map[string]bool{"year": true, "month": true}
	expr := makeOrExpr(
		makeAndExpr(
			makeEqExpr(makeColExpr(0, "year"), makeLitInt64(2024)),
			makeEqExpr(makeColExpr(1, "month"), makeLitInt64(1)),
		),
		makeAndExpr(
			makeEqExpr(makeColExpr(0, "year"), makeLitInt64(2025)),
			makeBetweenExpr(makeColExpr(1, "month"), makeLitInt64(2), makeLitInt64(3)),
		),
	)

	pruneExpr := ExtractPartitionPruneExprFromExprs(td, []*plan.Expr{expr}, partColSet)
	orExpr, ok := pruneExpr.(*PartitionOr)
	require.True(t, ok, "expected OR tree, got %T", pruneExpr)
	assert.Len(t, orExpr.Children, 2)
}

func TestExtractPartitionPruneExpr_RangeAndIntersection(t *testing.T) {
	td := makeTableDef("year")
	partColSet := map[string]bool{"year": true}

	pruneExpr := ExtractPartitionPruneExprFromExprs(td, []*plan.Expr{
		makeAndExpr(
			makeFuncExpr(function.GREAT_EQUAL, makeColExpr(0, "year"), makeLitInt64(2020)),
			makeFuncExpr(function.LESS_EQUAL, makeColExpr(0, "year"), makeLitInt64(2024)),
		),
	}, partColSet)
	andExpr, ok := pruneExpr.(*PartitionAnd)
	require.True(t, ok, "expected AND tree, got %T", pruneExpr)
	require.Len(t, andExpr.Children, 2)

	pruneExpr = ExtractPartitionPruneExprFromExprs(td, []*plan.Expr{
		makeAndExpr(
			makeFuncExpr(function.GREAT_THAN, makeColExpr(0, "year"), makeLitInt64(2025)),
			makeFuncExpr(function.LESS_THAN, makeColExpr(0, "year"), makeLitInt64(2020)),
		),
	}, partColSet)
	andExpr, ok = pruneExpr.(*PartitionAnd)
	require.True(t, ok, "expected empty-interval AND tree, got %T", pruneExpr)
	require.Len(t, andExpr.Children, 2)
}

func TestExtractPartitionPruneExpr_OrWithIn(t *testing.T) {
	td := makeTableDef("year")
	partColSet := map[string]bool{"year": true}
	pruneExpr := ExtractPartitionPruneExprFromExprs(td, []*plan.Expr{
		makeOrExpr(
			makeInExpr(makeColExpr(0, "year"), makeLitInt64(2020), makeLitInt64(2021)),
			makeEqExpr(makeColExpr(0, "year"), makeLitInt64(2024)),
		),
	}, partColSet)

	orExpr, ok := pruneExpr.(*PartitionOr)
	require.True(t, ok, "expected OR tree, got %T", pruneExpr)
	require.Len(t, orExpr.Children, 2)
}

func TestExtractPartitionPruneExpr_MixedOrFallback(t *testing.T) {
	td := makeTableDef("year", "amount", catalog.ExternalFilePath)
	partColSet := map[string]bool{"year": true}
	mixedPhysical := makeOrExpr(
		makeEqExpr(makeColExpr(0, "year"), makeLitInt64(2024)),
		makeFuncExpr(function.GREAT_THAN, makeColExpr(1, "amount"), makeLitInt64(10)),
	)
	mixedFilepath := makeOrExpr(
		makeEqExpr(makeColExpr(0, "year"), makeLitInt64(2024)),
		makeEqExpr(makeColExpr(2, catalog.ExternalFilePath), makeLitString("x")),
	)

	assert.Nil(t, ExtractPartitionPruneExprFromExprs(td, []*plan.Expr{mixedPhysical}, partColSet))
	assert.Nil(t, ExtractPartitionPruneExprFromExprs(td, []*plan.Expr{mixedFilepath}, partColSet))
}

func TestExtractPartitionPruneExpr_FunctionSubqueryNullFallback(t *testing.T) {
	td := makeTableDef("year")
	partColSet := map[string]bool{"year": true}
	wrappedPartitionCol := makeFuncExpr(function.UNARY_MINUS, makeColExpr(0, "year"))
	subquery := &plan.Expr{Expr: &plan.Expr_Sub{Sub: &plan.SubqueryRef{Typ: plan.SubqueryRef_SCALAR}}}
	nullLit := &plan.Expr{Expr: &plan.Expr_Lit{Lit: &plan.Literal{Isnull: true}}}

	assert.Nil(t, ExtractPartitionPruneExprFromExprs(td, []*plan.Expr{
		makeEqExpr(wrappedPartitionCol, makeLitInt64(2024)),
	}, partColSet))
	assert.Nil(t, ExtractPartitionPruneExprFromExprs(td, []*plan.Expr{
		makeEqExpr(makeColExpr(0, "year"), subquery),
	}, partColSet))
	assert.Nil(t, ExtractPartitionPruneExprFromExprs(td, []*plan.Expr{
		makeEqExpr(makeColExpr(0, "year"), nullLit),
	}, partColSet))
}

func TestExtractPartitionPruneExpr_NotFallback(t *testing.T) {
	td := makeTableDef("year")
	partColSet := map[string]bool{"year": true}

	notExpr := makeFuncExpr(function.NOT,
		makeFuncExpr(function.GREAT_EQUAL, makeColExpr(0, "year"), makeLitInt64(2024)))
	notInExpr := makeFuncExpr(function.NOT_IN, makeColExpr(0, "year"), makeLitInt64(2024))
	notBetweenExpr := makeFuncExpr(function.NOT, makeBetweenExpr(makeColExpr(0, "year"), makeLitInt64(2020), makeLitInt64(2024)))

	assert.Nil(t, ExtractPartitionPruneExprFromExprs(td, []*plan.Expr{notExpr}, partColSet))
	assert.Nil(t, ExtractPartitionPruneExprFromExprs(td, []*plan.Expr{notInExpr}, partColSet))
	assert.Nil(t, ExtractPartitionPruneExprFromExprs(td, []*plan.Expr{notBetweenExpr}, partColSet))
}

func TestExtractPartitionPruneExpr_IsNullAtoms(t *testing.T) {
	td := makeTableDef("year")
	partColSet := map[string]bool{"year": true}

	pruneExpr := ExtractPartitionPruneExprFromExprs(td, []*plan.Expr{
		makeFuncExpr(function.ISNULL, makeColExpr(0, "year")),
	}, partColSet)
	atom := requirePartitionAtom(t, pruneExpr)
	assert.Equal(t, PartOpIsNull, atom.Op)

	pruneExpr = ExtractPartitionPruneExprFromExprs(td, []*plan.Expr{
		makeFuncExpr(function.ISNOTNULL, makeColExpr(0, "year")),
	}, partColSet)
	atom = requirePartitionAtom(t, pruneExpr)
	assert.Equal(t, PartOpIsNotNull, atom.Op)
}

func TestExtractPartitionPruneExpr_UnaryMinusLiteral(t *testing.T) {
	td := makeTableDef("year")
	partColSet := map[string]bool{"year": true}

	pruneExpr := ExtractPartitionPruneExprFromExprs(td, []*plan.Expr{
		makeFuncExpr(function.GREAT_THAN, makeColExpr(0, "year"), makeUnaryMinusExpr(makeLitInt64(2020))),
	}, partColSet)
	atom := requirePartitionAtom(t, pruneExpr)
	assert.Equal(t, PartOpGt, atom.Op)
	assert.Equal(t, []string{"-2020"}, atom.Values)

	pruneExpr = ExtractPartitionPruneExprFromExprs(td, []*plan.Expr{
		makeBetweenExpr(makeColExpr(0, "year"), makeUnaryMinusExpr(makeLitInt64(10)), makeLitInt64(10)),
	}, partColSet)
	atom = requirePartitionAtom(t, pruneExpr)
	assert.Equal(t, PartOpBetween, atom.Op)
	assert.Equal(t, []string{"-10", "10"}, atom.Values)
}

func TestExtractPartitionPruneExpr_NodeLimitFallback(t *testing.T) {
	td := makeTableDef("year")
	partColSet := map[string]bool{"year": true}
	args := make([]*plan.Expr, 0, maxPartitionPruneExprNodes+1)
	for i := 0; i <= maxPartitionPruneExprNodes; i++ {
		args = append(args, makeEqExpr(makeColExpr(0, "year"), makeLitInt64(int64(i))))
	}

	assert.Nil(t, ExtractPartitionPruneExprFromExprs(td, []*plan.Expr{makeOrExpr(args...)}, partColSet))
}

func TestMatchPartitionValue_SetWithEnumvalues(t *testing.T) {
	// SET column stored as T_uint64 with non-empty Enumvalues must NOT be pruned
	ct := tree.HivePartColType{Id: int32(types.T_uint64), Enumvalues: "a,b,c"}
	assert.Equal(t, MatchUnknown, matchPartitionValue("1", []string{"2"}, ct),
		"SET column should always return MatchUnknown")
}

func TestFillConstantVector_Int64FloatFallback(t *testing.T) {
	proc := testutil.NewProc(t)
	mp := proc.Mp()
	vec := vector.NewVec(types.T_int64.ToType())
	col := &plan.ColDef{Name: "big", Typ: plan.Type{Id: int32(types.T_int64)}}

	err := fillConstantVector(vec, "1.5", col, 3, proc, "/test")
	require.NoError(t, err, "int64 float fallback should work")
	val := vector.MustFixedColNoTypeCheck[int64](vec)
	assert.Equal(t, int64(1), val[0])
	vec.Free(mp)
}

func TestFillConstantVector_Int64OverflowRejects(t *testing.T) {
	proc := testutil.NewProc(t)
	vec := vector.NewVec(types.T_int64.ToType())
	col := &plan.ColDef{Name: "big", Typ: plan.Type{Id: int32(types.T_int64)}}

	// 9223372036854775808 = MaxInt64 + 1 → ParseInt ErrRange → reject (no float fallback)
	err := fillConstantVector(vec, "9223372036854775808", col, 1, proc, "/test")
	require.Error(t, err, "int64 overflow must be rejected")
	vec.Free(nil)
}

func TestFillConstantVector_Uint64OverflowRejects(t *testing.T) {
	proc := testutil.NewProc(t)
	vec := vector.NewVec(types.T_uint64.ToType())
	col := &plan.ColDef{Name: "big", Typ: plan.Type{Id: int32(types.T_uint64)}}

	// 18446744073709551616 = MaxUint64 + 1 → ParseUint ErrRange → reject
	err := fillConstantVector(vec, "18446744073709551616", col, 1, proc, "/test")
	require.Error(t, err, "uint64 overflow must be rejected")
	vec.Free(nil)
}

// TestFillConstantVector_Int64DecimalBoundaryRejects guards the 64-bit float
// fallback against four classes of unsafe inputs:
//   - 2^63 / 2^64 slipping through due to float64 rounding of MaxInt64/MaxUint64
//   - NaN passing range checks (NaN < x and NaN >= x both false)
//   - ±Inf (covered by the strict upper bound being exact)
//   - "-9223372036854775809.0" (below MinInt64) rounding to -2^63 in float64;
//     rejected by the |f| >= 2^53 precision guard since any value reaching the
//     float fallback at that magnitude cannot be safely round-tripped to int64.
func TestFillConstantVector_Int64DecimalBoundaryRejects(t *testing.T) {
	proc := testutil.NewProc(t)
	col := &plan.ColDef{Name: "big", Typ: plan.Type{Id: int32(types.T_int64)}}

	cases := []string{
		"9223372036854775808.0",    // 2^63 in decimal form
		"9.223372036854775808e18",  // 2^63 in scientific form
		"9223372036854775808",      // MaxInt64+1 (ParseInt ErrRange path)
		"9999999999999999999.0",    // well above 2^63
		"-9223372036854775808.0",   // MinInt64 in decimal — float fallback, rejected by 2^53 guard
		"-9223372036854775809",     // below MinInt64 (ParseInt ErrRange)
		"-9223372036854775809.0",   // below MinInt64 in decimal; float64 rounds to -2^63
		"-9.223372036854775809e18", // same, scientific form
		"-99999999999999999999.0",  // well below MinInt64
		"1e20",                     // large positive scientific
		"-1e20",                    // large negative scientific
		"nan", "NaN", "NAN",        // non-finite: NaN slips past naive range checks
		"inf", "-inf", "+Inf", "Infinity", "-Infinity",
	}
	for _, s := range cases {
		t.Run(s, func(t *testing.T) {
			vec := vector.NewVec(types.T_int64.ToType())
			err := fillConstantVector(vec, s, col, 1, proc, "/test")
			require.Error(t, err, "%q must be rejected (would overflow int64 or be ambiguous)", s)
			vec.Free(nil)
		})
	}
}

func TestFillConstantVector_Int64DecimalBoundaryAccepts(t *testing.T) {
	// Values reaching the float fallback with |f| < 2^53 — float64 still
	// represents them exactly, so int64(f) is safe.
	proc := testutil.NewProc(t)
	mp := proc.Mp()
	col := &plan.ColDef{Name: "big", Typ: plan.Type{Id: int32(types.T_int64)}}

	cases := []struct {
		s    string
		want int64
	}{
		{"1.0", 1},
		{"-1.5", -1},
		{"0.0", 0},
		{"1.5e3", 1500}, // 1500 < 2^53
		{"-1.5e3", -1500},
		{"9007199254740991.0", 9007199254740991}, // 2^53 - 1, largest exact before the guard
		{"-9007199254740991.0", -9007199254740991}, // -(2^53 - 1)
	}
	for _, c := range cases {
		t.Run(c.s, func(t *testing.T) {
			vec := vector.NewVec(types.T_int64.ToType())
			err := fillConstantVector(vec, c.s, col, 1, proc, "/test")
			require.NoError(t, err)
			val := vector.MustFixedColNoTypeCheck[int64](vec)[0]
			assert.Equal(t, c.want, val)
			vec.Free(mp)
		})
	}
}

func TestFillConstantVector_Uint64DecimalBoundaryRejects(t *testing.T) {
	proc := testutil.NewProc(t)
	col := &plan.ColDef{Name: "big", Typ: plan.Type{Id: int32(types.T_uint64)}}

	cases := []string{
		"18446744073709551616.0",   // 2^64 in decimal form
		"1.8446744073709551616e19", // 2^64 in scientific form
		"18446744073709551616",     // MaxUint64+1 (ParseUint ErrRange path)
		"99999999999999999999.0",   // well above 2^64
		"-1.0",                     // negative
		"1e20",                     // large scientific
		"9007199254740992.0",       // 2^53 exactly — reached via float fallback, ambiguous
		"nan", "NaN",               // NaN silently passes naive < / >= checks
		"inf", "Infinity", "-inf", // ±Inf
	}
	for _, s := range cases {
		t.Run(s, func(t *testing.T) {
			vec := vector.NewVec(types.T_uint64.ToType())
			err := fillConstantVector(vec, s, col, 1, proc, "/test")
			require.Error(t, err, "%q must be rejected", s)
			vec.Free(nil)
		})
	}
}

func TestFillConstantVector_Uint64DecimalBoundaryAccepts(t *testing.T) {
	proc := testutil.NewProc(t)
	mp := proc.Mp()
	col := &plan.ColDef{Name: "big", Typ: plan.Type{Id: int32(types.T_uint64)}}

	cases := []struct {
		s    string
		want uint64
	}{
		{"0.0", 0},
		{"1.0", 1},
		{"1.7e3", 1700},
	}
	for _, c := range cases {
		t.Run(c.s, func(t *testing.T) {
			vec := vector.NewVec(types.T_uint64.ToType())
			err := fillConstantVector(vec, c.s, col, 1, proc, "/test")
			require.NoError(t, err)
			val := vector.MustFixedColNoTypeCheck[uint64](vec)[0]
			assert.Equal(t, c.want, val)
			vec.Free(mp)
		})
	}
}

// TestFillConstantVector_SmallIntFloatBoundaryRejects verifies the float
// fallback path checks bounds BEFORE truncation. Go's int64(f) truncates
// toward zero: without the pre-check, int32("-2147483648.9") would pass
// because int64(-2147483648.9) == -2147483648, which the post-check sees
// as within [-2^31, 2^31-1]. We must reject it, matching CSV loader.
func TestFillConstantVector_SmallIntFloatBoundaryRejects(t *testing.T) {
	proc := testutil.NewProc(t)

	type caseEntry struct {
		typId types.T
		val   string
	}
	cases := []caseEntry{
		// int8: [-128, 127]
		{types.T_int8, "-128.1"},
		{types.T_int8, "127.5"},
		// int16: [-32768, 32767]
		{types.T_int16, "-32768.1"},
		{types.T_int16, "32767.5"},
		// int32: [-2147483648, 2147483647]
		{types.T_int32, "-2147483648.9"},
		{types.T_int32, "2147483647.9"},
		// uint8: [0, 255]
		{types.T_uint8, "255.5"},
		// uint16: [0, 65535]
		{types.T_uint16, "65535.9"},
		// uint32: [0, 4294967295]
		{types.T_uint32, "4294967295.9"},
	}
	for _, c := range cases {
		name := fmt.Sprintf("%s_%s", c.typId, c.val)
		t.Run(name, func(t *testing.T) {
			col := &plan.ColDef{Name: "n", Typ: plan.Type{Id: int32(c.typId)}}
			vec := vector.NewVec(c.typId.ToType())
			err := fillConstantVector(vec, c.val, col, 1, proc, "/test")
			require.Error(t, err, "%s value %q must be rejected (float bound)", c.typId, c.val)
			vec.Free(nil)
		})
	}
}

// TestFillConstantVector_SmallIntFloatBoundaryAccepts verifies values safely
// inside the float bounds still pass and truncate toward zero, matching Go
// int/uint conversion semantics.
func TestFillConstantVector_SmallIntFloatBoundaryAccepts(t *testing.T) {
	proc := testutil.NewProc(t)
	mp := proc.Mp()

	type intEntry struct {
		typId types.T
		val   string
		want  int64
	}
	intCases := []intEntry{
		// Inside [min, max] as a float, then truncated toward zero.
		{types.T_int8, "126.9", 126},
		{types.T_int8, "127.0", 127},
		{types.T_int8, "-127.9", -127},
		{types.T_int8, "-128.0", -128},
		{types.T_int16, "32766.9", 32766},
		{types.T_int16, "32767.0", 32767},
		{types.T_int32, "2147483646.9", 2147483646},
		{types.T_int32, "2147483647.0", 2147483647},
	}
	for _, c := range intCases {
		name := fmt.Sprintf("%s_%s", c.typId, c.val)
		t.Run(name, func(t *testing.T) {
			col := &plan.ColDef{Name: "n", Typ: plan.Type{Id: int32(c.typId)}}
			vec := vector.NewVec(c.typId.ToType())
			err := fillConstantVector(vec, c.val, col, 1, proc, "/test")
			require.NoError(t, err)
			var got int64
			switch c.typId {
			case types.T_int8:
				got = int64(vector.MustFixedColNoTypeCheck[int8](vec)[0])
			case types.T_int16:
				got = int64(vector.MustFixedColNoTypeCheck[int16](vec)[0])
			case types.T_int32:
				got = int64(vector.MustFixedColNoTypeCheck[int32](vec)[0])
			}
			assert.Equal(t, c.want, got)
			vec.Free(mp)
		})
	}

	type uintEntry struct {
		typId types.T
		val   string
		want  uint64
	}
	uintCases := []uintEntry{
		{types.T_uint8, "254.9", 254},
		{types.T_uint8, "255.0", 255},
		{types.T_uint16, "65534.9", 65534},
		{types.T_uint16, "65535.0", 65535},
		{types.T_uint32, "4294967294.9", 4294967294},
		{types.T_uint32, "4294967295.0", 4294967295},
	}
	for _, c := range uintCases {
		name := fmt.Sprintf("%s_%s", c.typId, c.val)
		t.Run(name, func(t *testing.T) {
			col := &plan.ColDef{Name: "n", Typ: plan.Type{Id: int32(c.typId)}}
			vec := vector.NewVec(c.typId.ToType())
			err := fillConstantVector(vec, c.val, col, 1, proc, "/test")
			require.NoError(t, err)
			var got uint64
			switch c.typId {
			case types.T_uint8:
				got = uint64(vector.MustFixedColNoTypeCheck[uint8](vec)[0])
			case types.T_uint16:
				got = uint64(vector.MustFixedColNoTypeCheck[uint16](vec)[0])
			case types.T_uint32:
				got = uint64(vector.MustFixedColNoTypeCheck[uint32](vec)[0])
			}
			assert.Equal(t, c.want, got)
			vec.Free(mp)
		})
	}
}

// ---------------------------------------------------------------------------
// Virtual column filling tests
// ---------------------------------------------------------------------------

func TestIsHivePartitionCol(t *testing.T) {
	param := &ExternalParam{}
	param.Extern = &tree.ExternParam{
		ExParamConst: tree.ExParamConst{
			HivePartitioning:  true,
			HivePartitionCols: []string{"year", "month"},
		},
	}
	assert.True(t, param.isHivePartitionCol("year"))
	assert.True(t, param.isHivePartitionCol("Year"))
	assert.True(t, param.isHivePartitionCol("month"))
	assert.False(t, param.isHivePartitionCol("amount"))
	assert.False(t, param.isHivePartitionCol(""))
}

func TestIsHivePartitionCol_NotEnabled(t *testing.T) {
	param := &ExternalParam{}
	param.Extern = &tree.ExternParam{}
	assert.False(t, param.isHivePartitionCol("year"))
}

func TestRefreshPartitionValues(t *testing.T) {
	proc := testutil.NewProc(t)
	param := &ExternalParam{}
	param.Extern = &tree.ExternParam{
		ExParamConst: tree.ExParamConst{
			HivePartitioning:  true,
			HivePartitionCols: []string{"year", "month"},
			HivePartitionColTypes: []tree.HivePartColType{
				{Id: int32(types.T_int32), NullAbility: true},
				{Id: int32(types.T_varchar), NullAbility: true},
			},
		},
	}
	param.Extern.Filepath = "/data"
	param.Fileparam = &ExFileparam{Filepath: "/data/year=2025/month=06/file.parquet"}

	err := param.refreshPartitionValues(proc)
	require.NoError(t, err)
	assert.Equal(t, "2025", param.currentPartValues["year"])
	assert.Equal(t, "06", param.currentPartValues["month"])
}

func TestRefreshPartitionValues_ValidatesUnprojectedPartitionValue(t *testing.T) {
	proc := testutil.NewProc(t)
	basePath := "/warehouse/lake"
	param := &ExternalParam{}
	param.Extern = &tree.ExternParam{
		ExParamConst: tree.ExParamConst{
			Filepath:              basePath,
			HivePartitioning:      true,
			HivePartitionCols:     []string{"year"},
			HivePartitionColTypes: []tree.HivePartColType{{Id: int32(types.T_int32), NullAbility: true}},
		},
	}
	param.Fileparam = &ExFileparam{Filepath: basePath + "/year=abc/data.parquet"}

	err := param.refreshPartitionValues(proc)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "partition value type conversion failed")
	assert.Contains(t, err.Error(), "col=year")
	assert.Contains(t, err.Error(), "value='abc'")
	assert.Contains(t, err.Error(), "path=year=abc/data.parquet")
	assert.NotContains(t, err.Error(), basePath)
}

func TestFillConstantVector_Int(t *testing.T) {
	proc := testutil.NewProc(t)
	mp := proc.Mp()
	vec := vector.NewVec(types.T_int32.ToType())
	col := &plan.ColDef{Name: "year", Typ: plan.Type{Id: int32(types.T_int32)}}

	err := fillConstantVector(vec, "2025", col, 10, proc, "/test")
	require.NoError(t, err)
	assert.Equal(t, 10, vec.Length())
	val := vector.MustFixedColNoTypeCheck[int32](vec)
	assert.Equal(t, int32(2025), val[0])
	vec.Free(mp)
}

func TestFillConstantVector_IntFloatFallback(t *testing.T) {
	proc := testutil.NewProc(t)
	mp := proc.Mp()
	vec := vector.NewVec(types.T_int32.ToType())
	col := &plan.ColDef{Name: "year", Typ: plan.Type{Id: int32(types.T_int32)}}

	err := fillConstantVector(vec, "1.5", col, 5, proc, "/test")
	require.NoError(t, err)
	val := vector.MustFixedColNoTypeCheck[int32](vec)
	assert.Equal(t, int32(1), val[0])
	vec.Free(mp)
}

func TestFillConstantVector_Varchar(t *testing.T) {
	proc := testutil.NewProc(t)
	mp := proc.Mp()
	vec := vector.NewVec(types.T_varchar.ToType())
	col := &plan.ColDef{Name: "country", Typ: plan.Type{Id: int32(types.T_varchar)}}

	err := fillConstantVector(vec, "US", col, 3, proc, "/test")
	require.NoError(t, err)
	assert.Equal(t, 3, vec.Length())
	bs := vec.GetBytesAt(0)
	assert.Equal(t, "US", string(bs))
	vec.Free(mp)
}

func TestFillConstantVector_Bool(t *testing.T) {
	proc := testutil.NewProc(t)
	mp := proc.Mp()
	vec := vector.NewVec(types.T_bool.ToType())
	col := &plan.ColDef{Name: "flag", Typ: plan.Type{Id: int32(types.T_bool)}}

	err := fillConstantVector(vec, "true", col, 2, proc, "/test")
	require.NoError(t, err)
	val := vector.MustFixedColNoTypeCheck[bool](vec)
	assert.True(t, val[0])
	vec.Free(mp)
}

func TestFillConstantVector_UnsupportedVector(t *testing.T) {
	proc := testutil.NewProc(t)
	vec := vector.NewVec(types.T_array_float32.ToType())
	col := &plan.ColDef{Name: "emb", Typ: plan.Type{Id: int32(types.T_array_float32)}}

	err := fillConstantVector(vec, "[1,2,3]", col, 1, proc, "/test")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported")
}

func TestFillPartitionColumns_DefaultPartNull(t *testing.T) {
	proc := testutil.NewProc(t)
	mp := proc.Mp()

	vec := vector.NewVec(types.T_int32.ToType())
	bat := &batch.Batch{Vecs: []*vector.Vector{vec}}
	bat.SetRowCount(5)

	param := &ExternalParam{}
	param.Cols = []*plan.ColDef{
		{Name: "year", Typ: plan.Type{Id: int32(types.T_int32)},
			Default: &plan.Default{NullAbility: true}},
	}
	param.Ctx = context.Background()
	param.Fileparam = &ExFileparam{Filepath: "/data/year=__HIVE_DEFAULT_PARTITION__/f.parquet"}
	param.currentPartValues = map[string]string{"year": HiveDefaultPartition}

	h := &ParquetHandler{partitionColIndices: []int{0}}
	err := h.fillPartitionColumns(bat, param, proc)
	require.NoError(t, err)
	assert.True(t, vec.IsConstNull())
	vec.Free(mp)
}

func TestFillPartitionColumns_DefaultPartNotNull(t *testing.T) {
	proc := testutil.NewProc(t)
	mp := proc.Mp()

	vec := vector.NewVec(types.T_int32.ToType())
	bat := &batch.Batch{Vecs: []*vector.Vector{vec}}
	bat.SetRowCount(5)

	param := &ExternalParam{}
	param.Cols = []*plan.ColDef{
		{Name: "year", Typ: plan.Type{Id: int32(types.T_int32)},
			Default: &plan.Default{NullAbility: false}},
	}
	param.Ctx = context.Background()
	param.Fileparam = &ExFileparam{Filepath: "/data/year=__HIVE_DEFAULT_PARTITION__/f.parquet"}
	param.currentPartValues = map[string]string{"year": HiveDefaultPartition}

	h := &ParquetHandler{partitionColIndices: []int{0}}
	err := h.fillPartitionColumns(bat, param, proc)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "NOT NULL")
	vec.Free(mp)
}

func TestFillPartitionColumns_NotNullViaTypNotNullable_NegativeCase(t *testing.T) {
	proc := testutil.NewProc(t)
	mp := proc.Mp()

	vec := vector.NewVec(types.T_int32.ToType())
	bat := &batch.Batch{Vecs: []*vector.Vector{vec}}
	bat.SetRowCount(3)

	param := &ExternalParam{}
	param.Cols = []*plan.ColDef{
		{Name: "year", Typ: plan.Type{Id: int32(types.T_int32), NotNullable: true},
			Default: &plan.Default{NullAbility: true}},
	}
	param.Ctx = context.Background()
	param.Fileparam = &ExFileparam{Filepath: "/data/year=__HIVE_DEFAULT_PARTITION__/f.parquet"}
	param.currentPartValues = map[string]string{"year": HiveDefaultPartition}

	h := &ParquetHandler{partitionColIndices: []int{0}}
	err := h.fillPartitionColumns(bat, param, proc)
	require.NoError(t, err, "should use Default.NullAbility (true=nullable), not Typ.NotNullable")
	assert.True(t, vec.IsConstNull())
	vec.Free(mp)
}

func TestFillPartitionColumns_NotPresent(t *testing.T) {
	proc := testutil.NewProc(t)
	mp := proc.Mp()

	vec := vector.NewVec(types.T_int32.ToType())
	bat := &batch.Batch{Vecs: []*vector.Vector{vec}}
	bat.SetRowCount(3)

	param := &ExternalParam{}
	param.Cols = []*plan.ColDef{
		{Name: "year", Typ: plan.Type{Id: int32(types.T_int32)}},
	}
	param.Ctx = context.Background()
	param.Fileparam = &ExFileparam{Filepath: "/data/f.parquet"}
	param.currentPartValues = map[string]string{}

	h := &ParquetHandler{partitionColIndices: []int{0}}
	err := h.fillPartitionColumns(bat, param, proc)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not found in path")
	vec.Free(mp)
}

// TestFillPartitionColumns_RelPathWithExtern guards the relative-path contract
// in fillPartitionColumns: when param.Extern.Filepath is set (the normal
// production invariant for hive tables), error messages must reference the
// partition-relative path, NOT the absolute/base path. Without this test the
// existing coverage only exercises the fallback branch where Extern is nil
// (relPath == Fileparam.Filepath), hiding a latent bug where a future refactor
// could drop the nil guard and leak machine-local absolute paths into BVT
// .result files.
func TestFillPartitionColumns_RelPathWithExtern(t *testing.T) {
	proc := testutil.NewProc(t)
	mp := proc.Mp()

	// Two subtests cover both error paths that embed relPath:
	//   1) constraint-violation (NOT NULL + __HIVE_DEFAULT_PARTITION__)
	//   2) not-found-in-path (partition key missing from file path)

	t.Run("not-null default partition prints relative path", func(t *testing.T) {
		vec := vector.NewVec(types.T_int32.ToType())
		bat := &batch.Batch{Vecs: []*vector.Vector{vec}}
		bat.SetRowCount(5)

		basePath := "/warehouse/lake/data"
		filePath := basePath + "/year=__HIVE_DEFAULT_PARTITION__/part-0.parquet"
		param := &ExternalParam{}
		param.Cols = []*plan.ColDef{
			{Name: "year", Typ: plan.Type{Id: int32(types.T_int32)},
				Default: &plan.Default{NullAbility: false}},
		}
		param.Ctx = context.Background()
		param.Fileparam = &ExFileparam{Filepath: filePath}
		param.Extern = &tree.ExternParam{
			ExParamConst: tree.ExParamConst{Filepath: basePath},
		}
		param.currentPartValues = map[string]string{"year": HiveDefaultPartition}

		h := &ParquetHandler{partitionColIndices: []int{0}}
		err := h.fillPartitionColumns(bat, param, proc)
		require.Error(t, err)
		// Must include the relative form...
		assert.Contains(t, err.Error(), "year=__HIVE_DEFAULT_PARTITION__/part-0.parquet")
		// ...and must NOT include the base prefix (would leak machine paths).
		assert.NotContains(t, err.Error(), basePath)
		vec.Free(mp)
	})

	t.Run("missing partition key prints relative path", func(t *testing.T) {
		vec := vector.NewVec(types.T_int32.ToType())
		bat := &batch.Batch{Vecs: []*vector.Vector{vec}}
		bat.SetRowCount(2)

		basePath := "/warehouse/lake/data"
		filePath := basePath + "/oops/part-0.parquet"
		param := &ExternalParam{}
		param.Cols = []*plan.ColDef{
			{Name: "year", Typ: plan.Type{Id: int32(types.T_int32)}},
		}
		param.Ctx = context.Background()
		param.Fileparam = &ExFileparam{Filepath: filePath}
		param.Extern = &tree.ExternParam{
			ExParamConst: tree.ExParamConst{Filepath: basePath},
		}
		param.currentPartValues = map[string]string{} // year not parsed

		h := &ParquetHandler{partitionColIndices: []int{0}}
		err := h.fillPartitionColumns(bat, param, proc)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "oops/part-0.parquet")
		assert.NotContains(t, err.Error(), basePath)
		vec.Free(mp)
	})

	t.Run("type conversion failure prints relative path", func(t *testing.T) {
		vec := vector.NewVec(types.T_int32.ToType())
		bat := &batch.Batch{Vecs: []*vector.Vector{vec}}
		bat.SetRowCount(3)

		basePath := "/warehouse/lake/data"
		filePath := basePath + "/year=abc/part-0.parquet"
		param := &ExternalParam{}
		param.Cols = []*plan.ColDef{
			{Name: "year", Typ: plan.Type{Id: int32(types.T_int32)}},
		}
		param.Ctx = context.Background()
		param.Fileparam = &ExFileparam{Filepath: filePath}
		param.Extern = &tree.ExternParam{
			ExParamConst: tree.ExParamConst{Filepath: basePath},
		}
		param.currentPartValues = map[string]string{"year": "abc"}

		h := &ParquetHandler{partitionColIndices: []int{0}}
		err := h.fillPartitionColumns(bat, param, proc)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "year=abc/part-0.parquet")
		assert.NotContains(t, err.Error(), basePath)
		vec.Free(mp)
	})
}

// ---------------------------------------------------------------------------
// Pruning observability tests — assert ListCalls / PrunedCount precisely
// ---------------------------------------------------------------------------

func TestDiscoverHivePartitions_EQListCalls(t *testing.T) {
	dirs := map[string][]fileservice.DirEntry{
		"/data": {
			{Name: "year=2020", IsDir: true},
			{Name: "year=2021", IsDir: true},
			{Name: "year=2022", IsDir: true},
			{Name: "year=2023", IsDir: true},
			{Name: "year=2024", IsDir: true},
		},
		"/data/year=2024": {
			{Name: "part.parquet", IsDir: false, Size: 100},
		},
	}

	result, err := DiscoverHivePartitions(
		context.Background(),
		mockListDir(dirs),
		"/data",
		[]string{"year"},
		[]tree.HivePartColType{{Id: int32(types.T_int32)}},
		[]PartitionPredicate{{ColName: "year", Op: PartOpEq, Values: []string{"2024"}}},
	)
	require.NoError(t, err)
	assert.Equal(t, 2, result.ListCalls, "EQ single value: root + hit partition file dir = 2")
	assert.Equal(t, 1, result.PartitionCount)
	assert.Equal(t, 4, result.PrunedCount)
	assert.Equal(t, 1, len(result.Files))
}

func TestDiscoverHivePartitions_INTwoValuesListCalls(t *testing.T) {
	dirs := map[string][]fileservice.DirEntry{
		"/data": {
			{Name: "year=2020", IsDir: true},
			{Name: "year=2021", IsDir: true},
			{Name: "year=2022", IsDir: true},
		},
		"/data/year=2020": {
			{Name: "f.parquet", IsDir: false, Size: 100},
		},
		"/data/year=2022": {
			{Name: "f.parquet", IsDir: false, Size: 200},
		},
	}

	result, err := DiscoverHivePartitions(
		context.Background(),
		mockListDir(dirs),
		"/data",
		[]string{"year"},
		[]tree.HivePartColType{{Id: int32(types.T_int32)}},
		[]PartitionPredicate{{ColName: "year", Op: PartOpIn, Values: []string{"2020", "2022"}}},
	)
	require.NoError(t, err)
	assert.Equal(t, 3, result.ListCalls, "IN two values: root + 2 hit partition file dirs = 3")
	assert.Equal(t, 2, result.PartitionCount)
	assert.Equal(t, 1, result.PrunedCount)
	assert.Equal(t, 2, len(result.Files))
}

func TestDiscoverHivePartitions_BetweenPredicate(t *testing.T) {
	dirs := map[string][]fileservice.DirEntry{
		"/data": {
			{Name: "year=2019", IsDir: true},
			{Name: "year=2020", IsDir: true},
			{Name: "year=2021", IsDir: true},
			{Name: "year=2022", IsDir: true},
			{Name: "year=2023", IsDir: true},
		},
		"/data/year=2020": {
			{Name: "a.parquet", IsDir: false, Size: 100},
		},
		"/data/year=2021": {
			{Name: "b.parquet", IsDir: false, Size: 200},
		},
		"/data/year=2022": {
			{Name: "c.parquet", IsDir: false, Size: 300},
		},
	}

	result, err := DiscoverHivePartitions(
		context.Background(),
		mockListDir(dirs),
		"/data",
		[]string{"year"},
		[]tree.HivePartColType{{Id: int32(types.T_int32)}},
		[]PartitionPredicate{{ColName: "year", Op: PartOpBetween, Values: []string{"2020", "2022"}}},
	)
	require.NoError(t, err)
	assert.Equal(t, 4, result.ListCalls, "BETWEEN: root + 3 hit partition file dirs = 4")
	assert.Equal(t, 3, result.PartitionCount)
	assert.Equal(t, 2, result.PrunedCount)
	assert.Equal(t, 3, len(result.Files))
}

func TestDiscoverHivePartitions_RangePruneExpr(t *testing.T) {
	dirs := map[string][]fileservice.DirEntry{
		"/data": {
			{Name: "year=2021", IsDir: true},
			{Name: "year=2022", IsDir: true},
			{Name: "year=2023", IsDir: true},
			{Name: "year=2024", IsDir: true},
		},
		"/data/year=2023": {{Name: "a.parquet", IsDir: false, Size: 100}},
		"/data/year=2024": {{Name: "b.parquet", IsDir: false, Size: 200}},
	}

	result, err := DiscoverHivePartitionsWithPruneExpr(
		context.Background(),
		mockListDir(dirs),
		"/data",
		[]string{"year"},
		[]tree.HivePartColType{{Id: int32(types.T_int32)}},
		&PartitionAtom{ColName: "year", Op: PartOpGt, Values: []string{"2022"}},
		nil,
	)
	require.NoError(t, err)
	assert.Equal(t, 2, result.PartitionCount)
	assert.Equal(t, 2, result.PrunedCount)
	require.Len(t, result.Files, 2)
	assert.Equal(t, 2, result.PrunedFiles)
	assert.Equal(t, int64(300), result.PrunedBytes)
	assert.Equal(t, "/data/year=2023/a.parquet", result.Files[0].FilePath)
	assert.Equal(t, "/data/year=2024/b.parquet", result.Files[1].FilePath)
}

func TestDiscoverHivePartitions_EmptyRangeIntersection(t *testing.T) {
	dirs := map[string][]fileservice.DirEntry{
		"/data": {
			{Name: "year=2020", IsDir: true},
			{Name: "year=2025", IsDir: true},
		},
		"/data/year=2020": {{Name: "a.parquet", IsDir: false, Size: 100}},
		"/data/year=2025": {{Name: "b.parquet", IsDir: false, Size: 200}},
	}

	result, err := DiscoverHivePartitionsWithPruneExpr(
		context.Background(),
		mockListDir(dirs),
		"/data",
		[]string{"year"},
		[]tree.HivePartColType{{Id: int32(types.T_int32)}},
		&PartitionAnd{Children: []PartitionPruneExpr{
			&PartitionAtom{ColName: "year", Op: PartOpGt, Values: []string{"2025"}},
			&PartitionAtom{ColName: "year", Op: PartOpLt, Values: []string{"2020"}},
		}},
		nil,
	)
	require.NoError(t, err)
	assert.Empty(t, result.Files)
	assert.Equal(t, 0, result.PartitionCount)
	assert.Equal(t, 2, result.PrunedCount)
}

func TestDiscoverHivePartitions_OrPruneExpr(t *testing.T) {
	dirs := map[string][]fileservice.DirEntry{
		"/data": {
			{Name: "year=2020", IsDir: true},
			{Name: "year=2021", IsDir: true},
			{Name: "year=2024", IsDir: true},
		},
		"/data/year=2020": {{Name: "a.parquet", IsDir: false, Size: 100}},
		"/data/year=2024": {{Name: "b.parquet", IsDir: false, Size: 200}},
	}

	result, err := DiscoverHivePartitionsWithPruneExpr(
		context.Background(),
		mockListDir(dirs),
		"/data",
		[]string{"year"},
		[]tree.HivePartColType{{Id: int32(types.T_int32)}},
		&PartitionOr{Children: []PartitionPruneExpr{
			&PartitionAtom{ColName: "year", Op: PartOpEq, Values: []string{"2020"}},
			&PartitionAtom{ColName: "year", Op: PartOpEq, Values: []string{"2024"}},
		}},
		nil,
	)
	require.NoError(t, err)
	assert.Equal(t, 2, result.PartitionCount)
	assert.Equal(t, 1, result.PrunedCount)
	require.Len(t, result.Files, 2)
	assert.Equal(t, "/data/year=2020/a.parquet", result.Files[0].FilePath)
	assert.Equal(t, "/data/year=2024/b.parquet", result.Files[1].FilePath)
}

func TestDiscoverHivePartitions_OverlappingOrDoesNotDuplicateFiles(t *testing.T) {
	dirs := map[string][]fileservice.DirEntry{
		"/data": {
			{Name: "year=2024", IsDir: true},
			{Name: "year=2025", IsDir: true},
		},
		"/data/year=2024": {{Name: "a.parquet", IsDir: false, Size: 100}},
		"/data/year=2025": {{Name: "b.parquet", IsDir: false, Size: 200}},
	}

	result, err := DiscoverHivePartitionsWithPruneExpr(
		context.Background(),
		mockListDir(dirs),
		"/data",
		[]string{"year"},
		[]tree.HivePartColType{{Id: int32(types.T_int32)}},
		&PartitionOr{Children: []PartitionPruneExpr{
			&PartitionAtom{ColName: "year", Op: PartOpEq, Values: []string{"2024"}},
			&PartitionAtom{ColName: "year", Op: PartOpIn, Values: []string{"2024", "2025"}},
		}},
		nil,
	)
	require.NoError(t, err)
	require.Len(t, result.Files, 2)
	assert.Equal(t, "/data/year=2024/a.parquet", result.Files[0].FilePath)
	assert.Equal(t, "/data/year=2025/b.parquet", result.Files[1].FilePath)
}

func TestDiscoverHivePartitions_UnknownOrBranchKeepsDirectory(t *testing.T) {
	dirs := map[string][]fileservice.DirEntry{
		"/data": {
			{Name: "country=CN", IsDir: true},
			{Name: "country=US", IsDir: true},
		},
		"/data/country=CN": {{Name: "cn.parquet", IsDir: false, Size: 100}},
		"/data/country=US": {{Name: "us.parquet", IsDir: false, Size: 200}},
	}

	result, err := DiscoverHivePartitionsWithPruneExpr(
		context.Background(),
		mockListDir(dirs),
		"/data",
		[]string{"country"},
		[]tree.HivePartColType{{Id: int32(types.T_varchar)}},
		&PartitionOr{Children: []PartitionPruneExpr{
			&PartitionAtom{ColName: "country", Op: PartOpEq, Values: []string{"US"}},
			&PartitionAtom{ColName: "country", Op: PartOpEq, Values: []string{"FR"}},
		}},
		nil,
	)
	require.NoError(t, err)
	require.Len(t, result.Files, 2)
	assert.Equal(t, 0, result.PrunedCount, "varchar mismatches are unknown and must be kept")
}

func TestDiscoverHivePartitions_MultiLevelOrPruneExpr(t *testing.T) {
	dirs := map[string][]fileservice.DirEntry{
		"/data": {
			{Name: "year=2024", IsDir: true},
			{Name: "year=2025", IsDir: true},
		},
		"/data/year=2024": {
			{Name: "month=01", IsDir: true},
			{Name: "month=02", IsDir: true},
		},
		"/data/year=2025": {
			{Name: "month=01", IsDir: true},
			{Name: "month=02", IsDir: true},
		},
		"/data/year=2024/month=01": {{Name: "a.parquet", IsDir: false, Size: 100}},
		"/data/year=2025/month=02": {{Name: "b.parquet", IsDir: false, Size: 200}},
	}
	pruneExpr := &PartitionOr{Children: []PartitionPruneExpr{
		&PartitionAnd{Children: []PartitionPruneExpr{
			&PartitionAtom{ColName: "year", Op: PartOpEq, Values: []string{"2024"}},
			&PartitionAtom{ColName: "month", Op: PartOpEq, Values: []string{"1"}},
		}},
		&PartitionAnd{Children: []PartitionPruneExpr{
			&PartitionAtom{ColName: "year", Op: PartOpEq, Values: []string{"2025"}},
			&PartitionAtom{ColName: "month", Op: PartOpEq, Values: []string{"2"}},
		}},
	}}

	result, err := DiscoverHivePartitionsWithPruneExpr(
		context.Background(),
		mockListDir(dirs),
		"/data",
		[]string{"year", "month"},
		[]tree.HivePartColType{
			{Id: int32(types.T_int32)},
			{Id: int32(types.T_int32)},
		},
		pruneExpr,
		nil,
	)
	require.NoError(t, err)
	require.Len(t, result.Files, 2)
	assert.Equal(t, "/data/year=2024/month=01/a.parquet", result.Files[0].FilePath)
	assert.Equal(t, "/data/year=2025/month=02/b.parquet", result.Files[1].FilePath)
	assert.Equal(t, 2, result.PrunedCount)
}

func TestDiscoverHivePartitions_HalfBoundOrKeepsUnknownBranch(t *testing.T) {
	dirs := map[string][]fileservice.DirEntry{
		"/data": {
			{Name: "part_a=1", IsDir: true},
			{Name: "part_a=3", IsDir: true},
		},
		"/data/part_a=1": {
			{Name: "part_b=1", IsDir: true},
		},
		"/data/part_a=3": {
			{Name: "part_b=2", IsDir: true},
		},
		"/data/part_a=1/part_b=1": {{Name: "a.parquet", IsDir: false, Size: 100}},
		"/data/part_a=3/part_b=2": {{Name: "b.parquet", IsDir: false, Size: 200}},
	}
	pruneExpr := &PartitionOr{Children: []PartitionPruneExpr{
		&PartitionAtom{ColName: "part_a", Op: PartOpEq, Values: []string{"1"}},
		&PartitionAtom{ColName: "part_b", Op: PartOpEq, Values: []string{"2"}},
	}}

	result, err := DiscoverHivePartitionsWithPruneExpr(
		context.Background(),
		mockListDir(dirs),
		"/data",
		[]string{"part_a", "part_b"},
		[]tree.HivePartColType{
			{Id: int32(types.T_int32)},
			{Id: int32(types.T_int32)},
		},
		pruneExpr,
		nil,
	)
	require.NoError(t, err)
	require.Len(t, result.Files, 2)
	assert.Equal(t, "/data/part_a=1/part_b=1/a.parquet", result.Files[0].FilePath)
	assert.Equal(t, "/data/part_a=3/part_b=2/b.parquet", result.Files[1].FilePath)
}

func TestDiscoverHivePartitions_IsNullPruneExpr(t *testing.T) {
	dirs := map[string][]fileservice.DirEntry{
		"/data": {
			{Name: "year=2024", IsDir: true},
			{Name: "year=__HIVE_DEFAULT_PARTITION__", IsDir: true},
		},
		"/data/year=__HIVE_DEFAULT_PARTITION__": {{Name: "null.parquet", IsDir: false, Size: 100}},
		"/data/year=2024":                       {{Name: "nonnull.parquet", IsDir: false, Size: 200}},
	}

	result, err := DiscoverHivePartitionsWithPruneExpr(
		context.Background(),
		mockListDir(dirs),
		"/data",
		[]string{"year"},
		[]tree.HivePartColType{{Id: int32(types.T_int32)}},
		&PartitionAtom{ColName: "year", Op: PartOpIsNull},
		nil,
	)
	require.NoError(t, err)
	require.Len(t, result.Files, 1)
	assert.Equal(t, "/data/year=__HIVE_DEFAULT_PARTITION__/null.parquet", result.Files[0].FilePath)

	result, err = DiscoverHivePartitionsWithPruneExpr(
		context.Background(),
		mockListDir(dirs),
		"/data",
		[]string{"year"},
		[]tree.HivePartColType{{Id: int32(types.T_int32)}},
		&PartitionAtom{ColName: "year", Op: PartOpIsNotNull},
		nil,
	)
	require.NoError(t, err)
	require.Len(t, result.Files, 1)
	assert.Equal(t, "/data/year=2024/nonnull.parquet", result.Files[0].FilePath)
}

func TestHivePartitionListCache_DisabledCallsRealList(t *testing.T) {
	ResetHivePartitionListCacheForTest()
	var calls atomic.Int32
	dirs := map[string][]fileservice.DirEntry{
		"/data":           {{Name: "year=2024", IsDir: true}},
		"/data/year=2024": {{Name: "f.parquet", IsDir: false, Size: 100}},
	}
	listDir := func(ctx context.Context, prefix string) iter.Seq2[*fileservice.DirEntry, error] {
		calls.Add(1)
		return mockListDir(dirs)(ctx, prefix)
	}

	for i := 0; i < 2; i++ {
		_, err := DiscoverHivePartitionsWithPruneExpr(
			context.Background(),
			listDir,
			"/data",
			[]string{"year"},
			[]tree.HivePartColType{{Id: int32(types.T_int32)}},
			nil,
			nil,
		)
		require.NoError(t, err)
	}
	assert.Equal(t, int32(4), calls.Load())
}

func TestHivePartitionListCache_EnabledHitsSecondQuery(t *testing.T) {
	ResetHivePartitionListCacheForTest()
	var calls atomic.Int32
	dirs := map[string][]fileservice.DirEntry{
		"/data":           {{Name: "year=2024", IsDir: true}},
		"/data/year=2024": {{Name: "f.parquet", IsDir: false, Size: 100}},
	}
	listDir := func(ctx context.Context, prefix string) iter.Seq2[*fileservice.DirEntry, error] {
		calls.Add(1)
		return mockListDir(dirs)(ctx, prefix)
	}
	opts := &DiscoverOptions{
		CacheTTL:       time.Minute,
		CacheKeyPrefix: "test-cache",
	}

	first, err := DiscoverHivePartitionsWithPruneExpr(
		context.Background(), listDir, "/data",
		[]string{"year"}, []tree.HivePartColType{{Id: int32(types.T_int32)}}, nil, opts)
	require.NoError(t, err)
	assert.Equal(t, 0, first.CacheHits)
	assert.Equal(t, 2, first.CacheMisses)
	assert.Equal(t, 2, first.ListCalls)

	second, err := DiscoverHivePartitionsWithPruneExpr(
		context.Background(), listDir, "/data",
		[]string{"year"}, []tree.HivePartColType{{Id: int32(types.T_int32)}}, nil, opts)
	require.NoError(t, err)
	assert.Equal(t, 2, second.CacheHits)
	assert.Equal(t, 0, second.CacheMisses)
	assert.Equal(t, 0, second.ListCalls, "warm cache hit must not count as a real List call")
	assert.Equal(t, int32(2), calls.Load())
}

func TestHivePartitionDirectPrefixStats_WarmCacheSkipsRealParentList(t *testing.T) {
	ResetHivePartitionListCacheForTest()
	var mu sync.Mutex
	calls := make(map[string]int)
	dirs := map[string][]fileservice.DirEntry{
		"/data": {
			{Name: "year=2024", IsDir: true},
			{Name: "year=2025", IsDir: true},
		},
		"/data/year=2024": {{Name: "f2024.parquet", IsDir: false, Size: 100}},
		"/data/year=2025": {{Name: "f2025.parquet", IsDir: false, Size: 200}},
	}
	listDir := func(ctx context.Context, prefix string) iter.Seq2[*fileservice.DirEntry, error] {
		mu.Lock()
		calls[prefix]++
		mu.Unlock()
		return mockListDir(dirs)(ctx, prefix)
	}
	opts := &DiscoverOptions{CacheTTL: time.Minute, CacheKeyPrefix: "direct-warm"}

	first, err := DiscoverHivePartitionsWithPruneExpr(
		context.Background(), listDir, "/data",
		[]string{"year"}, []tree.HivePartColType{{Id: int32(types.T_int32)}}, nil, opts)
	require.NoError(t, err)
	assert.Equal(t, 0, first.DirectPrefixHits)
	assert.Equal(t, 3, first.DirectPrefixMisses)

	second, err := DiscoverHivePartitionsWithPruneExpr(
		context.Background(), listDir, "/data",
		[]string{"year"}, []tree.HivePartColType{{Id: int32(types.T_int32)}},
		&PartitionAtom{ColName: "year", Op: PartOpEq, Values: []string{"2024"}}, opts)
	require.NoError(t, err)
	require.Len(t, second.Files, 1)
	assert.Equal(t, "/data/year=2024/f2024.parquet", second.Files[0].FilePath)
	assert.Equal(t, 2, second.DirectPrefixHits)
	assert.Equal(t, 0, second.DirectPrefixMisses)
	assert.Equal(t, 0, second.ListCalls)

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, 1, calls["/data"], "warm direct prefix must not re-list the parent directory")
	assert.Equal(t, 1, calls["/data/year=2024"], "warm direct prefix reused the cached leaf file list")
	assert.Equal(t, 1, calls["/data/year=2025"])
}

func TestHivePartitionDirectPrefix_UsesCachedRawSegments(t *testing.T) {
	tests := []struct {
		name             string
		partCol          string
		colType          tree.HivePartColType
		rootEntry        string
		atomValue        string
		leafPrefix       string
		unwantedPrefixes []string
	}{
		{
			name:       "zero padded integer",
			partCol:    "month",
			colType:    tree.HivePartColType{Id: int32(types.T_int32)},
			rootEntry:  "month=01",
			atomValue:  "1",
			leafPrefix: "/data/month=01",
			unwantedPrefixes: []string{
				"/data/month=1",
			},
		},
		{
			name:       "url encoded string",
			partCol:    "country",
			colType:    tree.HivePartColType{Id: int32(types.T_varchar)},
			rootEntry:  "country=US%2FCA",
			atomValue:  "US/CA",
			leafPrefix: "/data/country=US%2FCA",
			unwantedPrefixes: []string{
				"/data/country=US/CA",
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ResetHivePartitionListCacheForTest()
			cachePrefix := "direct-raw-" + tc.name
			globalHivePartitionListCache.set(
				cachePrefix+"\x1f"+"prefix=/data",
				[]fileservice.DirEntry{{Name: tc.rootEntry, IsDir: true}},
				time.Minute, 10, 1<<20)

			var mu sync.Mutex
			calls := make(map[string]int)
			dirs := map[string][]fileservice.DirEntry{
				tc.leafPrefix: {{Name: "f.parquet", IsDir: false, Size: 100}},
			}
			listDir := func(ctx context.Context, prefix string) iter.Seq2[*fileservice.DirEntry, error] {
				mu.Lock()
				calls[prefix]++
				mu.Unlock()
				return mockListDir(dirs)(ctx, prefix)
			}

			result, err := DiscoverHivePartitionsWithPruneExpr(
				context.Background(),
				listDir,
				"/data",
				[]string{tc.partCol},
				[]tree.HivePartColType{tc.colType},
				&PartitionAtom{ColName: tc.partCol, Op: PartOpEq, Values: []string{tc.atomValue}},
				&DiscoverOptions{CacheTTL: time.Minute, CacheKeyPrefix: cachePrefix},
			)
			require.NoError(t, err)
			require.Len(t, result.Files, 1)
			assert.Equal(t, tc.leafPrefix+"/f.parquet", result.Files[0].FilePath)
			assert.Equal(t, 1, result.DirectPrefixHits)
			assert.Equal(t, 1, result.DirectPrefixMisses)

			mu.Lock()
			defer mu.Unlock()
			assert.Equal(t, 0, calls["/data"], "cached parent metadata avoids re-listing root")
			assert.Equal(t, 1, calls[tc.leafPrefix], "discovery must list the observed raw child prefix")
			for _, prefix := range tc.unwantedPrefixes {
				assert.Equal(t, 0, calls[prefix], "must not synthesize raw prefix from SQL literal")
			}
		})
	}
}

func TestHivePartitionDirectPrefix_DuplicateRawIntegerRepresentations(t *testing.T) {
	ResetHivePartitionListCacheForTest()
	cachePrefix := "direct-duplicate-raw"
	globalHivePartitionListCache.set(
		cachePrefix+"\x1f"+"prefix=/data",
		[]fileservice.DirEntry{
			{Name: "year=02024", IsDir: true},
			{Name: "year=2024", IsDir: true},
		},
		time.Minute, 10, 1<<20)

	var mu sync.Mutex
	calls := make(map[string]int)
	dirs := map[string][]fileservice.DirEntry{
		"/data/year=02024": {{Name: "padded.parquet", IsDir: false, Size: 100}},
		"/data/year=2024":  {{Name: "plain.parquet", IsDir: false, Size: 200}},
	}
	listDir := func(ctx context.Context, prefix string) iter.Seq2[*fileservice.DirEntry, error] {
		mu.Lock()
		calls[prefix]++
		mu.Unlock()
		return mockListDir(dirs)(ctx, prefix)
	}

	result, err := DiscoverHivePartitionsWithPruneExpr(
		context.Background(),
		listDir,
		"/data",
		[]string{"year"},
		[]tree.HivePartColType{{Id: int32(types.T_int32)}},
		&PartitionAtom{ColName: "year", Op: PartOpEq, Values: []string{"2024"}},
		&DiscoverOptions{CacheTTL: time.Minute, CacheKeyPrefix: cachePrefix},
	)
	require.NoError(t, err)
	require.Len(t, result.Files, 2)
	assert.Equal(t, "/data/year=02024/padded.parquet", result.Files[0].FilePath)
	assert.Equal(t, "/data/year=2024/plain.parquet", result.Files[1].FilePath)

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, 0, calls["/data"])
	assert.Equal(t, 1, calls["/data/year=02024"])
	assert.Equal(t, 1, calls["/data/year=2024"])
}

func TestHivePartitionListCache_TTLExpiryAndErrorNotCached(t *testing.T) {
	ResetHivePartitionListCacheForTest()
	var calls atomic.Int32
	wantErr := errors.New("temporary list error")
	listDir := func(ctx context.Context, prefix string) iter.Seq2[*fileservice.DirEntry, error] {
		call := calls.Add(1)
		return func(yield func(*fileservice.DirEntry, error) bool) {
			if call == 1 {
				yield(nil, wantErr)
				return
			}
			yield(&fileservice.DirEntry{Name: "year=2024", IsDir: true}, nil)
		}
	}
	opts := DiscoverOptions{CacheTTL: 10 * time.Millisecond, CacheKeyPrefix: "ttl"}
	result := &PartitionDiscoveryResult{}
	_, err := listHivePartitionDir(context.Background(), listDir, "/data", opts, result)
	require.ErrorIs(t, err, wantErr)

	entries, err := listHivePartitionDir(context.Background(), listDir, "/data", opts, result)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, int32(2), calls.Load())

	time.Sleep(20 * time.Millisecond)
	_, err = listHivePartitionDir(context.Background(), listDir, "/data", opts, result)
	require.NoError(t, err)
	assert.Equal(t, int32(3), calls.Load())
}

func TestHivePartitionListCache_DeepCopyAndEviction(t *testing.T) {
	ResetHivePartitionListCacheForTest()
	globalHivePartitionListCache.set("a", []fileservice.DirEntry{{Name: "year=2024", IsDir: true}}, time.Minute, 10, 1<<20)
	entries, ok := globalHivePartitionListCache.get("a")
	require.True(t, ok)
	entries[0].Name = "mutated"

	entries, ok = globalHivePartitionListCache.get("a")
	require.True(t, ok)
	assert.Equal(t, "year=2024", entries[0].Name)

	globalHivePartitionListCache.set("b", []fileservice.DirEntry{{Name: "year=2025", IsDir: true}}, time.Minute, 1, 1<<20)
	globalHivePartitionListCache.mu.Lock()
	_, hasA := globalHivePartitionListCache.entries["a"]
	_, hasB := globalHivePartitionListCache.entries["b"]
	globalHivePartitionListCache.mu.Unlock()
	assert.False(t, hasA)
	assert.True(t, hasB)

	ResetHivePartitionListCacheForTest()
	globalHivePartitionListCache.mu.Lock()
	assert.Empty(t, globalHivePartitionListCache.entries)
	globalHivePartitionListCache.mu.Unlock()
}

func TestHivePartitionListCache_Singleflight(t *testing.T) {
	ResetHivePartitionListCacheForTest()
	var calls atomic.Int32
	started := make(chan struct{})
	release := make(chan struct{})
	var closeStarted sync.Once
	listDir := func(ctx context.Context, prefix string) iter.Seq2[*fileservice.DirEntry, error] {
		calls.Add(1)
		closeStarted.Do(func() { close(started) })
		<-release
		return func(yield func(*fileservice.DirEntry, error) bool) {
			yield(&fileservice.DirEntry{Name: "year=2024", IsDir: true}, nil)
		}
	}
	opts := DiscoverOptions{CacheTTL: time.Minute, CacheKeyPrefix: "singleflight"}

	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := listHivePartitionDir(context.Background(), listDir, "/data", opts, &PartitionDiscoveryResult{})
			errs <- err
		}()
	}
	<-started
	close(release)
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	assert.Equal(t, int32(1), calls.Load())
}

func TestHivePartitionListCacheKey_DoesNotExposeSecret(t *testing.T) {
	param := &tree.ExternParam{
		ExParamConst: tree.ExParamConst{ScanType: tree.S3, Filepath: "s3://bucket/root"},
		ExParam: tree.ExParam{S3Param: &tree.S3Parameter{
			Endpoint:   "https://s3.example.com/",
			Bucket:     "bucket",
			Provider:   "AWS",
			APIKey:     "AKIA_TEST",
			APISecret:  "super-secret",
			RoleArn:    "arn:aws:iam::1:role/r",
			ExternalId: "external",
		}},
	}

	key := BuildHivePartitionListCacheKey(param, 42, "/root", "/root/year=2024")
	assert.Contains(t, key, "account=42")
	assert.Contains(t, key, "provider=aws")
	assert.NotContains(t, key, "super-secret")
	assert.NotContains(t, key, "AKIA_TEST")
	assert.Contains(t, key, hashHivePartitionAccessKeyID("AKIA_TEST"))
}

func TestHivePartitionListCacheKey_IsolatesIdentityDimensions(t *testing.T) {
	makeParam := func() *tree.ExternParam {
		return &tree.ExternParam{
			ExParamConst: tree.ExParamConst{ScanType: tree.S3, Filepath: "s3://bucket/root"},
			ExParam: tree.ExParam{S3Param: &tree.S3Parameter{
				Endpoint:   "https://s3.example.com/",
				Bucket:     "bucket-a",
				Provider:   "AWS",
				APIKey:     "AKIA_A",
				APISecret:  "secret-a",
				RoleArn:    "arn:aws:iam::1:role/a",
				ExternalId: "external-a",
			}},
		}
	}
	base := BuildHivePartitionListCacheKey(makeParam(), 42, "/root", "/root/year=2024")

	assert.NotEqual(t, base, BuildHivePartitionListCacheKey(makeParam(), 43, "/root", "/root/year=2024"))

	p := makeParam()
	p.S3Param.Endpoint = "https://s3.other.example.com/"
	assert.NotEqual(t, base, BuildHivePartitionListCacheKey(p, 42, "/root", "/root/year=2024"))

	p = makeParam()
	p.S3Param.Bucket = "bucket-b"
	assert.NotEqual(t, base, BuildHivePartitionListCacheKey(p, 42, "/root", "/root/year=2024"))

	p = makeParam()
	p.S3Param.RoleArn = "arn:aws:iam::1:role/b"
	assert.NotEqual(t, base, BuildHivePartitionListCacheKey(p, 42, "/root", "/root/year=2024"))

	p = makeParam()
	p.S3Param.APIKey = "AKIA_B"
	assert.NotEqual(t, base, BuildHivePartitionListCacheKey(p, 42, "/root", "/root/year=2024"))

	p = makeParam()
	p.S3Param.APISecret = "secret-b"
	assert.Equal(t, base, BuildHivePartitionListCacheKey(p, 42, "/root", "/root/year=2024"),
		"cache identity intentionally excludes APISecret")
}

func TestDiscoverHivePartitions_ListConcurrencyBoundAndStableOrder(t *testing.T) {
	var active atomic.Int32
	var maxActive atomic.Int32
	dirs := map[string][]fileservice.DirEntry{
		"/data": {
			{Name: "year=2025", IsDir: true},
			{Name: "year=2021", IsDir: true},
			{Name: "year=2024", IsDir: true},
			{Name: "year=2022", IsDir: true},
			{Name: "year=2023", IsDir: true},
		},
	}
	for year := 2021; year <= 2025; year++ {
		prefix := fmt.Sprintf("/data/year=%d", year)
		dirs[prefix] = []fileservice.DirEntry{{Name: fmt.Sprintf("f%d.parquet", year), IsDir: false, Size: int64(year)}}
	}
	listDir := func(ctx context.Context, prefix string) iter.Seq2[*fileservice.DirEntry, error] {
		cur := active.Add(1)
		for {
			old := maxActive.Load()
			if cur <= old || maxActive.CompareAndSwap(old, cur) {
				break
			}
		}
		time.Sleep(5 * time.Millisecond)
		active.Add(-1)
		return mockListDir(dirs)(ctx, prefix)
	}

	result, err := DiscoverHivePartitionsWithPruneExpr(
		context.Background(),
		listDir,
		"/data",
		[]string{"year"},
		[]tree.HivePartColType{{Id: int32(types.T_int32)}},
		nil,
		&DiscoverOptions{ListConcurrency: 2},
	)
	require.NoError(t, err)
	assert.LessOrEqual(t, maxActive.Load(), int32(2))
	require.Len(t, result.Files, 5)
	assert.Equal(t, "/data/year=2021/f2021.parquet", result.Files[0].FilePath)
	assert.Equal(t, "/data/year=2025/f2025.parquet", result.Files[4].FilePath)
}

func TestDiscoverHivePartitions_ConcurrentMaxListCalls(t *testing.T) {
	dirs := map[string][]fileservice.DirEntry{
		"/data": {
			{Name: "year=2021", IsDir: true},
			{Name: "year=2022", IsDir: true},
			{Name: "year=2023", IsDir: true},
		},
		"/data/year=2021": {{Name: "a.parquet", IsDir: false, Size: 100}},
		"/data/year=2022": {{Name: "b.parquet", IsDir: false, Size: 200}},
		"/data/year=2023": {{Name: "c.parquet", IsDir: false, Size: 300}},
	}

	_, err := DiscoverHivePartitionsWithPruneExpr(
		context.Background(),
		mockListDir(dirs),
		"/data",
		[]string{"year"},
		[]tree.HivePartColType{{Id: int32(types.T_int32)}},
		nil,
		&DiscoverOptions{ListConcurrency: 3, MaxListCalls: 2},
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "List calls")
}

func TestDiscoverHivePartitions_ConcurrentMaxPartitions(t *testing.T) {
	dirs := map[string][]fileservice.DirEntry{
		"/data": {
			{Name: "year=2021", IsDir: true},
			{Name: "year=2022", IsDir: true},
			{Name: "year=2023", IsDir: true},
		},
		"/data/year=2021": {{Name: "a.parquet", IsDir: false, Size: 100}},
		"/data/year=2022": {{Name: "b.parquet", IsDir: false, Size: 200}},
		"/data/year=2023": {{Name: "c.parquet", IsDir: false, Size: 300}},
	}

	_, err := DiscoverHivePartitionsWithPruneExpr(
		context.Background(),
		mockListDir(dirs),
		"/data",
		[]string{"year"},
		[]tree.HivePartColType{{Id: int32(types.T_int32)}},
		nil,
		&DiscoverOptions{ListConcurrency: 3, MaxPartitions: 2},
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "partitions")
}

func TestDiscoverHivePartitions_MultiLevelPartialPredicate(t *testing.T) {
	dirs := map[string][]fileservice.DirEntry{
		"/data": {
			{Name: "year=2024", IsDir: true},
			{Name: "year=2025", IsDir: true},
		},
		"/data/year=2024": {
			{Name: "month=01", IsDir: true},
			{Name: "month=02", IsDir: true},
			{Name: "month=03", IsDir: true},
		},
		"/data/year=2024/month=01": {
			{Name: "f.parquet", IsDir: false, Size: 100},
		},
		"/data/year=2024/month=02": {
			{Name: "f.parquet", IsDir: false, Size: 200},
		},
		"/data/year=2024/month=03": {
			{Name: "f.parquet", IsDir: false, Size: 300},
		},
	}

	result, err := DiscoverHivePartitions(
		context.Background(),
		mockListDir(dirs),
		"/data",
		[]string{"year", "month"},
		[]tree.HivePartColType{
			{Id: int32(types.T_int32)},
			{Id: int32(types.T_int32)},
		},
		[]PartitionPredicate{{ColName: "year", Op: PartOpEq, Values: []string{"2024"}}},
	)
	require.NoError(t, err)
	// year level: 1 pruned (2025), 1 kept (2024)
	// month level: no predicate, all 3 enter
	// ListCalls: root(1) + year=2024 months(1) + 3 file listings = 5
	assert.Equal(t, 5, result.ListCalls)
	assert.Equal(t, 1, result.PrunedCount, "only year=2025 is pruned")
	assert.Equal(t, 4, result.PartitionCount, "year=2024 + month=01 + month=02 + month=03")
	assert.Equal(t, 3, len(result.Files))
}

func TestDiscoverHivePartitions_WarnPartitionCount(t *testing.T) {
	// warnPartitionCount=5000. Use 5001 partitions.
	// List calls = 1 (root) + 5001 (file listing per partition) = 5002, under maxListCalls(10000).
	entries := make([]fileservice.DirEntry, 5001)
	for i := range entries {
		entries[i] = fileservice.DirEntry{Name: fmt.Sprintf("year=%d", i), IsDir: true}
	}
	fileEntries := []fileservice.DirEntry{{Name: "f.parquet", IsDir: false, Size: 10}}

	listDir := func(ctx context.Context, prefix string) iter.Seq2[*fileservice.DirEntry, error] {
		return func(yield func(*fileservice.DirEntry, error) bool) {
			if prefix == "/data" {
				for i := range entries {
					if !yield(&entries[i], nil) {
						return
					}
				}
			} else {
				for i := range fileEntries {
					if !yield(&fileEntries[i], nil) {
						return
					}
				}
			}
		}
	}

	result, err := DiscoverHivePartitions(
		context.Background(),
		listDir,
		"/data",
		[]string{"year"},
		[]tree.HivePartColType{{Id: int32(types.T_int32)}},
		nil,
	)
	require.NoError(t, err, "5001 partitions should NOT error (only warn)")
	assert.Equal(t, 5001, result.PartitionCount)
	assert.True(t, result.warnEmitted, "warning should have been emitted for >5000 partitions")
	assert.Equal(t, 5001, len(result.Files))
}

func TestPartitionPruneReferencedColsCoverageHack(t *testing.T) {
	var nilAtom *PartitionAtom
	assert.Nil(t, nilAtom.ReferencedCols())

	atom := &PartitionAtom{ColName: "Year", Op: PartOpEq, Values: []string{"2024"}}
	assert.Equal(t, map[string]bool{"year": true}, atom.ReferencedCols())

	andExpr := &PartitionAnd{Children: []PartitionPruneExpr{
		atom,
		&PartitionAtom{ColName: "Month", Op: PartOpEq, Values: []string{"01"}},
	}}
	assert.Equal(t, map[string]bool{"year": true, "month": true}, andExpr.ReferencedCols())

	orExpr := &PartitionOr{Children: []PartitionPruneExpr{andExpr}}
	assert.Equal(t, map[string]bool{"year": true, "month": true}, orExpr.ReferencedCols())
	assert.Nil(t, mergeReferencedCols(nil))
}

func TestPartitionCompareUnsignedCoverageHack(t *testing.T) {
	colType := tree.HivePartColType{Id: int32(types.T_uint8)}
	assert.Equal(t, MatchTrue, matchPartitionCompare("4", []string{"5"}, colType, PartOpLt))
	assert.Equal(t, MatchTrue, matchPartitionCompare("5", []string{"5"}, colType, PartOpLe))
	assert.Equal(t, MatchTrue, matchPartitionCompare("6", []string{"5"}, colType, PartOpGt))
	assert.Equal(t, MatchTrue, matchPartitionCompare("5", []string{"5"}, colType, PartOpGe))
	assert.Equal(t, MatchFalse, matchPartitionCompare("4", []string{"5"}, colType, PartOpGt))
	assert.Equal(t, MatchUnknown, matchPartitionCompare("4", []string{"5", "6"}, colType, PartOpGt))
	assert.False(t, compareUnsigned(1, 1, PartOpBetween))
	assert.False(t, compareSigned(1, 1, PartOpBetween))
}

func TestGetNegatedLiteralStringCoverageHack(t *testing.T) {
	litExpr := func(lit *plan.Literal) *plan.Expr {
		return &plan.Expr{Expr: &plan.Expr_Lit{Lit: lit}}
	}
	cases := []struct {
		name string
		lit  *plan.Literal
		want string
	}{
		{name: "i8", lit: &plan.Literal{Value: &plan.Literal_I8Val{I8Val: 1}}, want: "-1"},
		{name: "i16", lit: &plan.Literal{Value: &plan.Literal_I16Val{I16Val: 2}}, want: "-2"},
		{name: "i32", lit: &plan.Literal{Value: &plan.Literal_I32Val{I32Val: 3}}, want: "-3"},
		{name: "i64", lit: &plan.Literal{Value: &plan.Literal_I64Val{I64Val: 4}}, want: "-4"},
		{name: "u8 zero", lit: &plan.Literal{Value: &plan.Literal_U8Val{U8Val: 0}}, want: "0"},
		{name: "u8", lit: &plan.Literal{Value: &plan.Literal_U8Val{U8Val: 5}}, want: "-5"},
		{name: "u16 zero", lit: &plan.Literal{Value: &plan.Literal_U16Val{U16Val: 0}}, want: "0"},
		{name: "u16", lit: &plan.Literal{Value: &plan.Literal_U16Val{U16Val: 6}}, want: "-6"},
		{name: "u32 zero", lit: &plan.Literal{Value: &plan.Literal_U32Val{U32Val: 0}}, want: "0"},
		{name: "u32", lit: &plan.Literal{Value: &plan.Literal_U32Val{U32Val: 7}}, want: "-7"},
		{name: "u64 zero", lit: &plan.Literal{Value: &plan.Literal_U64Val{U64Val: 0}}, want: "0"},
		{name: "u64", lit: &plan.Literal{Value: &plan.Literal_U64Val{U64Val: 8}}, want: "-8"},
		{name: "float", lit: &plan.Literal{Value: &plan.Literal_Fval{Fval: 1.5}}, want: "-1.5"},
		{name: "double", lit: &plan.Literal{Value: &plan.Literal_Dval{Dval: 2.5}}, want: "-2.5"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := getNegatedLiteralString(litExpr(tc.lit))
			require.True(t, ok)
			assert.Equal(t, tc.want, got)
		})
	}

	_, ok := getNegatedLiteralString(&plan.Expr{Expr: &plan.Expr_Col{Col: &plan.ColRef{Name: "year"}}})
	assert.False(t, ok)
	_, ok = getNegatedLiteralString(litExpr(nil))
	assert.False(t, ok)
	_, ok = getNegatedLiteralString(litExpr(&plan.Literal{Isnull: true}))
	assert.False(t, ok)
	_, ok = getNegatedLiteralString(litExpr(&plan.Literal{Value: &plan.Literal_Sval{Sval: "x"}}))
	assert.False(t, ok)
}
