// Copyright 2026 Matrix Origin
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
	"fmt"
	"iter"
	"strconv"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func BenchmarkDiscoverHivePartitions(b *testing.B) {
	for _, partitions := range []int{100, 1000, 5000} {
		for _, concurrency := range []int{1, 8} {
			b.Run(fmt.Sprintf("partitions=%d/concurrency=%d", partitions, concurrency), func(b *testing.B) {
				listDir := benchmarkPartitionListDir(partitions)
				opts := &DiscoverOptions{ListConcurrency: concurrency}
				colTypes := []tree.HivePartColType{{Id: int32(types.T_int32)}}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					result, err := DiscoverHivePartitionsWithPruneExpr(
						context.Background(),
						listDir,
						"/bench",
						[]string{"year"},
						colTypes,
						nil,
						opts,
					)
					if err != nil {
						b.Fatal(err)
					}
					if len(result.Files) != partitions {
						b.Fatalf("got %d files, want %d", len(result.Files), partitions)
					}
				}
			})
		}
	}
}

func BenchmarkPartitionPruneExprEval(b *testing.B) {
	colTypes := map[string]tree.HivePartColType{
		"year":  {Id: int32(types.T_int32)},
		"month": {Id: int32(types.T_int32)},
	}
	values := map[string]string{"year": "2450900", "month": "6"}

	largeOR := &PartitionOr{Children: make([]PartitionPruneExpr, 0, 1000)}
	for i := 0; i < 1000; i++ {
		largeOR.Children = append(largeOR.Children, &PartitionAtom{
			ColName: "year",
			Op:      PartOpEq,
			Values:  []string{strconv.Itoa(2450000 + i)},
		})
	}

	deepAND := &PartitionAnd{Children: make([]PartitionPruneExpr, 0, 128)}
	for i := 0; i < 128; i++ {
		deepAND.Children = append(deepAND.Children, &PartitionAtom{
			ColName: "year",
			Op:      PartOpGe,
			Values:  []string{"2450000"},
		})
	}

	partialBinding := &PartitionOr{Children: []PartitionPruneExpr{
		&PartitionAnd{Children: []PartitionPruneExpr{
			&PartitionAtom{ColName: "year", Op: PartOpEq, Values: []string{"2450900"}},
			&PartitionAtom{ColName: "month", Op: PartOpEq, Values: []string{"7"}},
		}},
		&PartitionAnd{Children: []PartitionPruneExpr{
			&PartitionAtom{ColName: "year", Op: PartOpEq, Values: []string{"2450901"}},
			&PartitionAtom{ColName: "month", Op: PartOpEq, Values: []string{"6"}},
		}},
	}}

	for _, tc := range []struct {
		name string
		expr PartitionPruneExpr
	}{
		{"large-or", largeOR},
		{"deep-and", deepAND},
		{"partial-binding-or", partialBinding},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = tc.expr.EvalPartial(values, colTypes)
			}
		})
	}
}

func BenchmarkHivePartitionListCache(b *testing.B) {
	entries := makeBenchmarkDirEntries(500)
	listDir := func(ctx context.Context, prefix string) iter.Seq2[*fileservice.DirEntry, error] {
		return func(yield func(*fileservice.DirEntry, error) bool) {
			for i := range entries {
				if !yield(&entries[i], nil) {
					return
				}
			}
		}
	}
	opts := DiscoverOptions{
		CacheTTL:        time.Minute,
		CacheKeyPrefix:  "bench-cache",
		CacheMaxEntries: 8192,
		CacheMaxBytes:   256 << 20,
		MaxListCalls:    1 << 30,
		statsMu:         nil,
	}

	b.Run("cold", func(b *testing.B) {
		ResetHivePartitionListCacheForTest()
		result := &PartitionDiscoveryResult{}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			opts.CacheKeyPrefix = fmt.Sprintf("bench-cache-cold-%d", i)
			if _, err := listHivePartitionDir(context.Background(), listDir, "/bench", opts, result); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("warm", func(b *testing.B) {
		ResetHivePartitionListCacheForTest()
		result := &PartitionDiscoveryResult{}
		if _, err := listHivePartitionDir(context.Background(), listDir, "/bench", opts, result); err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := listHivePartitionDir(context.Background(), listDir, "/bench", opts, result); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("expired", func(b *testing.B) {
		ResetHivePartitionListCacheForTest()
		result := &PartitionDiscoveryResult{}
		key := opts.CacheKeyPrefix + "\x1f" + "prefix=" + normalizeExternalPath("/bench")
		if _, err := listHivePartitionDir(context.Background(), listDir, "/bench", opts, result); err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			globalHivePartitionListCache.mu.Lock()
			globalHivePartitionListCache.entries[key].ExpireAt = time.Now().Add(-time.Second)
			globalHivePartitionListCache.mu.Unlock()
			if _, err := listHivePartitionDir(context.Background(), listDir, "/bench", opts, result); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkHivePartitionCacheEntrySize50000(b *testing.B) {
	entries := makeBenchmarkDirEntries(50000)
	size := estimateDirEntriesSize(entries)
	b.ReportMetric(float64(size), "entryset-bytes")
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = cloneDirEntries(entries)
	}
}

func BenchmarkHivePartitionCacheEviction(b *testing.B) {
	entries := makeBenchmarkDirEntries(100)
	ResetHivePartitionListCacheForTest()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		globalHivePartitionListCache.set(
			fmt.Sprintf("evict-%d", i),
			entries,
			time.Minute,
			128,
			int64(estimateDirEntriesSize(entries)*64),
		)
	}
	b.StopTimer()
	globalHivePartitionListCache.mu.Lock()
	b.ReportMetric(float64(len(globalHivePartitionListCache.entries)), "entries-live")
	b.ReportMetric(float64(globalHivePartitionListCache.bytes), "bytes-live")
	globalHivePartitionListCache.mu.Unlock()
}

func benchmarkPartitionListDir(partitions int) ListDirFunc {
	rootEntries := make([]fileservice.DirEntry, partitions)
	for i := range rootEntries {
		rootEntries[i] = fileservice.DirEntry{Name: fmt.Sprintf("year=%d", 2000+i), IsDir: true}
	}
	fileEntries := []fileservice.DirEntry{{Name: "data.parquet", Size: 1024}}
	return func(ctx context.Context, prefix string) iter.Seq2[*fileservice.DirEntry, error] {
		return func(yield func(*fileservice.DirEntry, error) bool) {
			entries := fileEntries
			if prefix == "/bench" {
				entries = rootEntries
			}
			for i := range entries {
				if !yield(&entries[i], nil) {
					return
				}
			}
		}
	}
}

func makeBenchmarkDirEntries(n int) []fileservice.DirEntry {
	entries := make([]fileservice.DirEntry, n)
	for i := range entries {
		entries[i] = fileservice.DirEntry{
			Name:  fmt.Sprintf("year=%05d", i),
			IsDir: true,
			Size:  1024,
		}
	}
	return entries
}
