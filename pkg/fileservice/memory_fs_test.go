// Copyright 2022 Matrix Origin
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

package fileservice

import (
	"context"
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMemoryFS(t *testing.T) {

	t.Run("file service", func(t *testing.T) {
		testFileService(t, 0, func(name string) FileService {
			fs, err := NewMemoryFS(name, DisabledCacheConfig, nil)
			assert.Nil(t, err)
			return fs
		})
	})

	t.Run("replaceable file service", func(t *testing.T) {
		testReplaceableFileService(t, func() ReplaceableFileService {
			fs, err := NewMemoryFS("memory", DisabledCacheConfig, nil)
			assert.Nil(t, err)
			return fs
		})
	})

}

func TestMemoryFSListAllowsMutation(t *testing.T) {
	ctx := context.Background()
	fs, err := NewMemoryFS("memory", DisabledCacheConfig, nil)
	require.NoError(t, err)
	defer fs.Close(ctx)
	write := func(name string) {
		t.Helper()
		require.NoError(t, fs.Write(ctx, IOVector{FilePath: "dir/" + name, Entries: []IOEntry{{Size: 1, Data: []byte("x")}}}))
	}
	write("a")
	write("b")
	var names []string
	for entry, err := range fs.List(ctx, "dir") {
		require.NoError(t, err)
		names = append(names, entry.Name)
		if len(names) == 1 {
			require.NoError(t, fs.Delete(ctx, "dir/a", "dir/b"))
			write("c")
		}
	}
	require.Equal(t, []string{"a", "b"}, names)
	entries, err := SortedList(fs.List(ctx, "dir"))
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, "c", entries[0].Name)
	for _, err := range fs.List(ctx, "dir") {
		require.NoError(t, err)
		break
	}
	require.NoError(t, fs.Delete(ctx, "dir/c"), "breaking iteration must release its snapshot")
}

func BenchmarkMemoryFS(b *testing.B) {
	benchmarkFileService(context.Background(), b, func() FileService {
		fs, err := NewMemoryFS("memory", DisabledCacheConfig, nil)
		assert.Nil(b, err)
		return fs
	})
}

func BenchmarkMemoryFSWithMemoryCache(b *testing.B) {
	ctx := context.Background()
	var counterSet perfcounter.CounterSet
	ctx = perfcounter.WithCounterSet(ctx, &counterSet)

	cache := NewMemCache(
		fscache.ConstCapacity(128*1024*1024),
		nil,
		nil,
		"",
	)
	defer cache.Close(ctx)

	benchmarkFileService(ctx, b, func() FileService {
		fs, err := NewMemoryFS("memory", DisabledCacheConfig, nil)
		assert.Nil(b, err)
		fs.caches = append(fs.caches, cache)
		return fs
	})

	read := counterSet.FileService.Cache.Memory.Read.Load()
	hit := counterSet.FileService.Cache.Memory.Hit.Load()
	fmt.Printf("hit rate: %v / %v = %.4v\n", hit, read, float64(hit)/float64(read))
}

func BenchmarkMemoryFSWithMemoryCacheLowCapacity(b *testing.B) {
	ctx := context.Background()
	var counterSet perfcounter.CounterSet
	ctx = perfcounter.WithCounterSet(ctx, &counterSet)

	cache := NewMemCache(
		fscache.ConstCapacity(2*1024*1024), nil, nil,
		"",
	)
	defer cache.Close(ctx)

	benchmarkFileService(ctx, b, func() FileService {
		fs, err := NewMemoryFS("memory", DisabledCacheConfig, nil)
		assert.Nil(b, err)
		fs.caches = append(fs.caches, cache)
		return fs
	})

	read := counterSet.FileService.Cache.Memory.Read.Load()
	hit := counterSet.FileService.Cache.Memory.Hit.Load()
	fmt.Printf("hit rate: %v / %v = %.4v\n", hit, read, float64(hit)/float64(read))
}
