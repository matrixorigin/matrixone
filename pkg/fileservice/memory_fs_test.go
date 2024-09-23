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

	benchmarkFileService(ctx, b, func() FileService {
		fs, err := NewMemoryFS("memory", DisabledCacheConfig, nil)
		assert.Nil(b, err)
		fs.caches = append(fs.caches, NewMemCache(
			fscache.ConstCapacity(128*1024*1024),
			nil,
			nil,
			"",
		))
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

	benchmarkFileService(ctx, b, func() FileService {
		fs, err := NewMemoryFS("memory", DisabledCacheConfig, nil)
		assert.Nil(b, err)
		fs.caches = append(fs.caches, NewMemCache(
			fscache.ConstCapacity(2*1024*1024), nil, nil,
			"",
		))
		return fs
	})

	read := counterSet.FileService.Cache.Memory.Read.Load()
	hit := counterSet.FileService.Cache.Memory.Hit.Load()
	fmt.Printf("hit rate: %v / %v = %.4v\n", hit, read, float64(hit)/float64(read))
}
