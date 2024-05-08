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
	"io"
	"runtime"
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/fileservice/memorycache"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/stretchr/testify/assert"
)

func TestMemCacheLeak(t *testing.T) {
	ctx := context.Background()
	var counter perfcounter.CounterSet
	ctx = perfcounter.WithCounterSet(ctx, &counter)

	fs, err := NewMemoryFS("test", DisabledCacheConfig, nil)
	assert.Nil(t, err)
	err = fs.Write(ctx, IOVector{
		FilePath: "foo",
		Entries: []IOEntry{
			{
				Size: 3,
				Data: []byte("foo"),
			},
		},
	})
	assert.Nil(t, err)

	size := int64(4 * runtime.GOMAXPROCS(0))
	m := NewMemCache(NewMemoryCache(size, true, nil), nil)

	vec := &IOVector{
		FilePath: "foo",
		Entries: []IOEntry{
			{
				Size: 3,
				ToCacheData: func(reader io.Reader, data []byte, allocator CacheDataAllocator) (memorycache.CacheData, error) {
					cacheData := allocator.Alloc(1)
					cacheData.Bytes()[0] = 42
					return cacheData, nil
				},
			},
		},
	}
	err = m.Read(ctx, vec)
	assert.Nil(t, err)
	vec.Release()
	err = fs.Read(ctx, vec)
	assert.Nil(t, err)
	err = m.Update(ctx, vec, false)
	assert.Nil(t, err)
	vec.Release()
	assert.Equal(t, int64(1), m.cache.Capacity()-m.cache.Available())
	assert.Equal(t, int64(size), counter.FileService.Cache.Memory.Available.Load())
	assert.Equal(t, int64(0), counter.FileService.Cache.Memory.Used.Load())

	// read from cache
	vec = &IOVector{
		FilePath: "foo",
		Entries: []IOEntry{
			{
				Size: 3,
				ToCacheData: func(reader io.Reader, data []byte, allocator CacheDataAllocator) (memorycache.CacheData, error) {
					cacheData := allocator.Alloc(1)
					cacheData.Bytes()[0] = 42
					return cacheData, nil
				},
			},
		},
	}
	err = m.Read(ctx, vec)
	assert.Nil(t, err)
	vec.Release()
	err = fs.Read(ctx, vec)
	assert.Nil(t, err)
	err = m.Update(ctx, vec, false)
	assert.Nil(t, err)
	vec.Release()
	assert.Equal(t, int64(1), m.cache.Capacity()-m.cache.Available())
	assert.Equal(t, int64(size)-1, counter.FileService.Cache.Memory.Available.Load())
	assert.Equal(t, int64(1), counter.FileService.Cache.Memory.Used.Load())

}

// TestHighConcurrency this test is to mainly test concurrency issue in objectCache
// and dataOverlap-checker.
func TestHighConcurrency(t *testing.T) {
	m := NewMemCache(NewMemoryCache(2, true, nil), nil)
	ctx := context.Background()

	n := 10

	var vecArr []*IOVector
	for i := 0; i < n; i++ {
		vecArr = append(vecArr,
			&IOVector{
				FilePath: fmt.Sprintf("key%d", i),
				Entries: []IOEntry{
					{
						Size: 4,
						Data: []byte(fmt.Sprintf("val%d", i)),
					},
				},
			},
		)
	}

	// n go-routines are spun.
	wg := sync.WaitGroup{}
	wg.Add(n)

	for i := 0; i < n; i++ {
		// each go-routine tries to insert vecArr[i] for 10 times
		// these updates should invoke postSet and postEvict inside objectCache.
		// Since postSet and postEvict are guarded by objectCache mutex, this test
		// should not throw concurrency related panics.
		go func(idx int) {
			for j := 0; j < 10; j++ {
				_ = m.Update(ctx, vecArr[idx], false)
			}
			wg.Done()
		}(i)
	}
	wg.Wait()
}
