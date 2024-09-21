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
	"io"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/stretchr/testify/assert"
)

func testCachingFileService(
	t *testing.T,
	newFS func() CachingFileService,
) {

	fs := newFS()
	fs.SetAsyncUpdate(false)
	ctx := context.Background()
	var counterSet perfcounter.CounterSet
	ctx = perfcounter.WithCounterSet(ctx, &counterSet)

	m := api.Int64Map{
		M: map[int64]int64{42: 42},
	}
	data, err := m.Marshal()
	assert.Nil(t, err)

	err = fs.Write(ctx, IOVector{
		FilePath: "foo",
		Entries: []IOEntry{
			{
				Size: int64(len(data)),
				Data: data,
			},
		},
		Policy: SkipAllCache,
	})
	assert.Nil(t, err)

	makeVec := func() *IOVector {
		return &IOVector{
			FilePath: "foo",
			Entries: []IOEntry{
				{
					Size: int64(len(data)),
					ToCacheData: func(r io.Reader, data []byte, allocator CacheDataAllocator) (fscache.Data, error) {
						bs, err := io.ReadAll(r)
						assert.Nil(t, err)
						if len(data) > 0 {
							assert.Equal(t, bs, data)
						}
						cacheData := allocator.AllocateCacheData(len(bs))
						copy(cacheData.Bytes(), bs)
						return cacheData, nil
					},
				},
			},
		}
	}

	// nocache
	vec := makeVec()
	vec.Policy = SkipAllCache
	err = fs.Read(ctx, vec)
	assert.Nil(t, err)

	err = m.Unmarshal(vec.Entries[0].CachedData.Bytes())
	assert.NoError(t, err)
	assert.Equal(t, 1, len(m.M))
	assert.Equal(t, int64(42), m.M[42])
	assert.Equal(t, int64(0), counterSet.FileService.Cache.Read.Load())
	assert.Equal(t, int64(0), counterSet.FileService.Cache.Hit.Load())

	vec.Release()

	// read, not hit
	vec = makeVec()
	err = fs.Read(ctx, vec)
	assert.Nil(t, err)
	err = m.Unmarshal(vec.Entries[0].CachedData.Bytes())
	assert.NoError(t, err)
	assert.Equal(t, 1, len(m.M))
	assert.Equal(t, int64(42), m.M[42])
	assert.True(t, counterSet.FileService.Cache.Memory.Read.Load() == 1 ||
		counterSet.FileService.Cache.Disk.Read.Load() == 1)
	assert.Equal(t, int64(0), counterSet.FileService.Cache.Hit.Load())

	vec.Release()

	// read again, hit cache
	vec = makeVec()
	err = fs.Read(ctx, vec)
	assert.Nil(t, err)
	err = m.Unmarshal(vec.Entries[0].CachedData.Bytes())
	assert.NoError(t, err)
	assert.Equal(t, 1, len(m.M))
	assert.Equal(t, int64(42), m.M[42])
	assert.True(t, counterSet.FileService.Cache.Memory.Read.Load() == 2 ||
		counterSet.FileService.Cache.Disk.Read.Load() == 2)
	assert.True(t, counterSet.FileService.Cache.Memory.Hit.Load() == 1 ||
		counterSet.FileService.Cache.Disk.Hit.Load() == 1)

	vec.Release()

	// flush
	fs.FlushCache()

}
