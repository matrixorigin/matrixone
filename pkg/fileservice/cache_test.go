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
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
)

func Test_readCache(t *testing.T) {
	ctx := context.Background()

	slowCacheReadThreshold = time.Second

	size := int64(128)
	m := NewMemCache(fscache.ConstCapacity(size), nil, nil, "")
	defer m.Close(ctx)

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*3)
	defer cancel()

	newReadVec := func() *IOVector {
		vec := &IOVector{
			FilePath: "foo",
			Entries: []IOEntry{
				{
					Size: 3,
					ToCacheData: func(ctx context.Context, reader io.Reader, data []byte, allocator CacheDataAllocator) (fscache.Data, error) {
						cacheData := allocator.AllocateCacheData(ctx, 1)
						cacheData.Bytes()[0] = 42
						return cacheData, nil
					},
				},
			},
		}
		return vec
	}

	err := readCache(ctx, m, newReadVec())
	assert.NoError(t, err)

	ctx, cancel = context.WithTimeoutCause(context.Background(), 0, moerr.NewInternalErrorNoCtx("ut tester"))
	defer cancel()

	newReadVec2 := func() *IOVector {
		vec := &IOVector{
			FilePath: "etl:\u00ad",
			Entries: []IOEntry{
				{
					Size: 3,
					ToCacheData: func(ctx context.Context, reader io.Reader, data []byte, allocator CacheDataAllocator) (fscache.Data, error) {
						return nil, context.DeadlineExceeded
					},
				},
			},
		}
		return vec
	}

	err = readCache(ctx, m, newReadVec2())
	assert.Error(t, err)
}

var _ IOVectorCache = (*testCache)(nil)

type testCache struct {
}

func (cache *testCache) Read(ctx context.Context, vector *IOVector) error {
	return context.DeadlineExceeded
}

func (cache *testCache) Update(ctx context.Context, vector *IOVector, async bool) error {
	//TODO implement me
	panic("implement me")
}

func (cache *testCache) Flush(ctx context.Context) {
	//TODO implement me
	panic("implement me")
}

func (cache *testCache) DeletePaths(ctx context.Context, paths []string) error {
	//TODO implement me
	panic("implement me")
}

func (cache *testCache) Evict(ctx context.Context, done chan int64) {
	//TODO implement me
	panic("implement me")
}

func (cache *testCache) Close(ctx context.Context) {}

func Test_readCache2(t *testing.T) {
	ctx := context.Background()

	slowCacheReadThreshold = time.Second

	m := &testCache{}
	defer m.Close(ctx)

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*3)
	defer cancel()

	newReadVec := func() *IOVector {
		vec := &IOVector{
			FilePath: "foo",
			Entries: []IOEntry{
				{
					Size: 3,
					ToCacheData: func(ctx context.Context, reader io.Reader, data []byte, allocator CacheDataAllocator) (fscache.Data, error) {
						cacheData := allocator.AllocateCacheData(ctx, 1)
						cacheData.Bytes()[0] = 42
						return cacheData, nil
					},
				},
			},
		}
		return vec
	}

	err := readCache(ctx, m, newReadVec())
	assert.NoError(t, err)
}
