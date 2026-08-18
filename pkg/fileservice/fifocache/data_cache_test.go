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

package fifocache

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
)

func BenchmarkEnsureNBytesAndSet(b *testing.B) {
	ctx := context.Background()
	cache := NewDataCache(
		fscache.ConstCapacity(1024),
		nil, nil, nil,
	)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			cache.EnsureNBytes(ctx, 1)
		}
	})
}

func TestShardCacheKey(t *testing.T) {
	var shards [numShards]int
	for offset := range int64(128) {
		for size := range int64(128) {
			sum := shardCacheKey(fscache.CacheKey{
				Path:   fmt.Sprintf("%v%v", offset, size),
				Offset: offset,
				Sz:     size,
			})
			shards[sum%numShards]++
		}
	}
	for _, n := range shards {
		if n == 0 {
			t.Fatal("not good")
		}
	}
}

func TestShardCacheKeyAllocs(t *testing.T) {
	key := fscache.CacheKey{
		Sz:     42,
		Offset: 3,
		Path:   strings.Repeat("abc", 42),
	}
	if n := testing.AllocsPerRun(64, func() {
		shardCacheKey(key)
	}); n != 0 {
		t.Fatalf("should not allocate")
	}
}

func BenchmarkDataCacheSet(b *testing.B) {
	cache := NewDataCache(
		fscache.ConstCapacity(1024),
		nil, nil, nil,
	)
	b.ResetTimer()
	for i := range b.N {
		cache.Set(
			context.Background(),
			fscache.CacheKey{
				Path:   "foo" + strconv.Itoa(i),
				Sz:     int64(i % 65536),
				Offset: int64(i),
			},
			testBytes([]byte("foo")),
		)
	}
}

func BenchmarkDataCacheGet(b *testing.B) {
	cache := NewDataCache(
		fscache.ConstCapacity(1024),
		nil, nil, nil,
	)
	key := fscache.CacheKey{
		Path:   "foo",
		Sz:     42,
		Offset: 42,
	}
	cache.Set(
		context.Background(),
		key,
		testBytes([]byte("foo")),
	)
	b.ResetTimer()
	for range b.N {
		cache.Get(context.Background(), key)
	}
}

type testBytes []byte

var _ fscache.Data = testBytes{}

func (t testBytes) Bytes() []byte {
	return t
}

func (t testBytes) Size() int64 {
	return int64(len(t))
}

func (t testBytes) Capacity() int64 {
	return int64(cap(t))
}

func (t testBytes) Release() {
}

func (t testBytes) Retain() {
}

func (t testBytes) Slice(length int) fscache.Data {
	return t[:length]
}

type sizedTestData struct {
	bytesCalls int
}

func (s *sizedTestData) Bytes() []byte {
	s.bytesCalls++
	return []byte("foo")
}

func (*sizedTestData) Size() int64              { return 3 }
func (*sizedTestData) Capacity() int64          { return 7 }
func (*sizedTestData) Release()                 {}
func (*sizedTestData) Retain()                  {}
func (s *sizedTestData) Slice(int) fscache.Data { return s }

func TestDataCacheSetUsesCapacityWithoutExposingBytes(t *testing.T) {
	cache := NewDataCache(fscache.ConstCapacity(1024), nil, nil, nil)
	data := new(sizedTestData)
	_, err := cache.Set(context.Background(), fscache.CacheKey{Path: "foo", Sz: 3}, data)
	if err != nil {
		t.Fatal(err)
	}
	if data.bytesCalls != 0 {
		t.Fatalf("Bytes called %d times", data.bytesCalls)
	}
	if got := cache.Used(); got != 7 {
		t.Fatalf("cache used bytes = %d, want physical capacity 7", got)
	}
}

func TestDataCacheCallbacksReceiveCapturedLogicalSize(t *testing.T) {
	type callbackSizes struct {
		logical int64
		backing int64
	}

	var postSet, postEvict callbackSizes
	cache := NewDataCache(
		fscache.ConstCapacity(8),
		func(_ context.Context, _ fscache.CacheKey, _ fscache.Data, logicalSize, size int64, _ uint64) {
			postSet = callbackSizes{logical: logicalSize, backing: size}
		},
		nil,
		func(_ context.Context, _ fscache.CacheKey, _ fscache.Data, logicalSize, size int64, _ uint64) {
			postEvict = callbackSizes{logical: logicalSize, backing: size}
		},
	)
	key := fscache.CacheKey{Path: "foo", Sz: 3}
	if _, err := cache.Set(context.Background(), key, testBytes(make([]byte, 3, 8))); err != nil {
		t.Fatal(err)
	}
	cache.DeletePaths(context.Background(), []string{"foo"})

	want := callbackSizes{logical: 3, backing: 8}
	if postSet != want {
		t.Fatalf("post-set sizes = %+v, want %+v", postSet, want)
	}
	if postEvict != want {
		t.Fatalf("post-evict sizes = %+v, want %+v", postEvict, want)
	}
}
