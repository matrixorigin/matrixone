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

package s3fifo

import (
	"context"
	"math/rand"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
)

type benchData struct {
	b    []byte
	refs atomic.Int32
}

func (d *benchData) Size() int64              { return int64(len(d.b)) }
func (d *benchData) Capacity() int64          { return int64(cap(d.b)) }
func (d *benchData) Bytes() []byte            { return d.b }
func (d *benchData) Slice(n int) fscache.Data { d.b = d.b[:n]; return d }
func (d *benchData) Retain()                  { d.refs.Add(1) }
func (d *benchData) Release()                 { d.refs.Add(-1) }
func (d *benchData) String() string           { return strconv.Itoa(len(d.b)) }
func benchKey(i int) fscache.CacheKey {
	return fscache.CacheKey{Path: "obj" + strconv.Itoa(i>>6), Offset: int64(i), Sz: 4096}
}
func newBenchData(size int) *benchData {
	d := &benchData{b: make([]byte, size)}
	d.refs.Store(1)
	return d
}
func benchCapacity(keys, size int) func() int64 {
	return func() int64 { return int64(keys * size / 2) }
}
func runBench(b *testing.B, c fscache.DataCache, getPercent int) {
	const keys, size = 1 << 16, 4096
	values := make([]*benchData, keys)
	cacheKeys := make([]fscache.CacheKey, keys)
	for i := range values {
		values[i] = newBenchData(size)
		cacheKeys[i] = benchKey(i)
	}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		r := rand.New(rand.NewSource(rand.Int63()))
		for pb.Next() {
			i := int(r.ExpFloat64()*keys/8) % keys
			if r.Intn(100) < getPercent {
				if d, ok := c.Get(ctx, cacheKeys[i]); ok {
					d.Release()
					continue
				}
			}
			_, _ = c.Set(ctx, cacheKeys[i], values[i])
		}
	})
}

func newDataCache(keys, size int) fscache.DataCache {
	return NewDataCache(DataOptions{Capacity: benchCapacity(keys, size)})
}

func BenchmarkDataCache(b *testing.B) {
	for _, mix := range []int{95, 50} {
		b.Run("get"+strconv.Itoa(mix), func(b *testing.B) {
			runBench(b, newDataCache(1<<16, 4096), mix)
		})
	}
}
