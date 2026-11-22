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

func BenchmarkMetadataCacheRead(b *testing.B) {
	for _, tc := range []struct {
		name    string
		shared  bool
		withTry bool
	}{
		{name: "default_get"},
		{name: "metadata_get", shared: true},
		{name: "metadata_get_and_try", shared: true, withTry: true},
	} {
		b.Run(tc.name, func(b *testing.B) {
			opts := Options[int, intValue]{Capacity: func() int64 { return 100 }, Hash: hashInt}
			var c *Cache[int, intValue]
			if tc.shared {
				c = NewWithConcurrentRetain(opts)
			} else {
				c = New(opts)
			}
			if inserted, _ := c.Set(1, 1, 1); !inserted {
				b.Fatal("failed to seed the cache")
			}
			b.Cleanup(func() { c.Delete(1) })
			var hits, failures atomic.Uint64
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				var localHits, localFailures uint64
				for pb.Next() {
					value, _, ok, err := c.Get(1, nil)
					if err != nil || !ok || value != 1 {
						localFailures++
						continue
					}
					value.Release()
					if tc.withTry {
						value, _, ok, err = c.TryGet(1, nil)
						if err != nil {
							localFailures++
						} else if ok {
							localHits++
							value.Release()
						}
					} else {
						localHits++
					}
				}
				hits.Add(localHits)
				failures.Add(localFailures)
			})
			b.StopTimer()
			if failures.Load() != 0 {
				b.Fatalf("%d unexpected cache errors", failures.Load())
			}
			b.ReportMetric(float64(hits.Load())/float64(b.N), "hits/op")
		})
	}
}

func BenchmarkMetadataSecondChance(b *testing.B) {
	for _, shared := range []bool{false, true} {
		name := "default"
		if shared {
			name = "metadata"
		}
		b.Run(name, func(b *testing.B) {
			opts := Options[int, intValue]{Capacity: func() int64 { return 100 }, Hash: hashInt}
			var c *Cache[int, intValue]
			if shared {
				c = NewWithConcurrentRetain(opts)
			} else {
				c = New(opts)
			}
			if inserted, _ := c.Set(1, 1, 1); !inserted {
				b.Fatal("failed to seed the cache")
			}
			b.Cleanup(func() { c.Delete(1) })
			for range 2 {
				value, _, ok, err := c.Get(1, nil)
				if err != nil || !ok {
					b.Fatal("failed to warm the cache")
				}
				value.Release()
			}
			ps := c.policyOf(hashInt(1))
			if _, evicted := c.step(ps, true, ps.small.head.Load()); evicted {
				b.Fatal("failed to promote the entry")
			}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				value, _, ok, err := c.Get(1, nil)
				if err != nil || !ok {
					b.Fatal("cache hit failed")
				}
				value.Release()
				if _, evicted := c.step(ps, false, ps.main.head.Load()); evicted {
					b.Fatal("hot entry was evicted")
				}
			}
		})
	}
}
