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
	"hash/maphash"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
)

// DataValue is a cached fscache.Data with the logical length it had when it was set.
type DataValue struct {
	Data        fscache.Data
	LogicalSize int64
}

func (v DataValue) Retain()  { v.Data.Retain() }
func (v DataValue) Release() { v.Data.Release() }

// DataOptions configures a DataCache. Only Capacity is required.
type DataOptions struct {
	Capacity  fscache.CapacityFunc
	Admission func(capacity int64) (target int64, ok bool)
	// ChargeGuard is held while the bytes of data reserved through fscache.DataCacheReservation
	// are charged and the reservation is committed.
	ChargeGuard sync.Locker
	OnInsert    func(key fscache.CacheKey, value DataValue, size int64, seq uint64)
	OnRemove    func(key fscache.CacheKey, value DataValue, size int64, seq uint64, reason Reason)
}

// DataCache is an fscache.DataCache over a Cache. Its size of an entry is the data's backing
// capacity.
type DataCache struct {
	c *Cache[fscache.CacheKey, DataValue]
}

var _ fscache.DataCacheWithPinAdmission = (*DataCache)(nil)

var keySeed = maphash.MakeSeed()

func hashKey(key fscache.CacheKey) uint64 {
	return maphash.Comparable(keySeed, key)
}

// NewDataCache returns a DataCache configured by opts.
func NewDataCache(opts DataOptions) *DataCache {
	o := Options[fscache.CacheKey, DataValue]{
		Capacity:  opts.Capacity,
		Hash:      hashKey,
		Group:     func(key fscache.CacheKey) string { return key.Path },
		Admission: opts.Admission,
		OnInsert:  opts.OnInsert,
		OnRemove:  opts.OnRemove,
	}
	if opts.ChargeGuard != nil {
		o.ChargeGuard = opts.ChargeGuard
		o.Guarded = func(v DataValue) bool {
			switch r := v.Data.(type) {
			case fscache.DataCachePendingReservation:
				return r.CacheReservationPending()
			case fscache.DataCacheReservation:
				return true
			}
			return false
		}
		o.OnCharge = func(v DataValue) {
			if r, ok := v.Data.(fscache.DataCacheReservation); ok {
				r.CommitCacheReservation()
			}
		}
	}
	return &DataCache{c: New(o)}
}

func (d *DataCache) Capacity() int64 { return d.c.Capacity() }

func (d *DataCache) Used() int64 { return d.c.Used() }

func (d *DataCache) Available() int64 { return max(d.c.Capacity()-d.c.Used(), 0) }

// CurrentSeq returns the sequence number of key's cached data.
func (d *DataCache) CurrentSeq(key fscache.CacheKey) (uint64, bool) { return d.c.CurrentSeq(key) }

func (d *DataCache) Get(_ context.Context, key fscache.CacheKey) (fscache.Data, bool) {
	v, _, ok, _ := d.c.Get(key, nil)
	return v.Data, ok
}

func (d *DataCache) GetWithPinAdmission(_ context.Context, key fscache.CacheKey, admit fscache.DataCachePinAdmission) (fscache.Data, func(), bool, error) {
	var a func(size int64) (func(), error)
	if admit != nil {
		a = func(size int64) (func(), error) { return admit(size) }
	}
	v, release, ok, err := d.c.Get(key, a)
	return v.Data, release, ok, err
}

// Set caches data, retaining it; the caller keeps its own reference.
func (d *DataCache) Set(_ context.Context, key fscache.CacheKey, data fscache.Data) (bool, error) {
	logicalSize, size := data.Size(), data.Capacity()
	if logicalSize < 0 || size < logicalSize {
		panic("cache data reports an invalid logical size or backing capacity")
	}
	inserted, rejected := d.c.Set(key, DataValue{Data: data, LogicalSize: logicalSize}, size)
	if rejected {
		return false, fscache.ErrCacheAdmissionRejected
	}
	return inserted, nil
}

func (d *DataCache) DeletePaths(_ context.Context, paths []string) {
	for _, path := range paths {
		d.c.DeleteGroup(path)
	}
}

func (d *DataCache) EnsureNBytes(ctx context.Context, want int) {
	d.c.EvictTo(ctx, d.c.Capacity()-int64(want))
}

func (d *DataCache) Flush(ctx context.Context) { d.c.EvictTo(ctx, 0) }

// Evict evicts to the capacity and, when done is not nil, sends it the capacity.
func (d *DataCache) Evict(ctx context.Context, done chan int64) {
	target := d.c.Capacity()
	d.c.EvictTo(ctx, target)
	if done != nil {
		done <- target
	}
}

func (d *DataCache) EvictToTargetWithWait(ctx context.Context, target int64) int64 {
	return d.c.EvictTo(ctx, min(target, d.c.Capacity()))
}
