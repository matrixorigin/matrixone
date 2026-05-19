// Copyright 2021 Matrix Origin
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

package objectio

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	metric "github.com/matrixorigin/matrixone/pkg/util/metric/v2"

	"go.uber.org/zap"

	"github.com/cespare/xxhash/v2"
	"github.com/shirou/gopsutil/v3/mem"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/fileservice/fifocache"
	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/util/toml"
)

const (
	cacheKeyTypeLen = 2
	cacheKeyLen     = ObjectNameShortLen + cacheKeyTypeLen
)

const (
	cacheKeyTypeMeta uint16 = iota
	cacheKeyTypeBloomFilter
)

type CacheConfig struct {
	MemoryCapacity toml.ByteSize `toml:"memory-capacity"`
}

// BlockReadStats collect blk read related cache statistics,
// include mem and disk
type BlockReadStats struct {
	// using this we can collect the number of blks have read and hit among them
	BlkCacheHitStats hitStats
	// using this we can collect the number of entries have read and hit among them
	EntryCacheHitStats hitStats
	// using this we can collect the number of blks each reader will read
	BlksByReaderStats hitStats
	CounterSet        *perfcounter.CounterSet
}

func newBlockReadStats() *BlockReadStats {
	s := BlockReadStats{
		CounterSet: new(perfcounter.CounterSet),
	}
	return &s
}

var BlkReadStats = newBlockReadStats()

type mataCacheKey [cacheKeyLen]byte

var metaCache *fifocache.Cache[mataCacheKey, []byte]
var onceInit sync.Once

// metaLoadGroup deduplicates concurrent loads for the same cache key,
// preventing cache stampede when many goroutines miss the same entry simultaneously.
// Uses mutex+map instead of sync.Map so entries are fully reclaimed after deletion.
var metaLoadMu sync.Mutex
var metaLoadCalls = make(map[mataCacheKey]*loadCall)

type loadCall struct {
	done chan struct{}
	val  []byte
	err  error
}

func metaCacheSize() int64 {
	v, err := mem.VirtualMemory()
	if err != nil {
		panic(err)
	}

	total := v.Total
	if total < 2*mpool.GB {
		return int64(total / 4)
	}
	if total < 16*mpool.GB {
		return 512 * mpool.MB
	}
	if total < 32*mpool.GB {
		return 1 * mpool.GB
	}
	return 2 * mpool.GB
}

func shardMetaCacheKey(key mataCacheKey) uint64 {
	return xxhash.Sum64(key[:])
}

var GlobalCacheCapacityHint atomic.Int64

func cacheCapacityFunc(size int64) fscache.CapacityFunc {
	return func() int64 {
		if n := GlobalCacheCapacityHint.Load(); n > 0 {
			return n
		}
		return size
	}
}

func init() {
	metaCache = newMetaCache(cacheCapacityFunc(metaCacheSize()))
}

func InitMetaCache(size int64) {
	onceInit.Do(func() {
		metaCache = newMetaCache(cacheCapacityFunc(size))
	})
}

func newMetaCache(capacity fscache.CapacityFunc) *fifocache.Cache[mataCacheKey, []byte] {
	inuseBytes, capacityBytes := metric.GetFsCacheBytesGauge("", "meta")
	capacityBytes.Set(float64(capacity()))
	return fifocache.New[mataCacheKey, []byte](
		capacity,
		shardMetaCacheKey,
		func(_ context.Context, _ mataCacheKey, _ []byte, size int64) { // postSet
			inuseBytes.Add(float64(size))
			capacityBytes.Set(float64(capacity()))
		},
		nil,
		func(_ context.Context, _ mataCacheKey, _ []byte, size int64) { // postEvict
			inuseBytes.Add(float64(-size))
			capacityBytes.Set(float64(capacity()))
		})
}

func EvictCache(ctx context.Context) (target int64) {
	ch := make(chan int64, 1)
	metaCache.Evict(ctx, ch, 0)
	target = <-ch
	logutil.Info("metadata cache forced evicted",
		zap.Any("target", target),
	)
	return
}

func encodeCacheKey(name ObjectNameShort, cacheKeyType uint16) mataCacheKey {
	var key mataCacheKey
	copy(key[:], name[:])
	copy(key[ObjectNameShortLen:], types.EncodeUint16(&cacheKeyType))
	return key
}

func LoadObjectMetaByExtent(
	ctx context.Context,
	name *ObjectName,
	extent *Extent,
	prefetch bool,
	policy fileservice.Policy,
	fs fileservice.FileService,
) (meta ObjectMeta, err error) {
	metric.FSReadReadMetaCounter.Add(1)
	key := encodeCacheKey(*name.Short(), cacheKeyTypeMeta)
	v, ok := metaCache.Get(ctx, key)
	if ok {
		metric.FSReadHitMetaCounter.Add(1)
		return MustObjectMeta(v), nil
	}
	if extent.Length() == 0 {
		logutil.Warn("[LoadObjectMetaByExtent]",
			zap.String("name", name.String()),
			zap.String("extent", extent.String()))
	}
	v, err = dedupLoad(ctx, key, func() ([]byte, error) {
		return ReadExtent(ctx, name.UnsafeString(), extent, policy, fs, constructorFactory)
	})
	if err != nil {
		return
	}
	meta = MustObjectMeta(v)
	return
}

func FastLoadBF(
	ctx context.Context,
	location Location,
	isPrefetch bool,
	fs fileservice.FileService,
) (BloomFilter, error) {
	metric.FSReadReadMetaCounter.Add(1)
	key := encodeCacheKey(*location.ShortName(), cacheKeyTypeBloomFilter)
	v, ok := metaCache.Get(ctx, key)
	if ok {
		metric.FSReadHitMetaCounter.Add(1)
		return v, nil
	}
	meta, err := FastLoadObjectMeta(ctx, &location, isPrefetch, fs)
	if err != nil {
		return nil, err
	}
	return LoadBFWithMeta(ctx, meta.MustDataMeta(), location, fs)
}

func LoadBFWithMeta(
	ctx context.Context,
	meta ObjectDataMeta,
	location Location,
	fs fileservice.FileService,
) (BloomFilter, error) {
	metric.FSReadReadMetaCounter.Add(1)
	key := encodeCacheKey(*location.ShortName(), cacheKeyTypeBloomFilter)
	v, ok := metaCache.Get(ctx, key)
	if ok {
		metric.FSReadHitMetaCounter.Add(1)
		return v, nil
	}
	extent := meta.BlockHeader().BFExtent()
	bf, err := dedupLoad(ctx, key, func() ([]byte, error) {
		return ReadBloomFilter(ctx, location.Name().String(), &extent, fileservice.SkipFullFilePreloads, fs)
	})
	if err != nil {
		return nil, err
	}
	return bf, nil
}

// dedupLoad ensures that for a given cache key, only one goroutine performs
// the actual I/O load. Other concurrent callers for the same key wait and
// share the result. This prevents cache stampede under high concurrency.
//
// Uses mutex+map (not sync.Map) so the map shrinks naturally when keys are
// deleted. Waiters read through metaCache after the load finishes so they do
// not keep per-call copies of large metadata buffers.
func dedupLoad(ctx context.Context, key mataCacheKey, load func() ([]byte, error)) ([]byte, error) {
	metaLoadMu.Lock()
	if call, ok := metaLoadCalls[key]; ok {
		metaLoadMu.Unlock()
		select {
		case <-call.done:
			if v, ok := metaCache.Get(ctx, key); ok {
				return v, nil
			}
			if call.err != nil {
				return nil, call.err
			}
			return call.val, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	call := &loadCall{done: make(chan struct{})}
	call.err = moerr.NewInternalErrorNoCtx("dedup load did not complete")
	metaLoadCalls[key] = call
	metaLoadMu.Unlock()

	defer func() {
		metaLoadMu.Lock()
		delete(metaLoadCalls, key)
		metaLoadMu.Unlock()
		close(call.done)
	}()

	call.val, call.err = load()
	if call.err == nil {
		metaCache.Set(ctx, key, call.val, int64(len(call.val)))
	}
	return call.val, call.err
}

func FastLoadObjectMeta(
	ctx context.Context,
	location *Location,
	prefetch bool,
	fs fileservice.FileService,
) (ObjectMeta, error) {
	extent := location.Extent()
	name := location.Name()
	return LoadObjectMetaByExtent(ctx, &name, &extent, prefetch, fileservice.SkipFullFilePreloads, fs)
}
