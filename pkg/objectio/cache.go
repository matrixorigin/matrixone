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
	if v, err = ReadExtent(ctx, name.String(), extent, policy, fs, constructorFactory); err != nil {
		return
	}
	meta = MustObjectMeta(v)
	metaCache.Set(ctx, key, v[:], int64(len(v)))
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
	bf, err := ReadBloomFilter(ctx, location.Name().String(), &extent, fileservice.SkipFullFilePreloads, fs)
	if err != nil {
		return nil, err
	}
	metaCache.Set(ctx, key, bf, int64(len(bf)))
	return bf, nil
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
