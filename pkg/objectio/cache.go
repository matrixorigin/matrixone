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
	"bytes"
	"context"
	"fmt"
	"sync"

	"github.com/cespare/xxhash/v2"
	"github.com/shirou/gopsutil/v3/mem"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/fileservice/fifocache"
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
var metaCacheStats hitStats
var metaCacheHitStats hitStats

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

func shardMetaCacheKey(key mataCacheKey) uint8 {
	return uint8(xxhash.Sum64(key[:]))
}

func init() {
	metaCache = fifocache.New[mataCacheKey, []byte](int(metaCacheSize()), nil, shardMetaCacheKey)
}

func InitMetaCache(size int64) {
	onceInit.Do(func() {
		metaCache = fifocache.New[mataCacheKey, []byte](int(size), nil, shardMetaCacheKey)
	})
}

func encodeCacheKey(name ObjectNameShort, cacheKeyType uint16) mataCacheKey {
	var key mataCacheKey
	copy(key[:], name[:])
	copy(key[ObjectNameShortLen:], types.EncodeUint16(&cacheKeyType))
	return key
}

func ExportCacheStats() string {
	var buf bytes.Buffer
	hw, hwt := metaCacheHitStats.ExportW()
	ht, htt := metaCacheHitStats.Export()
	w, wt := metaCacheStats.ExportW()
	t, tt := metaCacheStats.Export()

	fmt.Fprintf(
		&buf,
		"MetaCacheWindow: %d/%d | %d/%d, MetaCacheTotal: %d/%d | %d/%d", hw, hwt, w, wt, ht, htt, t, tt,
	)

	return buf.String()
}

func LoadObjectMetaByExtent(
	ctx context.Context,
	name *ObjectName,
	extent *Extent,
	prefetch bool,
	policy fileservice.Policy,
	fs fileservice.FileService,
) (meta ObjectMeta, err error) {
	key := encodeCacheKey(*name.Short(), cacheKeyTypeMeta)
	v, ok := metaCache.Get(key)
	if ok {
		var obj any
		obj, err = Decode(v)
		if err != nil {
			return
		}
		meta = obj.(ObjectMeta)
		// metaCacheStats.Record(1, 1)
		// if !prefetch {
		// 	metaCacheHitStats.Record(1, 1)
		// }
		return
	}
	if v, err = ReadExtent(ctx, name.String(), extent, policy, fs, constructorFactory); err != nil {
		return
	}
	var obj any
	obj, err = Decode(v)
	if err != nil {
		return
	}
	meta = obj.(ObjectMeta)
	metaCache.Set(key, v[:], len(v))
	// metaCacheStats.Record(0, 1)
	// if !prefetch {
	// 	metaCacheHitStats.Record(0, 1)
	// }
	return
}

func FastLoadBF(
	ctx context.Context,
	location Location,
	isPrefetch bool,
	fs fileservice.FileService,
) (BloomFilter, error) {
	key := encodeCacheKey(*location.ShortName(), cacheKeyTypeBloomFilter)
	v, ok := metaCache.Get(key)
	if ok {
		// metaCacheStats.Record(1, 1)
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
	key := encodeCacheKey(*location.ShortName(), cacheKeyTypeBloomFilter)
	v, ok := metaCache.Get(key)
	if ok {
		// metaCacheStats.Record(1, 1)
		return v, nil
	}
	extent := meta.BlockHeader().BFExtent()
	bf, err := ReadBloomFilter(ctx, location.Name().String(), &extent, fileservice.SkipMemoryCache|fileservice.SkipFullFilePreloads, fs)
	if err != nil {
		return nil, err
	}
	metaCache.Set(key, bf, len(bf))
	// metaCacheStats.Record(0, 1)
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
	var metaReadPolicy fileservice.Policy
	metaReadPolicy = fileservice.SkipMemoryCache
	metaReadPolicy |= fileservice.SkipFullFilePreloads
	return LoadObjectMetaByExtent(ctx, &name, &extent, prefetch, metaReadPolicy, fs)
}
