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

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/fileservice/lrucache"
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

type mataCacheKey [cacheKeyLen]byte

var metaCache *lrucache.LRU[mataCacheKey, fileservice.Bytes]
var onceInit sync.Once
var metaCacheStats hitStats
var metaCacheHitStats hitStats

func init() {
	metaCache = lrucache.New[mataCacheKey, fileservice.Bytes](512*1024*1024, nil, nil, nil)
}

func InitMetaCache(size int64) {
	onceInit.Do(func() {
		metaCache = lrucache.New[mataCacheKey, fileservice.Bytes](size, nil, nil, nil)
	})
}

func encodeCacheKey(name ObjectNameShort, cacheKeyType uint16) mataCacheKey {
	var key mataCacheKey
	copy(key[:], name[:])
	copy(key[ObjectNameShortLen:], types.EncodeUint16(&cacheKeyType))
	return key
}

func ExportMetaCacheStats() string {
	var buf bytes.Buffer
	hw, hwt := metaCacheHitStats.ExportW()
	ht, htt := metaCacheHitStats.Export()
	w, wt := metaCacheStats.ExportW()
	t, tt := metaCacheStats.Export()
	fmt.Fprintf(
		&buf,
		"MetaCacheWindow: %d/%d | %d/%d, MetaCacheTotal: %d/%d | %d/%d",
		hw, hwt, w, wt, ht, htt, t, tt,
	)
	return buf.String()
}

func LoadObjectMetaByExtent(
	ctx context.Context,
	name *ObjectName,
	extent *Extent,
	prefetch bool,
	cachePolicy fileservice.CachePolicy,
	fs fileservice.FileService,
) (meta ObjectMeta, err error) {
	key := encodeCacheKey(*name.Short(), cacheKeyTypeMeta)
	v, ok := metaCache.Get(ctx, key, false)
	if ok {
		var obj any
		obj, err = Decode(v)
		if err != nil {
			return
		}
		meta = obj.(ObjectMeta)
		metaCacheStats.Record(1, 1)
		if !prefetch {
			metaCacheHitStats.Record(1, 1)
		}
		return
	}
	if v, err = ReadExtent(ctx, name.String(), extent, cachePolicy, fs, constructorFactory); err != nil {
		return
	}
	var obj any
	obj, err = Decode(v)
	if err != nil {
		return
	}
	meta = obj.(ObjectMeta)
	metaCache.Set(ctx, key, v[:], false)
	metaCacheStats.Record(0, 1)
	if !prefetch {
		metaCacheHitStats.Record(0, 1)
	}
	return
}

func LoadBFWithMeta(
	ctx context.Context,
	meta ObjectDataMeta,
	location Location,
	fs fileservice.FileService,
) (BloomFilter, error) {
	key := encodeCacheKey(*location.ShortName(), cacheKeyTypeBloomFilter)
	v, ok := metaCache.Get(ctx, key, false)
	if ok {
		metaCacheStats.Record(1, 1)
		return v.Bytes(), nil
	}
	extent := meta.BlockHeader().BFExtent()
	bf, err := ReadBloomFilter(ctx, location.Name().String(), &extent, fileservice.SkipMemory, fs)
	if err != nil {
		return nil, err
	}
	metaCache.Set(ctx, key, fileservice.Bytes(bf), false)
	metaCacheStats.Record(0, 1)
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
	return LoadObjectMetaByExtent(ctx, &name, &extent, prefetch, fileservice.SkipMemory, fs)
}
