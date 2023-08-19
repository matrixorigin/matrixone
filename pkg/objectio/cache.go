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
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/fileservice/lrucache"
	"github.com/matrixorigin/matrixone/pkg/util/toml"
)

type CacheConfig struct {
	MemoryCapacity toml.ByteSize `toml:"memory-capacity"`
}

type BlockReadStats struct {
	// the mem cache statistics info for each block
	BlkMemCacheHitStats hitStats
	// the mem cache statistics info for each entry
	EntryMemCacheHitStats hitStats
	BlksByReaderStats     hitStats
	CounterSet            perfcounter.CounterSet
}

var BlkReadStats BlockReadStats

var metaCache *lrucache.LRU[ObjectNameShort, fileservice.Bytes]
var onceInit sync.Once
var metaCacheStats hitStats
var metaCacheHitStats hitStats

func init() {
	metaCache = lrucache.New[ObjectNameShort, fileservice.Bytes](512*1024*1024, nil, nil, nil)
}

func InitMetaCache(size int64) {
	onceInit.Do(func() {
		metaCache = lrucache.New[ObjectNameShort, fileservice.Bytes](size, nil, nil, nil)
	})
}

func ExportCacheStats() string {
	var buf bytes.Buffer
	hw, hwt := metaCacheHitStats.ExportW()
	ht, htt := metaCacheHitStats.Export()
	w, wt := metaCacheStats.ExportW()
	t, tt := metaCacheStats.Export()

	h1, t1 := BlkReadStats.BlkMemCacheHitStats.ExportW()
	h2, t2 := BlkReadStats.EntryMemCacheHitStats.ExportW()
	ratio1 := float32(1)
	if t1 != 0 {
		ratio1 = float32(h1) / float32(t1)
	}
	h3, t3 := BlkReadStats.BlksByReaderStats.ExportW()
	ratio3 := float32(1)
	if t3 != 0 {
		ratio3 = float32(h3) / float32(t3)
	}
	fmt.Fprintf(
		&buf,
		"MetaCacheWindow: %d/%d | %d/%d, MetaCacheTotal: %d/%d | %d/%d, BlkReadStats: (%d/%d=%0.3f vs %d/%d) |||(%d/%d=%0.3f)",
		hw, hwt, w, wt, ht, htt, t, tt,
		h1, t1, ratio1,
		h2, t2,
		h3, t3, ratio3,
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
	v, ok := metaCache.Get(ctx, *name.Short())
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
	metaCache.Set(ctx, *name.Short(), v[:])
	metaCacheStats.Record(0, 1)
	if !prefetch {
		metaCacheHitStats.Record(0, 1)
	}
	return
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
