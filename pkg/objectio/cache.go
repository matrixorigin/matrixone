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

	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/fileservice/lrucache"
	"github.com/matrixorigin/matrixone/pkg/util/toml"
)

type CacheConfig struct {
	MemoryCapacity toml.ByteSize `toml:"memory-capacity"`
}

var metaCache *lrucache.LRU[ObjectNameShort, fileservice.Bytes]
var onceInit sync.Once
var metaCacheHitStats hitStats

func init() {
	metaCache = lrucache.New[ObjectNameShort, fileservice.Bytes](512*1024*1024, nil, nil)
}

func InitMetaCache(size int64) {
	onceInit.Do(func() {
		metaCache = lrucache.New[ObjectNameShort, fileservice.Bytes](size, nil, nil)
	})
}

func ExportMetaCacheHitWindow() (int64, int64) {
	return metaCacheHitStats.ExportW()
}

func ExportMetaCacheHitTotal() (int64, int64) {
	return metaCacheHitStats.Export()
}

func LoadObjectMetaByExtent(
	ctx context.Context,
	name *ObjectName,
	extent *Extent,
	noLRUCache bool,
	fs fileservice.FileService,
) (meta ObjectMeta, err error) {
	v, ok := metaCache.Get(ctx, *name.Short(), false)
	if ok {
		var obj any
		obj, err = Decode(v)
		meta = obj.(ObjectMeta)
		metaCacheHitStats.Record(1, 1)
		if err != nil {
			return
		}
		return
	}
	if v, err = ReadExtent(ctx, name.String(), extent, noLRUCache, fs, constructorFactory); err != nil {
		return
	}
	var obj any
	obj, err = Decode(v)
	if err != nil {
		return
	}
	meta = obj.(ObjectMeta)
	metaCache.Set(ctx, *name.Short(), v[:], false)
	metaCacheHitStats.Record(0, 1)
	return
}

func FastLoadObjectMeta(ctx context.Context, location *Location, fs fileservice.FileService) (ObjectMeta, error) {
	extent := location.Extent()
	name := location.Name()
	return LoadObjectMetaByExtent(ctx, &name, &extent, true, fs)
}
