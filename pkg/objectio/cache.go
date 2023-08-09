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
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/fileservice/lrucache"
	"github.com/matrixorigin/matrixone/pkg/util/toml"
	"sync"
)

type CacheConfig struct {
	MemoryCapacity toml.ByteSize `toml:"memory-capacity"`
}

var metaCache *lrucache.LRU[ObjectNameShort, fileservice.Bytes]
var onceInit sync.Once

func init() {
	metaCache = lrucache.New[ObjectNameShort, fileservice.Bytes](512*1024*1024, nil, nil)
}

func InitMetaCache(size int64) {
	onceInit.Do(func() {
		metaCache = lrucache.New[ObjectNameShort, fileservice.Bytes](size, nil, nil)
	})
}

func LoadObjectMetaByExtent(
	ctx context.Context,
	name *ObjectName,
	extent *Extent,
	noLRUCache bool,
	fs fileservice.FileService) (meta ObjectMeta, err error) {
	v, ok := metaCache.Get(ctx, *name.Short(), false)
	if ok {
		meta = ObjectMeta(v)
		return
	}
	if meta, err = ReadObjectMeta(ctx, name.String(), extent, noLRUCache, fs); err != nil {
		return
	}
	metaCache.Set(ctx, *name.Short(), fileservice.Bytes(meta), false)
	return
}

func FastLoadObjectMeta(ctx context.Context, location *Location, fs fileservice.FileService) (ObjectMeta, error) {
	extent := location.Extent()
	name := location.Name()
	return LoadObjectMetaByExtent(ctx, &name, &extent, true, fs)
}
