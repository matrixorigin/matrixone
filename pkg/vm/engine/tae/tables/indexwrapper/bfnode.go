// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package indexwrapper

import (
	"context"

	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/model"
)

type BfReader struct {
	typ        types.T
	indexCache model.LRUCache
	key        objectio.Location
	fs         *objectio.ObjectFS
}

func NewBfReader(
	typ types.T,
	metaLoc objectio.Location,
	indexCache model.LRUCache,
	fs *objectio.ObjectFS,
) *BfReader {
	return &BfReader{
		indexCache: indexCache,
		key:        metaLoc,
		fs:         fs,
		typ:        typ,
	}
}

func (r *BfReader) MayContainsKey(key any) (b bool, err error) {
	bf, err := LoadBF(context.Background(), r.key, r.indexCache, r.fs.Service, false)
	if err != nil {
		return
	}
	// bloomFilter must be allocated on the stack
	buf := bf.GetBloomFilter(uint32(r.key.ID()))
	bloomFilter := index.NewEmptyBinaryFuseFilter()
	if err = index.DecodeBloomFilter(bloomFilter, buf); err != nil {
		return
	}
	v := types.EncodeValue(key, r.typ)
	return bloomFilter.MayContainsKey(v)
}

func (r *BfReader) MayContainsAnyKeys(
	keys containers.Vector,
	bf objectio.BloomFilter,
) (b bool, m *roaring.Bitmap, err error) {
	if bf.Size() == 0 {
		if bf, err = LoadBF(context.Background(), r.key, r.indexCache, r.fs.Service, false); err != nil {
			return
		}
	}
	buf := bf.GetBloomFilter(uint32(r.key.ID()))
	// bloomFilter must be allocated on the stack
	bloomFilter := index.NewEmptyBinaryFuseFilter()
	err = index.DecodeBloomFilter(bloomFilter, buf)
	if err != nil {
		return
	}
	return bloomFilter.MayContainsAnyKeys(keys)
}

func (r *BfReader) Destroy() error { return nil }

func LoadBF(
	ctx context.Context,
	loc objectio.Location,
	cache model.LRUCache,
	fs fileservice.FileService,
	noLoad bool,
) (bf objectio.BloomFilter, err error) {
	v, ok := cache.Get(*loc.ShortName())
	if ok {
		bf = objectio.BloomFilter(v)
		return
	}
	if noLoad {
		return
	}
	r, _ := blockio.NewObjectReader(fs, loc)
	v, size, err := r.LoadAllBF(ctx)
	if err != nil {
		return
	}
	cache.Set(*loc.ShortName(), v, int64(size))
	bf = objectio.BloomFilter(v)
	return
}
