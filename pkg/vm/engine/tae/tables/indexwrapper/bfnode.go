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
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/model"
)

type BfReader struct {
	bfKey      objectio.Location
	reader     *blockio.BlockReader
	typ        types.T
	indexCache model.LRUCache
}

func NewBfReader(
	typ types.T,
	metaLoc objectio.Location,
	indexCache model.LRUCache,
	fs *objectio.ObjectFS,
) *BfReader {
	reader, _ := blockio.NewObjectReader(fs.Service, metaLoc)

	return &BfReader{
		indexCache: indexCache,
		bfKey:      metaLoc,
		reader:     reader,
		typ:        typ,
	}
}

func (r *BfReader) getBloomFilter() (objectio.BloomFilter, error) {
	var v []byte
	var size uint32
	var err error
	v, ok := r.indexCache.Get(*r.bfKey.ShortName())
	if !ok {
		v, size, err = r.reader.LoadAllBF(context.Background())
		if err != nil {
			return nil, err
		}
		r.indexCache.Set(*r.bfKey.ShortName(), v, int64(size))
	}

	return objectio.BloomFilter(v), nil
}

func (r *BfReader) MayContainsKey(key any) (b bool, err error) {
	bf, err := r.getBloomFilter()
	if err != nil {
		return
	}
	buf := bf.GetBloomFilter(uint32(r.bfKey.ID()))
	// bloomFilter must be allocated on the stack
	bloomFilter := index.NewEmptyBinaryFuseFilter()
	err = index.DecodeBloomFilter(bloomFilter, buf)
	if err != nil {
		return
	}
	v := types.EncodeValue(key, r.typ)
	return bloomFilter.MayContainsKey(v)
}

func (r *BfReader) MayContainsAnyKeys(keys containers.Vector) (b bool, m *roaring.Bitmap, err error) {
	bf, err := r.getBloomFilter()
	if err != nil {
		return
	}
	buf := bf.GetBloomFilter(uint32(r.bfKey.ID()))
	// bloomFilter must be allocated on the stack
	bloomFilter := index.NewEmptyBinaryFuseFilter()
	err = index.DecodeBloomFilter(bloomFilter, buf)
	if err != nil {
		return
	}
	return bloomFilter.MayContainsAnyKeys(keys)
}

func (r *BfReader) Destroy() error { return nil }
