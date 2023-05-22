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
	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	idxpkg "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/model"
)

var _ Index = (*immutableIndex)(nil)

type immutableIndex struct {
	zmReader *ZmReader
	bfReader *BfReader
}

func NewImmtableIndex(
	indexCache model.LRUCache,
	fs *objectio.ObjectFS,
	location objectio.Location,
	colDef *catalog.ColDef,
) (index *immutableIndex, err error) {
	index = new(immutableIndex)
	index.zmReader = NewZmReader(
		fs,
		uint16(colDef.Idx),
		location)

	if colDef.IsRealPrimary() {
		index.bfReader = NewBfReader(
			colDef.Type.Oid,
			location,
			indexCache,
			fs,
		)
	}
	return
}

func (index *immutableIndex) BatchUpsert(keysCtx *idxpkg.KeysCtx, offset int) (err error) {
	panic("not support")
}
func (index *immutableIndex) GetActiveRow(key any) ([]uint32, error) { panic("not support") }
func (index *immutableIndex) String() string                         { return "immutable" }
func (index *immutableIndex) Dedup(key any, _ func(row uint32) error) (err error) {
	exist := index.zmReader.Contains(key)
	// 1. if not in [min, max], key is definitely not found
	if !exist {
		return
	}
	if index.bfReader != nil {
		exist, err = index.bfReader.MayContainsKey(key)
		// 2. check bloomfilter has some error. return err
		if err != nil {
			err = TranslateError(err)
			return
		}
		// 3. all keys were checked. definitely not
		if !exist {
			return
		}
	}

	err = moerr.GetOkExpectedPossibleDup()
	return
}

func (index *immutableIndex) BatchDedup(
	keys containers.Vector,
	skipfn func(row uint32) (err error),
	zm idxpkg.ZM,
	bf objectio.BloomFilter,
) (sels *roaring.Bitmap, err error) {
	var exist bool
	if zm.Valid() {
		if exist = index.zmReader.Intersect(zm); !exist {
			return
		}
	} else {
		if keys.Length() == 1 {
			err = index.Dedup(keys.ShallowGet(0), skipfn)
			return
		}
		// 1. all keys are not in [min, max]. definitely not
		if exist = index.zmReader.FastContainsAny(keys); !exist {
			return
		}
	}
	if index.bfReader != nil {
		exist, sels, err = index.bfReader.MayContainsAnyKeys(keys, bf)
		// 2. check bloomfilter has some unknown error. return err
		if err != nil {
			err = TranslateError(err)
			return
		}
		// 3. all keys were checked. definitely not
		if !exist {
			return
		}
	}
	err = moerr.GetOkExpectedPossibleDup()
	return
}

func (index *immutableIndex) Close() (err error) {
	// TODO
	return
}

func (index *immutableIndex) Destroy() (err error) {
	if index.zmReader != nil {
		if err = index.zmReader.Destroy(); err != nil {
			return
		}
	}
	if index.bfReader != nil {
		err = index.bfReader.Destroy()
	}
	return
}

type PersistedIndex struct {
	zm       idxpkg.ZM
	bfLoader func() ([]byte, error)
}

func NewPersistedIndex(
	zm idxpkg.ZM,
	bfLoader func() ([]byte, error),
) PersistedIndex {
	return PersistedIndex{
		zm:       zm,
		bfLoader: bfLoader,
	}
}

func (index PersistedIndex) BatchDedup(
	keys containers.Vector,
	keysZM idxpkg.ZM,
) (sels *roaring.Bitmap, err error) {
	var exist bool
	if keysZM.Valid() {
		if exist = index.zm.FastIntersect(keysZM); !exist {
			// all keys are not in [min, max]. definitely not
			return
		}
	} else {
		if exist = index.zm.FastContainsAny(keys); !exist {
			// all keys are not in [min, max]. definitely not
			return
		}
	}

	// some keys are in [min, max]. check bloomfilter for those keys

	var buf []byte
	if index.bfLoader != nil {
		// load bloomfilter
		if buf, err = index.bfLoader(); err != nil {
			return
		}
	} else {
		// no bloomfilter. it is possible duplicate
		err = moerr.GetOkExpectedPossibleDup()
		return
	}

	bfIndex := idxpkg.NewEmptyBinaryFuseFilter()
	if err = idxpkg.DecodeBloomFilter(bfIndex, buf); err != nil {
		return
	}

	if exist, sels, err = bfIndex.MayContainsAnyKeys(keys); err != nil {
		// check bloomfilter has some unknown error. return err
		err = TranslateError(err)
		return
	} else if !exist {
		// all keys were checked. definitely not
		return
	}

	err = moerr.GetOkExpectedPossibleDup()
	return
}
