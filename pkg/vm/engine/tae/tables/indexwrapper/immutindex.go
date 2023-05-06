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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/model"
)

var _ Index = (*immutableIndex)(nil)

type immutableIndex struct {
	zmReader *ZmReader
	bfReader *BfReader
}

func NewImmutableIndex() *immutableIndex {
	return new(immutableIndex)
}
func (index *immutableIndex) BatchUpsert(keysCtx *index.KeysCtx, offset int) (err error) {
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
) (keyselects *roaring.Bitmap, err error) {
	if keys.Length() == 1 {
		err = index.Dedup(keys.ShallowGet(0), skipfn)
		return
	}
	exist := index.zmReader.FastContainsAny(keys)
	// 1. all keys are not in [min, max]. definitely not
	if !exist {
		return
	}
	if index.bfReader != nil {
		exist, keyselects, err = index.bfReader.MayContainsAnyKeys(keys)
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

func (index *immutableIndex) ReadFrom(
	indexCache model.LRUCache,
	fs *objectio.ObjectFS,
	location objectio.Location,
	colDef *catalog.ColDef,
	blockID *types.Blockid,
) (err error) {
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
			blockID,
		)
	}
	return
}
