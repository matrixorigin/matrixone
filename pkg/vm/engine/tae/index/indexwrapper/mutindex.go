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
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
)

type MutIndex struct {
	art     index.SecondaryIndex
	zonemap index.ZM
}

func NewMutIndex(typ types.Type) *MutIndex {
	return &MutIndex{
		art:     index.NewSimpleARTMap(),
		zonemap: index.NewZM(typ.Oid, typ.Scale),
	}
}

// BatchUpsert batch insert the specific keys
// If any deduplication, it will fetch the old value first, fill the active map with new value, insert the old value into delete map
// If any other unknown error hanppens, return error
func (idx *MutIndex) BatchUpsert(
	keys containers.Vector,
	offset int,
) (err error) {
	defer func() {
		err = TranslateError(err)
	}()
	if err = index.BatchUpdateZM(idx.zonemap, keys); err != nil {
		return
	}
	// logutil.Infof("Pre: %s", idx.art.String())
	err = idx.art.BatchInsert(keys, 0, keys.Length(), uint32(offset))
	// logutil.Infof("Post: %s", idx.art.String())
	return
}

func (idx *MutIndex) GetActiveRow(key any) (row []uint32, err error) {
	defer func() {
		err = TranslateError(err)
		// logutil.Infof("[Trace][GetActiveRow] key=%v: err=%v", key, err)
	}()
	exist := idx.zonemap.Contains(key)
	// 1. key is definitely not existed
	if !exist {
		err = moerr.NewNotFoundNoCtx()
		return
	}
	// 2. search art tree for key
	ikey := types.EncodeValue(key, idx.zonemap.GetType())
	row, err = idx.art.Search(ikey)
	err = TranslateError(err)
	return
}

func (idx *MutIndex) String() string {
	return idx.art.String()
}

// Dedup returns wether the specified key is existed
// If key is existed, return ErrDuplicate
// If any other unknown error happens, return error
// If key is not found, return nil
func (idx *MutIndex) Dedup(key any, skipfn func(row uint32) (err error)) (err error) {
	exist := idx.zonemap.Contains(key)
	if !exist {
		return
	}
	ikey := types.EncodeValue(key, idx.zonemap.GetType())
	rows, err := idx.art.Search(ikey)
	if err == index.ErrNotFound {
		err = nil
		return
	}
	for _, row := range rows {
		if err = skipfn(row); err != nil {
			return
		}
	}
	return
}

func (idx *MutIndex) BatchDedup(
	keys containers.Vector,
	keysZM index.ZM,
	skipfn func(row uint32) (err error),
	_ objectio.BloomFilter,
) (keyselects *roaring.Bitmap, err error) {
	if keysZM.Valid() {
		if exist := idx.zonemap.FastIntersect(keysZM); !exist {
			return
		}
	} else {
		if keys.Length() == 1 {
			err = idx.Dedup(keys.ShallowGet(0), skipfn)
			return
		}
		// 1. all keys are definitely not existed
		if exist := idx.zonemap.FastContainsAny(keys); !exist {
			return
		}
	}
	op := func(v []byte, _ bool, _ int) error {
		rows, err := idx.art.Search(v)
		if err == index.ErrNotFound {
			return nil
		}
		for _, row := range rows {
			if err = skipfn(row); err != nil {
				return err
			}
		}
		return nil
	}
	if err = containers.ForeachWindowBytes(keys, 0, keys.Length(), op, nil); err != nil {
		if moerr.IsMoErrCode(err, moerr.OkExpectedDup) || moerr.IsMoErrCode(err, moerr.ErrTxnWWConflict) {
			return
		} else {
			panic(err)
		}
	}
	return
}

func (idx *MutIndex) Close() error {
	idx.art = nil
	idx.zonemap = nil
	return nil
}
