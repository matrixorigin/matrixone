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

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
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
	keys *vector.Vector,
	offset int,
) (err error) {
	defer func() {
		err = TranslateError(err)
	}()
	if err = index.BatchUpdateZM(idx.zonemap, keys); err != nil {
		return
	}
	err = idx.art.BatchInsert(keys, 0, keys.Length(), uint32(offset))
	return
}

func (idx *MutIndex) DeleteAt(key any, row uint32) error {
	encoded := types.EncodeValue(key, idx.zonemap.GetType())
	err := idx.art.DeleteAt(encoded, row)
	if err == index.ErrNotFound {
		return nil
	}
	return err
}

func (idx *MutIndex) GetActiveRow(key any) (row []uint32, err error) {
	defer func() {
		err = TranslateError(err)
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

// ForeachRowsByKeys calls fn for every ART row whose key is present and
// non-null in keys. It does not perform MVCC filtering or mutate keys.
func (idx *MutIndex) ForeachRowsByKeys(
	keys *vector.Vector,
	keysZM index.ZM,
	fn func(row uint32),
) error {
	if keysZM.Valid() {
		if !idx.zonemap.FastIntersect(keysZM) {
			return nil
		}
	} else if !idx.zonemap.FastContainsAny(keys) {
		return nil
	}
	op := func(v []byte, isNull bool, _ int) error {
		if isNull {
			return nil
		}
		rows, err := idx.art.Search(v)
		if err == index.ErrNotFound {
			return nil
		}
		if err != nil {
			return err
		}
		for _, row := range rows {
			fn(row)
		}
		return nil
	}
	return containers.ForeachWindowBytes(keys, 0, keys.Length(), op, nil)
}

func (idx *MutIndex) String() string {
	return idx.art.String()
}

// Dedup returns wether the specified key is existed
// If key is existed, return ErrDuplicate
// If any other unknown error happens, return error
// If key is not found, return nil
func (idx *MutIndex) Dedup(ctx context.Context, key any, skipfn func(row uint32) (err error)) (err error) {
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
	ctx context.Context,
	keys *vector.Vector,
	keysZM index.ZM,
	skipfn func(row uint32) (err error),
	_ objectio.BloomFilter,
) (keyselects *roaring.Bitmap, err error) {
	if keysZM.Valid() {
		if exist := idx.zonemap.FastIntersect(keysZM); !exist {
			return
		}
	} else {
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

func (idx *MutIndex) GetDuplicatedRows(
	ctx context.Context,
	keys *vector.Vector,
	keysZM index.ZM,
	blkID *types.Blockid,
	rowIDs *vector.Vector,
	getRowSelectionFn func() (index.RowSelection, error),
	skipFn func(row uint32) error,
	mp *mpool.MPool,
) (err error) {
	if keysZM.Valid() {
		if exist := idx.zonemap.FastIntersect(keysZM); !exist {
			return
		}
	} else {
		// 1. all keys are definitely not existed
		if exist := idx.zonemap.FastContainsAny(keys); !exist {
			return
		}
	}
	selection, err := getRowSelectionFn()
	if err != nil {
		return
	}
	op := func(v []byte, _ bool, offset int) error {
		if !rowIDs.IsNull(uint64(offset)) {
			return nil
		}
		rows, err := idx.art.Search(v)
		if err == index.ErrNotFound {
			return nil
		}
		// Check the newest valid owner even when it is outside selection.  Rows
		// without an eligible append node (aborted, missing, or still
		// unprepared) are stale/future candidates, so keep walking toward older
		// owners instead of exposing the skip signal to the caller.
		if skipFn != nil {
			for i := len(rows) - 1; i >= 0; i-- {
				err = skipFn(rows[i])
				if err == index.ErrNotFound {
					continue
				}
				if err != nil {
					return err
				}
				break
			}
		}
		var maxRow uint32
		exist := false
		for i := len(rows) - 1; i >= 0; i-- {
			if !selection.Contains(rows[i]) {
				continue
			}
			if skipFn != nil {
				err = skipFn(rows[i])
				if err == index.ErrNotFound {
					continue
				}
				if err != nil {
					return err
				}
			}
			maxRow = rows[i]
			exist = true
			break
		}
		if !exist {
			return nil
		}
		rowID := objectio.NewRowid(blkID, maxRow)
		containers.UpdateValue(rowIDs, uint32(offset), rowID, false, mp)
		return nil
	}
	if err = containers.ForeachWindowBytes(keys, 0, keys.Length(), op, nil); err != nil {
		return
	}
	return
}

func (idx *MutIndex) Contains(
	ctx context.Context,
	keys *vector.Vector,
	keysZM index.ZM,
	blkID *types.Blockid,
	skipFn func(row uint32) error,
	mp *mpool.MPool,
) (err error) {
	if keysZM.Valid() {
		if exist := idx.zonemap.FastIntersect(keysZM); !exist {
			return
		}
	} else {
		// 1. all keys are definitely not existed
		if exist := idx.zonemap.FastContainsAny(keys); !exist {
			return
		}
	}
	op := func(v []byte, isNull bool, offset int) error {
		if isNull {
			return nil
		}
		rows, err := idx.art.Search(v)
		if err == index.ErrNotFound {
			return nil
		}
		// Concurrent deletes may temporarily add more than one tombstone for the
		// same rowid. Walk from the newest candidate to the oldest and ignore
		// candidates whose append node is aborted or still has UncommitTS; the
		// caller reports both cases as index.ErrNotFound. A prepared conflicting
		// candidate still propagates its write-write conflict.
		for i := len(rows) - 1; i >= 0; i-- {
			err = skipFn(rows[i])
			if err == index.ErrNotFound {
				continue
			}
			if err != nil {
				return err
			}
			containers.UpdateValue(keys, uint32(offset), nil, true, mp)
			return nil
		}
		return nil
	}
	if err = containers.ForeachWindowBytes(keys, 0, keys.Length(), op, nil); err != nil {
		return err
	}
	return
}
func (idx *MutIndex) Close() error {
	idx.art = nil
	idx.zonemap = nil
	return nil
}
