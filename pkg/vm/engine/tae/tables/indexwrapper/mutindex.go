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
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
)

var _ Index = (*mutableIndex)(nil)

type mutableIndex struct {
	art     index.SecondaryIndex
	zonemap *index.ZM
}

func NewPkMutableIndex(typ types.Type) *mutableIndex {
	return &mutableIndex{
		art:     index.NewSimpleARTMap(typ),
		zonemap: index.NewZM(typ.Oid),
	}
}

func (idx *mutableIndex) BatchUpsert(keysCtx *index.KeysCtx,
	offset int) (err error) {
	defer func() {
		err = TranslateError(err)
	}()
	if err = index.BatchUpdateZM(idx.zonemap, keysCtx.Keys); err != nil {
		return
	}
	// logutil.Infof("Pre: %s", idx.art.String())
	err = idx.art.BatchInsert(keysCtx, uint32(offset))
	// logutil.Infof("Post: %s", idx.art.String())
	return
}

func (idx *mutableIndex) Delete(key any) (err error) {
	defer func() {
		err = TranslateError(err)
	}()
	if _, err = idx.art.Delete(key); err != nil {
		return
	}
	return
}

func (idx *mutableIndex) GetActiveRow(key any) (row []uint32, err error) {
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
	row, err = idx.art.Search(key)
	err = TranslateError(err)
	return
}

func (idx *mutableIndex) String() string {
	return idx.art.String()
}
func (idx *mutableIndex) Dedup(key any, skipfn func(row uint32) (err error)) (err error) {
	exist := idx.zonemap.Contains(key)
	if !exist {
		return
	}
	rows, err := idx.art.Search(key)
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

func (idx *mutableIndex) BatchDedup(keys containers.Vector,
	skipfn func(row uint32) (err error)) (keyselects *roaring.Bitmap, err error) {
	if keys.Length() == 1 {
		err = idx.Dedup(keys.ShallowGet(0), skipfn)
		return
	}
	exist := idx.zonemap.FastContainsAny(keys)
	// 1. all keys are definitely not existed
	if !exist {
		return
	}
	op := getBatchDedupClosureFactory(keys.GetType().Oid, idx, skipfn)
	if err = containers.ForeachVectorWindow(keys, 0, keys.Length(), op, nil); err != nil {
		if moerr.IsMoErrCode(err, moerr.OkExpectedDup) || moerr.IsMoErrCode(err, moerr.ErrTxnWWConflict) {
			return
		} else {
			panic(err)
		}
	}
	return
}

func (idx *mutableIndex) Destroy() error {
	return idx.Close()
}

func (idx *mutableIndex) Close() error {
	idx.art = nil
	idx.zonemap = nil
	return nil
}

var _ Index = (*nonPkMutIndex)(nil)

type nonPkMutIndex struct {
	zonemap *index.ZM
}

func NewMutableIndex(typ types.Type) *nonPkMutIndex {
	return &nonPkMutIndex{
		zonemap: index.NewZM(typ.Oid),
	}
}

func (idx *nonPkMutIndex) Destroy() error {
	idx.zonemap = nil
	return nil
}

func (idx *nonPkMutIndex) Close() error {
	idx.zonemap = nil
	return nil
}
func (idx *nonPkMutIndex) GetActiveRow(any) ([]uint32, error) { panic("not support") }
func (idx *nonPkMutIndex) String() string                     { return "nonpk" }
func (idx *nonPkMutIndex) BatchUpsert(keysCtx *index.KeysCtx, offset int) (err error) {
	return TranslateError(index.BatchUpdateZM(idx.zonemap, keysCtx.Keys))
}

func (idx *nonPkMutIndex) Dedup(key any, _ func(uint32) error) (err error) {
	exist := idx.zonemap.Contains(key)
	// 1. if not in [min, max], key is definitely not found
	if !exist {
		return
	}
	err = moerr.GetOkExpectedPossibleDup()
	return
}

func (idx *nonPkMutIndex) BatchDedup(
	keys containers.Vector,
	skipfn func(row uint32) (err error)) (keyselects *roaring.Bitmap, err error) {
	keyselects, exist := idx.zonemap.ContainsAny(keys)
	// 1. all keys are definitely not existed
	if !exist {
		return
	}
	err = moerr.GetOkExpectedPossibleDup()
	return
}

func getBatchDedupClosureFactory(
	typ types.T,
	idx *mutableIndex,
	skipfn func(row uint32) (err error),
) any {
	switch typ {
	case types.T_bool:
		return batchDedupClosureFactory[bool](idx, skipfn)
	case types.T_int8:
		return batchDedupClosureFactory[int8](idx, skipfn)
	case types.T_int16:
		return batchDedupClosureFactory[int16](idx, skipfn)
	case types.T_int32:
		return batchDedupClosureFactory[int32](idx, skipfn)
	case types.T_int64:
		return batchDedupClosureFactory[int64](idx, skipfn)
	case types.T_uint8:
		return batchDedupClosureFactory[uint8](idx, skipfn)
	case types.T_uint16:
		return batchDedupClosureFactory[uint16](idx, skipfn)
	case types.T_uint32:
		return batchDedupClosureFactory[uint32](idx, skipfn)
	case types.T_uint64:
		return batchDedupClosureFactory[uint64](idx, skipfn)
	case types.T_float32:
		return batchDedupClosureFactory[float32](idx, skipfn)
	case types.T_float64:
		return batchDedupClosureFactory[float64](idx, skipfn)
	case types.T_timestamp:
		return batchDedupClosureFactory[types.Timestamp](idx, skipfn)
	case types.T_date:
		return batchDedupClosureFactory[types.Date](idx, skipfn)
	case types.T_time:
		return batchDedupClosureFactory[types.Time](idx, skipfn)
	case types.T_datetime:
		return batchDedupClosureFactory[types.Datetime](idx, skipfn)
	case types.T_decimal64:
		return batchDedupClosureFactory[types.Decimal64](idx, skipfn)
	case types.T_decimal128:
		return batchDedupClosureFactory[types.Decimal128](idx, skipfn)
	case types.T_decimal256:
		return batchDedupClosureFactory[types.Decimal256](idx, skipfn)
	case types.T_TS:
		return batchDedupClosureFactory[types.TS](idx, skipfn)
	case types.T_Rowid:
		return batchDedupClosureFactory[types.Rowid](idx, skipfn)
	case types.T_Blockid:
		return batchDedupClosureFactory[types.Blockid](idx, skipfn)
	case types.T_uuid:
		return batchDedupClosureFactory[types.Uuid](idx, skipfn)
	case types.T_char, types.T_varchar, types.T_blob, types.T_binary, types.T_varbinary, types.T_json, types.T_text:
		return batchDedupClosureFactory[[]byte](idx, skipfn)
	default:
		panic("unsupport")
	}
}

func batchDedupClosureFactory[T any](idx *mutableIndex, skipfn func(row uint32) (err error)) func(v T, _ bool, _ int) error {
	return func(v T, _ bool, _ int) error {
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
}
