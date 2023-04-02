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

package tables

import (
	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/compute"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
)

var dedupNABlkFunctions = map[types.T]any{
	types.T_bool:       dedupNABlkFuncFactory[bool](compute.CompareBool),
	types.T_int8:       dedupNABlkOrderedFunc[int8],
	types.T_int16:      dedupNABlkOrderedFunc[int16],
	types.T_int32:      dedupNABlkOrderedFunc[int32],
	types.T_int64:      dedupNABlkOrderedFunc[int64],
	types.T_uint8:      dedupNABlkOrderedFunc[uint8],
	types.T_uint16:     dedupNABlkOrderedFunc[uint16],
	types.T_uint32:     dedupNABlkOrderedFunc[uint32],
	types.T_uint64:     dedupNABlkOrderedFunc[uint64],
	types.T_float32:    dedupNABlkOrderedFunc[float32],
	types.T_float64:    dedupNABlkOrderedFunc[float64],
	types.T_timestamp:  dedupNABlkOrderedFunc[types.Timestamp],
	types.T_date:       dedupNABlkOrderedFunc[types.Date],
	types.T_time:       dedupNABlkOrderedFunc[types.Time],
	types.T_datetime:   dedupNABlkOrderedFunc[types.Datetime],
	types.T_decimal64:  dedupNABlkFuncFactory[types.Decimal64](types.CompareDecimal64),
	types.T_decimal128: dedupNABlkFuncFactory[types.Decimal128](types.CompareDecimal128),
	types.T_decimal256: dedupNABlkFuncFactory[types.Decimal256](types.CompareDecimal256),
	types.T_TS:         dedupNABlkFuncFactory[types.TS](types.CompareTSTSAligned),
	types.T_Rowid:      dedupNABlkFuncFactory[types.Rowid](types.CompareRowidRowidAligned),
	types.T_Blockid:    dedupNABlkFuncFactory[types.Blockid](types.CompareBlockidBlockidAligned),
	types.T_uuid:       dedupNABlkFuncFactory[types.Uuid](types.CompareUuid),

	types.T_char:      dedupNABlkBytesFunc,
	types.T_varchar:   dedupNABlkBytesFunc,
	types.T_blob:      dedupNABlkBytesFunc,
	types.T_binary:    dedupNABlkBytesFunc,
	types.T_varbinary: dedupNABlkBytesFunc,
	types.T_json:      dedupNABlkBytesFunc,
	types.T_text:      dedupNABlkBytesFunc,
}

func parseDedupArgs(args ...any) (vec *vector.Vector, mask *roaring.Bitmap, def *catalog.ColDef) {
	vec = args[0].(containers.Vector).GetDownstreamVector()
	if args[1] != nil {
		mask = args[1].(*roaring.Bitmap)
	}
	if args[2] != nil {
		def = args[2].(*catalog.ColDef)
	}
	return
}

func dedupNABlkFuncFactory[T any](comp func(T, T) int64) func(args ...any) func(T, bool, int) error {
	return func(args ...any) func(T, bool, int) error {
		vec, mask, def := parseDedupArgs(args...)
		vs := vector.MustFixedCol[T](vec)
		return func(v T, _ bool, row int) (err error) {
			// logutil.Infof("row=%d,v=%v", row, v)
			if _, existed := compute.GetOffsetWithFunc(
				vs,
				v,
				comp,
				mask,
			); existed {
				entry := common.TypeStringValue(*vec.GetType(), any(v))
				return moerr.NewDuplicateEntryNoCtx(entry, def.Name)
			}
			return
		}
	}
}

func dedupNABlkBytesFunc(args ...any) func([]byte, bool, int) error {
	vec, mask, def := parseDedupArgs(args...)
	return func(v []byte, _ bool, row int) (err error) {
		// logutil.Infof("row=%d,v=%v", row, v)
		if _, existed := compute.GetOffsetOfBytes(
			vec,
			v,
			mask,
		); existed {
			entry := common.TypeStringValue(*vec.GetType(), any(v))
			return moerr.NewDuplicateEntryNoCtx(entry, def.Name)
		}
		return
	}
}

func dedupNABlkOrderedFunc[T types.OrderedT](args ...any) func(T, bool, int) error {
	vec, mask, def := parseDedupArgs(args...)
	vs := vector.MustFixedCol[T](vec)
	return func(v T, _ bool, row int) (err error) {
		// logutil.Infof("row=%d,v=%v", row, v)
		if _, existed := compute.GetOffsetOfOrdered2(
			vs,
			v,
			mask,
		); existed {
			entry := common.TypeStringValue(*vec.GetType(), any(v))
			return moerr.NewDuplicateEntryNoCtx(entry, def.Name)
		}
		return
	}
}

func dedupNABlkClosure(
	vec containers.Vector,
	ts types.TS,
	mask *roaring.Bitmap,
	def *catalog.ColDef) func(any, bool, int) error {
	return func(v any, _ bool, _ int) (err error) {
		if _, existed := compute.GetOffsetByVal(vec, v, mask); existed {
			entry := common.TypeStringValue(vec.GetType(), v)
			return moerr.NewDuplicateEntryNoCtx(entry, def.Name)
		}
		return nil
	}
}

func dedupABlkClosureFactory(
	scan func() (containers.Vector, error),
) func(
	containers.Vector,
	types.TS,
	*roaring.Bitmap,
	*catalog.ColDef,
) func(any, bool, int) error {
	return func(vec containers.Vector, ts types.TS, mask *roaring.Bitmap, def *catalog.ColDef) func(any, bool, int) error {
		return func(v1 any, _ bool, _ int) (err error) {
			var tsVec containers.Vector
			defer func() {
				if tsVec != nil {
					tsVec.Close()
					tsVec = nil
				}
			}()
			return vec.ForeachShallow(func(v2 any, _ bool, row int) (err error) {
				if mask != nil && mask.ContainsInt(row) {
					return
				}
				if compute.CompareGeneric(v1, v2, vec.GetType()) != 0 {
					return
				}
				if tsVec == nil {
					tsVec, err = scan()
					if err != nil {
						return
					}
				}
				commitTS := tsVec.Get(row).(types.TS)
				if commitTS.Greater(ts) {
					return txnif.ErrTxnWWConflict
				}
				entry := common.TypeStringValue(vec.GetType(), v1)
				return moerr.NewDuplicateEntryNoCtx(entry, def.Name)
			}, nil)
		}
	}
}
