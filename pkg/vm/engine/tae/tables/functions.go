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

var dedupAlkFunctions = map[types.T]any{
	types.T_bool:       dedupABlkFuncFactory[bool](compute.CompareBool),
	types.T_int8:       dedupABlkFuncFactory[int8](compute.CompareOrdered[int8]),
	types.T_int16:      dedupABlkFuncFactory[int16](compute.CompareOrdered[int16]),
	types.T_int32:      dedupABlkFuncFactory[int32](compute.CompareOrdered[int32]),
	types.T_int64:      dedupABlkFuncFactory[int64](compute.CompareOrdered[int64]),
	types.T_uint8:      dedupABlkFuncFactory[uint8](compute.CompareOrdered[uint8]),
	types.T_uint16:     dedupABlkFuncFactory[uint16](compute.CompareOrdered[uint16]),
	types.T_uint32:     dedupABlkFuncFactory[uint32](compute.CompareOrdered[uint32]),
	types.T_uint64:     dedupABlkFuncFactory[uint64](compute.CompareOrdered[uint64]),
	types.T_float32:    dedupABlkFuncFactory[float32](compute.CompareOrdered[float32]),
	types.T_float64:    dedupABlkFuncFactory[float64](compute.CompareOrdered[float64]),
	types.T_timestamp:  dedupABlkFuncFactory[types.Timestamp](compute.CompareOrdered[types.Timestamp]),
	types.T_date:       dedupABlkFuncFactory[types.Date](compute.CompareOrdered[types.Date]),
	types.T_time:       dedupABlkFuncFactory[types.Time](compute.CompareOrdered[types.Time]),
	types.T_datetime:   dedupABlkFuncFactory[types.Datetime](compute.CompareOrdered[types.Datetime]),
	types.T_decimal64:  dedupABlkFuncFactory[types.Decimal64](types.CompareDecimal64),
	types.T_decimal128: dedupABlkFuncFactory[types.Decimal128](types.CompareDecimal128),
	types.T_decimal256: dedupABlkFuncFactory[types.Decimal256](types.CompareDecimal256),
	types.T_TS:         dedupABlkFuncFactory[types.TS](types.CompareTSTSAligned),
	types.T_Rowid:      dedupABlkFuncFactory[types.Rowid](types.CompareRowidRowidAligned),
	types.T_Blockid:    dedupABlkFuncFactory[types.Blockid](types.CompareBlockidBlockidAligned),
	types.T_uuid:       dedupABlkFuncFactory[types.Uuid](types.CompareUuid),

	types.T_char:      dedupABlkBytesFunc,
	types.T_varchar:   dedupABlkBytesFunc,
	types.T_blob:      dedupABlkBytesFunc,
	types.T_binary:    dedupABlkBytesFunc,
	types.T_varbinary: dedupABlkBytesFunc,
	types.T_json:      dedupABlkBytesFunc,
	types.T_text:      dedupABlkBytesFunc,
}

func parseNADedeupArgs(args ...any) (vec *vector.Vector, mask *roaring.Bitmap, def *catalog.ColDef) {
	vec = args[0].(containers.Vector).GetDownstreamVector()
	if args[1] != nil {
		mask = args[1].(*roaring.Bitmap)
	}
	if args[2] != nil {
		def = args[2].(*catalog.ColDef)
	}
	return
}

func parseADedeupArgs(args ...any) (
	vec containers.Vector, mask *roaring.Bitmap, def *catalog.ColDef, scan func() (containers.Vector, error), txn txnif.TxnReader) {
	vec = args[0].(containers.Vector)
	if args[1] != nil {
		mask = args[1].(*roaring.Bitmap)
	}
	if args[2] != nil {
		def = args[2].(*catalog.ColDef)
	}
	if args[3] != nil {
		scan = args[3].(func() (containers.Vector, error))
	}
	if args[4] != nil {
		txn = args[4].(txnif.TxnReader)
	}
	return
}

func dedupNABlkFuncFactory[T any](comp func(T, T) int64) func(args ...any) func(T, bool, int) error {
	return func(args ...any) func(T, bool, int) error {
		vec, mask, def := parseNADedeupArgs(args...)
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
	vec, mask, def := parseNADedeupArgs(args...)
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
	vec, mask, def := parseNADedeupArgs(args...)
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

func dedupABlkBytesFunc(args ...any) func([]byte, bool, int) error {
	vec, mask, def, scan, txn := parseADedeupArgs(args...)
	return func(v1 []byte, _ bool, _ int) error {
		var tsVec containers.Vector
		defer func() {
			if tsVec != nil {
				tsVec.Close()
				tsVec = nil
			}
		}()
		return containers.ForeachWindowVarlen(
			vec,
			0,
			vec.Length(),
			func(v2 []byte, _ bool, row int) (err error) {
				// logutil.Infof("row=%d,v1=%v,v2=%v", row, v1, v2)
				if mask != nil && mask.ContainsInt(row) {
					return
				}
				if compute.CompareBytes(v1, v2) != 0 {
					return
				}
				if tsVec == nil {
					if tsVec, err = scan(); err != nil {
						return
					}
				}
				commitTS := tsVec.Get(row).(types.TS)
				if commitTS.Greater(txn.GetStartTS()) {
					return txnif.ErrTxnWWConflict
				}
				entry := common.TypeStringValue(vec.GetType(), any(v1))
				return moerr.NewDuplicateEntryNoCtx(entry, def.Name)
			}, nil)
	}
}

func dedupABlkFuncFactory[T types.FixedSizeT](comp func(T, T) int64) func(args ...any) func(T, bool, int) error {
	return func(args ...any) func(T, bool, int) error {
		vec, mask, def, scan, txn := parseADedeupArgs(args...)
		return func(v1 T, _ bool, _ int) error {
			var tsVec containers.Vector
			defer func() {
				if tsVec != nil {
					tsVec.Close()
					tsVec = nil
				}
			}()
			return containers.ForeachWindowFixed(
				vec,
				0,
				vec.Length(),
				func(v2 T, _ bool, row int) (err error) {
					if mask != nil && mask.ContainsInt(row) {
						return
					}
					if comp(v1, v2) != 0 {
						return
					}
					if tsVec == nil {
						if tsVec, err = scan(); err != nil {
							return
						}
					}
					commitTS := tsVec.Get(row).(types.TS)
					if commitTS.Greater(txn.GetStartTS()) {
						return txnif.ErrTxnWWConflict
					}
					entry := common.TypeStringValue(vec.GetType(), any(v1))
					return moerr.NewDuplicateEntryNoCtx(entry, def.Name)
				}, nil)
		}
	}
}

func dedupNABlkClosure(
	vec containers.Vector,
	txn txnif.TxnReader,
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
	txnif.TxnReader,
	*roaring.Bitmap,
	*catalog.ColDef,
) func(any, bool, int) error {
	return func(vec containers.Vector, txn txnif.TxnReader, mask *roaring.Bitmap, def *catalog.ColDef) func(any, bool, int) error {
		return func(v1 any, _ bool, _ int) (err error) {
			var tsVec containers.Vector
			defer func() {
				if tsVec != nil {
					tsVec.Close()
					tsVec = nil
				}
			}()
			return vec.ForeachShallow(func(v2 any, _ bool, row int) (err error) {
				// logutil.Infof("%v, %v, %d", v1, v2, row)
				if mask != nil && mask.ContainsInt(row) {
					return
				}
				if compute.CompareGeneric(v1, v2, vec.GetType().Oid) != 0 {
					return
				}
				if tsVec == nil {
					tsVec, err = scan()
					if err != nil {
						return
					}
				}
				commitTS := tsVec.Get(row).(types.TS)
				if commitTS.Greater(txn.GetStartTS()) {
					return txnif.ErrTxnWWConflict
				}
				entry := common.TypeStringValue(vec.GetType(), v1)
				return moerr.NewDuplicateEntryNoCtx(entry, def.Name)
			}, nil)
		}
	}
}
