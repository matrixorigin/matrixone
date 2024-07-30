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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/compute"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
)

var dedupNABlkFunctions = map[types.T]any{
	types.T_bool:       dedupNABlkFuncFactory(compute.CompareBool),
	types.T_bit:        dedupNABlkOrderedFunc[uint64],
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
	types.T_enum:       dedupNABlkOrderedFunc[types.Enum],
	types.T_decimal64:  dedupNABlkFuncFactory(types.CompareDecimal64),
	types.T_decimal128: dedupNABlkFuncFactory(types.CompareDecimal128),
	types.T_decimal256: dedupNABlkFuncFactory(types.CompareDecimal256),
	types.T_TS:         dedupNABlkFuncFactory(types.CompareTSTSAligned),
	types.T_Rowid:      dedupNABlkFuncFactory(types.CompareRowidRowidAligned),
	types.T_Blockid:    dedupNABlkFuncFactory(types.CompareBlockidBlockidAligned),
	types.T_uuid:       dedupNABlkFuncFactory(types.CompareUuid),

	types.T_char:      dedupNABlkBytesFunc,
	types.T_varchar:   dedupNABlkBytesFunc,
	types.T_blob:      dedupNABlkBytesFunc,
	types.T_binary:    dedupNABlkBytesFunc,
	types.T_varbinary: dedupNABlkBytesFunc,
	types.T_json:      dedupNABlkBytesFunc,
	types.T_text:      dedupNABlkBytesFunc,
	types.T_datalink:  dedupNABlkBytesFunc,

	types.T_array_float32: dedupNABlkBytesFunc,
	types.T_array_float64: dedupNABlkBytesFunc,
}

var dedupAlkFunctions = map[types.T]any{
	types.T_bool:       dedupABlkFuncFactory(compute.CompareBool),
	types.T_bit:        dedupABlkFuncFactory(compute.CompareOrdered[uint64]),
	types.T_int8:       dedupABlkFuncFactory(compute.CompareOrdered[int8]),
	types.T_int16:      dedupABlkFuncFactory(compute.CompareOrdered[int16]),
	types.T_int32:      dedupABlkFuncFactory(compute.CompareOrdered[int32]),
	types.T_int64:      dedupABlkFuncFactory(compute.CompareOrdered[int64]),
	types.T_uint8:      dedupABlkFuncFactory(compute.CompareOrdered[uint8]),
	types.T_uint16:     dedupABlkFuncFactory(compute.CompareOrdered[uint16]),
	types.T_uint32:     dedupABlkFuncFactory(compute.CompareOrdered[uint32]),
	types.T_uint64:     dedupABlkFuncFactory(compute.CompareOrdered[uint64]),
	types.T_float32:    dedupABlkFuncFactory(compute.CompareOrdered[float32]),
	types.T_float64:    dedupABlkFuncFactory(compute.CompareOrdered[float64]),
	types.T_timestamp:  dedupABlkFuncFactory(compute.CompareOrdered[types.Timestamp]),
	types.T_date:       dedupABlkFuncFactory(compute.CompareOrdered[types.Date]),
	types.T_time:       dedupABlkFuncFactory(compute.CompareOrdered[types.Time]),
	types.T_datetime:   dedupABlkFuncFactory(compute.CompareOrdered[types.Datetime]),
	types.T_enum:       dedupABlkFuncFactory(compute.CompareOrdered[types.Enum]),
	types.T_decimal64:  dedupABlkFuncFactory(types.CompareDecimal64),
	types.T_decimal128: dedupABlkFuncFactory(types.CompareDecimal128),
	types.T_decimal256: dedupABlkFuncFactory(types.CompareDecimal256),
	types.T_TS:         dedupABlkFuncFactory(types.CompareTSTSAligned),
	types.T_Rowid:      dedupABlkFuncFactory(types.CompareRowidRowidAligned),
	types.T_Blockid:    dedupABlkFuncFactory(types.CompareBlockidBlockidAligned),
	types.T_uuid:       dedupABlkFuncFactory(types.CompareUuid),

	types.T_char:      dedupABlkBytesFunc,
	types.T_varchar:   dedupABlkBytesFunc,
	types.T_blob:      dedupABlkBytesFunc,
	types.T_binary:    dedupABlkBytesFunc,
	types.T_varbinary: dedupABlkBytesFunc,
	types.T_json:      dedupABlkBytesFunc,
	types.T_text:      dedupABlkBytesFunc,
	types.T_datalink:  dedupABlkBytesFunc,

	types.T_array_float32: dedupABlkBytesFunc,
	types.T_array_float64: dedupABlkBytesFunc,
}

func parseNADedeupArgs(args ...any) (vec *vector.Vector, mask *nulls.Bitmap, def *catalog.ColDef) {
	vec = args[0].(containers.Vector).GetDownstreamVector()
	if args[1] != nil {
		mask = args[1].(*nulls.Bitmap)
	}
	if args[2] != nil {
		def = args[2].(*catalog.ColDef)
	}
	return
}

func parseADedeupArgs(args ...any) (
	vec containers.Vector, mask *nulls.Bitmap, def *catalog.ColDef,
	scan func(uint16) (containers.Vector, error),
	txn txnif.TxnReader,
) {
	vec = args[0].(containers.Vector)
	if args[1] != nil {
		mask = args[1].(*nulls.Bitmap)
	}
	if args[2] != nil {
		def = args[2].(*catalog.ColDef)
	}
	if args[3] != nil {
		scan = args[3].(func(uint16) (containers.Vector, error))
	}
	if args[4] != nil {
		txn = args[4].(txnif.TxnReader)
	}
	return
}

func dedupNABlkFuncFactory[T any](comp func(T, T) int) func(args ...any) func(T, bool, int) error {
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
				entry := common.TypeStringValue(*vec.GetType(), any(v), false)
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
		if rowOffset, existed := compute.GetOffsetOfBytes(
			vec,
			v,
			mask,
		); existed {
			entry := common.TypeStringValue(*vec.GetType(), any(v), false)
			logutil.Infof("Duplicate: row %d", rowOffset)
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
		if _, existed := compute.GetOffsetOfOrdered(
			vs,
			v,
			mask,
		); existed {
			entry := common.TypeStringValue(*vec.GetType(), any(v), false)
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
			vec.GetDownstreamVector(),
			0,
			vec.Length(),
			func(v2 []byte, _ bool, row int) (err error) {
				// logutil.Infof("row=%d,v1=%v,v2=%v", row, v1, v2)
				if mask.Contains(uint64(row)) {
					return
				}
				if compute.CompareBytes(v1, v2) != 0 {
					return
				}
				if tsVec == nil {
					if tsVec, err = scan(0); err != nil {
						return
					}
				}
				commitTS := tsVec.Get(row).(types.TS)
				startTS := txn.GetStartTS()
				if commitTS.Greater(&startTS) {
					return txnif.ErrTxnWWConflict
				}
				entry := common.TypeStringValue(*vec.GetType(), any(v1), false)
				return moerr.NewDuplicateEntryNoCtx(entry, def.Name)
			}, nil, nil)
	}
}

func dedupABlkFuncFactory[T types.FixedSizeT](comp func(T, T) int) func(args ...any) func(T, bool, int) error {
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
				vec.GetDownstreamVector(),
				0,
				vec.Length(),
				func(v2 T, _ bool, row int) (err error) {
					if mask.Contains(uint64(row)) {
						return
					}
					if comp(v1, v2) != 0 {
						return
					}
					if tsVec == nil {
						if tsVec, err = scan(0); err != nil {
							return
						}
					}
					commitTS := tsVec.Get(row).(types.TS)
					startTS := txn.GetStartTS()
					if commitTS.Greater(&startTS) {
						return txnif.ErrTxnWWConflict
					}
					entry := common.TypeStringValue(*vec.GetType(), any(v1), false)
					return moerr.NewDuplicateEntryNoCtx(entry, def.Name)
				}, nil, nil)
		}
	}
}

func dedupNABlkClosure(
	vec containers.Vector,
	txn txnif.TxnReader,
	mask *nulls.Bitmap,
	def *catalog.ColDef) func(any, bool, int) error {
	return func(v any, _ bool, _ int) (err error) {
		if _, existed := compute.GetOffsetByVal(vec, v, mask); existed {
			entry := common.TypeStringValue(*vec.GetType(), v, false)
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
	*nulls.Bitmap,
	*catalog.ColDef,
) func(any, bool, int) error {
	return func(vec containers.Vector, txn txnif.TxnReader, mask *nulls.Bitmap, def *catalog.ColDef) func(any, bool, int) error {
		return func(v1 any, _ bool, _ int) (err error) {
			var tsVec containers.Vector
			defer func() {
				if tsVec != nil {
					tsVec.Close()
					tsVec = nil
				}
			}()
			return vec.Foreach(func(v2 any, _ bool, row int) (err error) {
				// logutil.Infof("%v, %v, %d", v1, v2, row)
				if mask.Contains(uint64(row)) {
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
				startTS := txn.GetStartTS()
				if commitTS.Greater(&startTS) {
					return txnif.ErrTxnWWConflict
				}
				entry := common.TypeStringValue(*vec.GetType(), v1, false)
				return moerr.NewDuplicateEntryNoCtx(entry, def.Name)
			}, nil)
		}
	}
}
