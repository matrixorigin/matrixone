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
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/compute"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"go.uber.org/zap"
)

var getDuplicatedRowIDNABlkFunctions = map[types.T]any{
	types.T_bool:       getDuplicateRowIDNABlkFuncFactory(compute.CompareBool),
	types.T_bit:        getDuplicatedRowIDNABlkOrderedFunc[uint64],
	types.T_int8:       getDuplicatedRowIDNABlkOrderedFunc[int8],
	types.T_int16:      getDuplicatedRowIDNABlkOrderedFunc[int16],
	types.T_int32:      getDuplicatedRowIDNABlkOrderedFunc[int32],
	types.T_int64:      getDuplicatedRowIDNABlkOrderedFunc[int64],
	types.T_uint8:      getDuplicatedRowIDNABlkOrderedFunc[uint8],
	types.T_uint16:     getDuplicatedRowIDNABlkOrderedFunc[uint16],
	types.T_uint32:     getDuplicatedRowIDNABlkOrderedFunc[uint32],
	types.T_uint64:     getDuplicatedRowIDNABlkOrderedFunc[uint64],
	types.T_float32:    getDuplicatedRowIDNABlkOrderedFunc[float32],
	types.T_float64:    getDuplicatedRowIDNABlkOrderedFunc[float64],
	types.T_timestamp:  getDuplicatedRowIDNABlkOrderedFunc[types.Timestamp],
	types.T_date:       getDuplicatedRowIDNABlkOrderedFunc[types.Date],
	types.T_time:       getDuplicatedRowIDNABlkOrderedFunc[types.Time],
	types.T_datetime:   getDuplicatedRowIDNABlkOrderedFunc[types.Datetime],
	types.T_enum:       getDuplicatedRowIDNABlkOrderedFunc[types.Enum],
	types.T_decimal64:  getDuplicateRowIDNABlkFuncFactory(types.CompareDecimal64),
	types.T_decimal128: getDuplicateRowIDNABlkFuncFactory(types.CompareDecimal128),
	types.T_decimal256: getDuplicateRowIDNABlkFuncFactory(types.CompareDecimal256),
	types.T_TS:         getDuplicateRowIDNABlkFuncFactory(types.CompareTSTSAligned),
	types.T_Rowid:      getDuplicateRowIDNABlkFuncFactory(types.CompareRowidRowidAligned),
	types.T_Blockid:    getDuplicateRowIDNABlkFuncFactory(types.CompareBlockidBlockidAligned),
	types.T_uuid:       getDuplicateRowIDNABlkFuncFactory(types.CompareUuid),

	types.T_char:      getDuplicatedRowIDsNABlkBytesFunc,
	types.T_varchar:   getDuplicatedRowIDsNABlkBytesFunc,
	types.T_blob:      getDuplicatedRowIDsNABlkBytesFunc,
	types.T_binary:    getDuplicatedRowIDsNABlkBytesFunc,
	types.T_varbinary: getDuplicatedRowIDsNABlkBytesFunc,
	types.T_json:      getDuplicatedRowIDsNABlkBytesFunc,
	types.T_text:      getDuplicatedRowIDsNABlkBytesFunc,
	types.T_datalink:  getDuplicatedRowIDsNABlkBytesFunc,

	types.T_array_float32: getDuplicatedRowIDsNABlkBytesFunc,
	types.T_array_float64: getDuplicatedRowIDsNABlkBytesFunc,
}

var getRowIDAlkFunctions = map[types.T]any{
	types.T_bool:       getDuplicatedRowIDABlkFuncFactory(compute.CompareBool),
	types.T_bit:        getDuplicatedRowIDABlkFuncFactory(compute.CompareOrdered[uint64]),
	types.T_int8:       getDuplicatedRowIDABlkFuncFactory(compute.CompareOrdered[int8]),
	types.T_int16:      getDuplicatedRowIDABlkFuncFactory(compute.CompareOrdered[int16]),
	types.T_int32:      getDuplicatedRowIDABlkFuncFactory(compute.CompareOrdered[int32]),
	types.T_int64:      getDuplicatedRowIDABlkFuncFactory(compute.CompareOrdered[int64]),
	types.T_uint8:      getDuplicatedRowIDABlkFuncFactory(compute.CompareOrdered[uint8]),
	types.T_uint16:     getDuplicatedRowIDABlkFuncFactory(compute.CompareOrdered[uint16]),
	types.T_uint32:     getDuplicatedRowIDABlkFuncFactory(compute.CompareOrdered[uint32]),
	types.T_uint64:     getDuplicatedRowIDABlkFuncFactory(compute.CompareOrdered[uint64]),
	types.T_float32:    getDuplicatedRowIDABlkFuncFactory(compute.CompareOrdered[float32]),
	types.T_float64:    getDuplicatedRowIDABlkFuncFactory(compute.CompareOrdered[float64]),
	types.T_timestamp:  getDuplicatedRowIDABlkFuncFactory(compute.CompareOrdered[types.Timestamp]),
	types.T_date:       getDuplicatedRowIDABlkFuncFactory(compute.CompareOrdered[types.Date]),
	types.T_time:       getDuplicatedRowIDABlkFuncFactory(compute.CompareOrdered[types.Time]),
	types.T_datetime:   getDuplicatedRowIDABlkFuncFactory(compute.CompareOrdered[types.Datetime]),
	types.T_enum:       getDuplicatedRowIDABlkFuncFactory(compute.CompareOrdered[types.Enum]),
	types.T_decimal64:  getDuplicatedRowIDABlkFuncFactory(types.CompareDecimal64),
	types.T_decimal128: getDuplicatedRowIDABlkFuncFactory(types.CompareDecimal128),
	types.T_decimal256: getDuplicatedRowIDABlkFuncFactory(types.CompareDecimal256),
	types.T_TS:         getDuplicatedRowIDABlkFuncFactory(types.CompareTSTSAligned),
	types.T_Rowid:      getDuplicatedRowIDABlkFuncFactory(types.CompareRowidRowidAligned),
	types.T_Blockid:    getDuplicatedRowIDABlkFuncFactory(types.CompareBlockidBlockidAligned),
	types.T_uuid:       getDuplicatedRowIDABlkFuncFactory(types.CompareUuid),

	types.T_char:      getDuplicatedRowIDABlkBytesFunc,
	types.T_varchar:   getDuplicatedRowIDABlkBytesFunc,
	types.T_blob:      getDuplicatedRowIDABlkBytesFunc,
	types.T_binary:    getDuplicatedRowIDABlkBytesFunc,
	types.T_varbinary: getDuplicatedRowIDABlkBytesFunc,
	types.T_json:      getDuplicatedRowIDABlkBytesFunc,
	types.T_text:      getDuplicatedRowIDABlkBytesFunc,
	types.T_datalink:  getDuplicatedRowIDABlkBytesFunc,

	types.T_array_float32: getDuplicatedRowIDABlkBytesFunc,
	types.T_array_float64: getDuplicatedRowIDABlkBytesFunc,
}
var containsNABlkFunctions = map[types.T]any{
	types.T_Rowid: containsNABlkFuncFactory(types.CompareRowidRowidAligned),
}

var containsAlkFunctions = map[types.T]any{
	types.T_Rowid: containsABlkFuncFactory(types.CompareRowidRowidAligned),
}

func parseNAGetDuplicatedArgs(args ...any) (vec *vector.Vector, rowIDs containers.Vector, blkID *types.Blockid) {
	vec = args[0].(containers.Vector).GetDownstreamVector()
	if args[1] != nil {
		rowIDs = args[1].(containers.Vector)
	}
	if args[2] != nil {
		blkID = args[2].(*types.Blockid)
	}
	return
}

func parseNAContainsArgs(args ...any) (vec *vector.Vector, rowIDs containers.Vector) {
	vec = args[0].(containers.Vector).GetDownstreamVector()
	if args[1] != nil {
		rowIDs = args[1].(containers.Vector)
	}
	return
}

func parseAGetDuplicateRowIDsArgs(args ...any) (
	vec containers.Vector, rowIDs containers.Vector, blkID *types.Blockid, maxRow uint32,
	scanFn func(uint16) (vec containers.Vector, err error), txn txnif.TxnReader, skipCommittedBeforeTxnForAblk bool,
) {
	vec = args[0].(containers.Vector)
	if args[1] != nil {
		rowIDs = args[1].(containers.Vector)
	}
	if args[2] != nil {
		blkID = args[2].(*types.Blockid)
	}
	if args[3] != nil {
		maxRow = args[3].(uint32)
	}
	if args[4] != nil {
		scanFn = args[4].(func(bid uint16) (vec containers.Vector, err error))
	}
	if args[5] != nil {
		txn = args[5].(txnif.TxnReader)
	}
	if args[6] != nil {
		skipCommittedBeforeTxnForAblk = args[6].(bool)
	}
	return
}

func parseAContainsArgs(args ...any) (
	vec containers.Vector, rowIDs containers.Vector,
	scanFn func(uint16) (vec containers.Vector, err error), txn txnif.TxnReader, delsFn func(rowID any, ts types.TS) (types.TS, error),
) {
	vec = args[0].(containers.Vector)
	if args[1] != nil {
		rowIDs = args[1].(containers.Vector)
	}
	if args[2] != nil {
		scanFn = args[2].(func(bid uint16) (vec containers.Vector, err error))
	}
	if args[3] != nil {
		txn = args[3].(txnif.TxnReader)
	}
	if args[4] != nil {
		delsFn = args[4].(func(rowID any, ts types.TS) (types.TS, error))
	}
	return
}

func getDuplicateRowIDNABlkFuncFactory[T any](comp func(T, T) int) func(args ...any) func(T, bool, int) error {
	return func(args ...any) func(T, bool, int) error {
		vec, rowIDs, blkID := parseNAGetDuplicatedArgs(args...)
		vs := vector.MustFixedColNoTypeCheck[T](vec)
		return func(v T, _ bool, row int) (err error) {
			// logutil.Infof("row=%d,v=%v", row, v)
			if !rowIDs.IsNull(row) {
				return
			}
			if offset, existed := compute.GetOffsetWithFunc(
				vs,
				v,
				comp,
				nil,
			); existed {
				rowID := objectio.NewRowid(blkID, uint32(offset))
				rowIDs.Update(row, *rowID, false)
			}
			return
		}
	}
}

func containsNABlkFuncFactory[T any](comp func(T, T) int) func(args ...any) func(T, bool, int) error {
	return func(args ...any) func(T, bool, int) error {
		vec, rowIDs := parseNAContainsArgs(args...)
		vs := vector.MustFixedColNoTypeCheck[T](vec)
		return func(v T, isNull bool, row int) (err error) {
			// logutil.Infof("row=%d,v=%v", row, v)
			if rowIDs.IsNull(row) {
				return
			}
			if _, existed := compute.GetOffsetWithFunc(
				vs,
				v,
				comp,
				nil,
			); existed {
				rowIDs.Update(row, nil, true)
			}
			return
		}
	}
}

func getDuplicatedRowIDsNABlkBytesFunc(args ...any) func([]byte, bool, int) error {
	vec, rowIDs, blkID := parseNAGetDuplicatedArgs(args...)
	return func(v []byte, _ bool, row int) (err error) {
		// logutil.Infof("row=%d,v=%v", row, v)
		if !rowIDs.IsNull(row) {
			return
		}
		if offset, existed := compute.GetOffsetOfBytes(
			vec,
			v,
			nil,
		); existed {
			rowID := objectio.NewRowid(blkID, uint32(offset))
			rowIDs.Update(row, *rowID, false)
		}
		return
	}
}

func getDuplicatedRowIDNABlkOrderedFunc[T types.OrderedT](args ...any) func(T, bool, int) error {
	vec, rowIDs, blkID := parseNAGetDuplicatedArgs(args...)
	vs := vector.MustFixedColNoTypeCheck[T](vec)
	return func(v T, _ bool, row int) (err error) {
		// logutil.Infof("row=%d,v=%v", row, v)
		if !rowIDs.IsNull(row) {
			return
		}
		if offset, existed := compute.GetOffsetOfOrdered(
			vs,
			v,
			nil,
		); existed {
			rowID := objectio.NewRowid(blkID, uint32(offset))
			rowIDs.Update(row, *rowID, false)
		}
		return
	}
}

func getDuplicatedRowIDABlkBytesFunc(args ...any) func([]byte, bool, int) error {
	vec, rowIDs, blkID, maxRow, scanFn, txn, skip := parseAGetDuplicateRowIDsArgs(args...)
	return func(v1 []byte, _ bool, rowOffset int) error {
		if !rowIDs.IsNull(rowOffset) {
			return nil
		}
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
			true,
			func(v2 []byte, _ bool, row int) (err error) {
				// logutil.Infof("row=%d,v1=%v,v2=%v", row, v1, v2)
				if row > int(maxRow) {
					return
				}
				if !rowIDs.IsNull(rowOffset) {
					return nil
				}
				if compute.CompareBytes(v1, v2) != 0 {
					return
				}
				if tsVec == nil {
					tsVec, err = scanFn(0)
					if err != nil {
						return err
					}
				}
				commitTS := vector.GetFixedAtNoTypeCheck[types.TS](tsVec.GetDownstreamVector(), row)
				startTS := txn.GetStartTS()
				if commitTS.GT(&startTS) {
					logutil.Info("Dedup-WW",
						zap.String("txn", txn.Repr()),
						zap.Int("row offset", row),
						zap.String("commit ts", commitTS.ToString()),
					)
					return txnif.ErrTxnWWConflict
				}
				if skip && commitTS.LT(&startTS) {
					return nil
				}
				rowID := objectio.NewRowid(blkID, uint32(row))
				rowIDs.Update(rowOffset, *rowID, false)
				return nil
			}, nil, nil)
	}
}

func getDuplicatedRowIDABlkFuncFactory[T types.FixedSizeT](comp func(T, T) int) func(args ...any) func(T, bool, int) error {
	return func(args ...any) func(T, bool, int) error {
		vec, rowIDs, blkID, maxVisibleRow, scanFn, txn, skip := parseAGetDuplicateRowIDsArgs(args...)
		return func(v1 T, _ bool, rowOffset int) error {
			if !rowIDs.IsNull(rowOffset) {
				return nil
			}
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
				true,
				func(v2 T, _ bool, row int) (err error) {
					if row > int(maxVisibleRow) {
						return
					}
					if !rowIDs.IsNull(rowOffset) {
						return nil
					}
					if comp(v1, v2) != 0 {
						return
					}
					if tsVec == nil {
						tsVec, err = scanFn(0)
						if err != nil {
							return err
						}
					}
					commitTS := tsVec.Get(row).(types.TS)
					startTS := txn.GetStartTS()
					if commitTS.GT(&startTS) {
						logutil.Info("Dedup-WW",
							zap.String("txn", txn.Repr()),
							zap.Int("row offset", row),
							zap.String("commit ts", commitTS.ToString()),
						)
						return txnif.ErrTxnWWConflict
					}
					if skip && commitTS.LT(&startTS) {
						return nil
					}
					rowID := objectio.NewRowid(blkID, uint32(row))
					rowIDs.Update(rowOffset, *rowID, false)
					return nil
				}, nil, nil)
		}
	}
}

func containsABlkFuncFactory[T types.FixedSizeT](comp func(T, T) int) func(args ...any) func(T, bool, int) error {
	return func(args ...any) func(T, bool, int) error {
		vec, rowIDs, scanFn, txn, delsFn := parseAContainsArgs(args...)
		vs := vector.MustFixedColNoTypeCheck[T](vec.GetDownstreamVector())
		return func(v1 T, _ bool, rowOffset int) error {
			if rowIDs.IsNull(rowOffset) {
				return nil
			}
			var tsVec containers.Vector
			defer func() {
				if tsVec != nil {
					tsVec.Close()
					tsVec = nil
				}
			}()
			if row, existed := compute.GetOffsetWithFunc(
				vs,
				v1,
				comp,
				nil,
			); existed {
				if tsVec == nil {
					var err error
					tsVec, err = scanFn(0)
					if err != nil {
						return err
					}
				}
				rowIDs.Update(rowOffset, nil, true)
				commitTS := tsVec.Get(row).(types.TS)
				startTS := txn.GetStartTS()
				if commitTS.GT(&startTS) {
					ts, err := delsFn(v1, commitTS)
					if err != nil {
						return err
					}
					if ts.GT(&startTS) {
						logutil.Info("Dedup-WW",
							zap.String("txn", txn.Repr()),
							zap.Int("row offset", row),
							zap.String("commit ts", commitTS.ToString()),
							zap.String("original commit ts", ts.ToString()),
						)
						return txnif.ErrTxnWWConflict
					}
				}
			}
			return nil
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
				if commitTS.GT(&startTS) {
					logutil.Info("Dedup-WW",
						zap.String("txn", txn.Repr()),
						zap.Int("row offset", row),
						zap.String("commit ts", commitTS.ToString()),
					)
					return txnif.ErrTxnWWConflict
				}
				entry := common.TypeStringValue(*vec.GetType(), v1, false)
				return moerr.NewDuplicateEntryNoCtx(entry, def.Name)
			}, nil)
		}
	}
}
