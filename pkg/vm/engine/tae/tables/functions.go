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
)

var dedupPersistedFunctions = map[types.T]any{
	types.T_bool:       dedupPersistedFuncFactory[bool](compute.CompareBool),
	types.T_int8:       dedupPersistedOrderedFunc[int8],
	types.T_int16:      dedupPersistedOrderedFunc[int16],
	types.T_int32:      dedupPersistedOrderedFunc[int32],
	types.T_int64:      dedupPersistedOrderedFunc[int64],
	types.T_uint8:      dedupPersistedOrderedFunc[uint8],
	types.T_uint16:     dedupPersistedOrderedFunc[uint16],
	types.T_uint32:     dedupPersistedOrderedFunc[uint32],
	types.T_uint64:     dedupPersistedOrderedFunc[uint64],
	types.T_float32:    dedupPersistedOrderedFunc[float32],
	types.T_float64:    dedupPersistedOrderedFunc[float64],
	types.T_timestamp:  dedupPersistedOrderedFunc[types.Timestamp],
	types.T_date:       dedupPersistedOrderedFunc[types.Date],
	types.T_time:       dedupPersistedOrderedFunc[types.Time],
	types.T_datetime:   dedupPersistedOrderedFunc[types.Datetime],
	types.T_decimal64:  dedupPersistedFuncFactory[types.Decimal64](types.CompareDecimal64),
	types.T_decimal128: dedupPersistedFuncFactory[types.Decimal128](types.CompareDecimal128),
	types.T_decimal256: dedupPersistedFuncFactory[types.Decimal256](types.CompareDecimal256),
	types.T_TS:         dedupPersistedFuncFactory[types.TS](types.CompareTSTSAligned),
	types.T_Rowid:      dedupPersistedFuncFactory[types.Rowid](types.CompareRowidRowidAligned),
	types.T_Blockid:    dedupPersistedFuncFactory[types.Blockid](types.CompareBlockidBlockidAligned),
	types.T_uuid:       dedupPersistedFuncFactory[types.Uuid](types.CompareUuid),

	types.T_char:      dedupPersistedBytesFunc,
	types.T_varchar:   dedupPersistedBytesFunc,
	types.T_blob:      dedupPersistedBytesFunc,
	types.T_binary:    dedupPersistedBytesFunc,
	types.T_varbinary: dedupPersistedBytesFunc,
	types.T_json:      dedupPersistedBytesFunc,
	types.T_text:      dedupPersistedBytesFunc,
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

func dedupPersistedFuncFactory[T any](comp func(T, T) int64) func(args ...any) func(T, bool, int) error {
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

func dedupPersistedBytesFunc(args ...any) func([]byte, bool, int) error {
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

func dedupPersistedOrderedFunc[T types.OrderedT](args ...any) func(T, bool, int) error {
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

func dedupPersistedClosure(
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
