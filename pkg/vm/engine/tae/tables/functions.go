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

type Comparable[T any] interface {
	Compare(T, T) int
}

var dedupFunctions = map[types.T]any{
	types.T_bool:       dedupWithCompareFuncFactory[bool](compute.CompareBool),
	types.T_int8:       dedupOrderedFunc[int8],
	types.T_int16:      dedupOrderedFunc[int16],
	types.T_int32:      dedupOrderedFunc[int32],
	types.T_int64:      dedupOrderedFunc[int64],
	types.T_uint8:      dedupOrderedFunc[uint8],
	types.T_uint16:     dedupOrderedFunc[uint16],
	types.T_uint32:     dedupOrderedFunc[uint32],
	types.T_uint64:     dedupOrderedFunc[uint64],
	types.T_float32:    dedupOrderedFunc[float32],
	types.T_float64:    dedupOrderedFunc[float64],
	types.T_timestamp:  dedupOrderedFunc[types.Timestamp],
	types.T_date:       dedupOrderedFunc[types.Date],
	types.T_time:       dedupOrderedFunc[types.Time],
	types.T_datetime:   dedupOrderedFunc[types.Datetime],
	types.T_decimal64:  dedupWithCompareFuncFactory[types.Decimal64](types.CompareDecimal64),
	types.T_decimal128: dedupWithCompareFuncFactory[types.Decimal128](types.CompareDecimal128),
	types.T_decimal256: dedupWithCompareFuncFactory[types.Decimal256](types.CompareDecimal256),
	types.T_TS:         dedupWithCompareFuncFactory[types.TS](types.CompareTSTSAligned),
	types.T_Rowid:      dedupWithCompareFuncFactory[types.Rowid](types.CompareRowidRowidAligned),
	types.T_Blockid:    dedupWithCompareFuncFactory[types.Blockid](types.CompareBlockidBlockidAligned),
	types.T_uuid:       dedupWithCompareFuncFactory[types.Uuid](types.CompareUuid),

	types.T_char:      dedupBytesFunc,
	types.T_varchar:   dedupBytesFunc,
	types.T_blob:      dedupBytesFunc,
	types.T_binary:    dedupBytesFunc,
	types.T_varbinary: dedupBytesFunc,
	types.T_json:      dedupBytesFunc,
	types.T_text:      dedupBytesFunc,
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

func dedupWithCompareFuncFactory[T any](comp func(T, T) int64) func(args ...any) func(T, bool, int) error {
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

func dedupBytesFunc(args ...any) func([]byte, bool, int) error {
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

func dedupOrderedFunc[T types.OrderedT](args ...any) func(T, bool, int) error {
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

func dedupClosure(
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
