// Copyright 2021-2024 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package readutil

import (
	"bytes"
	"sort"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"go.uber.org/zap"
)

/* Don't remove me. will be used lated
func DirectConstructBlockPKFilter(
	isFakePK bool,
	val []byte,
	inVec *vector.Vector,
	oid types.T,
) (f objectio.BlockReadFilter, err error) {
	var base BasePKFilter
	if inVec != nil {
		base.Op = function.IN
		base.Vec = inVec
		base.Oid = oid
	} else {
		base.Op = function.EQUAL
		base.LB = val
		base.Oid = oid
	}
	base.Valid = true
	return ConstructBlockPKFilter(isFakePK, base)
}
*/

func ConstructBlockPKFilter(
	isFakePK bool,
	basePKFilter BasePKFilter,
) (objectio.BlockReadFilter, error) {
	if !basePKFilter.Valid {
		return objectio.BlockReadFilter{}, nil
	}

	readFilter := objectio.BlockReadFilter{
		HasFakePK: isFakePK,
	}

	disjuncts := basePKFilter.Disjuncts
	if len(disjuncts) == 0 {
		disjuncts = []BasePKFilter{basePKFilter}
	}

	var (
		sortedMissing bool
		unsMissing    bool
		sortedFuncs   []func(*vector.Vector) []int64
		unsFuncs      []func(*vector.Vector) []int64
	)

	for idx := range disjuncts {
		sortedFunc, unsortedFunc, err := buildBlockPKSearchFuncs(disjuncts[idx])
		if err != nil {
			return objectio.BlockReadFilter{}, err
		}
		if sortedFunc == nil {
			sortedMissing = true
		} else {
			sortedFuncs = append(sortedFuncs, sortedFunc)
		}
		if unsortedFunc == nil {
			unsMissing = true
		} else {
			unsFuncs = append(unsFuncs, unsortedFunc)
		}
	}

	if !sortedMissing && len(sortedFuncs) > 0 {
		readFilter.SortedSearchFunc = combineOffsetFuncs(sortedFuncs)
	}
	if !unsMissing && len(unsFuncs) > 0 {
		readFilter.UnSortedSearchFunc = combineOffsetFuncs(unsFuncs)
	}

	if readFilter.SortedSearchFunc == nil && readFilter.UnSortedSearchFunc == nil {
		logutil.Warn("ConstructBlockPKFilter skipped data type",
			zap.Int("expr op", basePKFilter.Op),
			zap.String("data type", basePKFilter.Oid.String()))
		return readFilter, nil
	}

	readFilter.Valid = true
	return readFilter, nil
}

func buildBlockPKSearchFuncs(basePKFilter BasePKFilter) (sorted func(*vector.Vector) []int64, unSorted func(*vector.Vector) []int64, err error) {
	switch basePKFilter.Op {
	case function.EQUAL:
		switch basePKFilter.Oid {
		case types.T_int8:
			sorted = vector.OrderedBinarySearchOffsetByValFactory([]int8{types.DecodeInt8(basePKFilter.LB)})
			unSorted = vector.OrderedLinearSearchOffsetByValFactory([]int8{types.DecodeInt8(basePKFilter.LB)}, nil)
		case types.T_int16:
			sorted = vector.OrderedBinarySearchOffsetByValFactory([]int16{types.DecodeInt16(basePKFilter.LB)})
			unSorted = vector.OrderedLinearSearchOffsetByValFactory([]int16{types.DecodeInt16(basePKFilter.LB)}, nil)
		case types.T_int32:
			sorted = vector.OrderedBinarySearchOffsetByValFactory([]int32{types.DecodeInt32(basePKFilter.LB)})
			unSorted = vector.OrderedLinearSearchOffsetByValFactory([]int32{types.DecodeInt32(basePKFilter.LB)}, nil)
		case types.T_int64:
			sorted = vector.OrderedBinarySearchOffsetByValFactory([]int64{types.DecodeInt64(basePKFilter.LB)})
			unSorted = vector.OrderedLinearSearchOffsetByValFactory([]int64{types.DecodeInt64(basePKFilter.LB)}, nil)
		case types.T_float32:
			sorted = vector.OrderedBinarySearchOffsetByValFactory([]float32{types.DecodeFloat32(basePKFilter.LB)})
			unSorted = vector.OrderedLinearSearchOffsetByValFactory([]float32{types.DecodeFloat32(basePKFilter.LB)}, nil)
		case types.T_float64:
			sorted = vector.OrderedBinarySearchOffsetByValFactory([]float64{types.DecodeFloat64(basePKFilter.LB)})
			unSorted = vector.OrderedLinearSearchOffsetByValFactory([]float64{types.DecodeFloat64(basePKFilter.LB)}, nil)
		case types.T_uint8:
			sorted = vector.OrderedBinarySearchOffsetByValFactory([]uint8{uint8(types.DecodeUint8(basePKFilter.LB))})
			unSorted = vector.OrderedLinearSearchOffsetByValFactory([]uint8{uint8(types.DecodeUint8(basePKFilter.LB))}, nil)
		case types.T_uint16:
			sorted = vector.OrderedBinarySearchOffsetByValFactory([]uint16{uint16(types.DecodeUint16(basePKFilter.LB))})
			unSorted = vector.OrderedLinearSearchOffsetByValFactory([]uint16{uint16(types.DecodeUint16(basePKFilter.LB))}, nil)
		case types.T_uint32:
			sorted = vector.OrderedBinarySearchOffsetByValFactory([]uint32{types.DecodeUint32(basePKFilter.LB)})
			unSorted = vector.OrderedLinearSearchOffsetByValFactory([]uint32{types.DecodeUint32(basePKFilter.LB)}, nil)
		case types.T_uint64:
			sorted = vector.OrderedBinarySearchOffsetByValFactory([]uint64{types.DecodeUint64(basePKFilter.LB)})
			unSorted = vector.OrderedLinearSearchOffsetByValFactory([]uint64{types.DecodeUint64(basePKFilter.LB)}, nil)
		case types.T_date:
			sorted = vector.OrderedBinarySearchOffsetByValFactory([]types.Date{types.DecodeDate(basePKFilter.LB)})
			unSorted = vector.OrderedLinearSearchOffsetByValFactory([]types.Date{types.DecodeDate(basePKFilter.LB)}, nil)
		case types.T_time:
			sorted = vector.OrderedBinarySearchOffsetByValFactory([]types.Time{types.DecodeTime(basePKFilter.LB)})
			unSorted = vector.OrderedLinearSearchOffsetByValFactory([]types.Time{types.DecodeTime(basePKFilter.LB)}, nil)
		case types.T_datetime:
			sorted = vector.OrderedBinarySearchOffsetByValFactory([]types.Datetime{types.DecodeDatetime(basePKFilter.LB)})
			unSorted = vector.OrderedLinearSearchOffsetByValFactory([]types.Datetime{types.DecodeDatetime(basePKFilter.LB)}, nil)
		case types.T_timestamp:
			sorted = vector.OrderedBinarySearchOffsetByValFactory([]types.Timestamp{types.DecodeTimestamp(basePKFilter.LB)})
			unSorted = vector.OrderedLinearSearchOffsetByValFactory([]types.Timestamp{types.DecodeTimestamp(basePKFilter.LB)}, nil)
		case types.T_decimal64:
			sorted = vector.FixedSizedBinarySearchOffsetByValFactory([]types.Decimal64{types.DecodeDecimal64(basePKFilter.LB)}, types.CompareDecimal64)
			unSorted = vector.FixedSizeLinearSearchOffsetByValFactory([]types.Decimal64{types.DecodeDecimal64(basePKFilter.LB)}, types.CompareDecimal64)
		case types.T_decimal128:
			sorted = vector.FixedSizedBinarySearchOffsetByValFactory([]types.Decimal128{types.DecodeDecimal128(basePKFilter.LB)}, types.CompareDecimal128)
			unSorted = vector.FixedSizeLinearSearchOffsetByValFactory([]types.Decimal128{types.DecodeDecimal128(basePKFilter.LB)}, types.CompareDecimal128)
		case types.T_varchar, types.T_char, types.T_binary, types.T_json:
			sorted = vector.VarlenBinarySearchOffsetByValFactory([][]byte{basePKFilter.LB})
			unSorted = vector.VarlenLinearSearchOffsetByValFactory([][]byte{basePKFilter.LB})
		case types.T_enum:
			sorted = vector.OrderedBinarySearchOffsetByValFactory([]types.Enum{types.DecodeEnum(basePKFilter.LB)})
			unSorted = vector.OrderedLinearSearchOffsetByValFactory([]types.Enum{types.DecodeEnum(basePKFilter.LB)}, nil)
		case types.T_uuid:
			sorted = vector.FixedSizedBinarySearchOffsetByValFactory([]types.Uuid{types.Uuid(basePKFilter.LB)}, types.CompareUuid)
			unSorted = vector.FixedSizeLinearSearchOffsetByValFactory([]types.Uuid{types.Uuid(basePKFilter.LB)}, types.CompareUuid)
		default:
		}

	case function.PREFIX_EQ:
		sorted = vector.CollectOffsetsByPrefixEqFactory(basePKFilter.LB)
		unSorted = vector.LinearCollectOffsetsByPrefixEqFactory(basePKFilter.LB)

	case function.PREFIX_BETWEEN:
		sorted = vector.CollectOffsetsByPrefixBetweenFactory(basePKFilter.LB, basePKFilter.UB)
		unSorted = vector.LinearCollectOffsetsByPrefixBetweenFactory(basePKFilter.LB, basePKFilter.UB)

	case function.IN:
		vec := basePKFilter.Vec

		switch vec.GetType().Oid {
		case types.T_bit:
			sorted = vector.OrderedBinarySearchOffsetByValFactory(vector.MustFixedColNoTypeCheck[uint64](vec))
			unSorted = vector.OrderedLinearSearchOffsetByValFactory(vector.MustFixedColNoTypeCheck[uint64](vec), nil)
		case types.T_int8:
			sorted = vector.OrderedBinarySearchOffsetByValFactory(vector.MustFixedColNoTypeCheck[int8](vec))
			unSorted = vector.OrderedLinearSearchOffsetByValFactory(vector.MustFixedColNoTypeCheck[int8](vec), nil)
		case types.T_int16:
			sorted = vector.OrderedBinarySearchOffsetByValFactory(vector.MustFixedColNoTypeCheck[int16](vec))
			unSorted = vector.OrderedLinearSearchOffsetByValFactory(vector.MustFixedColNoTypeCheck[int16](vec), nil)
		case types.T_int32:
			sorted = vector.OrderedBinarySearchOffsetByValFactory(vector.MustFixedColNoTypeCheck[int32](vec))
			unSorted = vector.OrderedLinearSearchOffsetByValFactory(vector.MustFixedColNoTypeCheck[int32](vec), nil)
		case types.T_int64:
			sorted = vector.OrderedBinarySearchOffsetByValFactory(vector.MustFixedColNoTypeCheck[int64](vec))
			unSorted = vector.OrderedLinearSearchOffsetByValFactory(vector.MustFixedColNoTypeCheck[int64](vec), nil)
		case types.T_uint8:
			sorted = vector.OrderedBinarySearchOffsetByValFactory(vector.MustFixedColNoTypeCheck[uint8](vec))
			unSorted = vector.OrderedLinearSearchOffsetByValFactory(vector.MustFixedColNoTypeCheck[uint8](vec), nil)
		case types.T_uint16:
			sorted = vector.OrderedBinarySearchOffsetByValFactory(vector.MustFixedColNoTypeCheck[uint16](vec))
			unSorted = vector.OrderedLinearSearchOffsetByValFactory(vector.MustFixedColNoTypeCheck[uint16](vec), nil)
		case types.T_uint32:
			sorted = vector.OrderedBinarySearchOffsetByValFactory(vector.MustFixedColNoTypeCheck[uint32](vec))
			unSorted = vector.OrderedLinearSearchOffsetByValFactory(vector.MustFixedColNoTypeCheck[uint32](vec), nil)
		case types.T_uint64:
			sorted = vector.OrderedBinarySearchOffsetByValFactory(vector.MustFixedColNoTypeCheck[uint64](vec))
			unSorted = vector.OrderedLinearSearchOffsetByValFactory(vector.MustFixedColNoTypeCheck[uint64](vec), nil)
		case types.T_float32:
			sorted = vector.OrderedBinarySearchOffsetByValFactory(vector.MustFixedColNoTypeCheck[float32](vec))
			unSorted = vector.OrderedLinearSearchOffsetByValFactory(vector.MustFixedColNoTypeCheck[float32](vec), nil)
		case types.T_float64:
			sorted = vector.OrderedBinarySearchOffsetByValFactory(vector.MustFixedColNoTypeCheck[float64](vec))
			unSorted = vector.OrderedLinearSearchOffsetByValFactory(vector.MustFixedColNoTypeCheck[float64](vec), nil)
		case types.T_date:
			sorted = vector.OrderedBinarySearchOffsetByValFactory(vector.MustFixedColNoTypeCheck[types.Date](vec))
			unSorted = vector.OrderedLinearSearchOffsetByValFactory(vector.MustFixedColNoTypeCheck[types.Date](vec), nil)
		case types.T_time:
			sorted = vector.OrderedBinarySearchOffsetByValFactory(vector.MustFixedColNoTypeCheck[types.Time](vec))
			unSorted = vector.OrderedLinearSearchOffsetByValFactory(vector.MustFixedColNoTypeCheck[types.Time](vec), nil)
		case types.T_datetime:
			sorted = vector.OrderedBinarySearchOffsetByValFactory(vector.MustFixedColNoTypeCheck[types.Datetime](vec))
			unSorted = vector.OrderedLinearSearchOffsetByValFactory(vector.MustFixedColNoTypeCheck[types.Datetime](vec), nil)
		case types.T_timestamp:
			sorted = vector.OrderedBinarySearchOffsetByValFactory(vector.MustFixedColNoTypeCheck[types.Timestamp](vec))
			unSorted = vector.OrderedLinearSearchOffsetByValFactory(vector.MustFixedColNoTypeCheck[types.Timestamp](vec), nil)
		case types.T_decimal64:
			sorted = vector.FixedSizedBinarySearchOffsetByValFactory(vector.MustFixedColNoTypeCheck[types.Decimal64](vec), types.CompareDecimal64)
			unSorted = vector.FixedSizeLinearSearchOffsetByValFactory(vector.MustFixedColNoTypeCheck[types.Decimal64](vec), types.CompareDecimal64)
		case types.T_decimal128:
			sorted = vector.FixedSizedBinarySearchOffsetByValFactory(vector.MustFixedColNoTypeCheck[types.Decimal128](vec), types.CompareDecimal128)
			unSorted = vector.FixedSizeLinearSearchOffsetByValFactory(vector.MustFixedColNoTypeCheck[types.Decimal128](vec), types.CompareDecimal128)
		case types.T_char, types.T_varchar, types.T_binary, types.T_varbinary, types.T_json, types.T_blob, types.T_text,
			types.T_array_float32, types.T_array_float64, types.T_datalink:
			sorted = vector.VarlenBinarySearchOffsetByValFactory(vector.InefficientMustBytesCol(vec))
			unSorted = vector.VarlenLinearSearchOffsetByValFactory(vector.InefficientMustBytesCol(vec))
		case types.T_enum:
			sorted = vector.OrderedBinarySearchOffsetByValFactory(vector.MustFixedColNoTypeCheck[types.Enum](vec))
			unSorted = vector.OrderedLinearSearchOffsetByValFactory(vector.MustFixedColNoTypeCheck[types.Enum](vec), nil)
		case types.T_uuid:
			sorted = vector.FixedSizedBinarySearchOffsetByValFactory(vector.MustFixedColNoTypeCheck[types.Uuid](vec), types.CompareUuid)
			unSorted = vector.FixedSizeLinearSearchOffsetByValFactory(vector.MustFixedColNoTypeCheck[types.Uuid](vec), types.CompareUuid)
		default:
		}

	case function.PREFIX_IN:
		vec := basePKFilter.Vec

		sorted = vector.CollectOffsetsByPrefixInFactory(vec)
		unSorted = vector.LinearCollectOffsetsByPrefixInFactory(vec)

	case function.LESS_EQUAL, function.LESS_THAN:
		closed := basePKFilter.Op == function.LESS_EQUAL
		switch basePKFilter.Oid {
		case types.T_int8:
			sorted = vector.OrderedSearchOffsetsByLess(types.DecodeInt8(basePKFilter.LB), closed, true)
			unSorted = vector.OrderedSearchOffsetsByLess(types.DecodeInt8(basePKFilter.LB), closed, false)
		case types.T_int16:
			sorted = vector.OrderedSearchOffsetsByLess(types.DecodeInt16(basePKFilter.LB), closed, true)
			unSorted = vector.OrderedSearchOffsetsByLess(types.DecodeInt16(basePKFilter.LB), closed, false)
		case types.T_int32:
			sorted = vector.OrderedSearchOffsetsByLess(types.DecodeInt32(basePKFilter.LB), closed, true)
			unSorted = vector.OrderedSearchOffsetsByLess(types.DecodeInt32(basePKFilter.LB), closed, false)
		case types.T_int64:
			sorted = vector.OrderedSearchOffsetsByLess(types.DecodeInt64(basePKFilter.LB), closed, true)
			unSorted = vector.OrderedSearchOffsetsByLess(types.DecodeInt64(basePKFilter.LB), closed, false)
		case types.T_float32:
			sorted = vector.OrderedSearchOffsetsByLess(types.DecodeFloat32(basePKFilter.LB), closed, true)
			unSorted = vector.OrderedSearchOffsetsByLess(types.DecodeFloat32(basePKFilter.LB), closed, false)
		case types.T_float64:
			sorted = vector.OrderedSearchOffsetsByLess(types.DecodeFloat64(basePKFilter.LB), closed, true)
			unSorted = vector.OrderedSearchOffsetsByLess(types.DecodeFloat64(basePKFilter.LB), closed, false)
		case types.T_uint8:
			sorted = vector.OrderedSearchOffsetsByLess(types.DecodeUint8(basePKFilter.LB), closed, true)
			unSorted = vector.OrderedSearchOffsetsByLess(types.DecodeUint8(basePKFilter.LB), closed, false)
		case types.T_uint16:
			sorted = vector.OrderedSearchOffsetsByLess(types.DecodeUint16(basePKFilter.LB), closed, true)
			unSorted = vector.OrderedSearchOffsetsByLess(types.DecodeUint16(basePKFilter.LB), closed, false)
		case types.T_uint32:
			sorted = vector.OrderedSearchOffsetsByLess(types.DecodeUint32(basePKFilter.LB), closed, true)
			unSorted = vector.OrderedSearchOffsetsByLess(types.DecodeUint32(basePKFilter.LB), closed, false)
		case types.T_uint64:
			sorted = vector.OrderedSearchOffsetsByLess(types.DecodeUint64(basePKFilter.LB), closed, true)
			unSorted = vector.OrderedSearchOffsetsByLess(types.DecodeUint64(basePKFilter.LB), closed, false)
		case types.T_date:
			sorted = vector.OrderedSearchOffsetsByLess(types.DecodeDate(basePKFilter.LB), closed, true)
			unSorted = vector.OrderedSearchOffsetsByLess(types.DecodeDate(basePKFilter.LB), closed, false)
		case types.T_time:
			sorted = vector.OrderedSearchOffsetsByLess(types.DecodeTime(basePKFilter.LB), closed, true)
			unSorted = vector.OrderedSearchOffsetsByLess(types.DecodeTime(basePKFilter.LB), closed, false)
		case types.T_datetime:
			sorted = vector.OrderedSearchOffsetsByLess(types.DecodeDatetime(basePKFilter.LB), closed, true)
			unSorted = vector.OrderedSearchOffsetsByLess(types.DecodeDatetime(basePKFilter.LB), closed, false)
		case types.T_timestamp:
			sorted = vector.OrderedSearchOffsetsByLess(types.DecodeTimestamp(basePKFilter.LB), closed, true)
			unSorted = vector.OrderedSearchOffsetsByLess(types.DecodeTimestamp(basePKFilter.LB), closed, false)
		case types.T_decimal64:
			sorted = vector.FixedSizeSearchOffsetsByLessTypeChecked(types.DecodeDecimal64(basePKFilter.LB), closed, true, types.CompareDecimal64)
			unSorted = vector.FixedSizeSearchOffsetsByLessTypeChecked(types.DecodeDecimal64(basePKFilter.LB), closed, false, types.CompareDecimal64)
		case types.T_decimal128:
			sorted = vector.FixedSizeSearchOffsetsByLessTypeChecked(types.DecodeDecimal128(basePKFilter.LB), closed, true, types.CompareDecimal128)
			unSorted = vector.FixedSizeSearchOffsetsByLessTypeChecked(types.DecodeDecimal128(basePKFilter.LB), closed, false, types.CompareDecimal128)
		case types.T_varchar, types.T_char, types.T_binary, types.T_json:
			sorted = vector.VarlenSearchOffsetByLess(basePKFilter.LB, closed, true)
			unSorted = vector.VarlenSearchOffsetByLess(basePKFilter.LB, closed, false)
		case types.T_enum:
			sorted = vector.OrderedSearchOffsetsByLess(types.DecodeEnum(basePKFilter.LB), closed, true)
			unSorted = vector.OrderedSearchOffsetsByLess(types.DecodeEnum(basePKFilter.LB), closed, false)
		case types.T_uuid:
			sorted = vector.FixedSizeSearchOffsetsByLessTypeChecked(types.Uuid(basePKFilter.LB), closed, true, types.CompareUuid)
			unSorted = vector.FixedSizeSearchOffsetsByLessTypeChecked(types.Uuid(basePKFilter.LB), closed, false, types.CompareUuid)
		default:
		}

	case function.GREAT_EQUAL, function.GREAT_THAN:
		closed := basePKFilter.Op == function.GREAT_EQUAL
		switch basePKFilter.Oid {
		case types.T_int8:
			sorted = vector.OrderedSearchOffsetsByGreat(types.DecodeInt8(basePKFilter.LB), closed, true)
			unSorted = vector.OrderedSearchOffsetsByGreat(types.DecodeInt8(basePKFilter.LB), closed, false)
		case types.T_int16:
			sorted = vector.OrderedSearchOffsetsByGreat(types.DecodeInt16(basePKFilter.LB), closed, true)
			unSorted = vector.OrderedSearchOffsetsByGreat(types.DecodeInt16(basePKFilter.LB), closed, false)
		case types.T_int32:
			sorted = vector.OrderedSearchOffsetsByGreat(types.DecodeInt32(basePKFilter.LB), closed, true)
			unSorted = vector.OrderedSearchOffsetsByGreat(types.DecodeInt32(basePKFilter.LB), closed, false)
		case types.T_int64:
			sorted = vector.OrderedSearchOffsetsByGreat(types.DecodeInt64(basePKFilter.LB), closed, true)
			unSorted = vector.OrderedSearchOffsetsByGreat(types.DecodeInt64(basePKFilter.LB), closed, false)
		case types.T_float32:
			sorted = vector.OrderedSearchOffsetsByGreat(types.DecodeFloat32(basePKFilter.LB), closed, true)
			unSorted = vector.OrderedSearchOffsetsByGreat(types.DecodeFloat32(basePKFilter.LB), closed, false)
		case types.T_float64:
			sorted = vector.OrderedSearchOffsetsByGreat(types.DecodeFloat64(basePKFilter.LB), closed, true)
			unSorted = vector.OrderedSearchOffsetsByGreat(types.DecodeFloat64(basePKFilter.LB), closed, false)
		case types.T_uint8:
			sorted = vector.OrderedSearchOffsetsByGreat(types.DecodeUint8(basePKFilter.LB), closed, true)
			unSorted = vector.OrderedSearchOffsetsByGreat(types.DecodeUint8(basePKFilter.LB), closed, false)
		case types.T_uint16:
			sorted = vector.OrderedSearchOffsetsByGreat(types.DecodeUint16(basePKFilter.LB), closed, true)
			unSorted = vector.OrderedSearchOffsetsByGreat(types.DecodeUint16(basePKFilter.LB), closed, false)
		case types.T_uint32:
			sorted = vector.OrderedSearchOffsetsByGreat(types.DecodeUint32(basePKFilter.LB), closed, true)
			unSorted = vector.OrderedSearchOffsetsByGreat(types.DecodeUint32(basePKFilter.LB), closed, false)
		case types.T_uint64:
			sorted = vector.OrderedSearchOffsetsByGreat(types.DecodeUint64(basePKFilter.LB), closed, true)
			unSorted = vector.OrderedSearchOffsetsByGreat(types.DecodeUint64(basePKFilter.LB), closed, false)
		case types.T_date:
			sorted = vector.OrderedSearchOffsetsByGreat(types.DecodeDate(basePKFilter.LB), closed, true)
			unSorted = vector.OrderedSearchOffsetsByGreat(types.DecodeDate(basePKFilter.LB), closed, false)
		case types.T_time:
			sorted = vector.OrderedSearchOffsetsByGreat(types.DecodeTime(basePKFilter.LB), closed, true)
			unSorted = vector.OrderedSearchOffsetsByGreat(types.DecodeTime(basePKFilter.LB), closed, false)
		case types.T_datetime:
			sorted = vector.OrderedSearchOffsetsByGreat(types.DecodeDatetime(basePKFilter.LB), closed, true)
			unSorted = vector.OrderedSearchOffsetsByGreat(types.DecodeDatetime(basePKFilter.LB), closed, false)
		case types.T_timestamp:
			sorted = vector.OrderedSearchOffsetsByGreat(types.DecodeTimestamp(basePKFilter.LB), closed, true)
			unSorted = vector.OrderedSearchOffsetsByGreat(types.DecodeTimestamp(basePKFilter.LB), closed, false)
		case types.T_decimal64:
			sorted = vector.FixedSizeSearchOffsetsByGTTypeChecked(types.DecodeDecimal64(basePKFilter.LB), closed, true, types.CompareDecimal64)
			unSorted = vector.FixedSizeSearchOffsetsByGTTypeChecked(types.DecodeDecimal64(basePKFilter.LB), closed, false, types.CompareDecimal64)
		case types.T_decimal128:
			sorted = vector.FixedSizeSearchOffsetsByGTTypeChecked(types.DecodeDecimal128(basePKFilter.LB), closed, true, types.CompareDecimal128)
			unSorted = vector.FixedSizeSearchOffsetsByGTTypeChecked(types.DecodeDecimal128(basePKFilter.LB), closed, false, types.CompareDecimal128)
		case types.T_varchar, types.T_char, types.T_binary, types.T_json:
			sorted = vector.VarlenSearchOffsetByGreat(basePKFilter.LB, closed, true)
			unSorted = vector.VarlenSearchOffsetByGreat(basePKFilter.LB, closed, false)
		case types.T_enum:
			sorted = vector.OrderedSearchOffsetsByGreat(types.DecodeEnum(basePKFilter.LB), closed, true)
			unSorted = vector.OrderedSearchOffsetsByGreat(types.DecodeEnum(basePKFilter.LB), closed, false)
		case types.T_uuid:
			sorted = vector.FixedSizeSearchOffsetsByGTTypeChecked(types.Uuid(basePKFilter.LB), closed, true, types.CompareUuid)
			unSorted = vector.FixedSizeSearchOffsetsByGTTypeChecked(types.Uuid(basePKFilter.LB), closed, false, types.CompareUuid)
		default:
		}

	case function.BETWEEN, RangeLeftOpen, RangeRightOpen, RangeBothOpen:
		var closedLeft, closedRight bool

		switch basePKFilter.Op {
		case function.BETWEEN:
			closedLeft, closedRight = true, true
		case RangeLeftOpen:
			closedLeft, closedRight = false, true
		case RangeRightOpen:
			closedLeft, closedRight = true, false
		case RangeBothOpen:
			closedLeft, closedRight = false, false
		case function.PREFIX_BETWEEN:
			closedLeft, closedRight = true, true
		}

		// hint: 0 [,] 1 (,] 2 [,) 3 (,)
		hint := 0
		switch {
		case !closedLeft && closedRight:
			hint = 1
		case closedLeft && !closedRight:
			hint = 2
		case !closedLeft && !closedRight:
			hint = 3
		}

		switch basePKFilter.Oid {
		case types.T_int8:
			val1 := types.DecodeInt8(basePKFilter.LB)
			val2 := types.DecodeInt8(basePKFilter.UB)
			sorted = vector.CollectOffsetsByBetweenFactory(val1, val2, hint)
			unSorted = vector.LinearCollectOffsetsByBetweenFactory(val1, val2, hint)
		case types.T_int16:
			val1 := types.DecodeInt16(basePKFilter.LB)
			val2 := types.DecodeInt16(basePKFilter.UB)
			sorted = vector.CollectOffsetsByBetweenFactory(val1, val2, hint)
			unSorted = vector.LinearCollectOffsetsByBetweenFactory(val1, val2, hint)
		case types.T_int32:
			val1 := types.DecodeInt32(basePKFilter.LB)
			val2 := types.DecodeInt32(basePKFilter.UB)
			sorted = vector.CollectOffsetsByBetweenFactory(val1, val2, hint)
			unSorted = vector.LinearCollectOffsetsByBetweenFactory(val1, val2, hint)
		case types.T_int64:
			val1 := types.DecodeInt64(basePKFilter.LB)
			val2 := types.DecodeInt64(basePKFilter.UB)
			sorted = vector.CollectOffsetsByBetweenFactory(val1, val2, hint)
			unSorted = vector.LinearCollectOffsetsByBetweenFactory(val1, val2, hint)
		case types.T_uint8:
			val1 := types.DecodeUint8(basePKFilter.LB)
			val2 := types.DecodeUint8(basePKFilter.UB)
			sorted = vector.CollectOffsetsByBetweenFactory(val1, val2, hint)
			unSorted = vector.LinearCollectOffsetsByBetweenFactory(val1, val2, hint)
		case types.T_uint16:
			val1 := types.DecodeUint16(basePKFilter.LB)
			val2 := types.DecodeUint16(basePKFilter.UB)
			sorted = vector.CollectOffsetsByBetweenFactory(val1, val2, hint)
			unSorted = vector.LinearCollectOffsetsByBetweenFactory(val1, val2, hint)
		case types.T_uint32:
			val1 := types.DecodeUint32(basePKFilter.LB)
			val2 := types.DecodeUint32(basePKFilter.UB)
			sorted = vector.CollectOffsetsByBetweenFactory(val1, val2, hint)
			unSorted = vector.LinearCollectOffsetsByBetweenFactory(val1, val2, hint)
		case types.T_uint64:
			val1 := types.DecodeUint64(basePKFilter.LB)
			val2 := types.DecodeUint64(basePKFilter.UB)
			sorted = vector.CollectOffsetsByBetweenFactory(val1, val2, hint)
			unSorted = vector.LinearCollectOffsetsByBetweenFactory(val1, val2, hint)
		case types.T_float32:
			val1 := types.DecodeFloat32(basePKFilter.LB)
			val2 := types.DecodeFloat32(basePKFilter.UB)
			sorted = vector.CollectOffsetsByBetweenFactory(val1, val2, hint)
			unSorted = vector.LinearCollectOffsetsByBetweenFactory(val1, val2, hint)
		case types.T_float64:
			val1 := types.DecodeFloat64(basePKFilter.LB)
			val2 := types.DecodeFloat64(basePKFilter.UB)
			sorted = vector.CollectOffsetsByBetweenFactory(val1, val2, hint)
			unSorted = vector.LinearCollectOffsetsByBetweenFactory(val1, val2, hint)
		case types.T_date:
			val1 := types.DecodeDate(basePKFilter.LB)
			val2 := types.DecodeDate(basePKFilter.UB)
			sorted = vector.CollectOffsetsByBetweenFactory(val1, val2, hint)
			unSorted = vector.LinearCollectOffsetsByBetweenFactory(val1, val2, hint)
		case types.T_time:
			val1 := types.DecodeTime(basePKFilter.LB)
			val2 := types.DecodeTime(basePKFilter.UB)
			sorted = vector.CollectOffsetsByBetweenFactory(val1, val2, hint)
			unSorted = vector.LinearCollectOffsetsByBetweenFactory(val1, val2, hint)
		case types.T_datetime:
			val1 := types.DecodeDatetime(basePKFilter.LB)
			val2 := types.DecodeDatetime(basePKFilter.UB)
			sorted = vector.CollectOffsetsByBetweenFactory(val1, val2, hint)
			unSorted = vector.LinearCollectOffsetsByBetweenFactory(val1, val2, hint)
		case types.T_timestamp:
			val1 := types.DecodeTimestamp(basePKFilter.LB)
			val2 := types.DecodeTimestamp(basePKFilter.UB)
			sorted = vector.CollectOffsetsByBetweenFactory(val1, val2, hint)
			unSorted = vector.LinearCollectOffsetsByBetweenFactory(val1, val2, hint)
		case types.T_decimal64:
			val1 := types.DecodeDecimal64(basePKFilter.LB)
			val2 := types.DecodeDecimal64(basePKFilter.UB)

			sorted = vector.CollectOffsetsByBetweenWithCompareFactory(val1, val2, types.CompareDecimal64)
			unSorted = vector.FixedSizedLinearCollectOffsetsByBetweenFactory(val1, val2, types.CompareDecimal64)
		case types.T_decimal128:
			val1 := types.DecodeDecimal128(basePKFilter.LB)
			val2 := types.DecodeDecimal128(basePKFilter.UB)

			sorted = vector.CollectOffsetsByBetweenWithCompareFactory(val1, val2, types.CompareDecimal128)
			unSorted = vector.FixedSizedLinearCollectOffsetsByBetweenFactory(val1, val2, types.CompareDecimal128)
		case types.T_text, types.T_datalink, types.T_varchar, types.T_char, types.T_binary, types.T_json:
			lb := string(basePKFilter.LB)
			ub := string(basePKFilter.UB)
			sorted = vector.CollectOffsetsByBetweenString(lb, ub, hint)
			unSorted = vector.LinearCollectOffsetsByBetweenString(lb, ub, hint)
		case types.T_enum:
			lb := types.DecodeEnum(basePKFilter.LB)
			ub := types.DecodeEnum(basePKFilter.UB)
			sorted = vector.CollectOffsetsByBetweenFactory(lb, ub, hint)
			unSorted = vector.LinearCollectOffsetsByBetweenFactory(lb, ub, hint)

		case types.T_uuid:
			val1 := types.Uuid(basePKFilter.LB)
			val2 := types.Uuid(basePKFilter.UB)

			sorted = vector.CollectOffsetsByBetweenWithCompareFactory(val1, val2, types.CompareUuid)
			unSorted = vector.FixedSizedLinearCollectOffsetsByBetweenFactory(val1, val2, types.CompareUuid)
		default:
		}
	}

	return
}

func combineOffsetFuncs(funcs []func(*vector.Vector) []int64) func(*vector.Vector) []int64 {
	if len(funcs) == 1 {
		return funcs[0]
	}

	return func(vec *vector.Vector) []int64 {
		seen := make(map[int64]struct{})
		var out []int64
		for _, fn := range funcs {
			if fn == nil {
				continue
			}
			offsets := fn(vec)
			for _, off := range offsets {
				if _, ok := seen[off]; ok {
					continue
				}
				seen[off] = struct{}{}
				out = append(out, off)
			}
		}
		sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
		return out
	}
}

func mergeBaseFilterInKind(
	left, right BasePKFilter, isOR bool, mp *mpool.MPool,
) (ret BasePKFilter, err error) {
	var va, vb *vector.Vector
	ret.Vec = vector.NewVec(left.Oid.ToType())

	va = left.Vec
	vb = right.Vec

	switch va.GetType().Oid {
	case types.T_int8:
		a := vector.MustFixedColNoTypeCheck[int8](va)
		b := vector.MustFixedColNoTypeCheck[int8](vb)
		cmp := func(x, y int8) int { return int(x - y) }

		if isOR {
			err = vector.Union2VectorOrdered(a, b, ret.Vec, mp, cmp)
		} else {
			err = vector.Intersection2VectorOrdered(a, b, ret.Vec, mp, cmp)
		}
	case types.T_int16:
		a := vector.MustFixedColNoTypeCheck[int16](va)
		b := vector.MustFixedColNoTypeCheck[int16](vb)
		cmp := func(x, y int16) int { return int(x - y) }

		if isOR {
			err = vector.Union2VectorOrdered(a, b, ret.Vec, mp, cmp)
		} else {
			err = vector.Intersection2VectorOrdered(a, b, ret.Vec, mp, cmp)
		}
	case types.T_int32:
		a := vector.MustFixedColNoTypeCheck[int32](va)
		b := vector.MustFixedColNoTypeCheck[int32](vb)
		cmp := func(x, y int32) int { return int(x - y) }

		if isOR {
			err = vector.Union2VectorOrdered(a, b, ret.Vec, mp, cmp)
		} else {
			err = vector.Intersection2VectorOrdered(a, b, ret.Vec, mp, cmp)
		}
	case types.T_int64:
		a := vector.MustFixedColNoTypeCheck[int64](va)
		b := vector.MustFixedColNoTypeCheck[int64](vb)
		cmp := func(x, y int64) int { return int(x - y) }

		if isOR {
			err = vector.Union2VectorOrdered(a, b, ret.Vec, mp, cmp)
		} else {
			err = vector.Intersection2VectorOrdered(a, b, ret.Vec, mp, cmp)
		}
	case types.T_float32:
		a := vector.MustFixedColNoTypeCheck[float32](va)
		b := vector.MustFixedColNoTypeCheck[float32](vb)
		cmp := func(x, y float32) int { return int(x - y) }

		if isOR {
			err = vector.Union2VectorOrdered(a, b, ret.Vec, mp, cmp)
		} else {
			err = vector.Intersection2VectorOrdered(a, b, ret.Vec, mp, cmp)
		}
	case types.T_float64:
		a := vector.MustFixedColNoTypeCheck[float64](va)
		b := vector.MustFixedColNoTypeCheck[float64](vb)
		cmp := func(x, y float64) int { return int(x - y) }

		if isOR {
			err = vector.Union2VectorOrdered(a, b, ret.Vec, mp, cmp)
		} else {
			err = vector.Intersection2VectorOrdered(a, b, ret.Vec, mp, cmp)
		}
	case types.T_uint8:
		a := vector.MustFixedColNoTypeCheck[uint8](va)
		b := vector.MustFixedColNoTypeCheck[uint8](vb)
		cmp := func(x, y uint8) int { return int(x) - int(y) }

		if isOR {
			err = vector.Union2VectorOrdered(a, b, ret.Vec, mp, cmp)
		} else {
			err = vector.Intersection2VectorOrdered(a, b, ret.Vec, mp, cmp)
		}
	case types.T_uint16:
		a := vector.MustFixedColNoTypeCheck[uint16](va)
		b := vector.MustFixedColNoTypeCheck[uint16](vb)
		cmp := func(x, y uint16) int { return int(x) - int(y) }

		if isOR {
			err = vector.Union2VectorOrdered(a, b, ret.Vec, mp, cmp)
		} else {
			err = vector.Intersection2VectorOrdered(a, b, ret.Vec, mp, cmp)
		}
	case types.T_uint32:
		a := vector.MustFixedColNoTypeCheck[uint32](va)
		b := vector.MustFixedColNoTypeCheck[uint32](vb)
		cmp := func(x, y uint32) int { return int(x) - int(y) }

		if isOR {
			err = vector.Union2VectorOrdered(a, b, ret.Vec, mp, cmp)
		} else {
			err = vector.Intersection2VectorOrdered(a, b, ret.Vec, mp, cmp)
		}
	case types.T_uint64:
		a := vector.MustFixedColNoTypeCheck[uint64](va)
		b := vector.MustFixedColNoTypeCheck[uint64](vb)
		cmp := func(x, y uint64) int { return int(x) - int(y) }

		if isOR {
			err = vector.Union2VectorOrdered(a, b, ret.Vec, mp, cmp)
		} else {
			err = vector.Intersection2VectorOrdered(a, b, ret.Vec, mp, cmp)
		}
	case types.T_date:
		a := vector.MustFixedColNoTypeCheck[types.Date](va)
		b := vector.MustFixedColNoTypeCheck[types.Date](vb)
		cmp := func(x, y types.Date) int { return int(x - y) }

		if isOR {
			err = vector.Union2VectorOrdered(a, b, ret.Vec, mp, cmp)
		} else {
			err = vector.Intersection2VectorOrdered(a, b, ret.Vec, mp, cmp)
		}
	case types.T_time:
		a := vector.MustFixedColNoTypeCheck[types.Time](va)
		b := vector.MustFixedColNoTypeCheck[types.Time](vb)
		cmp := func(x, y types.Time) int { return int(x - y) }

		if isOR {
			err = vector.Union2VectorOrdered(a, b, ret.Vec, mp, cmp)
		} else {
			err = vector.Intersection2VectorOrdered(a, b, ret.Vec, mp, cmp)
		}
	case types.T_datetime:
		a := vector.MustFixedColNoTypeCheck[types.Datetime](va)
		b := vector.MustFixedColNoTypeCheck[types.Datetime](vb)
		cmp := func(x, y types.Datetime) int { return int(x - y) }

		if isOR {
			err = vector.Union2VectorOrdered(a, b, ret.Vec, mp, cmp)
		} else {
			err = vector.Intersection2VectorOrdered(a, b, ret.Vec, mp, cmp)
		}
	case types.T_timestamp:
		a := vector.MustFixedColNoTypeCheck[types.Timestamp](va)
		b := vector.MustFixedColNoTypeCheck[types.Timestamp](vb)
		cmp := func(x, y types.Timestamp) int { return int(x - y) }
		if isOR {
			err = vector.Union2VectorOrdered(a, b, ret.Vec, mp, cmp)
		} else {
			err = vector.Intersection2VectorOrdered(a, b, ret.Vec, mp, cmp)
		}
	case types.T_decimal64:
		a := vector.MustFixedColNoTypeCheck[types.Decimal64](va)
		b := vector.MustFixedColNoTypeCheck[types.Decimal64](vb)
		cmp := func(x, y types.Decimal64) int { return int(x - y) }
		if isOR {
			err = vector.Union2VectorOrdered(a, b, ret.Vec, mp, cmp)
		} else {
			err = vector.Intersection2VectorOrdered(a, b, ret.Vec, mp, cmp)
		}

	case types.T_decimal128:
		a := vector.MustFixedColNoTypeCheck[types.Decimal128](va)
		b := vector.MustFixedColNoTypeCheck[types.Decimal128](vb)
		if isOR {
			err = vector.Union2VectorOrdered(a, b, ret.Vec, mp,
				func(x, y types.Decimal128) int { return types.CompareDecimal128(x, y) })
		} else {
			err = vector.Intersection2VectorOrdered(a, b, ret.Vec, mp,
				func(x, y types.Decimal128) int { return types.CompareDecimal128(x, y) })
		}

	case types.T_varchar, types.T_char, types.T_json, types.T_binary, types.T_text, types.T_datalink:
		if isOR {
			err = vector.Union2VectorValen(va, vb, ret.Vec, mp)
		} else {
			err = vector.Intersection2VectorVarlen(va, vb, ret.Vec, mp)
		}

	case types.T_enum:
		a := vector.MustFixedColNoTypeCheck[types.Enum](va)
		b := vector.MustFixedColNoTypeCheck[types.Enum](vb)
		cmp := func(x, y types.Enum) int { return int(x - y) }
		if isOR {
			err = vector.Union2VectorOrdered(a, b, ret.Vec, mp, cmp)
		} else {
			err = vector.Intersection2VectorOrdered(a, b, ret.Vec, mp, cmp)
		}

	default:
		return BasePKFilter{}, err
	}

	ret.Valid = true
	ret.Op = left.Op
	ret.Oid = left.Oid

	return ret, err
}

// left op in (">", ">=", "=", "<", "<="), right op in (">", ">=", "=", "<", "<=")
// left op AND right op
// left op OR right op
func mergeFilters(
	left, right *BasePKFilter,
	connector int,
	mp *mpool.MPool,
) (finalFilter BasePKFilter, err error) {
	defer func() {
		finalFilter.Oid = left.Oid

		if !finalFilter.Valid && connector == function.AND {
			// choose the left as default
			finalFilter = *left
			if left.Vec != nil {
				// why dup here?
				// once the merge filter generated, the others will be free.
				//finalFilter.Vec, err = left.Vec.Dup(proc.Mp())

				// or just set the left.Vec = nil to avoid free it
				(*left).Vec = nil
			}
		}
	}()

	switch connector {
	case function.AND:
		switch left.Op {
		case function.IN:
			switch right.Op {
			case function.IN:
				// a in (...) and a in (...) and a in (...) and ...
				finalFilter, err = mergeBaseFilterInKind(*left, *right, false, mp)
			}

		case function.GREAT_EQUAL:
			switch right.Op {
			case function.GREAT_EQUAL, function.GREAT_THAN:
				// a >= x and a >= y --> a >= max(x, y)
				// a >= x and a > y  --> a > y or a >= x
				if bytes.Compare(left.LB, right.LB) >= 0 { // x >= y
					return *left, nil
				} else { // x < y
					return *right, nil
				}

			case function.LESS_EQUAL, function.LESS_THAN:
				// a >= x and a <= y --> [x, y]
				// a >= x and a < y  --> [x, y)
				finalFilter.LB = left.LB
				finalFilter.UB = right.LB
				if right.Op == function.LESS_THAN {
					finalFilter.Op = RangeRightOpen
				} else {
					finalFilter.Op = function.BETWEEN
				}
				finalFilter.Valid = true

			case function.EQUAL:
				// a >= x and a = y --> a = y if y >= x
				if bytes.Compare(left.LB, right.LB) <= 0 {
					finalFilter.Op = function.EQUAL
					finalFilter.LB = right.LB
					finalFilter.Valid = true
				}
			}

		case function.GREAT_THAN:
			switch right.Op {
			case function.GREAT_EQUAL, function.GREAT_THAN:
				// a > x and a >= y
				// a > x and a > y
				if bytes.Compare(left.LB, right.LB) >= 0 { // x >= y
					return *left, nil
				} else { // x < y
					return *right, nil
				}

			case function.LESS_EQUAL, function.LESS_THAN:
				// a > x and a <= y --> (x, y]
				// a > x and a < y  --> (x, y)
				finalFilter.LB = left.LB
				finalFilter.UB = right.LB
				if right.Op == function.LESS_THAN {
					finalFilter.Op = RangeBothOpen
				} else {
					finalFilter.Op = RangeLeftOpen
				}
				finalFilter.Valid = true

			case function.EQUAL:
				// a > x and a = y --> a = y if y > x
				if bytes.Compare(left.LB, right.LB) < 0 { // x < y
					finalFilter.Op = function.EQUAL
					finalFilter.LB = right.LB
					finalFilter.Valid = true
				}
			}

		case function.LESS_EQUAL:
			switch right.Op {
			case function.GREAT_EQUAL, function.GREAT_THAN:
				// a <= x and a >= y --> [y, x]
				// a <= x and a > y  --> (y, x]
				finalFilter.LB = right.LB
				finalFilter.UB = left.LB
				if right.Op == function.GREAT_EQUAL {
					finalFilter.Op = function.BETWEEN
				} else {
					finalFilter.Op = RangeLeftOpen
				}
				finalFilter.Valid = true

			case function.LESS_EQUAL, function.LESS_THAN:
				// a <= x and a <= y --> a <= min(x,y)
				// a <= x and a < y  --> a <= x if x < y | a < y if x >= y
				if bytes.Compare(left.LB, right.LB) < 0 { // x < y
					return *left, nil
				} else {
					return *right, nil
				}

			case function.EQUAL:
				// a <= x and a = y --> a = y if x >= y
				if bytes.Compare(left.LB, right.LB) >= 0 {
					finalFilter.Op = function.EQUAL
					finalFilter.LB = right.LB
					finalFilter.Valid = true
				}
			}

		case function.LESS_THAN:
			switch right.Op {
			case function.GREAT_EQUAL, function.GREAT_THAN:
				// a < x and a >= y --> [y, x)
				// a < x and a > y  --> (y, x)
				finalFilter.LB = right.LB
				finalFilter.UB = left.LB
				if right.Op == function.GREAT_EQUAL {
					finalFilter.Op = RangeRightOpen
				} else {
					finalFilter.Op = RangeBothOpen
				}
				finalFilter.Valid = true

			case function.LESS_EQUAL, function.LESS_THAN:
				// a < x and a <= y --> a < x if x <= y | a <= y if x > y
				// a < x and a < y  --> a < min(x,y)
				finalFilter.Op = function.LESS_THAN
				if bytes.Compare(left.LB, right.LB) <= 0 {
					finalFilter.LB = left.LB
				} else {
					finalFilter.LB = right.LB
					if right.Op == function.LESS_EQUAL {
						finalFilter.Op = function.LESS_EQUAL
					}
				}
				finalFilter.Valid = true

			case function.EQUAL:
				// a < x and a = y --> a = y if x > y
				if bytes.Compare(left.LB, right.LB) > 0 {
					finalFilter.Op = function.EQUAL
					finalFilter.LB = right.LB
					finalFilter.Valid = true
				}
			}

		case function.EQUAL:
			switch right.Op {
			case function.GREAT_EQUAL, function.GREAT_THAN:
				// a = x and a >= y --> a = x if x >= y
				// a = x and a > y  --> a = x if x > y
				if ret := bytes.Compare(left.LB, right.LB); ret > 0 {
					return *left, nil
				} else if ret == 0 && right.Op == function.GREAT_EQUAL {
					return *left, nil
				}

			case function.LESS_EQUAL, function.LESS_THAN:
				// a = x and a <= y --> a = x if x <= y
				// a = x and a < y  --> a = x if x < y
				if ret := bytes.Compare(left.LB, right.LB); ret < 0 {
					return *left, nil
				} else if ret == 0 && right.Op == function.LESS_EQUAL {
					return *left, nil
				}

			case function.EQUAL:
				// a = x and a = y --> a = y if x = y
				if bytes.Equal(left.LB, right.LB) {
					return *left, nil
				}
			}
		}

	case function.OR:
		switch left.Op {
		case function.IN:
			switch right.Op {
			case function.IN:
				// a in (...) and a in (...)
				finalFilter, err = mergeBaseFilterInKind(*left, *right, true, mp)
			}

		case function.GREAT_EQUAL:
			switch right.Op {
			case function.GREAT_EQUAL, function.GREAT_THAN:
				// a >= x or a >= y --> a >= min(x, y)
				// a >= x or a > y  --> a >= x if x <= y | a > y if x > y
				if bytes.Compare(left.LB, right.LB) <= 0 { // x <= y
					return *left, nil
				} else { // x > y
					return *right, nil
				}

			case function.LESS_EQUAL, function.LESS_THAN:
				// a >= x or a <= y --> all if x <= y | [] or []
				// a >= x or a < y  -->
				//finalFilter.LB = left.LB
				//finalFilter.UB = right.LB
				//if right.Op == function.LESS_THAN {
				//	finalFilter.Op = RangeRightOpen
				//} else {
				//	finalFilter.Op = function.BETWEEN
				//}
				//finalFilter.Valid = true

			case function.EQUAL:
				// a >= x or a = y --> a >= x if x <= y | [], x
				if bytes.Compare(left.LB, right.LB) <= 0 {
					finalFilter.Op = function.GREAT_EQUAL
					finalFilter.LB = left.LB
					finalFilter.Valid = true
				}
			}

		case function.GREAT_THAN:
			switch right.Op {
			case function.GREAT_EQUAL, function.GREAT_THAN:
				// a > x or a >= y --> a >= y if x >= y | a > x if x < y
				// a > x or a > y  --> a > y if x >= y | a > x if x < y
				if bytes.Compare(left.LB, right.LB) >= 0 { // x >= y
					return *right, nil
				} else { // x < y
					return *left, nil
				}

			case function.LESS_EQUAL, function.LESS_THAN:
				// a > x or a <= y --> (x, y]
				// a > x or a < y  --> (x, y)
				//finalFilter.LB = left.LB
				//finalFilter.UB = right.LB
				//if right.Op == function.LESS_THAN {
				//	finalFilter.Op = rangeBothOpen
				//} else {
				//	finalFilter.Op = rangeLeftOpen
				//}
				//finalFilter.Valid = true

			case function.EQUAL:
				// a > x or a = y --> a > x if x < y | a >= x if x == y
				if ret := bytes.Compare(left.LB, right.LB); ret < 0 { // x < y
					return *left, nil
				} else if ret == 0 {
					finalFilter = *left
					finalFilter.Op = function.GREAT_EQUAL
				}
			}

		case function.LESS_EQUAL:
			switch right.Op {
			case function.GREAT_EQUAL, function.GREAT_THAN:
				// a <= x or a >= y -->
				// a <= x or a > y  -->
				//finalFilter.LB = right.LB
				//finalFilter.UB = left.LB
				//if right.Op == function.GREAT_EQUAL {
				//	finalFilter.Op = function.BETWEEN
				//} else {
				//	finalFilter.Op = rangeLeftOpen
				//}
				//finalFilter.Valid = true

			case function.LESS_EQUAL, function.LESS_THAN:
				// a <= x or a <= y --> a <= max(x,y)
				// a <= x or a < y  --> a <= x if x >= y | a < y if x < y
				if bytes.Compare(left.LB, right.LB) >= 0 { // x >= y
					return *left, nil
				} else {
					return *right, nil
				}

			case function.EQUAL:
				// a <= x or a = y --> a <= x if x >= y | [], x
				if bytes.Compare(left.LB, right.LB) >= 0 {
					return *left, nil
				}
			}

		case function.LESS_THAN:
			switch right.Op {
			case function.GREAT_EQUAL, function.GREAT_THAN:
				// a < x or a >= y
				// a < x or a > y
				//finalFilter.LB = right.LB
				//finalFilter.UB = left.LB
				//if right.Op == function.GREAT_EQUAL {
				//	finalFilter.Op = RangeRightOpen
				//} else {
				//	finalFilter.Op = rangeBothOpen
				//}
				//finalFilter.Valid = true

			case function.LESS_EQUAL, function.LESS_THAN:
				// a < x or a <= y --> a <= y if x <= y | a < x if x > y
				// a < x or a < y  --> a < y if x <= y | a < x if x > y
				if bytes.Compare(left.LB, right.LB) <= 0 { // a <= y
					return *right, nil
				} else {
					return *left, nil
				}

			case function.EQUAL:
				// a < x or a = y --> a < x if x > y | a <= x if x = y
				if ret := bytes.Compare(left.LB, right.LB); ret > 0 {
					return *left, nil
				} else if ret == 0 {
					finalFilter = *left
					finalFilter.Op = function.LESS_EQUAL
				}
			}

		case function.EQUAL:
			switch right.Op {
			case function.GREAT_EQUAL, function.GREAT_THAN:
				// a = x or a >= y --> a >= y if x >= y
				// a = x or a > y  --> a > y if x > y | a >= y if x = y
				if ret := bytes.Compare(left.LB, right.LB); ret > 0 {
					return *right, nil
				} else if ret == 0 {
					finalFilter = *right
					finalFilter.Op = function.GREAT_EQUAL
				}

			case function.LESS_EQUAL, function.LESS_THAN:
				// a = x or a <= y --> a <= y if x <= y
				// a = x or a < y  --> a < y if x < y | a <= y if x = y
				if ret := bytes.Compare(left.LB, right.LB); ret < 0 {
					return *right, nil
				} else if ret == 0 {
					finalFilter = *right
					finalFilter.Op = function.LESS_EQUAL
				}

			case function.EQUAL:
				// a = x or a = y --> a = x if x = y
				//                --> a in (x, y) if x != y
				if bytes.Equal(left.LB, right.LB) {
					return *left, nil
				}

			}
		}
	}
	return
}
