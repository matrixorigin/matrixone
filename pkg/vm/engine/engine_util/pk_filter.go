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

package engine_util

import (
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// func SimpleConstructBlockPKFilter(
// 	pkName string,
// 	val []byte,
// 	inVec *vector.Vector,
// 	oid types.T,
// ) (f objectio.BlockReadFilter, err error) {
// 	var base BasePKFilter
// 	if inVec != nil {
// 		base.Op = function.IN
// 		base.Vec = inVec
// 		base.Oid = oid
// 	} else {
// 		base.Op = function.EQUAL
// 		base.LB = val
// 		base.Oid = oid
// 	}
// 	base.Valid = true
// 	return ConstructBlockPKFilter(pkName, base)
// }

func ConstructBlockPKFilter(
	isFakePK bool,
	basePKFilter BasePKFilter,
) (f objectio.BlockReadFilter, err error) {
	if !basePKFilter.Valid {
		return objectio.BlockReadFilter{}, nil
	}

	var readFilter objectio.BlockReadFilter
	var sortedSearchFunc, unSortedSearchFunc func(*vector.Vector) []int64

	readFilter.HasFakePK = isFakePK

	switch basePKFilter.Op {
	case function.EQUAL:
		switch basePKFilter.Oid {
		case types.T_int8:
			sortedSearchFunc = vector.OrderedBinarySearchOffsetByValFactory([]int8{types.DecodeInt8(basePKFilter.LB)})
			unSortedSearchFunc = vector.OrderedLinearSearchOffsetByValFactory([]int8{types.DecodeInt8(basePKFilter.LB)}, nil)
		case types.T_int16:
			sortedSearchFunc = vector.OrderedBinarySearchOffsetByValFactory([]int16{types.DecodeInt16(basePKFilter.LB)})
			unSortedSearchFunc = vector.OrderedLinearSearchOffsetByValFactory([]int16{types.DecodeInt16(basePKFilter.LB)}, nil)
		case types.T_int32:
			sortedSearchFunc = vector.OrderedBinarySearchOffsetByValFactory([]int32{types.DecodeInt32(basePKFilter.LB)})
			unSortedSearchFunc = vector.OrderedLinearSearchOffsetByValFactory([]int32{types.DecodeInt32(basePKFilter.LB)}, nil)
		case types.T_int64:
			sortedSearchFunc = vector.OrderedBinarySearchOffsetByValFactory([]int64{types.DecodeInt64(basePKFilter.LB)})
			unSortedSearchFunc = vector.OrderedLinearSearchOffsetByValFactory([]int64{types.DecodeInt64(basePKFilter.LB)}, nil)
		case types.T_float32:
			sortedSearchFunc = vector.OrderedBinarySearchOffsetByValFactory([]float32{types.DecodeFloat32(basePKFilter.LB)})
			unSortedSearchFunc = vector.OrderedLinearSearchOffsetByValFactory([]float32{types.DecodeFloat32(basePKFilter.LB)}, nil)
		case types.T_float64:
			sortedSearchFunc = vector.OrderedBinarySearchOffsetByValFactory([]float64{types.DecodeFloat64(basePKFilter.LB)})
			unSortedSearchFunc = vector.OrderedLinearSearchOffsetByValFactory([]float64{types.DecodeFloat64(basePKFilter.LB)}, nil)
		case types.T_uint8:
			sortedSearchFunc = vector.OrderedBinarySearchOffsetByValFactory([]uint8{uint8(types.DecodeUint8(basePKFilter.LB))})
			unSortedSearchFunc = vector.OrderedLinearSearchOffsetByValFactory([]uint8{uint8(types.DecodeUint8(basePKFilter.LB))}, nil)
		case types.T_uint16:
			sortedSearchFunc = vector.OrderedBinarySearchOffsetByValFactory([]uint16{uint16(types.DecodeUint16(basePKFilter.LB))})
			unSortedSearchFunc = vector.OrderedLinearSearchOffsetByValFactory([]uint16{uint16(types.DecodeUint16(basePKFilter.LB))}, nil)
		case types.T_uint32:
			sortedSearchFunc = vector.OrderedBinarySearchOffsetByValFactory([]uint32{types.DecodeUint32(basePKFilter.LB)})
			unSortedSearchFunc = vector.OrderedLinearSearchOffsetByValFactory([]uint32{types.DecodeUint32(basePKFilter.LB)}, nil)
		case types.T_uint64:
			sortedSearchFunc = vector.OrderedBinarySearchOffsetByValFactory([]uint64{types.DecodeUint64(basePKFilter.LB)})
			unSortedSearchFunc = vector.OrderedLinearSearchOffsetByValFactory([]uint64{types.DecodeUint64(basePKFilter.LB)}, nil)
		case types.T_date:
			sortedSearchFunc = vector.OrderedBinarySearchOffsetByValFactory([]types.Date{types.DecodeDate(basePKFilter.LB)})
			unSortedSearchFunc = vector.OrderedLinearSearchOffsetByValFactory([]types.Date{types.DecodeDate(basePKFilter.LB)}, nil)
		case types.T_time:
			sortedSearchFunc = vector.OrderedBinarySearchOffsetByValFactory([]types.Time{types.DecodeTime(basePKFilter.LB)})
			unSortedSearchFunc = vector.OrderedLinearSearchOffsetByValFactory([]types.Time{types.DecodeTime(basePKFilter.LB)}, nil)
		case types.T_datetime:
			sortedSearchFunc = vector.OrderedBinarySearchOffsetByValFactory([]types.Datetime{types.DecodeDatetime(basePKFilter.LB)})
			unSortedSearchFunc = vector.OrderedLinearSearchOffsetByValFactory([]types.Datetime{types.DecodeDatetime(basePKFilter.LB)}, nil)
		case types.T_timestamp:
			sortedSearchFunc = vector.OrderedBinarySearchOffsetByValFactory([]types.Timestamp{types.DecodeTimestamp(basePKFilter.LB)})
			unSortedSearchFunc = vector.OrderedLinearSearchOffsetByValFactory([]types.Timestamp{types.DecodeTimestamp(basePKFilter.LB)}, nil)
		case types.T_decimal64:
			sortedSearchFunc = vector.FixedSizedBinarySearchOffsetByValFactory([]types.Decimal64{types.DecodeDecimal64(basePKFilter.LB)}, types.CompareDecimal64)
			unSortedSearchFunc = vector.FixedSizeLinearSearchOffsetByValFactory([]types.Decimal64{types.DecodeDecimal64(basePKFilter.LB)}, types.CompareDecimal64)
		case types.T_decimal128:
			sortedSearchFunc = vector.FixedSizedBinarySearchOffsetByValFactory([]types.Decimal128{types.DecodeDecimal128(basePKFilter.LB)}, types.CompareDecimal128)
			unSortedSearchFunc = vector.FixedSizeLinearSearchOffsetByValFactory([]types.Decimal128{types.DecodeDecimal128(basePKFilter.LB)}, types.CompareDecimal128)
		case types.T_varchar:
			sortedSearchFunc = vector.VarlenBinarySearchOffsetByValFactory([][]byte{basePKFilter.LB})
			unSortedSearchFunc = vector.VarlenLinearSearchOffsetByValFactory([][]byte{basePKFilter.LB})
		case types.T_json:
			sortedSearchFunc = vector.VarlenBinarySearchOffsetByValFactory([][]byte{basePKFilter.LB})
			unSortedSearchFunc = vector.VarlenLinearSearchOffsetByValFactory([][]byte{basePKFilter.LB})
		case types.T_enum:
			sortedSearchFunc = vector.OrderedBinarySearchOffsetByValFactory([]types.Enum{types.DecodeEnum(basePKFilter.LB)})
			unSortedSearchFunc = vector.OrderedLinearSearchOffsetByValFactory([]types.Enum{types.DecodeEnum(basePKFilter.LB)}, nil)
		}

	case function.PREFIX_EQ:
		sortedSearchFunc = vector.CollectOffsetsByPrefixEqFactory(basePKFilter.LB)
		unSortedSearchFunc = vector.LinearCollectOffsetsByPrefixEqFactory(basePKFilter.LB)

	case function.PREFIX_BETWEEN:
		sortedSearchFunc = vector.CollectOffsetsByPrefixBetweenFactory(basePKFilter.LB, basePKFilter.UB)
		unSortedSearchFunc = vector.LinearCollectOffsetsByPrefixBetweenFactory(basePKFilter.LB, basePKFilter.UB)

	case function.IN:
		vec := basePKFilter.Vec

		switch vec.GetType().Oid {
		case types.T_bit:
			sortedSearchFunc = vector.OrderedBinarySearchOffsetByValFactory(vector.MustFixedColNoTypeCheck[uint64](vec))
			unSortedSearchFunc = vector.OrderedLinearSearchOffsetByValFactory(vector.MustFixedColNoTypeCheck[uint64](vec), nil)
		case types.T_int8:
			sortedSearchFunc = vector.OrderedBinarySearchOffsetByValFactory(vector.MustFixedColNoTypeCheck[int8](vec))
			unSortedSearchFunc = vector.OrderedLinearSearchOffsetByValFactory(vector.MustFixedColNoTypeCheck[int8](vec), nil)
		case types.T_int16:
			sortedSearchFunc = vector.OrderedBinarySearchOffsetByValFactory(vector.MustFixedColNoTypeCheck[int16](vec))
			unSortedSearchFunc = vector.OrderedLinearSearchOffsetByValFactory(vector.MustFixedColNoTypeCheck[int16](vec), nil)
		case types.T_int32:
			sortedSearchFunc = vector.OrderedBinarySearchOffsetByValFactory(vector.MustFixedColNoTypeCheck[int32](vec))
			unSortedSearchFunc = vector.OrderedLinearSearchOffsetByValFactory(vector.MustFixedColNoTypeCheck[int32](vec), nil)
		case types.T_int64:
			sortedSearchFunc = vector.OrderedBinarySearchOffsetByValFactory(vector.MustFixedColNoTypeCheck[int64](vec))
			unSortedSearchFunc = vector.OrderedLinearSearchOffsetByValFactory(vector.MustFixedColNoTypeCheck[int64](vec), nil)
		case types.T_uint8:
			sortedSearchFunc = vector.OrderedBinarySearchOffsetByValFactory(vector.MustFixedColNoTypeCheck[uint8](vec))
			unSortedSearchFunc = vector.OrderedLinearSearchOffsetByValFactory(vector.MustFixedColNoTypeCheck[uint8](vec), nil)
		case types.T_uint16:
			sortedSearchFunc = vector.OrderedBinarySearchOffsetByValFactory(vector.MustFixedColNoTypeCheck[uint16](vec))
			unSortedSearchFunc = vector.OrderedLinearSearchOffsetByValFactory(vector.MustFixedColNoTypeCheck[uint16](vec), nil)
		case types.T_uint32:
			sortedSearchFunc = vector.OrderedBinarySearchOffsetByValFactory(vector.MustFixedColNoTypeCheck[uint32](vec))
			unSortedSearchFunc = vector.OrderedLinearSearchOffsetByValFactory(vector.MustFixedColNoTypeCheck[uint32](vec), nil)
		case types.T_uint64:
			sortedSearchFunc = vector.OrderedBinarySearchOffsetByValFactory(vector.MustFixedColNoTypeCheck[uint64](vec))
			unSortedSearchFunc = vector.OrderedLinearSearchOffsetByValFactory(vector.MustFixedColNoTypeCheck[uint64](vec), nil)
		case types.T_float32:
			sortedSearchFunc = vector.OrderedBinarySearchOffsetByValFactory(vector.MustFixedColNoTypeCheck[float32](vec))
			unSortedSearchFunc = vector.OrderedLinearSearchOffsetByValFactory(vector.MustFixedColNoTypeCheck[float32](vec), nil)
		case types.T_float64:
			sortedSearchFunc = vector.OrderedBinarySearchOffsetByValFactory(vector.MustFixedColNoTypeCheck[float64](vec))
			unSortedSearchFunc = vector.OrderedLinearSearchOffsetByValFactory(vector.MustFixedColNoTypeCheck[float64](vec), nil)
		case types.T_date:
			sortedSearchFunc = vector.OrderedBinarySearchOffsetByValFactory(vector.MustFixedColNoTypeCheck[types.Date](vec))
			unSortedSearchFunc = vector.OrderedLinearSearchOffsetByValFactory(vector.MustFixedColNoTypeCheck[types.Date](vec), nil)
		case types.T_time:
			sortedSearchFunc = vector.OrderedBinarySearchOffsetByValFactory(vector.MustFixedColNoTypeCheck[types.Time](vec))
			unSortedSearchFunc = vector.OrderedLinearSearchOffsetByValFactory(vector.MustFixedColNoTypeCheck[types.Time](vec), nil)
		case types.T_datetime:
			sortedSearchFunc = vector.OrderedBinarySearchOffsetByValFactory(vector.MustFixedColNoTypeCheck[types.Datetime](vec))
			unSortedSearchFunc = vector.OrderedLinearSearchOffsetByValFactory(vector.MustFixedColNoTypeCheck[types.Datetime](vec), nil)
		case types.T_timestamp:
			sortedSearchFunc = vector.OrderedBinarySearchOffsetByValFactory(vector.MustFixedColNoTypeCheck[types.Timestamp](vec))
			unSortedSearchFunc = vector.OrderedLinearSearchOffsetByValFactory(vector.MustFixedColNoTypeCheck[types.Timestamp](vec), nil)
		case types.T_decimal64:
			sortedSearchFunc = vector.FixedSizedBinarySearchOffsetByValFactory(vector.MustFixedColNoTypeCheck[types.Decimal64](vec), types.CompareDecimal64)
			unSortedSearchFunc = vector.FixedSizeLinearSearchOffsetByValFactory(vector.MustFixedColNoTypeCheck[types.Decimal64](vec), types.CompareDecimal64)
		case types.T_decimal128:
			sortedSearchFunc = vector.FixedSizedBinarySearchOffsetByValFactory(vector.MustFixedColNoTypeCheck[types.Decimal128](vec), types.CompareDecimal128)
			unSortedSearchFunc = vector.FixedSizeLinearSearchOffsetByValFactory(vector.MustFixedColNoTypeCheck[types.Decimal128](vec), types.CompareDecimal128)
		case types.T_char, types.T_varchar, types.T_binary, types.T_varbinary, types.T_json, types.T_blob, types.T_text,
			types.T_array_float32, types.T_array_float64, types.T_datalink:
			sortedSearchFunc = vector.VarlenBinarySearchOffsetByValFactory(vector.InefficientMustBytesCol(vec))
			unSortedSearchFunc = vector.VarlenLinearSearchOffsetByValFactory(vector.InefficientMustBytesCol(vec))
		case types.T_enum:
			sortedSearchFunc = vector.OrderedBinarySearchOffsetByValFactory(vector.MustFixedColNoTypeCheck[types.Enum](vec))
			unSortedSearchFunc = vector.OrderedLinearSearchOffsetByValFactory(vector.MustFixedColNoTypeCheck[types.Enum](vec), nil)
		}

	case function.PREFIX_IN:
		vec := basePKFilter.Vec

		sortedSearchFunc = vector.CollectOffsetsByPrefixInFactory(vec)
		unSortedSearchFunc = vector.LinearCollectOffsetsByPrefixInFactory(vec)

	case function.LESS_EQUAL, function.LESS_THAN:
		closed := basePKFilter.Op == function.LESS_EQUAL
		switch basePKFilter.Oid {
		case types.T_int8:
			sortedSearchFunc = vector.OrderedSearchOffsetsByLess(types.DecodeInt8(basePKFilter.LB), closed, true)
			unSortedSearchFunc = vector.OrderedSearchOffsetsByLess(types.DecodeInt8(basePKFilter.LB), closed, false)
		case types.T_int16:
			sortedSearchFunc = vector.OrderedSearchOffsetsByLess(types.DecodeInt16(basePKFilter.LB), closed, true)
			unSortedSearchFunc = vector.OrderedSearchOffsetsByLess(types.DecodeInt16(basePKFilter.LB), closed, false)
		case types.T_int32:
			sortedSearchFunc = vector.OrderedSearchOffsetsByLess(types.DecodeInt32(basePKFilter.LB), closed, true)
			unSortedSearchFunc = vector.OrderedSearchOffsetsByLess(types.DecodeInt32(basePKFilter.LB), closed, false)
		case types.T_int64:
			sortedSearchFunc = vector.OrderedSearchOffsetsByLess(types.DecodeInt64(basePKFilter.LB), closed, true)
			unSortedSearchFunc = vector.OrderedSearchOffsetsByLess(types.DecodeInt64(basePKFilter.LB), closed, false)
		case types.T_float32:
			sortedSearchFunc = vector.OrderedSearchOffsetsByLess(types.DecodeFloat32(basePKFilter.LB), closed, true)
			unSortedSearchFunc = vector.OrderedSearchOffsetsByLess(types.DecodeFloat32(basePKFilter.LB), closed, false)
		case types.T_float64:
			sortedSearchFunc = vector.OrderedSearchOffsetsByLess(types.DecodeFloat64(basePKFilter.LB), closed, true)
			unSortedSearchFunc = vector.OrderedSearchOffsetsByLess(types.DecodeFloat64(basePKFilter.LB), closed, false)
		case types.T_uint8:
			sortedSearchFunc = vector.OrderedSearchOffsetsByLess(types.DecodeUint8(basePKFilter.LB), closed, true)
			unSortedSearchFunc = vector.OrderedSearchOffsetsByLess(types.DecodeUint8(basePKFilter.LB), closed, false)
		case types.T_uint16:
			sortedSearchFunc = vector.OrderedSearchOffsetsByLess(types.DecodeUint16(basePKFilter.LB), closed, true)
			unSortedSearchFunc = vector.OrderedSearchOffsetsByLess(types.DecodeUint16(basePKFilter.LB), closed, false)
		case types.T_uint32:
			sortedSearchFunc = vector.OrderedSearchOffsetsByLess(types.DecodeUint32(basePKFilter.LB), closed, true)
			unSortedSearchFunc = vector.OrderedSearchOffsetsByLess(types.DecodeUint32(basePKFilter.LB), closed, false)
		case types.T_uint64:
			sortedSearchFunc = vector.OrderedSearchOffsetsByLess(types.DecodeUint64(basePKFilter.LB), closed, true)
			unSortedSearchFunc = vector.OrderedSearchOffsetsByLess(types.DecodeUint64(basePKFilter.LB), closed, false)
		case types.T_date:
			sortedSearchFunc = vector.OrderedSearchOffsetsByLess(types.DecodeDate(basePKFilter.LB), closed, true)
			unSortedSearchFunc = vector.OrderedSearchOffsetsByLess(types.DecodeDate(basePKFilter.LB), closed, false)
		case types.T_time:
			sortedSearchFunc = vector.OrderedSearchOffsetsByLess(types.DecodeTime(basePKFilter.LB), closed, true)
			unSortedSearchFunc = vector.OrderedSearchOffsetsByLess(types.DecodeTime(basePKFilter.LB), closed, false)
		case types.T_datetime:
			sortedSearchFunc = vector.OrderedSearchOffsetsByLess(types.DecodeDatetime(basePKFilter.LB), closed, true)
			unSortedSearchFunc = vector.OrderedSearchOffsetsByLess(types.DecodeDatetime(basePKFilter.LB), closed, false)
		case types.T_timestamp:
			sortedSearchFunc = vector.OrderedSearchOffsetsByLess(types.DecodeTimestamp(basePKFilter.LB), closed, true)
			unSortedSearchFunc = vector.OrderedSearchOffsetsByLess(types.DecodeTimestamp(basePKFilter.LB), closed, false)
		case types.T_decimal64:
			sortedSearchFunc = vector.FixedSizeSearchOffsetsByLessTypeChecked(types.DecodeDecimal64(basePKFilter.LB), closed, true, types.CompareDecimal64)
			unSortedSearchFunc = vector.FixedSizeSearchOffsetsByLessTypeChecked(types.DecodeDecimal64(basePKFilter.LB), closed, false, types.CompareDecimal64)
		case types.T_decimal128:
			sortedSearchFunc = vector.FixedSizeSearchOffsetsByLessTypeChecked(types.DecodeDecimal128(basePKFilter.LB), closed, true, types.CompareDecimal128)
			unSortedSearchFunc = vector.FixedSizeSearchOffsetsByLessTypeChecked(types.DecodeDecimal128(basePKFilter.LB), closed, false, types.CompareDecimal128)
		case types.T_varchar:
			sortedSearchFunc = vector.VarlenSearchOffsetByLess(basePKFilter.LB, closed, true)
			unSortedSearchFunc = vector.VarlenSearchOffsetByLess(basePKFilter.LB, closed, false)
		case types.T_json:
			sortedSearchFunc = vector.VarlenSearchOffsetByLess(basePKFilter.LB, closed, true)
			unSortedSearchFunc = vector.VarlenSearchOffsetByLess(basePKFilter.LB, closed, false)
		case types.T_enum:
			sortedSearchFunc = vector.OrderedSearchOffsetsByLess(types.DecodeEnum(basePKFilter.LB), closed, true)
			unSortedSearchFunc = vector.OrderedSearchOffsetsByLess(types.DecodeEnum(basePKFilter.LB), closed, false)
		}

	case function.GREAT_EQUAL, function.GREAT_THAN:
		closed := basePKFilter.Op == function.GREAT_EQUAL
		switch basePKFilter.Oid {
		case types.T_int8:
			sortedSearchFunc = vector.OrderedSearchOffsetsByGreat(types.DecodeInt8(basePKFilter.LB), closed, true)
			unSortedSearchFunc = vector.OrderedSearchOffsetsByGreat(types.DecodeInt8(basePKFilter.LB), closed, false)
		case types.T_int16:
			sortedSearchFunc = vector.OrderedSearchOffsetsByGreat(types.DecodeInt16(basePKFilter.LB), closed, true)
			unSortedSearchFunc = vector.OrderedSearchOffsetsByGreat(types.DecodeInt16(basePKFilter.LB), closed, false)
		case types.T_int32:
			sortedSearchFunc = vector.OrderedSearchOffsetsByGreat(types.DecodeInt32(basePKFilter.LB), closed, true)
			unSortedSearchFunc = vector.OrderedSearchOffsetsByGreat(types.DecodeInt32(basePKFilter.LB), closed, false)
		case types.T_int64:
			sortedSearchFunc = vector.OrderedSearchOffsetsByGreat(types.DecodeInt64(basePKFilter.LB), closed, true)
			unSortedSearchFunc = vector.OrderedSearchOffsetsByGreat(types.DecodeInt64(basePKFilter.LB), closed, false)
		case types.T_float32:
			sortedSearchFunc = vector.OrderedSearchOffsetsByGreat(types.DecodeFloat32(basePKFilter.LB), closed, true)
			unSortedSearchFunc = vector.OrderedSearchOffsetsByGreat(types.DecodeFloat32(basePKFilter.LB), closed, false)
		case types.T_float64:
			sortedSearchFunc = vector.OrderedSearchOffsetsByGreat(types.DecodeFloat64(basePKFilter.LB), closed, true)
			unSortedSearchFunc = vector.OrderedSearchOffsetsByGreat(types.DecodeFloat64(basePKFilter.LB), closed, false)
		case types.T_uint8:
			sortedSearchFunc = vector.OrderedSearchOffsetsByGreat(types.DecodeUint8(basePKFilter.LB), closed, true)
			unSortedSearchFunc = vector.OrderedSearchOffsetsByGreat(types.DecodeUint8(basePKFilter.LB), closed, false)
		case types.T_uint16:
			sortedSearchFunc = vector.OrderedSearchOffsetsByGreat(types.DecodeUint16(basePKFilter.LB), closed, true)
			unSortedSearchFunc = vector.OrderedSearchOffsetsByGreat(types.DecodeUint16(basePKFilter.LB), closed, false)
		case types.T_uint32:
			sortedSearchFunc = vector.OrderedSearchOffsetsByGreat(types.DecodeUint32(basePKFilter.LB), closed, true)
			unSortedSearchFunc = vector.OrderedSearchOffsetsByGreat(types.DecodeUint32(basePKFilter.LB), closed, false)
		case types.T_uint64:
			sortedSearchFunc = vector.OrderedSearchOffsetsByGreat(types.DecodeUint64(basePKFilter.LB), closed, true)
			unSortedSearchFunc = vector.OrderedSearchOffsetsByGreat(types.DecodeUint64(basePKFilter.LB), closed, false)
		case types.T_date:
			sortedSearchFunc = vector.OrderedSearchOffsetsByGreat(types.DecodeDate(basePKFilter.LB), closed, true)
			unSortedSearchFunc = vector.OrderedSearchOffsetsByGreat(types.DecodeDate(basePKFilter.LB), closed, false)
		case types.T_time:
			sortedSearchFunc = vector.OrderedSearchOffsetsByGreat(types.DecodeTime(basePKFilter.LB), closed, true)
			unSortedSearchFunc = vector.OrderedSearchOffsetsByGreat(types.DecodeTime(basePKFilter.LB), closed, false)
		case types.T_datetime:
			sortedSearchFunc = vector.OrderedSearchOffsetsByGreat(types.DecodeDatetime(basePKFilter.LB), closed, true)
			unSortedSearchFunc = vector.OrderedSearchOffsetsByGreat(types.DecodeDatetime(basePKFilter.LB), closed, false)
		case types.T_timestamp:
			sortedSearchFunc = vector.OrderedSearchOffsetsByGreat(types.DecodeTimestamp(basePKFilter.LB), closed, true)
			unSortedSearchFunc = vector.OrderedSearchOffsetsByGreat(types.DecodeTimestamp(basePKFilter.LB), closed, false)
		case types.T_decimal64:
			sortedSearchFunc = vector.FixedSizeSearchOffsetsByGTTypeChecked(types.DecodeDecimal64(basePKFilter.LB), closed, true, types.CompareDecimal64)
			unSortedSearchFunc = vector.FixedSizeSearchOffsetsByGTTypeChecked(types.DecodeDecimal64(basePKFilter.LB), closed, false, types.CompareDecimal64)
		case types.T_decimal128:
			sortedSearchFunc = vector.FixedSizeSearchOffsetsByGTTypeChecked(types.DecodeDecimal128(basePKFilter.LB), closed, true, types.CompareDecimal128)
			unSortedSearchFunc = vector.FixedSizeSearchOffsetsByGTTypeChecked(types.DecodeDecimal128(basePKFilter.LB), closed, false, types.CompareDecimal128)
		case types.T_varchar:
			sortedSearchFunc = vector.VarlenSearchOffsetByGreat(basePKFilter.LB, closed, true)
			unSortedSearchFunc = vector.VarlenSearchOffsetByGreat(basePKFilter.LB, closed, false)
		case types.T_json:
			sortedSearchFunc = vector.VarlenSearchOffsetByGreat(basePKFilter.LB, closed, true)
			unSortedSearchFunc = vector.VarlenSearchOffsetByGreat(basePKFilter.LB, closed, false)
		case types.T_enum:
			sortedSearchFunc = vector.OrderedSearchOffsetsByGreat(types.DecodeEnum(basePKFilter.LB), closed, true)
			unSortedSearchFunc = vector.OrderedSearchOffsetsByGreat(types.DecodeEnum(basePKFilter.LB), closed, false)
		}

	case function.BETWEEN, RangeLeftOpen, RangeRightOpen, RangeBothOpen:
		var hint int
		switch basePKFilter.Op {
		case function.BETWEEN:
			hint = 0
		case RangeLeftOpen:
			hint = 1
		case RangeRightOpen:
			hint = 2
		case RangeBothOpen:
			hint = 3
		}
		switch basePKFilter.Oid {
		case types.T_int8:
			lb := types.DecodeInt8(basePKFilter.LB)
			ub := types.DecodeInt8(basePKFilter.UB)
			sortedSearchFunc = vector.CollectOffsetsByBetweenFactory(lb, ub, hint)
			unSortedSearchFunc = vector.LinearCollectOffsetsByBetweenFactory(lb, ub, hint)
		case types.T_int16:
			lb := types.DecodeInt16(basePKFilter.LB)
			ub := types.DecodeInt16(basePKFilter.UB)
			sortedSearchFunc = vector.CollectOffsetsByBetweenFactory(lb, ub, hint)
			unSortedSearchFunc = vector.LinearCollectOffsetsByBetweenFactory(lb, ub, hint)
		case types.T_int32:
			lb := types.DecodeInt32(basePKFilter.LB)
			ub := types.DecodeInt32(basePKFilter.UB)
			sortedSearchFunc = vector.CollectOffsetsByBetweenFactory(lb, ub, hint)
			unSortedSearchFunc = vector.LinearCollectOffsetsByBetweenFactory(lb, ub, hint)
		case types.T_int64:
			lb := types.DecodeInt64(basePKFilter.LB)
			ub := types.DecodeInt64(basePKFilter.UB)
			sortedSearchFunc = vector.CollectOffsetsByBetweenFactory(lb, ub, hint)
			unSortedSearchFunc = vector.LinearCollectOffsetsByBetweenFactory(lb, ub, hint)
		case types.T_float32:
			lb := types.DecodeFloat32(basePKFilter.LB)
			ub := types.DecodeFloat32(basePKFilter.UB)
			sortedSearchFunc = vector.CollectOffsetsByBetweenFactory(lb, ub, hint)
			unSortedSearchFunc = vector.LinearCollectOffsetsByBetweenFactory(lb, ub, hint)
		case types.T_float64:
			lb := types.DecodeFloat64(basePKFilter.LB)
			ub := types.DecodeFloat64(basePKFilter.UB)
			sortedSearchFunc = vector.CollectOffsetsByBetweenFactory(lb, ub, hint)
			unSortedSearchFunc = vector.LinearCollectOffsetsByBetweenFactory(lb, ub, hint)
		case types.T_uint8:
			lb := types.DecodeUint8(basePKFilter.LB)
			ub := types.DecodeUint8(basePKFilter.UB)
			sortedSearchFunc = vector.CollectOffsetsByBetweenFactory(lb, ub, hint)
			unSortedSearchFunc = vector.LinearCollectOffsetsByBetweenFactory(lb, ub, hint)
		case types.T_uint16:
			lb := types.DecodeUint16(basePKFilter.LB)
			ub := types.DecodeUint16(basePKFilter.UB)
			sortedSearchFunc = vector.CollectOffsetsByBetweenFactory(lb, ub, hint)
			unSortedSearchFunc = vector.LinearCollectOffsetsByBetweenFactory(lb, ub, hint)
		case types.T_uint32:
			lb := types.DecodeUint32(basePKFilter.LB)
			ub := types.DecodeUint32(basePKFilter.UB)
			sortedSearchFunc = vector.CollectOffsetsByBetweenFactory(lb, ub, hint)
			unSortedSearchFunc = vector.LinearCollectOffsetsByBetweenFactory(lb, ub, hint)
		case types.T_uint64:
			lb := types.DecodeUint64(basePKFilter.LB)
			ub := types.DecodeUint64(basePKFilter.UB)
			sortedSearchFunc = vector.CollectOffsetsByBetweenFactory(lb, ub, hint)
			unSortedSearchFunc = vector.LinearCollectOffsetsByBetweenFactory(lb, ub, hint)
		case types.T_date:
			lb := types.DecodeDate(basePKFilter.LB)
			ub := types.DecodeDate(basePKFilter.UB)
			sortedSearchFunc = vector.CollectOffsetsByBetweenFactory(lb, ub, hint)
			unSortedSearchFunc = vector.LinearCollectOffsetsByBetweenFactory(lb, ub, hint)
		case types.T_time:
			lb := types.DecodeTime(basePKFilter.LB)
			ub := types.DecodeTime(basePKFilter.UB)
			sortedSearchFunc = vector.CollectOffsetsByBetweenFactory(lb, ub, hint)
			unSortedSearchFunc = vector.LinearCollectOffsetsByBetweenFactory(lb, ub, hint)
		case types.T_datetime:
			lb := types.DecodeDatetime(basePKFilter.LB)
			ub := types.DecodeDatetime(basePKFilter.UB)
			sortedSearchFunc = vector.CollectOffsetsByBetweenFactory(lb, ub, hint)
			unSortedSearchFunc = vector.LinearCollectOffsetsByBetweenFactory(lb, ub, hint)
		case types.T_timestamp:
			lb := types.DecodeTimestamp(basePKFilter.LB)
			ub := types.DecodeTimestamp(basePKFilter.UB)
			sortedSearchFunc = vector.CollectOffsetsByBetweenFactory(lb, ub, hint)
			unSortedSearchFunc = vector.LinearCollectOffsetsByBetweenFactory(lb, ub, hint)
		case types.T_decimal64:
			lb := types.DecodeDecimal64(basePKFilter.LB)
			ub := types.DecodeDecimal64(basePKFilter.UB)
			sortedSearchFunc = vector.CollectOffsetsByBetweenFactory(lb, ub, hint)
			unSortedSearchFunc = vector.LinearCollectOffsetsByBetweenFactory(lb, ub, hint)
		case types.T_decimal128:
			val1 := types.DecodeDecimal128(basePKFilter.LB)
			val2 := types.DecodeDecimal128(basePKFilter.UB)

			sortedSearchFunc = vector.CollectOffsetsByBetweenWithCompareFactory(val1, val2, types.CompareDecimal128)
			unSortedSearchFunc = vector.FixedSizedLinearCollectOffsetsByBetweenFactory(val1, val2, types.CompareDecimal128)
		case types.T_text, types.T_datalink, types.T_varchar:
			lb := string(basePKFilter.LB)
			ub := string(basePKFilter.UB)
			sortedSearchFunc = vector.CollectOffsetsByBetweenFactory(lb, ub, hint)
			unSortedSearchFunc = vector.LinearCollectOffsetsByBetweenFactory(lb, ub, hint)
		case types.T_json:
			lb := string(basePKFilter.LB)
			ub := string(basePKFilter.UB)
			sortedSearchFunc = vector.CollectOffsetsByBetweenFactory(lb, ub, hint)
			unSortedSearchFunc = vector.LinearCollectOffsetsByBetweenFactory(lb, ub, hint)
		case types.T_enum:
			lb := types.DecodeEnum(basePKFilter.LB)
			ub := types.DecodeEnum(basePKFilter.UB)
			sortedSearchFunc = vector.CollectOffsetsByBetweenFactory(lb, ub, hint)
			unSortedSearchFunc = vector.LinearCollectOffsetsByBetweenFactory(lb, ub, hint)
		}
	}

	if sortedSearchFunc != nil {
		readFilter.SortedSearchFunc = func(vecs []*vector.Vector) []int64 {
			return sortedSearchFunc(vecs[0])
		}
		readFilter.UnSortedSearchFunc = func(vecs []*vector.Vector) []int64 {
			return unSortedSearchFunc(vecs[0])
		}
		readFilter.Valid = true
		return readFilter, nil
	}
	return readFilter, nil
}

func mergeBaseFilterInKind(
	left, right BasePKFilter, isOR bool, proc *process.Process,
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
			err = vector.Union2VectorOrdered(a, b, ret.Vec, proc.Mp(), cmp)
		} else {
			err = vector.Intersection2VectorOrdered(a, b, ret.Vec, proc.Mp(), cmp)
		}
	case types.T_int16:
		a := vector.MustFixedColNoTypeCheck[int16](va)
		b := vector.MustFixedColNoTypeCheck[int16](vb)
		cmp := func(x, y int16) int { return int(x - y) }

		if isOR {
			err = vector.Union2VectorOrdered(a, b, ret.Vec, proc.Mp(), cmp)
		} else {
			err = vector.Intersection2VectorOrdered(a, b, ret.Vec, proc.Mp(), cmp)
		}
	case types.T_int32:
		a := vector.MustFixedColNoTypeCheck[int32](va)
		b := vector.MustFixedColNoTypeCheck[int32](vb)
		cmp := func(x, y int32) int { return int(x - y) }

		if isOR {
			err = vector.Union2VectorOrdered(a, b, ret.Vec, proc.Mp(), cmp)
		} else {
			err = vector.Intersection2VectorOrdered(a, b, ret.Vec, proc.Mp(), cmp)
		}
	case types.T_int64:
		a := vector.MustFixedColNoTypeCheck[int64](va)
		b := vector.MustFixedColNoTypeCheck[int64](vb)
		cmp := func(x, y int64) int { return int(x - y) }

		if isOR {
			err = vector.Union2VectorOrdered(a, b, ret.Vec, proc.Mp(), cmp)
		} else {
			err = vector.Intersection2VectorOrdered(a, b, ret.Vec, proc.Mp(), cmp)
		}
	case types.T_float32:
		a := vector.MustFixedColNoTypeCheck[float32](va)
		b := vector.MustFixedColNoTypeCheck[float32](vb)
		cmp := func(x, y float32) int { return int(x - y) }

		if isOR {
			err = vector.Union2VectorOrdered(a, b, ret.Vec, proc.Mp(), cmp)
		} else {
			err = vector.Intersection2VectorOrdered(a, b, ret.Vec, proc.Mp(), cmp)
		}
	case types.T_float64:
		a := vector.MustFixedColNoTypeCheck[float64](va)
		b := vector.MustFixedColNoTypeCheck[float64](vb)
		cmp := func(x, y float64) int { return int(x - y) }

		if isOR {
			err = vector.Union2VectorOrdered(a, b, ret.Vec, proc.Mp(), cmp)
		} else {
			err = vector.Intersection2VectorOrdered(a, b, ret.Vec, proc.Mp(), cmp)
		}
	case types.T_uint8:
		a := vector.MustFixedColNoTypeCheck[uint8](va)
		b := vector.MustFixedColNoTypeCheck[uint8](vb)
		cmp := func(x, y uint8) int { return int(x) - int(y) }

		if isOR {
			err = vector.Union2VectorOrdered(a, b, ret.Vec, proc.Mp(), cmp)
		} else {
			err = vector.Intersection2VectorOrdered(a, b, ret.Vec, proc.Mp(), cmp)
		}
	case types.T_uint16:
		a := vector.MustFixedColNoTypeCheck[uint16](va)
		b := vector.MustFixedColNoTypeCheck[uint16](vb)
		cmp := func(x, y uint16) int { return int(x) - int(y) }

		if isOR {
			err = vector.Union2VectorOrdered(a, b, ret.Vec, proc.Mp(), cmp)
		} else {
			err = vector.Intersection2VectorOrdered(a, b, ret.Vec, proc.Mp(), cmp)
		}
	case types.T_uint32:
		a := vector.MustFixedColNoTypeCheck[uint32](va)
		b := vector.MustFixedColNoTypeCheck[uint32](vb)
		cmp := func(x, y uint32) int { return int(x) - int(y) }

		if isOR {
			err = vector.Union2VectorOrdered(a, b, ret.Vec, proc.Mp(), cmp)
		} else {
			err = vector.Intersection2VectorOrdered(a, b, ret.Vec, proc.Mp(), cmp)
		}
	case types.T_uint64:
		a := vector.MustFixedColNoTypeCheck[uint64](va)
		b := vector.MustFixedColNoTypeCheck[uint64](vb)
		cmp := func(x, y uint64) int { return int(x) - int(y) }

		if isOR {
			err = vector.Union2VectorOrdered(a, b, ret.Vec, proc.Mp(), cmp)
		} else {
			err = vector.Intersection2VectorOrdered(a, b, ret.Vec, proc.Mp(), cmp)
		}
	case types.T_date:
		a := vector.MustFixedColNoTypeCheck[types.Date](va)
		b := vector.MustFixedColNoTypeCheck[types.Date](vb)
		cmp := func(x, y types.Date) int { return int(x - y) }

		if isOR {
			err = vector.Union2VectorOrdered(a, b, ret.Vec, proc.Mp(), cmp)
		} else {
			err = vector.Intersection2VectorOrdered(a, b, ret.Vec, proc.Mp(), cmp)
		}
	case types.T_time:
		a := vector.MustFixedColNoTypeCheck[types.Time](va)
		b := vector.MustFixedColNoTypeCheck[types.Time](vb)
		cmp := func(x, y types.Time) int { return int(x - y) }

		if isOR {
			err = vector.Union2VectorOrdered(a, b, ret.Vec, proc.Mp(), cmp)
		} else {
			err = vector.Intersection2VectorOrdered(a, b, ret.Vec, proc.Mp(), cmp)
		}
	case types.T_datetime:
		a := vector.MustFixedColNoTypeCheck[types.Datetime](va)
		b := vector.MustFixedColNoTypeCheck[types.Datetime](vb)
		cmp := func(x, y types.Datetime) int { return int(x - y) }

		if isOR {
			err = vector.Union2VectorOrdered(a, b, ret.Vec, proc.Mp(), cmp)
		} else {
			err = vector.Intersection2VectorOrdered(a, b, ret.Vec, proc.Mp(), cmp)
		}
	case types.T_timestamp:
		a := vector.MustFixedColNoTypeCheck[types.Timestamp](va)
		b := vector.MustFixedColNoTypeCheck[types.Timestamp](vb)
		cmp := func(x, y types.Timestamp) int { return int(x - y) }
		if isOR {
			err = vector.Union2VectorOrdered(a, b, ret.Vec, proc.Mp(), cmp)
		} else {
			err = vector.Intersection2VectorOrdered(a, b, ret.Vec, proc.Mp(), cmp)
		}
	case types.T_decimal64:
		a := vector.MustFixedColNoTypeCheck[types.Decimal64](va)
		b := vector.MustFixedColNoTypeCheck[types.Decimal64](vb)
		cmp := func(x, y types.Decimal64) int { return int(x - y) }
		if isOR {
			err = vector.Union2VectorOrdered(a, b, ret.Vec, proc.Mp(), cmp)
		} else {
			err = vector.Intersection2VectorOrdered(a, b, ret.Vec, proc.Mp(), cmp)
		}

	case types.T_decimal128:
		a := vector.MustFixedColNoTypeCheck[types.Decimal128](va)
		b := vector.MustFixedColNoTypeCheck[types.Decimal128](vb)
		if isOR {
			err = vector.Union2VectorOrdered(a, b, ret.Vec, proc.Mp(),
				func(x, y types.Decimal128) int { return types.CompareDecimal128(x, y) })
		} else {
			err = vector.Intersection2VectorOrdered(a, b, ret.Vec, proc.Mp(),
				func(x, y types.Decimal128) int { return types.CompareDecimal128(x, y) })
		}

	case types.T_varchar, types.T_char, types.T_json, types.T_binary, types.T_text, types.T_datalink:
		if isOR {
			err = vector.Union2VectorValen(va, vb, ret.Vec, proc.Mp())
		} else {
			err = vector.Intersection2VectorVarlen(va, vb, ret.Vec, proc.Mp())
		}

	case types.T_enum:
		a := vector.MustFixedColNoTypeCheck[types.Enum](va)
		b := vector.MustFixedColNoTypeCheck[types.Enum](vb)
		cmp := func(x, y types.Enum) int { return int(x - y) }
		if isOR {
			err = vector.Union2VectorOrdered(a, b, ret.Vec, proc.Mp(), cmp)
		} else {
			err = vector.Intersection2VectorOrdered(a, b, ret.Vec, proc.Mp(), cmp)
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
	proc *process.Process,
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
				finalFilter, err = mergeBaseFilterInKind(*left, *right, false, proc)
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
				finalFilter, err = mergeBaseFilterInKind(*left, *right, true, proc)
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
