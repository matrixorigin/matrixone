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

package disttae

import (
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
)

func newBlockReadPKFilter(
	pkName string,
	basePKFilter basePKFilter,
) blockio.BlockReadFilter {
	if !basePKFilter.valid {
		return blockio.BlockReadFilter{}
	}

	var readFilter blockio.BlockReadFilter
	var sortedSearchFunc, unSortedSearchFunc func(*vector.Vector) []int32

	readFilter.HasFakePK = pkName == catalog.FakePrimaryKeyColName

	switch basePKFilter.op {
	case function.EQUAL:
		switch basePKFilter.oid {
		case types.T_int8:
			sortedSearchFunc = vector.OrderedBinarySearchOffsetByValFactory([]int8{types.DecodeInt8(basePKFilter.lb)})
			unSortedSearchFunc = vector.OrderedLinearSearchOffsetByValFactory([]int8{types.DecodeInt8(basePKFilter.lb)}, nil)
		case types.T_int16:
			sortedSearchFunc = vector.OrderedBinarySearchOffsetByValFactory([]int16{types.DecodeInt16(basePKFilter.lb)})
			unSortedSearchFunc = vector.OrderedLinearSearchOffsetByValFactory([]int16{types.DecodeInt16(basePKFilter.lb)}, nil)
		case types.T_int32:
			sortedSearchFunc = vector.OrderedBinarySearchOffsetByValFactory([]int32{types.DecodeInt32(basePKFilter.lb)})
			unSortedSearchFunc = vector.OrderedLinearSearchOffsetByValFactory([]int32{types.DecodeInt32(basePKFilter.lb)}, nil)
		case types.T_int64:
			sortedSearchFunc = vector.OrderedBinarySearchOffsetByValFactory([]int64{types.DecodeInt64(basePKFilter.lb)})
			unSortedSearchFunc = vector.OrderedLinearSearchOffsetByValFactory([]int64{types.DecodeInt64(basePKFilter.lb)}, nil)
		case types.T_float32:
			sortedSearchFunc = vector.OrderedBinarySearchOffsetByValFactory([]float32{types.DecodeFloat32(basePKFilter.lb)})
			unSortedSearchFunc = vector.OrderedLinearSearchOffsetByValFactory([]float32{types.DecodeFloat32(basePKFilter.lb)}, nil)
		case types.T_float64:
			sortedSearchFunc = vector.OrderedBinarySearchOffsetByValFactory([]float64{types.DecodeFloat64(basePKFilter.lb)})
			unSortedSearchFunc = vector.OrderedLinearSearchOffsetByValFactory([]float64{types.DecodeFloat64(basePKFilter.lb)}, nil)
		case types.T_uint8:
			sortedSearchFunc = vector.OrderedBinarySearchOffsetByValFactory([]uint8{uint8(types.DecodeUint8(basePKFilter.lb))})
			unSortedSearchFunc = vector.OrderedLinearSearchOffsetByValFactory([]uint8{uint8(types.DecodeUint8(basePKFilter.lb))}, nil)
		case types.T_uint16:
			sortedSearchFunc = vector.OrderedBinarySearchOffsetByValFactory([]uint16{uint16(types.DecodeUint16(basePKFilter.lb))})
			unSortedSearchFunc = vector.OrderedLinearSearchOffsetByValFactory([]uint16{uint16(types.DecodeUint16(basePKFilter.lb))}, nil)
		case types.T_uint32:
			sortedSearchFunc = vector.OrderedBinarySearchOffsetByValFactory([]uint32{types.DecodeUint32(basePKFilter.lb)})
			unSortedSearchFunc = vector.OrderedLinearSearchOffsetByValFactory([]uint32{types.DecodeUint32(basePKFilter.lb)}, nil)
		case types.T_uint64:
			sortedSearchFunc = vector.OrderedBinarySearchOffsetByValFactory([]uint64{types.DecodeUint64(basePKFilter.lb)})
			unSortedSearchFunc = vector.OrderedLinearSearchOffsetByValFactory([]uint64{types.DecodeUint64(basePKFilter.lb)}, nil)
		case types.T_date:
			sortedSearchFunc = vector.OrderedBinarySearchOffsetByValFactory([]types.Date{types.DecodeDate(basePKFilter.lb)})
			unSortedSearchFunc = vector.OrderedLinearSearchOffsetByValFactory([]types.Date{types.DecodeDate(basePKFilter.lb)}, nil)
		case types.T_time:
			sortedSearchFunc = vector.OrderedBinarySearchOffsetByValFactory([]types.Time{types.DecodeTime(basePKFilter.lb)})
			unSortedSearchFunc = vector.OrderedLinearSearchOffsetByValFactory([]types.Time{types.DecodeTime(basePKFilter.lb)}, nil)
		case types.T_datetime:
			sortedSearchFunc = vector.OrderedBinarySearchOffsetByValFactory([]types.Datetime{types.DecodeDatetime(basePKFilter.lb)})
			unSortedSearchFunc = vector.OrderedLinearSearchOffsetByValFactory([]types.Datetime{types.DecodeDatetime(basePKFilter.lb)}, nil)
		case types.T_timestamp:
			sortedSearchFunc = vector.OrderedBinarySearchOffsetByValFactory([]types.Timestamp{types.DecodeTimestamp(basePKFilter.lb)})
			unSortedSearchFunc = vector.OrderedLinearSearchOffsetByValFactory([]types.Timestamp{types.DecodeTimestamp(basePKFilter.lb)}, nil)
		case types.T_decimal64:
			sortedSearchFunc = vector.FixedSizedBinarySearchOffsetByValFactory([]types.Decimal64{types.DecodeDecimal64(basePKFilter.lb)}, types.CompareDecimal64)
			unSortedSearchFunc = vector.FixedSizeLinearSearchOffsetByValFactory([]types.Decimal64{types.DecodeDecimal64(basePKFilter.lb)}, types.CompareDecimal64)
		case types.T_decimal128:
			sortedSearchFunc = vector.FixedSizedBinarySearchOffsetByValFactory([]types.Decimal128{types.DecodeDecimal128(basePKFilter.lb)}, types.CompareDecimal128)
			unSortedSearchFunc = vector.FixedSizeLinearSearchOffsetByValFactory([]types.Decimal128{types.DecodeDecimal128(basePKFilter.lb)}, types.CompareDecimal128)
		case types.T_varchar:
			sortedSearchFunc = vector.VarlenBinarySearchOffsetByValFactory([][]byte{basePKFilter.lb})
			unSortedSearchFunc = vector.VarlenLinearSearchOffsetByValFactory([][]byte{basePKFilter.lb})
		case types.T_json:
			sortedSearchFunc = vector.VarlenBinarySearchOffsetByValFactory([][]byte{basePKFilter.lb})
			unSortedSearchFunc = vector.VarlenLinearSearchOffsetByValFactory([][]byte{basePKFilter.lb})
		case types.T_enum:
			sortedSearchFunc = vector.OrderedBinarySearchOffsetByValFactory([]types.Enum{types.DecodeEnum(basePKFilter.lb)})
			unSortedSearchFunc = vector.OrderedLinearSearchOffsetByValFactory([]types.Enum{types.DecodeEnum(basePKFilter.lb)}, nil)
		}

	case function.PREFIX_EQ:
		sortedSearchFunc = vector.CollectOffsetsByPrefixEqFactory(basePKFilter.lb)
		unSortedSearchFunc = vector.LinearCollectOffsetsByPrefixEqFactory(basePKFilter.lb)

	case function.PREFIX_BETWEEN:
		sortedSearchFunc = vector.CollectOffsetsByPrefixBetweenFactory(basePKFilter.lb, basePKFilter.ub)
		unSortedSearchFunc = vector.LinearCollectOffsetsByPrefixBetweenFactory(basePKFilter.lb, basePKFilter.ub)

	case function.IN:
		var ok bool
		var vec *vector.Vector
		if vec, ok = basePKFilter.vec.(*vector.Vector); !ok {
			vec = vector.NewVec(types.T_any.ToType())
			vec.UnmarshalBinary(basePKFilter.vec.([]byte))
		}

		switch vec.GetType().Oid {
		case types.T_bit:
			sortedSearchFunc = vector.OrderedBinarySearchOffsetByValFactory(vector.MustFixedCol[uint64](vec))
			unSortedSearchFunc = vector.OrderedLinearSearchOffsetByValFactory(vector.MustFixedCol[uint64](vec), nil)
		case types.T_int8:
			sortedSearchFunc = vector.OrderedBinarySearchOffsetByValFactory(vector.MustFixedCol[int8](vec))
			unSortedSearchFunc = vector.OrderedLinearSearchOffsetByValFactory(vector.MustFixedCol[int8](vec), nil)
		case types.T_int16:
			sortedSearchFunc = vector.OrderedBinarySearchOffsetByValFactory(vector.MustFixedCol[int16](vec))
			unSortedSearchFunc = vector.OrderedLinearSearchOffsetByValFactory(vector.MustFixedCol[int16](vec), nil)
		case types.T_int32:
			sortedSearchFunc = vector.OrderedBinarySearchOffsetByValFactory(vector.MustFixedCol[int32](vec))
			unSortedSearchFunc = vector.OrderedLinearSearchOffsetByValFactory(vector.MustFixedCol[int32](vec), nil)
		case types.T_int64:
			sortedSearchFunc = vector.OrderedBinarySearchOffsetByValFactory(vector.MustFixedCol[int64](vec))
			unSortedSearchFunc = vector.OrderedLinearSearchOffsetByValFactory(vector.MustFixedCol[int64](vec), nil)
		case types.T_uint8:
			sortedSearchFunc = vector.OrderedBinarySearchOffsetByValFactory(vector.MustFixedCol[uint8](vec))
			unSortedSearchFunc = vector.OrderedLinearSearchOffsetByValFactory(vector.MustFixedCol[uint8](vec), nil)
		case types.T_uint16:
			sortedSearchFunc = vector.OrderedBinarySearchOffsetByValFactory(vector.MustFixedCol[uint16](vec))
			unSortedSearchFunc = vector.OrderedLinearSearchOffsetByValFactory(vector.MustFixedCol[uint16](vec), nil)
		case types.T_uint32:
			sortedSearchFunc = vector.OrderedBinarySearchOffsetByValFactory(vector.MustFixedCol[uint32](vec))
			unSortedSearchFunc = vector.OrderedLinearSearchOffsetByValFactory(vector.MustFixedCol[uint32](vec), nil)
		case types.T_uint64:
			sortedSearchFunc = vector.OrderedBinarySearchOffsetByValFactory(vector.MustFixedCol[uint64](vec))
			unSortedSearchFunc = vector.OrderedLinearSearchOffsetByValFactory(vector.MustFixedCol[uint64](vec), nil)
		case types.T_float32:
			sortedSearchFunc = vector.OrderedBinarySearchOffsetByValFactory(vector.MustFixedCol[float32](vec))
			unSortedSearchFunc = vector.OrderedLinearSearchOffsetByValFactory(vector.MustFixedCol[float32](vec), nil)
		case types.T_float64:
			sortedSearchFunc = vector.OrderedBinarySearchOffsetByValFactory(vector.MustFixedCol[float64](vec))
			unSortedSearchFunc = vector.OrderedLinearSearchOffsetByValFactory(vector.MustFixedCol[float64](vec), nil)
		case types.T_date:
			sortedSearchFunc = vector.OrderedBinarySearchOffsetByValFactory(vector.MustFixedCol[types.Date](vec))
			unSortedSearchFunc = vector.OrderedLinearSearchOffsetByValFactory(vector.MustFixedCol[types.Date](vec), nil)
		case types.T_time:
			sortedSearchFunc = vector.OrderedBinarySearchOffsetByValFactory(vector.MustFixedCol[types.Time](vec))
			unSortedSearchFunc = vector.OrderedLinearSearchOffsetByValFactory(vector.MustFixedCol[types.Time](vec), nil)
		case types.T_datetime:
			sortedSearchFunc = vector.OrderedBinarySearchOffsetByValFactory(vector.MustFixedCol[types.Datetime](vec))
			unSortedSearchFunc = vector.OrderedLinearSearchOffsetByValFactory(vector.MustFixedCol[types.Datetime](vec), nil)
		case types.T_timestamp:
			sortedSearchFunc = vector.OrderedBinarySearchOffsetByValFactory(vector.MustFixedCol[types.Timestamp](vec))
			unSortedSearchFunc = vector.OrderedLinearSearchOffsetByValFactory(vector.MustFixedCol[types.Timestamp](vec), nil)
		case types.T_decimal64:
			sortedSearchFunc = vector.FixedSizedBinarySearchOffsetByValFactory(vector.MustFixedCol[types.Decimal64](vec), types.CompareDecimal64)
			unSortedSearchFunc = vector.FixedSizeLinearSearchOffsetByValFactory(vector.MustFixedCol[types.Decimal64](vec), types.CompareDecimal64)
		case types.T_decimal128:
			sortedSearchFunc = vector.FixedSizedBinarySearchOffsetByValFactory(vector.MustFixedCol[types.Decimal128](vec), types.CompareDecimal128)
			unSortedSearchFunc = vector.FixedSizeLinearSearchOffsetByValFactory(vector.MustFixedCol[types.Decimal128](vec), types.CompareDecimal128)
		case types.T_char, types.T_varchar, types.T_binary, types.T_varbinary, types.T_json, types.T_blob, types.T_text,
			types.T_array_float32, types.T_array_float64, types.T_datalink:
			sortedSearchFunc = vector.VarlenBinarySearchOffsetByValFactory(vector.InefficientMustBytesCol(vec))
			unSortedSearchFunc = vector.VarlenLinearSearchOffsetByValFactory(vector.InefficientMustBytesCol(vec))
		case types.T_enum:
			sortedSearchFunc = vector.OrderedBinarySearchOffsetByValFactory(vector.MustFixedCol[types.Enum](vec))
			unSortedSearchFunc = vector.OrderedLinearSearchOffsetByValFactory(vector.MustFixedCol[types.Enum](vec), nil)
		}

	case function.PREFIX_IN:
		var ok bool
		var vec *vector.Vector
		if vec, ok = basePKFilter.vec.(*vector.Vector); !ok {
			vec = vector.NewVec(types.T_any.ToType())
			vec.UnmarshalBinary(basePKFilter.vec.([]byte))
		}

		sortedSearchFunc = vector.CollectOffsetsByPrefixInFactory(vec)
		unSortedSearchFunc = vector.LinearCollectOffsetsByPrefixInFactory(vec)

	case function.LESS_EQUAL, function.LESS_THAN:
		closed := basePKFilter.op == function.LESS_EQUAL
		switch basePKFilter.oid {
		case types.T_int8:
			sortedSearchFunc = vector.OrderedSearchOffsetsByLess(types.DecodeInt8(basePKFilter.lb), closed, true)
			unSortedSearchFunc = vector.OrderedSearchOffsetsByLess(types.DecodeInt8(basePKFilter.lb), closed, false)
		case types.T_int16:
			sortedSearchFunc = vector.OrderedSearchOffsetsByLess(types.DecodeInt16(basePKFilter.lb), closed, true)
			unSortedSearchFunc = vector.OrderedSearchOffsetsByLess(types.DecodeInt16(basePKFilter.lb), closed, false)
		case types.T_int32:
			sortedSearchFunc = vector.OrderedSearchOffsetsByLess(types.DecodeInt32(basePKFilter.lb), closed, true)
			unSortedSearchFunc = vector.OrderedSearchOffsetsByLess(types.DecodeInt32(basePKFilter.lb), closed, false)
		case types.T_int64:
			sortedSearchFunc = vector.OrderedSearchOffsetsByLess(types.DecodeInt64(basePKFilter.lb), closed, true)
			unSortedSearchFunc = vector.OrderedSearchOffsetsByLess(types.DecodeInt64(basePKFilter.lb), closed, false)
		case types.T_float32:
			sortedSearchFunc = vector.OrderedSearchOffsetsByLess(types.DecodeFloat32(basePKFilter.lb), closed, true)
			unSortedSearchFunc = vector.OrderedSearchOffsetsByLess(types.DecodeFloat32(basePKFilter.lb), closed, false)
		case types.T_float64:
			sortedSearchFunc = vector.OrderedSearchOffsetsByLess(types.DecodeFloat64(basePKFilter.lb), closed, true)
			unSortedSearchFunc = vector.OrderedSearchOffsetsByLess(types.DecodeFloat64(basePKFilter.lb), closed, false)
		case types.T_uint8:
			sortedSearchFunc = vector.OrderedSearchOffsetsByLess(types.DecodeUint8(basePKFilter.lb), closed, true)
			unSortedSearchFunc = vector.OrderedSearchOffsetsByLess(types.DecodeUint8(basePKFilter.lb), closed, false)
		case types.T_uint16:
			sortedSearchFunc = vector.OrderedSearchOffsetsByLess(types.DecodeUint16(basePKFilter.lb), closed, true)
			unSortedSearchFunc = vector.OrderedSearchOffsetsByLess(types.DecodeUint16(basePKFilter.lb), closed, false)
		case types.T_uint32:
			sortedSearchFunc = vector.OrderedSearchOffsetsByLess(types.DecodeUint32(basePKFilter.lb), closed, true)
			unSortedSearchFunc = vector.OrderedSearchOffsetsByLess(types.DecodeUint32(basePKFilter.lb), closed, false)
		case types.T_uint64:
			sortedSearchFunc = vector.OrderedSearchOffsetsByLess(types.DecodeUint64(basePKFilter.lb), closed, true)
			unSortedSearchFunc = vector.OrderedSearchOffsetsByLess(types.DecodeUint64(basePKFilter.lb), closed, false)
		case types.T_date:
			sortedSearchFunc = vector.OrderedSearchOffsetsByLess(types.DecodeDate(basePKFilter.lb), closed, true)
			unSortedSearchFunc = vector.OrderedSearchOffsetsByLess(types.DecodeDate(basePKFilter.lb), closed, false)
		case types.T_time:
			sortedSearchFunc = vector.OrderedSearchOffsetsByLess(types.DecodeTime(basePKFilter.lb), closed, true)
			unSortedSearchFunc = vector.OrderedSearchOffsetsByLess(types.DecodeTime(basePKFilter.lb), closed, false)
		case types.T_datetime:
			sortedSearchFunc = vector.OrderedSearchOffsetsByLess(types.DecodeDatetime(basePKFilter.lb), closed, true)
			unSortedSearchFunc = vector.OrderedSearchOffsetsByLess(types.DecodeDatetime(basePKFilter.lb), closed, false)
		case types.T_timestamp:
			sortedSearchFunc = vector.OrderedSearchOffsetsByLess(types.DecodeTimestamp(basePKFilter.lb), closed, true)
			unSortedSearchFunc = vector.OrderedSearchOffsetsByLess(types.DecodeTimestamp(basePKFilter.lb), closed, false)
		case types.T_decimal64:
			sortedSearchFunc = vector.FixedSizeSearchOffsetsByLess(types.DecodeDecimal64(basePKFilter.lb), closed, true, types.CompareDecimal64)
			unSortedSearchFunc = vector.FixedSizeSearchOffsetsByLess(types.DecodeDecimal64(basePKFilter.lb), closed, false, types.CompareDecimal64)
		case types.T_decimal128:
			sortedSearchFunc = vector.FixedSizeSearchOffsetsByLess(types.DecodeDecimal128(basePKFilter.lb), closed, true, types.CompareDecimal128)
			unSortedSearchFunc = vector.FixedSizeSearchOffsetsByLess(types.DecodeDecimal128(basePKFilter.lb), closed, false, types.CompareDecimal128)
		case types.T_varchar:
			sortedSearchFunc = vector.VarlenSearchOffsetByLess(basePKFilter.lb, closed, true)
			unSortedSearchFunc = vector.VarlenSearchOffsetByLess(basePKFilter.lb, closed, false)
		case types.T_json:
			sortedSearchFunc = vector.VarlenSearchOffsetByLess(basePKFilter.lb, closed, true)
			unSortedSearchFunc = vector.VarlenSearchOffsetByLess(basePKFilter.lb, closed, false)
		case types.T_enum:
			sortedSearchFunc = vector.OrderedSearchOffsetsByLess(types.DecodeEnum(basePKFilter.lb), closed, true)
			unSortedSearchFunc = vector.OrderedSearchOffsetsByLess(types.DecodeEnum(basePKFilter.lb), closed, false)
		}

	case function.GREAT_EQUAL, function.GREAT_THAN:
		closed := basePKFilter.op == function.GREAT_EQUAL
		switch basePKFilter.oid {
		case types.T_int8:
			sortedSearchFunc = vector.OrderedSearchOffsetsByGreat(types.DecodeInt8(basePKFilter.lb), closed, true)
			unSortedSearchFunc = vector.OrderedSearchOffsetsByGreat(types.DecodeInt8(basePKFilter.lb), closed, false)
		case types.T_int16:
			sortedSearchFunc = vector.OrderedSearchOffsetsByGreat(types.DecodeInt16(basePKFilter.lb), closed, true)
			unSortedSearchFunc = vector.OrderedSearchOffsetsByGreat(types.DecodeInt16(basePKFilter.lb), closed, false)
		case types.T_int32:
			sortedSearchFunc = vector.OrderedSearchOffsetsByGreat(types.DecodeInt32(basePKFilter.lb), closed, true)
			unSortedSearchFunc = vector.OrderedSearchOffsetsByGreat(types.DecodeInt32(basePKFilter.lb), closed, false)
		case types.T_int64:
			sortedSearchFunc = vector.OrderedSearchOffsetsByGreat(types.DecodeInt64(basePKFilter.lb), closed, true)
			unSortedSearchFunc = vector.OrderedSearchOffsetsByGreat(types.DecodeInt64(basePKFilter.lb), closed, false)
		case types.T_float32:
			sortedSearchFunc = vector.OrderedSearchOffsetsByGreat(types.DecodeFloat32(basePKFilter.lb), closed, true)
			unSortedSearchFunc = vector.OrderedSearchOffsetsByGreat(types.DecodeFloat32(basePKFilter.lb), closed, false)
		case types.T_float64:
			sortedSearchFunc = vector.OrderedSearchOffsetsByGreat(types.DecodeFloat64(basePKFilter.lb), closed, true)
			unSortedSearchFunc = vector.OrderedSearchOffsetsByGreat(types.DecodeFloat64(basePKFilter.lb), closed, false)
		case types.T_uint8:
			sortedSearchFunc = vector.OrderedSearchOffsetsByGreat(types.DecodeUint8(basePKFilter.lb), closed, true)
			unSortedSearchFunc = vector.OrderedSearchOffsetsByGreat(types.DecodeUint8(basePKFilter.lb), closed, false)
		case types.T_uint16:
			sortedSearchFunc = vector.OrderedSearchOffsetsByGreat(types.DecodeUint16(basePKFilter.lb), closed, true)
			unSortedSearchFunc = vector.OrderedSearchOffsetsByGreat(types.DecodeUint16(basePKFilter.lb), closed, false)
		case types.T_uint32:
			sortedSearchFunc = vector.OrderedSearchOffsetsByGreat(types.DecodeUint32(basePKFilter.lb), closed, true)
			unSortedSearchFunc = vector.OrderedSearchOffsetsByGreat(types.DecodeUint32(basePKFilter.lb), closed, false)
		case types.T_uint64:
			sortedSearchFunc = vector.OrderedSearchOffsetsByGreat(types.DecodeUint64(basePKFilter.lb), closed, true)
			unSortedSearchFunc = vector.OrderedSearchOffsetsByGreat(types.DecodeUint64(basePKFilter.lb), closed, false)
		case types.T_date:
			sortedSearchFunc = vector.OrderedSearchOffsetsByGreat(types.DecodeDate(basePKFilter.lb), closed, true)
			unSortedSearchFunc = vector.OrderedSearchOffsetsByGreat(types.DecodeDate(basePKFilter.lb), closed, false)
		case types.T_time:
			sortedSearchFunc = vector.OrderedSearchOffsetsByGreat(types.DecodeTime(basePKFilter.lb), closed, true)
			unSortedSearchFunc = vector.OrderedSearchOffsetsByGreat(types.DecodeTime(basePKFilter.lb), closed, false)
		case types.T_datetime:
			sortedSearchFunc = vector.OrderedSearchOffsetsByGreat(types.DecodeDatetime(basePKFilter.lb), closed, true)
			unSortedSearchFunc = vector.OrderedSearchOffsetsByGreat(types.DecodeDatetime(basePKFilter.lb), closed, false)
		case types.T_timestamp:
			sortedSearchFunc = vector.OrderedSearchOffsetsByGreat(types.DecodeTimestamp(basePKFilter.lb), closed, true)
			unSortedSearchFunc = vector.OrderedSearchOffsetsByGreat(types.DecodeTimestamp(basePKFilter.lb), closed, false)
		case types.T_decimal64:
			sortedSearchFunc = vector.FixedSizeSearchOffsetsByGreat(types.DecodeDecimal64(basePKFilter.lb), closed, true, types.CompareDecimal64)
			unSortedSearchFunc = vector.FixedSizeSearchOffsetsByGreat(types.DecodeDecimal64(basePKFilter.lb), closed, false, types.CompareDecimal64)
		case types.T_decimal128:
			sortedSearchFunc = vector.FixedSizeSearchOffsetsByGreat(types.DecodeDecimal128(basePKFilter.lb), closed, true, types.CompareDecimal128)
			unSortedSearchFunc = vector.FixedSizeSearchOffsetsByGreat(types.DecodeDecimal128(basePKFilter.lb), closed, false, types.CompareDecimal128)
		case types.T_varchar:
			sortedSearchFunc = vector.VarlenSearchOffsetByGreat(basePKFilter.lb, closed, true)
			unSortedSearchFunc = vector.VarlenSearchOffsetByGreat(basePKFilter.lb, closed, false)
		case types.T_json:
			sortedSearchFunc = vector.VarlenSearchOffsetByGreat(basePKFilter.lb, closed, true)
			unSortedSearchFunc = vector.VarlenSearchOffsetByGreat(basePKFilter.lb, closed, false)
		case types.T_enum:
			sortedSearchFunc = vector.OrderedSearchOffsetsByGreat(types.DecodeEnum(basePKFilter.lb), closed, true)
			unSortedSearchFunc = vector.OrderedSearchOffsetsByGreat(types.DecodeEnum(basePKFilter.lb), closed, false)
		}

	case function.BETWEEN, rangeLeftOpen, rangeRightOpen, rangeBothOpen:
		var hint int
		switch basePKFilter.op {
		case function.BETWEEN:
			hint = 0
		case rangeLeftOpen:
			hint = 1
		case rangeRightOpen:
			hint = 2
		case rangeBothOpen:
			hint = 3
		}
		switch basePKFilter.oid {
		case types.T_int8:
			lb := types.DecodeInt8(basePKFilter.lb)
			ub := types.DecodeInt8(basePKFilter.ub)
			sortedSearchFunc = vector.CollectOffsetsByBetweenFactory(lb, ub, hint)
			unSortedSearchFunc = vector.LinearCollectOffsetsByBetweenFactory(lb, ub, hint)
		case types.T_int16:
			lb := types.DecodeInt16(basePKFilter.lb)
			ub := types.DecodeInt16(basePKFilter.ub)
			sortedSearchFunc = vector.CollectOffsetsByBetweenFactory(lb, ub, hint)
			unSortedSearchFunc = vector.LinearCollectOffsetsByBetweenFactory(lb, ub, hint)
		case types.T_int32:
			lb := types.DecodeInt32(basePKFilter.lb)
			ub := types.DecodeInt32(basePKFilter.ub)
			sortedSearchFunc = vector.CollectOffsetsByBetweenFactory(lb, ub, hint)
			unSortedSearchFunc = vector.LinearCollectOffsetsByBetweenFactory(lb, ub, hint)
		case types.T_int64:
			lb := types.DecodeInt64(basePKFilter.lb)
			ub := types.DecodeInt64(basePKFilter.ub)
			sortedSearchFunc = vector.CollectOffsetsByBetweenFactory(lb, ub, hint)
			unSortedSearchFunc = vector.LinearCollectOffsetsByBetweenFactory(lb, ub, hint)
		case types.T_float32:
			lb := types.DecodeFloat32(basePKFilter.lb)
			ub := types.DecodeFloat32(basePKFilter.ub)
			sortedSearchFunc = vector.CollectOffsetsByBetweenFactory(lb, ub, hint)
			unSortedSearchFunc = vector.LinearCollectOffsetsByBetweenFactory(lb, ub, hint)
		case types.T_float64:
			lb := types.DecodeFloat64(basePKFilter.lb)
			ub := types.DecodeFloat64(basePKFilter.ub)
			sortedSearchFunc = vector.CollectOffsetsByBetweenFactory(lb, ub, hint)
			unSortedSearchFunc = vector.LinearCollectOffsetsByBetweenFactory(lb, ub, hint)
		case types.T_uint8:
			lb := types.DecodeUint8(basePKFilter.lb)
			ub := types.DecodeUint8(basePKFilter.ub)
			sortedSearchFunc = vector.CollectOffsetsByBetweenFactory(lb, ub, hint)
			unSortedSearchFunc = vector.LinearCollectOffsetsByBetweenFactory(lb, ub, hint)
		case types.T_uint16:
			lb := types.DecodeUint16(basePKFilter.lb)
			ub := types.DecodeUint16(basePKFilter.ub)
			sortedSearchFunc = vector.CollectOffsetsByBetweenFactory(lb, ub, hint)
			unSortedSearchFunc = vector.LinearCollectOffsetsByBetweenFactory(lb, ub, hint)
		case types.T_uint32:
			lb := types.DecodeUint32(basePKFilter.lb)
			ub := types.DecodeUint32(basePKFilter.ub)
			sortedSearchFunc = vector.CollectOffsetsByBetweenFactory(lb, ub, hint)
			unSortedSearchFunc = vector.LinearCollectOffsetsByBetweenFactory(lb, ub, hint)
		case types.T_uint64:
			lb := types.DecodeUint64(basePKFilter.lb)
			ub := types.DecodeUint64(basePKFilter.ub)
			sortedSearchFunc = vector.CollectOffsetsByBetweenFactory(lb, ub, hint)
			unSortedSearchFunc = vector.LinearCollectOffsetsByBetweenFactory(lb, ub, hint)
		case types.T_date:
			lb := types.DecodeDate(basePKFilter.lb)
			ub := types.DecodeDate(basePKFilter.ub)
			sortedSearchFunc = vector.CollectOffsetsByBetweenFactory(lb, ub, hint)
			unSortedSearchFunc = vector.LinearCollectOffsetsByBetweenFactory(lb, ub, hint)
		case types.T_time:
			lb := types.DecodeTime(basePKFilter.lb)
			ub := types.DecodeTime(basePKFilter.ub)
			sortedSearchFunc = vector.CollectOffsetsByBetweenFactory(lb, ub, hint)
			unSortedSearchFunc = vector.LinearCollectOffsetsByBetweenFactory(lb, ub, hint)
		case types.T_datetime:
			lb := types.DecodeDatetime(basePKFilter.lb)
			ub := types.DecodeDatetime(basePKFilter.ub)
			sortedSearchFunc = vector.CollectOffsetsByBetweenFactory(lb, ub, hint)
			unSortedSearchFunc = vector.LinearCollectOffsetsByBetweenFactory(lb, ub, hint)
		case types.T_timestamp:
			lb := types.DecodeTimestamp(basePKFilter.lb)
			ub := types.DecodeTimestamp(basePKFilter.ub)
			sortedSearchFunc = vector.CollectOffsetsByBetweenFactory(lb, ub, hint)
			unSortedSearchFunc = vector.LinearCollectOffsetsByBetweenFactory(lb, ub, hint)
		case types.T_decimal64:
			lb := types.DecodeDecimal64(basePKFilter.lb)
			ub := types.DecodeDecimal64(basePKFilter.ub)
			sortedSearchFunc = vector.CollectOffsetsByBetweenFactory(lb, ub, hint)
			unSortedSearchFunc = vector.LinearCollectOffsetsByBetweenFactory(lb, ub, hint)
		case types.T_decimal128:
			val1 := types.DecodeDecimal128(basePKFilter.lb)
			val2 := types.DecodeDecimal128(basePKFilter.ub)

			sortedSearchFunc = vector.CollectOffsetsByBetweenWithCompareFactory(val1, val2, types.CompareDecimal128)
			unSortedSearchFunc = vector.FixedSizedLinearCollectOffsetsByBetweenFactory(val1, val2, types.CompareDecimal128)
		case types.T_text, types.T_datalink:
			lb := string(basePKFilter.lb)
			ub := string(basePKFilter.ub)
			sortedSearchFunc = vector.CollectOffsetsByBetweenFactory(lb, ub, hint)
			unSortedSearchFunc = vector.LinearCollectOffsetsByBetweenFactory(lb, ub, hint)
		case types.T_json:
			lb := string(basePKFilter.lb)
			ub := string(basePKFilter.ub)
			sortedSearchFunc = vector.CollectOffsetsByBetweenFactory(lb, ub, hint)
			unSortedSearchFunc = vector.LinearCollectOffsetsByBetweenFactory(lb, ub, hint)
		case types.T_enum:
			lb := types.DecodeEnum(basePKFilter.lb)
			ub := types.DecodeEnum(basePKFilter.ub)
			sortedSearchFunc = vector.CollectOffsetsByBetweenFactory(lb, ub, hint)
			unSortedSearchFunc = vector.LinearCollectOffsetsByBetweenFactory(lb, ub, hint)
		}
	}

	if sortedSearchFunc != nil {
		readFilter.SortedSearchFunc = func(vecs []*vector.Vector) []int32 {
			return sortedSearchFunc(vecs[0])
		}
		readFilter.UnSortedSearchFunc = func(vecs []*vector.Vector) []int32 {
			return unSortedSearchFunc(vecs[0])
		}
		readFilter.Valid = true
		return readFilter
	}
	return readFilter
}

func evalLiteralExpr2(expr *plan.Literal, oid types.T) (ret []byte, can bool) {
	can = true
	switch val := expr.Value.(type) {
	case *plan.Literal_I8Val:
		i8 := int8(val.I8Val)
		ret = types.EncodeInt8(&i8)
	case *plan.Literal_I16Val:
		i16 := int16(val.I16Val)
		ret = types.EncodeInt16(&i16)
	case *plan.Literal_I32Val:
		i32 := val.I32Val
		ret = types.EncodeInt32(&i32)
	case *plan.Literal_I64Val:
		i64 := val.I64Val
		ret = types.EncodeInt64(&i64)
	case *plan.Literal_Dval:
		if oid == types.T_float32 {
			fval := float32(val.Dval)
			ret = types.EncodeFloat32(&fval)
		} else {
			dval := val.Dval
			ret = types.EncodeFloat64(&dval)
		}
	case *plan.Literal_Sval:
		ret = []byte(val.Sval)
	case *plan.Literal_Bval:
		ret = types.EncodeBool(&val.Bval)
	case *plan.Literal_U8Val:
		u8 := uint8(val.U8Val)
		ret = types.EncodeUint8(&u8)
	case *plan.Literal_U16Val:
		u16 := uint16(val.U16Val)
		ret = types.EncodeUint16(&u16)
	case *plan.Literal_U32Val:
		u32 := val.U32Val
		ret = types.EncodeUint32(&u32)
	case *plan.Literal_U64Val:
		u64 := val.U64Val
		ret = types.EncodeUint64(&u64)
	case *plan.Literal_Fval:
		if oid == types.T_float32 {
			fval := float32(val.Fval)
			ret = types.EncodeFloat32(&fval)
		} else {
			fval := float64(val.Fval)
			ret = types.EncodeFloat64(&fval)
		}
	case *plan.Literal_Dateval:
		v := types.Date(val.Dateval)
		ret = types.EncodeDate(&v)
	case *plan.Literal_Timeval:
		v := types.Time(val.Timeval)
		ret = types.EncodeTime(&v)
	case *plan.Literal_Datetimeval:
		v := types.Datetime(val.Datetimeval)
		ret = types.EncodeDatetime(&v)
	case *plan.Literal_Timestampval:
		v := types.Timestamp(val.Timestampval)
		ret = types.EncodeTimestamp(&v)
	case *plan.Literal_Decimal64Val:
		v := types.Decimal64(val.Decimal64Val.A)
		ret = types.EncodeDecimal64(&v)
	case *plan.Literal_Decimal128Val:
		v := types.Decimal128{B0_63: uint64(val.Decimal128Val.A), B64_127: uint64(val.Decimal128Val.B)}
		ret = types.EncodeDecimal128(&v)
	case *plan.Literal_EnumVal:
		v := types.Enum(val.EnumVal)
		ret = types.EncodeEnum(&v)
	case *plan.Literal_Jsonval:
		ret = []byte(val.Jsonval)
	default:
		can = false
	}

	return
}
