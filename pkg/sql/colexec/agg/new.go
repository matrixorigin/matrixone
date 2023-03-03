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

package agg

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

// ReturnType get aggregate operator's return type according to its operator-id and input-types.
func ReturnType(op int, typ types.Type) (types.Type, error) {
	var otyp types.Type

	switch op {
	case AggregateAvg:
		otyp = AvgReturnType([]types.Type{typ})
	case AggregateMax:
		otyp = MaxReturnType([]types.Type{typ})
	case AggregateMin:
		otyp = MinReturnType([]types.Type{typ})
	case AggregateSum:
		otyp = SumReturnType([]types.Type{typ})
	case AggregateCount, AggregateStarCount:
		otyp = CountReturnType([]types.Type{typ})
	case AggregateApproxCountDistinct:
		otyp = ApproxCountReturnType([]types.Type{typ})
	case AggregateVariance:
		otyp = VarianceReturnType([]types.Type{typ})
	case AggregateBitAnd:
		otyp = BitAndReturnType([]types.Type{typ})
	case AggregateBitXor:
		otyp = BitXorReturnType([]types.Type{typ})
	case AggregateBitOr:
		otyp = BitOrReturnType([]types.Type{typ})
	case AggregateStdDevPop:
		otyp = StdDevPopReturnType([]types.Type{typ})
	case AggregateMedian:
		otyp = MedianReturnType([]types.Type{typ})
	}
	if otyp.Oid == types.T_any {
		return typ, moerr.NewInternalErrorNoCtx("'%v' not support %s", typ, Names[op])
	}
	return otyp, nil
}

func New(op int, dist bool, typ types.Type) (Agg[any], error) {
	switch op {
	case AggregateSum:
		return newSum(typ, dist), nil
	case AggregateAvg:
		return newAvg(typ, dist), nil
	case AggregateMax:
		return newMax(typ, dist), nil
	case AggregateMin:
		return newMin(typ, dist), nil
	case AggregateCount:
		return newCount(typ, dist, false), nil
	case AggregateStarCount:
		return newCount(typ, dist, true), nil
	case AggregateApproxCountDistinct:
		return newApprox(typ, dist), nil
	case AggregateVariance:
		return newVariance(typ, dist), nil
	case AggregateBitAnd:
		return newBitAnd(typ, dist), nil
	case AggregateBitXor:
		return newBitXor(typ, dist), nil
	case AggregateBitOr:
		return newBitOr(typ, dist), nil
	case AggregateStdDevPop:
		return newStdDevPop(typ, dist), nil
	case AggregateAnyValue:
		return newAnyValue(typ, dist), nil
	case AggregateMedian:
		return newMedian(typ, dist), nil
	}
	panic(moerr.NewInternalErrorNoCtx("unsupported type '%s' for aggregate %s", typ, Names[op]))
}

func newCount(typ types.Type, dist bool, isStar bool) Agg[any] {
	switch typ.Oid {
	case types.T_bool:
		return newGenericCount[bool](typ, dist, isStar)
	case types.T_int8:
		return newGenericCount[int8](typ, dist, isStar)
	case types.T_int16:
		return newGenericCount[int16](typ, dist, isStar)
	case types.T_int32:
		return newGenericCount[int32](typ, dist, isStar)
	case types.T_int64:
		return newGenericCount[int64](typ, dist, isStar)
	case types.T_uint8:
		return newGenericCount[uint8](typ, dist, isStar)
	case types.T_uint16:
		return newGenericCount[uint16](typ, dist, isStar)
	case types.T_uint32:
		return newGenericCount[uint32](typ, dist, isStar)
	case types.T_uint64:
		return newGenericCount[uint64](typ, dist, isStar)
	case types.T_float32:
		return newGenericCount[float32](typ, dist, isStar)
	case types.T_float64:
		return newGenericCount[float64](typ, dist, isStar)
	case types.T_char:
		return newGenericCount[[]byte](typ, dist, isStar)
	case types.T_varchar:
		return newGenericCount[[]byte](typ, dist, isStar)
	case types.T_blob:
		return newGenericCount[[]byte](typ, dist, isStar)
	case types.T_json:
		return newGenericCount[[]byte](typ, dist, isStar)
	case types.T_text:
		return newGenericCount[[]byte](typ, dist, isStar)
	case types.T_binary:
		return newGenericCount[[]byte](typ, dist, isStar)
	case types.T_varbinary:
		return newGenericCount[[]byte](typ, dist, isStar)
	case types.T_date:
		return newGenericCount[types.Date](typ, dist, isStar)
	case types.T_datetime:
		return newGenericCount[types.Datetime](typ, dist, isStar)
	case types.T_time:
		return newGenericCount[types.Time](typ, dist, isStar)
	case types.T_timestamp:
		return newGenericCount[types.Timestamp](typ, dist, isStar)
	case types.T_decimal64:
		return newGenericCount[types.Decimal64](typ, dist, isStar)
	case types.T_decimal128:
		return newGenericCount[types.Decimal128](typ, dist, isStar)
	case types.T_uuid:
		return newGenericCount[types.Uuid](typ, dist, isStar)
	}
	panic(moerr.NewInternalErrorNoCtx("unsupported type '%s' for count", typ))
}

func newAnyValue(typ types.Type, dist bool) Agg[any] {
	switch typ.Oid {
	case types.T_bool:
		return newGenericAnyValue[bool](typ, dist)
	case types.T_int8:
		return newGenericAnyValue[int8](typ, dist)
	case types.T_int16:
		return newGenericAnyValue[int16](typ, dist)
	case types.T_int32:
		return newGenericAnyValue[int32](typ, dist)
	case types.T_int64:
		return newGenericAnyValue[int64](typ, dist)
	case types.T_uint8:
		return newGenericAnyValue[uint8](typ, dist)
	case types.T_uint16:
		return newGenericAnyValue[uint16](typ, dist)
	case types.T_uint32:
		return newGenericAnyValue[uint32](typ, dist)
	case types.T_uint64:
		return newGenericAnyValue[uint64](typ, dist)
	case types.T_float32:
		return newGenericAnyValue[float32](typ, dist)
	case types.T_float64:
		return newGenericAnyValue[float64](typ, dist)
	case types.T_char:
		return newGenericAnyValue[[]byte](typ, dist)
	case types.T_varchar:
		return newGenericAnyValue[[]byte](typ, dist)
	case types.T_binary:
		return newGenericAnyValue[[]byte](typ, dist)
	case types.T_varbinary:
		return newGenericAnyValue[[]byte](typ, dist)
	case types.T_blob:
		return newGenericAnyValue[[]byte](typ, dist)
	case types.T_text:
		return newGenericApproxcd[[]byte](typ, dist)
	case types.T_date:
		return newGenericAnyValue[types.Date](typ, dist)
	case types.T_datetime:
		return newGenericAnyValue[types.Datetime](typ, dist)
	case types.T_time:
		return newGenericAnyValue[types.Time](typ, dist)
	case types.T_timestamp:
		return newGenericAnyValue[types.Timestamp](typ, dist)
	case types.T_decimal64:
		return newGenericAnyValue[types.Decimal64](typ, dist)
	case types.T_decimal128:
		return newGenericAnyValue[types.Decimal128](typ, dist)
	case types.T_uuid:
		return newGenericAnyValue[types.Uuid](typ, dist)
	}
	panic(moerr.NewInternalErrorNoCtx("unsupported type '%s' for anyvalue", typ))
}

func newAvg(typ types.Type, dist bool) Agg[any] {
	switch typ.Oid {
	case types.T_int8:
		return newGenericAvg[int8](typ, dist)
	case types.T_int16:
		return newGenericAvg[int16](typ, dist)
	case types.T_int32:
		return newGenericAvg[int32](typ, dist)
	case types.T_int64:
		return newGenericAvg[int64](typ, dist)
	case types.T_uint8:
		return newGenericAvg[uint8](typ, dist)
	case types.T_uint16:
		return newGenericAvg[uint16](typ, dist)
	case types.T_uint32:
		return newGenericAvg[uint32](typ, dist)
	case types.T_uint64:
		return newGenericAvg[uint64](typ, dist)
	case types.T_float32:
		return newGenericAvg[float32](typ, dist)
	case types.T_float64:
		return newGenericAvg[float64](typ, dist)
	case types.T_decimal64:
		aggPriv := NewD64Avg(typ)
		if dist {
			return NewUnaryDistAgg(AggregateAvg, aggPriv, false, typ, AvgReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
		}
		return NewUnaryAgg(AggregateAvg, aggPriv, false, typ, AvgReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill, aggPriv.BatchFill)
	case types.T_decimal128:
		aggPriv := NewD128Avg(typ)
		if dist {
			return NewUnaryDistAgg(AggregateAvg, aggPriv, false, typ, AvgReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
		}
		return NewUnaryAgg(AggregateAvg, aggPriv, false, typ, AvgReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill, aggPriv.BatchFill)
	}
	panic(moerr.NewInternalErrorNoCtx("unsupported type '%s' for avg", typ))
}

func newSum(typ types.Type, dist bool) Agg[any] {
	switch typ.Oid {
	case types.T_int8:
		return newGenericSum[int8, int64](typ, dist)
	case types.T_int16:
		return newGenericSum[int16, int64](typ, dist)
	case types.T_int32:
		return newGenericSum[int32, int64](typ, dist)
	case types.T_int64:
		return newGenericSum[int64, int64](typ, dist)
	case types.T_uint8:
		return newGenericSum[uint8, uint64](typ, dist)
	case types.T_uint16:
		return newGenericSum[uint16, uint64](typ, dist)
	case types.T_uint32:
		return newGenericSum[uint32, uint64](typ, dist)
	case types.T_uint64:
		return newGenericSum[uint64, uint64](typ, dist)
	case types.T_float32:
		return newGenericSum[float32, float64](typ, dist)
	case types.T_float64:
		return newGenericSum[float64, float64](typ, dist)
	case types.T_decimal64:
		aggPriv := NewD64Sum()
		if dist {
			return NewUnaryDistAgg(AggregateSum, aggPriv, false, typ, SumReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
		}
		return NewUnaryAgg(AggregateSum, aggPriv, false, typ, SumReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill, aggPriv.BatchFill)
	case types.T_decimal128:
		aggPriv := NewD128Sum()
		if dist {
			return NewUnaryDistAgg(AggregateSum, aggPriv, false, typ, SumReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
		}
		return NewUnaryAgg(AggregateSum, aggPriv, false, typ, SumReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill, aggPriv.BatchFill)
	}
	panic(moerr.NewInternalErrorNoCtx("unsupported type '%s' for sum", typ))
}

func newMax(typ types.Type, dist bool) Agg[any] {
	switch typ.Oid {
	case types.T_bool:
		aggPriv := NewBoolMax()
		if dist {
			return NewUnaryDistAgg(AggregateMax, aggPriv, false, typ, MaxReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
		}
		return NewUnaryAgg(AggregateMax, aggPriv, false, typ, MaxReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill, nil)
	case types.T_int8:
		return newGenericMax[int8](typ, dist)
	case types.T_int16:
		return newGenericMax[int16](typ, dist)
	case types.T_int32:
		return newGenericMax[int32](typ, dist)
	case types.T_int64:
		return newGenericMax[int64](typ, dist)
	case types.T_uint8:
		return newGenericMax[uint8](typ, dist)
	case types.T_uint16:
		return newGenericMax[uint16](typ, dist)
	case types.T_uint32:
		return newGenericMax[uint32](typ, dist)
	case types.T_uint64:
		return newGenericMax[uint64](typ, dist)
	case types.T_float32:
		return newGenericMax[float32](typ, dist)
	case types.T_float64:
		return newGenericMax[float64](typ, dist)
	case types.T_binary:
		aggPriv := NewStrMax()
		if dist {
			return NewUnaryDistAgg(AggregateMax, aggPriv, false, typ, MaxReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
		}
		return NewUnaryAgg(AggregateMax, aggPriv, false, typ, MaxReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill, nil)
	case types.T_varbinary:
		aggPriv := NewStrMax()
		if dist {
			return NewUnaryDistAgg(AggregateMax, aggPriv, false, typ, MaxReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
		}
		return NewUnaryAgg(AggregateMax, aggPriv, false, typ, MaxReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill, nil)
	case types.T_char:
		aggPriv := NewStrMax()
		if dist {
			return NewUnaryDistAgg(AggregateMax, aggPriv, false, typ, MaxReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
		}
		return NewUnaryAgg(AggregateMax, aggPriv, false, typ, MaxReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill, nil)
	case types.T_varchar:
		aggPriv := NewStrMax()
		if dist {
			return NewUnaryDistAgg(AggregateMax, aggPriv, false, typ, MaxReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
		}
		return NewUnaryAgg(AggregateMax, aggPriv, false, typ, MaxReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill, nil)
	case types.T_blob:
		aggPriv := NewStrMax()
		if dist {
			return NewUnaryDistAgg(AggregateMax, aggPriv, false, typ, MaxReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
		}
		return NewUnaryAgg(AggregateMax, aggPriv, false, typ, MaxReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill, nil)
	case types.T_text:
		aggPriv := NewStrMax()
		if dist {
			return NewUnaryDistAgg(AggregateMax, aggPriv, false, typ, MaxReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
		}
		return NewUnaryAgg(AggregateMax, aggPriv, false, typ, MaxReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill, nil)
	case types.T_date:
		return newGenericMax[types.Date](typ, dist)
	case types.T_datetime:
		return newGenericMax[types.Datetime](typ, dist)
	case types.T_time:
		return newGenericMax[types.Time](typ, dist)
	case types.T_timestamp:
		return newGenericMax[types.Timestamp](typ, dist)
	case types.T_decimal64:
		aggPriv := NewD64Max()
		if dist {
			return NewUnaryDistAgg(AggregateMax, aggPriv, false, typ, MaxReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
		}
		return NewUnaryAgg(AggregateMax, aggPriv, false, typ, MaxReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill, nil)
	case types.T_decimal128:
		aggPriv := NewD128Max()
		if dist {
			return NewUnaryDistAgg(AggregateMax, aggPriv, false, typ, MaxReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
		}
		return NewUnaryAgg(AggregateMax, aggPriv, false, typ, MaxReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill, nil)
	case types.T_uuid:
		aggPriv := NewUuidMax()
		if dist {
			return NewUnaryDistAgg(AggregateMax, aggPriv, false, typ, MaxReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
		}
		return NewUnaryAgg(AggregateMax, aggPriv, false, typ, MaxReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill, nil)
	}
	panic(moerr.NewInternalErrorNoCtx("unsupported type '%s' for max", typ))
}

func newMin(typ types.Type, dist bool) Agg[any] {
	switch typ.Oid {
	case types.T_bool:
		aggPriv := NewBoolMin()
		if dist {
			return NewUnaryDistAgg(AggregateMin, aggPriv, false, typ, MinReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
		}
		return NewUnaryAgg(AggregateMin, aggPriv, false, typ, MinReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill, nil)
	case types.T_int8:
		return newGenericMin[int8](typ, dist)
	case types.T_int16:
		return newGenericMin[int16](typ, dist)
	case types.T_int32:
		return newGenericMin[int32](typ, dist)
	case types.T_int64:
		return newGenericMin[int64](typ, dist)
	case types.T_uint8:
		return newGenericMin[uint8](typ, dist)
	case types.T_uint16:
		return newGenericMin[uint16](typ, dist)
	case types.T_uint32:
		return newGenericMin[uint32](typ, dist)
	case types.T_uint64:
		return newGenericMin[uint64](typ, dist)
	case types.T_float32:
		return newGenericMin[float32](typ, dist)
	case types.T_float64:
		return newGenericMin[float64](typ, dist)
	case types.T_binary:
		aggPriv := NewStrMin()
		if dist {
			return NewUnaryDistAgg(AggregateMin, aggPriv, false, typ, MinReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
		}
		return NewUnaryAgg(AggregateMin, aggPriv, false, typ, MinReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill, nil)
	case types.T_varbinary:
		aggPriv := NewStrMin()
		if dist {
			return NewUnaryDistAgg(AggregateMin, aggPriv, false, typ, MinReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
		}
		return NewUnaryAgg(AggregateMin, aggPriv, false, typ, MinReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill, nil)
	case types.T_char:
		aggPriv := NewStrMin()
		if dist {
			return NewUnaryDistAgg(AggregateMin, aggPriv, false, typ, MinReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
		}
		return NewUnaryAgg(AggregateMin, aggPriv, false, typ, MinReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill, nil)
	case types.T_varchar:
		aggPriv := NewStrMin()
		if dist {
			return NewUnaryDistAgg(AggregateMin, aggPriv, false, typ, MinReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
		}
		return NewUnaryAgg(AggregateMin, aggPriv, false, typ, MinReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill, nil)
	case types.T_blob:
		aggPriv := NewStrMin()
		if dist {
			return NewUnaryDistAgg(AggregateMin, aggPriv, false, typ, MinReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
		}
		return NewUnaryAgg(AggregateMin, aggPriv, false, typ, MinReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill, nil)
	case types.T_text:
		aggPriv := NewStrMin()
		if dist {
			return NewUnaryDistAgg(AggregateMin, aggPriv, false, typ, MinReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
		}
		return NewUnaryAgg(AggregateMin, aggPriv, false, typ, MinReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill, nil)
	case types.T_date:
		return newGenericMin[types.Date](typ, dist)
	case types.T_datetime:
		return newGenericMin[types.Datetime](typ, dist)
	case types.T_time:
		return newGenericMin[types.Time](typ, dist)
	case types.T_timestamp:
		return newGenericMin[types.Timestamp](typ, dist)
	case types.T_decimal64:
		aggPriv := NewD64Min()
		if dist {
			return NewUnaryDistAgg(AggregateMin, aggPriv, false, typ, MinReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
		}
		return NewUnaryAgg(AggregateMin, aggPriv, false, typ, MinReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill, nil)
	case types.T_decimal128:
		aggPriv := NewD128Min()
		if dist {
			return NewUnaryDistAgg(AggregateMin, aggPriv, false, typ, MinReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
		}
		return NewUnaryAgg(AggregateMin, aggPriv, false, typ, MinReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill, nil)
	case types.T_uuid:
		aggPriv := NewUuidMin()
		if dist {
			return NewUnaryDistAgg(AggregateMin, aggPriv, false, typ, MinReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
		}
		return NewUnaryAgg(AggregateMin, aggPriv, false, typ, MinReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill, nil)
	}
	panic(moerr.NewInternalErrorNoCtx("unsupported type '%s' for min", typ))
}

func newApprox(typ types.Type, dist bool) Agg[any] {
	switch typ.Oid {
	case types.T_bool:
		return newGenericApproxcd[bool](typ, dist)
	case types.T_int8:
		return newGenericApproxcd[int8](typ, dist)
	case types.T_int16:
		return newGenericApproxcd[int16](typ, dist)
	case types.T_int32:
		return newGenericApproxcd[int32](typ, dist)
	case types.T_int64:
		return newGenericApproxcd[int64](typ, dist)
	case types.T_uint8:
		return newGenericApproxcd[uint8](typ, dist)
	case types.T_uint16:
		return newGenericApproxcd[uint16](typ, dist)
	case types.T_uint32:
		return newGenericApproxcd[uint32](typ, dist)
	case types.T_uint64:
		return newGenericApproxcd[uint64](typ, dist)
	case types.T_float32:
		return newGenericApproxcd[float32](typ, dist)
	case types.T_float64:
		return newGenericApproxcd[float64](typ, dist)
	case types.T_char:
		return newGenericApproxcd[[]byte](typ, dist)
	case types.T_varchar:
		return newGenericApproxcd[[]byte](typ, dist)
	case types.T_blob:
		return newGenericApproxcd[[]byte](typ, dist)
	case types.T_text:
		return newGenericApproxcd[[]byte](typ, dist)
	case types.T_binary:
		return newGenericApproxcd[[]byte](typ, dist)
	case types.T_varbinary:
		return newGenericApproxcd[[]byte](typ, dist)
	case types.T_date:
		return newGenericApproxcd[types.Date](typ, dist)
	case types.T_datetime:
		return newGenericApproxcd[types.Datetime](typ, dist)
	case types.T_time:
		return newGenericApproxcd[types.Time](typ, dist)
	case types.T_timestamp:
		return newGenericApproxcd[types.Timestamp](typ, dist)
	case types.T_decimal64:
		return newGenericApproxcd[types.Decimal64](typ, dist)
	case types.T_decimal128:
		return newGenericApproxcd[types.Decimal128](typ, dist)
	case types.T_uuid:
		return newGenericApproxcd[types.Uuid](typ, dist)
	}
	panic(moerr.NewInternalErrorNoCtx("unsupported type '%s' for approx_count_distinct", typ))
}

func newBitOr(typ types.Type, dist bool) Agg[any] {
	switch typ.Oid {
	case types.T_int8:
		return newGenericBitOr[int8](typ, dist)
	case types.T_int16:
		return newGenericBitOr[int16](typ, dist)
	case types.T_int32:
		return newGenericBitOr[int32](typ, dist)
	case types.T_int64:
		return newGenericBitOr[int64](typ, dist)
	case types.T_uint8:
		return newGenericBitOr[uint8](typ, dist)
	case types.T_uint16:
		return newGenericBitOr[uint16](typ, dist)
	case types.T_uint32:
		return newGenericBitOr[uint32](typ, dist)
	case types.T_uint64:
		return newGenericBitOr[uint64](typ, dist)
	case types.T_float32:
		return newGenericBitOr[float32](typ, dist)
	case types.T_float64:
		return newGenericBitOr[float64](typ, dist)
	case types.T_binary, types.T_varbinary:
		aggPriv := NewBitOrBinary()
		if dist {
			return NewUnaryDistAgg(AggregateBitOr, aggPriv, false, typ, BitOrReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
		}
		return NewUnaryAgg(AggregateBitOr, aggPriv, false, typ, BitOrReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill, nil)
	}
	panic(moerr.NewInternalErrorNoCtx("unsupported type '%s' for bitor", typ))
}

func newBitXor(typ types.Type, dist bool) Agg[any] {
	switch typ.Oid {
	case types.T_int8:
		return newGenericBitXor[int8](typ, dist)
	case types.T_int16:
		return newGenericBitXor[int16](typ, dist)
	case types.T_int32:
		return newGenericBitXor[int32](typ, dist)
	case types.T_int64:
		return newGenericBitXor[int64](typ, dist)
	case types.T_uint8:
		return newGenericBitXor[uint8](typ, dist)
	case types.T_uint16:
		return newGenericBitXor[uint16](typ, dist)
	case types.T_uint32:
		return newGenericBitXor[uint32](typ, dist)
	case types.T_uint64:
		return newGenericBitXor[uint64](typ, dist)
	case types.T_float32:
		return newGenericBitXor[float32](typ, dist)
	case types.T_float64:
		return newGenericBitXor[float64](typ, dist)
	case types.T_binary, types.T_varbinary:
		aggPriv := NewBitXorBinary()
		if dist {
			return NewUnaryDistAgg(AggregateBitXor, aggPriv, false, typ, BitXorReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
		}
		return NewUnaryAgg(AggregateBitXor, aggPriv, false, typ, BitXorReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill, nil)

	}
	panic(moerr.NewInternalErrorNoCtx("unsupported type '%s' for bitxor", typ))
}

func newBitAnd(typ types.Type, dist bool) Agg[any] {
	switch typ.Oid {
	case types.T_int8:
		return newGenericBitAnd[int8](typ, dist)
	case types.T_int16:
		return newGenericBitAnd[int16](typ, dist)
	case types.T_int32:
		return newGenericBitAnd[int32](typ, dist)
	case types.T_int64:
		return newGenericBitAnd[int64](typ, dist)
	case types.T_uint8:
		return newGenericBitAnd[uint8](typ, dist)
	case types.T_uint16:
		return newGenericBitAnd[uint16](typ, dist)
	case types.T_uint32:
		return newGenericBitAnd[uint32](typ, dist)
	case types.T_uint64:
		return newGenericBitAnd[uint64](typ, dist)
	case types.T_float32:
		return newGenericBitAnd[float32](typ, dist)
	case types.T_float64:
		return newGenericBitAnd[float64](typ, dist)
	case types.T_binary, types.T_varbinary:
		aggPriv := NewBitAndBinary()
		if dist {
			return NewUnaryDistAgg(AggregateBitAnd, aggPriv, false, typ, BitAndReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
		}
		return NewUnaryAgg(AggregateBitAnd, aggPriv, false, typ, BitAndReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill, nil)
	}
	panic(moerr.NewInternalErrorNoCtx("unsupported type '%s' for bitand", typ))
}

func newVariance(typ types.Type, dist bool) Agg[any] {
	switch typ.Oid {
	case types.T_int8:
		return newGenericVariance[int8](typ, dist)
	case types.T_int16:
		return newGenericVariance[int16](typ, dist)
	case types.T_int32:
		return newGenericVariance[int32](typ, dist)
	case types.T_int64:
		return newGenericVariance[int64](typ, dist)
	case types.T_uint8:
		return newGenericVariance[uint8](typ, dist)
	case types.T_uint16:
		return newGenericVariance[uint16](typ, dist)
	case types.T_uint32:
		return newGenericVariance[uint32](typ, dist)
	case types.T_uint64:
		return newGenericVariance[uint64](typ, dist)
	case types.T_float32:
		return newGenericVariance[float32](typ, dist)
	case types.T_float64:
		return newGenericVariance[float64](typ, dist)
	case types.T_decimal64:
		aggPriv := NewVD64(typ)
		if dist {
			return NewUnaryDistAgg(AggregateVariance, aggPriv, false, typ, VarianceReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
		}
		return NewUnaryAgg(AggregateVariance, aggPriv, false, typ, VarianceReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill, nil)
	case types.T_decimal128:
		aggPriv := NewVD128(typ)
		if dist {
			return NewUnaryDistAgg(AggregateVariance, aggPriv, false, typ, VarianceReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
		}
		return NewUnaryAgg(AggregateVariance, aggPriv, false, typ, VarianceReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill, nil)

	}
	panic(moerr.NewInternalErrorNoCtx("unsupported type '%s' for variance", typ))
}

func newStdDevPop(typ types.Type, dist bool) Agg[any] {
	switch typ.Oid {
	case types.T_int8:
		return newGenericStdDevPop[int8](typ, dist)
	case types.T_int16:
		return newGenericStdDevPop[int16](typ, dist)
	case types.T_int32:
		return newGenericStdDevPop[int32](typ, dist)
	case types.T_int64:
		return newGenericStdDevPop[int64](typ, dist)
	case types.T_uint8:
		return newGenericStdDevPop[uint8](typ, dist)
	case types.T_uint16:
		return newGenericStdDevPop[uint16](typ, dist)
	case types.T_uint32:
		return newGenericStdDevPop[uint32](typ, dist)
	case types.T_uint64:
		return newGenericStdDevPop[uint64](typ, dist)
	case types.T_float32:
		return newGenericStdDevPop[float32](typ, dist)
	case types.T_float64:
		return newGenericStdDevPop[float64](typ, dist)
	case types.T_decimal64:
		aggPriv := NewStdD64(typ)
		if dist {
			return NewUnaryDistAgg(AggregateStdDevPop, aggPriv, false, typ, StdDevPopReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
		}
		return NewUnaryAgg(AggregateStdDevPop, aggPriv, false, typ, StdDevPopReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill, nil)
	case types.T_decimal128:
		aggPriv := NewStdD128(typ)
		if dist {
			return NewUnaryDistAgg(AggregateStdDevPop, aggPriv, false, typ, StdDevPopReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
		}
		return NewUnaryAgg(AggregateStdDevPop, aggPriv, false, typ, StdDevPopReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill, nil)

	}
	panic(moerr.NewInternalErrorNoCtx("unsupported type '%s' for stddev", typ))
}

func newMedian(typ types.Type, dist bool) Agg[any] {
	switch typ.Oid {
	case types.T_int8:
		return newGenericMedian[int8](typ, dist)
	case types.T_int16:
		return newGenericMedian[int16](typ, dist)
	case types.T_int32:
		return newGenericMedian[int32](typ, dist)
	case types.T_int64:
		return newGenericMedian[int64](typ, dist)
	case types.T_uint8:
		return newGenericMedian[uint8](typ, dist)
	case types.T_uint16:
		return newGenericMedian[uint16](typ, dist)
	case types.T_uint32:
		return newGenericMedian[uint32](typ, dist)
	case types.T_uint64:
		return newGenericMedian[uint64](typ, dist)
	case types.T_float32:
		return newGenericMedian[float32](typ, dist)
	case types.T_float64:
		return newGenericMedian[float64](typ, dist)
	case types.T_decimal64:
		aggPriv := NewD64Median()
		if dist {
			panic(moerr.NewNotSupportedNoCtx("median in distinct mode"))
		}
		return NewUnaryAgg(AggregateMedian, aggPriv, false, typ, MedianReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill, nil)
	case types.T_decimal128:
		aggPriv := NewD128Median()
		if dist {
			panic(moerr.NewNotSupportedNoCtx("median in distinct mode"))
		}
		return NewUnaryAgg(AggregateMedian, aggPriv, false, typ, MedianReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill, nil)
	}
	panic(moerr.NewNotSupportedNoCtx("median on type '%s'", typ))
}

func newGenericAnyValue[T any](typ types.Type, dist bool) Agg[any] {
	aggPriv := NewAnyValue[T]()
	if dist {
		return NewUnaryDistAgg(AggregateAnyValue, aggPriv, false, typ, AnyValueReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
	}
	return NewUnaryAgg(AggregateAnyValue, aggPriv, false, typ, AnyValueReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill, nil)
}

func newGenericSum[T1 Numeric, T2 ReturnTyp](typ types.Type, dist bool) Agg[any] {
	aggPriv := NewSum[T1, T2]()
	if dist {
		return NewUnaryDistAgg(AggregateSum, aggPriv, false, typ, SumReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
	}
	return NewUnaryAgg(AggregateSum, aggPriv, false, typ, SumReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill, nil)
}

func newGenericAvg[T Numeric](typ types.Type, dist bool) Agg[any] {
	aggPriv := NewAvg[T]()
	if dist {
		return NewUnaryDistAgg(AggregateAvg, aggPriv, false, typ, AvgReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
	}
	return NewUnaryAgg(AggregateAvg, aggPriv, false, typ, AvgReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill, nil)
}

func newGenericMax[T Compare](typ types.Type, dist bool) Agg[any] {
	aggPriv := NewMax[T]()
	if dist {
		return NewUnaryDistAgg(AggregateMax, aggPriv, false, typ, MaxReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
	}
	return NewUnaryAgg(AggregateMax, aggPriv, false, typ, MaxReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill, nil)
}

func newGenericMin[T Compare](typ types.Type, dist bool) Agg[any] {
	aggPriv := NewMin[T]()
	if dist {
		return NewUnaryDistAgg(AggregateMin, aggPriv, false, typ, MinReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
	}
	return NewUnaryAgg(AggregateMin, aggPriv, false, typ, MinReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill, nil)
}

func newGenericCount[T types.OrderedT | Decimal128AndString](typ types.Type, dist bool, isStar bool) Agg[any] {
	aggPriv := NewCount[T](isStar)
	if dist {
		return NewUnaryDistAgg(AggregateCount, aggPriv, true, typ, CountReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
	}
	return NewUnaryAgg(AggregateCount, aggPriv, true, typ, CountReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill, aggPriv.BatchFill)
}

func newGenericApproxcd[T any](typ types.Type, dist bool) Agg[any] {
	aggPriv := NewApproxc[T]()
	if dist {
		return NewUnaryDistAgg(AggregateApproxCountDistinct, aggPriv, false, typ, ApproxCountReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
	}
	return NewUnaryAgg(AggregateApproxCountDistinct, aggPriv, false, typ, ApproxCountReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill, nil)
}

func newGenericBitOr[T types.Ints | types.UInts | types.Floats](typ types.Type, dist bool) Agg[any] {
	aggPriv := NewBitOr[T]()
	if dist {
		return NewUnaryDistAgg(AggregateBitOr, aggPriv, false, typ, BitOrReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
	}
	return NewUnaryAgg(AggregateBitOr, aggPriv, false, typ, BitOrReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill, nil)
}

func newGenericBitXor[T types.Ints | types.UInts | types.Floats](typ types.Type, dist bool) Agg[any] {
	aggPriv := NewBitXor[T]()
	if dist {
		return NewUnaryDistAgg(AggregateBitXor, aggPriv, false, typ, BitXorReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
	}
	return NewUnaryAgg(AggregateBitXor, aggPriv, false, typ, BitXorReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill, nil)
}

func newGenericBitAnd[T types.Ints | types.UInts | types.Floats](typ types.Type, dist bool) Agg[any] {
	aggPriv := NewBitAnd[T]()
	if dist {
		return NewUnaryDistAgg(AggregateBitAnd, aggPriv, false, typ, BitAndReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
	}
	return NewUnaryAgg(AggregateBitAnd, aggPriv, false, typ, BitAndReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill, nil)
}

func newGenericVariance[T types.Ints | types.UInts | types.Floats](typ types.Type, dist bool) Agg[any] {
	aggPriv := NewVariance[T]()
	if dist {
		return NewUnaryDistAgg(AggregateVariance, aggPriv, false, typ, VarianceReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
	}
	return NewUnaryAgg(AggregateVariance, aggPriv, false, typ, VarianceReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill, nil)
}

func newGenericStdDevPop[T types.Ints | types.UInts | types.Floats](typ types.Type, dist bool) Agg[any] {
	aggPriv := NewStdDevPop[T]()
	if dist {
		return NewUnaryDistAgg(AggregateStdDevPop, aggPriv, false, typ, StdDevPopReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
	}
	return NewUnaryAgg(AggregateStdDevPop, aggPriv, false, typ, StdDevPopReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill, nil)
}
func newGenericMedian[T Numeric](typ types.Type, dist bool) Agg[any] {
	aggPriv := NewMedian[T]()
	if dist {
		panic(moerr.NewNotSupportedNoCtx("median in distinct mode"))
	}
	return NewUnaryAgg(AggregateMedian, aggPriv, false, typ, MedianReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill, nil)
}
