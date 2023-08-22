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
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

func New(op int, dist bool, typ types.Type) (Agg[any], error) {
	return NewWithConfig(op, dist, typ, nil)
}

func NewWithConfig(op int, dist bool, inputType types.Type, config any) (a Agg[any], err error) {
	overloadID := int64(op)

	f, exist := function.GetFunctionByIdWithoutError(overloadID)
	if !exist {
		panic(moerr.NewInternalErrorNoCtx("unsupported aggregate %d", overloadID))
	}
	functionID, _ := function.DecodeOverloadID(overloadID)

	retTyp := f.GetReturnTypeMethod()
	outputTyp := retTyp([]types.Type{inputType})

	switch functionID {
	case function.SUM:
		return newSum(inputType, outputTyp, dist), nil
	case function.AVG:
		return newAvg(inputType, outputTyp, dist), nil
	case function.MAX:
		return newMax(inputType, outputTyp, dist), nil
	case function.MIN:
		return newMin(inputType, outputTyp, dist), nil
	case function.COUNT:
		return newCount(inputType, outputTyp, dist, false), nil
	case function.STARCOUNT:
		return newCount(inputType, outputTyp, dist, true), nil
	case function.APPROX_COUNT_DISTINCT:
		return newApprox(inputType, outputTyp, dist), nil
	case function.VAR_POP:
		return newVariance(inputType, outputTyp, dist), nil
	case function.BIT_AND:
		return newBitAnd(inputType, outputTyp, dist), nil
	case function.BIT_XOR:
		return newBitXor(inputType, outputTyp, dist), nil
	case function.BIT_OR:
		return newBitOr(inputType, outputTyp, dist), nil
	case function.STDDEV_POP:
		return newStdDevPop(inputType, outputTyp, dist), nil
	case function.ANY_VALUE:
		return newAnyValue(inputType, outputTyp, dist), nil
	case function.MEDIAN:
		return newMedian(inputType, outputTyp, dist), nil
	case function.GROUP_CONCAT:
		outputTyp = retTyp(nil)
		return NewGroupConcat(inputType, outputTyp, dist, config), nil
	case function.RANK:
		r := NewRank()
		return NewUnaryAgg(WinRank, r, false, inputType, outputTyp, r.Grows, r.Eval, r.Merge, r.Fill, nil), nil
	case function.ROW_NUMBER:
		r := NewRowNumber()
		return NewUnaryAgg(WinRowNumber, r, false, inputType, outputTyp, r.Grows, r.Eval, r.Merge, r.Fill, nil), nil
	case function.DENSE_RANK:
		r := NewDenseRank()
		return NewUnaryAgg(WinDenseRank, r, false, inputType, outputTyp, r.Grows, r.Eval, r.Merge, r.Fill, nil), nil
	}
	panic(moerr.NewInternalErrorNoCtx("unsupported type '%s' for aggregate %s", inputType, Names[functionID]))
}

func newCount(typ types.Type, otyp types.Type, dist bool, isStar bool) Agg[any] {
	switch typ.Oid {
	case types.T_bool:
		return newGenericCount[bool](typ, otyp, dist, isStar)
	case types.T_int8:
		return newGenericCount[int8](typ, otyp, dist, isStar)
	case types.T_int16:
		return newGenericCount[int16](typ, otyp, dist, isStar)
	case types.T_int32:
		return newGenericCount[int32](typ, otyp, dist, isStar)
	case types.T_int64:
		return newGenericCount[int64](typ, otyp, dist, isStar)
	case types.T_uint8:
		return newGenericCount[uint8](typ, otyp, dist, isStar)
	case types.T_uint16:
		return newGenericCount[uint16](typ, otyp, dist, isStar)
	case types.T_uint32:
		return newGenericCount[uint32](typ, otyp, dist, isStar)
	case types.T_uint64:
		return newGenericCount[uint64](typ, otyp, dist, isStar)
	case types.T_float32:
		return newGenericCount[float32](typ, otyp, dist, isStar)
	case types.T_float64:
		return newGenericCount[float64](typ, otyp, dist, isStar)
	case types.T_char:
		return newGenericCount[[]byte](typ, otyp, dist, isStar)
	case types.T_varchar:
		return newGenericCount[[]byte](typ, otyp, dist, isStar)
	case types.T_blob:
		return newGenericCount[[]byte](typ, otyp, dist, isStar)
	case types.T_json:
		return newGenericCount[[]byte](typ, otyp, dist, isStar)
	case types.T_text:
		return newGenericCount[[]byte](typ, otyp, dist, isStar)
	case types.T_binary:
		return newGenericCount[[]byte](typ, otyp, dist, isStar)
	case types.T_varbinary:
		return newGenericCount[[]byte](typ, otyp, dist, isStar)
	case types.T_date:
		return newGenericCount[types.Date](typ, otyp, dist, isStar)
	case types.T_datetime:
		return newGenericCount[types.Datetime](typ, otyp, dist, isStar)
	case types.T_time:
		return newGenericCount[types.Time](typ, otyp, dist, isStar)
	case types.T_timestamp:
		return newGenericCount[types.Timestamp](typ, otyp, dist, isStar)
	case types.T_enum:
		return newGenericCount[types.Enum](typ, otyp, dist, isStar)
	case types.T_decimal64:
		return newGenericCount[types.Decimal64](typ, otyp, dist, isStar)
	case types.T_decimal128:
		return newGenericCount[types.Decimal128](typ, otyp, dist, isStar)
	case types.T_uuid:
		return newGenericCount[types.Uuid](typ, otyp, dist, isStar)
	}
	panic(moerr.NewInternalErrorNoCtx("unsupported type '%s' for count", typ))
}

func newAnyValue(typ types.Type, otyp types.Type, dist bool) Agg[any] {
	switch typ.Oid {
	case types.T_bool:
		return newGenericAnyValue[bool](typ, otyp, dist)
	case types.T_int8:
		return newGenericAnyValue[int8](typ, otyp, dist)
	case types.T_int16:
		return newGenericAnyValue[int16](typ, otyp, dist)
	case types.T_int32:
		return newGenericAnyValue[int32](typ, otyp, dist)
	case types.T_int64:
		return newGenericAnyValue[int64](typ, otyp, dist)
	case types.T_uint8:
		return newGenericAnyValue[uint8](typ, otyp, dist)
	case types.T_uint16:
		return newGenericAnyValue[uint16](typ, otyp, dist)
	case types.T_uint32:
		return newGenericAnyValue[uint32](typ, otyp, dist)
	case types.T_uint64:
		return newGenericAnyValue[uint64](typ, otyp, dist)
	case types.T_float32:
		return newGenericAnyValue[float32](typ, otyp, dist)
	case types.T_float64:
		return newGenericAnyValue[float64](typ, otyp, dist)
	case types.T_char:
		return newStrAnyValue(typ, otyp, dist)
	case types.T_varchar:
		return newStrAnyValue(typ, otyp, dist)
	case types.T_binary:
		return newStrAnyValue(typ, otyp, dist)
	case types.T_varbinary:
		return newStrAnyValue(typ, otyp, dist)
	case types.T_blob:
		return newStrAnyValue(typ, otyp, dist)
	case types.T_text:
		return newStrAnyValue(typ, otyp, dist)
	case types.T_date:
		return newGenericAnyValue[types.Date](typ, otyp, dist)
	case types.T_datetime:
		return newGenericAnyValue[types.Datetime](typ, otyp, dist)
	case types.T_time:
		return newGenericAnyValue[types.Time](typ, otyp, dist)
	case types.T_timestamp:
		return newGenericAnyValue[types.Timestamp](typ, otyp, dist)
	case types.T_enum:
		return newGenericAnyValue[types.Enum](typ, otyp, dist)
	case types.T_decimal64:
		return newGenericAnyValue[types.Decimal64](typ, otyp, dist)
	case types.T_decimal128:
		return newGenericAnyValue[types.Decimal128](typ, otyp, dist)
	case types.T_uuid:
		return newGenericAnyValue[types.Uuid](typ, otyp, dist)
	}
	panic(moerr.NewInternalErrorNoCtx("unsupported type '%s' for any_value", typ))
}

func newAvg(typ types.Type, otyp types.Type, dist bool) Agg[any] {
	switch typ.Oid {
	case types.T_int8:
		return newGenericAvg[int8](typ, otyp, dist)
	case types.T_int16:
		return newGenericAvg[int16](typ, otyp, dist)
	case types.T_int32:
		return newGenericAvg[int32](typ, otyp, dist)
	case types.T_int64:
		return newGenericAvg[int64](typ, otyp, dist)
	case types.T_uint8:
		return newGenericAvg[uint8](typ, otyp, dist)
	case types.T_uint16:
		return newGenericAvg[uint16](typ, otyp, dist)
	case types.T_uint32:
		return newGenericAvg[uint32](typ, otyp, dist)
	case types.T_uint64:
		return newGenericAvg[uint64](typ, otyp, dist)
	case types.T_float32:
		return newGenericAvg[float32](typ, otyp, dist)
	case types.T_float64:
		return newGenericAvg[float64](typ, otyp, dist)
	case types.T_decimal64:
		aggPriv := NewD64Avg(typ)
		if dist {
			return NewUnaryDistAgg(function.AVG, aggPriv, false, typ, otyp, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
		}
		return NewUnaryAgg(function.AVG, aggPriv, false, typ, otyp, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill, nil)
	case types.T_decimal128:
		aggPriv := NewD128Avg(typ)
		if dist {
			return NewUnaryDistAgg(function.AVG, aggPriv, false, typ, otyp, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
		}
		return NewUnaryAgg(function.AVG, aggPriv, false, typ, otyp, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill, nil)
	}
	panic(moerr.NewInternalErrorNoCtx("unsupported type '%s' for avg", typ))
}

func newSum(ityp types.Type, otyp types.Type, dist bool) Agg[any] {
	switch ityp.Oid {
	case types.T_int8:
		return newGenericSum[int8, int64](ityp, otyp, dist)
	case types.T_int16:
		return newGenericSum[int16, int64](ityp, otyp, dist)
	case types.T_int32:
		return newGenericSum[int32, int64](ityp, otyp, dist)
	case types.T_int64:
		return newGenericSum[int64, int64](ityp, otyp, dist)
	case types.T_uint8:
		return newGenericSum[uint8, uint64](ityp, otyp, dist)
	case types.T_uint16:
		return newGenericSum[uint16, uint64](ityp, otyp, dist)
	case types.T_uint32:
		return newGenericSum[uint32, uint64](ityp, otyp, dist)
	case types.T_uint64:
		return newGenericSum[uint64, uint64](ityp, otyp, dist)
	case types.T_float32:
		return newGenericSum[float32, float64](ityp, otyp, dist)
	case types.T_float64:
		return newGenericSum[float64, float64](ityp, otyp, dist)
	case types.T_decimal64:
		aggPriv := NewD64Sum()
		if dist {
			return NewUnaryDistAgg(function.SUM, aggPriv, false, ityp, otyp, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
		}
		return NewUnaryAgg(function.SUM, aggPriv, false, ityp, otyp, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill, nil)
	case types.T_decimal128:
		aggPriv := NewD128Sum()
		if dist {
			return NewUnaryDistAgg(function.SUM, aggPriv, false, ityp, otyp, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
		}
		return NewUnaryAgg(function.SUM, aggPriv, false, ityp, otyp, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill, nil)
	}
	panic(moerr.NewInternalErrorNoCtx("unsupported type '%s' for sum", ityp))
}

func newMax(typ types.Type, otyp types.Type, dist bool) Agg[any] {
	switch typ.Oid {
	case types.T_bool:
		aggPriv := NewBoolMax()
		if dist {
			return NewUnaryDistAgg(function.MAX, aggPriv, false, typ, otyp, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
		}
		return NewUnaryAgg(function.MAX, aggPriv, false, typ, otyp, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill, nil)
	case types.T_int8:
		return newGenericMax[int8](typ, otyp, dist)
	case types.T_int16:
		return newGenericMax[int16](typ, otyp, dist)
	case types.T_int32:
		return newGenericMax[int32](typ, otyp, dist)
	case types.T_int64:
		return newGenericMax[int64](typ, otyp, dist)
	case types.T_uint8:
		return newGenericMax[uint8](typ, otyp, dist)
	case types.T_uint16:
		return newGenericMax[uint16](typ, otyp, dist)
	case types.T_uint32:
		return newGenericMax[uint32](typ, otyp, dist)
	case types.T_uint64:
		return newGenericMax[uint64](typ, otyp, dist)
	case types.T_float32:
		return newGenericMax[float32](typ, otyp, dist)
	case types.T_float64:
		return newGenericMax[float64](typ, otyp, dist)
	case types.T_binary:
		aggPriv := NewStrMax()
		if dist {
			return NewUnaryDistAgg(function.MAX, aggPriv, false, typ, otyp, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
		}
		return NewUnaryAgg(function.MAX, aggPriv, false, typ, otyp, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill, nil)
	case types.T_varbinary:
		aggPriv := NewStrMax()
		if dist {
			return NewUnaryDistAgg(function.MAX, aggPriv, false, typ, otyp, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
		}
		return NewUnaryAgg(function.MAX, aggPriv, false, typ, otyp, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill, nil)
	case types.T_char:
		aggPriv := NewStrMax()
		if dist {
			return NewUnaryDistAgg(function.MAX, aggPriv, false, typ, otyp, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
		}
		return NewUnaryAgg(function.MAX, aggPriv, false, typ, otyp, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill, nil)
	case types.T_varchar:
		aggPriv := NewStrMax()
		if dist {
			return NewUnaryDistAgg(function.MAX, aggPriv, false, typ, otyp, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
		}
		return NewUnaryAgg(function.MAX, aggPriv, false, typ, otyp, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill, nil)
	case types.T_blob:
		aggPriv := NewStrMax()
		if dist {
			return NewUnaryDistAgg(function.MAX, aggPriv, false, typ, otyp, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
		}
		return NewUnaryAgg(function.MAX, aggPriv, false, typ, otyp, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill, nil)
	case types.T_text:
		aggPriv := NewStrMax()
		if dist {
			return NewUnaryDistAgg(function.MAX, aggPriv, false, typ, otyp, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
		}
		return NewUnaryAgg(function.MAX, aggPriv, false, typ, otyp, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill, nil)
	case types.T_date:
		return newGenericMax[types.Date](typ, otyp, dist)
	case types.T_datetime:
		return newGenericMax[types.Datetime](typ, otyp, dist)
	case types.T_time:
		return newGenericMax[types.Time](typ, otyp, dist)
	case types.T_timestamp:
		return newGenericMax[types.Timestamp](typ, otyp, dist)
	case types.T_enum:
		return newGenericMax[types.Enum](typ, otyp, dist)
	case types.T_decimal64:
		aggPriv := NewD64Max()
		if dist {
			return NewUnaryDistAgg(function.MAX, aggPriv, false, typ, otyp, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
		}
		return NewUnaryAgg(function.MAX, aggPriv, false, typ, otyp, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill, nil)
	case types.T_decimal128:
		aggPriv := NewD128Max()
		if dist {
			return NewUnaryDistAgg(function.MAX, aggPriv, false, typ, otyp, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
		}
		return NewUnaryAgg(function.MAX, aggPriv, false, typ, otyp, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill, nil)
	case types.T_uuid:
		aggPriv := NewUuidMax()
		if dist {
			return NewUnaryDistAgg(function.MAX, aggPriv, false, typ, otyp, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
		}
		return NewUnaryAgg(function.MAX, aggPriv, false, typ, otyp, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill, nil)
	}
	panic(moerr.NewInternalErrorNoCtx("unsupported type '%s' for max", typ))
}

func newMin(typ types.Type, otyp types.Type, dist bool) Agg[any] {
	switch typ.Oid {
	case types.T_bool:
		aggPriv := NewBoolMin()
		if dist {
			return NewUnaryDistAgg(function.MIN, aggPriv, false, typ, otyp, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
		}
		return NewUnaryAgg(function.MIN, aggPriv, false, typ, otyp, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill, nil)
	case types.T_int8:
		return newGenericMin[int8](typ, otyp, dist)
	case types.T_int16:
		return newGenericMin[int16](typ, otyp, dist)
	case types.T_int32:
		return newGenericMin[int32](typ, otyp, dist)
	case types.T_int64:
		return newGenericMin[int64](typ, otyp, dist)
	case types.T_uint8:
		return newGenericMin[uint8](typ, otyp, dist)
	case types.T_uint16:
		return newGenericMin[uint16](typ, otyp, dist)
	case types.T_uint32:
		return newGenericMin[uint32](typ, otyp, dist)
	case types.T_uint64:
		return newGenericMin[uint64](typ, otyp, dist)
	case types.T_float32:
		return newGenericMin[float32](typ, otyp, dist)
	case types.T_float64:
		return newGenericMin[float64](typ, otyp, dist)
	case types.T_binary:
		aggPriv := NewStrMin()
		if dist {
			return NewUnaryDistAgg(function.MIN, aggPriv, false, typ, otyp, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
		}
		return NewUnaryAgg(function.MIN, aggPriv, false, typ, otyp, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill, nil)
	case types.T_varbinary:
		aggPriv := NewStrMin()
		if dist {
			return NewUnaryDistAgg(function.MIN, aggPriv, false, typ, otyp, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
		}
		return NewUnaryAgg(function.MIN, aggPriv, false, typ, otyp, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill, nil)
	case types.T_char:
		aggPriv := NewStrMin()
		if dist {
			return NewUnaryDistAgg(function.MIN, aggPriv, false, typ, otyp, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
		}
		return NewUnaryAgg(function.MIN, aggPriv, false, typ, otyp, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill, nil)
	case types.T_varchar:
		aggPriv := NewStrMin()
		if dist {
			return NewUnaryDistAgg(function.MIN, aggPriv, false, typ, otyp, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
		}
		return NewUnaryAgg(function.MIN, aggPriv, false, typ, otyp, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill, nil)
	case types.T_blob:
		aggPriv := NewStrMin()
		if dist {
			return NewUnaryDistAgg(function.MIN, aggPriv, false, typ, otyp, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
		}
		return NewUnaryAgg(function.MIN, aggPriv, false, typ, otyp, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill, nil)
	case types.T_text:
		aggPriv := NewStrMin()
		if dist {
			return NewUnaryDistAgg(function.MIN, aggPriv, false, typ, otyp, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
		}
		return NewUnaryAgg(function.MIN, aggPriv, false, typ, otyp, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill, nil)
	case types.T_date:
		return newGenericMin[types.Date](typ, otyp, dist)
	case types.T_datetime:
		return newGenericMin[types.Datetime](typ, otyp, dist)
	case types.T_time:
		return newGenericMin[types.Time](typ, otyp, dist)
	case types.T_timestamp:
		return newGenericMin[types.Timestamp](typ, otyp, dist)
	case types.T_enum:
		return newGenericMin[types.Enum](typ, otyp, dist)
	case types.T_decimal64:
		aggPriv := NewD64Min()
		if dist {
			return NewUnaryDistAgg(function.MIN, aggPriv, false, typ, otyp, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
		}
		return NewUnaryAgg(function.MIN, aggPriv, false, typ, otyp, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill, nil)
	case types.T_decimal128:
		aggPriv := NewD128Min()
		if dist {
			return NewUnaryDistAgg(function.MIN, aggPriv, false, typ, otyp, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
		}
		return NewUnaryAgg(function.MIN, aggPriv, false, typ, otyp, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill, nil)
	case types.T_uuid:
		aggPriv := NewUuidMin()
		if dist {
			return NewUnaryDistAgg(function.MIN, aggPriv, false, typ, otyp, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
		}
		return NewUnaryAgg(function.MIN, aggPriv, false, typ, otyp, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill, nil)
	}
	panic(moerr.NewInternalErrorNoCtx("unsupported type '%s' for min", typ))
}

func newApprox(typ types.Type, otyp types.Type, dist bool) Agg[any] {
	switch typ.Oid {
	case types.T_bool:
		return newGenericApproxcd[bool](typ, otyp, dist)
	case types.T_int8:
		return newGenericApproxcd[int8](typ, otyp, dist)
	case types.T_int16:
		return newGenericApproxcd[int16](typ, otyp, dist)
	case types.T_int32:
		return newGenericApproxcd[int32](typ, otyp, dist)
	case types.T_int64:
		return newGenericApproxcd[int64](typ, otyp, dist)
	case types.T_uint8:
		return newGenericApproxcd[uint8](typ, otyp, dist)
	case types.T_uint16:
		return newGenericApproxcd[uint16](typ, otyp, dist)
	case types.T_uint32:
		return newGenericApproxcd[uint32](typ, otyp, dist)
	case types.T_uint64:
		return newGenericApproxcd[uint64](typ, otyp, dist)
	case types.T_float32:
		return newGenericApproxcd[float32](typ, otyp, dist)
	case types.T_float64:
		return newGenericApproxcd[float64](typ, otyp, dist)
	case types.T_char:
		return newGenericApproxcd[[]byte](typ, otyp, dist)
	case types.T_varchar:
		return newGenericApproxcd[[]byte](typ, otyp, dist)
	case types.T_blob:
		return newGenericApproxcd[[]byte](typ, otyp, dist)
	case types.T_text:
		return newGenericApproxcd[[]byte](typ, otyp, dist)
	case types.T_binary:
		return newGenericApproxcd[[]byte](typ, otyp, dist)
	case types.T_varbinary:
		return newGenericApproxcd[[]byte](typ, otyp, dist)
	case types.T_date:
		return newGenericApproxcd[types.Date](typ, otyp, dist)
	case types.T_datetime:
		return newGenericApproxcd[types.Datetime](typ, otyp, dist)
	case types.T_time:
		return newGenericApproxcd[types.Time](typ, otyp, dist)
	case types.T_timestamp:
		return newGenericApproxcd[types.Timestamp](typ, otyp, dist)
	case types.T_enum:
		return newGenericApproxcd[types.Enum](typ, otyp, dist)
	case types.T_decimal64:
		return newGenericApproxcd[types.Decimal64](typ, otyp, dist)
	case types.T_decimal128:
		return newGenericApproxcd[types.Decimal128](typ, otyp, dist)
	case types.T_uuid:
		return newGenericApproxcd[types.Uuid](typ, otyp, dist)
	}
	panic(moerr.NewInternalErrorNoCtx("unsupported type '%s' for approx_count_distinct", typ))
}

func newBitOr(typ types.Type, otyp types.Type, dist bool) Agg[any] {
	switch typ.Oid {
	case types.T_int8:
		return newGenericBitOr[int8](typ, otyp, dist)
	case types.T_int16:
		return newGenericBitOr[int16](typ, otyp, dist)
	case types.T_int32:
		return newGenericBitOr[int32](typ, otyp, dist)
	case types.T_int64:
		return newGenericBitOr[int64](typ, otyp, dist)
	case types.T_uint8:
		return newGenericBitOr[uint8](typ, otyp, dist)
	case types.T_uint16:
		return newGenericBitOr[uint16](typ, otyp, dist)
	case types.T_uint32:
		return newGenericBitOr[uint32](typ, otyp, dist)
	case types.T_uint64:
		return newGenericBitOr[uint64](typ, otyp, dist)
	case types.T_float32:
		return newGenericBitOr[float32](typ, otyp, dist)
	case types.T_float64:
		return newGenericBitOr[float64](typ, otyp, dist)
	case types.T_binary, types.T_varbinary:
		aggPriv := NewBitOrBinary()
		if dist {
			return NewUnaryDistAgg(function.BIT_OR, aggPriv, false, typ, otyp, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
		}
		return NewUnaryAgg(function.BIT_OR, aggPriv, false, typ, otyp, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill, nil)
	}
	panic(moerr.NewInternalErrorNoCtx("unsupported type '%s' for bitor", typ))
}

func newBitXor(typ types.Type, otyp types.Type, dist bool) Agg[any] {
	switch typ.Oid {
	case types.T_int8:
		return newGenericBitXor[int8](typ, otyp, dist)
	case types.T_int16:
		return newGenericBitXor[int16](typ, otyp, dist)
	case types.T_int32:
		return newGenericBitXor[int32](typ, otyp, dist)
	case types.T_int64:
		return newGenericBitXor[int64](typ, otyp, dist)
	case types.T_uint8:
		return newGenericBitXor[uint8](typ, otyp, dist)
	case types.T_uint16:
		return newGenericBitXor[uint16](typ, otyp, dist)
	case types.T_uint32:
		return newGenericBitXor[uint32](typ, otyp, dist)
	case types.T_uint64:
		return newGenericBitXor[uint64](typ, otyp, dist)
	case types.T_float32:
		return newGenericBitXor[float32](typ, otyp, dist)
	case types.T_float64:
		return newGenericBitXor[float64](typ, otyp, dist)
	case types.T_binary, types.T_varbinary:
		aggPriv := NewBitXorBinary()
		if dist {
			return NewUnaryDistAgg(function.BIT_XOR, aggPriv, false, typ, otyp, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
		}
		return NewUnaryAgg(function.BIT_XOR, aggPriv, false, typ, otyp, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill, nil)

	}
	panic(moerr.NewInternalErrorNoCtx("unsupported type '%s' for bitxor", typ))
}

func newBitAnd(typ types.Type, otyp types.Type, dist bool) Agg[any] {
	switch typ.Oid {
	case types.T_int8:
		return newGenericBitAnd[int8](typ, otyp, dist)
	case types.T_int16:
		return newGenericBitAnd[int16](typ, otyp, dist)
	case types.T_int32:
		return newGenericBitAnd[int32](typ, otyp, dist)
	case types.T_int64:
		return newGenericBitAnd[int64](typ, otyp, dist)
	case types.T_uint8:
		return newGenericBitAnd[uint8](typ, otyp, dist)
	case types.T_uint16:
		return newGenericBitAnd[uint16](typ, otyp, dist)
	case types.T_uint32:
		return newGenericBitAnd[uint32](typ, otyp, dist)
	case types.T_uint64:
		return newGenericBitAnd[uint64](typ, otyp, dist)
	case types.T_float32:
		return newGenericBitAnd[float32](typ, otyp, dist)
	case types.T_float64:
		return newGenericBitAnd[float64](typ, otyp, dist)
	case types.T_binary, types.T_varbinary:
		aggPriv := NewBitAndBinary()
		if dist {
			return NewUnaryDistAgg(function.BIT_AND, aggPriv, false, typ, otyp, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
		}
		return NewUnaryAgg(function.BIT_AND, aggPriv, false, typ, otyp, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill, nil)
	}
	panic(moerr.NewInternalErrorNoCtx("unsupported type '%s' for bitand", typ))
}

func newVariance(typ types.Type, otyp types.Type, dist bool) Agg[any] {
	switch typ.Oid {
	case types.T_int8:
		return newGenericVariance[int8](typ, otyp, dist)
	case types.T_int16:
		return newGenericVariance[int16](typ, otyp, dist)
	case types.T_int32:
		return newGenericVariance[int32](typ, otyp, dist)
	case types.T_int64:
		return newGenericVariance[int64](typ, otyp, dist)
	case types.T_uint8:
		return newGenericVariance[uint8](typ, otyp, dist)
	case types.T_uint16:
		return newGenericVariance[uint16](typ, otyp, dist)
	case types.T_uint32:
		return newGenericVariance[uint32](typ, otyp, dist)
	case types.T_uint64:
		return newGenericVariance[uint64](typ, otyp, dist)
	case types.T_float32:
		return newGenericVariance[float32](typ, otyp, dist)
	case types.T_float64:
		return newGenericVariance[float64](typ, otyp, dist)
	case types.T_decimal64:
		aggPriv := NewVD64(typ)
		if dist {
			return NewUnaryDistAgg(function.VAR_POP, aggPriv, false, typ, otyp, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
		}
		return NewUnaryAgg(function.VAR_POP, aggPriv, false, typ, otyp, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill, nil)
	case types.T_decimal128:
		aggPriv := NewVD128(typ)
		if dist {
			return NewUnaryDistAgg(function.VAR_POP, aggPriv, false, typ, otyp, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
		}
		return NewUnaryAgg(function.VAR_POP, aggPriv, false, typ, otyp, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill, nil)

	}
	panic(moerr.NewInternalErrorNoCtx("unsupported type '%s' for variance", typ))
}

func newStdDevPop(typ types.Type, otyp types.Type, dist bool) Agg[any] {
	switch typ.Oid {
	case types.T_int8:
		return newGenericStdDevPop[int8](typ, otyp, dist)
	case types.T_int16:
		return newGenericStdDevPop[int16](typ, otyp, dist)
	case types.T_int32:
		return newGenericStdDevPop[int32](typ, otyp, dist)
	case types.T_int64:
		return newGenericStdDevPop[int64](typ, otyp, dist)
	case types.T_uint8:
		return newGenericStdDevPop[uint8](typ, otyp, dist)
	case types.T_uint16:
		return newGenericStdDevPop[uint16](typ, otyp, dist)
	case types.T_uint32:
		return newGenericStdDevPop[uint32](typ, otyp, dist)
	case types.T_uint64:
		return newGenericStdDevPop[uint64](typ, otyp, dist)
	case types.T_float32:
		return newGenericStdDevPop[float32](typ, otyp, dist)
	case types.T_float64:
		return newGenericStdDevPop[float64](typ, otyp, dist)
	case types.T_decimal64:
		aggPriv := NewStdD64(typ)
		if dist {
			return NewUnaryDistAgg(function.STDDEV_POP, aggPriv, false, typ, otyp, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
		}
		return NewUnaryAgg(function.STDDEV_POP, aggPriv, false, typ, otyp, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill, nil)
	case types.T_decimal128:
		aggPriv := NewStdD128(typ)
		if dist {
			return NewUnaryDistAgg(function.STDDEV_POP, aggPriv, false, typ, otyp, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
		}
		return NewUnaryAgg(function.STDDEV_POP, aggPriv, false, typ, otyp, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill, nil)

	}
	panic(moerr.NewInternalErrorNoCtx("unsupported type '%s' for stddev", typ))
}

func newMedian(typ types.Type, otyp types.Type, dist bool) Agg[any] {
	switch typ.Oid {
	case types.T_int8:
		return newGenericMedian[int8](typ, otyp, dist)
	case types.T_int16:
		return newGenericMedian[int16](typ, otyp, dist)
	case types.T_int32:
		return newGenericMedian[int32](typ, otyp, dist)
	case types.T_int64:
		return newGenericMedian[int64](typ, otyp, dist)
	case types.T_uint8:
		return newGenericMedian[uint8](typ, otyp, dist)
	case types.T_uint16:
		return newGenericMedian[uint16](typ, otyp, dist)
	case types.T_uint32:
		return newGenericMedian[uint32](typ, otyp, dist)
	case types.T_uint64:
		return newGenericMedian[uint64](typ, otyp, dist)
	case types.T_float32:
		return newGenericMedian[float32](typ, otyp, dist)
	case types.T_float64:
		return newGenericMedian[float64](typ, otyp, dist)
	case types.T_decimal64:
		aggPriv := NewD64Median()
		if dist {
			panic(moerr.NewNotSupportedNoCtx("median in distinct mode"))
		}
		return NewUnaryAgg(function.MEDIAN, aggPriv, false, typ, otyp, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill, nil)
	case types.T_decimal128:
		aggPriv := NewD128Median()
		if dist {
			panic(moerr.NewNotSupportedNoCtx("median in distinct mode"))
		}
		return NewUnaryAgg(function.MEDIAN, aggPriv, false, typ, otyp, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill, nil)
	}
	panic(moerr.NewNotSupportedNoCtx("median on type '%s'", typ))
}

func newGenericAnyValue[T any](typ types.Type, otyp types.Type, dist bool) Agg[any] {
	aggPriv := NewAnyValue[T]()
	if dist {
		return NewUnaryDistAgg(function.ANY_VALUE, aggPriv, false, typ, otyp, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
	}
	return NewUnaryAgg(function.ANY_VALUE, aggPriv, false, typ, otyp, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill, nil)
}

func newStrAnyValue(typ types.Type, otyp types.Type, dist bool) Agg[any] {
	aggPriv := NewStrAnyValue()
	if dist {
		return NewUnaryDistAgg(function.ANY_VALUE, aggPriv, false, typ, otyp, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
	}
	return NewUnaryAgg(function.ANY_VALUE, aggPriv, false, typ, otyp, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill, nil)
}

func newGenericSum[T1 Numeric, T2 ReturnTyp](typ types.Type, otyp types.Type, dist bool) Agg[any] {
	aggPriv := NewSum[T1, T2]()
	if dist {
		return NewUnaryDistAgg(function.SUM, aggPriv, false, typ, otyp, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
	}
	return NewUnaryAgg(function.SUM, aggPriv, false, typ, otyp, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill, nil)
}

func newGenericAvg[T Numeric](typ types.Type, otyp types.Type, dist bool) Agg[any] {
	aggPriv := NewAvg[T]()
	if dist {
		return NewUnaryDistAgg(function.AVG, aggPriv, false, typ, otyp, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
	}
	return NewUnaryAgg(function.AVG, aggPriv, false, typ, otyp, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill, nil)
}

func newGenericMax[T Compare](typ types.Type, otyp types.Type, dist bool) Agg[any] {
	aggPriv := NewMax[T]()
	if dist {
		return NewUnaryDistAgg(function.MAX, aggPriv, false, typ, otyp, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
	}
	return NewUnaryAgg(function.MAX, aggPriv, false, typ, otyp, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill, nil)
}

func newGenericMin[T Compare](typ types.Type, otyp types.Type, dist bool) Agg[any] {
	aggPriv := NewMin[T]()
	if dist {
		return NewUnaryDistAgg(function.MIN, aggPriv, false, typ, otyp, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
	}
	return NewUnaryAgg(function.MIN, aggPriv, false, typ, otyp, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill, nil)
}

func newGenericCount[T types.OrderedT | Decimal128AndString](typ types.Type, otyp types.Type, dist bool, isStar bool) Agg[any] {
	aggPriv := NewCount[T](isStar)
	if dist {
		return NewUnaryDistAgg(function.COUNT, aggPriv, true, typ, otyp, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
	}
	return NewUnaryAgg(function.COUNT, aggPriv, true, typ, otyp, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill, nil)
}

func newGenericApproxcd[T any](typ types.Type, otyp types.Type, dist bool) Agg[any] {
	aggPriv := NewApproxc[T]()
	if dist {
		return NewUnaryDistAgg(function.APPROX_COUNT, aggPriv, false, typ, otyp, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
	}
	return NewUnaryAgg(function.APPROX_COUNT, aggPriv, false, typ, otyp, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill, nil)
}

func newGenericBitOr[T types.Ints | types.UInts | types.Floats](typ types.Type, otyp types.Type, dist bool) Agg[any] {
	aggPriv := NewBitOr[T]()
	if dist {
		return NewUnaryDistAgg(function.BIT_OR, aggPriv, false, typ, otyp, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
	}
	return NewUnaryAgg(function.BIT_OR, aggPriv, false, typ, otyp, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill, nil)
}

func newGenericBitXor[T types.Ints | types.UInts | types.Floats](typ types.Type, otyp types.Type, dist bool) Agg[any] {
	aggPriv := NewBitXor[T]()
	if dist {
		return NewUnaryDistAgg(function.BIT_XOR, aggPriv, false, typ, otyp, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
	}
	return NewUnaryAgg(function.BIT_XOR, aggPriv, false, typ, otyp, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill, nil)
}

func newGenericBitAnd[T types.Ints | types.UInts | types.Floats](typ types.Type, otyp types.Type, dist bool) Agg[any] {
	aggPriv := NewBitAnd[T]()
	if dist {
		return NewUnaryDistAgg(function.BIT_AND, aggPriv, false, typ, otyp, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
	}
	return NewUnaryAgg(function.BIT_AND, aggPriv, false, typ, otyp, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill, nil)
}

func newGenericVariance[T types.Ints | types.UInts | types.Floats](typ types.Type, otyp types.Type, dist bool) Agg[any] {
	aggPriv := NewVariance[T]()
	if dist {
		return NewUnaryDistAgg(function.VAR_POP, aggPriv, false, typ, otyp, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
	}
	return NewUnaryAgg(function.VAR_POP, aggPriv, false, typ, otyp, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill, nil)
}

func newGenericStdDevPop[T types.Ints | types.UInts | types.Floats](typ types.Type, otyp types.Type, dist bool) Agg[any] {
	aggPriv := NewStdDevPop[T]()
	if dist {
		return NewUnaryDistAgg(function.STDDEV_POP, aggPriv, false, typ, otyp, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
	}
	return NewUnaryAgg(function.STDDEV_POP, aggPriv, false, typ, otyp, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill, nil)
}
func newGenericMedian[T Numeric](typ types.Type, otyp types.Type, dist bool) Agg[any] {
	aggPriv := NewMedian[T]()
	if dist {
		panic(moerr.NewNotSupportedNoCtx("median in distinct mode"))
	}
	return NewUnaryAgg(function.MEDIAN, aggPriv, false, typ, otyp, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill, nil)
}

func NewGroupConcat(typ types.Type, otyp types.Type, dist bool, config any) Agg[any] {

	separator := ","
	bytes, ok := config.([]byte)
	if bytes != nil && ok {
		separator = string(bytes)
	}

	switch typ.Oid {
	case types.T_varchar:
		aggPriv := newGroupConcat(separator)
		if dist {
			return NewUnaryDistAgg(function.GROUP_CONCAT, aggPriv, false, typ, otyp, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
		}
		return NewUnaryAgg(function.GROUP_CONCAT, aggPriv, false, typ, otyp, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill, nil)
	}

	panic(moerr.NewInternalErrorNoCtx("unsupported type '%s' for group_concat", typ))
}
