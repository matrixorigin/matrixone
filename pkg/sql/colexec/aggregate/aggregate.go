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

package aggregate

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/agg"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/agg/anyvalue"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/agg/approxcd"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/agg/avg"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/agg/bit_and"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/agg/bit_or"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/agg/bit_xor"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/agg/count"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/agg/max"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/agg/min"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/agg/stddevpop"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/agg/sum"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/agg/variance"
)

func ReturnType(op int, typ types.Type) (types.Type, error) {
	var otyp types.Type

	switch op {
	case Avg:
		otyp = avg.ReturnType([]types.Type{typ})
	case Max:
		otyp = max.ReturnType([]types.Type{typ})
	case Min:
		otyp = min.ReturnType([]types.Type{typ})
	case Sum:
		otyp = sum.ReturnType([]types.Type{typ})
	case Count, StarCount:
		otyp = count.ReturnType([]types.Type{typ})
	case ApproxCountDistinct:
		otyp = approxcd.ReturnType([]types.Type{typ})
	case Variance:
		otyp = variance.ReturnType([]types.Type{typ})
	case BitAnd:
		otyp = bit_and.ReturnType([]types.Type{typ})
	case BitXor:
		otyp = bit_or.ReturnType([]types.Type{typ})
	case BitOr:
		otyp = bit_xor.ReturnType([]types.Type{typ})
	case StdDevPop:
		otyp = stddevpop.ReturnType([]types.Type{typ})
	}
	if otyp.Oid == types.T_any {
		return typ, fmt.Errorf("'%v' not support %s", typ, Names[op])
	}
	return otyp, nil
}

func New(op int, dist bool, typ types.Type) (agg.Agg[any], error) {
	switch op {
	case Sum:
		return NewSum(typ, dist), nil
	case Avg:
		return NewAvg(typ, dist), nil
	case Max:
		return NewMax(typ, dist), nil
	case Min:
		return NewMin(typ, dist), nil
	case Count:
		return NewCount(typ, dist, false), nil
	case StarCount:
		return NewCount(typ, dist, true), nil
	case ApproxCountDistinct:
		return NewApprox(typ, dist), nil
	case Variance:
		return NewVariance(typ, dist), nil
	case BitAnd:
		return NewBitAnd(typ, dist), nil
	case BitXor:
		return NewBitXor(typ, dist), nil
	case BitOr:
		return NewBitOr(typ, dist), nil
	case StdDevPop:
		return NewStdDevPop(typ, dist), nil
	case AnyValue:
		return NewAnyValue(typ, dist), nil
	}
	panic(fmt.Errorf("unsupport type '%s' for aggregate %s", typ, Names[op]))
}

func NewCount(typ types.Type, dist bool, isStar bool) agg.Agg[any] {
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
	case types.T_date:
		return newGenericCount[types.Date](typ, dist, isStar)
	case types.T_datetime:
		return newGenericCount[types.Datetime](typ, dist, isStar)
	case types.T_timestamp:
		return newGenericCount[types.Timestamp](typ, dist, isStar)
	case types.T_decimal64:
		return newGenericCount[types.Decimal64](typ, dist, isStar)
	case types.T_decimal128:
		return newGenericCount[types.Decimal128](typ, dist, isStar)
	}
	panic(fmt.Errorf("unsupport type '%s' for anyvalue", typ))
}

func NewAnyValue(typ types.Type, dist bool) agg.Agg[any] {
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
	case types.T_blob:
		return newGenericAnyValue[[]byte](typ, dist)
	case types.T_date:
		return newGenericAnyValue[types.Date](typ, dist)
	case types.T_datetime:
		return newGenericAnyValue[types.Datetime](typ, dist)
	case types.T_timestamp:
		return newGenericAnyValue[types.Timestamp](typ, dist)
	case types.T_decimal64:
		return newGenericAnyValue[types.Decimal64](typ, dist)
	case types.T_decimal128:
		return newGenericAnyValue[types.Decimal128](typ, dist)
	}
	panic(fmt.Errorf("unsupport type '%s' for anyvalue", typ))
}

func NewAvg(typ types.Type, dist bool) agg.Agg[any] {
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
		aggPriv := avg.NewD64Avg()
		if dist {
			return agg.NewUnaryDistAgg(false, typ, avg.ReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
		}
		return agg.NewUnaryAgg(aggPriv, false, typ, avg.ReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
	case types.T_decimal128:
		aggPriv := avg.NewD128Avg()
		if dist {
			return agg.NewUnaryDistAgg(false, typ, avg.ReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
		}
		return agg.NewUnaryAgg(aggPriv, false, typ, avg.ReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
	}
	panic(fmt.Errorf("unsupport type '%s' for avg", typ))
}

func NewSum(typ types.Type, dist bool) agg.Agg[any] {
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
		aggPriv := sum.NewD64Sum()
		if dist {
			return agg.NewUnaryDistAgg(false, typ, sum.ReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
		}
		return agg.NewUnaryAgg(aggPriv, false, typ, sum.ReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
	case types.T_decimal128:
		aggPriv := sum.NewD128Sum()
		if dist {
			return agg.NewUnaryDistAgg(false, typ, sum.ReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
		}
		return agg.NewUnaryAgg(aggPriv, false, typ, sum.ReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
	}
	panic(fmt.Errorf("unsupport type '%s' for sum", typ))
}

func NewMax(typ types.Type, dist bool) agg.Agg[any] {
	switch typ.Oid {
	case types.T_bool:
		aggPriv := max.NewBoolMax()
		if dist {
			return agg.NewUnaryDistAgg(false, typ, max.ReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
		}
		return agg.NewUnaryAgg(aggPriv, false, typ, max.ReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
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
	case types.T_char:
		aggPriv := max.NewStrMax()
		if dist {
			return agg.NewUnaryDistAgg(false, typ, max.ReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
		}
		return agg.NewUnaryAgg(aggPriv, false, typ, max.ReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
	case types.T_varchar:
		aggPriv := max.NewStrMax()
		if dist {
			return agg.NewUnaryDistAgg(false, typ, max.ReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
		}
		return agg.NewUnaryAgg(aggPriv, false, typ, max.ReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
	case types.T_blob:
		aggPriv := max.NewStrMax()
		if dist {
			return agg.NewUnaryDistAgg(false, typ, max.ReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
		}
		return agg.NewUnaryAgg(aggPriv, false, typ, max.ReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
	case types.T_date:
		return newGenericMax[types.Date](typ, dist)
	case types.T_datetime:
		return newGenericMax[types.Datetime](typ, dist)
	case types.T_timestamp:
		return newGenericMax[types.Timestamp](typ, dist)
	case types.T_decimal64:
		aggPriv := max.NewD64Max()
		if dist {
			return agg.NewUnaryDistAgg(false, typ, max.ReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
		}
		return agg.NewUnaryAgg(aggPriv, false, typ, max.ReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
	case types.T_decimal128:
		aggPriv := max.NewD128Max()
		if dist {
			return agg.NewUnaryDistAgg(false, typ, max.ReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
		}
		return agg.NewUnaryAgg(aggPriv, false, typ, max.ReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
	}
	panic(fmt.Errorf("unsupport type '%s' for anyvalue", typ))
}

func NewMin(typ types.Type, dist bool) agg.Agg[any] {
	switch typ.Oid {
	case types.T_bool:
		aggPriv := min.NewBoolMin()
		if dist {
			return agg.NewUnaryDistAgg(false, typ, min.ReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
		}
		return agg.NewUnaryAgg(aggPriv, false, typ, min.ReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
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
	case types.T_char:
		aggPriv := min.NewStrMin()
		if dist {
			return agg.NewUnaryDistAgg(false, typ, min.ReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
		}
		return agg.NewUnaryAgg(aggPriv, false, typ, min.ReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
	case types.T_varchar:
		aggPriv := min.NewStrMin()
		if dist {
			return agg.NewUnaryDistAgg(false, typ, min.ReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
		}
		return agg.NewUnaryAgg(aggPriv, false, typ, min.ReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
	case types.T_blob:
		aggPriv := min.NewStrMin()
		if dist {
			return agg.NewUnaryDistAgg(false, typ, min.ReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
		}
		return agg.NewUnaryAgg(aggPriv, false, typ, min.ReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
	case types.T_date:
		return newGenericMin[types.Date](typ, dist)
	case types.T_datetime:
		return newGenericMin[types.Datetime](typ, dist)
	case types.T_timestamp:
		return newGenericMin[types.Timestamp](typ, dist)
	case types.T_decimal64:
		aggPriv := min.NewD64Min()
		if dist {
			return agg.NewUnaryDistAgg(false, typ, min.ReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
		}
		return agg.NewUnaryAgg(aggPriv, false, typ, min.ReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
	case types.T_decimal128:
		aggPriv := min.NewD128Min()
		if dist {
			return agg.NewUnaryDistAgg(false, typ, min.ReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
		}
		return agg.NewUnaryAgg(aggPriv, false, typ, min.ReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
	}
	panic(fmt.Errorf("unsupport type '%s' for anyvalue", typ))
}

func NewApprox(typ types.Type, dist bool) agg.Agg[any] {
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
	case types.T_date:
		return newGenericApproxcd[types.Date](typ, dist)
	case types.T_datetime:
		return newGenericApproxcd[types.Datetime](typ, dist)
	case types.T_timestamp:
		return newGenericApproxcd[types.Timestamp](typ, dist)
	case types.T_decimal64:
		return newGenericApproxcd[types.Decimal64](typ, dist)
	case types.T_decimal128:
		return newGenericApproxcd[types.Decimal128](typ, dist)
	}
	panic(fmt.Errorf("unsupport type '%s' for anyvalue", typ))
}

func NewBitOr(typ types.Type, dist bool) agg.Agg[any] {
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
	}
	panic(fmt.Errorf("unsupport type '%s' for avg", typ))
}

func NewBitXor(typ types.Type, dist bool) agg.Agg[any] {
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
	}
	panic(fmt.Errorf("unsupport type '%s' for avg", typ))
}

func NewBitAnd(typ types.Type, dist bool) agg.Agg[any] {
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
	}
	panic(fmt.Errorf("unsupport type '%s' for avg", typ))
}

func NewVariance(typ types.Type, dist bool) agg.Agg[any] {
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
		aggPriv := variance.New2()
		if dist {
			return agg.NewUnaryDistAgg(false, typ, variance.ReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
		}
		return agg.NewUnaryAgg(aggPriv, false, typ, variance.ReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
	case types.T_decimal128:
		aggPriv := variance.New3()
		if dist {
			return agg.NewUnaryDistAgg(false, typ, variance.ReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
		}
		return agg.NewUnaryAgg(aggPriv, false, typ, variance.ReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)

	}
	panic(fmt.Errorf("unsupport type '%s' for avg", typ))
}

func NewStdDevPop(typ types.Type, dist bool) agg.Agg[any] {
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
		aggPriv := stddevpop.New2()
		if dist {
			return agg.NewUnaryDistAgg(false, typ, stddevpop.ReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
		}
		return agg.NewUnaryAgg(aggPriv, false, typ, stddevpop.ReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
	case types.T_decimal128:
		aggPriv := stddevpop.New3()
		if dist {
			return agg.NewUnaryDistAgg(false, typ, stddevpop.ReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
		}
		return agg.NewUnaryAgg(aggPriv, false, typ, stddevpop.ReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)

	}
	panic(fmt.Errorf("unsupport type '%s' for avg", typ))
}

func newGenericAnyValue[T any](typ types.Type, dist bool) agg.Agg[any] {
	aggPriv := anyvalue.NewAnyvalue[T]()
	if dist {
		return agg.NewUnaryDistAgg(false, typ, anyvalue.ReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
	}
	return agg.NewUnaryAgg(aggPriv, false, typ, anyvalue.ReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
}

func newGenericSum[T1 sum.Numeric, T2 sum.ReturnTyp](typ types.Type, dist bool) agg.Agg[any] {
	aggPriv := sum.NewSum[T1, T2]()
	if dist {
		return agg.NewUnaryDistAgg(false, typ, sum.ReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
	}
	return agg.NewUnaryAgg(aggPriv, false, typ, sum.ReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
}

func newGenericAvg[T sum.Numeric](typ types.Type, dist bool) agg.Agg[any] {
	aggPriv := avg.NewAvg[T]()
	if dist {
		return agg.NewUnaryDistAgg(false, typ, avg.ReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
	}
	return agg.NewUnaryAgg(aggPriv, false, typ, avg.ReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
}

func newGenericMax[T max.Compare](typ types.Type, dist bool) agg.Agg[any] {
	aggPriv := max.NewMax[T]()
	if dist {
		return agg.NewUnaryDistAgg(false, typ, max.ReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
	}
	return agg.NewUnaryAgg(aggPriv, false, typ, max.ReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
}

func newGenericMin[T max.Compare](typ types.Type, dist bool) agg.Agg[any] {
	aggPriv := min.NewMin[T]()
	if dist {
		return agg.NewUnaryDistAgg(false, typ, min.ReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
	}
	return agg.NewUnaryAgg(aggPriv, false, typ, min.ReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
}

func newGenericCount[T types.Generic | count.Decimal128AndString](typ types.Type, dist bool, isStar bool) agg.Agg[any] {
	aggPriv := count.New[T](isStar)
	if dist {
		return agg.NewUnaryDistAgg(true, typ, count.ReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
	}
	return agg.NewUnaryAgg(aggPriv, true, typ, count.ReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
}

func newGenericApproxcd[T any](typ types.Type, dist bool) agg.Agg[any] {
	aggPriv := approxcd.NewApproxc[T]()
	if dist {
		return agg.NewUnaryDistAgg(false, typ, approxcd.ReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
	}
	return agg.NewUnaryAgg(aggPriv, false, typ, approxcd.ReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
}

func newGenericBitOr[T types.Ints | types.UInts | types.Floats](typ types.Type, dist bool) agg.Agg[any] {
	aggPriv := bit_or.New[T]()
	if dist {
		return agg.NewUnaryDistAgg(false, typ, bit_or.ReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
	}
	return agg.NewUnaryAgg(aggPriv, false, typ, bit_or.ReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
}

func newGenericBitXor[T types.Ints | types.UInts | types.Floats](typ types.Type, dist bool) agg.Agg[any] {
	aggPriv := bit_xor.New[T]()
	if dist {
		return agg.NewUnaryDistAgg(false, typ, bit_xor.ReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
	}
	return agg.NewUnaryAgg(aggPriv, false, typ, bit_xor.ReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
}

func newGenericBitAnd[T types.Ints | types.UInts | types.Floats](typ types.Type, dist bool) agg.Agg[any] {
	aggPriv := bit_and.New[T]()
	if dist {
		return agg.NewUnaryDistAgg(false, typ, bit_and.ReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
	}
	return agg.NewUnaryAgg(aggPriv, false, typ, bit_and.ReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
}

func newGenericVariance[T types.Ints | types.UInts | types.Floats](typ types.Type, dist bool) agg.Agg[any] {
	aggPriv := variance.New1[T]()
	if dist {
		return agg.NewUnaryDistAgg(false, typ, variance.ReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
	}
	return agg.NewUnaryAgg(aggPriv, false, typ, variance.ReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
}

func newGenericStdDevPop[T types.Ints | types.UInts | types.Floats](typ types.Type, dist bool) agg.Agg[any] {
	aggPriv := stddevpop.New[T]()
	if dist {
		return agg.NewUnaryDistAgg(false, typ, stddevpop.ReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
	}
	return agg.NewUnaryAgg(aggPriv, false, typ, stddevpop.ReturnType([]types.Type{typ}), aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill)
}
