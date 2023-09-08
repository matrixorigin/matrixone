// Copyright 2021 - 2022 Matrix Origin
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

package functionAgg

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/agg"
)

var (
	AggAvgSupportedParameters = []types.T{
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_float32, types.T_float64,
		types.T_decimal64, types.T_decimal128,
	}
	AggAvgReturnType = func(typs []types.Type) types.Type {
		switch typs[0].Oid {
		case types.T_decimal64:
			s := int32(12)
			if s < typs[0].Scale {
				s = typs[0].Scale
			}
			if s > typs[0].Scale+6 {
				s = typs[0].Scale + 6
			}
			return types.New(types.T_decimal128, 18, s)
		case types.T_decimal128:
			s := int32(12)
			if s < typs[0].Scale {
				s = typs[0].Scale
			}
			if s > typs[0].Scale+6 {
				s = typs[0].Scale + 6
			}
			return types.New(types.T_decimal128, 18, s)
		case types.T_float32, types.T_float64:
			return types.New(types.T_float64, 0, 0)
		case types.T_int8, types.T_int16, types.T_int32, types.T_int64:
			return types.New(types.T_float64, 0, 0)
		case types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64:
			return types.New(types.T_float64, 0, 0)
		}
		panic(moerr.NewInternalErrorNoCtx("unsupported type '%v' for avg", typs[0]))
	}
)

func NewAggAvg(overloadID int64, dist bool, inputTypes []types.Type, outputType types.Type, _ any) (agg.Agg[any], error) {
	switch inputTypes[0].Oid {
	case types.T_uint8:
		return newGenericAvg[uint8](overloadID, inputTypes[0], outputType, dist)
	case types.T_uint16:
		return newGenericAvg[uint16](overloadID, inputTypes[0], outputType, dist)
	case types.T_uint32:
		return newGenericAvg[uint32](overloadID, inputTypes[0], outputType, dist)
	case types.T_uint64:
		return newGenericAvg[uint64](overloadID, inputTypes[0], outputType, dist)
	case types.T_int8:
		return newGenericAvg[int8](overloadID, inputTypes[0], outputType, dist)
	case types.T_int16:
		return newGenericAvg[int16](overloadID, inputTypes[0], outputType, dist)
	case types.T_int32:
		return newGenericAvg[int32](overloadID, inputTypes[0], outputType, dist)
	case types.T_int64:
		return newGenericAvg[int64](overloadID, inputTypes[0], outputType, dist)
	case types.T_float32:
		return newGenericAvg[float32](overloadID, inputTypes[0], outputType, dist)
	case types.T_float64:
		return newGenericAvg[float64](overloadID, inputTypes[0], outputType, dist)
	case types.T_decimal64:
		aggPriv := agg.NewD64Avg(inputTypes[0])
		if dist {
			return agg.NewUnaryDistAgg(overloadID, aggPriv, false, inputTypes[0], outputType, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill), nil
		}
		return agg.NewUnaryAgg(overloadID, aggPriv, false, inputTypes[0], outputType, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill, nil), nil
	case types.T_decimal128:
		aggPriv := agg.NewD64Avg(inputTypes[0])
		if dist {
			return agg.NewUnaryDistAgg(overloadID, aggPriv, false, inputTypes[0], outputType, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill), nil
		}
		return agg.NewUnaryAgg(overloadID, aggPriv, false, inputTypes[0], outputType, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill, nil), nil
	}
	return nil, moerr.NewInternalErrorNoCtx("unsupported type '%s' for avg", inputTypes[0])
}

func newGenericAvg[T numeric](overloadID int64, typ types.Type, otyp types.Type, dist bool) (agg.Agg[any], error) {
	aggPriv := agg.NewAvg[T]()
	if dist {
		return agg.NewUnaryDistAgg(overloadID, aggPriv, false, typ, otyp, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill), nil
	}
	return agg.NewUnaryAgg(overloadID, aggPriv, false, typ, otyp, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill, nil), nil
}

type sAggAvg[T numeric] struct { cnts []int64}
type sAggDecimal64Avg struct { cnts []int64, typ types.Type}
type sAggDecimal128Avg struct { cnts []int64, typ types.Type}

func (s *sAggAvg[T]) Grows(cnt int) {
	for i := 0; i < cnt; i++ {
		s.cnts = append(s.cnts, 0)
	}
}
func (s *sAggAvg[T]) Free(_ *mpool.MPool) {}
func (s *sAggAvg[T]) Fill(groupNumber int64, values T, lastResult float64, count int64, isEmpty bool, isNull bool) (newResult float64, isStillEmpty bool, err error) {
	if !isNull {
		s.cnts[groupNumber] += count
		return lastResult + float64(values) * float64(count), false, nil
	}
	return lastResult, isEmpty, nil
}
func (s *sAggAvg[T]) Merge(groupNumber1 int64, groupNumber2 int64, result1 types.Decimal64, result2 types.Decimal64, isEmpty1 bool, isEmpty2 bool, priv2 any) (newResult types.Decimal64, isStillEmpty bool, err error) {
	if !isEmpty2 {
		bPriv := priv2.(*sAggAvg[T])
		s.cnts[groupNumber1] += bPriv.cnts[groupNumber2]
		if !isEmpty1 {
			return result1 + result2, false, nil
		}
		return result2, false, nil
	}
	return result1, isEmpty1, nil
}
func (s *sAggAvg[T]) Eval(lastResult []T, _ error) ([]T, error) {
	for i := range lastResult {
		if s.cnts[i] == 0 {
			continue
		}
		lastResult[i] = lastResult[i] / T(s.cnts[i])
	}
	return lastResult, nil
}
func (s *sAggAvg[T]) MarshalBinary() ([]byte, error) { return types.EncodeSlice[int64](s.cnts), nil }
func (s *sAggAvg[T]) UnmarshalBinary(data []byte) error   {
	// avoid rpc reusing the buffer.
	copyData := make([]byte, len(data))
	copy(copyData, data)
	s.cnts = types.DecodeSlice[int64](copyData)
	return nil
}