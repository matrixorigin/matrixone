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
	"math"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/agg"
)

var (
	// bit_and() supported input type and output type.
	AggBitAndSupportedParameters = []types.T{
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_float32, types.T_float64,
		types.T_binary, types.T_varbinary,
	}
	AggBitAndReturnType = func(typs []types.Type) types.Type {
		if typs[0].Oid == types.T_binary || typs[0].Oid == types.T_varbinary {
			return typs[0]
		}
		return types.New(types.T_uint64, 0, 0)
	}
)

func NewAggBitAnd(overloadID int64, dist bool, inputTypes []types.Type, outputType types.Type, _ any) (agg.Agg[any], error) {
	switch inputTypes[0].Oid {
	case types.T_uint8:
		return newGenericBitAnd[uint8](overloadID, inputTypes[0], outputType, dist)
	case types.T_uint16:
		return newGenericBitAnd[uint16](overloadID, inputTypes[0], outputType, dist)
	case types.T_uint32:
		return newGenericBitAnd[uint32](overloadID, inputTypes[0], outputType, dist)
	case types.T_uint64:
		return newGenericBitAnd[uint64](overloadID, inputTypes[0], outputType, dist)
	case types.T_int8:
		return newGenericBitAnd[int8](overloadID, inputTypes[0], outputType, dist)
	case types.T_int16:
		return newGenericBitAnd[int16](overloadID, inputTypes[0], outputType, dist)
	case types.T_int32:
		return newGenericBitAnd[int32](overloadID, inputTypes[0], outputType, dist)
	case types.T_int64:
		return newGenericBitAnd[int64](overloadID, inputTypes[0], outputType, dist)
	case types.T_float32:
		return newGenericBitAnd[float32](overloadID, inputTypes[0], outputType, dist)
	case types.T_float64:
		return newGenericBitAnd[float64](overloadID, inputTypes[0], outputType, dist)
	case types.T_binary, types.T_varbinary:
		aggPriv := &sAggBinaryBitAnd{}
		if dist {
			return agg.NewUnaryDistAgg(overloadID, aggPriv, false, inputTypes[0], outputType, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill), nil
		}
		return agg.NewUnaryAgg(overloadID, aggPriv, false, inputTypes[0], outputType, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill), nil
	}
	return nil, moerr.NewInternalErrorNoCtx("unsupported type '%s' for bit_and", inputTypes[0])
}

func newGenericBitAnd[T numeric](overloadID int64, inputType types.Type, outputType types.Type, dist bool) (agg.Agg[any], error) {
	aggPriv := &sAggBitAnd[T]{}
	if dist {
		return agg.NewUnaryDistAgg[T, uint64](overloadID, aggPriv, false, inputType, outputType, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill), nil
	}
	return agg.NewUnaryAgg[T, uint64](overloadID, aggPriv, false, inputType, outputType, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill), nil
}

type sAggBitAnd[T numeric] struct{}
type sAggBinaryBitAnd struct{}

func (s *sAggBitAnd[T]) Dup() agg.AggStruct {
	return &sAggBitAnd[T]{}
}
func (s *sAggBitAnd[T]) Grows(_ int)         {}
func (s *sAggBitAnd[T]) Free(_ *mpool.MPool) {}
func (s *sAggBitAnd[T]) Fill(groupNumber int64, value T, lastResult uint64, count int64, isEmpty bool, isNull bool) (uint64, bool, error) {
	if !isNull {
		if isEmpty {
			lastResult = ^uint64(0)
		}

		vv := float64(value)
		if vv > math.MaxUint64 {
			return math.MaxInt64 & lastResult, false, nil
		}
		if vv < 0 {
			return uint64(int64(value)) & lastResult, false, nil
		}
		return uint64(value) & lastResult, false, nil
	}
	return lastResult, isEmpty, nil
}
func (s *sAggBitAnd[T]) Merge(groupNumber1 int64, groupNumber2 int64, result1, result2 uint64, isEmpty1, isEmpty2 bool, _ any) (uint64, bool, error) {
	if isEmpty1 {
		result1 = ^uint64(0)
	}
	if isEmpty2 {
		result2 = ^uint64(0)
	}
	return result1 & result2, isEmpty1 && isEmpty2, nil
}
func (s *sAggBitAnd[T]) Eval(lastResult []uint64) ([]uint64, error) {
	return lastResult, nil
}
func (s *sAggBitAnd[T]) MarshalBinary() ([]byte, error) {
	return nil, nil
}
func (s *sAggBitAnd[T]) UnmarshalBinary(_ []byte) error {
	return nil
}

func (s *sAggBinaryBitAnd) Dup() agg.AggStruct {
	return &sAggBinaryBitAnd{}
}
func (s *sAggBinaryBitAnd) Grows(_ int)         {}
func (s *sAggBinaryBitAnd) Free(_ *mpool.MPool) {}
func (s *sAggBinaryBitAnd) Fill(groupNumber int64, value []byte, lastResult []byte, count int64, isEmpty bool, isNull bool) ([]byte, bool, error) {
	if !isNull {
		if isEmpty {
			result := make([]byte, len(value))
			copy(result, value)
			return result, false, nil
		}

		types.BitAnd(lastResult, lastResult, value)
		return lastResult, false, nil
	}
	return lastResult, isEmpty, nil
}
func (s *sAggBinaryBitAnd) Merge(groupNumber1 int64, groupNumber2 int64, result1, result2 []byte, isEmpty1, isEmpty2 bool, _ any) ([]byte, bool, error) {
	if isEmpty1 {
		return result2, isEmpty2, nil
	}
	if isEmpty2 {
		return result1, isEmpty1, nil
	}
	types.BitAnd(result1, result1, result2)
	return result1, false, nil
}
func (s *sAggBinaryBitAnd) Eval(lastResult [][]byte) ([][]byte, error) {
	return lastResult, nil
}
func (s *sAggBinaryBitAnd) MarshalBinary() ([]byte, error) { return nil, nil }
func (s *sAggBinaryBitAnd) UnmarshalBinary(_ []byte) error { return nil }
