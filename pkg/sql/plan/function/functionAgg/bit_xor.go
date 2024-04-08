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
	// bit_xor() supported input type and output type.
	AggBitXorSupportedParameters = []types.T{
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_float32, types.T_float64,
		types.T_binary, types.T_varbinary,
	}
	AggBitXorReturnType = AggBitAndReturnType
)

func NewAggBitXor(overloadID int64, dist bool, inputTypes []types.Type, outputType types.Type, _ any) (agg.Agg[any], error) {
	switch inputTypes[0].Oid {
	case types.T_uint8:
		return newGenericBitXor[uint8](overloadID, inputTypes[0], outputType, dist)
	case types.T_uint16:
		return newGenericBitXor[uint16](overloadID, inputTypes[0], outputType, dist)
	case types.T_uint32:
		return newGenericBitXor[uint32](overloadID, inputTypes[0], outputType, dist)
	case types.T_uint64:
		return newGenericBitXor[uint64](overloadID, inputTypes[0], outputType, dist)
	case types.T_int8:
		return newGenericBitXor[int8](overloadID, inputTypes[0], outputType, dist)
	case types.T_int16:
		return newGenericBitXor[int16](overloadID, inputTypes[0], outputType, dist)
	case types.T_int32:
		return newGenericBitXor[int32](overloadID, inputTypes[0], outputType, dist)
	case types.T_int64:
		return newGenericBitXor[int64](overloadID, inputTypes[0], outputType, dist)
	case types.T_float32:
		return newGenericBitXor[float32](overloadID, inputTypes[0], outputType, dist)
	case types.T_float64:
		return newGenericBitXor[float64](overloadID, inputTypes[0], outputType, dist)
	case types.T_binary, types.T_varbinary:
		aggPriv := &sAggBinaryBitXor{}
		if dist {
			return agg.NewUnaryDistAgg(overloadID, aggPriv, false, inputTypes[0], outputType, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill), nil
		}
		return agg.NewUnaryAgg(overloadID, aggPriv, false, inputTypes[0], outputType, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill), nil
	}
	return nil, moerr.NewInternalErrorNoCtx("unsupported type '%s' for bit_xor", inputTypes[0])
}

func newGenericBitXor[T numeric](overloadID int64, inputType types.Type, outputType types.Type, dist bool) (agg.Agg[any], error) {
	aggPriv := &sAggBitXor[T]{}
	if dist {
		return agg.NewUnaryDistAgg(overloadID, aggPriv, false, inputType, outputType, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill), nil
	}
	return agg.NewUnaryAgg(overloadID, aggPriv, false, inputType, outputType, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill), nil
}

type sAggBitXor[T numeric] struct{}
type sAggBinaryBitXor struct{}

func (s *sAggBitXor[T]) Dup() agg.AggStruct {
	return &sAggBitXor[T]{}
}
func (s *sAggBitXor[T]) Grows(_ int)         {}
func (s *sAggBitXor[T]) Free(_ *mpool.MPool) {}
func (s *sAggBitXor[T]) Fill(groupNumber int64, value T, lastResult uint64, count int64, isEmpty bool, isNull bool) (uint64, bool, error) {
	if !isNull {
		if count%2 == 0 {
			return lastResult, isEmpty, nil
		}
		if isEmpty {
			lastResult = 0
		}

		vv := float64(value)
		if vv > math.MaxUint64 {
			return math.MaxInt64 ^ lastResult, false, nil
		}
		if vv < 0 {
			return uint64(int64(value)) ^ lastResult, false, nil
		}
		return uint64(value) ^ lastResult, false, nil
	}
	return lastResult, isEmpty, nil
}
func (s *sAggBitXor[T]) Merge(groupNumber1 int64, groupNumber2 int64, result1, result2 uint64, isEmpty1, isEmpty2 bool, _ any) (uint64, bool, error) {
	if isEmpty1 {
		result1 = uint64(0)
	}
	if isEmpty2 {
		result2 = uint64(0)
	}
	return result1 ^ result2, isEmpty1 && isEmpty2, nil
}
func (s *sAggBitXor[T]) Eval(lastResult []uint64) ([]uint64, error) {
	return lastResult, nil
}
func (s *sAggBitXor[T]) MarshalBinary() ([]byte, error) {
	return nil, nil
}
func (s *sAggBitXor[T]) UnmarshalBinary(_ []byte) error {
	return nil
}

func (s *sAggBinaryBitXor) Dup() agg.AggStruct {
	return &sAggBinaryBitXor{}
}
func (s *sAggBinaryBitXor) Grows(_ int)         {}
func (s *sAggBinaryBitXor) Free(_ *mpool.MPool) {}
func (s *sAggBinaryBitXor) Fill(groupNumber int64, value []byte, lastResult []byte, count int64, isEmpty bool, isNull bool) ([]byte, bool, error) {
	if !isNull {
		if isEmpty {
			result := make([]byte, len(value))
			copy(result, value)
			return result, false, nil
		}

		types.BitXor(lastResult, lastResult, value)
		return lastResult, false, nil
	}
	return lastResult, isEmpty, nil
}
func (s *sAggBinaryBitXor) Merge(groupNumber1 int64, groupNumber2 int64, result1, result2 []byte, isEmpty1, isEmpty2 bool, _ any) ([]byte, bool, error) {
	if isEmpty1 {
		return result2, isEmpty2, nil
	}
	if isEmpty2 {
		return result1, isEmpty1, nil
	}
	types.BitXor(result1, result1, result2)
	return result1, false, nil
}
func (s *sAggBinaryBitXor) Eval(lastResult [][]byte) ([][]byte, error) {
	return lastResult, nil
}
func (s *sAggBinaryBitXor) MarshalBinary() ([]byte, error) { return nil, nil }
func (s *sAggBinaryBitXor) UnmarshalBinary(_ []byte) error { return nil }
