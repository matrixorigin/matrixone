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
	// bit_or() supported input type and output type.
	AggBitOrSupportedParameters = []types.T{
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_float32, types.T_float64,
		types.T_binary, types.T_varbinary,
	}
	AggBitOrReturnType = AggBitAndReturnType
)

func NewAggBitOr(overloadID int64, dist bool, inputTypes []types.Type, outputType types.Type, _ any) (agg.Agg[any], error) {
	switch inputTypes[0].Oid {
	case types.T_uint8:
		return newGenericBitOr[uint8](overloadID, inputTypes[0], outputType, dist)
	case types.T_uint16:
		return newGenericBitOr[uint16](overloadID, inputTypes[0], outputType, dist)
	case types.T_uint32:
		return newGenericBitOr[uint32](overloadID, inputTypes[0], outputType, dist)
	case types.T_uint64:
		return newGenericBitOr[uint64](overloadID, inputTypes[0], outputType, dist)
	case types.T_int8:
		return newGenericBitOr[int8](overloadID, inputTypes[0], outputType, dist)
	case types.T_int16:
		return newGenericBitOr[int16](overloadID, inputTypes[0], outputType, dist)
	case types.T_int32:
		return newGenericBitOr[int32](overloadID, inputTypes[0], outputType, dist)
	case types.T_int64:
		return newGenericBitOr[int64](overloadID, inputTypes[0], outputType, dist)
	case types.T_float32:
		return newGenericBitOr[float32](overloadID, inputTypes[0], outputType, dist)
	case types.T_float64:
		return newGenericBitOr[float64](overloadID, inputTypes[0], outputType, dist)
	case types.T_binary, types.T_varbinary:
		aggPriv := &sAggBinaryBitOr{}
		if dist {
			return agg.NewUnaryDistAgg(overloadID, aggPriv, false, inputTypes[0], outputType, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill), nil
		}
		return agg.NewUnaryAgg(overloadID, aggPriv, false, inputTypes[0], outputType, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill), nil
	}
	return nil, moerr.NewInternalErrorNoCtx("unsupported type '%s' for bit_or", inputTypes[0])
}

func newGenericBitOr[T numeric](overloadID int64, inputType types.Type, outputType types.Type, dist bool) (agg.Agg[any], error) {
	aggPriv := &sAggBitOr[T]{}
	if dist {
		return agg.NewUnaryDistAgg[T, uint64](overloadID, aggPriv, false, inputType, outputType, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill), nil
	}
	return agg.NewUnaryAgg[T, uint64](overloadID, aggPriv, false, inputType, outputType, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill), nil
}

type sAggBitOr[T numeric] struct{}
type sAggBinaryBitOr struct{}

func (s *sAggBitOr[T]) Dup() agg.AggStruct {
	return &sAggBitOr[T]{}
}
func (s *sAggBitOr[T]) Grows(_ int)         {}
func (s *sAggBitOr[T]) Free(_ *mpool.MPool) {}
func (s *sAggBitOr[T]) Fill(groupNumber int64, value T, lastResult uint64, count int64, isEmpty bool, isNull bool) (uint64, bool, error) {
	if !isNull {
		if isEmpty {
			lastResult = 0
		}

		vv := float64(value)
		if vv > math.MaxUint64 {
			return math.MaxInt64, false, nil
		}
		if vv < 0 {
			return uint64(int64(value)) | lastResult, false, nil
		}
		return uint64(value) | lastResult, false, nil
	}
	return lastResult, isEmpty, nil
}
func (s *sAggBitOr[T]) Merge(groupNumber1 int64, groupNumber2 int64, result1, result2 uint64, isEmpty1, isEmpty2 bool, _ any) (uint64, bool, error) {
	if isEmpty1 {
		result1 = uint64(0)
	}
	if isEmpty2 {
		result2 = uint64(0)
	}
	return result1 | result2, isEmpty1 && isEmpty2, nil
}
func (s *sAggBitOr[T]) Eval(lastResult []uint64) ([]uint64, error) {
	return lastResult, nil
}
func (s *sAggBitOr[T]) MarshalBinary() ([]byte, error) {
	return nil, nil
}
func (s *sAggBitOr[T]) UnmarshalBinary(_ []byte) error {
	return nil
}

func (s *sAggBinaryBitOr) Dup() agg.AggStruct {
	return &sAggBinaryBitOr{}
}
func (s *sAggBinaryBitOr) Grows(_ int)         {}
func (s *sAggBinaryBitOr) Free(_ *mpool.MPool) {}
func (s *sAggBinaryBitOr) Fill(groupNumber int64, value []byte, lastResult []byte, count int64, isEmpty bool, isNull bool) ([]byte, bool, error) {
	if !isNull {
		if isEmpty {
			result := make([]byte, len(value))
			copy(result, value)
			return result, false, nil
		}

		types.BitOr(lastResult, lastResult, value)
		return lastResult, false, nil
	}
	return lastResult, isEmpty, nil
}
func (s *sAggBinaryBitOr) Merge(groupNumber1 int64, groupNumber2 int64, result1, result2 []byte, isEmpty1, isEmpty2 bool, _ any) ([]byte, bool, error) {
	if isEmpty1 {
		return result2, isEmpty2, nil
	}
	if isEmpty2 {
		return result1, isEmpty1, nil
	}
	types.BitOr(result1, result1, result2)
	return result1, false, nil
}
func (s *sAggBinaryBitOr) Eval(lastResult [][]byte) ([][]byte, error) {
	return lastResult, nil
}
func (s *sAggBinaryBitOr) MarshalBinary() ([]byte, error) { return nil, nil }
func (s *sAggBinaryBitOr) UnmarshalBinary(_ []byte) error { return nil }
