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
	"bytes"

	hll "github.com/axiomhq/hyperloglog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/agg"
)

var (
	// approx_count() supported input type and output type.
	AggApproxCountReturnType = func(typs []types.Type) types.Type {
		return types.T_uint64.ToType()
	}
)

func NewAggApproxCount(overloadID int64, dist bool, inputTypes []types.Type, outputType types.Type, _ any) (agg.Agg[any], error) {
	switch inputTypes[0].Oid {
	case types.T_bool:
		return newGenericApprox[bool](overloadID, inputTypes[0], outputType, dist)
	case types.T_uint8:
		return newGenericApprox[uint8](overloadID, inputTypes[0], outputType, dist)
	case types.T_uint16:
		return newGenericApprox[uint16](overloadID, inputTypes[0], outputType, dist)
	case types.T_uint32:
		return newGenericApprox[uint32](overloadID, inputTypes[0], outputType, dist)
	case types.T_uint64:
		return newGenericApprox[uint64](overloadID, inputTypes[0], outputType, dist)
	case types.T_int8:
		return newGenericApprox[int8](overloadID, inputTypes[0], outputType, dist)
	case types.T_int16:
		return newGenericApprox[int16](overloadID, inputTypes[0], outputType, dist)
	case types.T_int32:
		return newGenericApprox[int32](overloadID, inputTypes[0], outputType, dist)
	case types.T_int64:
		return newGenericApprox[int64](overloadID, inputTypes[0], outputType, dist)
	case types.T_float32:
		return newGenericApprox[float32](overloadID, inputTypes[0], outputType, dist)
	case types.T_float64:
		return newGenericApprox[float64](overloadID, inputTypes[0], outputType, dist)
	case types.T_date:
		return newGenericApprox[types.Date](overloadID, inputTypes[0], outputType, dist)
	case types.T_datetime:
		return newGenericApprox[types.Datetime](overloadID, inputTypes[0], outputType, dist)
	case types.T_timestamp:
		return newGenericApprox[types.Timestamp](overloadID, inputTypes[0], outputType, dist)
	case types.T_time:
		return newGenericApprox[types.Time](overloadID, inputTypes[0], outputType, dist)
	case types.T_enum:
		return newGenericApprox[types.Enum](overloadID, inputTypes[0], outputType, dist)
	case types.T_decimal64:
		return newGenericApprox[types.Decimal64](overloadID, inputTypes[0], outputType, dist)
	case types.T_decimal128:
		return newGenericApprox[types.Decimal128](overloadID, inputTypes[0], outputType, dist)
	case types.T_uuid:
		return newGenericApprox[types.Uuid](overloadID, inputTypes[0], outputType, dist)
	case types.T_char, types.T_varchar, types.T_blob, types.T_json, types.T_text, types.T_binary, types.T_varbinary:
		return newGenericApprox[[]byte](overloadID, inputTypes[0], outputType, dist)
	}
	return nil, moerr.NewInternalErrorNoCtx("unsupported type '%s' for approx_count", inputTypes[0])
}

func newGenericApprox[T allTypes](overloadID int64, inputType types.Type, outputType types.Type, dist bool) (agg.Agg[any], error) {
	aggPriv := &sAggApproxCountDistinct[T]{}
	// TODO: bad agg. what is the difference between dist and non-dist?
	//if dist {
	//	return agg.NewUnaryDistAgg[T, uint64](overloadID, aggPriv, false, inputType, outputType, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill), nil
	//}
	return agg.NewUnaryAgg[T, uint64](overloadID, aggPriv, false, inputType, outputType, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill), nil
}

type sAggApproxCountDistinct[T any] struct {
	sk []*hll.Sketch
}

func (s *sAggApproxCountDistinct[T]) Dup() agg.AggStruct {
	val := &sAggApproxCountDistinct[T]{
		sk: make([]*hll.Sketch, len(s.sk)),
	}
	for i, sk := range s.sk {
		val.sk[i] = sk.Clone()
	}
	return val
}
func (s *sAggApproxCountDistinct[T]) Grows(cnt int) {
	oldLength := len(s.sk)
	if len(s.sk) < cnt {
		s.sk = append(s.sk, make([]*hll.Sketch, cnt)...)
	}
	for i := oldLength; i < cnt; i++ {
		s.sk[i] = hll.New()
	}
}
func (s *sAggApproxCountDistinct[T]) Free(_ *mpool.MPool) {}
func (s *sAggApproxCountDistinct[T]) Fill(groupNumber int64, values T, lastResult uint64, count int64, isEmpty bool, isNull bool) (newResult uint64, isStillEmpty bool, err error) {
	if !isNull {
		data := getTheBytes(values)
		s.sk[groupNumber].Insert(data)
		return lastResult, false, nil
	}
	return lastResult, isEmpty, nil
}
func (s *sAggApproxCountDistinct[T]) Merge(groupNumber1 int64, groupNumber2 int64, result1 uint64, result2 uint64, isEmpty1 bool, isEmpty2 bool, priv2 any) (newResult uint64, isStillEmpty bool, err error) {
	if !isEmpty2 {
		s2 := priv2.(*sAggApproxCountDistinct[T])
		if !isEmpty1 {
			err = s.sk[groupNumber1].Merge(s2.sk[groupNumber2])
		} else {
			s.sk[groupNumber1] = s2.sk[groupNumber2]
			s2.sk[groupNumber2] = nil
		}
	}
	return result1, isEmpty1 && isEmpty2, err
}
func (s *sAggApproxCountDistinct[T]) Eval(lastResult []uint64) ([]uint64, error) {
	for i := range lastResult {
		lastResult[i] = s.sk[i].Estimate()
	}
	return lastResult, nil
}
func (s *sAggApproxCountDistinct[T]) MarshalBinary() ([]byte, error) {
	if len(s.sk) == 0 {
		return nil, nil
	}
	var buf bytes.Buffer

	l := int32(len(s.sk))
	buf.Write(types.EncodeInt32(&l))
	for i := 0; i < int(l); i++ {
		data, err := s.sk[i].MarshalBinary()
		if err != nil {
			return nil, err
		}
		size := int32(len(data))
		buf.Write(types.EncodeInt32(&size))
		buf.Write(data)
	}

	return buf.Bytes(), nil
}
func (s *sAggApproxCountDistinct[T]) UnmarshalBinary(data []byte) error {
	if len(data) == 0 {
		return nil
	}

	l := types.DecodeInt32(data[:4])
	data = data[4:]
	sks := make([]*hll.Sketch, l)
	for i := 0; i < int(l); i++ {
		size := types.DecodeInt32(data[:4])
		data = data[4:]

		sk := new(hll.Sketch)
		if err := sk.UnmarshalBinary(data[:size]); err != nil {
			return err
		}
		data = data[size:]
		sks[i] = sk
	}
	s.sk = sks
	return nil
}

func getTheBytes(value any) []byte {
	var data []byte
	switch v := value.(type) {
	case uint8:
		data = append(data, types.EncodeFixed(v)...)
	case uint16:
		data = append(data, types.EncodeFixed(v)...)
	case uint32:
		data = append(data, types.EncodeFixed(v)...)
	case uint64:
		data = append(data, types.EncodeFixed(v)...)
	case int8:
		data = append(data, types.EncodeFixed(v)...)
	case int16:
		data = append(data, types.EncodeFixed(v)...)
	case int32:
		data = append(data, types.EncodeFixed(v)...)
	case int64:
		data = append(data, types.EncodeFixed(v)...)
	case float32:
		data = append(data, types.EncodeFixed(v)...)
	case float64:
		data = append(data, types.EncodeFixed(v)...)
	case []byte:
		data = append(data, v...)
	case types.Decimal64:
		data = append(data, types.EncodeFixed(v)...)
	case types.Decimal128:
		data = append(data, types.EncodeFixed(v)...)
	default:
		panic("not support for type")
	}
	return data
}
