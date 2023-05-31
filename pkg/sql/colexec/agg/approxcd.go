// Copyright 2022 Matrix Origin
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
	"bytes"

	hll "github.com/axiomhq/hyperloglog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

type ApproxCountDistic[T any] struct {
	Sk []*hll.Sketch
}

func ApproxCountReturnType(_ []types.Type) types.Type {
	return types.New(types.T_uint64, 0, 0)
}

func NewApproxc[T any]() *ApproxCountDistic[T] {
	return &ApproxCountDistic[T]{}
}

func (a *ApproxCountDistic[T]) Grows(n int) {
	if len(a.Sk) == 0 {
		a.Sk = make([]*hll.Sketch, 0)
	}

	for i := 0; i < n; i++ {
		a.Sk = append(a.Sk, hll.New())
	}
}

func (a *ApproxCountDistic[T]) Eval(vs []uint64, err error) ([]uint64, error) {
	for i := range vs {
		vs[i] = a.Sk[i].Estimate()
	}

	return vs, nil
}

func (a *ApproxCountDistic[T]) Fill(n int64, v1 T, v2 uint64, _ int64, isEmpty bool, isNull bool) (uint64, bool, error) {
	if !isNull {
		data := getTheBytes(v1)
		a.Sk[n].Insert(data)
		isEmpty = false
	}
	return v2, isEmpty, nil
}

func (a *ApproxCountDistic[T]) Merge(xIndex int64, yIndex int64, x uint64, _ uint64, xEmpty bool, yEmpty bool, yApxc any) (uint64, bool, error) {
	ret := true
	if !yEmpty {
		ya := yApxc.(*ApproxCountDistic[T])
		if !xEmpty {
			if err := a.Sk[xIndex].Merge(ya.Sk[yIndex]); err != nil {
				panic(err)
			}
		} else {
			a.Sk[xIndex] = ya.Sk[yIndex].Clone()
		}
		ret = false
	}
	return x, ret, nil
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

func (a *ApproxCountDistic[T]) MarshalBinary() ([]byte, error) {
	// Sk []*hll.Sketch
	if len(a.Sk) == 0 {
		return nil, nil
	}

	var buf bytes.Buffer

	l := int32(len(a.Sk))
	buf.Write(types.EncodeInt32(&l))
	for i := 0; i < int(l); i++ {
		data, err := a.Sk[i].MarshalBinary()
		if err != nil {
			return nil, err
		}
		size := int32(len(data))
		buf.Write(types.EncodeInt32(&size))
		buf.Write(data)
	}

	return buf.Bytes(), nil
}

func (a *ApproxCountDistic[T]) UnmarshalBinary(data []byte) error {
	if len(data) == 0 {
		return nil
	}

	// avoid resulting errors caused by morpc overusing memory
	copyData := make([]byte, len(data))
	copy(copyData, data)

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
	a.Sk = sks
	return nil
}
