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
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vectorize/sum"
)

type Numeric interface {
	types.Ints | types.UInts | types.Floats
}

type Avg[T Numeric] struct {
	Cnts []int64
}

type Decimal64Avg struct {
	Cnts []int64
}

type Decimal128Avg struct {
	Cnts []int64
}

func AvgReturnType(typs []types.Type) types.Type {
	switch typs[0].Oid {
	case types.T_decimal64:
		return types.New(types.T_decimal128, 0, typs[0].Scale)
	case types.T_decimal128:
		return types.New(types.T_decimal128, 0, typs[0].Scale)
	case types.T_float32, types.T_float64:
		return types.New(types.T_float64, 0, 0)
	case types.T_int8, types.T_int16, types.T_int32, types.T_int64:
		return types.New(types.T_float64, 0, 0)
	case types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64:
		return types.New(types.T_float64, 0, 0)
	default:
		return types.Type{}
	}
}

func NewAvg[T Numeric]() *Avg[T] {
	return &Avg[T]{}
}

func (a *Avg[T]) Grows(cnt int) {
	for i := 0; i < cnt; i++ {
		a.Cnts = append(a.Cnts, 0)
	}
}

func (a *Avg[T]) Eval(vs []float64) []float64 {
	for i := range vs {
		if a.Cnts[i] == 0 {
			continue
		}
		vs[i] = vs[i] / float64(a.Cnts[i])
	}
	return vs
}

func (a *Avg[T]) Fill(i int64, value T, ov float64, z int64, isEmpty bool, isNull bool) (float64, bool) {
	if !isNull {
		a.Cnts[i] += z
		return ov + float64(value)*float64(z), false
	}
	return ov, isEmpty
}

func (a *Avg[T]) Merge(xIndex int64, yIndex int64, x float64, y float64, xEmpty bool, yEmpty bool, yAvg any) (float64, bool) {
	if !yEmpty {
		ya := yAvg.(*Avg[T])
		a.Cnts[xIndex] += ya.Cnts[yIndex]
		if !xEmpty {
			return x + y, false
		}
		return y, false
	}

	return x, xEmpty
}

func (a *Avg[T]) MarshalBinary() ([]byte, error) {
	return types.EncodeSlice(a.Cnts), nil
}

func (a *Avg[T]) UnmarshalBinary(data []byte) error {
	// avoid resulting errors caused by morpc overusing memory
	copyData := make([]byte, len(data))
	copy(copyData, data)
	a.Cnts = types.DecodeSlice[int64](copyData)
	return nil
}

func NewD64Avg() *Decimal64Avg {
	return &Decimal64Avg{}
}

func (a *Decimal64Avg) Grows(cnt int) {
	for i := 0; i < cnt; i++ {
		a.Cnts = append(a.Cnts, 0)
	}
}

func (a *Decimal64Avg) Eval(vs []types.Decimal128) []types.Decimal128 {
	for i := range vs {
		if a.Cnts[i] == 0 {
			continue
		}
		vs[i] = vs[i].DivInt64(a.Cnts[i])
	}
	return vs
}

func (a *Decimal64Avg) Fill(i int64, value types.Decimal64, ov types.Decimal128, z int64, isEmpty bool, isNull bool) (types.Decimal128, bool) {
	if !isNull {
		a.Cnts[i] += z
		tmp64 := value.MulInt64(z)
		return ov.Add(types.Decimal128_FromDecimal64(tmp64)), false
	}
	return ov, isEmpty
}

func (a *Decimal64Avg) Merge(xIndex int64, yIndex int64, x types.Decimal128, y types.Decimal128, xEmpty bool, yEmpty bool, yAvg any) (types.Decimal128, bool) {
	if !yEmpty {
		ya := yAvg.(*Decimal64Avg)
		a.Cnts[xIndex] += ya.Cnts[yIndex]
		if !xEmpty {
			return x.Add(y), false
		}
		return y, false
	}

	return x, xEmpty
}

func (a *Decimal64Avg) BatchFill(rs, vs any, start, count int64, vps []uint64, zs []int64, nsp *nulls.Nulls) error {
	if err := sum.Decimal64Sum128(rs.([]types.Decimal128), vs.([]types.Decimal64), start, count, vps, zs, nsp); err != nil {
		return err
	}
	for i := int64(0); i < count; i++ {
		if nsp.Contains(uint64(i + start)) {
			continue
		}
		if vps[i] == 0 {
			continue
		}
		j := vps[i] - 1
		a.Cnts[j] += zs[i+start]
	}
	return nil
}

func (a *Decimal64Avg) MarshalBinary() ([]byte, error) {
	return types.EncodeSlice(a.Cnts), nil
}

func (a *Decimal64Avg) UnmarshalBinary(data []byte) error {
	// avoid resulting errors caused by morpc overusing memory
	copyData := make([]byte, len(data))
	copy(copyData, data)
	a.Cnts = types.DecodeSlice[int64](copyData)
	return nil
}

func NewD128Avg() *Decimal128Avg {
	return &Decimal128Avg{}
}

func (a *Decimal128Avg) Grows(cnt int) {
	for i := 0; i < cnt; i++ {
		a.Cnts = append(a.Cnts, 0)
	}
}

func (a *Decimal128Avg) Eval(vs []types.Decimal128) []types.Decimal128 {
	for i := range vs {
		if a.Cnts[i] == 0 {
			continue
		}
		vs[i] = vs[i].DivInt64(a.Cnts[i])
	}
	return vs
}

func (a *Decimal128Avg) Fill(i int64, value types.Decimal128, ov types.Decimal128, z int64, isEmpty bool, isNull bool) (types.Decimal128, bool) {
	if !isNull {
		a.Cnts[i] += z
		return ov.Add(value.MulInt64(z)), false
	}
	return ov, isEmpty
}

func (a *Decimal128Avg) Merge(xIndex int64, yIndex int64, x types.Decimal128, y types.Decimal128, xEmpty bool, yEmpty bool, yAvg any) (types.Decimal128, bool) {
	if !yEmpty {
		ya := yAvg.(*Decimal128Avg)
		a.Cnts[xIndex] += ya.Cnts[yIndex]
		if !xEmpty {
			return x.Add(y), false
		}
		return y, false
	}

	return x, xEmpty
}

func (a *Decimal128Avg) BatchFill(rs, vs any, start, count int64, vps []uint64, zs []int64, nsp *nulls.Nulls) error {
	if err := sum.Decimal128Sum(rs.([]types.Decimal128), vs.([]types.Decimal128), start, count, vps, zs, nsp); err != nil {
		return err
	}
	for i := int64(0); i < count; i++ {
		if nsp.Contains(uint64(i + start)) {
			continue
		}
		if vps[i] == 0 {
			continue
		}
		j := vps[i] - 1
		a.Cnts[j] += zs[i+start]
	}
	return nil
}

func (a *Decimal128Avg) MarshalBinary() ([]byte, error) {
	return types.EncodeSlice(a.Cnts), nil
}

func (a *Decimal128Avg) UnmarshalBinary(data []byte) error {
	// avoid resulting errors caused by morpc overusing memory
	copyData := make([]byte, len(data))
	copy(copyData, data)
	a.Cnts = types.DecodeSlice[int64](copyData)
	return nil
}
