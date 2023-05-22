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
	"math"

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

type Variance[T1 types.Floats | types.Ints | types.UInts] struct {
	Sum    []float64
	Counts []float64
}

type EncodeVariance struct {
	Sum    []float64
	Counts []float64
}

// VD64 Variance for decimal64
type VD64 struct {
	Sum         []types.Decimal128
	Counts      []int64
	Typ         types.Type
	ScaleMul    int32
	ScaleDiv    int32
	ScaleMulDiv int32
	ScaleDivMul int32
}

// VD128 Variance for decimal128
type VD128 struct {
	Sum         []types.Decimal128
	Counts      []int64
	Typ         types.Type
	ScaleMul    int32
	ScaleDiv    int32
	ScaleMulDiv int32
	ScaleDivMul int32
}

type EncodeDecimalV struct {
	Sum    []types.Decimal128
	Counts []int64
}

var VarianceSupported = []types.T{
	types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
	types.T_int8, types.T_int16, types.T_int32, types.T_int64,
	types.T_float32, types.T_float64,
	types.T_decimal64, types.T_decimal128,
}

func VarianceReturnType(typs []types.Type) types.Type {
	switch typs[0].Oid {
	case types.T_decimal64, types.T_decimal128:
		s := int32(12)
		if typs[0].Scale > s {
			s = typs[0].Scale
		}
		return types.New(types.T_decimal128, 38, s)
	default:
		return types.New(types.T_float64, 0, 0)
	}
}

// NewVariance is used to create a Variance which supports float,int,uint
func NewVariance[T1 types.Floats | types.Ints | types.UInts]() *Variance[T1] {
	return &Variance[T1]{}
}

func (variance *Variance[T1]) Grows(count int) {
	if len(variance.Sum) == 0 {
		variance.Sum = make([]float64, count)
		variance.Counts = make([]float64, count)
	} else {
		for i := 0; i < count; i++ {
			variance.Sum = append(variance.Sum, 0)
			variance.Counts = append(variance.Counts, 0)
		}
	}
}

func (variance *Variance[T1]) Eval(vs []float64) []float64 {
	for i, v := range vs {
		avg := (variance.Sum[i]) / (variance.Counts[i])
		vs[i] = (v)/(variance.Counts[i]) - math.Pow(avg, 2)
	}
	return vs
}

func (variance *Variance[T1]) Merge(groupIndex1, groupIndex2 int64, x, y float64, IsEmpty1 bool, IsEmpty2 bool, agg any) (float64, bool) {
	variance2 := agg.(*Variance[T1])
	if IsEmpty1 && !IsEmpty2 {
		variance.Sum[groupIndex1] = variance2.Sum[groupIndex2]
		variance.Counts[groupIndex1] = variance2.Counts[groupIndex2]
		return y, false
	} else if IsEmpty2 && !IsEmpty1 {
		return x, false
	} else if IsEmpty1 && IsEmpty2 {
		return x, true
	} else {
		variance.Counts[groupIndex1] += variance2.Counts[groupIndex2]
		variance.Sum[groupIndex1] += variance2.Sum[groupIndex2]
		return x + y, false
	}
}

func (variance *Variance[T1]) Fill(groupIndex int64, v1 T1, v2 float64, z int64, IsEmpty bool, hasNull bool) (float64, bool) {
	if hasNull {
		return v2, IsEmpty
	} else if IsEmpty {
		f1 := float64(v1)
		variance.Sum[groupIndex] = f1 * float64(z)
		variance.Counts[groupIndex] += float64(z)
		return math.Pow(f1, 2) * float64(z), false
	}
	f1 := float64(v1)
	f2 := v2
	variance.Sum[groupIndex] += f1 * float64(z)
	variance.Counts[groupIndex] += float64(z)
	return f2 + math.Pow(f1, 2)*float64(z), false
}

func (variance *Variance[T1]) MarshalBinary() ([]byte, error) {
	ev := EncodeVariance{
		Sum:    variance.Sum,
		Counts: variance.Counts,
	}
	return ev.Marshal()
}

func (variance *Variance[T1]) UnmarshalBinary(data []byte) error {
	var ev EncodeVariance
	if err := ev.Unmarshal(data); err != nil {
		return err
	}
	variance.Sum = ev.Sum
	variance.Counts = ev.Counts
	return nil
}

func NewVD64(typ types.Type) *VD64 {
	scalemul := int32(12)
	scalediv := int32(12)
	scalemuldiv := int32(12)
	scaledivmul := int32(12)
	if typ.Scale > 12 {
		scalemul = typ.Scale
		scalediv = typ.Scale
		scalemuldiv = typ.Scale
		scaledivmul = typ.Scale
	}
	if typ.Scale < 6 {
		scalemul = typ.Scale * 2
		scalediv = typ.Scale + 6
		if typ.Scale < 3 {
			scalemuldiv = typ.Scale*2 + 6
		}
	}
	return &VD64{Typ: typ, ScaleMul: scalemul, ScaleDiv: scalediv, ScaleMulDiv: scalemuldiv, ScaleDivMul: scaledivmul}
}

func (v *VD64) Grows(cnt int) {
	d := types.Decimal128{B0_63: 0, B64_127: 0}
	for i := 0; i < cnt; i++ {
		v.Sum = append(v.Sum, d)
		v.Counts = append(v.Counts, 0)
	}
}

func (v *VD64) Eval(vs []types.Decimal128) []types.Decimal128 {
	for i, k := range vs {
		if v.Counts[i] == 1 {
			vs[i] = types.Decimal128{B0_63: 0, B64_127: 0}
			continue
		}
		a, _, _ := v.Sum[i].Div(types.Decimal128{B0_63: uint64(v.Counts[i]), B64_127: 0}, v.Typ.Scale, 0)
		a2, _, _ := a.Mul(a, v.ScaleDiv, v.ScaleDiv)
		d, _, _ := k.Div(types.Decimal128{B0_63: uint64(v.Counts[i]), B64_127: 0}, v.ScaleMul, 0)
		vs[i], _, _ = d.Sub(a2, v.ScaleMulDiv, v.ScaleDivMul)
	}
	return vs
}

func (v *VD64) Merge(xIndex, yIndex int64, x types.Decimal128, y types.Decimal128, xEmpty bool, yEmpty bool, agg any) (types.Decimal128, bool) {
	if !yEmpty {
		vd := agg.(*VD64)
		v.Counts[xIndex] += vd.Counts[yIndex]
		v.Sum[xIndex], _ = v.Sum[xIndex].Add128(vd.Sum[yIndex])
		if !xEmpty {
			x, _ = x.Add128(y)
			return x, false
		}
		return y, false
	}
	return x, xEmpty
}

func (v *VD64) Fill(i int64, v1 types.Decimal64, v2 types.Decimal128, z int64, isEmpty bool, isNull bool) (types.Decimal128, bool) {
	if isNull {
		return v2, isEmpty
	}
	x := types.Decimal128{B0_63: uint64(v1), B64_127: 0}
	if v1>>63 != 0 {
		x.B64_127 = ^x.B64_127
	}
	v.Counts[i] += z
	if isEmpty {
		v.Sum[i], _, _ = x.Mul(types.Decimal128{B0_63: uint64(z), B64_127: 0}, v.Typ.Scale, 0)
		x, _, _ = v.Sum[i].Mul(x, v.Typ.Scale, v.Typ.Scale)
		return x, false
	}
	y, _, _ := x.Mul(types.Decimal128{B0_63: uint64(z), B64_127: 0}, v.Typ.Scale, 0)
	v.Sum[i], _ = v.Sum[i].Add128(y)
	y, _, _ = y.Mul(x, v.Typ.Scale, v.Typ.Scale)
	v2, _ = v2.Add128(y)
	return v2, false
}

func (v *VD64) MarshalBinary() ([]byte, error) {
	ed := &EncodeDecimalV{
		Sum:    v.Sum,
		Counts: v.Counts,
	}
	return ed.Marshal()
}

func (v *VD64) UnmarshalBinary(data []byte) error {
	var ed EncodeDecimalV
	if err := ed.Unmarshal(data); err != nil {
		return err
	}
	v.Sum = ed.Sum
	v.Counts = ed.Counts
	return nil
}

func NewVD128(typ types.Type) *VD128 {
	scalemul := int32(12)
	scalediv := int32(12)
	scalemuldiv := int32(12)
	scaledivmul := int32(12)
	if typ.Scale > 12 {
		scalemul = typ.Scale
		scalediv = typ.Scale
		scalemuldiv = typ.Scale
		scaledivmul = typ.Scale
	}
	if typ.Scale < 6 {
		scalemul = typ.Scale * 2
		scalediv = typ.Scale + 6
		if typ.Scale < 3 {
			scalemuldiv = typ.Scale*2 + 6
		}
	}
	return &VD128{Typ: typ, ScaleMul: scalemul, ScaleDiv: scalediv, ScaleMulDiv: scalemuldiv, ScaleDivMul: scaledivmul}
}

func (v *VD128) Grows(cnt int) {
	d := types.Decimal128{B0_63: 0, B64_127: 0}
	for i := 0; i < cnt; i++ {
		v.Sum = append(v.Sum, d)
		v.Counts = append(v.Counts, 0)
	}
}

func (v *VD128) Eval(vs []types.Decimal128) []types.Decimal128 {
	for i, k := range vs {
		if v.Counts[i] == 1 {
			vs[i] = types.Decimal128{B0_63: 0, B64_127: 0}
			continue
		}
		a, _, _ := v.Sum[i].Div(types.Decimal128{B0_63: uint64(v.Counts[i]), B64_127: 0}, v.Typ.Scale, 0)
		a2, _, _ := a.Mul(a, v.ScaleDiv, v.ScaleDiv)
		d, _, _ := k.Div(types.Decimal128{B0_63: uint64(v.Counts[i]), B64_127: 0}, v.ScaleMul, 0)
		vs[i], _, _ = d.Sub(a2, v.ScaleMulDiv, v.ScaleDivMul)
	}
	return vs
}

func (v *VD128) Merge(xIndex, yIndex int64, x types.Decimal128, y types.Decimal128, xEmpty bool, yEmpty bool, agg any) (types.Decimal128, bool) {
	if !yEmpty {
		vd := agg.(*VD128)
		v.Counts[xIndex] += vd.Counts[yIndex]
		v.Sum[xIndex], _ = v.Sum[xIndex].Add128(vd.Sum[yIndex])
		if !xEmpty {
			x, _ = x.Add128(y)
			return x, false
		}
		return y, false
	}
	return x, xEmpty
}

func (v *VD128) Fill(i int64, v1 types.Decimal128, v2 types.Decimal128, z int64, isEmpty bool, isNull bool) (types.Decimal128, bool) {
	if isNull {
		return v2, isEmpty
	}
	v.Counts[i] += z
	if isEmpty {
		v.Sum[i], _, _ = v1.Mul(types.Decimal128{B0_63: uint64(z), B64_127: 0}, v.Typ.Scale, 0)
		v1, _, _ = v.Sum[i].Mul(v1, v.Typ.Scale, v.Typ.Scale)
		return v1, false
	}
	y, _, _ := v1.Mul(types.Decimal128{B0_63: uint64(z), B64_127: 0}, v.Typ.Scale, 0)
	v.Sum[i], _ = v.Sum[i].Add128(y)
	y, _, _ = y.Mul(v1, v.Typ.Scale, v.Typ.Scale)
	v2, _ = v2.Add128(y)
	return v2, false
}

func (v *VD128) MarshalBinary() ([]byte, error) {
	ed := &EncodeDecimalV{
		Sum:    v.Sum,
		Counts: v.Counts,
	}
	return ed.Marshal()
}

func (v *VD128) UnmarshalBinary(data []byte) error {
	var ed EncodeDecimalV
	if err := ed.Unmarshal(data); err != nil {
		return err
	}
	v.Sum = ed.Sum
	v.Counts = ed.Counts
	return nil
}
