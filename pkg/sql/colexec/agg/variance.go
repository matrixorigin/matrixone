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
	"reflect"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

type Variance[T1 types.Floats | types.Ints | types.UInts] struct {
	sum   []float64
	count []float64
}

// VD64 Variance for decimal64
type VD64 struct {
	sum    []types.Decimal128
	counts []int64
}

// VD128 Variance for decimal128
type VD128 struct {
	sum    []types.Decimal128
	counts []int64
}

func VarianceReturnType(typs []types.Type) types.Type {
	switch typs[0].Oid {
	case types.T_decimal64, types.T_decimal128:
		return types.New(types.T_decimal128, 0, typs[0].Scale, typs[0].Precision)
	default:
		return types.New(types.T_float64, 0, 0, 0)
	}
}

func String(b []byte) (s string) {
	pbytes := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	pstring := (*reflect.StringHeader)(unsafe.Pointer(&s))
	pstring.Data = pbytes.Data
	pstring.Len = pbytes.Len
	return
}

// NewVariance is used to create a Variance which supports float,int,uint
func NewVariance[T1 types.Floats | types.Ints | types.UInts]() *Variance[T1] {
	return &Variance[T1]{}
}

func (variance *Variance[T1]) Grows(count int) {
	if len(variance.sum) == 0 {
		variance.sum = make([]float64, count)
		variance.count = make([]float64, count)
	} else {
		for i := 0; i < count; i++ {
			variance.sum = append(variance.sum, 0)
			variance.count = append(variance.count, 0)
		}
	}
}

func (variance *Variance[T1]) Eval(vs []float64) []float64 {
	for i, v := range vs {
		avg := (variance.sum[i]) / (variance.count[i])
		vs[i] = (v)/(variance.count[i]) - math.Pow(avg, 2)
	}
	return vs
}

func (variance *Variance[T1]) Merge(groupIndex1, groupIndex2 int64, x, y float64, IsEmpty1 bool, IsEmpty2 bool, agg any) (float64, bool) {
	variance2 := agg.(*Variance[T1])
	if IsEmpty1 && !IsEmpty2 {
		variance.sum[groupIndex1] = variance2.sum[groupIndex2]
		variance.count[groupIndex1] = variance2.count[groupIndex2]
		return y, false
	} else if IsEmpty2 && !IsEmpty1 {
		return x, false
	} else if IsEmpty1 && IsEmpty2 {
		return x, true
	} else {
		variance.count[groupIndex1] += variance2.count[groupIndex2]
		variance.sum[groupIndex1] += variance2.sum[groupIndex2]
		return x + y, false
	}
}

func (variance *Variance[T1]) Fill(groupIndex int64, v1 T1, v2 float64, z int64, IsEmpty bool, hasNull bool) (float64, bool) {
	if hasNull {
		return v2, IsEmpty
	} else if IsEmpty {
		f1 := float64(v1)
		variance.sum[groupIndex] = f1 * float64(z)
		variance.count[groupIndex] += float64(z)
		return math.Pow(f1, 2) * float64(z), false
	}
	f1 := float64(v1)
	f2 := v2
	variance.sum[groupIndex] += f1 * float64(z)
	variance.count[groupIndex] += float64(z)
	return f2 + math.Pow(f1, 2)*float64(z), false
}

func NewVD64() *VD64 {
	return &VD64{}
}

func (v *VD64) Grows(cnt int) {
	d, _ := types.Decimal128_FromInt64(0, 64, 0)
	for i := 0; i < cnt; i++ {
		v.sum = append(v.sum, d)
		v.counts = append(v.counts, 0)
	}
}

func (v *VD64) Eval(vs []types.Decimal128) []types.Decimal128 {
	for i, k := range vs {
		a := types.Decimal128Int64Div(v.sum[i], v.counts[i])
		a2 := types.Decimal128Decimal128Mul(a, a)
		d := types.Decimal128Int64Div(k, v.counts[i])
		vs[i] = d.Sub(a2)
	}
	return vs
}

func (v *VD64) Merge(xIndex, yIndex int64, x types.Decimal128, y types.Decimal128, xEmpty bool, yEmpty bool, agg any) (types.Decimal128, bool) {
	if !yEmpty {
		vd := agg.(*VD64)
		v.counts[xIndex] += vd.counts[yIndex]
		v.sum[xIndex] = v.sum[xIndex].Add(vd.sum[yIndex])
		if !xEmpty {
			return x.Add(y), false
		}
		return y, false
	}
	return x, xEmpty
}

func (v *VD64) Fill(i int64, v1 types.Decimal64, v2 types.Decimal128, z int64, isEmpty bool, isNull bool) (types.Decimal128, bool) {
	if isNull {
		return v2, isEmpty
	}
	x := types.Decimal128_FromDecimal64(v1)
	v.counts[i] += z
	if isEmpty {
		v.sum[i] = types.Decimal128Int64Mul(x, z)
		return types.Decimal128Int64Mul(types.Decimal128Decimal128Mul(x, x), z), false
	}
	v.sum[i] = v.sum[i].Add(types.Decimal128Int64Mul(x, z))
	return v2.Add(types.Decimal128Int64Mul(types.Decimal128Decimal128Mul(x, x), z)), false
}

func NewVD128() *VD128 {
	return &VD128{}
}

func (v *VD128) Grows(cnt int) {
	d, _ := types.Decimal128_FromInt64(0, 64, 0)
	for i := 0; i < cnt; i++ {
		v.sum = append(v.sum, d)
		v.counts = append(v.counts, 0)
	}
}

func (v *VD128) Eval(vs []types.Decimal128) []types.Decimal128 {
	for i, k := range vs {
		a := types.Decimal128Int64Div(v.sum[i], v.counts[i])
		a2 := types.Decimal128Decimal128Mul(a, a)
		d := types.Decimal128Int64Div(k, v.counts[i])
		vs[i] = d.Sub(a2)
	}
	return vs
}

func (v *VD128) Merge(xIndex, yIndex int64, x types.Decimal128, y types.Decimal128, xEmpty bool, yEmpty bool, agg any) (types.Decimal128, bool) {
	if !yEmpty {
		vd := agg.(*VD128)
		v.counts[xIndex] += vd.counts[yIndex]
		v.sum[xIndex] = v.sum[xIndex].Add(vd.sum[yIndex])
		if !xEmpty {
			return x.Add(y), false
		}
		return y, false
	}
	return x, xEmpty
}

func (v *VD128) Fill(i int64, v1 types.Decimal128, v2 types.Decimal128, z int64, isEmpty bool, isNull bool) (types.Decimal128, bool) {
	if isNull {
		return v2, isEmpty
	}
	v.counts[i] += z
	if isEmpty {
		v.sum[i] = types.Decimal128Int64Mul(v1, z)
		return types.Decimal128Int64Mul(types.Decimal128Decimal128Mul(v1, v1), z), false
	}
	v.sum[i] = v.sum[i].Add(types.Decimal128Int64Mul(v1, z))
	return v2.Add(types.Decimal128Int64Mul(types.Decimal128Decimal128Mul(v1, v1), z)), false
}
