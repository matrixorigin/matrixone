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

package Variance

import (
	"math"
	"reflect"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

func ReturnType(_ []types.Type) types.Type {
	return types.New(types.T_float64, 0, 0, 0)
}

func String(b []byte) (s string) {
	pbytes := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	pstring := (*reflect.StringHeader)(unsafe.Pointer(&s))
	pstring.Data = pbytes.Data
	pstring.Len = pbytes.Len
	return
}

//New1 is used to Created a Variance which supports float,int,uint
func New1[T1 types.Floats | types.Ints | types.UInts]() *Variance[T1] {
	return &Variance[T1]{}
}

//New2 is used to Created a Variance which supports decimal64
//if is decimal,you need to give pricision
func New2() *Variance2 {
	return &Variance2{inputType: types.New(types.T_decimal64, 0, 0, 0)}
}

//New2 is used to Created a Variance which supports decimal64
//if is decimal,you need to give pricision
func New3() *Variance3 {
	return &Variance3{inputType: types.New(types.T_decimal64, 0, 0, 0)}
}

func (variance *Variance[T1]) Grows(count int) {
	if len(variance.sum) == 0 {
		variance.sum = make([]float64, count)
		variance.count = make([]float64, count)
	} else {
		for i := 0; i < count; i++ {
			var a float64
			variance.sum = append(variance.sum, a)
			variance.count = append(variance.count, 0)
		}
	}
}

func (variance *Variance[T1]) Eval(vs []float64) []float64 {
	for i, v := range vs {
		avg := (variance.sum[i]) / (variance.count[i])
		vs[i] = (v)/(variance.count[i]) - math.Pow(float64(avg), 2)
	}
	return vs
}

func (variance *Variance[T1]) Merge(groupIndex1, groupIndex2 int64, x, y float64, IsEmpty1 bool, IsEmpty2 bool, agg any) (float64, bool) {
	variance2 := agg.(*Variance[T1])
	if IsEmpty1 && !IsEmpty2 {
		variance.sum[groupIndex1] = variance2.sum[groupIndex2]
		variance.count[groupIndex1] = variance.count[groupIndex2]
		return y, false
	} else if IsEmpty2 && !IsEmpty1 {
		return x, false
	} else if IsEmpty1 && IsEmpty2 {
		return x, true
	} else {
		variance.count[groupIndex1] += variance2.count[groupIndex2]
		variance.sum[groupIndex1] += variance2.sum[groupIndex2]
		return (x + y), false
	}
}

func (variance *Variance[T1]) Fill(groupIndex int64, v1 T1, v2 float64, z int64, IsEmpty bool, hasNull bool) (float64, bool) {
	if hasNull {
		return v2, IsEmpty
	} else if IsEmpty {
		f1 := float64(v1)
		variance.sum[groupIndex] = any(f1 * float64(z)).(float64)
		variance.count[groupIndex] += float64(z)
		return math.Pow(f1, 2) * float64(z), false
	}
	f1 := float64(v1)
	f2 := float64(v2)
	variance.sum[groupIndex] = float64(variance.sum[groupIndex]) + f1*float64(z)
	variance.count[groupIndex] += float64(z)
	return f2 + math.Pow(f1, 2)*float64(z), false
}

func (variance *Variance2) Grows(count int) {
	if len(variance.sum) == 0 {
		variance.sum = make([]float64, count)
		variance.count = make([]float64, count)
	} else {
		for i := 0; i < count; i++ {
			var a float64
			variance.sum = append(variance.sum, a)
			variance.count = append(variance.count, 0)
		}
	}
}

func (variance *Variance2) Eval(vs []float64) []float64 {
	for i, v := range vs {
		avg := (variance.sum[i]) / (variance.count[i])
		vs[i] = (v)/(variance.count[i]) - math.Pow(float64(avg), 2)
	}
	return vs
}

func (variance *Variance2) Merge(groupIndex1, groupIndex2 int64, x, y float64, IsEmpty1 bool, IsEmpty2 bool, agg any) (float64, bool) {
	variance2 := agg.(*Variance2)
	if IsEmpty1 && !IsEmpty2 {
		variance.sum[groupIndex1] = variance2.sum[groupIndex2]
		variance.count[groupIndex1] = variance.count[groupIndex2]
		return y, false
	} else if IsEmpty2 && !IsEmpty1 {
		return x, false
	} else if IsEmpty1 && IsEmpty2 {
		return x, true
	} else {
		variance.count[groupIndex1] += variance2.count[groupIndex2]
		variance.sum[groupIndex1] += variance2.sum[groupIndex2]
		return (x + y), false
	}
}

func (variance *Variance2) Fill(groupIndex int64, v1 types.Decimal64, v2 float64, z int64, IsEmpty bool, hasNull bool) (float64, bool) {
	if hasNull {
		return v2, IsEmpty
	} else if IsEmpty {
		f1 := v1.ToFloat64()
		variance.sum[groupIndex] = f1 * float64(z)
		variance.count[groupIndex] += float64(z)
		return math.Pow(f1, 2) * float64(z), false
	}
	f1 := v1.ToFloat64()
	variance.sum[groupIndex] = float64(variance.sum[groupIndex]) + f1*float64(z)
	variance.count[groupIndex] += float64(z)
	return v2 + math.Pow(f1, 2)*float64(z), false
}

func (variance *Variance3) Grows(count int) {
	if len(variance.sum) == 0 {
		variance.sum = make([]float64, count)
		variance.count = make([]float64, count)
	} else {
		for i := 0; i < count; i++ {
			var a float64
			variance.sum = append(variance.sum, a)
			variance.count = append(variance.count, 0)
		}
	}
}

func (variance *Variance3) Eval(vs []float64) []float64 {
	for i, v := range vs {
		avg := (variance.sum[i]) / (variance.count[i])
		vs[i] = (v)/(variance.count[i]) - math.Pow(float64(avg), 2)
	}
	return vs
}

func (variance *Variance3) Merge(groupIndex1, groupIndex2 int64, x, y float64, IsEmpty1 bool, IsEmpty2 bool, agg any) (float64, bool) {
	variance2 := agg.(*Variance2)
	if IsEmpty1 && !IsEmpty2 {
		variance.sum[groupIndex1] = variance2.sum[groupIndex2]
		variance.count[groupIndex1] = variance.count[groupIndex2]
		return y, false
	} else if IsEmpty2 && !IsEmpty1 {
		return x, false
	} else if IsEmpty1 && IsEmpty2 {
		return x, true
	} else {
		variance.count[groupIndex1] += variance2.count[groupIndex2]
		variance.sum[groupIndex1] += variance2.sum[groupIndex2]
		return (x + y), false
	}
}

func (variance *Variance3) Fill(groupIndex int64, v1 types.Decimal128, v2 float64, z int64, IsEmpty bool, hasNull bool) (float64, bool) {
	if hasNull {
		return v2, IsEmpty
	} else if IsEmpty {
		f1 := v1.ToFloat64()
		variance.sum[groupIndex] = f1 * float64(z)
		variance.count[groupIndex] += float64(z)
		return math.Pow(f1, 2) * float64(z), false
	}
	f1 := v1.ToFloat64()
	variance.sum[groupIndex] = float64(variance.sum[groupIndex]) + f1*float64(z)
	variance.count[groupIndex] += float64(z)
	return v2 + math.Pow(f1, 2)*float64(z), false
}
