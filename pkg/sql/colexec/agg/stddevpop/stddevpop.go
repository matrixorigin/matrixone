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

package stddevpop

import (
	"math"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	Variance "github.com/matrixorigin/matrixone/pkg/sql/colexec/agg/variance"
)

func ReturnType(_ []types.Type) types.Type {
	return types.New(types.T_float64, 0, 0, 0)
}

//New1 is used to Created a Variance which supports float,int,uint
func New[T1 types.Floats | types.Ints | types.UInts]() *Stddevpop[T1] {
	return &Stddevpop[T1]{variance: Variance.New1[T1]()}
}

//New2 is used to Created a Variance which supports decimal64
//if is decimal,you need to give pricision
func New2() *Stddevpop2 {
	return &Stddevpop2{variance: Variance.New2()}
}

//New2 is used to Created a Variance which supports decimal64
//if is decimal,you need to give pricision
func New3() *Stddevpop3 {
	return &Stddevpop3{variance: Variance.New3()}
}

func (sdp *Stddevpop[T1]) Grows(sizes int) {
	sdp.variance.Grows(sizes)
}

func (sdp *Stddevpop[T1]) Eval(vs []float64) []float64 {
	sdp.variance.Eval(vs)
	for i, v := range vs {
		vs[i] = math.Sqrt(v)
	}
	return vs
}

func (sdp *Stddevpop[T1]) Merge(groupIndex1, groupIndex2 int64, x, y float64, IsEmpty1 bool, IsEmpty2 bool, agg any) (float64, bool) {
	Stddevpop := agg.(*Stddevpop[T1])
	return sdp.variance.Merge(groupIndex1, groupIndex2, x, y, IsEmpty1, IsEmpty2, Stddevpop.variance)
}

func (sdp *Stddevpop[T1]) Fill(groupIndex int64, v1 T1, v2 float64, z int64, IsEmpty bool, hasNull bool) (float64, bool) {
	return sdp.variance.Fill(groupIndex, v1, v2, z, IsEmpty, hasNull)
}

func (sdp *Stddevpop2) Grows(sizes int) {
	sdp.variance.Grows(sizes)
}

func (sdp *Stddevpop2) Eval(vs []float64) []float64 {
	sdp.variance.Eval(vs)
	for i, v := range vs {
		vs[i] = math.Sqrt(v)
	}
	return vs
}

func (sdp *Stddevpop2) Merge(groupIndex1, groupIndex2 int64, x, y float64, IsEmpty1 bool, IsEmpty2 bool, agg any) (float64, bool) {
	Stddevpop2 := agg.(*Stddevpop2)
	return sdp.variance.Merge(groupIndex1, groupIndex2, x, y, IsEmpty1, IsEmpty2, Stddevpop2.variance)
}

func (sdp *Stddevpop2) Fill(groupIndex int64, v1 types.Decimal64, v2 float64, z int64, IsEmpty bool, hasNull bool) (float64, bool) {
	return sdp.variance.Fill(groupIndex, v1, v2, z, IsEmpty, hasNull)
}

func (sdp *Stddevpop3) Grows(sizes int) {
	sdp.variance.Grows(sizes)
}

func (sdp *Stddevpop3) Eval(vs []float64) []float64 {
	sdp.variance.Eval(vs)
	for i, v := range vs {
		vs[i] = math.Sqrt(v)
	}
	return vs
}

func (sdp *Stddevpop3) Merge(groupIndex1, groupIndex2 int64, x, y float64, IsEmpty1 bool, IsEmpty2 bool, agg any) (float64, bool) {
	Stddevpop3 := agg.(*Stddevpop2)
	return sdp.variance.Merge(groupIndex1, groupIndex2, x, y, IsEmpty1, IsEmpty2, Stddevpop3.variance)
}

func (sdp *Stddevpop3) Fill(groupIndex int64, v1 types.Decimal128, v2 float64, z int64, IsEmpty bool, hasNull bool) (float64, bool) {
	return sdp.variance.Fill(groupIndex, v1, v2, z, IsEmpty, hasNull)
}
