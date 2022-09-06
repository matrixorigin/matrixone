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

func ReturnType(typs []types.Type) types.Type {
	switch typs[0].Oid {
	case types.T_decimal64, types.T_decimal128:
		return types.New(types.T_decimal128, 0, typs[0].Scale, typs[0].Precision)
	default:
		return types.New(types.T_float64, 0, 0, 0)
	}
}

// New1 is used to Created a Variance which supports float,int,uint
func New[T1 types.Floats | types.Ints | types.UInts]() *Stddevpop[T1] {
	return &Stddevpop[T1]{variance: Variance.New1[T1]()}
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

func NewStdD64() *StdD64 {
	return &StdD64{variance: Variance.NewVD64()}
}

func (s *StdD64) Grows(size int) {
	s.variance.Grows(size)
}

func (s *StdD64) Eval(vs []types.Decimal128) []types.Decimal128 {
	s.variance.Eval(vs)
	for i, v := range vs {
		tmp := math.Sqrt(v.ToFloat64())
		vs[i] = types.Decimal128_FromFloat64(tmp)
	}
	return vs
}

func (s *StdD64) Merge(groupIndex1, groupIndex2 int64, x, y types.Decimal128, IsEmpty1 bool, IsEmpty2 bool, agg any) (types.Decimal128, bool) {
	ss := agg.(*StdD64)
	return s.variance.Merge(groupIndex1, groupIndex2, x, y, IsEmpty1, IsEmpty2, ss.variance)
}

func (s *StdD64) Fill(groupIndex int64, v1 types.Decimal64, v2 types.Decimal128, z int64, IsEmpty bool, hasNull bool) (types.Decimal128, bool) {
	return s.variance.Fill(groupIndex, v1, v2, z, IsEmpty, hasNull)
}

func NewStdD128() *StdD128 {
	return &StdD128{variance: Variance.NewVD128()}
}

func (s *StdD128) Grows(size int) {
	s.variance.Grows(size)
}

func (s *StdD128) Eval(vs []types.Decimal128) []types.Decimal128 {
	s.variance.Eval(vs)
	for i, v := range vs {
		tmp := math.Sqrt(v.ToFloat64())
		vs[i] = types.Decimal128_FromFloat64(tmp)
	}
	return vs
}

func (s *StdD128) Merge(groupIndex1, groupIndex2 int64, x, y types.Decimal128, IsEmpty1 bool, IsEmpty2 bool, agg any) (types.Decimal128, bool) {
	ss := agg.(*StdD128)
	return s.variance.Merge(groupIndex1, groupIndex2, x, y, IsEmpty1, IsEmpty2, ss.variance)
}

func (s *StdD128) Fill(groupIndex int64, v1 types.Decimal128, v2 types.Decimal128, z int64, IsEmpty bool, hasNull bool) (types.Decimal128, bool) {
	return s.variance.Fill(groupIndex, v1, v2, z, IsEmpty, hasNull)
}
