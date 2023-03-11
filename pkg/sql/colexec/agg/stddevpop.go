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

type Stddevpop[T1 types.Floats | types.Ints | types.UInts] struct {
	Variance *Variance[T1]
}

type StdD64 struct {
	Variance *VD64
}

type StdD128 struct {
	Variance *VD128
}

func StdDevPopReturnType(typs []types.Type) types.Type {
	switch typs[0].Oid {
	case types.T_decimal64, types.T_decimal128:
		return types.New(types.T_decimal128, 0, typs[0].Scale)
	default:
		return types.New(types.T_float64, 0, 0)
	}
}

// NewStdDevPop is used to create a StdDevPop which supports float,int,uint
func NewStdDevPop[T1 types.Floats | types.Ints | types.UInts]() *Stddevpop[T1] {
	return &Stddevpop[T1]{Variance: NewVariance[T1]()}
}

func (sdp *Stddevpop[T1]) Grows(sizes int) {
	sdp.Variance.Grows(sizes)
}

func (sdp *Stddevpop[T1]) Eval(vs []float64) []float64 {
	sdp.Variance.Eval(vs)
	for i, v := range vs {
		vs[i] = math.Sqrt(v)
	}
	return vs
}

func (sdp *Stddevpop[T1]) Merge(groupIndex1, groupIndex2 int64, x, y float64, IsEmpty1 bool, IsEmpty2 bool, agg any) (float64, bool) {
	Stddevpop := agg.(*Stddevpop[T1])
	return sdp.Variance.Merge(groupIndex1, groupIndex2, x, y, IsEmpty1, IsEmpty2, Stddevpop.Variance)
}

func (sdp *Stddevpop[T1]) Fill(groupIndex int64, v1 T1, v2 float64, z int64, IsEmpty bool, hasNull bool) (float64, bool) {
	return sdp.Variance.Fill(groupIndex, v1, v2, z, IsEmpty, hasNull)
}

func (sdp *Stddevpop[T1]) MarshalBinary() ([]byte, error) {
	return types.Encode(sdp.Variance)
}

func (sdp *Stddevpop[T1]) UnmarshalBinary(data []byte) error {
	// avoid resulting errors caused by morpc overusing memory
	copyData := make([]byte, len(data))
	copy(copyData, data)
	types.Decode(copyData, sdp.Variance)
	return nil
}

func NewStdD64() *StdD64 {
	return &StdD64{Variance: NewVD64()}
}

func (s *StdD64) Grows(size int) {
	s.Variance.Grows(size)
}

func (s *StdD64) Eval(vs []types.Decimal128) []types.Decimal128 {
	s.Variance.Eval(vs)
	for i, v := range vs {
		tmp := math.Sqrt(v.ToFloat64())
		d, _ := types.Decimal128_FromFloat64(tmp, 34, 10)
		vs[i] = d
	}
	return vs
}

func (s *StdD64) Merge(groupIndex1, groupIndex2 int64, x, y types.Decimal128, IsEmpty1 bool, IsEmpty2 bool, agg any) (types.Decimal128, bool) {
	ss := agg.(*StdD64)
	return s.Variance.Merge(groupIndex1, groupIndex2, x, y, IsEmpty1, IsEmpty2, ss.Variance)
}

func (s *StdD64) Fill(groupIndex int64, v1 types.Decimal64, v2 types.Decimal128, z int64, IsEmpty bool, hasNull bool) (types.Decimal128, bool) {
	return s.Variance.Fill(groupIndex, v1, v2, z, IsEmpty, hasNull)
}

func (s *StdD64) MarshalBinary() ([]byte, error) {
	return types.Encode(s.Variance)
}

func (s *StdD64) UnmarshalBinary(data []byte) error {
	// avoid resulting errors caused by morpc overusing memory
	copyData := make([]byte, len(data))
	copy(copyData, data)
	types.Decode(copyData, s.Variance)
	return nil
}

func NewStdD128() *StdD128 {
	return &StdD128{Variance: NewVD128()}
}

func (s *StdD128) Grows(size int) {
	s.Variance.Grows(size)
}

func (s *StdD128) Eval(vs []types.Decimal128) []types.Decimal128 {
	s.Variance.Eval(vs)
	for i, v := range vs {
		tmp := math.Sqrt(v.ToFloat64())
		d, _ := types.Decimal128_FromFloat64(tmp, 34, 10)
		vs[i] = d
	}
	return vs
}

func (s *StdD128) Merge(groupIndex1, groupIndex2 int64, x, y types.Decimal128, IsEmpty1 bool, IsEmpty2 bool, agg any) (types.Decimal128, bool) {
	ss := agg.(*StdD128)
	return s.Variance.Merge(groupIndex1, groupIndex2, x, y, IsEmpty1, IsEmpty2, ss.Variance)
}

func (s *StdD128) Fill(groupIndex int64, v1 types.Decimal128, v2 types.Decimal128, z int64, IsEmpty bool, hasNull bool) (types.Decimal128, bool) {
	return s.Variance.Fill(groupIndex, v1, v2, z, IsEmpty, hasNull)
}

func (s *StdD128) MarshalBinary() ([]byte, error) {
	return types.Encode(s.Variance)
}

func (s *StdD128) UnmarshalBinary(data []byte) error {
	// avoid resulting errors caused by morpc overusing memory
	copyData := make([]byte, len(data))
	copy(copyData, data)
	types.Decode(copyData, s.Variance)
	return nil
}
