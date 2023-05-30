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
)

type RowNumber struct {
	Ps [][]int64
}

func RowNumberReturnType() types.Type {
	return types.New(types.T_int64, 0, 0)
}

func NewRowNumber() *RowNumber {
	return &RowNumber{}
}

func (r *RowNumber) Grows(_ int) {}

func (r *RowNumber) Eval(vs []int64, err error) ([]int64, error) {
	idx := 0
	for _, p := range r.Ps {
		n := p[len(p)-1] - p[0]
		for i := int64(1); i <= n; i++ {
			vs[idx] = i
			idx++
		}
	}
	return vs, nil
}

func (r *RowNumber) Fill(i int64, value int64, ov int64, z int64, isEmpty bool, isNull bool) (int64, bool, error) {
	n := int(i) - len(r.Ps)
	for j := 0; j < n+1; j++ {
		r.Ps = append(r.Ps, []int64{})
	}
	r.Ps[i] = append(r.Ps[i], value)
	return 0, false, nil
}

func (r *RowNumber) Merge(xIndex int64, yIndex int64, x int64, y int64, xEmpty bool, yEmpty bool, yAvg any) (int64, bool, error) {
	return 0, false, nil
}

func (r *RowNumber) BatchFill(rs, vs any, start, count int64, vps []uint64, zs []int64, nsp *nulls.Nulls) error {
	return nil
}

func (r *RowNumber) MarshalBinary() ([]byte, error) {
	return types.EncodeSlice(r.Ps), nil
}

func (r *RowNumber) UnmarshalBinary(data []byte) error {
	copyData := make([]byte, len(data))
	copy(copyData, data)
	r.Ps = types.DecodeSlice[[]int64](copyData)
	return nil
}
