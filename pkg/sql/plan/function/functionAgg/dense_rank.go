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
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/agg"
)

var (
	// dense_rank() supported input type and output type.
	WinDenseRankReturnType = func(typs []types.Type) types.Type {
		return types.T_int64.ToType()
	}
)

func NewWinDenseRank(overloadID int64, dist bool, inputTypes []types.Type, outputType types.Type, _ any) (agg.Agg[any], error) {
	winPriv := &sWindowBase{}
	return agg.NewUnaryAgg(overloadID, winPriv, false, inputTypes[0], outputType, winPriv.Grows, winPriv.EvalDenseRank, winPriv.Merge, winPriv.Fill), nil
}

type sWindowBase struct{ ps [][]int64 }

func (s *sWindowBase) Dup() agg.AggStruct {
	val := &sWindowBase{
		ps: make([][]int64, len(s.ps)),
	}
	for i, s := range s.ps {
		val.ps[i] = make([]int64, len(s))
		copy(val.ps[i], s)
	}
	return val
}
func (s *sWindowBase) Grows(_ int)         {}
func (s *sWindowBase) Free(_ *mpool.MPool) {}
func (s *sWindowBase) Fill(groupNumber int64, value int64, lastResult int64, count int64, isEmpty bool, isNull bool) (int64, bool, error) {
	n := int(groupNumber) - len(s.ps)
	for i := 0; i < n+1; i++ {
		s.ps = append(s.ps, []int64{})
	}
	s.ps[groupNumber] = append(s.ps[groupNumber], value)
	return 0, false, nil
}
func (s *sWindowBase) Merge(groupNumber1 int64, groupNumber2 int64, result1, result2 int64, isEmpty1, isEmpty2 bool, _ any) (int64, bool, error) {
	return 0, false, nil
}
func (s *sWindowBase) EvalDenseRank(result []int64) ([]int64, error) {
	idx := 0
	for _, p := range s.ps {
		sn := int64(1)
		for i := 1; i < len(p); i++ {
			m := int(p[i] - p[i-1])
			for t := 1; t <= m; t++ {
				result[idx] = sn
				idx++
			}
			sn++
		}
	}
	return result, nil
}
func (s *sWindowBase) MarshalBinary() ([]byte, error) { return types.EncodeSlice(s.ps), nil }
func (s *sWindowBase) UnmarshalBinary(data []byte) error {
	copyData := make([]byte, len(data))
	copy(copyData, data)
	s.ps = types.DecodeSlice[[]int64](copyData)
	return nil
}
