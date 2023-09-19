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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/agg"
)

var (
	// rank() supported input type and output type.
	WinRankReturnType = func(typs []types.Type) types.Type {
		return types.T_int64.ToType()
	}
)

func NewWinRank(overloadID int64, dist bool, inputTypes []types.Type, outputType types.Type, _ any) (agg.Agg[any], error) {
	winPriv := &sWindowBase{}
	return agg.NewUnaryAgg(overloadID, winPriv, false, inputTypes[0], outputType, winPriv.Grows, winPriv.EvalRank, winPriv.Merge, winPriv.Fill), nil
}

func (s *sWindowBase) EvalRank(result []int64) ([]int64, error) {
	idx := 0
	for _, p := range s.ps {
		sn := int64(1)
		for i := 1; i < len(p); i++ {

			m := int(p[i] - p[i-1])

			tmp := sn
			for t := 1; t <= m; t++ {
				result[idx] = sn
				tmp++
				idx++
			}
			sn = tmp
		}
	}
	return result, nil
}
