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
	// row_number() supported input type and output type.
	WinRowNumberReturnType = func(typs []types.Type) types.Type {
		return types.T_int64.ToType()
	}
)

func NewWinRowNumber(overloadID int64, dist bool, inputTypes []types.Type, outputType types.Type, _ any) (agg.Agg[any], error) {
	winPriv := &sWindowBase{}
	return agg.NewUnaryAgg(overloadID, winPriv, false, inputTypes[0], outputType, winPriv.Grows, winPriv.EvalRowNumber, winPriv.Merge, winPriv.Fill), nil
}

func (s *sWindowBase) EvalRowNumber(result []int64) ([]int64, error) {
	idx := 0
	for _, p := range s.ps {
		n := p[len(p)-1] - p[0]
		for i := int64(1); i <= n; i++ {
			result[idx] = i
			idx++
		}
	}
	return result, nil
}
