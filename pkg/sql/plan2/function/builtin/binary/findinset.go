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

package binary

import (
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/encoding"
	"github.com/matrixorigin/matrixone/pkg/vectorize/findinset"
	"github.com/matrixorigin/matrixone/pkg/vm/process2"
)

// findinset function's evaluation for arguments: [varchar, varchar], [char, char], [varchar, char], [char, varchar],
func FdsFindintset(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	lvs, rvs := vs[0].Col.(*types.Bytes), vs[1].Col.(*types.Bytes)

	var resultsLen int
	if len(lvs.Lengths) > len(rvs.Lengths) {
		resultsLen = len(lvs.Lengths)
	} else {
		resultsLen = len(rvs.Lengths)
	}

	retVec, err := process.Get(proc, 8*int64(resultsLen), types.Type{Oid: types.T_uint64, Size: 8})
	if err != nil {
		return nil, err
	}
	results := encoding.DecodeUint64Slice(retVec.Data)
	results = results[:resultsLen]
	retVec.Col = results

	nulls.Or(vs[0].Nsp, vs[1].Nsp, retVec.Nsp)

	switch {
	case vs[0].IsConstant() && vs[1].IsConstant():
		vector.SetCol(retVec, findinset.FindInSetWithAllConst(lvs, rvs, results))
	case vs[0].IsConstant():
		vector.SetCol(retVec, findinset.FindInSetWithLeftConst(lvs, rvs, results))
	case vs[1].IsConstant():
		vector.SetCol(retVec, findinset.FindInSetWithRightConst(lvs, rvs, results))
	default:
		vector.SetCol(retVec, findinset.FindInSet(lvs, rvs, results))
	}

	return retVec, nil
}
