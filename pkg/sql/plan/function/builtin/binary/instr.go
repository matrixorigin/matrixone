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
	"github.com/matrixorigin/matrixone/pkg/vectorize/instr"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func Instr(vecs []*vector.Vector, proc *process.Process) (ret *vector.Vector, err error) {
	defer func() {
		if err != nil && ret != nil {
			ret.Free(proc.Mp())
		}
	}()
	v1, v2 := vecs[0], vecs[1]
	resultType := types.T_int64.ToType()
	if v1.IsScalarNull() || v2.IsScalarNull() {
		ret = proc.AllocScalarNullVector(resultType)
		return
	}
	s1, s2 := vector.MustStrCols(v1), vector.MustStrCols(v2)
	if v1.IsScalar() && v2.IsScalar() {
		ret = proc.AllocScalarVector(resultType)
		str, substr := s1[0], s2[0]
		err = ret.Append(instr.Single(str, substr), false, proc.Mp())
		return
	}
	maxLen := len(s1)
	if len(s2) > maxLen {
		maxLen = len(s2)
	}
	nsp := nulls.NewWithSize(maxLen)
	nulls.Or(v1.Nsp, v2.Nsp, nsp)
	ret, err = proc.AllocVectorOfRows(resultType, int64(maxLen), nsp)
	if err != nil {
		return
	}
	rs := vector.MustTCols[int64](ret)
	instr.Instr(s1, s2, rs, ret.Nsp)
	return
}
