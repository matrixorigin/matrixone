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
	maxLen := v1.Length()
	if v2.Length() > v1.Length() {
		maxLen = v2.Length()
	}
	rtyp := types.T_int64.ToType()
	if v1.IsConstNull() || v2.IsConstNull() {
		ret = vector.NewConstNull(rtyp, v1.Length(), proc.Mp())
		return
	}
	s1, s2 := vector.MustStrCol(v1), vector.MustStrCol(v2)
	if v1.IsConst() && v2.IsConst() {
		str, substr := s1[0], s2[0]
		ret = vector.NewConstFixed(rtyp, instr.Single(str, substr), v1.Length(), proc.Mp())
		return
	}
	ret, err = proc.AllocVectorOfRows(rtyp, maxLen, nil)
	if err != nil {
		return
	}
	rs := vector.MustFixedCol[int64](ret)
	instr.Instr(s1, s2, []*nulls.Nulls{v1.GetNulls(), v2.GetNulls()}, rs, ret.GetNulls())
	return
}
