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

package operator

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func IsNot(ivecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	lv := ivecs[0]
	rv := ivecs[1]
	rtyp := types.T_bool.ToType()

	if !rv.IsConst() || rv.IsConstNull() {
		return nil, moerr.NewInternalError(proc.Ctx, "second parameter of IS must be TRUE or FALSE")
	}
	right := vector.MustFixedCol[bool](rv)[0]

	if lv.IsConst() {
		if lv.IsConstNull() {
			return vector.NewConstFixed(rtyp, false, ivecs[0].Length(), proc.Mp()), nil
		} else {
			lefts := vector.MustFixedCol[bool](lv)
			return vector.NewConstFixed(rtyp, lefts[0] != right, ivecs[0].Length(), proc.Mp()), nil
		}
	} else {
		lefts := vector.MustFixedCol[bool](lv)
		l := len(lefts)
		vec, err := proc.AllocVectorOfRows(*lv.GetType(), l, lv.GetNulls())
		if err != nil {
			return nil, err
		}
		col := vector.MustFixedCol[bool](vec)
		for i := range lefts {
			if nulls.Contains(lv.GetNulls(), uint64(i)) {
				col[i] = false
			} else {
				col[i] = lefts[i] != right
			}
		}
		return vec, nil
	}
}
