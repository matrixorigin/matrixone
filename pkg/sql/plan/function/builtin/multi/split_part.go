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

package multi

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vectorize/split_part"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var fnMap = []func(s1, s2 []string, s3 []uint32, nsps []*nulls.Nulls, rs []string, rnsp *nulls.Nulls){
	1: split_part.SplitPart1,
	split_part.SplitPart2,
	split_part.SplitPart3,
	split_part.SplitPart4,
	split_part.SplitPart5,
	split_part.SplitPart6,
	split_part.SplitPart7,
}

func SplitPart(ivecs []*vector.Vector, proc *process.Process) (vec *vector.Vector, err error) {
	defer func() {
		if err != nil && vec != nil {
			vec.Free(proc.Mp())
		}
	}()
	v1, v2, v3 := ivecs[0], ivecs[1], ivecs[2]
	rtyp := types.T_varchar.ToType()
	maxLen := findMaxLen(ivecs)
	if v1.IsConstNull() || v2.IsConstNull() || v3.IsConstNull() {
		vec = vector.NewConstNull(rtyp, v1.Length(), proc.Mp())
		return
	}
	if !validCount(v3) {
		err = moerr.NewInvalidInput(proc.Ctx, "split_part: field contains non-positive integer")
		return
	}
	s1, s2, s3 := vector.MustStrCol(v1), vector.MustStrCol(v2), vector.MustFixedCol[uint32](v3)
	if v1.IsConst() && v2.IsConst() && v3.IsConst() {
		ret, isNull := split_part.SplitSingle(s1[0], s2[0], s3[0])
		if isNull {
			vec = vector.NewConstNull(rtyp, v1.Length(), proc.Mp())
			return
		}
		vec = vector.NewConstBytes(rtyp, []byte(ret), v1.Length(), proc.Mp())
		return
	}
	vec, err = proc.AllocVectorOfRows(rtyp, maxLen, nil)
	if err != nil {
		return
	}
	fnIdx := determineFn(ivecs)

	rs := make([]string, maxLen)
	fnMap[fnIdx](s1, s2, s3, []*nulls.Nulls{v1.GetNulls(), v2.GetNulls(), v3.GetNulls()}, rs, vec.GetNulls())
	for i, r := range rs {
		if vec.GetNulls().Contains(uint64(i)) {
			continue
		}
		err = vector.SetStringAt(vec, i, r, proc.Mp())
		if err != nil {
			return
		}
	}
	return vec, err
}

func findMaxLen(vecs []*vector.Vector) int {
	ret := 0
	for _, vec := range vecs {
		if vec.Length() > ret {
			ret = vec.Length()
		}
	}
	return ret
}

func determineFn(vecs []*vector.Vector) int {
	ret := 0
	vecCnt := 3
	for i, vec := range vecs {
		if !vec.IsConst() {
			ret |= 1 << (vecCnt - i - 1)
		}
	}
	return ret
}

func validCount(v *vector.Vector) bool {
	s := vector.MustFixedCol[uint32](v)
	for i, x := range s {
		if v.GetNulls().Contains(uint64(i)) {
			continue
		}
		if x == 0 {
			return false
		}
	}
	return true
}
