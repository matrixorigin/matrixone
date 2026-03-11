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

package multi

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vectorize/regular"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func RegularReplace(ivecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	firstVector := ivecs[0]
	secondVector := ivecs[1]
	thirdVector := ivecs[2]
	firstValues := vector.MustStrCol(firstVector)
	secondValues := vector.MustStrCol(secondVector)
	thirdValues := vector.MustStrCol(thirdVector)
	rtyp := types.T_varchar.ToType()

	//maxLen
	maxLen := ivecs[0].Length()
	for i := range ivecs {
		val := ivecs[i].Length()
		if val > maxLen {
			maxLen = val
		}
	}

	//optional arguments
	var pos []int64
	var occ []int64
	var match_type []string

	//different parameter length conditions
	switch len(ivecs) {
	case 3:
		pos = []int64{1}
		occ = []int64{0}
		match_type = []string{"c"}

	case 4:
		pos = vector.MustFixedCol[int64](ivecs[3])
		occ = []int64{0}
		match_type = []string{"c"}
	case 5:
		pos = vector.MustFixedCol[int64](ivecs[3])
		occ = vector.MustFixedCol[int64](ivecs[4])
		match_type = []string{"c"}
	}

	if firstVector.IsConstNull() || secondVector.IsConstNull() || thirdVector.IsConstNull() {
		return vector.NewConstNull(rtyp, maxLen, proc.Mp()), nil
	}

	rvec, err := proc.AllocVectorOfRows(rtyp, 0, nil)
	if err != nil {
		return nil, err
	}
	err = regular.RegularReplaceWithArrays(firstValues, secondValues, thirdValues, pos, occ, match_type, firstVector.GetNulls(), secondVector.GetNulls(), thirdVector.GetNulls(), rvec, proc, maxLen)
	if err != nil {
		return nil, err
	}
	return rvec, nil
}
