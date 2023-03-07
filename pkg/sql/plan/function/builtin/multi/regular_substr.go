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
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vectorize/regular"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func RegularSubstr(ivecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	firstVector := ivecs[0]
	secondVector := ivecs[1]
	rtyp := types.T_varchar.ToType()
	if firstVector.IsConstNull() || secondVector.IsConstNull() {
		return vector.NewConstNull(rtyp, firstVector.Length(), proc.Mp()), nil
	}

	firstValues := vector.MustStrCol(firstVector)
	secondValues := vector.MustStrCol(secondVector)

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
	case 2:
		pos = []int64{1}
		occ = []int64{1}
		match_type = []string{"c"}

	case 3:
		pos = vector.MustFixedCol[int64](ivecs[2])
		occ = []int64{1}
		match_type = []string{"c"}
	case 4:
		pos = vector.MustFixedCol[int64](ivecs[2])
		occ = vector.MustFixedCol[int64](ivecs[3])
		match_type = []string{"c"}
	}

	rvec, err := proc.AllocVectorOfRows(rtyp, firstVector.Length(), nil)
	if err != nil {
		return nil, err
	}
	nulls.Or(firstVector.GetNulls(), secondVector.GetNulls(), rvec.GetNulls())
	err = regular.RegularSubstrWithArrays(firstValues, secondValues, pos, occ, match_type, rvec, proc, maxLen)
	if err != nil {
		return nil, err
	}

	return rvec, nil
}
