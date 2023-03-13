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

func Replace(ivecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
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

	if firstVector.IsConstNull() || secondVector.IsConstNull() || thirdVector.IsConstNull() {
		return vector.NewConstNull(rtyp, maxLen, proc.Mp()), nil
	}

	rvec, err := proc.AllocVectorOfRows(rtyp, 0, nil)
	if err != nil {
		return nil, err
	}
	err = regular.ReplaceWithArrays(firstValues, secondValues, thirdValues, firstVector.GetNulls(), secondVector.GetNulls(), thirdVector.GetNulls(), rvec, proc, maxLen)
	if err != nil {
		return nil, err
	}

	return rvec, nil
}
