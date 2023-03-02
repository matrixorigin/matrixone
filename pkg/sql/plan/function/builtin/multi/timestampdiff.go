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
	"github.com/matrixorigin/matrixone/pkg/vectorize/datediff"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func TimestampDiff(ivecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	//input vectors
	firstVector := ivecs[0]
	secondVector := ivecs[1]
	thirdVector := ivecs[2]
	firstValues := vector.MustStrCol(firstVector)
	secondValues := vector.MustFixedCol[types.Datetime](secondVector)
	thirdValues := vector.MustFixedCol[types.Datetime](thirdVector)
	rtyp := types.T_int64.ToType()

	//the max Length of all vectors
	maxLen := ivecs[0].Length()
	for i := range ivecs {
		val := ivecs[i].Length()
		if val > maxLen {
			maxLen = val
		}
	}

	if firstVector.IsConstNull() || secondVector.IsConstNull() || thirdVector.IsConstNull() {
		return vector.NewConstNull(rtyp, ivecs[0].Length(), proc.Mp()), nil
	}

	rvec, err := proc.AllocVectorOfRows(rtyp, maxLen, nil)
	if err != nil {
		return nil, err
	}
	rvals := vector.MustFixedCol[int64](rvec)
	err = datediff.TimestampDiffWithCols(firstValues, secondValues, thirdValues, firstVector.GetNulls(), secondVector.GetNulls(), thirdVector.GetNulls(), rvec.GetNulls(), rvals, maxLen)
	if err != nil {
		return nil, err
	}
	return rvec, nil
}
