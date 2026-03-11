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
	"github.com/matrixorigin/matrixone/pkg/vectorize/timediff"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func TimeDiff[T timediff.DiffT](ivecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	//input vectors
	firstVector := ivecs[0]
	secondVector := ivecs[1]
	firstValues := vector.MustFixedCol[T](firstVector)
	secondValues := vector.MustFixedCol[T](secondVector)
	rtyp := types.T_time.ToType()

	scale := firstVector.GetType().Scale
	if firstVector.GetType().Scale < secondVector.GetType().Scale {
		scale = secondVector.GetType().Scale
	}
	rtyp.Scale = scale

	if firstVector.IsConstNull() || secondVector.IsConstNull() {
		return vector.NewConstNull(rtyp, firstVector.Length(), proc.Mp()), nil
	}

	vectorLen := len(firstValues)
	if firstVector.IsConst() {
		vectorLen = len(secondValues)
	}

	rvec, err := proc.AllocVectorOfRows(rtyp, vectorLen, nil)
	if err != nil {
		return nil, err
	}

	rs := vector.MustFixedCol[types.Time](rvec)
	nulls.Or(firstVector.GetNulls(), secondVector.GetNulls(), rvec.GetNulls())
	if err = timediff.TimeDiffWithTimeFn(firstValues, secondValues, rs); err != nil {
		return nil, err
	}
	return rvec, nil
}
