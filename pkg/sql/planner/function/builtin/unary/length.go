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

package unary

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func Length(ivecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	inputVector := ivecs[0]
	rtyp := types.T_int64.ToType()
	if inputVector.IsConstNull() {
		return vector.NewConstNull(rtyp, ivecs[0].Length(), proc.Mp()), nil
	}
	ivals := vector.MustStrCol(inputVector)
	if inputVector.IsConst() {
		return vector.NewConstFixed(rtyp, int64(len(ivals[0])), ivecs[0].Length(), proc.Mp()), nil
	} else {
		rvec, err := proc.AllocVectorOfRows(rtyp, len(ivals), inputVector.GetNulls())
		if err != nil {
			return nil, err
		}
		rvals := vector.MustFixedCol[int64](rvec)
		strLength(ivals, rvals)
		return rvec, nil
	}
}

func strLength(xs []string, rs []int64) []int64 {
	for i, s := range xs {
		rs[i] = int64(len(s))
	}
	return rs
}
