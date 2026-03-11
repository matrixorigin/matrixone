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
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// Cast to binary but no right-padding.
func Binary(ivecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	rtyp := types.T_binary.ToType()

	if ivecs[0].IsConstNull() {
		return vector.NewConstNull(rtyp, ivecs[0].Length(), proc.Mp()), nil
	}

	ivals := vector.MustBytesCol(ivecs[0])
	if ivecs[0].IsConst() {
		return vector.NewConstBytes(rtyp, doBinary(ivals[0]), ivecs[0].Length(), proc.Mp()), nil
	}

	rvec, err := proc.AllocVectorOfRows(rtyp, len(ivals), nil)
	if err != nil {
		return nil, err
	}

	nulls.Set(rvec.GetNulls(), ivecs[0].GetNulls())
	for i, s := range ivals {
		//Check nulls.
		if !rvec.GetNulls().Contains(uint64(i)) {
			vector.SetBytesAt(rvec, i, doBinary(s), proc.Mp())
		}
	}
	return rvec, nil
}

func doBinary(orig []byte) []byte {
	if len(orig) > types.MaxBinaryLen {
		return orig[:types.MaxBinaryLen]
	} else {
		return orig
	}
}
