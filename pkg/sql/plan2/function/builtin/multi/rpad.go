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
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vectorize/rpad"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func Rpad(origVecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	if origVecs[0].IsScalarNull() || origVecs[1].IsScalarNull() || origVecs[2].IsScalarNull() {
		return proc.AllocScalarNullVector(origVecs[0].Typ), nil
	}

	isConst := []bool{origVecs[0].IsScalar(), origVecs[1].IsScalar(), origVecs[2].IsScalar()}

	// gets all args
	strs, sizes, padstrs := origVecs[0].Col.(*types.Bytes), origVecs[1].Col, origVecs[2].Col
	oriNsps := []*nulls.Nulls{origVecs[0].Nsp, origVecs[1].Nsp, origVecs[2].Nsp}

	// gets a new vector to store our result
	resultVec, err := proc.AllocVector(origVecs[0].Typ, 24*int64(len(strs.Lengths)))
	if err != nil {
		return nil, err
	}
	if origVecs[0].IsScalar() && origVecs[1].IsScalar() && origVecs[2].IsScalar() {
		resultVec.IsConst = true
	}

	result, nsp, err := rpad.Rpad(strs, sizes, padstrs, isConst, oriNsps)
	if err != nil {
		return nil, err
	}
	resultVec.Nsp = nsp
	vector.SetCol(resultVec, result)
	return resultVec, nil
}
