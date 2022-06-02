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
	"errors"
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

	if origVecs[0].IsScalar() {
		return nil, errors.New("The first argument of the lpad function can not be a constant")
	}

	// gets a new vector to store our result
	resultVec, err := process.Get(proc, 24*int64(len(strs.Lengths)), origVecs[0].Typ)
	if err != nil {
		return nil, err
	}
	result, nsp, err := rpad.Rpad(strs, sizes, padstrs, isConst, oriNsps)
	if err != nil {
		return nil, err
	}
	resultVec.Nsp = nsp
	vector.SetCol(resultVec, result)
	return resultVec, nil
}
