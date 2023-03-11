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
	"github.com/matrixorigin/matrixone/pkg/vectorize/format"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func Format(ivecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	var paramNum = len(ivecs)
	if ivecs[0].IsConstNull() || ivecs[1].IsConstNull() {
		return vector.NewConstNull(*ivecs[0].GetType(), ivecs[0].Length(), proc.Mp()), nil
	}

	//get the first arg number
	numberCols := vector.MustStrCol(ivecs[0])
	//get the second arg scale
	scaleCols := vector.MustStrCol(ivecs[1])
	//get the third arg locale
	var localeCols []string
	if paramNum == 2 || ivecs[2].IsConstNull() {
		localeCols = []string{"en_US"}
	} else {
		localeCols = vector.MustStrCol(ivecs[2])
	}

	//calcute rows
	rowCount := ivecs[0].Length()

	var rvec *vector.Vector = nil
	rvals := make([]string, rowCount)
	resultNsp := nulls.NewWithSize(rowCount)

	// set null row
	nulls.Or(ivecs[0].GetNulls(), ivecs[1].GetNulls(), resultNsp)

	var constVectors []bool
	if paramNum == 2 || ivecs[2].IsConstNull() {
		constVectors = []bool{ivecs[0].IsConst(), ivecs[1].IsConst(), true}
	} else {
		constVectors = []bool{ivecs[0].IsConst(), ivecs[1].IsConst(), ivecs[2].IsConst()}
	}

	//get result values
	err := format.Format(numberCols, scaleCols, localeCols, rowCount, constVectors, rvals)
	if err != nil {
		return nil, err
	}
	rvec = vector.NewVec(types.T_varchar.ToType())
	vector.AppendStringList(rvec, rvals, nil, proc.Mp())
	rvec.SetNulls(ivecs[0].GetNulls().Clone())

	return rvec, nil
}
