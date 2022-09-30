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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vectorize/regular"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func RegularInstr(vectors []*vector.Vector, proc *process.Process, funcName string) (*vector.Vector, error){
	firstVector := vectors[0]
	secondVector := vectors[1]
	firstValues := vector.MustStrCols(firstVector)
	secondValues := vector.MustStrCols(secondVector)
	resultType := types.T_int64.ToType()

	//maxLen
	var maxLen int
	if len(firstValues) > len(secondValues) {
		maxLen = len(firstValues)
	} else {
		maxLen = len(secondValues)
	}	
    //option parameters
	pos := make([]int64, maxLen)
	occ := make([]int64, maxLen)
	opt := make([]uint8, maxLen)
	match_type := make([]string, maxLen)

	switch funcName{
	case "RegularInstrOnly":
		for i := range pos{
			pos[i] = 1
			occ[i] = 1
			opt[i] = 0
			match_type[i] = ""
		}	
	case "RegularInstrWithPos":
		if len(vectors) < 3{
			return nil, moerr.NewInvalidArg("regexp_instr function have invalid parameter length", len(vectors))
		}
		pos = vector.MustTCols[int64](vectors[2])
		for i := range pos{
			occ[i] = 1
			opt[i] = 0
			match_type[i] = ""
		}
	case "RegularInstrWithPosAndOcc":
		if len(vectors) < 4{
			return nil, moerr.NewInvalidArg("regexp_instr function have invalid parameter length", len(vectors))
		}
		pos = vector.MustTCols[int64](vectors[2])
		occ = vector.MustTCols[int64](vectors[3])
		for i := range pos{
			opt[i] = 0
			match_type[i] = ""
		}
	case "RegularInstrWithPosOccAndOpt":
	    if len(vectors) < 5{
			return nil, moerr.NewInvalidArg("regexp_instr function have invalid parameter length", len(vectors))
	    }
		pos = vector.MustTCols[int64](vectors[2])
		occ = vector.MustTCols[int64](vectors[3])
		opt = vector.MustTCols[uint8](vectors[4])
		for i := range pos{
			match_type[i] = ""
		}
	}

	if firstVector.IsScalar() && secondVector.IsScalar() {
		if firstVector.IsScalarNull() || secondVector.IsScalarNull() {
			return proc.AllocScalarNullVector(resultType), nil
		}
		resultVector := proc.AllocScalarVector(resultType)
		rs := make([]int64, 1)
		res, err := regular.RegularInstrWithArrays(firstValues, secondValues, pos, occ, opt, match_type, firstVector.Nsp, secondVector.Nsp, resultVector.Nsp, rs)
		vector.SetCol(resultVector, res)
		return resultVector, err
	} else {
		resultVector, err := proc.AllocVectorOfRows(resultType, int64(maxLen), nil)
		if err != nil {
			return nil, err
		}
		resultValues := vector.MustTCols[int64](resultVector)
		_, err = regular.RegularInstrWithArrays(firstValues, secondValues, pos, occ, opt, match_type, firstVector.Nsp, secondVector.Nsp, resultVector.Nsp, resultValues)
		return resultVector, err
	}
}


func RegularInstrOnly(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error){
	return RegularInstr(vectors, proc, "RegularInstrOnly")
}

func RegularInstrWithPos(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error){
	return RegularInstr(vectors, proc, "RegularInstrWithPos")
}

func RegularInstrWithPosAndOcc(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error){
	return RegularInstr(vectors, proc, "RegularInstrWithPosAndOcc")
}

func RegularInstrWithPosOccAndOpt(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error){
	return RegularInstr(vectors, proc, "RegularInstrWithPosOccAndOpt")
}