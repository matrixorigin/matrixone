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
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func DateToYear(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	inputVector := vectors[0]
	resultType := types.Type{Oid: types.T_int64, Size: 8}
	inputValues := vector.MustTCols[types.Date](inputVector)
	if inputVector.IsScalar() {
		if inputVector.ConstVectorIsNull() {
			return proc.AllocScalarNullVector(resultType), nil
		}
		resultValues := make([]int64, 1)
		DateToYearPlan2(inputValues, resultValues)
		return vector.NewConstFixed(resultType, 1, resultValues[0], proc.Mp()), nil
	} else {
		resultVector, err := proc.AllocVectorOfRows(resultType, int64(len(inputValues)), inputVector.Nsp)
		if err != nil {
			return nil, err
		}
		resultValues := vector.MustTCols[int64](resultVector)
		DateToYearPlan2(inputValues, resultValues)
		// resultValues2 := make([]int64, len(resultValues))
		// for i, x := range resultValues {
		// 	resultValues2[i] = int64(x)
		// }
		return resultVector, nil
	}
}

func DatetimeToYear(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	inputVector := vectors[0]
	resultType := types.Type{Oid: types.T_int64, Size: 8}
	inputValues := vector.MustTCols[types.Datetime](inputVector)
	if inputVector.IsScalar() {
		if inputVector.ConstVectorIsNull() {
			return proc.AllocScalarNullVector(resultType), nil
		}
		resultVector := vector.NewConst(resultType, 1)
		resultValues := make([]int64, 1)
		DatetimeToYearPlan2(inputValues, resultValues)
		vector.SetCol(resultVector, resultValues)
		return resultVector, nil
	} else {
		resultVector, err := proc.AllocVectorOfRows(resultType, int64(len(inputValues)), inputVector.Nsp)
		if err != nil {
			return nil, err
		}
		resultValues := vector.MustTCols[int64](resultVector)
		DatetimeToYearPlan2(inputValues, resultValues)
		return resultVector, nil
	}
}

func DateStringToYear(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	inputVector := vectors[0]
	resultType := types.Type{Oid: types.T_int64, Size: 8}
	inputValues := vector.MustStrCols(inputVector)
	if inputVector.IsConst() {
		if inputVector.ConstVectorIsNull() {
			return proc.AllocScalarNullVector(resultType), nil
		}
		resultValues := make([]int64, 1)
		DateStringToYearPlan2(inputValues, nil, resultValues)
		return vector.NewConstFixed(resultType, 1, resultValues[0], proc.Mp()), nil
	} else {
		resultVector, err := proc.AllocVectorOfRows(resultType, int64(len(inputValues)), inputVector.Nsp)
		if err != nil {
			return nil, err
		}
		resultValues := vector.MustTCols[int64](resultVector)
		DateStringToYearPlan2(inputValues, resultVector.Nsp, resultValues)
		return resultVector, nil
	}
}

func DateToYearPlan2(xs []types.Date, rs []int64) []int64 {
	for i, x := range xs {
		rs[i] = int64(x.Year())
	}
	return rs
}

func DatetimeToYearPlan2(xs []types.Datetime, rs []int64) []int64 {
	for i, x := range xs {
		rs[i] = int64(x.Year())
	}
	return rs
}

func DateStringToYearPlan2(xs []string, ns *nulls.Nulls, rs []int64) []int64 {
	for i, str := range xs {
		d, e := types.ParseDateCast(str)
		if e != nil {
			panic(e)
		}
		rs[i] = int64(d.Year())
	}
	return rs
}
