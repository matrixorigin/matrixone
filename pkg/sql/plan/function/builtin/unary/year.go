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
	resultElementSize := int(resultType.Size)
	inputValues := vector.MustTCols[types.Date](inputVector)
	if inputVector.IsScalar() {
		if inputVector.ConstVectorIsNull() {
			return proc.AllocScalarNullVector(resultType), nil
		}
		resultVector := vector.NewConst(resultType, 1)
		resultValues := make([]int64, 1)
		DateToYearPlan2(inputValues, resultValues)
		// resultValues2 := make([]int64, 1)
		// resultValues2[0] = int64(resultValues[0])
		vector.SetCol(resultVector, resultValues)
		return resultVector, nil
	} else {
		resultVector, err := proc.AllocVector(resultType, int64(resultElementSize*len(inputValues)))
		if err != nil {
			return nil, err
		}
		resultValues := types.DecodeInt64Slice(resultVector.Data)
		resultValues = resultValues[:len(inputValues)]
		nulls.Set(resultVector.Nsp, inputVector.Nsp)
		DateToYearPlan2(inputValues, resultValues)
		// resultValues2 := make([]int64, len(resultValues))
		// for i, x := range resultValues {
		// 	resultValues2[i] = int64(x)
		// }
		vector.SetCol(resultVector, resultValues)
		return resultVector, nil
	}
}

func DatetimeToYear(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	inputVector := vectors[0]
	resultType := types.Type{Oid: types.T_int64, Size: 8}
	resultElementSize := int(resultType.Size)
	inputValues := vector.MustTCols[types.Datetime](inputVector)
	if inputVector.IsScalar() {
		if inputVector.ConstVectorIsNull() {
			return proc.AllocScalarNullVector(resultType), nil
		}
		resultVector := vector.NewConst(resultType, 1)
		resultValues := make([]int64, 1)
		DatetimeToYearPlan2(inputValues, resultValues)
		// resultValues2 := make([]int64, 1)
		// resultValues2[0] = int64(resultValues[0])
		vector.SetCol(resultVector, resultValues)
		return resultVector, nil
	} else {
		resultVector, err := proc.AllocVector(resultType, int64(resultElementSize*len(inputValues)))
		if err != nil {
			return nil, err
		}
		// resultValues := make([]uint16, len(inputValues))
		resultValues := types.DecodeInt64Slice(resultVector.Data)
		resultValues = resultValues[:len(inputValues)]
		nulls.Set(resultVector.Nsp, inputVector.Nsp)
		DatetimeToYearPlan2(inputValues, resultValues)
		// resultValues2 := make([]int64, len(resultValues))
		// for i, x := range resultValues {
		// 	resultValues2[i] = int64(x)
		// }
		vector.SetCol(resultVector, resultValues)
		return resultVector, nil
	}
}

func DateStringToYear(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	inputVector := vectors[0]
	resultType := types.Type{Oid: types.T_int64, Size: 8}
	resultElementSize := int(resultType.Size)
	inputValues := vector.MustBytesCols(inputVector)
	if inputVector.IsConst {
		if inputVector.ConstVectorIsNull() {
			return proc.AllocScalarNullVector(resultType), nil
		}
		resultVector := vector.NewConst(resultType, 1)
		resultValues := make([]int64, 1)
		DateStringToYearPlan2(inputValues, resultVector.Nsp, resultValues)
		// resultValues2 := make([]int64, 1)
		// resultValues2[0] = int64(resultValues[0])
		vector.SetCol(resultVector, resultValues)
		return resultVector, nil
	} else {
		resultVector, err := proc.AllocVector(resultType, int64(resultElementSize*len(inputValues.Lengths)))
		if err != nil {
			return nil, err
		}
		resultValues := types.DecodeInt64Slice(resultVector.Data)
		resultValues = resultValues[:len(inputValues.Lengths)]
		nulls.Set(resultVector.Nsp, inputVector.Nsp)
		DateStringToYearPlan2(inputValues, resultVector.Nsp, resultValues)
		// resultValues2 := make([]int64, len(resultValues))
		// for i, x := range resultValues {
		// 	resultValues2[i] = int64(x)
		// }
		vector.SetCol(resultVector, resultValues)
		return resultVector, nil
	}
}

// vectorize year and toYear function
var (
	DateToYearPlan2       func([]types.Date, []int64) []int64
	DatetimeToYearPlan2   func([]types.Datetime, []int64) []int64
	DateStringToYearPlan2 func(*types.Bytes, *nulls.Nulls, []int64) []int64
)

func init() {
	DateToYearPlan2 = dateToYearPlan2
	DatetimeToYearPlan2 = datetimeToYearPlan2
	DateStringToYearPlan2 = dateStringToYearPlan2
}

func dateToYear(xs []types.Date, rs []uint16) []uint16 {
	for i, x := range xs {
		rs[i] = x.Year()
	}
	return rs
}

func datetimeToYear(xs []types.Datetime, rs []uint16) []uint16 {
	for i, x := range xs {
		rs[i] = x.Year()
	}
	return rs
}

func dateStringToYear(xs *types.Bytes, ns *nulls.Nulls, rs []uint16) []uint16 {
	for i := range xs.Lengths {
		str := string(xs.Get(int64(i)))
		d, e := types.ParseDateCast(str)
		if e != nil {
			// set null
			nulls.Add(ns, uint64(i))
			rs[i] = 0
			continue
		}
		rs[i] = d.Year()
	}
	return rs
}

func dateToYearPlan2(xs []types.Date, rs []int64) []int64 {
	for i, x := range xs {
		rs[i] = int64(x.Year())
	}
	return rs
}

func datetimeToYearPlan2(xs []types.Datetime, rs []int64) []int64 {
	for i, x := range xs {
		rs[i] = int64(x.Year())
	}
	return rs
}

func dateStringToYearPlan2(xs *types.Bytes, ns *nulls.Nulls, rs []int64) []int64 {
	for i := range xs.Lengths {
		str := string(xs.Get(int64(i)))
		d, e := types.ParseDateCast(str)
		if e != nil {
			// set null
			nulls.Add(ns, uint64(i))
			rs[i] = 0
			continue
		}
		rs[i] = int64(d.Year())
	}
	return rs
}
