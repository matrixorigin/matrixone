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

package builtin

import (
	"errors"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vectorize/eq"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/guest"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/host"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"golang.org/x/exp/constraints"
)

// todo(broc): when vector.New generics are ready, change this to generic
func MakeInt8Vector(vs []int8) *vector.Vector {
	ret := vector.New(types.Type{Oid: types.T_int8, Size: 1})
	vector.SetCol(ret, vs)
	return ret
}

func MakeInt64Vector(vs []int64) *vector.Vector {
	ret := vector.New(types.Type{Oid: types.T_int64, Size: 8})
	vector.SetCol(ret, vs)
	return ret
}

func MakeUInt64Vector(vs []uint64) *vector.Vector {
	ret := vector.New(types.Type{Oid: types.T_uint64, Size: 8})
	vector.SetCol(ret, vs)
	return ret
}

func MakeFloat64Vector(vs []float64) *vector.Vector {
	ret := vector.New(types.Type{Oid: types.T_float64, Size: 8})
	vector.SetCol(ret, vs)
	return ret
}

type TestCase struct {
	Function        func([]*vector.Vector, *process.Process) (*vector.Vector, error)
	InputVectors    []*vector.Vector
	ExpectedVectors []*vector.Vector
	ExpectError     bool
}

func RunTestCaseFloat64(testCases []TestCase) error {
	mmu := host.New(1024 * 1024)
	gm := guest.New(1024*1024, mmu)
	mp := mheap.New(gm)
	proc := process.New(mp)
	for i, testCase := range testCases {
		resultVector, err := testCase.Function(testCase.InputVectors, proc)
		if err != nil {
			if testCase.ExpectError {
				break
			} else {
				return errors.New(fmt.Sprintf("unexpected error for vector %d", i))
			}
		}

		resultValues := resultVector.Col.([]float64)
		expectedVector := testCase.ExpectedVectors[i]
		expectedValues := expectedVector.Col.([]float64)
		fmt.Println(expectedValues, resultValues)
		for j, resultValue := range resultValues {
			if !eq.Float64Equal(resultValue, expectedValues[j]) {
				return errors.New(fmt.Sprintf("wrong result for vector %d", i))
			}
		}
	}
	return nil
}

func RunTestCaseInteger[outputT constraints.Integer](testCases []TestCase) error {
	mmu := host.New(1024 * 1024)
	gm := guest.New(1024*1024, mmu)
	mp := mheap.New(gm)
	proc := process.New(mp)
	for i, testCase := range testCases {
		resultVector, err := testCase.Function(testCase.InputVectors, proc)
		if err != nil {
			if testCase.ExpectError {
				break
			} else {
				return errors.New(fmt.Sprintf("unexpected error for vector %d", i))
			}
		}

		resultValues := resultVector.Col.([]outputT)
		fmt.Println(resultValues)
		expectedVector := testCase.ExpectedVectors[i]
		expectedValues := expectedVector.Col.([]outputT)
		for j, resultValue := range resultValues {
			if resultValue != expectedValues[j] {
				return errors.New(fmt.Sprintf("wrong result for vector %d", i))
			}
		}
	}
	return nil
}
