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
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/guest"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/host"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

/*
type TestCase struct {
	Function        func([]*vector.Vector, *process.Process) (*vector.Vector, error)
	InputVectors    []*vector.Vector
	ExpectedVectors []*vector.Vector
	ExpectError     bool
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
				return fmt.Errorf("unexpected error for vector %d", i)
			}
		}

		resultValues := resultVector.Col.([]outputT)
		expectedVector := testCase.ExpectedVectors[i]
		expectedValues := expectedVector.Col.([]outputT)
		for j, resultValue := range resultValues {
			if resultValue != expectedValues[j] {
				return fmt.Errorf("wrong result for vector %d", i)
			}
		}
	}
	return nil
}
*/

func NewTestProc() *process.Process {
	mmu := host.New(1024 * 1024)
	gm := guest.New(1024*1024, mmu)
	mp := mheap.New(gm)
	proc := process.New(mp)
	return proc
}

/*
func RunTestCaseChar(testCases []TestCase) error {
	proc := NewTestProc()
	for i, testCase := range testCases {
		resultVector, err := testCase.Function(testCase.InputVectors, proc)
		if err != nil {
			if testCase.ExpectError {
				break
			} else {
				return fmt.Errorf("unexpected error for vector %d", i)
			}
		}

		resultValues := resultVector.Col.(*types.Bytes)
		expectedVector := testCase.ExpectedVectors[i]
		expectedValues := expectedVector.Col.(*types.Bytes)
		for j := 0; j < len(resultValues.Lengths)-1; j++ {
			fmt.Println("...", resultValues.Offsets[j], string(resultValues.Data[resultValues.Offsets[j]:resultValues.Offsets[j]+resultValues.Lengths[j]]))
			expectedNull := nulls.Contains(expectedVector.Nsp, uint64(j))
			if expectedNull {
				if !nulls.Contains(resultVector.Nsp, uint64(j)) {
					return fmt.Errorf("wrong result for vector %d", i)
				}
			} else {
				if reflect.DeepEqual(resultValues.Data[resultValues.Offsets[j]:resultValues.Offsets[j]+resultValues.Lengths[j]], expectedValues.Data[expectedValues.Offsets[j]:expectedValues.Offsets[j]+expectedValues.Lengths[j]]) {
					return fmt.Errorf("wrong result for vector %d", i)
				}
			}
		}
	}
	return nil
}

*/

func makeInt64Vector(values []int64, nsp []uint64) *vector.Vector {
	vec := vector.New(types.Type{Oid: types.T_int64})
	vec.Col = values
	for _, n := range nsp {
		nulls.Add(vec.Nsp, n)
	}
	return vec
}

func makeUint64Vector(values []uint64, nsp []uint64) *vector.Vector {
	vec := vector.New(types.Type{Oid: types.T_uint64})
	vec.Col = values
	for _, n := range nsp {
		nulls.Add(vec.Nsp, n)
	}
	return vec
}

func makeFloat64Vector(values []float64, nsp []uint64) *vector.Vector {
	vec := vector.New(types.Type{Oid: types.T_float64})
	vec.Col = values
	for _, n := range nsp {
		nulls.Add(vec.Nsp, n)
	}
	return vec
}

/*
func makeCharVector(values []string, nsp []uint64) *vector.Vector {
	vec := vector.New(types.Type{Oid: types.T_char})
	colValue := new(types.Bytes)
	colValue.Offsets = make([]uint32, len(values))
	colValue.Lengths = make([]uint32, len(values))
	offset := uint32(0)
	for i, s := range values {
		lengthS := len(s)
		colValue.Data = append(colValue.Data, []byte(s)...)
		colValue.Offsets[i] = offset
		colValue.Lengths[i] = uint32(lengthS)
		offset += uint32(lengthS)
	}
	for _, n := range nsp {
		nulls.Add(vec.Nsp, n)
	}
	vec.Col = colValue
	return vec
}

*/

func TestSpaceUint64(t *testing.T) {
	inputVector := makeUint64Vector([]uint64{1, 2, 3, 0, 8000}, []uint64{4})
	proc := NewTestProc()
	output, err := SpaceUint64([]*vector.Vector{inputVector}, proc)
	require.NoError(t, err)
	result := output.Col.(*types.Bytes)
	// the correct result should be:
	// [32 32 32 32 32 32] [1 2 3 0 0] [0 1 3 6 6]
	fmt.Println(result.Data, result.Lengths, result.Offsets)

}

func TestSpaceInt64(t *testing.T) {
	inputVector := makeInt64Vector([]int64{1, 2, 3, 0, -1, 8000}, []uint64{4})
	proc := NewTestProc()
	output, err := SpaceInt64([]*vector.Vector{inputVector}, proc)
	require.NoError(t, err)
	result := output.Col.(*types.Bytes)
	// the correct result should be:
	// [32 32 32 32 32 32] [1 2 3 0 0 0] [0 1 3 6 6 6]
	fmt.Println(result.Data, result.Lengths, result.Offsets)

}

func TestSpaceFloat64(t *testing.T) {
	inputVector := makeFloat64Vector([]float64{1.4, 1.6, 3.3, 0, -1, 8000}, []uint64{4})
	proc := NewTestProc()
	output, err := SpaceFloat[float64]([]*vector.Vector{inputVector}, proc)
	require.NoError(t, err)
	result := output.Col.(*types.Bytes)
	// the correct result should be:
	// [32 32 32 32 32 32] [1 2 3 0 0 0] [0 1 3 6 6 6]
	fmt.Println(result.Data, result.Lengths, result.Offsets)

}
