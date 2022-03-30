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

package acos

import (
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestAcosFloat32(t *testing.T) {
	//Test values
	nums := []float32{0, 1}
	//Predefined Correct Values
	acosNums := []float64{1.5707963267948966, 0}
	//Init a new variable
	newNums := make([]float64, len(nums))
	//Run acos function
	AcosResult := acosFloat32(nums, newNums)
	require.Equal(t, nulls.Any(AcosResult.Nsp), false)

	for i := range AcosResult.Result {
		require.Equal(t, acosNums[i], AcosResult.Result[i])
	}
}

func TestAcosFloat64(t *testing.T) {
	//Test values
	nums := []float64{0, 1}
	//Predefined Correct Values
	acosNums := []float64{1.5707963267948966, 0}
	//Init a new variable
	newNums := make([]float64, len(nums))
	//Run acos function
	AcosResult := acosFloat64(nums, newNums)
	require.Equal(t, nulls.Any(AcosResult.Nsp), false)

	for i := range AcosResult.Result {
		require.Equal(t, acosNums[i], AcosResult.Result[i])
	}
}

func TestAcosUint8(t *testing.T) {
	//Test values
	nums := []uint8{0, 1}
	//Predefined Correct Values
	acosNums := []float64{1.5707963267948966, 0}
	//Init a new variable
	newNums := make([]float64, len(nums))
	//Run acos function
	AcosResult := acosUint8(nums, newNums)
	require.Equal(t, nulls.Any(AcosResult.Nsp), false)

	for i := range AcosResult.Result {
		require.Equal(t, acosNums[i], AcosResult.Result[i])
	}
}

func TestAcosUint16(t *testing.T) {
	//Test values
	nums := []uint16{0, 1}
	//Predefined Correct Values
	acosNums := []float64{1.5707963267948966, 0}
	//Init a new variable
	newNums := make([]float64, len(nums))
	//Run acos function
	AcosResult := acosUint16(nums, newNums)
	require.Equal(t, nulls.Any(AcosResult.Nsp), false)

	for i := range AcosResult.Result {
		require.Equal(t, acosNums[i], AcosResult.Result[i])
	}
}

func TestAcosUint32(t *testing.T) {
	//Test values
	nums := []uint32{0, 1}
	//Predefined Correct Values
	acosNums := []float64{1.5707963267948966, 0}
	//Init a new variable
	newNums := make([]float64, len(nums))
	//Run acos function
	AcosResult := acosUint32(nums, newNums)
	require.Equal(t, nulls.Any(AcosResult.Nsp), false)

	for i := range AcosResult.Result {
		require.Equal(t, acosNums[i], AcosResult.Result[i])
	}
}

func TestAcosUint64(t *testing.T) {
	//Test values
	nums := []uint64{0, 1}
	//Predefined Correct Values
	acosNums := []float64{1.5707963267948966, 0}
	//Init a new variable
	newNums := make([]float64, len(nums))
	//Run acos function
	AcosResult := acosUint64(nums, newNums)
	require.Equal(t, nulls.Any(AcosResult.Nsp), false)

	for i := range AcosResult.Result {
		require.Equal(t, acosNums[i], AcosResult.Result[i])
	}
}

func TestAcosInt8(t *testing.T) {
	//Test values
	nums := []int8{0, 1}
	//Predefined Correct Values
	acosNums := []float64{1.5707963267948966, 0}
	//Init a new variable
	newNums := make([]float64, len(nums))
	//Run acos function
	AcosResult := acosInt8(nums, newNums)
	require.Equal(t, nulls.Any(AcosResult.Nsp), false)

	for i := range AcosResult.Result {
		require.Equal(t, acosNums[i], AcosResult.Result[i])
	}
}

func TestAcosInt16(t *testing.T) {
	//Test values
	nums := []int16{0, 1}
	//Predefined Correct Values
	acosNums := []float64{1.5707963267948966, 0}
	//Init a new variable
	newNums := make([]float64, len(nums))
	//Run acos function
	AcosResult := acosInt16(nums, newNums)
	require.Equal(t, nulls.Any(AcosResult.Nsp), false)

	for i := range AcosResult.Result {
		require.Equal(t, acosNums[i], AcosResult.Result[i])
	}
}

func TestAcosInt32(t *testing.T) {
	//Test values
	nums := []int32{0, 1}
	//Predefined Correct Values
	acosNums := []float64{1.5707963267948966, 0}
	//Init a new variable
	newNums := make([]float64, len(nums))
	//Run acos function
	AcosResult := acosInt32(nums, newNums)
	require.Equal(t, nulls.Any(AcosResult.Nsp), false)

	for i := range AcosResult.Result {
		require.Equal(t, acosNums[i], AcosResult.Result[i])
	}
}

func TestAcosInt64(t *testing.T) {
	//Test values
	nums := []int64{0, 1}
	//Predefined Correct Values
	acosNums := []float64{1.5707963267948966, 0}
	//Init a new variable
	newNums := make([]float64, len(nums))
	//Run acos function
	AcosResult := acosInt64(nums, newNums)
	require.Equal(t, nulls.Any(AcosResult.Nsp), false)

	for i := range AcosResult.Result {
		require.Equal(t, acosNums[i], AcosResult.Result[i])
	}
}
