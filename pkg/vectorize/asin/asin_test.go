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

package asin

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/stretchr/testify/require"
)

func TestAsinFloat32(t *testing.T) {
	//Test values
	nums := []float32{0, 1}
	//Predefined Correct Values
	asinNums := []float64{0, 1.5707963267948966}
	//Init a new variable
	newNums := make([]float64, len(nums))
	//Run asin function
	AsinResult := asinFloat32(nums, newNums)
	require.Equal(t, nulls.Any(AsinResult.Nsp), false)

	for i := range AsinResult.Result {
		require.Equal(t, asinNums[i], AsinResult.Result[i])
	}
}

func TestAsinFloat64(t *testing.T) {
	//Test values
	nums := []float64{0, 1}
	//Predefined Correct Values
	asinNums := []float64{0, 1.5707963267948966}
	//Init a new variable
	newNums := make([]float64, len(nums))
	//Run asin function
	AsinResult := asinFloat64(nums, newNums)
	require.Equal(t, nulls.Any(AsinResult.Nsp), false)

	for i := range AsinResult.Result {
		require.Equal(t, asinNums[i], AsinResult.Result[i])
	}
}

func TestAsinUint8(t *testing.T) {
	//Test values
	nums := []uint8{0, 1}
	//Predefined Correct Values
	asinNums := []float64{0, 1.5707963267948966}
	//Init a new variable
	newNums := make([]float64, len(nums))
	//Run asin function
	AsinResult := asinUint8(nums, newNums)
	require.Equal(t, nulls.Any(AsinResult.Nsp), false)

	for i := range AsinResult.Result {
		require.Equal(t, asinNums[i], AsinResult.Result[i])
	}
}

func TestAsinUint16(t *testing.T) {
	//Test values
	nums := []uint16{0, 1}
	//Predefined Correct Values
	asinNums := []float64{0, 1.5707963267948966}
	//Init a new variable
	newNums := make([]float64, len(nums))
	//Run asin function
	AsinResult := asinUint16(nums, newNums)
	require.Equal(t, nulls.Any(AsinResult.Nsp), false)

	for i := range AsinResult.Result {
		require.Equal(t, asinNums[i], AsinResult.Result[i])
	}
}

func TestAsinUint32(t *testing.T) {
	//Test values
	nums := []uint32{0, 1}
	//Predefined Correct Values
	asinNums := []float64{0, 1.5707963267948966}
	//Init a new variable
	newNums := make([]float64, len(nums))
	//Run asin function
	AsinResult := asinUint32(nums, newNums)
	require.Equal(t, nulls.Any(AsinResult.Nsp), false)

	for i := range AsinResult.Result {
		require.Equal(t, asinNums[i], AsinResult.Result[i])
	}
}

func TestAsinUint64(t *testing.T) {
	//Test values
	nums := []uint64{0, 1}
	//Predefined Correct Values
	asinNums := []float64{0, 1.5707963267948966}
	//Init a new variable
	newNums := make([]float64, len(nums))
	//Run asin function
	AsinResult := asinUint64(nums, newNums)
	require.Equal(t, nulls.Any(AsinResult.Nsp), false)

	for i := range AsinResult.Result {
		require.Equal(t, asinNums[i], AsinResult.Result[i])
	}
}

func TestAsinInt8(t *testing.T) {
	//Test values
	nums := []int8{0, 1}
	//Predefined Correct Values
	asinNums := []float64{0, 1.5707963267948966}
	//Init a new variable
	newNums := make([]float64, len(nums))
	//Run asin function
	AsinResult := asinInt8(nums, newNums)
	require.Equal(t, nulls.Any(AsinResult.Nsp), false)

	for i := range AsinResult.Result {
		require.Equal(t, asinNums[i], AsinResult.Result[i])
	}
}

func TestAsinInt16(t *testing.T) {
	//Test values
	nums := []int16{0, 1}
	//Predefined Correct Values
	asinNums := []float64{0, 1.5707963267948966}
	//Init a new variable
	newNums := make([]float64, len(nums))
	//Run asin function
	AsinResult := asinInt16(nums, newNums)
	require.Equal(t, nulls.Any(AsinResult.Nsp), false)

	for i := range AsinResult.Result {
		require.Equal(t, asinNums[i], AsinResult.Result[i])
	}
}

func TestAsinInt32(t *testing.T) {
	//Test values
	nums := []int32{0, 1}
	//Predefined Correct Values
	asinNums := []float64{0, 1.5707963267948966}
	//Init a new variable
	newNums := make([]float64, len(nums))
	//Run asin function
	AsinResult := asinInt32(nums, newNums)
	require.Equal(t, nulls.Any(AsinResult.Nsp), false)

	for i := range AsinResult.Result {
		require.Equal(t, asinNums[i], AsinResult.Result[i])
	}
}

func TestAsinInt64(t *testing.T) {
	//Test values
	nums := []int64{0, 1}
	//Predefined Correct Values
	asinNums := []float64{0, 1.5707963267948966}
	//Init a new variable
	newNums := make([]float64, len(nums))
	//Run asin function
	AsinResult := asinInt64(nums, newNums)
	require.Equal(t, nulls.Any(AsinResult.Nsp), false)

	for i := range AsinResult.Result {
		require.Equal(t, asinNums[i], AsinResult.Result[i])
	}
}
