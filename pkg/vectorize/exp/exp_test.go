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

package exp

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/stretchr/testify/require"
)

func TestExpUint8(t *testing.T) {
	//Test values
	nums := []uint8{1, 2, 3, 8, 10, 12, 40}
	//Predefined Correct Values
	expNums := []float64{2.718281828459045, 7.38905609893065, 20.085536923187668, 2980.9579870417283, 22026.465794806718, 162754.79141900392, 2.3538526683702e+17}
	//Init a new variable
	newNums := make([]float64, len(nums))
	//Run abs function
	expResult := expUint8(nums, newNums)
	require.Equal(t, nulls.Any(expResult.Nsp), false)

	for i := range expResult.Result {
		require.Equal(t, expNums[i], expResult.Result[i])
	}
}

func TestExpUint16(t *testing.T) {
	//Test values
	nums := []uint16{1, 2, 3, 8, 10, 12, 40}
	//Predefined Correct Values
	expNums := []float64{2.718281828459045, 7.38905609893065, 20.085536923187668, 2980.9579870417283, 22026.465794806718, 162754.79141900392, 2.3538526683702e+17}
	//Init a new variable
	newNums := make([]float64, len(nums))
	//Run abs function
	ExpResult := expUint16(nums, newNums)
	require.Equal(t, nulls.Any(ExpResult.Nsp), false)

	for i := range ExpResult.Result {
		require.Equal(t, expNums[i], ExpResult.Result[i])
	}
}

func TestExpUint32(t *testing.T) {
	//Test values
	nums := []uint32{1, 2, 3, 8, 10, 12, 40}
	//Predefined Correct Values
	expNums := []float64{2.718281828459045, 7.38905609893065, 20.085536923187668, 2980.9579870417283, 22026.465794806718, 162754.79141900392, 2.3538526683702e+17}
	//Init a new variable
	newNums := make([]float64, len(nums))
	//Run abs function
	ExpResult := expUint32(nums, newNums)
	require.Equal(t, nulls.Any(ExpResult.Nsp), false)

	for i := range ExpResult.Result {
		require.Equal(t, expNums[i], ExpResult.Result[i])
	}
}

func TestExpUint64(t *testing.T) {
	//Test values
	nums := []uint64{1, 2, 3, 8, 10, 12, 40}
	//Predefined Correct Values
	expNums := []float64{2.718281828459045, 7.38905609893065, 20.085536923187668, 2980.9579870417283, 22026.465794806718, 162754.79141900392, 2.3538526683702e+17}
	//Init a new variable
	newNums := make([]float64, len(nums))
	//Run abs function
	ExpResult := expUint64(nums, newNums)
	require.Equal(t, nulls.Any(ExpResult.Nsp), false)

	for i := range ExpResult.Result {
		require.Equal(t, expNums[i], ExpResult.Result[i])
	}
}

func TestExpint8(t *testing.T) {
	//Test values
	nums := []int8{1, 2, 3, 8, 10, 12, 40}
	//Predefined Correct Values
	expNums := []float64{2.718281828459045, 7.38905609893065, 20.085536923187668, 2980.9579870417283, 22026.465794806718, 162754.79141900392, 2.3538526683702e+17}
	//Init a new variable
	newNums := make([]float64, len(nums))
	//Run abs function
	ExpResult := expInt8(nums, newNums)
	require.Equal(t, nulls.Any(ExpResult.Nsp), false)

	for i := range ExpResult.Result {
		require.Equal(t, expNums[i], ExpResult.Result[i])
	}
}

func TestExpint16(t *testing.T) {
	//Test values
	nums := []int16{1, 2, 3, 8, 10, 12, 40}
	//Predefined Correct Values
	expNums := []float64{2.718281828459045, 7.38905609893065, 20.085536923187668, 2980.9579870417283, 22026.465794806718, 162754.79141900392, 2.3538526683702e+17}
	//Init a new variable
	newNums := make([]float64, len(nums))
	//Run abs function
	ExpResult := expInt16(nums, newNums)
	require.Equal(t, nulls.Any(ExpResult.Nsp), false)

	for i := range ExpResult.Result {
		require.Equal(t, expNums[i], ExpResult.Result[i])
	}
}

func TestExpint32(t *testing.T) {
	//Test values
	nums := []int32{1, 2, 3, 8, 10, 12, 40}
	//Predefined Correct Values
	expNums := []float64{2.718281828459045, 7.38905609893065, 20.085536923187668, 2980.9579870417283, 22026.465794806718, 162754.79141900392, 2.3538526683702e+17}
	//Init a new variable
	newNums := make([]float64, len(nums))
	//Run abs function
	ExpResult := expInt32(nums, newNums)
	require.Equal(t, nulls.Any(ExpResult.Nsp), false)

	for i := range ExpResult.Result {
		require.Equal(t, expNums[i], ExpResult.Result[i])
	}
}

func TestExpint64(t *testing.T) {
	//Test values
	nums := []int64{1, 2, 3, 8, 10, 12, 40}
	//Predefined Correct Values
	expNums := []float64{2.718281828459045, 7.38905609893065, 20.085536923187668, 2980.9579870417283, 22026.465794806718, 162754.79141900392, 2.3538526683702e+17}
	//Init a new variable
	newNums := make([]float64, len(nums))
	//Run abs function
	ExpResult := expInt64(nums, newNums)
	require.Equal(t, nulls.Any(ExpResult.Nsp), false)

	for i := range ExpResult.Result {
		require.Equal(t, expNums[i], ExpResult.Result[i])
	}
}

func TestExpfloat32(t *testing.T) {
	//Test values
	nums := []float32{1.234, 2.345, 3.456, 8.901234, 10.0191, 12.454, 40.12312}
	//Predefined Correct Values
	expNums := []float64{3.4349417494230234, 10.433273026047246, 31.68996564623823, 7341.024379390792, 22451.218979158784, 256273.48987091612, 2.662253044114003e+17}
	//Init a new variable
	newNums := make([]float64, len(nums))
	//Run abs function
	ExpResult := expFloat32(nums, newNums)
	require.Equal(t, nulls.Any(ExpResult.Nsp), false)

	for i := range ExpResult.Result {
		require.Equal(t, expNums[i], ExpResult.Result[i])
	}
}

func TestExpfloat64(t *testing.T) {
	//Test values
	nums := []float64{1.234, 2.345, 3.456, 8.901234, 10.0191, 12.454, 40.12312}
	//Predefined Correct Values
	expNums := []float64{3.43494186080076, 10.433272727548918, 31.689962805379164, 7341.026779203601, 22451.21473118684, 256273.36864782812, 2.662254763269661e+17}
	//Init a new variable
	newNums := make([]float64, len(nums))
	//Run abs function
	ExpResult := expFloat64(nums, newNums)
	require.Equal(t, nulls.Any(ExpResult.Nsp), false)

	for i := range ExpResult.Result {
		require.Equal(t, expNums[i], ExpResult.Result[i])
	}
}
