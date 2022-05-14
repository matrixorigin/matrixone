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

package sqrt

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/nulls"

	"github.com/stretchr/testify/require"
)

func TestSqrtUint8(t *testing.T) {
	//Test values
	nums := []uint8{1, 3, 4, 6, 10, 11, 15, 16}
	//Predefined Correct Values
	sqrtNums := []float64{1, 1.7320508075688772, 2, 2.4494897427831779, 3.1622776601683795, 3.3166247903553998, 3.8729833462074170, 4}
	//Init a new variable
	newNums := make([]float64, len(nums))
	//Run sqrt function
	SqrtResult := SqrtUint8(nums, newNums)
	require.Equal(t, nulls.Any(SqrtResult.Nsp), false)

	for i := range SqrtResult.Result {
		require.Equal(t, sqrtNums[i], SqrtResult.Result[i])
	}
}

func TestSqrtUint16(t *testing.T) {
	//Test values
	nums := []uint16{1, 3, 4, 6, 10, 11, 15, 16}
	//Predefined Correct Values
	sqrtNums := []float64{1, 1.7320508075688772, 2, 2.4494897427831779, 3.1622776601683795, 3.3166247903553998, 3.8729833462074170, 4}
	//Init a new variable
	newNums := make([]float64, len(nums))
	//Run sqrt function
	SqrtResult := SqrtUint16(nums, newNums)
	require.Equal(t, nulls.Any(SqrtResult.Nsp), false)

	for i := range SqrtResult.Result {
		require.Equal(t, sqrtNums[i], SqrtResult.Result[i])
	}
}

func TestSqrtUint32(t *testing.T) {
	//Test values
	nums := []uint32{1, 3, 4, 6, 10, 11, 15, 16}
	//Predefined Correct Values
	sqrtNums := []float64{1, 1.7320508075688772, 2, 2.4494897427831779, 3.1622776601683795, 3.3166247903553998, 3.8729833462074170, 4}
	//Init a new variable
	newNums := make([]float64, len(nums))
	//Run sqrt function
	SqrtResult := SqrtUint32(nums, newNums)
	require.Equal(t, nulls.Any(SqrtResult.Nsp), false)

	for i := range SqrtResult.Result {
		require.Equal(t, sqrtNums[i], SqrtResult.Result[i])
	}
}

func TestSqrtUint64(t *testing.T) {
	//Test values
	nums := []uint64{1, 3, 4, 6, 10, 11, 15, 16}
	//Predefined Correct Values
	sqrtNums := []float64{1, 1.7320508075688772, 2, 2.4494897427831779, 3.1622776601683795, 3.3166247903553998, 3.8729833462074170, 4}
	//Init a new variable
	newNums := make([]float64, len(nums))
	//Run sqrt function
	SqrtResult := SqrtUint64(nums, newNums)
	require.Equal(t, nulls.Any(SqrtResult.Nsp), false)

	for i := range SqrtResult.Result {
		require.Equal(t, sqrtNums[i], SqrtResult.Result[i])
	}
}

func TestSqrtInt8(t *testing.T) {
	//Test values
	nums := []int8{1, 3, 4, 6, 10, 11, 15, 16}
	//Predefined Correct Values
	sqrtNums := []float64{1, 1.7320508075688772, 2, 2.4494897427831779, 3.1622776601683795, 3.3166247903553998, 3.8729833462074170, 4}
	//Init a new variable
	newNums := make([]float64, len(nums))
	//Run sqrt function
	SqrtResult := SqrtInt8(nums, newNums)
	require.Equal(t, nulls.Any(SqrtResult.Nsp), false)

	for i := range SqrtResult.Result {
		require.Equal(t, sqrtNums[i], SqrtResult.Result[i])
	}
}

func TestSqrtInt16(t *testing.T) {
	//Test values
	nums := []int16{1, 3, 4, 6, 10, 11, 15, 16}
	//Predefined Correct Values
	sqrtNums := []float64{1, 1.7320508075688772, 2, 2.4494897427831779, 3.1622776601683795, 3.3166247903553998, 3.8729833462074170, 4}
	//Init a new variable
	newNums := make([]float64, len(nums))
	//Run sqrt function
	SqrtResult := SqrtInt16(nums, newNums)
	require.Equal(t, nulls.Any(SqrtResult.Nsp), false)

	for i := range SqrtResult.Result {
		require.Equal(t, sqrtNums[i], SqrtResult.Result[i])
	}
}

func TestSqrtInt32(t *testing.T) {
	//Test values
	nums := []int32{1, 3, 4, 6, 10, 11, 15, 16}
	//Predefined Correct Values
	sqrtNums := []float64{1, 1.7320508075688772, 2, 2.4494897427831779, 3.1622776601683795, 3.3166247903553998, 3.8729833462074170, 4}
	//Init a new variable
	newNums := make([]float64, len(nums))
	//Run sqrt function
	SqrtResult := SqrtInt32(nums, newNums)
	require.Equal(t, nulls.Any(SqrtResult.Nsp), false)

	for i := range SqrtResult.Result {
		require.Equal(t, sqrtNums[i], SqrtResult.Result[i])
	}
}

func TestSqrtInt64(t *testing.T) {
	//Test values
	nums := []int64{1, 3, 4, 6, 10, 11, 15, 16}
	//Predefined Correct Values
	sqrtNums := []float64{1, 1.7320508075688772, 2, 2.4494897427831779, 3.1622776601683795, 3.3166247903553998, 3.8729833462074170, 4}
	//Init a new variable
	newNums := make([]float64, len(nums))
	//Run sqrt function
	SqrtResult := SqrtInt64(nums, newNums)
	require.Equal(t, nulls.Any(SqrtResult.Nsp), false)

	for i := range SqrtResult.Result {
		require.Equal(t, sqrtNums[i], SqrtResult.Result[i])
	}
}

func TestSqrtfloat32(t *testing.T) {
	//Test values
	nums := []float32{1.34, 4.56, 7.34, 9.99, 16.00, 25.14}
	//Predefined Correct Values
	sqrtNums := []float64{1.1575837046963822, 2.1354156370082946, 2.7092434649894224, 3.1606960896483174, 4, 5.0139803938236973}
	//Init a new variable
	newNums := make([]float64, len(nums))
	//Run sqrt function
	SqrtResult := SqrtFloat32(nums, newNums)
	require.Equal(t, nulls.Any(SqrtResult.Nsp), false)

	for i := range SqrtResult.Result {
		require.Equal(t, sqrtNums[i], SqrtResult.Result[i])
	}
}

func TestSqrtfloat64(t *testing.T) {
	//Test values
	nums := []float64{1.34, 4.56, 7.34, 9.99, 16.00, 25.14}
	//Predefined Correct Values
	sqrtNums := []float64{1.1575836902790226, 2.1354156504062622, 2.7092434368288134, 3.1606961258558215, 4, 5.0139804546886699}
	//Init a new variable
	newNums := make([]float64, len(nums))
	//Run sqrt function
	SqrtResult := SqrtFloat64(nums, newNums)
	require.Equal(t, nulls.Any(SqrtResult.Nsp), false)

	for i := range SqrtResult.Result {
		require.Equal(t, sqrtNums[i], SqrtResult.Result[i])
	}
}
