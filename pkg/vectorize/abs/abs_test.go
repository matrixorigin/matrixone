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

package abs

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAbsUint8(t *testing.T) {
	//Test values
	nums := []uint8{1, 2, 3, 4, 5, 6, 7, 8, 9, 8, 7, 6, 5, 4, 3, 2, 1, 11, 2, 33, 22, 55, 44, 33, 22}
	//Predefined Correct Values
	absNums := []uint8{1, 2, 3, 4, 5, 6, 7, 8, 9, 8, 7, 6, 5, 4, 3, 2, 1, 11, 2, 33, 22, 55, 44, 33, 22}

	//Init a new variable
	newNums := make([]uint8, len(nums))
	//Run abs function
	newNums = absUint8(nums, newNums)

	for i := range newNums {
		require.Equal(t, absNums[i], newNums[i])
	}
}

func TestAbsUint16(t *testing.T) {
	//Test values
	nums := []uint16{1, 2, 3, 4, 5, 6, 7, 8, 9, 8, 7, 6, 5, 4, 3, 2, 1, 11, 2, 33, 22, 55, 44, 33, 22}
	//Predefined Correct Values
	absNums := []uint16{1, 2, 3, 4, 5, 6, 7, 8, 9, 8, 7, 6, 5, 4, 3, 2, 1, 11, 2, 33, 22, 55, 44, 33, 22}

	//Init a new variable
	newNums := make([]uint16, len(nums))
	//Run abs function
	newNums = absUint16(nums, newNums)

	for i := range newNums {
		require.Equal(t, absNums[i], newNums[i])
	}
}

func TestAbsUint32(t *testing.T) {
	//Test values
	nums := []uint32{1, 2, 3, 4, 5, 6, 7, 8, 9, 8, 7, 6, 5, 4, 3, 2, 1, 11, 2, 33, 22, 55, 44, 33, 22}
	//Predefined Correct Values
	absNums := []uint32{1, 2, 3, 4, 5, 6, 7, 8, 9, 8, 7, 6, 5, 4, 3, 2, 1, 11, 2, 33, 22, 55, 44, 33, 22}

	//Init a new variable
	newNums := make([]uint32, len(nums))
	//Run abs function
	newNums = absUint32(nums, newNums)

	for i := range newNums {
		require.Equal(t, absNums[i], newNums[i])
	}
}

func TestAbsUint64(t *testing.T) {
	//Test values
	nums := []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 8, 7, 6, 5, 4, 3, 2, 1, 11, 2, 33, 22, 55, 44, 33, 22}
	//Predefined Correct Values
	absNums := []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 8, 7, 6, 5, 4, 3, 2, 1, 11, 2, 33, 22, 55, 44, 33, 22}

	//Init a new variable
	newNums := make([]uint64, len(nums))
	//Run abs function
	newNums = absUint64(nums, newNums)

	for i := range newNums {
		require.Equal(t, absNums[i], newNums[i])
	}
}

func TestAbsInt8(t *testing.T) {
	//Test values
	nums := []int8{-11, 22, -33, 44, -55, -66, 77, -88, 99}
	//Predefined Correct Values
	absNums := []int8{11, 22, 33, 44, 55, 66, 77, 88, 99}

	//Init a new variable
	newNums := make([]int8, len(nums))
	//Run abs function
	newNums = absInt8(nums, newNums)

	for i := range newNums {
		require.Equal(t, absNums[i], newNums[i])
	}
}

func TestAbsInt16(t *testing.T) {
	//Test values
	nums := []int16{-11, 22, -33, 44, -55, -66, 77, -88, 99}
	//Predefined Correct Values
	absNums := []int16{11, 22, 33, 44, 55, 66, 77, 88, 99}

	//Init a new variable
	newNums := make([]int16, len(nums))
	//Run abs function
	newNums = absInt16(nums, newNums)

	for i := range newNums {
		require.Equal(t, absNums[i], newNums[i])
	}
}

func TestAbsInt32(t *testing.T) {
	//Test values
	nums := []int32{-111, 222, -333, 444, -555, 666, -777, 888, -99999}
	//Predefined Correct Values
	absNums := []int32{111, 222, 333, 444, 555, 666, 777, 888, 99999}

	//Init a new variable
	newNums := make([]int32, len(nums))
	//Run abs function
	newNums = absInt32(nums, newNums)

	for i := range newNums {
		require.Equal(t, absNums[i], newNums[i])
	}
}

func TestAbsInt64(t *testing.T) {
	//Test values
	nums := []int64{-111, 222, -333, 444, -555, 666, -777, 888, -99999, -987654321}
	//Predefined Correct Values
	absNums := []int64{111, 222, 333, 444, 555, 666, 777, 888, 99999, 987654321}

	//Init a new variable
	newNums := make([]int64, len(nums))
	//Run abs function
	newNums = absInt64(nums, newNums)

	for i := range newNums {
		require.Equal(t, absNums[i], newNums[i])
	}
}

func TestAbsFloat32(t *testing.T) {
	//Test values
	nums := []float32{1.5, -1.5, 2.5, -2.5, 1.2, 12.3, 123.4, 1234.5, 12345.6, 1234.567, -1.2, -12.3, -123.4, -1234.5, -12345.6}
	//Predefined Correct Values
	absNums := []float32{1.5, 1.5, 2.5, 2.5, 1.2, 12.3, 123.4, 1234.5, 12345.6, 1234.567, 1.2, 12.3, 123.4, 1234.5, 12345.6}

	//Init a new variable
	newNums := make([]float32, len(nums))
	//Run abs function
	newNums = absFloat32(nums, newNums)

	for i := range newNums {
		require.Equal(t, absNums[i], newNums[i])
	}
}

func TestAbsFloat64(t *testing.T) {
	//Test values
	nums := []float64{1.5, -1.5, 2.5, -2.5, 1.2, 12.3, 123.4, 1234.5, 12345.6, 1234.567, -1.2, -12.3, -123.4, -1234.5, -12345.6}
	//Predefined Correct Values
	absNums := []float64{1.5, 1.5, 2.5, 2.5, 1.2, 12.3, 123.4, 1234.5, 12345.6, 1234.567, 1.2, 12.3, 123.4, 1234.5, 12345.6}

	//Init a new variable
	newNums := make([]float64, len(nums))
	//Run abs function
	newNums = absFloat64(nums, newNums)

	for i := range newNums {
		require.Equal(t, absNums[i], newNums[i])
	}
}
