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

package sign

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSignUint8(t *testing.T) {
	//Test values
	nums := []uint8{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 8, 7, 6, 5, 4, 3, 2, 1, 11, 2, 33, 22, 55, 44, 33, 22}
	//Predefined Correct Values
	signNums := []int8{0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}

	//Init a new variable
	newNums := make([]int8, len(nums))
	//Run sign function
	newNums = sign(nums, newNums)

	for i := range newNums {
		require.Equal(t, signNums[i], newNums[i])
	}
}

func TestSignUint16(t *testing.T) {
	//Test values
	nums := []uint16{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 8, 7, 6, 5, 4, 3, 2, 1, 11, 2, 33, 22, 55, 44, 33, 22, 512, 6528}
	//Predefined Correct Values
	signNums := []int8{0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}

	//Init a new variable
	newNums := make([]int8, len(nums))
	//Run sign function
	newNums = sign(nums, newNums)

	for i := range newNums {
		require.Equal(t, signNums[i], newNums[i])
	}
}

func TestSignUint32(t *testing.T) {
	//Test values
	nums := []uint32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 8, 7, 6, 5, 4, 3, 2, 1, 11, 2, 33, 22, 55, 44, 33, 22, 1010101010}
	//Predefined Correct Values
	signNums := []int8{0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}

	//Init a new variable
	newNums := make([]int8, len(nums))
	//Run sign function
	newNums = sign(nums, newNums)

	for i := range newNums {
		require.Equal(t, signNums[i], newNums[i])
	}
}

func TestSignUint64(t *testing.T) {
	//Test values
	nums := []uint64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 8, 7, 6, 5, 4, 3, 2, 1, 11, 2, 33, 22, 55, 44, 33, 22, 10101010101010101010}
	//Predefined Correct Values
	signNums := []int8{0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}

	//Init a new variable
	newNums := make([]int8, len(nums))
	//Run sign function
	newNums = sign(nums, newNums)

	for i := range newNums {
		require.Equal(t, signNums[i], newNums[i])
	}
}

func TestSignInt8(t *testing.T) {
	//Test values
	nums := []int8{-11, 22, -33, 44, -55, -66, 77, -88, 99, 0}
	//Predefined Correct Values
	signNums := []int8{-1, 1, -1, 1, -1, -1, 1, -1, 1, 0}

	//Init a new variable
	newNums := make([]int8, len(nums))
	//Run sign function
	newNums = sign(nums, newNums)

	for i := range newNums {
		require.Equal(t, signNums[i], newNums[i])
	}
}

func TestSignInt16(t *testing.T) {
	//Test values
	nums := []int16{-11, 22, -33, 44, -55, -66, 77, -88, 99, 0}
	//Predefined Correct Values
	signNums := []int8{-1, 1, -1, 1, -1, -1, 1, -1, 1, 0}

	//Init a new variable
	newNums := make([]int8, len(nums))
	//Run sign function
	newNums = sign(nums, newNums)

	for i := range newNums {
		require.Equal(t, signNums[i], newNums[i])
	}
}

func TestSignInt32(t *testing.T) {
	//Test values
	nums := []int32{-111, 222, -333, 444, -555, 666, -777, 888, -99999, -1000000000, 0}
	//Predefined Correct Values
	signNums := []int8{-1, 1, -1, 1, -1, 1, -1, 1, -1, -1, 0}

	//Init a new variable
	newNums := make([]int8, len(nums))
	//Run sign function
	newNums = sign(nums, newNums)

	for i := range newNums {
		require.Equal(t, signNums[i], newNums[i])
	}
}

func TestSignInt64(t *testing.T) {
	//Test values
	nums := []int64{-111, 222, -333, 444, -555, 666, -777, 888, -99999, -987654321, 0}
	//Predefined Correct Values
	signNums := []int8{-1, 1, -1, 1, -1, 1, -1, 1, -1, -1, 0}

	//Init a new variable
	newNums := make([]int8, len(nums))
	//Run sign function
	newNums = sign(nums, newNums)

	for i := range newNums {
		require.Equal(t, signNums[i], newNums[i])
	}
}

func TestSignFloat32(t *testing.T) {
	//Test values
	nums := []float32{1.5, -1.5, 2.5, -2.5, 1.2, 12.3, 123.4, 1234.5, 12345.6, 1234.567, -1.2, -12.3, -123.4, -1234.5, -12345.6, 0}
	//Predefined Correct Values
	signNums := []int8{1, -1, 1, -1, 1, 1, 1, 1, 1, 1, -1, -1, -1, -1, -1, 0}

	//Init a new variable
	newNums := make([]int8, len(nums))
	//Run sign function
	newNums = sign(nums, newNums)

	for i := range newNums {
		require.Equal(t, signNums[i], newNums[i])
	}
}

func TestSignFloat64(t *testing.T) {
	//Test values
	nums := []float64{1.5, -1.5, 2.5, -2.5, 1.2, 12.3, 123.4, 1234.5, 12345.6, 1234.567, -1.2, -12.3, -123.4, -1234.5, -12345.6, 0}
	//Predefined Correct Values
	signNums := []int8{1, -1, 1, -1, 1, 1, 1, 1, 1, 1, -1, -1, -1, -1, -1, 0}

	//Init a new variable
	newNums := make([]int8, len(nums))
	//Run sign function
	newNums = sign(nums, newNums)

	for i := range newNums {
		require.Equal(t, signNums[i], newNums[i])
	}
}
