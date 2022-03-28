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

package atan

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAtanUint8(t *testing.T) {
	//Test values
	nums := []uint8{0, 1, 2, 55, 44, 33, 22}
	//Predefined Correct Values
	atanNums := []float64{0, 0.7853981633974483, 1.1071487177940904, 1.5526165117219182, 1.5480729659532555, 1.5405025668761214, 1.5253730473733196}
	//Init a new variable
	newNums := make([]float64, len(nums))
	//Run atan function
	newNums = atanUint8(nums, newNums)

	for i := range newNums {
		require.Equal(t, atanNums[i], newNums[i])
	}
}

func TestAtanUint16(t *testing.T) {
	//Test values
	nums := []uint16{0, 1, 2, 55, 44, 33, 22}
	//Predefined Correct Values
	atanNums := []float64{0, 0.7853981633974483, 1.1071487177940904, 1.5526165117219182, 1.5480729659532555, 1.5405025668761214, 1.5253730473733196}
	//Init a new variable
	newNums := make([]float64, len(nums))
	//Run atan function
	newNums = atanUint16(nums, newNums)

	for i := range newNums {
		require.Equal(t, atanNums[i], newNums[i])
	}
}

func TestAtanUint32(t *testing.T) {
	//Test values
	nums := []uint32{0, 1, 2, 55, 44, 33, 22}
	//Predefined Correct Values
	atanNums := []float64{0, 0.7853981633974483, 1.1071487177940904, 1.5526165117219182, 1.5480729659532555, 1.5405025668761214, 1.5253730473733196}
	//Init a new variable
	newNums := make([]float64, len(nums))
	//Run atan function
	newNums = atanUint32(nums, newNums)

	for i := range newNums {
		require.Equal(t, atanNums[i], newNums[i])
	}
}

func TestAtanUint64(t *testing.T) {
	//Test values
	nums := []uint64{0, 1, 2, 55, 44, 33, 22}
	//Predefined Correct Values
	atanNums := []float64{0, 0.7853981633974483, 1.1071487177940904, 1.5526165117219182, 1.5480729659532555, 1.5405025668761214, 1.5253730473733196}
	//Init a new variable
	newNums := make([]float64, len(nums))
	//Run atan function
	newNums = atanUint64(nums, newNums)

	for i := range newNums {
		require.Equal(t, atanNums[i], newNums[i])
	}
}

func TestAtanInt8(t *testing.T) {
	//Test values
	nums := []int8{-55, -10, -1, 0, 1, 10, 55}
	//Predefined Correct Values
	atanNums := []float64{-1.5526165117219182, -1.4711276743037345, -0.7853981633974483, 0, 0.7853981633974483, 1.4711276743037345, 1.5526165117219182}
	//Init a new variable
	newNums := make([]float64, len(nums))
	//Run atan function
	newNums = atanInt8(nums, newNums)

	for i := range newNums {
		require.Equal(t, atanNums[i], newNums[i])
	}
}

func TestAtanInt16(t *testing.T) {
	//Test values
	nums := []int16{-55, -10, -1, 0, 1, 10, 55}
	//Predefined Correct Values
	atanNums := []float64{-1.5526165117219182, -1.4711276743037345, -0.7853981633974483, 0, 0.7853981633974483, 1.4711276743037345, 1.5526165117219182}
	//Init a new variable
	newNums := make([]float64, len(nums))
	//Run atan function
	newNums = atanInt16(nums, newNums)

	for i := range newNums {
		require.Equal(t, atanNums[i], newNums[i])
	}
}

func TestAtanInt32(t *testing.T) {
	//Test values
	nums := []int32{-55, -10, -1, 0, 1, 10, 55}
	//Predefined Correct Values
	atanNums := []float64{-1.5526165117219182, -1.4711276743037345, -0.7853981633974483, 0, 0.7853981633974483, 1.4711276743037345, 1.5526165117219182}
	//Init a new variable
	newNums := make([]float64, len(nums))
	//Run atan function
	newNums = atanInt32(nums, newNums)

	for i := range newNums {
		require.Equal(t, atanNums[i], newNums[i])
	}
}

func TestAtanInt64(t *testing.T) {
	//Test values
	nums := []int64{-55, -10, -1, 0, 1, 10, 55}
	//Predefined Correct Values
	atanNums := []float64{-1.5526165117219182, -1.4711276743037345, -0.7853981633974483, 0, 0.7853981633974483, 1.4711276743037345, 1.5526165117219182}
	//Init a new variable
	newNums := make([]float64, len(nums))
	//Run atan function
	newNums = atanInt64(nums, newNums)

	for i := range newNums {
		require.Equal(t, atanNums[i], newNums[i])
	}
}

func TestAtanFloat32(t *testing.T) {
	//Test values
	nums := []float32{-55.66, -10.22, -1.33, 0, 0.77, 11.22, 55.66}
	//Predefined Correct Values
	atanNums := []float64{-1.5528320359484806, -1.4732594565525021, -0.9260933110025209, 0, 0.6561787060173309, 1.4819046443245365, 1.5528320359484806}
	//Init a new variable
	newNums := make([]float64, len(nums))
	//Run atan function
	newNums = atanFloat32(nums, newNums)

	for i := range newNums {
		require.Equal(t, atanNums[i], newNums[i])
	}
}

func TestAtanFloat64(t *testing.T) {
	//Test values
	nums := []float64{-55.66, -10.22, -1.33, 0, 0.77, 11.22, 55.66}
	//Predefined Correct Values
	atanNums := []float64{-1.5528320359977177, -1.4732594540201849, -0.9260932955034623, 0, 0.6561787179913948, 1.4819046422200983, 1.5528320359977177}
	//Init a new variable
	newNums := make([]float64, len(nums))
	//Run atan function
	newNums = atanFloat64(nums, newNums)

	for i := range newNums {
		require.Equal(t, atanNums[i], newNums[i])
	}
}
