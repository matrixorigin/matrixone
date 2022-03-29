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
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAtanUint8(t *testing.T) {
	//Test values
	nums := []uint8{0, 1, 2, 55, 44, 33, 22}
	//Init a new variable
	atanNums := make([]float64, len(nums))
	//Run atan function
	atanNums = atanUint8(nums, atanNums)

	for i, n := range nums {
		require.Equal(t, math.Atan(float64(n)), atanNums[i])
	}
}

func TestAtanUint16(t *testing.T) {
	//Test values
	nums := []uint16{0, 1, 2, 55, 44, 33, 22}
	//Init a new variable
	atanNums := make([]float64, len(nums))
	//Run atan function
	atanNums = atanUint16(nums, atanNums)

	for i, n := range nums {
		require.Equal(t, math.Atan(float64(n)), atanNums[i])
	}
}

func TestAtanUint32(t *testing.T) {
	//Test values
	nums := []uint32{0, 1, 2, 55, 44, 33, 22}
	//Init a new variable
	atanNums := make([]float64, len(nums))
	//Run atan function
	atanNums = atanUint32(nums, atanNums)

	for i, n := range nums {
		require.Equal(t, math.Atan(float64(n)), atanNums[i])
	}
}

func TestAtanUint64(t *testing.T) {
	//Test values
	nums := []uint64{0, 1, 2, 55, 44, 33, 22}
	//Init a new variable
	atanNums := make([]float64, len(nums))
	//Run atan function
	atanNums = atanUint64(nums, atanNums)

	for i, n := range nums {
		require.Equal(t, math.Atan(float64(n)), atanNums[i])
	}
}

func TestAtanInt8(t *testing.T) {
	//Test values
	nums := []int8{-55, -10, -1, 0, 1, 10, 55}
	//Init a new variable
	atanNums := make([]float64, len(nums))
	//Run atan function
	atanNums = atanInt8(nums, atanNums)

	for i, n := range nums {
		require.Equal(t, math.Atan(float64(n)), atanNums[i])
	}
}

func TestAtanInt16(t *testing.T) {
	//Test values
	nums := []int16{-55, -10, -1, 0, 1, 10, 55}
	//Init a new variable
	atanNums := make([]float64, len(nums))
	//Run atan function
	atanNums = atanInt16(nums, atanNums)

	for i, n := range nums {
		require.Equal(t, math.Atan(float64(n)), atanNums[i])
	}
}

func TestAtanInt32(t *testing.T) {
	//Test values
	nums := []int32{-55, -10, -1, 0, 1, 10, 55}
	//Init a new variable
	atanNums := make([]float64, len(nums))
	//Run atan function
	atanNums = atanInt32(nums, atanNums)

	for i, n := range nums {
		require.Equal(t, math.Atan(float64(n)), atanNums[i])
	}
}

func TestAtanInt64(t *testing.T) {
	//Test values
	nums := []int64{-55, -10, -1, 0, 1, 10, 55}
	//Init a new variable
	atanNums := make([]float64, len(nums))
	//Run atan function
	atanNums = atanInt64(nums, atanNums)

	for i, n := range nums {
		require.Equal(t, math.Atan(float64(n)), atanNums[i])
	}
}

func TestAtanFloat32(t *testing.T) {
	//Test values
	nums := []float32{-55.66, -10.22, -1.33, 0, 0.77, 11.22, 55.66}
	//Init a new variable
	atanNums := make([]float64, len(nums))
	//Run atan function
	atanNums = atanFloat32(nums, atanNums)

	for i, n := range nums {
		require.Equal(t, math.Atan(float64(n)), atanNums[i])
	}
}

func TestAtanFloat64(t *testing.T) {
	//Test values
	nums := []float64{-55.66, -10.22, -1.33, 0, 0.77, 11.22, 55.66}
	//Init a new variable
	atanNums := make([]float64, len(nums))
	//Run atan function
	atanNums = atanFloat64(nums, atanNums)

	for i, n := range nums {
		require.Equal(t, math.Atan(n), atanNums[i])
	}
}
