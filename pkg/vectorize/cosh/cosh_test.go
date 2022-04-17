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

package cosh

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCoshFloat32(t *testing.T) {
	//Test values
	nums := []float32{-math.MaxFloat32, -math.SmallestNonzeroFloat32, -1, 0, 1, math.SmallestNonzeroFloat32, math.MaxFloat32}
	//Init a new variable
	tempNums := make([]float64, len(nums))
	//Run cosh function
	cosNums := coshFloat32(nums, tempNums)

	for i, n := range nums {
		require.Equal(t, math.Cosh(float64(n)), cosNums[i])
	}
}

func TestCoshFloat64(t *testing.T) {
	//Test values
	nums := []float64{-math.MaxFloat64, -math.SmallestNonzeroFloat64, -1, 0, 1, math.SmallestNonzeroFloat64, math.MaxFloat64}
	//Init a new variable
	tempNums := make([]float64, len(nums))
	//Run cosh function
	cosNums := coshFloat64(nums, tempNums)

	for i, n := range nums {
		require.Equal(t, math.Cosh(n), cosNums[i])
	}
}

func TestCoshUint8(t *testing.T) {
	//Test values
	nums := []uint8{0, 1, 10, math.MaxUint8}
	//Init a new variable
	tempNums := make([]float64, len(nums))
	//Run cosh function
	cosNums := coshUint8(nums, tempNums)

	for i, n := range nums {
		require.Equal(t, math.Cosh(float64(n)), cosNums[i])
	}
}

func TestCoshUint16(t *testing.T) {
	//Test values
	nums := []uint16{0, 1, 10, math.MaxUint16}
	//Init a new variable
	tempNums := make([]float64, len(nums))
	//Run cosh function
	cosNums := coshUint16(nums, tempNums)

	for i, n := range nums {
		require.Equal(t, math.Cosh(float64(n)), cosNums[i])
	}
}

func TestCoshUint32(t *testing.T) {
	//Test values
	nums := []uint32{0, 1, 10, math.MaxUint32}
	//Init a new variable
	tempNums := make([]float64, len(nums))
	//Run cosh function
	cosNums := coshUint32(nums, tempNums)

	for i, n := range nums {
		require.Equal(t, math.Cosh(float64(n)), cosNums[i])
	}
}

func TestCoshUint64(t *testing.T) {
	//Test values
	nums := []uint64{0, 1, 10, math.MaxUint64}
	//Init a new variable
	tempNums := make([]float64, len(nums))
	//Run cosh function
	cosNums := coshUint64(nums, tempNums)

	for i, n := range nums {
		require.Equal(t, math.Cosh(float64(n)), cosNums[i])
	}
}

func TestCoshInt8(t *testing.T) {
	//Test values
	nums := []int8{math.MinInt8, -10, -1, 0, 1, 10, math.MaxInt8}
	//Init a new variable
	tempNums := make([]float64, len(nums))
	//Run cosh function
	cosNums := coshInt8(nums, tempNums)

	for i, n := range nums {
		require.Equal(t, math.Cosh(float64(n)), cosNums[i])
	}
}

func TestCoshInt16(t *testing.T) {
	//Test values
	nums := []int16{math.MinInt16, -10, -1, 0, 1, 10, math.MaxInt16}
	//Init a new variable
	tempNums := make([]float64, len(nums))
	//Run cosh function
	cosNums := coshInt16(nums, tempNums)

	for i, n := range nums {
		require.Equal(t, math.Cosh(float64(n)), cosNums[i])
	}
}

func TestCoshInt32(t *testing.T) {
	//Test values
	nums := []int32{math.MinInt32, -10, -1, 0, 1, 10, math.MaxInt32}
	//Init a new variable
	tempNums := make([]float64, len(nums))
	//Run cosh function
	cosNums := coshInt32(nums, tempNums)

	for i, n := range nums {
		require.Equal(t, math.Cosh(float64(n)), cosNums[i])
	}
}

func TestCoshInt64(t *testing.T) {
	//Test values
	nums := []int64{math.MinInt64, -10, -1, 0, 1, 10, math.MaxInt64}
	//Init a new variable
	tempNums := make([]float64, len(nums))
	//Run cosh function
	cosNums := coshInt64(nums, tempNums)

	for i, n := range nums {
		require.Equal(t, math.Cosh(float64(n)), cosNums[i])
	}
}
