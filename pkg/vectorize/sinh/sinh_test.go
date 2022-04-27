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

package sinh

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSinhFloat32(t *testing.T) {
	//Test values
	nums := []float32{-math.MaxFloat32, -math.SmallestNonzeroFloat32, -1, 0, 1, math.SmallestNonzeroFloat32, math.MaxFloat32}
	//Init a new variable
	tempNums := make([]float64, len(nums))
	//Run sinh function
	sinNums := sinhFloat32(nums, tempNums)

	for i, n := range nums {
		require.Equal(t, math.Sinh(float64(n)), sinNums[i])
	}
}

func TestSinhFloat64(t *testing.T) {
	//Test values
	nums := []float64{-math.MaxFloat64, -math.SmallestNonzeroFloat64, -1, 0, 1, math.SmallestNonzeroFloat64, math.MaxFloat64}
	//Init a new variable
	tempNums := make([]float64, len(nums))
	//Run sinh function
	sinNums := sinhFloat64(nums, tempNums)

	for i, n := range nums {
		require.Equal(t, math.Sinh(n), sinNums[i])
	}
}

func TestSinhUint8(t *testing.T) {
	//Test values
	nums := []uint8{0, 1, 10, math.MaxUint8}
	//Init a new variable
	tempNums := make([]float64, len(nums))
	//Run sinh function
	sinNums := sinhUint8(nums, tempNums)

	for i, n := range nums {
		require.Equal(t, math.Sinh(float64(n)), sinNums[i])
	}
}

func TestSinhUint16(t *testing.T) {
	//Test values
	nums := []uint16{0, 1, 10, math.MaxUint16}
	//Init a new variable
	tempNums := make([]float64, len(nums))
	//Run sinh function
	sinNums := sinhUint16(nums, tempNums)

	for i, n := range nums {
		require.Equal(t, math.Sinh(float64(n)), sinNums[i])
	}
}

func TestSinhUint32(t *testing.T) {
	//Test values
	nums := []uint32{0, 1, 10, math.MaxUint32}
	//Init a new variable
	tempNums := make([]float64, len(nums))
	//Run sinh function
	sinNums := sinhUint32(nums, tempNums)

	for i, n := range nums {
		require.Equal(t, math.Sinh(float64(n)), sinNums[i])
	}
}

func TestSinhUint64(t *testing.T) {
	//Test values
	nums := []uint64{0, 1, 10, math.MaxUint64}
	//Init a new variable
	tempNums := make([]float64, len(nums))
	//Run sinh function
	sinNums := sinhUint64(nums, tempNums)

	for i, n := range nums {
		require.Equal(t, math.Sinh(float64(n)), sinNums[i])
	}
}

func TestSinhInt8(t *testing.T) {
	//Test values
	nums := []int8{math.MinInt8, -10, -1, 0, 1, 10, math.MaxInt8}
	//Init a new variable
	tempNums := make([]float64, len(nums))
	//Run sinh function
	sinNums := sinhInt8(nums, tempNums)

	for i, n := range nums {
		require.Equal(t, math.Sinh(float64(n)), sinNums[i])
	}
}

func TestSinhInt16(t *testing.T) {
	//Test values
	nums := []int16{math.MinInt16, -10, -1, 0, 1, 10, math.MaxInt16}
	//Init a new variable
	tempNums := make([]float64, len(nums))
	//Run sinh function
	sinNums := sinhInt16(nums, tempNums)

	for i, n := range nums {
		require.Equal(t, math.Sinh(float64(n)), sinNums[i])
	}
}

func TestSinhInt32(t *testing.T) {
	//Test values
	nums := []int32{math.MinInt32, -10, -1, 0, 1, 10, math.MaxInt32}
	//Init a new variable
	tempNums := make([]float64, len(nums))
	//Run sinh function
	sinNums := sinhInt32(nums, tempNums)

	for i, n := range nums {
		require.Equal(t, math.Sinh(float64(n)), sinNums[i])
	}
}

func TestSinhInt64(t *testing.T) {
	//Test values
	nums := []int64{math.MinInt64, -10, -1, 0, 1, 10, math.MaxInt64}
	//Init a new variable
	tempNums := make([]float64, len(nums))
	//Run sinh function
	sinNums := sinhInt64(nums, tempNums)

	for i, n := range nums {
		require.Equal(t, math.Sinh(float64(n)), sinNums[i])
	}
}
