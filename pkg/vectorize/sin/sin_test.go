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

package sin

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSinFloat32(t *testing.T) {
	//Test values
	nums := []float32{1, 2, 3, 4, 0, -1, -2}
	//Init a new variable
	tempNums := make([]float64, len(nums))
	//Run sin function
	sinNums := sinFloat32(nums, tempNums)

	for i, n := range nums {
		require.Equal(t, math.Sin(float64(n)), sinNums[i])
	}
}

func TestSinFloat64(t *testing.T) {
	//Test values
	nums := []float64{1, 2, 3, 4, 0, -1, -2}
	//Init a new variable
	tempNums := make([]float64, len(nums))
	//Run sin function
	sinNums := sinFloat64(nums, tempNums)

	for i, n := range nums {
		require.Equal(t, math.Sin(n), sinNums[i])
	}
}

func TestSinUint8(t *testing.T) {
	//Test values
	nums := []uint8{1, 2, 3, 4, 0, 10, 40}
	//Init a new variable
	tempNums := make([]float64, len(nums))
	//Run sin function
	sinNums := sinUint8(nums, tempNums)

	for i, n := range nums {
		require.Equal(t, math.Sin(float64(n)), sinNums[i])
	}
}

func TestSinUint16(t *testing.T) {
	//Test values
	nums := []uint16{1, 2, 3, 4, 0, 10, 40}
	//Init a new variable
	tempNums := make([]float64, len(nums))
	//Run sin function
	sinNums := sinUint16(nums, tempNums)

	for i, n := range nums {
		require.Equal(t, math.Sin(float64(n)), sinNums[i])
	}
}

func TestSinUint32(t *testing.T) {
	//Test values
	nums := []uint32{1, 2, 3, 4, 0, 10, 40}
	//Init a new variable
	tempNums := make([]float64, len(nums))
	//Run sin function
	sinNums := sinUint32(nums, tempNums)

	for i, n := range nums {
		require.Equal(t, math.Sin(float64(n)), sinNums[i])
	}
}

func TestSinUint64(t *testing.T) {
	//Test values
	nums := []uint64{1, 2, 3, 4, 0, 10, 40}
	//Init a new variable
	tempNums := make([]float64, len(nums))
	//Run sin function
	sinNums := sinUint64(nums, tempNums)

	for i, n := range nums {
		require.Equal(t, math.Sin(float64(n)), sinNums[i])
	}
}

func TestSinInt8(t *testing.T) {
	//Test values
	nums := []int8{1, 2, 3, 4, 0, 10, 40}
	//Init a new variable
	tempNums := make([]float64, len(nums))
	//Run sin function
	sinNums := sinInt8(nums, tempNums)

	for i, n := range nums {
		require.Equal(t, math.Sin(float64(n)), sinNums[i])
	}
}

func TestSinInt16(t *testing.T) {
	//Test values
	nums := []int16{1, 2, 3, 4, 0, 10, 40}
	//Init a new variable
	tempNums := make([]float64, len(nums))
	//Run sin function
	sinNums := sinInt16(nums, tempNums)

	for i, n := range nums {
		require.Equal(t, math.Sin(float64(n)), sinNums[i])
	}
}

func TestSinInt32(t *testing.T) {
	//Test values
	nums := []int32{1, 2, 3, 4, 0, 10, 40}
	//Init a new variable
	tempNums := make([]float64, len(nums))
	//Run sin function
	sinNums := sinInt32(nums, tempNums)

	for i, n := range nums {
		require.Equal(t, math.Sin(float64(n)), sinNums[i])
	}
}

func TestSinInt64(t *testing.T) {
	//Test values
	nums := []int64{1, 2, 3, 4, 0, 10, 40}
	//Init a new variable
	tempNums := make([]float64, len(nums))
	//Run sin function
	sinNums := sinInt64(nums, tempNums)

	for i, n := range nums {
		require.Equal(t, math.Sin(float64(n)), sinNums[i])
	}
}
