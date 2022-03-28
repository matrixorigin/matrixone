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

package acos

import (
	"github.com/stretchr/testify/require"
	"math"
	"testing"
)

func TestAcosFloat32(t *testing.T) {
	//Test values
	nums := []float32{-1, 0, 1}
	//Init a new variable
	tempNums := make([]float64, len(nums))
	//Run acos function
	acosNums := acosFloat32(nums, tempNums)

	for i, n := range nums {
		require.Equal(t, math.Acos(float64(n)), acosNums[i])
	}
}

func TestAcosFloat64(t *testing.T) {
	//Test values
	nums := []float64{-1, 0, 1}
	//Init a new variable
	tempNums := make([]float64, len(nums))
	//Run acos function
	acosNums := acosFloat64(nums, tempNums)

	for i, n := range nums {
		require.Equal(t, math.Acos(n), acosNums[i])
	}
}

func TestAcosUint8(t *testing.T) {
	//Test values
	nums := []uint8{0, 1}
	//Init a new variable
	tempNums := make([]float64, len(nums))
	//Run acos function
	acosNums := acosUint8(nums, tempNums)

	for i, n := range nums {
		require.Equal(t, math.Acos(float64(n)), acosNums[i])
	}
}

func TestAcosUint16(t *testing.T) {
	//Test values
	nums := []uint16{0, 1}
	//Init a new variable
	tempNums := make([]float64, len(nums))
	//Run acos function
	acosNums := acosUint16(nums, tempNums)

	for i, n := range nums {
		require.Equal(t, math.Acos(float64(n)), acosNums[i])
	}
}

func TestAcosUint32(t *testing.T) {
	//Test values
	nums := []uint32{0, 1}
	//Init a new variable
	tempNums := make([]float64, len(nums))
	//Run acos function
	acosNums := acosUint32(nums, tempNums)

	for i, n := range nums {
		require.Equal(t, math.Acos(float64(n)), acosNums[i])
	}
}

func TestAcosUint64(t *testing.T) {
	//Test values
	nums := []uint64{0, 1}
	//Init a new variable
	tempNums := make([]float64, len(nums))
	//Run acos function
	acosNums := acosUint64(nums, tempNums)

	for i, n := range nums {
		require.Equal(t, math.Acos(float64(n)), acosNums[i])
	}
}

func TestAcosInt8(t *testing.T) {
	//Test values
	nums := []int8{-1, 0, 1}
	//Init a new variable
	tempNums := make([]float64, len(nums))
	//Run acos function
	acosNums := acosInt8(nums, tempNums)

	for i, n := range nums {
		require.Equal(t, math.Acos(float64(n)), acosNums[i])
	}
}

func TestAcosInt16(t *testing.T) {
	//Test values
	nums := []int16{-1, 0, 1}
	//Init a new variable
	tempNums := make([]float64, len(nums))
	//Run acos function
	acosNums := acosInt16(nums, tempNums)

	for i, n := range nums {
		require.Equal(t, math.Acos(float64(n)), acosNums[i])
	}
}

func TestAcosInt32(t *testing.T) {
	//Test values
	nums := []int32{-1, 0, 1}
	//Init a new variable
	tempNums := make([]float64, len(nums))
	//Run acos function
	acosNums := acosInt32(nums, tempNums)

	for i, n := range nums {
		require.Equal(t, math.Acos(float64(n)), acosNums[i])
	}
}

func TestAcosInt64(t *testing.T) {
	//Test values
	nums := []int64{-1, 0, 1}
	//Init a new variable
	tempNums := make([]float64, len(nums))
	//Run acos function
	acosNums := acosInt64(nums, tempNums)

	for i, n := range nums {
		require.Equal(t, math.Acos(float64(n)), acosNums[i])
	}
}
