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

	"github.com/stretchr/testify/require"
)

func TestExpUint8(t *testing.T) {
	//Test values
	nums := []uint8{1, 2, 3, 8, 10, 12, 40}
	//Init a new variable
	newNums := make([]float64, len(nums))
	//Run abs function
	expResult := expUint8(nums, newNums)

	for i, actual := range expResult {
		expected := Exponential(float64(nums[i]))
		require.Equal(t, expected, actual)
	}
}

func TestExpUint16(t *testing.T) {
	//Test values
	nums := []uint16{1, 2, 3, 8, 10, 12, 40}
	//Init a new variable
	newNums := make([]float64, len(nums))
	//Run abs function
	expResult := expUint16(nums, newNums)

	for i, actual := range expResult {
		expected := Exponential(float64(nums[i]))
		require.Equal(t, expected, actual)
	}
}

func TestExpUint32(t *testing.T) {
	//Test values
	nums := []uint32{1, 2, 3, 8, 10, 12, 40}
	//Init a new variable
	newNums := make([]float64, len(nums))
	//Run abs function
	expResult := expUint32(nums, newNums)

	for i, actual := range expResult {
		expected := Exponential(float64(nums[i]))
		require.Equal(t, expected, actual)
	}
}

func TestExpUint64(t *testing.T) {
	//Test values
	nums := []uint64{1, 2, 3, 8, 10, 12, 40}
	//Init a new variable
	newNums := make([]float64, len(nums))
	//Run abs function
	expResult := expUint64(nums, newNums)

	for i, actual := range expResult {
		expected := Exponential(float64(nums[i]))
		require.Equal(t, expected, actual)
	}
}

func TestExpint8(t *testing.T) {
	//Test values
	nums := []int8{1, 2, 3, 8, 10, 12, 40}
	//Init a new variable
	newNums := make([]float64, len(nums))
	//Run abs function
	expResult := expInt8(nums, newNums)

	for i, actual := range expResult {
		expected := Exponential(float64(nums[i]))
		require.Equal(t, expected, actual)
	}
}

func TestExpint16(t *testing.T) {
	//Test values
	nums := []int16{1, 2, 3, 8, 10, 12, 40}
	//Init a new variable
	newNums := make([]float64, len(nums))
	//Run abs function
	expResult := expInt16(nums, newNums)

	for i, actual := range expResult {
		expected := Exponential(float64(nums[i]))
		require.Equal(t, expected, actual)
	}
}

func TestExpint32(t *testing.T) {
	//Test values
	nums := []int32{1, 2, 3, 8, 10, 12, 40}
	//Init a new variable
	newNums := make([]float64, len(nums))
	//Run abs function
	expResult := expInt32(nums, newNums)

	for i, actual := range expResult {
		expected := Exponential(float64(nums[i]))
		require.Equal(t, expected, actual)
	}
}

func TestExpint64(t *testing.T) {
	//Test values
	nums := []int64{1, 2, 3, 8, 10, 12, 40}
	//Init a new variable
	newNums := make([]float64, len(nums))
	//Run abs function
	expResult := expInt64(nums, newNums)

	for i, actual := range expResult {
		expected := Exponential(float64(nums[i]))
		require.Equal(t, expected, actual)
	}
}

func TestExpfloat32(t *testing.T) {
	//Test values
	nums := []float32{1.234, 2.345, 3.456, 8.901234, 10.0191, 12.454, 40.12312}
	//Init a new variable
	newNums := make([]float64, len(nums))
	//Run abs function
	expResult := expFloat32(nums, newNums)

	for i, actual := range expResult {
		expected := Exponential(float64(nums[i]))
		require.Equal(t, expected, actual)
	}
}

func TestExpfloat64(t *testing.T) {
	//Test values
	nums := []float64{1.234, 2.345, 3.456, 8.901234, 10.0191, 12.454, 40.12312}
	//Init a new variable
	newNums := make([]float64, len(nums))
	//Run abs function
	expResult := expFloat64(nums, newNums)

	for i, actual := range expResult {
		expected := Exponential(nums[i])
		require.Equal(t, expected, actual)
	}
}
