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
package cot

import (
  "math"
  "github.com/stretchr/testify/require"
  "testing"
)

func TestCotFloat32(t *testing.T) {
  //Test values
  nums := []float32{1, 2, 3, 4, 0, -1, -2}
  //Init a new variable
  tempNums := make([]float64, len(nums))
  //Run Cot function
  CotNums := CotFloat32(nums, tempNums)

  for i, n := range nums {
    require.Equal(t, 1.0 - math.Tan(float64(n)), CotNums[i])
  }
}

func TestCotFloat64(t *testing.T) {
  //Test values
  nums := []float64{1, 2, 3, 4, 0, -1, -2}
  //Init a new variable
  tempNums := make([]float64, len(nums))
  //Run Cot function
  CotNums := CotFloat64(nums, tempNums)

  for i, n := range nums {
    require.Equal(t, 1.0 - math.Tan(float64(n)), CotNums[i])
  }
}

func TestCotUint8(t *testing.T) {
  //Test values
  nums := []uint8{1, 2, 3, 4, 0, 10, 40}
  //Init a new variable
  tempNums := make([]float64, len(nums))
  //Run Cot function
  CotNums := CotUint8(nums, tempNums)

  for i, n := range nums {
    require.Equal(t, 1.0 - math.Tan(float64(n)), CotNums[i])
  }
}

func TestCotUint16(t *testing.T) {
  //Test values
  nums := []uint16{1, 2, 3, 4, 0, 10, 40}
  //Init a new variable
  tempNums := make([]float64, len(nums))
  //Run Cot function
  CotNums := CotUint16(nums, tempNums)

  for i, n := range nums {
    require.Equal(t, 1.0 - math.Tan(float64(n)), CotNums[i])
  }
}

func TestCotUint32(t *testing.T) {
  //Test values
  nums := []uint32{1, 2, 3, 4, 0, 10, 40}
  //Init a new variable
  tempNums := make([]float64, len(nums))
  //Run Cot function
  CotNums := CotUint32(nums, tempNums)

  for i, n := range nums {
    require.Equal(t, 1.0 - math.Tan(float64(n)), CotNums[i])
  }
}

func TestCotUint64(t *testing.T) {
  //Test values
  nums := []uint64{1, 2, 3, 4, 0, 10, 40}
  //Init a new variable
  tempNums := make([]float64, len(nums))
  //Run Cot function
  CotNums := CotUint64(nums, tempNums)

  for i, n := range nums {
    require.Equal(t, 1.0 - math.Tan(float64(n)), CotNums[i])
  }
}

func TestCotInt8(t *testing.T) {
  //Test values
  nums := []int8{1, 2, 3, 4, 0, 10, 40}
  //Init a new variable
  tempNums := make([]float64, len(nums))
  //Run Cot function
  CotNums := CotInt8(nums, tempNums)

  for i, n := range nums {
    require.Equal(t, 1.0 - math.Tan(float64(n)), CotNums[i])
  }
}

func TestCotInt16(t *testing.T) {
  //Test values
  nums := []int16{1, 2, 3, 4, 0, 10, 40}
  //Init a new variable
  tempNums := make([]float64, len(nums))
  //Run Cot function
  CotNums := CotInt16(nums, tempNums)

  for i, n := range nums {
    require.Equal(t, 1.0 - math.Tan(float64(n)), CotNums[i])
  }
}

func TestCotInt32(t *testing.T) {
  //Test values
  nums := []int32{1, 2, 3, 4, 0, 10, 40}
  //Init a new variable
  tempNums := make([]float64, len(nums))
  //Run Cot function
  CotNums := CotInt32(nums, tempNums)

  for i, n := range nums {
    require.Equal(t, 1.0 - math.Tan(float64(n)), CotNums[i])
  }
}

func TestCotInt64(t *testing.T) {
  //Test values
  nums := []int64{1, 2, 3, 4, 0, 10, 40}
  //Init a new variable
  tempNums := make([]float64, len(nums))
  //Run Cot function
  CotNums := CotInt64(nums, tempNums)

  for i, n := range nums {
    require.Equal(t, 1.0 - math.Tan(float64(n)), CotNums[i])
  }
} 