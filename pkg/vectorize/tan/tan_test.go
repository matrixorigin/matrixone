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

package tan

import (
  "math"
  "testing"
  "github.com/stretchr/testify/require"
)

func TestTanFloat32(t *testing.T) {
  //Test values
  nums := []float32{1, 2, 3, 4, 0, -1, -2}
  //Init a new variable
  tempNums := make([]float64, len(nums))
  //Run tan function
  tanNums := tanFloat32(nums, tempNums)

  for i, n := range nums {
    require.Equal(t, math.Tan(float64(n)), tanNums[i])
  }
}

func TestTanFloat64(t *testing.T) {
  //Test values
  nums := []float64{1, 2, 3, 4, 0, -1, -2}
  //Init a new variable
  tempNums := make([]float64, len(nums))
  //Run tan function
  tanNums := tanFloat64(nums, tempNums)

  for i, n := range nums {
    require.Equal(t, math.Tan(n), tanNums[i])
  }
}

func TestTanUint8(t *testing.T) {
  //Test values
  nums := []uint8{1, 2, 3, 4, 0, 10, 40}
  //Init a new variable
  tempNums := make([]float64, len(nums))
  //Run tan function
  tanNums := tanUint8(nums, tempNums)

  for i, n := range nums {
    require.Equal(t, math.Tan(float64(n)), tanNums[i])
  }
}

func TestTanUint16(t *testing.T) {
  //Test values
  nums := []uint16{1, 2, 3, 4, 0, 10, 40}
  //Init a new variable
  tempNums := make([]float64, len(nums))
  //Run tan function
  tanNums := tanUint16(nums, tempNums)

  for i, n := range nums {
    require.Equal(t, math.Tan(float64(n)), tanNums[i])
  }
}

func TestTanUint32(t *testing.T) {
  //Test values
  nums := []uint32{1, 2, 3, 4, 0, 10, 40}
  //Init a new variable
  tempNums := make([]float64, len(nums))
  //Run tan function
  tanNums := tanUint32(nums, tempNums)

  for i, n := range nums {
    require.Equal(t, math.Tan(float64(n)), tanNums[i])
  }
}

func TestTanUint64(t *testing.T) {
  //Test values
  nums := []uint64{1, 2, 3, 4, 0, 10, 40}
  //Init a new variable
  tempNums := make([]float64, len(nums))
  //Run tan function
  tanNums := tanUint64(nums, tempNums)

  for i, n := range nums {
    require.Equal(t, math.Tan(float64(n)), tanNums[i])
  }
}

func TestTanInt8(t *testing.T) {
  //Test values
  nums := []int8{1, 2, 3, 4, 0, 10, 40}
  //Init a new variable
  tempNums := make([]float64, len(nums))
  //Run tan function
  tanNums := tanInt8(nums, tempNums)

  for i, n := range nums {
    require.Equal(t, math.Tan(float64(n)), tanNums[i])
  }
}

func TestTanInt16(t *testing.T) {
  //Test values
  nums := []int16{1, 2, 3, 4, 0, 10, 40}
  //Init a new variable
  tempNums := make([]float64, len(nums))
  //Run tan function
  tanNums := tanInt16(nums, tempNums)

  for i, n := range nums {
    require.Equal(t, math.Tan(float64(n)), tanNums[i])
  }
}

func TestTanInt32(t *testing.T) {
  //Test values
  nums := []int32{1, 2, 3, 4, 0, 10, 40}
  //Init a new variable
  tempNums := make([]float64, len(nums))
  //Run tan function
  tanNums := tanInt32(nums, tempNums)

  for i, n := range nums {
    require.Equal(t, math.Tan(float64(n)), tanNums[i])
  }
}

func TestTanInt64(t *testing.T) {
  //Test values
  nums := []int64{1, 2, 3, 4, 0, 10, 40}
  //Init a new variable
  tempNums := make([]float64, len(nums))
  //Run tan function
  tanNums := tanInt64(nums, tempNums)

  for i, n := range nums {
    require.Equal(t, math.Tan(float64(n)), tanNums[i])
  }
}