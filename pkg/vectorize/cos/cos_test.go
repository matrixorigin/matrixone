package cos

import (
  "math"
  "testing"
  "github.com/stretchr/testify/require"
)

func TestCosFloat32(t *testing.T) {
  //Test values
  nums := []float32{1, 2, 3, 4, 0, -1, -2}
  //Init a new variable
  tempNums := make([]float64, len(nums))
  //Run cos function
  cosNums := cosFloat32(nums, tempNums)

  for i, n := range nums {
    require.Equal(t, math.Cos(float64(n)), cosNums[i])
  }
}

func TestCosFloat64(t *testing.T) {
  //Test values
  nums := []float64{1, 2, 3, 4, 0, -1, -2}
  //Init a new variable
  tempNums := make([]float64, len(nums))
  //Run cos function
  cosNums := cosFloat64(nums, tempNums)

  for i, n := range nums {
    require.Equal(t, math.Cos(n), cosNums[i])
  }
}

func TestCosUint8(t *testing.T) {
  //Test values
  nums := []uint8{1, 2, 3, 4, 0, 10, 40}
  //Init a new variable
  tempNums := make([]float64, len(nums))
  //Run cos function
  cosNums := cosUint8(nums, tempNums)

  for i, n := range nums {
    require.Equal(t, math.Cos(float64(n)), cosNums[i])
  }
}

func TestCosUint16(t *testing.T) {
  //Test values
  nums := []uint16{1, 2, 3, 4, 0, 10, 40}
  //Init a new variable
  tempNums := make([]float64, len(nums))
  //Run cos function
  cosNums := cosUint16(nums, tempNums)

  for i, n := range nums {
    require.Equal(t, math.Cos(float64(n)), cosNums[i])
  }
}

func TestCosUint32(t *testing.T) {
  //Test values
  nums := []uint32{1, 2, 3, 4, 0, 10, 40}
  //Init a new variable
  tempNums := make([]float64, len(nums))
  //Run cos function
  cosNums := cosUint32(nums, tempNums)

  for i, n := range nums {
    require.Equal(t, math.Cos(float64(n)), cosNums[i])
  }
}

func TestCosUint64(t *testing.T) {
  //Test values
  nums := []uint64{1, 2, 3, 4, 0, 10, 40}
  //Init a new variable
  tempNums := make([]float64, len(nums))
  //Run cos function
  cosNums := cosUint64(nums, tempNums)

  for i, n := range nums {
    require.Equal(t, math.Cos(float64(n)), cosNums[i])
  }
}

func TestCosInt8(t *testing.T) {
  //Test values
  nums := []int8{1, 2, 3, 4, 0, 10, 40}
  //Init a new variable
  tempNums := make([]float64, len(nums))
  //Run cos function
  cosNums := cosInt8(nums, tempNums)

  for i, n := range nums {
    require.Equal(t, math.Cos(float64(n)), cosNums[i])
  }
}

func TestCosInt16(t *testing.T) {
  //Test values
  nums := []int16{1, 2, 3, 4, 0, 10, 40}
  //Init a new variable
  tempNums := make([]float64, len(nums))
  //Run cos function
  cosNums := cosInt16(nums, tempNums)

  for i, n := range nums {
    require.Equal(t, math.Cos(float64(n)), cosNums[i])
  }
}

func TestCosInt32(t *testing.T) {
  //Test values
  nums := []int32{1, 2, 3, 4, 0, 10, 40}
  //Init a new variable
  tempNums := make([]float64, len(nums))
  //Run cos function
  cosNums := cosInt32(nums, tempNums)

  for i, n := range nums {
    require.Equal(t, math.Cos(float64(n)), cosNums[i])
  }
}

func TestCosInt64(t *testing.T) {
  //Test values
  nums := []int64{1, 2, 3, 4, 0, 10, 40}
  //Init a new variable
  tempNums := make([]float64, len(nums))
  //Run cos function
  cosNums := cosInt64(nums, tempNums)

  for i, n := range nums {
    require.Equal(t, math.Cos(float64(n)), cosNums[i])
  }
}