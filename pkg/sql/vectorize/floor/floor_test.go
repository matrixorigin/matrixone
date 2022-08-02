// Copyright 2021 Matrix Origin
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

package floor

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFloorUint8(t *testing.T) {
	nums := []uint8{1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233}
	res := make([]uint8, len(nums))
	res0 := floorUint8(nums, res, 0)
	correctRes0 := []uint8{1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233}
	for i := range res0 {
		require.Equal(t, res0[i], correctRes0[i])
	}
	resMinus1 := floorUint8(nums, res, -1)
	correctResMinus1 := []uint8{0, 0, 0, 0, 0, 10, 20, 30, 50, 80, 140, 230}
	for i := range resMinus1 {
		require.Equal(t, correctResMinus1[i], resMinus1[i])
	}
	resMinus2 := floorUint8(nums, res, -2)
	correctResMinus2 := []uint8{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 100, 200}
	for i := range resMinus2 {
		require.Equal(t, correctResMinus2[i], resMinus2[i])
	}
}

func TestFloorUint16(t *testing.T) {
	nums := []uint16{1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233, 377, 610, 987, 1597, 2584, 4181, 6765, 10946, 17711, 28657, 46368}
	res := make([]uint16, len(nums))
	res0 := floorUint16(nums, res, 0)
	correctRes0 := []uint16{1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233, 377, 610, 987, 1597, 2584, 4181, 6765, 10946, 17711, 28657, 46368}
	for i := range res0 {
		require.Equal(t, correctRes0[i], res0[i])
	}
	resMinus1 := floorUint16(nums, res, -1)
	correctResMinus1 := []uint16{0, 0, 0, 0, 0, 10, 20, 30, 50, 80, 140, 230, 370, 610, 980, 1590, 2580, 4180, 6760, 10940, 17710, 28650, 46360}
	for i := range resMinus1 {
		require.Equal(t, correctResMinus1[i], resMinus1[i])
	}
	resMinus2 := floorUint16(nums, res, -2)
	correctResMinus2 := []uint16{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 100, 200, 300, 600, 900, 1500, 2500, 4100, 6700, 10900, 17700, 28600, 46300}
	for i := range resMinus2 {
		require.Equal(t, correctResMinus2[i], resMinus2[i])
	}
}

func TestFloorUint32(t *testing.T) {
	nums := []uint32{1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233, 377, 610, 987, 1597, 2584, 4181, 6765, 10946, 17711, 28657, 46368, 75025, 121393, 196418, 317811}
	res := make([]uint32, len(nums))
	res0 := floorUint32(nums, res, 0)
	correctRes0 := []uint32{1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233, 377, 610, 987, 1597, 2584, 4181, 6765, 10946, 17711, 28657, 46368, 75025, 121393, 196418, 317811}
	for i := range res0 {
		require.Equal(t, res0[i], correctRes0[i])
	}
	resMinus1 := floorUint32(nums, res, -1)
	correctResMinus1 := []uint32{0, 0, 0, 0, 0, 10, 20, 30, 50, 80, 140, 230, 370, 610, 980, 1590, 2580, 4180, 6760, 10940, 17710, 28650, 46360, 75020, 121390, 196410, 317810}
	for i := range res0 {
		require.Equal(t, resMinus1[i], correctResMinus1[i])
	}
	resMinus2 := floorUint32(nums, res, -2)

	correctResMinus2 := []uint32{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 100, 200, 300, 600, 900, 1500, 2500, 4100, 6700, 10900, 17700, 28600, 46300, 75000, 121300, 196400, 317800}
	for i := range res0 {
		require.Equal(t, resMinus2[i], correctResMinus2[i])
	}
}

func TestFloorUint64(t *testing.T) {
	nums := []uint64{1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233, 377, 610, 987, 1597, 2584, 4181, 6765, 10946, 17711, 28657, 46368, 75025, 121393, 196418, 317811}
	res := make([]uint64, len(nums))
	res0 := floorUint64(nums, res, 0)
	correctRes0 := []uint64{1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233, 377, 610, 987, 1597, 2584, 4181, 6765, 10946, 17711, 28657, 46368, 75025, 121393, 196418, 317811}
	for i := range res0 {
		require.Equal(t, correctRes0[i], res0[i])
	}
	resMinus1 := floorUint64(nums, res, -1)
	correctResMinus1 := []uint64{0, 0, 0, 0, 0, 10, 20, 30, 50, 80, 140, 230, 370, 610, 980, 1590, 2580, 4180, 6760, 10940, 17710, 28650, 46360, 75020, 121390, 196410, 317810}
	for i := range res0 {
		require.Equal(t, correctResMinus1[i], resMinus1[i])
	}
	resMinus2 := floorUint64(nums, res, -2)
	correctResMinus2 := []uint64{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 100, 200, 300, 600, 900, 1500, 2500, 4100, 6700, 10900, 17700, 28600, 46300, 75000, 121300, 196400, 317800}
	for i := range res0 {
		require.Equal(t, correctResMinus2[i], resMinus2[i])
	}
}

func TestFloorInt8(t *testing.T) {
	nums := []int8{1, 2, 3, 5, 8, 13, 21, 34, 55, 89, -1, -2, -3, -5, -8, -13, -21, -34, -55, -89}
	res := make([]int8, len(nums))
	res0 := floorInt8(nums, res, 0)
	correctRes0 := []int8{1, 2, 3, 5, 8, 13, 21, 34, 55, 89, -1, -2, -3, -5, -8, -13, -21, -34, -55, -89}
	for i := range res0 {
		require.Equal(t, correctRes0[i], res0[i])
	}
	resMinus1 := floorInt8(nums, res, -1)
	correctResMinus1 := []int8{0, 0, 0, 0, 0, 10, 20, 30, 50, 80, -10, -10, -10, -10, -10, -20, -30, -40, -60, -90}
	for i := range res0 {
		require.Equal(t, correctResMinus1[i], resMinus1[i])
	}
}

func TestFloorInt16(t *testing.T) {
	nums := []int16{1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233, 377, 610, 987, 1597, 2584, 4181, -1, -2, -3, -5, -8, -13, -21, -34, -55, -89, -144, -233, -377, -610, -987, -1597, -2584, -4181}
	res := make([]int16, len(nums))
	res0 := floorInt16(nums, res, 0)
	correctRes0 := []int16{1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233, 377, 610, 987, 1597, 2584, 4181, -1, -2, -3, -5, -8, -13, -21, -34, -55, -89, -144, -233, -377, -610, -987, -1597, -2584, -4181}
	for i := range res0 {
		require.Equal(t, correctRes0[i], res0[i])
	}
	resMinus1 := floorInt16(nums, res, -1)

	correctResMinus1 := []int16{0, 0, 0, 0, 0, 10, 20, 30, 50, 80, 140, 230, 370, 610, 980, 1590, 2580, 4180, -10, -10, -10, -10, -10, -20, -30, -40, -60, -90, -150, -240, -380, -610, -990, -1600, -2590, -4190}
	for i := range res0 {
		require.Equal(t, correctResMinus1[i], resMinus1[i])
	}
}

func TestFloorInt32(t *testing.T) {
	nums := []int32{1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233, 377, 610, 987, 1597, 2584, 4181, -1, -2, -3, -5, -8, -13, -21, -34, -55, -89, -144, -233, -377, -610, -987, -1597, -2584, -4181}
	res := make([]int32, len(nums))
	res0 := floorInt32(nums, res, 0)
	correctRes0 := []int32{1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233, 377, 610, 987, 1597, 2584, 4181, -1, -2, -3, -5, -8, -13, -21, -34, -55, -89, -144, -233, -377, -610, -987, -1597, -2584, -4181}
	for i := range res0 {
		require.Equal(t, correctRes0[i], res0[i])
	}
	resMinus1 := floorInt32(nums, res, -1)

	correctResMinus1 := []int32{0, 0, 0, 0, 0, 10, 20, 30, 50, 80, 140, 230, 370, 610, 980, 1590, 2580, 4180, -10, -10, -10, -10, -10, -20, -30, -40, -60, -90, -150, -240, -380, -610, -990, -1600, -2590, -4190}
	for i := range resMinus1 {
		require.Equal(t, correctResMinus1[i], resMinus1[i])
	}
}

func TestFloorInt64(t *testing.T) {
	nums := []int64{1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233, 377, 610, 987, 1597, 2584, 4181, -1, -2, -3, -5, -8, -13, -21, -34, -55, -89, -144, -233, -377, -610, -987, -1597, -2584, -4181, 32768, 1234567}
	res := make([]int64, len(nums))
	res0 := floorInt64(nums, res, 0)
	correctRes0 := []int64{1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233, 377, 610, 987, 1597, 2584, 4181, -1, -2, -3, -5, -8, -13, -21, -34, -55, -89, -144, -233, -377, -610, -987, -1597, -2584, -4181, 32768, 1234567}
	for i := range res0 {
		require.Equal(t, correctRes0[i], res0[i])
	}
	resMinus1 := floorInt64(nums, res, -1)
	correctResMinus1 := []int64{0, 0, 0, 0, 0, 10, 20, 30, 50, 80, 140, 230, 370, 610, 980, 1590, 2580, 4180, -10, -10, -10, -10, -10, -20, -30, -40, -60, -90, -150, -240, -380, -610, -990, -1600, -2590, -4190, 32760, 1234560}
	for i := range resMinus1 {
		require.Equal(t, correctResMinus1[i], resMinus1[i])
	}
}

const tolerance = .00001

func floatCompare(x, y float64) bool {
	diff := math.Abs(x - y)
	mean := math.Abs(x + y)
	if math.IsNaN(diff / mean) {
		return true
	}
	return (diff / mean) < tolerance
}

func TestFloorFloat32(t *testing.T) {
	nums := []float32{1.5, -1.5, 2.5, -2.5, 1.2, 12.3, 123.4, 1234.5, 12345.6, 1234.567, -1.2, -12.3, -123.4, -1234.5, -12345.6}
	res := make([]float32, len(nums))
	res0 := floorFloat32(nums, res, 0)
	correctRes0 := []float32{1, -2, 2, -3, 1, 12, 123, 1234, 12345, 1234, -2, -13, -124, -1235, -12346}
	for i := range nums {
		assert.True(t, floatCompare(float64(correctRes0[i]), float64(res0[i])))
	}
	resMinus1 := floorFloat32(nums, res, -1)
	correctResMinus1 := []float32{0, -10, 0, -10, 0, 10, 120, 1230, 12340, 1230, -10, -20, -130, -1240, -12350}
	for i := range nums {
		assert.True(t, floatCompare(float64(correctResMinus1[i]), float64(resMinus1[i])))
	}
	res1 := floorFloat32(nums, res, 1)
	correctRes1 := []float32{1.5, -1.5, 2.5, -2.5, 1.2, 12.3, 123.4, 1234.5, 12345.6, 1234.5, -1.2, -12.3, -123.4, -1234.5, -12345.6}
	for i := range res1 {
		assert.True(t, floatCompare(float64(correctRes1[i]), float64(res1[i])))
	}
}

func TestFloorFloat64(t *testing.T) {
	nums := []float64{1.5, -1.5, 2.5, -2.5, 1.2, 12.3, 123.4, 1234.5, 12345.6, 1234.567, -1.2, -12.3, -123.4, -1234.5, -12345.6}
	res := make([]float64, len(nums))
	res0 := floorFloat64(nums, res, 0)
	correctRes0 := []float64{1, -2, 2, -3, 1, 12, 123, 1234, 12345, 1234, -2, -13, -124, -1235, -12346}
	for i := range nums {
		assert.True(t, floatCompare(correctRes0[i], res0[i]))
	}
	resMinus1 := floorFloat64(nums, res, -1)
	correctResMinus1 := []float64{0, -10, 0, -10, 0, 10, 120, 1230, 12340, 1230, -10, -20, -130, -1240, -12350}
	for i := range nums {
		assert.True(t, floatCompare(correctResMinus1[i], resMinus1[i]))
	}
	res1 := floorFloat64(nums, res, 1)
	correctRes1 := []float64{1.5, -1.5, 2.5, -2.5, 1.2, 12.3, 123.4, 1234.5, 12345.6, 1234.5, -1.2, -12.3, -123.4, -1234.5, -12345.6}
	for i := range res1 {
		assert.True(t, floatCompare(correctRes1[i], res1[i]))
	}
}
