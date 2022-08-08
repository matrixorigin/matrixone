// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package ceil

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCeilUint8(t *testing.T) {
	nums := []uint8{1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 199, 233}
	res := make([]uint8, len(nums))
	res0 := ceilUint8(nums, res, 0)
	correctRes0 := []uint8{1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 199, 233}
	for i := range res0 {
		require.Equal(t, res0[i], correctRes0[i])
	}
	resMinus1 := ceilUint8(nums, res, -1)
	correctResMinus1 := []uint8{10, 10, 10, 10, 10, 20, 30, 40, 60, 90, 150, 200, 240}
	for i := range resMinus1 {
		require.Equal(t, correctResMinus1[i], resMinus1[i])
	}
	resMinus2 := ceilUint8(nums, res, -2)
	correctResMinus2 := []uint8{100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 200, 200, 0}
	for i := range resMinus2 {
		require.Equal(t, correctResMinus2[i], resMinus2[i])
	}
}
func TestCeilUint16(t *testing.T) {
	nums := []uint16{1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233, 377, 610, 987, 1597, 2584, 4181, 6765, 10946, 17711, 28657, 46368}
	res := make([]uint16, len(nums))
	res0 := CeilUint16(nums, res, 0)
	correctRes0 := []uint16{1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233, 377, 610, 987, 1597, 2584, 4181, 6765, 10946, 17711, 28657, 46368}
	for i := range res0 {
		require.Equal(t, correctRes0[i], res0[i])
	}
	resMinus1 := CeilUint16(nums, res, -1)
	correctResMinus1 := []uint16{10, 10, 10, 10, 10, 20, 30, 40, 60, 90, 150, 240, 380, 610, 990, 1600, 2590, 4190, 6770, 10950, 17720, 28660, 46370}
	for i := range resMinus1 {
		require.Equal(t, correctResMinus1[i], resMinus1[i])
	}
	resMinus2 := CeilUint16(nums, res, -2)
	correctResMinus2 := []uint16{100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 200, 300, 400, 700, 1000, 1600, 2600, 4200, 6800, 11000, 17800, 28700, 46400}
	for i := range resMinus2 {
		require.Equal(t, correctResMinus2[i], resMinus2[i])
	}
}
func TestCeilUint32(t *testing.T) {
	nums := []uint32{1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233, 377, 610, 987, 1597, 2584, 4181, 6765, 10946, 17711, 28657, 46368, 75025, 121393, 196418, 317811}
	res := make([]uint32, len(nums))
	res0 := CeilUint32(nums, res, 0)
	correctRes0 := []uint32{1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233, 377, 610, 987, 1597, 2584, 4181, 6765, 10946, 17711, 28657, 46368, 75025, 121393, 196418, 317811}
	for i := range res0 {
		require.Equal(t, res0[i], correctRes0[i])
	}
	resMinus1 := CeilUint32(nums, res, -1)
	correctResMinus1 := []uint32{10, 10, 10, 10, 10, 20, 30, 40, 60, 90, 150, 240, 380, 610, 990, 1600, 2590, 4190, 6770, 10950, 17720, 28660, 46370, 75030, 121400, 196420, 317820}
	for i := range res0 {
		require.Equal(t, resMinus1[i], correctResMinus1[i])
	}
	resMinus2 := CeilUint32(nums, res, -2)

	correctResMinus2 := []uint32{100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 200, 300, 400, 700, 1000, 1600, 2600, 4200, 6800, 11000, 17800, 28700, 46400, 75100, 121400, 196500, 317900}
	for i := range res0 {
		require.Equal(t, resMinus2[i], correctResMinus2[i])
	}
}

func TestCeilUint64(t *testing.T) {
	nums := []uint64{1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233, 377, 610, 987, 1597, 2584, 4181, 6765, 10946, 17711, 28657, 46368, 75025, 121393, 196418, 317811}
	res := make([]uint64, len(nums))
	res0 := CeilUint64(nums, res, 0)
	correctRes0 := []uint64{1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233, 377, 610, 987, 1597, 2584, 4181, 6765, 10946, 17711, 28657, 46368, 75025, 121393, 196418, 317811}
	for i := range res0 {
		require.Equal(t, res0[i], correctRes0[i])
	}
	resMinus1 := CeilUint64(nums, res, -1)
	correctResMinus1 := []uint64{10, 10, 10, 10, 10, 20, 30, 40, 60, 90, 150, 240, 380, 610, 990, 1600, 2590, 4190, 6770, 10950, 17720, 28660, 46370, 75030, 121400, 196420, 317820}
	for i := range res0 {
		require.Equal(t, resMinus1[i], correctResMinus1[i])
	}
	resMinus2 := CeilUint64(nums, res, -2)

	correctResMinus2 := []uint64{100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 200, 300, 400, 700, 1000, 1600, 2600, 4200, 6800, 11000, 17800, 28700, 46400, 75100, 121400, 196500, 317900}
	for i := range res0 {
		require.Equal(t, resMinus2[i], correctResMinus2[i])
	}
}

func TestCeilInt8(t *testing.T) {
	nums := []int8{1, 2, 3, 5, 8, 13, 21, 34, 55, 89, -1, -2, -3, -5, -8, -13, -21, -34, -55, -89}
	res := make([]int8, len(nums))
	res0 := CeilInt8(nums, res, 0)
	correctRes0 := []int8{1, 2, 3, 5, 8, 13, 21, 34, 55, 89, -1, -2, -3, -5, -8, -13, -21, -34, -55, -89}
	for i := range res0 {
		require.Equal(t, correctRes0[i], res0[i])
	}
	resMinus1 := CeilInt8(nums, res, -1)
	correctResMinus1 := []int8{10, 10, 10, 10, 10, 20, 30, 40, 60, 90, 0, 0, 0, 0, 0, -10, -20, -30, -50, -80}
	for i := range res0 {
		require.Equal(t, correctResMinus1[i], resMinus1[i])
	}
}

func TestCeilInt16(t *testing.T) {
	nums := []int16{1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233, 377, 610, 987, 1597, 2584, 4181, -1, -2, -3, -5, -8, -13, -21, -34, -55, -89, -144, -233, -377, -610, -987, -1597, -2584, -4181}
	res := make([]int16, len(nums))
	res0 := CeilInt16(nums, res, 0)
	correctRes0 := []int16{1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233, 377, 610, 987, 1597, 2584, 4181, -1, -2, -3, -5, -8, -13, -21, -34, -55, -89, -144, -233, -377, -610, -987, -1597, -2584, -4181}
	for i := range res0 {
		require.Equal(t, correctRes0[i], res0[i])
	}
	resMinus1 := CeilInt16(nums, res, -1)

	correctResMinus1 := []int16{10, 10, 10, 10, 10, 20, 30, 40, 60, 90, 150, 240, 380, 610, 990, 1600, 2590, 4190, 0, 0, 0, 0, 0, -10, -20, -30, -50, -80, -140, -230, -370, -610, -980, -1590, -2580, -4180}
	for i := range res0 {
		require.Equal(t, correctResMinus1[i], resMinus1[i])
	}
}

func TestCeilInt32(t *testing.T) {
	nums := []int32{1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233, 377, 610, 987, 1597, 2584, 4181, -1, -2, -3, -5, -8, -13, -21, -34, -55, -89, -144, -233, -377, -610, -987, -1597, -2584, -4181}
	res := make([]int32, len(nums))
	res0 := CeilInt32(nums, res, 0)
	correctRes0 := []int32{1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233, 377, 610, 987, 1597, 2584, 4181, -1, -2, -3, -5, -8, -13, -21, -34, -55, -89, -144, -233, -377, -610, -987, -1597, -2584, -4181}
	for i := range res0 {
		require.Equal(t, correctRes0[i], res0[i])
	}
	resMinus1 := CeilInt32(nums, res, -1)

	correctResMinus1 := []int32{10, 10, 10, 10, 10, 20, 30, 40, 60, 90, 150, 240, 380, 610, 990, 1600, 2590, 4190, 0, 0, 0, 0, 0, -10, -20, -30, -50, -80, -140, -230, -370, -610, -980, -1590, -2580, -4180}
	for i := range res0 {
		require.Equal(t, correctResMinus1[i], resMinus1[i])
	}
}

func TestCeilInt64(t *testing.T) {
	nums := []int64{1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233, 377, 610, 987, 1597, 2584, 4181, -1, -2, -3, -5, -8, -13, -21, -34, -55, -89, -144, -233, -377, -610, -987, -1597, -2584, -4181, 32768, 1234567}
	res := make([]int64, len(nums))
	res0 := CeilInt64(nums, res, 0)
	correctRes0 := []int64{1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233, 377, 610, 987, 1597, 2584, 4181, -1, -2, -3, -5, -8, -13, -21, -34, -55, -89, -144, -233, -377, -610, -987, -1597, -2584, -4181, 32768, 1234567}
	for i := range res0 {
		require.Equal(t, correctRes0[i], res0[i])
	}
	resMinus1 := CeilInt64(nums, res, -1)
	correctResMinus1 := []int64{10, 10, 10, 10, 10, 20, 30, 40, 60, 90, 150, 240, 380, 610, 990, 1600, 2590, 4190, 0, 0, 0, 0, 0, -10, -20, -30, -50, -80, -140, -230, -370, -610, -980, -1590, -2580, -4180, 32770, 1234570}
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

func TestCeilCeil32(t *testing.T) {
	nums := []float32{1.5, -1.5, 2.5, -2.5, 1.2, 12.3, 123.4, 1234.5, 12345.6, 1234.567, -1.2, -12.3, -123.4, -1234.5, -12345.6}
	res := make([]float32, len(nums))
	res0 := ceilFloat32(nums, res, 0)
	correctRes0 := []float32{2, -1, 3, -2, 2, 13, 124, 1235, 12346, 1235, -1, -12, -123, -1234, -12345}
	for i := range nums {
		assert.True(t, floatCompare(float64(correctRes0[i]), float64(res0[i])))
	}
	resMinus1 := ceilFloat32(nums, res, -1)
	correctResMinus1 := []float32{10, 0, 10, 0, 10, 20, 130, 1240, 12350, 1240, 0, -10, -120, -1230, -12340}
	for i := range nums {
		assert.True(t, floatCompare(float64(correctResMinus1[i]), float64(resMinus1[i])))
	}
	res1 := ceilFloat32(nums, res, 1)
	correctRes1 := []float32{1.5, -1.5, 2.5, -2.5, 1.2, 12.3, 123.4, 1234.5, 12345.6, 1234.6, -1.2, -12.3, -123.4, -1234.5, -12345.6}
	for i := range res1 {
		assert.True(t, floatCompare(float64(correctRes1[i]), float64(res1[i])))
	}
}

func TestCeilFloat64(t *testing.T) {
	nums := []float64{1.5, -1.5, 2.5, -2.5, 1.2, 12.3, 123.4, 1234.5, 12345.6, 1234.567, -1.2, -12.3, -123.4, -1234.5, -12345.6}
	res := make([]float64, len(nums))
	res0 := ceilFloat64(nums, res, 0)
	correctRes0 := []float64{2, -1, 3, -2, 2, 13, 124, 1235, 12346, 1235, -1, -12, -123, -1234, -12345}
	for i := range nums {
		assert.True(t, floatCompare(correctRes0[i], res0[i]))
	}
	resMinus1 := ceilFloat64(nums, res, -1)
	correctResMinus1 := []float64{10, 0, 10, 0, 10, 20, 130, 1240, 12350, 1240, 0, -10, -120, -1230, -12340}
	for i := range nums {
		assert.True(t, floatCompare(correctResMinus1[i], resMinus1[i]))
	}
	res1 := ceilFloat64(nums, res, 1)
	correctRes1 := []float64{1.5, -1.5, 2.5, -2.5, 1.2, 12.3, 123.4, 1234.5, 12345.6, 1234.6, -1.2, -12.3, -123.4, -1234.5, -12345.6}
	for i := range res1 {
		assert.True(t, floatCompare(correctRes1[i], res1[i]))
	}
}
