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
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/exp/constraints"
)

func myCot(x float64) float64 {
	return math.Cos(x) / math.Sin(x)
}

func testCot[T constraints.Integer | constraints.Float](nums []T, t *testing.T) {
	//Init a new variable
	tempNums := make([]float64, len(nums))
	//Run Cot function
	CotNums := Cot(nums, tempNums)
	for i, n := range nums {
		my := myCot(float64(n))
		require.InEpsilon(t, my, CotNums[i], 0.001)
	}
}

func TestCotFloat32(t *testing.T) {
	//Test values
	nums := []float32{1, 2, 3, 4, -1, -2}
	testCot(nums, t)
}

func TestCotFloat64(t *testing.T) {
	//Test values
	nums := []float64{1, 2, 3, 4, -1, -2}
	testCot(nums, t)
}

func TestCotUint8(t *testing.T) {
	//Test values
	nums := []uint8{1, 2, 3, 4, 10, 40}
	testCot(nums, t)
}

func TestCotUint16(t *testing.T) {
	//Test values
	nums := []uint16{1, 2, 3, 4, 10, 40}
	testCot(nums, t)
}

func TestCotUint32(t *testing.T) {
	//Test values
	nums := []uint32{1, 2, 3, 4, 10, 40}
	testCot(nums, t)
}

func TestCotUint64(t *testing.T) {
	//Test values
	nums := []uint64{1, 2, 3, 4, 10, 40}
	testCot(nums, t)
}

func TestCotInt8(t *testing.T) {
	//Test values
	nums := []int8{1, 2, 3, 4, 10, 40}
	testCot(nums, t)
}

func TestCotInt16(t *testing.T) {
	//Test values
	nums := []int16{1, 2, 3, 4, 10, 40}
	testCot(nums, t)
}

func TestCotInt32(t *testing.T) {
	//Test values
	nums := []int32{1, 2, 3, 4, 10, 40}
	testCot(nums, t)
}

func TestCotInt64(t *testing.T) {
	//Test values
	nums := []int64{1, 2, 3, 4, 10, 40}
	testCot(nums, t)
}
