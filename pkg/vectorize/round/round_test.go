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

package round

import (
	"fmt"
	"testing"
)

func TestRoundUint8(t *testing.T) {
	nums := []uint8{1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233}
	res := make([]uint8, len(nums))
	res_0 := roundUint8(nums, res, 0)
	for i, n := range nums {
		fmt.Printf("round(%d, 0) is %d\t", n, res_0[i])
	}
	fmt.Println()
	res_minus_1 := roundUint8(nums, res, -1)
	for i, n := range nums {
		fmt.Printf("round(%d, -1) is %d\t", n, res_minus_1[i])
	}
	fmt.Println()
	res_minus_2 := roundUint8(nums, res, -2)
	for i, n := range nums {
		fmt.Printf("round(%d, -2) is %d\t", n, res_minus_2[i])
	}
	fmt.Println()
}

func TestRoundUint16(t *testing.T) {
	nums := []uint16{1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233, 377, 610, 987, 1597, 2584, 4181, 6765, 10946, 17711, 28657, 46368}
	res := make([]uint16, len(nums))
	res_0 := roundUint16(nums, res, 0)
	for i, n := range nums {
		fmt.Printf("round(%d, 0) is %d\t", n, res_0[i])
	}
	fmt.Println()
	res_minus_1 := roundUint16(nums, res, -1)
	for i, n := range nums {
		fmt.Printf("round(%d, -1) is %d\t", n, res_minus_1[i])
	}
	fmt.Println()
	res_minus_2 := roundUint16(nums, res, -2)
	for i, n := range nums {
		fmt.Printf("round(%d, -2) is %d\t", n, res_minus_2[i])
	}
	fmt.Println()
}

func TestRoundUint32(t *testing.T) {
	nums := []uint32{1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233, 377, 610, 987, 1597, 2584, 4181, 6765, 10946, 17711, 28657, 46368, 75025, 121393, 196418, 317811}
	res := make([]uint32, len(nums))
	res_0 := roundUint32(nums, res, 0)
	for i, n := range nums {
		fmt.Printf("round(%d, 0) is %d\t", n, res_0[i])
	}
	fmt.Println()
	res_minus_1 := roundUint32(nums, res, -1)
	for i, n := range nums {
		fmt.Printf("round(%d, -1) is %d\t", n, res_minus_1[i])
	}
	fmt.Println()
	res_minus_2 := roundUint32(nums, res, -2)
	for i, n := range nums {
		fmt.Printf("round(%d, -2) is %d\t", n, res_minus_2[i])
	}
	fmt.Println()
}

func TestRoundUint64(t *testing.T) {
	nums := []uint64{1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233, 377, 610, 987, 1597, 2584, 4181, 6765, 10946, 17711, 28657, 46368, 75025, 121393, 196418, 317811}
	res := make([]uint64, len(nums))
	res_0 := roundUint64(nums, res, 0)
	for i, n := range nums {
		fmt.Printf("round(%d, 0) is %d\t", n, res_0[i])
	}
	fmt.Println()
	res_minus_1 := roundUint64(nums, res, -1)
	for i, n := range nums {
		fmt.Printf("round(%d, -1) is %d\t", n, res_minus_1[i])
	}
	fmt.Println()
	res_minus_2 := roundUint64(nums, res, -2)
	for i, n := range nums {
		fmt.Printf("round(%d, -2) is %d\t", n, res_minus_2[i])
	}
	fmt.Println()
}

func TestRoundInt8(t *testing.T) {
	nums := []int8{1, 2, 3, 5, 8, 13, 21, 34, 55, 89, -1, -2, -3, -5, -8, -13, -21, -34, -55, -89}
	res := make([]int8, len(nums))
	res_0 := roundInt8(nums, res, 0)
	for i, n := range nums {
		fmt.Printf("round(%d, 0) is %d\t", n, res_0[i])
	}
	fmt.Println()
	res_minus_1 := roundInt8(nums, res, -1)
	for i, n := range nums {
		fmt.Printf("round(%d, -1) is %d\t", n, res_minus_1[i])
	}
	fmt.Println()
	res_minus_2 := roundInt8(nums, res, -2)
	for i, n := range nums {
		fmt.Printf("round(%d, -2) is %d\t", n, res_minus_2[i])
	}
	fmt.Println()
}

func TestRoundInt16(t *testing.T) {
	nums := []int16{1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233, 377, 610, 987, 1597, 2584, 4181, -1, -2, -3, -5, -8, -13, -21, -34, -55, -89, -144, -233, -377, -610, -987, -1597, -2584, -4181}
	res := make([]int16, len(nums))
	res_0 := roundInt16(nums, res, 0)
	for i, n := range nums {
		fmt.Printf("round(%d, 0) is %d\t", n, res_0[i])
	}
	fmt.Println()
	res_minus_1 := roundInt16(nums, res, -1)
	for i, n := range nums {
		fmt.Printf("round(%d, -1) is %d\t", n, res_minus_1[i])
	}
	fmt.Println()
	res_minus_2 := roundInt16(nums, res, -2)
	for i, n := range nums {
		fmt.Printf("round(%d, -2) is %d\t", n, res_minus_2[i])
	}
	fmt.Println()
}

func TestRoundInt32(t *testing.T) {
	nums := []int32{1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233, 377, 610, 987, 1597, 2584, 4181, -1, -2, -3, -5, -8, -13, -21, -34, -55, -89, -144, -233, -377, -610, -987, -1597, -2584, -4181}
	res := make([]int32, len(nums))
	res_0 := roundInt32(nums, res, 0)
	for i, n := range nums {
		fmt.Printf("round(%d, 0) is %d\t", n, res_0[i])
	}
	fmt.Println()
	res_minus_1 := roundInt32(nums, res, -1)
	// res_minus_2 := roundUint16(nums, res, -2)
	for i, n := range nums {
		fmt.Printf("round(%d, -1) is %d\t", n, res_minus_1[i])
	}
	fmt.Println()
	res_minus_2 := roundInt32(nums, res, -2)
	for i, n := range nums {
		fmt.Printf("round(%d, -2) is %d\n", n, res_minus_2[i])
	}
	fmt.Println()
}

func TestRoundInt64(t *testing.T) {
	nums := []int64{1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233, 377, 610, 987, 1597, 2584, 4181, -1, -2, -3, -5, -8, -13, -21, -34, -55, -89, -144, -233, -377, -610, -987, -1597, -2584, -4181}
	res := make([]int64, len(nums))
	res_0 := roundInt64(nums, res, 0)
	for i, n := range nums {
		fmt.Printf("round(%d, 0) is %d\t", n, res_0[i])
	}
	fmt.Println()
	res_minus_1 := roundInt64(nums, res, -1)
	for i, n := range nums {
		fmt.Printf("round(%d, -1) is %d\t", n, res_minus_1[i])
	}
	fmt.Println()
	res_minus_2 := roundInt64(nums, res, -2)
	for i, n := range nums {
		fmt.Printf("round(%d, -2) is %d\t", n, res_minus_2[i])
	}
	fmt.Println()
}

func TestRoundFloat32(t *testing.T) {
	nums := []float32{1.5, -1.5, 2.5, -2.5, 1.2, 12.3, 123.4, 1234.5, 12345.6, 123456.7, 1234567.8, 123456789.0, 12345678.9, 1234567.89, 123456.789, 12345.6789, 1234.56789, -1.2, -12.3, -123.4, -1234.5, -12345.6, -123456.7, -1234567.8, -123456789.0, -12345678.9, -1234567.89, -123456.789, -12345.6789, -1234.56789}
	res := make([]float32, len(nums))
	res_0 := roundFloat32Pure(nums, res, 0)
	for i, n := range nums {
		fmt.Printf("round(%f, 0) is %f\t", n, res_0[i])
	}
	fmt.Println()
	res_1 := roundFloat32Pure(nums, res, 1)
	for i, n := range nums {
		fmt.Printf("round(%f, 1) is %f\t", n, res_1[i])
	}
	fmt.Println()
	res_2 := roundFloat32Pure(nums, res, 2)
	for i, n := range nums {
		fmt.Printf("round(%f, 2) is %f\t", n, res_2[i])
	}
	fmt.Println()
}

func TestRoundFloat64(t *testing.T) {
	nums := []float64{1.2, 12.3, 123.4, 1234.5, 12345.6, 123456.7, 1234567.8, 123456789.0, 12345678.9, 1234567.89, 123456.789, 12345.6789, 1234.56789, -1.2, -12.3, -123.4, -1234.5, -12345.6, -123456.7, -1234567.8, -123456789.0, -12345678.9, -1234567.89, -123456.789, -12345.6789, -1234.56789}
	res := make([]float64, len(nums))
	res_0 := roundFloat64Pure(nums, res, 0)
	for i, n := range nums {
		fmt.Printf("round(%f, j) is %f\t", n, res_0[i])
	}
	fmt.Println()
	res_minus_1 := roundFloat64Pure(nums, res, 1)
	for i, n := range nums {
		fmt.Printf("round(%f, 1) is %f\t", n, res_minus_1[i])
	}
	fmt.Println()
	res_minus_2 := roundFloat64Pure(nums, res, 2)
	for i, n := range nums {
		fmt.Printf("round(%f, 2) is %f\t", n, res_minus_2[i])
	}
	fmt.Println()
}
