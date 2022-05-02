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

package add

import (
	"fmt"
	"testing"

	"golang.org/x/exp/constraints"
)

func makeBuf[T constraints.Integer | constraints.Float](n int) []T {
	buf := make([]T, n)
	for i := range buf {
		buf[i] = T(i)
	}
	return buf
}

func checkBuf[T constraints.Integer | constraints.Float](a, b []T) error {
	if len(a) != len(b) {
		return fmt.Errorf("Length diff %d != %d", len(a), len(b))
	}
	for i := range a {
		if a[i] != b[i] {
			return fmt.Errorf("Check failed, %d-th number differ %v != %v", i, a[i], b[i])
		}
	}
	return nil
}

func dumbAdd[T constraints.Integer | constraints.Float](a, b []T) []T {
	c := make([]T, len(a))
	for i := range a {
		c[i] = a[i] + b[i]
	}
	return c
}

func testAdd[T constraints.Integer | constraints.Float]() error {
	x := makeBuf[T](4096)
	y := makeBuf[T](4096)
	res := dumbAdd(x, y)
	res2 := numericAdd(x, x, y)
	return checkBuf(res, res2)
}

func TestAddT(t *testing.T) {
	if err := testAdd[int8](); err != nil {
		t.Errorf("add int8 failed, %v", err)
	}
	if err := testAdd[int16](); err != nil {
		t.Errorf("add int16 failed, %v", err)
	}
	if err := testAdd[int32](); err != nil {
		t.Errorf("add int32 failed, %v", err)
	}
	if err := testAdd[int64](); err != nil {
		t.Errorf("add int64 failed, %v", err)
	}

	if err := testAdd[uint8](); err != nil {
		t.Errorf("add uint8 failed, %v", err)
	}
	if err := testAdd[uint16](); err != nil {
		t.Errorf("add uint16 failed, %v", err)
	}
	if err := testAdd[uint32](); err != nil {
		t.Errorf("add uint32 failed, %v", err)
	}
	if err := testAdd[uint64](); err != nil {
		t.Errorf("add uint64 failed, %v", err)
	}
	if err := testAdd[float32](); err != nil {
		t.Errorf("add float32 failed, %v", err)
	}
	if err := testAdd[float64](); err != nil {
		t.Errorf("add float64 failed, %v", err)
	}
}

func TestAddOF(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			// OK, expected.  nothing to see here
		} else {
			t.Errorf("Should never reach here")
		}
	}()

	x := makeBuf[int8](16)
	y := makeBuf[int8](16)
	res := dumbAdd(x, y)
	res2 := cInt8Add(x, x, y)
	if err := checkBuf(res, res2); err != nil {
		t.Errorf("add float64 failed, %v", err)
	}

	x = makeBuf[int8](4096)
	y = makeBuf[int8](4096)
	res2 = cInt8Add(x, y, y)
	t.Errorf("Should never reach here")
	if err := checkBuf(res, res2); err != nil {
		t.Errorf("add float64 failed, %v", err)
	}
}

func BenchmarkInt8Add(b *testing.B) {
	x := make([]int8, 4096)
	y := make([]int8, 4096)
	for i := 0; i < 4096; i++ {
		x[i] = 1
		y[i] = 2
	}
	z := make([]int8, 4096)

	for n := 0; n < b.N; n++ {
		z = Int8Add(x, y, z)
	}
}

func Benchmark_CInt8Add(b *testing.B) {
	x := make([]int8, 4096)
	y := make([]int8, 4096)
	for i := 0; i < 4096; i++ {
		x[i] = 1
		y[i] = 2
	}
	z := make([]int8, 4096)

	for n := 0; n < b.N; n++ {
		z = cInt8Add(x, y, z)
	}
}

func Benchmark_CInt64Add(b *testing.B) {
	x := make([]int64, 4096)
	y := make([]int64, 4096)
	for i := 0; i < 4096; i++ {
		x[i] = 1
		y[i] = 2
	}
	z := make([]int64, 4096)

	for n := 0; n < b.N; n++ {
		z = cInt64Add(x, y, z)
	}
}

func Benchmark_CUint8Add(b *testing.B) {
	x := make([]uint8, 4096)
	y := make([]uint8, 4096)
	for i := 0; i < 4096; i++ {
		x[i] = 1
		y[i] = 2
	}
	z := make([]uint8, 4096)

	for n := 0; n < b.N; n++ {
		z = cUint8Add(x, y, z)
	}
}

func Benchmark_CUint64Add(b *testing.B) {
	x := make([]uint64, 4096)
	y := make([]uint64, 4096)
	for i := 0; i < 4096; i++ {
		x[i] = 1
		y[i] = 2
	}
	z := make([]uint64, 4096)

	for n := 0; n < b.N; n++ {
		z = cUint64Add(x, y, z)
	}
}
