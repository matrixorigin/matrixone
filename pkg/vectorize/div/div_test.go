// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package div

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestInt8Div(t *testing.T) {
	xs := []int8{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	ys := []int8{4, 3, 2, 1, -1, -2, -3, -4, -5, -6}
	rs := make([]int8, len(xs))
	rs = Int8Div(xs, ys, rs)
	rsCorrect := make([]int8, len(xs))
	for i := range xs {
		rsCorrect[i] = xs[i] / ys[i]
	}
	require.Equal(t, rsCorrect, rs)
}

func TestInt8DivSels(t *testing.T) {
	xs := []int8{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	ys := []int8{4, 3, 2, 1, -1, -2, -3, -4, -5, -6}
	selects := []int64{1, 3, 5, 6}
	rs := make([]int8, len(xs))
	rs = Int8DivSels(xs, ys, rs, selects)
	rsCorrect := make([]int8, len(xs))
	for _, sel := range selects {
		rsCorrect[sel] = xs[sel] / ys[sel]
	}
	require.Equal(t, rsCorrect, rs)
}

func TestInt8DivScalar(t *testing.T) {
	x := int8(6)
	ys := []int8{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	rs := make([]int8, len(ys))
	rs = Int8DivScalar(x, ys, rs)
	rsCorrect := make([]int8, len(ys))
	for i := range ys {
		rsCorrect[i] = x / ys[i]
	}
	require.Equal(t, rsCorrect, rs)
}

func TestInt8DivScalarSels(t *testing.T) {
	x := int8(6)
	ys := []int8{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	rs := make([]int8, len(ys))
	selects := []int64{1, 3, 5, 6}
	rs = Int8DivScalarSels(x, ys, rs, selects)
	rsCorrect := make([]int8, len(ys))
	for _, sel := range selects {
		rsCorrect[sel] = x / ys[sel]
	}
	require.Equal(t, rsCorrect, rs)
}

func TestInt8DivByScalar(t *testing.T) {
	x := int8(6)
	ys := []int8{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	rs := make([]int8, len(ys))
	rs = Int8DivByScalar(x, ys, rs)
	rsCorrect := make([]int8, len(ys))
	for i := range ys {
		rsCorrect[i] = ys[i] / x
	}
	require.Equal(t, rsCorrect, rs)
}

func TestInt8DivByScalarSels(t *testing.T) {
	x := int8(6)
	ys := []int8{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	rs := make([]int8, len(ys))
	selects := []int64{1, 3, 5, 6}
	rs = Int8DivByScalarSels(x, ys, rs, selects)
	rsCorrect := make([]int8, len(ys))
	for _, sel := range selects {
		rsCorrect[sel] = ys[sel] / x
	}
	require.Equal(t, rsCorrect, rs)
}

func TestInt16Div(t *testing.T) {
	xs := []int16{-500, -345, 123, 345, 567, 7890, 15}
	ys := []int16{10, 16, 29, 65, 5, 1, 4}
	rs := make([]int16, len(ys))
	rsCorrect := make([]int16, len(ys))
	rs = Int16Div(xs, ys, rs)
	for i := range xs {
		rsCorrect[i] = xs[i] / ys[i]
	}
	require.Equal(t, rsCorrect, rs)
}

func TestInt16DivSels(t *testing.T) {
	xs := []int16{-500, -345, 123, 345, 567, 7890, 15}
	ys := []int16{10, 16, 29, 65, 5, 1, 4}
	rs := make([]int16, len(ys))

	rsCorrect := make([]int16, len(ys))
	selects := []int64{1, 3, 5}
	rs = Int16DivSels(xs, ys, rs, selects)
	for _, sel := range selects {
		rsCorrect[sel] = xs[sel] / ys[sel]
	}
	require.Equal(t, rsCorrect, rs)
}

func TestInt16DivScalar(t *testing.T) {
	x := int16(123)
	ys := []int16{-500, -345, 123, 345, 567, 7890, 15}
	rs := make([]int16, len(ys))
	rsCorrect := make([]int16, len(ys))
	rs = Int16DivScalar(x, ys, rs)
	for i := range ys {
		rsCorrect[i] = x / ys[i]
	}
	require.Equal(t, rsCorrect, rs)
}

func TestInt16DivScalarSels(t *testing.T) {
	x := int16(123)
	ys := []int16{-500, -345, 123, 345, 567, 7890, 15}
	selects := []int64{1, 3, 5}
	rs := make([]int16, len(ys))
	rsCorrect := make([]int16, len(ys))
	rs = Int16DivScalarSels(x, ys, rs, selects)
	for _, sel := range selects {
		rsCorrect[sel] = x / ys[sel]
	}
	require.Equal(t, rsCorrect, rs)
}

func TestInt16DivByScalar(t *testing.T) {
	x := int16(123)
	ys := []int16{-500, -345, 123, 345, 567, 7890, 15}
	rs := make([]int16, len(ys))
	rsCorrect := make([]int16, len(ys))
	rs = Int16DivByScalar(x, ys, rs)
	for i := range ys {
		rsCorrect[i] = ys[i] / x
	}
	require.Equal(t, rsCorrect, rs)
}

func TestInt32Div(t *testing.T) {
	xs := []int16{-500, -345, 123, 345, 567, 7890, 15}
	ys := []int16{10, 16, 29, 65, 5, 1, 4}
	rs := make([]int16, len(ys))
	rsCorrect := make([]int16, len(ys))
	rs = Int16Div(xs, ys, rs)
	for i := range xs {
		rsCorrect[i] = xs[i] / ys[i]
	}
	require.Equal(t, rsCorrect, rs)
}

func TestInt32DivSels(t *testing.T) {
	xs := []int32{-500, -345, 123, 345, 567, 7890, 15}
	ys := []int32{10, 16, 29, 65, 5, 1, 4}
	rs := make([]int32, len(ys))

	rsCorrect := make([]int32, len(ys))
	selects := []int64{1, 3, 5}
	rs = Int32DivSels(xs, ys, rs, selects)
	for _, sel := range selects {
		rsCorrect[sel] = xs[sel] / ys[sel]
	}
	require.Equal(t, rsCorrect, rs)
}

func TestInt32DivScalar(t *testing.T) {
	x := int32(123)
	ys := []int32{-500, -345, 123, 345, 567, 7890, 15}
	rs := make([]int32, len(ys))
	rsCorrect := make([]int32, len(ys))
	rs = Int32DivScalar(x, ys, rs)
	for i := range ys {
		rsCorrect[i] = x / ys[i]
	}
	require.Equal(t, rsCorrect, rs)
}

func TestInt32DivScalarSels(t *testing.T) {
	x := int32(123)
	ys := []int32{-500, -345, 123, 345, 567, 7890, 15}
	selects := []int64{1, 3, 5}
	rs := make([]int32, len(ys))
	rsCorrect := make([]int32, len(ys))
	rs = Int32DivScalarSels(x, ys, rs, selects)
	for _, sel := range selects {
		rsCorrect[sel] = x / ys[sel]
	}
	require.Equal(t, rsCorrect, rs)
}

func TestInt32DivByScalar(t *testing.T) {
	x := int32(123)
	ys := []int32{-500, -345, 123, 345, 567, 7890, 15}
	rs := make([]int32, len(ys))
	rsCorrect := make([]int32, len(ys))
	rs = Int32DivByScalar(x, ys, rs)
	for i := range ys {
		rsCorrect[i] = ys[i] / x
	}
	require.Equal(t, rsCorrect, rs)
}

func TestInt64DivScalar(t *testing.T) {
	x := int64(123)
	ys := []int64{-500, -345, 123, 345, 567, 7890, 15}
	rs := make([]int64, len(ys))
	rsCorrect := make([]int64, len(ys))
	rs = Int64DivScalar(x, ys, rs)
	for i := range ys {
		rsCorrect[i] = x / ys[i]
	}
	require.Equal(t, rsCorrect, rs)
}

func TestInt64DivScalarSels(t *testing.T) {
	x := int64(123)
	ys := []int64{-500, -345, 123, 345, 567, 7890, 15}
	selects := []int64{1, 3, 5}
	rs := make([]int64, len(ys))
	rsCorrect := make([]int64, len(ys))
	rs = Int64DivScalarSels(x, ys, rs, selects)
	for _, sel := range selects {
		rsCorrect[sel] = x / ys[sel]
	}
	require.Equal(t, rsCorrect, rs)
}

func TestInt64DivByScalar(t *testing.T) {
	x := int64(123)
	ys := []int64{-500, -345, 123, 345, 567, 7890, 15}
	rs := make([]int64, len(ys))
	rsCorrect := make([]int64, len(ys))
	rs = Int64DivByScalar(x, ys, rs)
	for i := range ys {
		rsCorrect[i] = ys[i] / x
	}
	require.Equal(t, rsCorrect, rs)
}

func TestUint8Div(t *testing.T) {
	xs := []uint8{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	ys := []uint8{4, 3, 2, 1, 9, 12, 13, 24, 55, 96}
	rs := make([]uint8, len(xs))
	rs = Uint8Div(xs, ys, rs)
	rsCorrect := make([]uint8, len(xs))
	for i := range xs {
		rsCorrect[i] = xs[i] / ys[i]
	}
	require.Equal(t, rsCorrect, rs)
}

func TestUint8DivSels(t *testing.T) {
	xs := []uint8{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	ys := []uint8{4, 3, 2, 1, 9, 12, 13, 24, 55, 96}
	selects := []int64{1, 3, 5, 6}
	rs := make([]uint8, len(xs))
	rs = Uint8DivSels(xs, ys, rs, selects)
	rsCorrect := make([]uint8, len(xs))
	for _, sel := range selects {
		rsCorrect[sel] = xs[sel] / ys[sel]
	}
	require.Equal(t, rsCorrect, rs)
}

func TestUint8DivScalar(t *testing.T) {
	x := uint8(6)
	ys := []uint8{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	rs := make([]uint8, len(ys))
	rs = Uint8DivScalar(x, ys, rs)
	rsCorrect := make([]uint8, len(ys))
	for i := range ys {
		rsCorrect[i] = x / ys[i]
	}
	require.Equal(t, rsCorrect, rs)
}

func TestUint8DivScalarSels(t *testing.T) {
	x := uint8(6)
	ys := []uint8{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	rs := make([]uint8, len(ys))
	selects := []int64{1, 3, 5, 6}
	rs = Uint8DivScalarSels(x, ys, rs, selects)
	rsCorrect := make([]uint8, len(ys))
	for _, sel := range selects {
		rsCorrect[sel] = x / ys[sel]
	}
	require.Equal(t, rsCorrect, rs)
}

func TestUint8DivByScalar(t *testing.T) {
	x := uint8(6)
	ys := []uint8{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	rs := make([]uint8, len(ys))
	rs = Uint8DivByScalar(x, ys, rs)
	rsCorrect := make([]uint8, len(ys))
	for i := range ys {
		rsCorrect[i] = ys[i] / x
	}
	require.Equal(t, rsCorrect, rs)
}

func TestUint8DivByScalarSels(t *testing.T) {
	x := uint8(6)
	ys := []uint8{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	rs := make([]uint8, len(ys))
	selects := []int64{1, 3, 5, 6}
	rs = Uint8DivByScalarSels(x, ys, rs, selects)
	rsCorrect := make([]uint8, len(ys))
	for _, sel := range selects {
		rsCorrect[sel] = ys[sel] / x
	}
	require.Equal(t, rsCorrect, rs)
}

func TestUint16Div(t *testing.T) {
	xs := []uint16{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 20, 30, 44, 55, 1234, 12345}
	ys := []uint16{4, 3, 2, 1, 9, 12, 13, 24, 55, 96, 12, 10, 9, 6, 5, 15}
	rs := make([]uint16, len(xs))
	rs = Uint16Div(xs, ys, rs)
	rsCorrect := make([]uint16, len(xs))
	for i := range xs {
		rsCorrect[i] = xs[i] / ys[i]
	}
	require.Equal(t, rsCorrect, rs)
}

func TestUint16DivSels(t *testing.T) {
	xs := []uint16{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 20, 30, 44, 55, 1234, 12345}
	ys := []uint16{4, 3, 2, 1, 9, 12, 13, 24, 55, 96, 12, 10, 9, 6, 5, 15}
	selects := []int64{1, 3, 5, 6}
	rs := make([]uint16, len(xs))
	rs = Uint16DivSels(xs, ys, rs, selects)
	rsCorrect := make([]uint16, len(xs))
	for _, sel := range selects {
		rsCorrect[sel] = xs[sel] / ys[sel]
	}
	require.Equal(t, rsCorrect, rs)
}

func TestUint16DivScalar(t *testing.T) {
	x := uint16(6)
	ys := []uint16{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 20, 30, 44, 55, 1234, 12345}
	rs := make([]uint16, len(ys))
	rs = Uint16DivScalar(x, ys, rs)
	rsCorrect := make([]uint16, len(ys))
	for i := range ys {
		rsCorrect[i] = x / ys[i]
	}
	require.Equal(t, rsCorrect, rs)
}

func TestUint16DivScalarSels(t *testing.T) {
	x := uint16(6)
	ys := []uint16{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 20, 30, 44, 55, 1234, 12345}
	rs := make([]uint16, len(ys))
	selects := []int64{1, 3, 5, 6}
	rs = Uint16DivScalarSels(x, ys, rs, selects)
	rsCorrect := make([]uint16, len(ys))
	for _, sel := range selects {
		rsCorrect[sel] = x / ys[sel]
	}
	require.Equal(t, rsCorrect, rs)
}

func TestUint16DivByScalar(t *testing.T) {
	x := uint16(6)
	ys := []uint16{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 20, 30, 44, 55, 1234, 12345}
	rs := make([]uint16, len(ys))
	rs = Uint16DivByScalar(x, ys, rs)
	rsCorrect := make([]uint16, len(ys))
	for i := range ys {
		rsCorrect[i] = ys[i] / x
	}
	require.Equal(t, rsCorrect, rs)
}

func TestUint16DivByScalarSels(t *testing.T) {
	x := uint16(6)
	ys := []uint16{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 20, 30, 44, 55, 1234, 12345}
	rs := make([]uint16, len(ys))
	selects := []int64{1, 3, 5, 6}
	rs = Uint16DivByScalarSels(x, ys, rs, selects)
	rsCorrect := make([]uint16, len(ys))
	for _, sel := range selects {
		rsCorrect[sel] = ys[sel] / x
	}
	require.Equal(t, rsCorrect, rs)
}

func TestUint32Div(t *testing.T) {
	xs := []uint32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 20, 30, 44, 55, 1234, 12345}
	ys := []uint32{4, 3, 2, 1, 9, 12, 13, 24, 55, 96, 12, 10, 9, 6, 5, 15}
	rs := make([]uint32, len(xs))
	rs = Uint32Div(xs, ys, rs)
	rsCorrect := make([]uint32, len(xs))
	for i := range xs {
		rsCorrect[i] = xs[i] / ys[i]
	}
	require.Equal(t, rsCorrect, rs)
}

func TestUint32DivSels(t *testing.T) {
	xs := []uint32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 20, 30, 44, 55, 1234, 12345}
	ys := []uint32{4, 3, 2, 1, 9, 12, 13, 24, 55, 96, 12, 10, 9, 6, 5, 15}
	selects := []int64{1, 3, 5, 6}
	rs := make([]uint32, len(xs))
	rs = Uint32DivSels(xs, ys, rs, selects)
	rsCorrect := make([]uint32, len(xs))
	for _, sel := range selects {
		rsCorrect[sel] = xs[sel] / ys[sel]
	}
	require.Equal(t, rsCorrect, rs)
}

func TestUint32DivScalar(t *testing.T) {
	x := uint32(6)
	ys := []uint32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 20, 30, 44, 55, 1234, 12345}
	rs := make([]uint32, len(ys))
	rs = Uint32DivScalar(x, ys, rs)
	rsCorrect := make([]uint32, len(ys))
	for i := range ys {
		rsCorrect[i] = x / ys[i]
	}
	require.Equal(t, rsCorrect, rs)
}

func TestUint32DivScalarSels(t *testing.T) {
	x := uint32(6)
	ys := []uint32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 20, 30, 44, 55, 1234, 12345}
	rs := make([]uint32, len(ys))
	selects := []int64{1, 3, 5, 6}
	rs = Uint32DivScalarSels(x, ys, rs, selects)
	rsCorrect := make([]uint32, len(ys))
	for _, sel := range selects {
		rsCorrect[sel] = x / ys[sel]
	}
	require.Equal(t, rsCorrect, rs)
}

func TestUint32DivByScalar(t *testing.T) {
	x := uint32(6)
	ys := []uint32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 20, 30, 44, 55, 1234, 12345}
	rs := make([]uint32, len(ys))
	rs = Uint32DivByScalar(x, ys, rs)
	rsCorrect := make([]uint32, len(ys))
	for i := range ys {
		rsCorrect[i] = ys[i] / x
	}
	require.Equal(t, rsCorrect, rs)
}

func TestUint32DivByScalarSels(t *testing.T) {
	x := uint32(6)
	ys := []uint32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 20, 30, 44, 55, 1234, 12345}
	rs := make([]uint32, len(ys))
	selects := []int64{1, 3, 5, 6}
	rs = Uint32DivByScalarSels(x, ys, rs, selects)
	rsCorrect := make([]uint32, len(ys))
	for _, sel := range selects {
		rsCorrect[sel] = ys[sel] / x
	}
	require.Equal(t, rsCorrect, rs)
}

func TestUint64Div(t *testing.T) {
	xs := []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 20, 30, 44, 55, 1234, 12345}
	ys := []uint64{4, 3, 2, 1, 9, 12, 13, 24, 55, 96, 12, 10, 9, 6, 5, 15}
	rs := make([]uint64, len(xs))
	rs = Uint64Div(xs, ys, rs)
	rsCorrect := make([]uint64, len(xs))
	for i := range xs {
		rsCorrect[i] = xs[i] / ys[i]
	}
	require.Equal(t, rsCorrect, rs)
}

func TestUint64DivSels(t *testing.T) {
	xs := []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 20, 30, 44, 55, 1234, 12345}
	ys := []uint64{4, 3, 2, 1, 9, 12, 13, 24, 55, 96, 12, 10, 9, 6, 5, 15}
	selects := []int64{1, 3, 5, 6}
	rs := make([]uint64, len(xs))
	rs = Uint64DivSels(xs, ys, rs, selects)
	rsCorrect := make([]uint64, len(xs))
	for _, sel := range selects {
		rsCorrect[sel] = xs[sel] / ys[sel]
	}
	require.Equal(t, rsCorrect, rs)
}

func TestUint64DivScalar(t *testing.T) {
	x := uint64(6)
	ys := []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 20, 30, 44, 55, 1234, 12345}
	rs := make([]uint64, len(ys))
	rs = Uint64DivScalar(x, ys, rs)
	rsCorrect := make([]uint64, len(ys))
	for i := range ys {
		rsCorrect[i] = x / ys[i]
	}
	require.Equal(t, rsCorrect, rs)
}

func TestUint64DivScalarSels(t *testing.T) {
	x := uint64(6)
	ys := []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 20, 30, 44, 55, 1234, 12345}
	rs := make([]uint64, len(ys))
	selects := []int64{1, 3, 5, 6}
	rs = Uint64DivScalarSels(x, ys, rs, selects)
	rsCorrect := make([]uint64, len(ys))
	for _, sel := range selects {
		rsCorrect[sel] = x / ys[sel]
	}
	require.Equal(t, rsCorrect, rs)
}

func TestUint64DivByScalar(t *testing.T) {
	x := uint64(6)
	ys := []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 20, 30, 44, 55, 1234, 12345}
	rs := make([]uint64, len(ys))
	rs = Uint64DivByScalar(x, ys, rs)
	rsCorrect := make([]uint64, len(ys))
	for i := range ys {
		rsCorrect[i] = ys[i] / x
	}
	require.Equal(t, rsCorrect, rs)
}

func TestUint64DivByScalarSels(t *testing.T) {
	x := uint64(6)
	ys := []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 20, 30, 44, 55, 1234, 12345}
	rs := make([]uint64, len(ys))
	selects := []int64{1, 3, 5, 6}
	rs = Uint64DivByScalarSels(x, ys, rs, selects)
	rsCorrect := make([]uint64, len(ys))
	for _, sel := range selects {
		rsCorrect[sel] = ys[sel] / x
	}
	require.Equal(t, rsCorrect, rs)
}

func TestFloat32Div(t *testing.T) {
	xs := []float32{1.5, 2.6, 35.6, 44.4, 55.9, 126, 77.7, 88.8, 99.9, 110, 220, 330, 440, 505, 12.3, 123.45}
	ys := []float32{4, 3, 2, 1, 9, 12, 13.5, 24.5, 55.5, 96.6, 12.43, 10.5, 9.9, 6.2, 55, 15.6}
	rs := make([]float32, len(xs))
	rs = Float32Div(xs, ys, rs)
	rsCorrect := make([]float32, len(xs))
	for i := range xs {
		rsCorrect[i] = xs[i] / ys[i]
	}
	require.Equal(t, rsCorrect, rs)
}

func TestFloat32DivSels(t *testing.T) {
	xs := []float32{1.5, 2.6, 35.6, 44.4, 55.9, 126, 77.7, 88.8, 99.9, 110, 220, 330, 440, 505, 12.3, 123.45}
	ys := []float32{4, 3, 2, 1, 9, 12, 13.5, 24.5, 55.5, 96.6, 12.43, 10.5, 9.9, 6.2, 55, 15.6}
	selects := []int64{1, 3, 5, 6}
	rs := make([]float32, len(xs))
	rs = Float32DivSels(xs, ys, rs, selects)
	rsCorrect := make([]float32, len(xs))
	for _, sel := range selects {
		rsCorrect[sel] = xs[sel] / ys[sel]
	}
	require.Equal(t, rsCorrect, rs)
}

func TestFloat32DivScalar(t *testing.T) {
	x := float32(6)
	ys := []float32{1.5, 2.6, 35.6, 44.4, 55.9, 126, 77.7, 88.8, 99.9, 110, 220, 330, 440, 505, 12.3, 123.45}
	rs := make([]float32, len(ys))
	rs = Float32DivScalar(x, ys, rs)
	rsCorrect := make([]float32, len(ys))
	for i := range ys {
		rsCorrect[i] = x / ys[i]
	}
	require.Equal(t, rsCorrect, rs)
}

func TestFloat32DivScalarSels(t *testing.T) {
	x := float32(6)
	ys := []float32{1.5, 2.6, 35.6, 44.4, 55.9, 126, 77.7, 88.8, 99.9, 110, 220, 330, 440, 505, 12.3, 123.45}
	rs := make([]float32, len(ys))
	selects := []int64{1, 3, 5, 6}
	rs = Float32DivScalarSels(x, ys, rs, selects)
	rsCorrect := make([]float32, len(ys))
	for _, sel := range selects {
		rsCorrect[sel] = x / ys[sel]
	}
	require.Equal(t, rsCorrect, rs)
}

func TestFloat32DivByScalar(t *testing.T) {
	x := float32(6)
	ys := []float32{1.5, 2.6, 35.6, 44.4, 55.9, 126, 77.7, 88.8, 99.9, 110, 220, 330, 440, 505, 12.3, 123.45}
	rs := make([]float32, len(ys))
	rs = Float32DivByScalar(x, ys, rs)
	rsCorrect := make([]float32, len(ys))
	for i := range ys {
		rsCorrect[i] = ys[i] / x
	}
	require.Equal(t, rsCorrect, rs)
}

func TestFloat32DivByScalarSels(t *testing.T) {
	x := float32(6)
	ys := []float32{1.5, 2.6, 35.6, 44.4, 55.9, 126, 77.7, 88.8, 99.9, 110, 220, 330, 440, 505, 12.3, 123.45}
	rs := make([]float32, len(ys))
	selects := []int64{1, 3, 5, 6}
	rs = Float32DivByScalarSels(x, ys, rs, selects)
	rsCorrect := make([]float32, len(ys))
	for _, sel := range selects {
		rsCorrect[sel] = ys[sel] / x
	}
	require.Equal(t, rsCorrect, rs)
}

func TestFloat64Div(t *testing.T) {
	xs := []float64{1.5, 2.6, 35.6, 44.4, 55.9, 126, 77.7, 88.8, 99.9, 110, 220, 330, 440, 505, 12.3, 123.45}
	ys := []float64{4, 3, 2, 1, 9, 12, 13.5, 24.5, 55.5, 96.6, 12.43, 10.5, 9.9, 6.2, 55, 15.6}
	rs := make([]float64, len(xs))
	rs = Float64Div(xs, ys, rs)
	rsCorrect := make([]float64, len(xs))
	for i := range xs {
		rsCorrect[i] = xs[i] / ys[i]
	}
	require.Equal(t, rsCorrect, rs)
}

func TestFloat64DivSels(t *testing.T) {
	xs := []float64{1.5, 2.6, 35.6, 44.4, 55.9, 126, 77.7, 88.8, 99.9, 110, 220, 330, 440, 505, 12.3, 123.45}
	ys := []float64{4, 3, 2, 1, 9, 12, 13.5, 24.5, 55.5, 96.6, 12.43, 10.5, 9.9, 6.2, 55, 15.6}
	selects := []int64{1, 3, 5, 6}
	rs := make([]float64, len(xs))
	rs = Float64DivSels(xs, ys, rs, selects)
	rsCorrect := make([]float64, len(xs))
	for _, sel := range selects {
		rsCorrect[sel] = xs[sel] / ys[sel]
	}
	require.Equal(t, rsCorrect, rs)
}

func TestFloat64DivScalar(t *testing.T) {
	x := float64(6)
	ys := []float64{1.5, 2.6, 35.6, 44.4, 55.9, 126, 77.7, 88.8, 99.9, 110, 220, 330, 440, 505, 12.3, 123.45}
	rs := make([]float64, len(ys))
	rs = Float64DivScalar(x, ys, rs)
	rsCorrect := make([]float64, len(ys))
	for i := range ys {
		rsCorrect[i] = x / ys[i]
	}
	require.Equal(t, rsCorrect, rs)
}

func TestFloat64DivScalarSels(t *testing.T) {
	x := float64(6)
	ys := []float64{1.5, 2.6, 35.6, 44.4, 55.9, 126, 77.7, 88.8, 99.9, 110, 220, 330, 440, 505, 12.3, 123.45}
	rs := make([]float64, len(ys))
	selects := []int64{1, 3, 5, 6}
	rs = Float64DivScalarSels(x, ys, rs, selects)
	rsCorrect := make([]float64, len(ys))
	for _, sel := range selects {
		rsCorrect[sel] = x / ys[sel]
	}
	require.Equal(t, rsCorrect, rs)
}

func TestFloat64DivByScalar(t *testing.T) {
	x := float64(6)
	ys := []float64{1.5, 2.6, 35.6, 44.4, 55.9, 126, 77.7, 88.8, 99.9, 110, 220, 330, 440, 505, 12.3, 123.45}
	rs := make([]float64, len(ys))
	rs = Float64DivByScalar(x, ys, rs)
	rsCorrect := make([]float64, len(ys))
	for i := range ys {
		rsCorrect[i] = ys[i] / x
	}
	require.Equal(t, rsCorrect, rs)
}

func TestFloat64DivByScalarSels(t *testing.T) {
	x := float64(6)
	ys := []float64{1.5, 2.6, 35.6, 44.4, 55.9, 126, 77.7, 88.8, 99.9, 110, 220, 330, 440, 505, 12.3, 123.45}
	rs := make([]float64, len(ys))
	selects := []int64{1, 3, 5, 6}
	rs = Float64DivByScalarSels(x, ys, rs, selects)
	rsCorrect := make([]float64, len(ys))
	for _, sel := range selects {
		rsCorrect[sel] = ys[sel] / x
	}
	require.Equal(t, rsCorrect, rs)
}

func TestFloat64IntegerDiv(t *testing.T) {
	xs := []float64{1.5, 2.6, 35.6, 44.4, 55.9, 126, 77.7, 88.8, 99.9, 110, 220, 330, 440, 505, 12.3, 123.45}
	ys := []float64{4, 3, 2, 1, 9, 12, 13.5, 24.5, 55.5, 96.6, 12.43, 10.5, 9.9, 6.2, 55, 15.6}
	rs := make([]int64, len(xs))
	rs = Float64IntegerDiv(xs, ys, rs)
	rsCorrect := make([]int64, len(xs))
	for i := range xs {
		rsCorrect[i] = int64(xs[i] / ys[i])
	}
	require.Equal(t, rsCorrect, rs)
}

func TestFloat64IntegerDivSels(t *testing.T) {
	xs := []float64{1.5, 2.6, 35.6, 44.4, 55.9, 126, 77.7, 88.8, 99.9, 110, 220, 330, 440, 505, 12.3, 123.45}
	ys := []float64{4, 3, 2, 1, 9, 12, 13.5, 24.5, 55.5, 96.6, 12.43, 10.5, 9.9, 6.2, 55, 15.6}
	selects := []int64{1, 3, 5, 6}
	rs := make([]int64, len(xs))
	rs = Float64IntegerDivSels(xs, ys, rs, selects)
	rsCorrect := make([]int64, len(xs))
	for _, sel := range selects {
		rsCorrect[sel] = int64(xs[sel] / ys[sel])
	}
	require.Equal(t, rsCorrect, rs)
}

func TestFloat64IntegerDivScalar(t *testing.T) {
	x := float64(6)
	ys := []float64{1.5, 2.6, 35.6, 44.4, 55.9, 126, 77.7, 88.8, 99.9, 110, 220, 330, 440, 505, 12.3, 123.45}
	rs := make([]int64, len(ys))
	rs = Float64IntegerDivScalar(x, ys, rs)
	rsCorrect := make([]int64, len(ys))
	for i := range ys {
		rsCorrect[i] = int64(x / ys[i])
	}
	require.Equal(t, rsCorrect, rs)
}

func TestFloat64IntegerDivScalarSels(t *testing.T) {
	x := float64(6)
	ys := []float64{1.5, 2.6, 35.6, 44.4, 55.9, 126, 77.7, 88.8, 99.9, 110, 220, 330, 440, 505, 12.3, 123.45}
	rs := make([]int64, len(ys))
	selects := []int64{1, 3, 5, 6}
	rs = Float64IntegerDivScalarSels(x, ys, rs, selects)
	rsCorrect := make([]int64, len(ys))
	for _, sel := range selects {
		rsCorrect[sel] = int64(x / ys[sel])
	}
	require.Equal(t, rsCorrect, rs)
}

func TestFloat64IntegerDivByScalar(t *testing.T) {
	x := float64(6)
	ys := []float64{1.5, 2.6, 35.6, 44.4, 55.9, 126, 77.7, 88.8, 99.9, 110, 220, 330, 440, 505, 12.3, 123.45}
	rs := make([]int64, len(ys))
	rs = Float64IntegerDivByScalar(x, ys, rs)
	rsCorrect := make([]int64, len(ys))
	for i := range ys {
		rsCorrect[i] = int64(ys[i] / x)
	}
	require.Equal(t, rsCorrect, rs)
}

func TestFloat64IntegerDivByScalarSels(t *testing.T) {
	x := float64(6)
	ys := []float64{1.5, 2.6, 35.6, 44.4, 55.9, 126, 77.7, 88.8, 99.9, 110, 220, 330, 440, 505, 12.3, 123.45}
	rs := make([]int64, len(ys))
	selects := []int64{1, 3, 5, 6}
	rs = Float64IntegerDivByScalarSels(x, ys, rs, selects)
	rsCorrect := make([]int64, len(ys))
	for _, sel := range selects {
		rsCorrect[sel] = int64(ys[sel] / x)
	}
	require.Equal(t, rsCorrect, rs)
}
