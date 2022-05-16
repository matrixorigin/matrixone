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

package bin

import (
	"bytes"
	"math"
	"math/bits"
	"strconv"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestCountBitLenForInt(t *testing.T) {
	// count bits for unsigned int
	// eg: 0(0), 1(1), 2(10), 3(11)
	ttUint64 := []struct {
		num  uint64
		want int64
	}{
		{0, 1},
		{1, 1},
		{2, 2},
		{3, 2},
		{4, 3},
		{8, 4},
		{127, 7},
		{128, 8},
		{math.MaxUint64, 64},
	}

	for _, tc := range ttUint64 {
		require.Equal(t, tc.want, Uint64BitLen([]uint64{tc.num}), tc.num)
	}

	// count bits for signed int
	// eg: -1(0xffffffffffffffff, 64bits)
	// 127(1111111)
	// -128(1111111111111111111111111111111111111111111111111111111110000000, 64bits)
	ttInt64 := []struct {
		num  int64
		want int64
	}{
		{-1, 64},
		{127, 7},
		{-128, 64},
		{17, 5},
		{200, 8},
		{-1e7, 64},
		{-1e9, 64},
		{1e7, 24},
		{1e9, 30},
	}
	for _, tc := range ttInt64 {
		require.Equal(t, tc.want, Int64BitLen([]int64{tc.num}), tc.num)
	}
}

// due to differences between x86/arm, this TestCountBitLenForFloat function has some compatibility issues and therefore commented out
func TestCountBitLenForFloat(t *testing.T) {
	// count bits for float
	// eg: 0.2(0), 1.8(1), 2.99(10), 3.14(11)
	ttF32 := []struct {
		num  float32
		want int64
	}{
		{.2, 1},
		{1.8, 1},
		{2.99, 2},
		{3.14, 2},
		{-1.99, 64},
		{127.99, 7},
		{-128.99, 64},
		{100.99, 7},
		{200.99, 8},
	}

	for _, tc := range ttF32 {
		require.Equal(t, tc.want, Float32BitLen([]float32{tc.num}), tc.num)
	}

	ttF64 := []struct {
		num  float64
		want int64
	}{
		{-1e7, 64},
		{-1e9, 64},
		{1e7, 24},
		{1e9, 30},
		// Phi=1.61...(1), E=2.7(10), Pi=3.14(11)
		{math.Phi, 1},
		{math.E, 2},
		{math.Pi, 2},
		// max length of bits should be 64 to follow the behavior of mysql 8.0
		{math.MaxFloat64, 64},
		{-math.MaxFloat64, 64},
		{-math.MaxUint64 - 1, 64},
		{math.MaxUint64 + 1, 64},
	}

	for _, tc := range ttF64 {
		require.Equal(t, tc.want, Float64BitLen([]float64{tc.num}), strconv.FormatFloat(tc.num, 'f', -1, 64))
	}
}

func TestUnsignedIntToBinary(t *testing.T) {
	cases := []uint64{0, 1, 2, 3, 127, 128}
	var buf bytes.Buffer
	for _, x := range cases {
		buf.WriteString(strconv.FormatUint(x, 2))
	}

	bytesNeed := Uint64BitLen(cases)
	ret := &types.Bytes{
		Data:    make([]byte, bytesNeed),
		Lengths: make([]uint32, len(cases)),
		Offsets: make([]uint32, len(cases)),
	}
	ret = Uint64ToBinary(cases, ret)
	var (
		len, offset int
	)
	for i, x := range cases {
		len = bits.Len64(x)
		if x == 0 {
			len = 1
		}
		require.Equal(t, uint32(len), ret.Lengths[i])
		require.Equal(t, uint32(offset), ret.Offsets[i])
		offset += len
	}

	require.Equal(t, buf.Bytes(), ret.Data)
}

func TestIntToBinary(t *testing.T) {
	cases := []int64{-1, 127, -128, 1e9, 1e7, -1e9}
	var buf bytes.Buffer
	for _, x := range cases {
		buf.WriteString(strconv.FormatUint(uint64(x), 2))
	}

	bytesNeed := Int64BitLen(cases)
	ret := &types.Bytes{
		Data:    make([]byte, bytesNeed),
		Lengths: make([]uint32, len(cases)),
		Offsets: make([]uint32, len(cases)),
	}
	ret = Int64ToBinary(cases, ret)

	var (
		len, offset int
	)
	for i, x := range cases {
		len = bits.Len64(uint64(x))
		require.Equal(t, uint32(len), ret.Lengths[i])
		require.Equal(t, uint32(offset), ret.Offsets[i])
		offset += len
	}

	require.Equal(t, buf.Bytes(), ret.Data)
}

func TestFloatToBinary(t *testing.T) {
	cases := []float64{float64(math.Phi), float64(math.E), float64(math.Pi)}
	var buf bytes.Buffer
	for _, x := range cases {
		buf.WriteString(strconv.FormatUint(uint64(x), 2))
	}

	bytesNeed := Float64BitLen(cases)
	ret := &types.Bytes{
		Data:    make([]byte, bytesNeed),
		Lengths: make([]uint32, len(cases)),
		Offsets: make([]uint32, len(cases)),
	}
	ret = Float64ToBinary(cases, ret)

	var (
		length, offset int
	)
	for i, x := range cases {
		length = bits.Len64(uint64(x))
		require.Equal(t, uint32(length), ret.Lengths[i])
		require.Equal(t, uint32(offset), ret.Offsets[i])
		offset += length
	}

	require.Equal(t, buf.Bytes(), ret.Data)
}

func TestFormatUintToBinary(t *testing.T) {
	ttUint := []struct {
		num  uint64
		want string
	}{
		{0, "0"},
		{1, "1"},
		{2, "10"},
		{3, "11"},
		{127, "1111111"},
		{1e7, "100110001001011010000000"},
		{1e9, "111011100110101100101000000000"},
	}

	for _, tc := range ttUint {
		require.Equal(t, uintToBinStr(tc.num), tc.want, tc.num)
	}
}
