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

package ascii

import (
	"bytes"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func MakeBytes(strs []string) *types.Bytes {
	ret := &types.Bytes{
		Lengths: make([]uint32, len(strs)),
		Offsets: make([]uint32, len(strs)),
	}
	cur := 0
	var buf bytes.Buffer
	for i, s := range strs {
		buf.WriteString(s)
		ret.Lengths[i] = uint32(len(s))
		ret.Offsets[i] = uint32(cur)
		cur += len(s)
	}
	ret.Data = buf.Bytes()
	return ret
}

func TestAsciiUint8(t *testing.T) {
	nums := []uint8{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 11, 33, 22, 55, 44}
	asciiNums := []uint8{48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 49, 51, 50, 53, 52}

	newNums := make([]uint8, len(nums))
	newNums = AsciiUint8(nums, newNums)

	for i := range newNums {
		require.Equal(t, asciiNums[i], newNums[i])
	}
}

func TestAsciiUint16(t *testing.T) {
	nums := []uint16{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 11, 33, 22, 55, 44}
	asciiNums := []uint8{48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 49, 51, 50, 53, 52}

	newNums := make([]uint8, len(nums))
	newNums = AsciiUint16(nums, newNums)

	for i := range newNums {
		require.Equal(t, asciiNums[i], newNums[i])
	}
}

func TestAsciiUint32(t *testing.T) {
	nums := []uint32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 11, 33, 22, 55, 44}
	asciiNums := []uint8{48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 49, 51, 50, 53, 52}

	newNums := make([]uint8, len(nums))
	newNums = AsciiUint32(nums, newNums)

	for i := range newNums {
		require.Equal(t, asciiNums[i], newNums[i])
	}
}

func TestAsciiUint64(t *testing.T) {
	nums := []uint64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 11, 33, 22, 55, 44}
	asciiNums := []uint8{48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 49, 51, 50, 53, 52}

	newNums := make([]uint8, len(nums))
	newNums = AsciiUint64(nums, newNums)

	for i := range newNums {
		require.Equal(t, asciiNums[i], newNums[i])
	}
}

func TestAsciiInt8(t *testing.T) {
	nums := []int8{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 11, 33, 22, 55, 44, -1, -2, -3, -4, -5}
	asciiNums := []uint8{48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 49, 51, 50, 53, 52, 45, 45, 45, 45, 45}

	newNums := make([]uint8, len(nums))
	newNums = AsciiInt8(nums, newNums)

	for i := range newNums {
		require.Equal(t, asciiNums[i], newNums[i])
	}
}

func TestAsciiInt16(t *testing.T) {
	nums := []int16{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 11, 33, 22, 55, 44, -1, -2, -3, -4, -5}
	asciiNums := []uint8{48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 49, 51, 50, 53, 52, 45, 45, 45, 45, 45}

	newNums := make([]uint8, len(nums))
	newNums = AsciiInt16(nums, newNums)

	for i := range newNums {
		require.Equal(t, asciiNums[i], newNums[i])
	}
}

func TestAsciiInt32(t *testing.T) {
	nums := []int32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 11, 33, 22, 55, 44, -1, -2, -3, -4, -5}
	asciiNums := []uint8{48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 49, 51, 50, 53, 52, 45, 45, 45, 45, 45}

	newNums := make([]uint8, len(nums))
	newNums = AsciiInt32(nums, newNums)

	for i := range newNums {
		require.Equal(t, asciiNums[i], newNums[i])
	}
}

func TestAsciiInt64(t *testing.T) {
	nums := []int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 11, 33, 22, 55, 44, -1, -2, -3, -4, -5}
	asciiNums := []uint8{48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 49, 51, 50, 53, 52, 45, 45, 45, 45, 45}

	newNums := make([]uint8, len(nums))
	newNums = AsciiInt64(nums, newNums)

	for i := range newNums {
		require.Equal(t, asciiNums[i], newNums[i])
	}
}

func TestAsciiFloat32(t *testing.T) {
	nums := []float32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 11, 33, 22, 55, 44, -1, -2, -3, -4, -5, 0.32, -0.7, 1.5, -1.5}
	asciiNums := []uint8{48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 49, 51, 50, 53, 52, 45, 45, 45, 45, 45, 48, 45, 49, 45}

	newNums := make([]uint8, len(nums))
	newNums = AsciiFloat32(nums, newNums)

	for i := range newNums {
		require.Equal(t, asciiNums[i], newNums[i])
	}
}

func TestAsciiFloat64(t *testing.T) {
	nums := []float64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 11, 33, 22, 55, 44, -1, -2, -3, -4, -5, 0.32, -0.7, 1.5, -1.5}
	asciiNums := []uint8{48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 49, 51, 50, 53, 52, 45, 45, 45, 45, 45, 48, 45, 49, 45}

	newNums := make([]uint8, len(nums))
	newNums = AsciiFloat64(nums, newNums)

	for i := range newNums {
		require.Equal(t, asciiNums[i], newNums[i])
	}
}

func TestAsciiBytes(t *testing.T) {
	tt := []struct {
		name string
		xs   *types.Bytes
		rs   []uint8
		want []uint8
	}{
		{
			name: "Empty",
			xs:   MakeBytes([]string{""}),
			rs:   make([]uint8, 1),
			want: []uint8{0},
		},
		{
			name: "Ascii",
			xs:   MakeBytes([]string{"dx", "Hello", "hello", "World", "world", " ", "\t", "\n", "\r"}),
			rs:   make([]uint8, 9),
			want: []uint8{100, 72, 104, 87, 119, 32, 9, 10, 13},
		},
		{
			name: "Not Ascii",
			xs:   MakeBytes([]string{"你好", "鐨", "é", "Ä"}),
			rs:   make([]uint8, 4),
			want: []uint8{228, 233, 195, 195},
		},
	}

	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			newNums := make([]uint8, len(tc.rs))
			rs := AsciiBytes(tc.xs, newNums)

			for i := range rs.Result {
				if tc.name == "Not Ascii" {
					require.Equal(t, nulls.Contains(rs.Nsp, uint64(i)), true)
				} else {
					require.Equal(t, nulls.Contains(rs.Nsp, uint64(i)), false)
					require.Equal(t, tc.want[i], rs.Result[i])
				}
			}
		})
	}
}
