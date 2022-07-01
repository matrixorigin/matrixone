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

package oct

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestOctUint8(t *testing.T) {
	args := []uint8{12, 99, 100, 255}
	want := &types.Bytes{
		Data:    []byte("14143144377"),
		Lengths: []uint32{2, 3, 3, 3},
		Offsets: []uint32{0, 2, 5, 8},
	}

	out := &types.Bytes{
		Data:    []byte{},
		Lengths: make([]uint32, len(args)),
		Offsets: make([]uint32, len(args)),
	}
	out = OctUint8(args, out)
	require.Equal(t, want, out)
}

func TestOctUint16(t *testing.T) {
	args := []uint16{12, 99, 100, 255, 1024, 10000, 65535}
	want := &types.Bytes{
		Data:    []byte("14143144377200023420177777"),
		Lengths: []uint32{2, 3, 3, 3, 4, 5, 6},
		Offsets: []uint32{0, 2, 5, 8, 11, 15, 20},
	}

	out := &types.Bytes{
		Data:    []byte{},
		Lengths: make([]uint32, len(args)),
		Offsets: make([]uint32, len(args)),
	}
	out = OctUint16(args, out)
	require.Equal(t, want, out)
}

func TestOctUint32(t *testing.T) {
	args := []uint32{12, 99, 100, 255, 1024, 10000, 65535, 4294967295}
	want := &types.Bytes{
		Data:    []byte("1414314437720002342017777737777777777"),
		Lengths: []uint32{2, 3, 3, 3, 4, 5, 6, 11},
		Offsets: []uint32{0, 2, 5, 8, 11, 15, 20, 26},
	}

	out := &types.Bytes{
		Data:    []byte{},
		Lengths: make([]uint32, len(args)),
		Offsets: make([]uint32, len(args)),
	}
	out = OctUint32(args, out)
	require.Equal(t, want, out)
}

func TestOctUint64(t *testing.T) {
	args := []uint64{12, 99, 100, 255, 1024, 10000, 65535, 4294967295, 18446744073709551615}
	want := &types.Bytes{
		Data:    []byte("14143144377200023420177777377777777771777777777777777777777"),
		Lengths: []uint32{2, 3, 3, 3, 4, 5, 6, 11, 22},
		Offsets: []uint32{0, 2, 5, 8, 11, 15, 20, 26, 37},
	}

	out := &types.Bytes{
		Data:    []byte{},
		Lengths: make([]uint32, len(args)),
		Offsets: make([]uint32, len(args)),
	}
	out = OctUint64(args, out)
	require.Equal(t, want, out)
}

func TestOctInt8(t *testing.T) {
	args := []int8{-128, -1, 127}
	want := &types.Bytes{
		Data:    []byte("17777777777777777776001777777777777777777777177"),
		Lengths: []uint32{22, 22, 3},
		Offsets: []uint32{0, 22, 44},
	}

	out := &types.Bytes{
		Data:    []byte{},
		Lengths: make([]uint32, len(args)),
		Offsets: make([]uint32, len(args)),
	}
	out = OctInt8(args, out)
	require.Equal(t, want, out)
}

func TestOctInt16(t *testing.T) {
	args := []int16{-32768}
	want := &types.Bytes{
		Data:    []byte("1777777777777777700000"),
		Lengths: []uint32{22},
		Offsets: []uint32{0},
	}

	out := &types.Bytes{
		Data:    []byte{},
		Lengths: make([]uint32, len(args)),
		Offsets: make([]uint32, len(args)),
	}
	out = OctInt16(args, out)
	require.Equal(t, want, out)
}

func TestOctInt32(t *testing.T) {
	args := []int32{-2147483648}
	want := &types.Bytes{
		Data:    []byte("1777777777760000000000"),
		Lengths: []uint32{22},
		Offsets: []uint32{0},
	}

	out := &types.Bytes{
		Data:    []byte{},
		Lengths: make([]uint32, len(args)),
		Offsets: make([]uint32, len(args)),
	}
	out = OctInt32(args, out)
	require.Equal(t, want, out)
}

func TestOctInt64(t *testing.T) {
	args := []int64{-9223372036854775808}
	want := &types.Bytes{
		Data:    []byte("1000000000000000000000"),
		Lengths: []uint32{22},
		Offsets: []uint32{0},
	}

	out := &types.Bytes{
		Data:    []byte{},
		Lengths: make([]uint32, len(args)),
		Offsets: make([]uint32, len(args)),
	}
	out = OctInt64(args, out)
	require.Equal(t, want, out)
}
