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

package substring

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestSliceFromLeftConstantOffsetUnbounded(t *testing.T) {
	cases := []struct {
		name  string
		args1 *types.Bytes
		start int64
		want  *types.Bytes
	}{
		{
			name: "Test01",
			args1: &types.Bytes{
				Data:    []byte("abcdefghijklmn"),
				Lengths: []uint32{uint32(len("abcdefghijklmn"))},
				Offsets: []uint32{0},
			},
			start: 5,
			want: &types.Bytes{
				Data:    []byte("efghijklmn"),
				Lengths: []uint32{uint32(len("efghijklmn"))},
				Offsets: []uint32{0},
			},
		},
		{
			name: "Test02",
			args1: &types.Bytes{
				Data:    []byte("abcdefghijklmn"),
				Lengths: []uint32{uint32(len("abcdefghijklmn"))},
				Offsets: []uint32{0},
			},
			start: 12,
			want: &types.Bytes{
				Data:    []byte("lmn"),
				Lengths: []uint32{uint32(len("lmn"))},
				Offsets: []uint32{0},
			},
		},
		{
			name: "Test03",
			args1: &types.Bytes{
				Data:    []byte("abcdefghijklmn"),
				Lengths: []uint32{uint32(len("abcdefghijklmn"))},
				Offsets: []uint32{0},
			},
			start: 15,
			want: &types.Bytes{
				Data:    []byte(""),
				Lengths: []uint32{uint32(len(""))},
				Offsets: []uint32{0},
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			out := &types.Bytes{
				Data:    make([]byte, len(c.args1.Data)),
				Lengths: make([]uint32, len(c.args1.Lengths)),
				Offsets: make([]uint32, len(c.args1.Offsets)),
			}

			got := SliceFromLeftConstantOffsetUnbounded(c.args1, out, c.start-1)
			require.Equal(t, c.want.String(), got.String())
		})
	}
}

func TestSliceFromRightConstantOffsetUnbounded(t *testing.T) {
	cases := []struct {
		name  string
		args1 *types.Bytes
		start int64
		want  *types.Bytes
	}{
		{
			name: "Test01",
			args1: &types.Bytes{
				Data:    []byte("abcdefghijklmn"),
				Lengths: []uint32{uint32(len("abcdefghijklmn"))},
				Offsets: []uint32{0},
			},
			start: -5,
			want: &types.Bytes{
				Data:    []byte("jklmn"),
				Lengths: []uint32{uint32(len("jklmn"))},
				Offsets: []uint32{0},
			},
		},
		{
			name: "Test02",
			args1: &types.Bytes{
				Data:    []byte("abcdefghijklmn"),
				Lengths: []uint32{uint32(len("abcdefghijklmn"))},
				Offsets: []uint32{0},
			},
			start: -14,
			want: &types.Bytes{
				Data:    []byte("abcdefghijklmn"),
				Lengths: []uint32{uint32(len("abcdefghijklmn"))},
				Offsets: []uint32{0},
			},
		},
		{
			name: "Test03",
			args1: &types.Bytes{
				Data:    []byte("abcdefghijklmn"),
				Lengths: []uint32{uint32(len("abcdefghijklmn"))},
				Offsets: []uint32{0},
			},
			start: -16,
			want: &types.Bytes{
				Data:    []byte("abcdefghijklmn"),
				Lengths: []uint32{uint32(len("abcdefghijklmn"))},
				Offsets: []uint32{0},
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			out := &types.Bytes{
				Data:    make([]byte, len(c.args1.Data)),
				Lengths: make([]uint32, len(c.args1.Lengths)),
				Offsets: make([]uint32, len(c.args1.Offsets)),
			}

			got := SliceFromRightConstantOffsetUnbounded(c.args1, out, -c.start)
			require.Equal(t, c.want.String(), got.String())
		})
	}
}

func TestSliceFromZeroConstantOffsetUnbounded(t *testing.T) {
	cases := []struct {
		name  string
		args1 *types.Bytes
		start int64
		want  *types.Bytes
	}{
		{
			name: "Test01",
			args1: &types.Bytes{
				Data:    []byte("abcdefghijklmn"),
				Lengths: []uint32{uint32(len("abcdefghijklmn"))},
				Offsets: []uint32{0},
			},
			start: 0,
			want: &types.Bytes{
				Data:    []byte(""),
				Lengths: []uint32{uint32(len(""))},
				Offsets: []uint32{0},
			},
		},
		{
			name: "Test02",
			args1: &types.Bytes{
				Data:    []byte("abcd132456"),
				Lengths: []uint32{uint32(len("abcd132456"))},
				Offsets: []uint32{0},
			},
			start: 0,
			want: &types.Bytes{
				Data:    []byte(""),
				Lengths: []uint32{uint32(len(""))},
				Offsets: []uint32{0},
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			out := &types.Bytes{
				Data:    make([]byte, len(c.args1.Data)),
				Lengths: make([]uint32, len(c.args1.Lengths)),
				Offsets: make([]uint32, len(c.args1.Offsets)),
			}

			got := SliceFromZeroConstantOffsetUnbounded(c.args1, out)
			require.Equal(t, c.want.String(), got.String())
		})
	}
}

func TestSliceDynamicOffsetUnbounded(t *testing.T) {
	// SliceDynamicOffsetUnbounded(src *types.Bytes, res *types.Bytes, start_column interface{}, start_column_type types.T) *types.Bytes
	cases := []struct {
		name       string
		src_args   *types.Bytes
		start_args interface{}
		start_type types.T
		want       *types.Bytes
	}{
		{
			name: "Test01",
			src_args: &types.Bytes{
				Data:    []byte("abcdefghijklmn"),
				Lengths: []uint32{uint32(len("abcdefghijklmn"))},
				Offsets: []uint32{0},
			},
			start_args: []uint8{5},
			start_type: types.T_uint8,
			want: &types.Bytes{
				Data:    []byte("efghijklmn"),
				Lengths: []uint32{uint32(len("efghijklmn"))},
				Offsets: []uint32{0},
			},
		},
		{
			name: "Test02",
			src_args: &types.Bytes{
				Data:    []byte("abcdefghijklmn"),
				Lengths: []uint32{uint32(len("abcdefghijklmn"))},
				Offsets: []uint32{0},
			},
			start_args: []uint16{5},
			start_type: types.T_uint16,
			want: &types.Bytes{
				Data:    []byte("efghijklmn"),
				Lengths: []uint32{uint32(len("efghijklmn"))},
				Offsets: []uint32{0},
			},
		},
		{
			name: "Test03",
			src_args: &types.Bytes{
				Data:    []byte("abcdefghijklmn"),
				Lengths: []uint32{uint32(len("abcdefghijklmn"))},
				Offsets: []uint32{0},
			},
			start_args: []uint32{5},
			start_type: types.T_uint32,
			want: &types.Bytes{
				Data:    []byte("efghijklmn"),
				Lengths: []uint32{uint32(len("efghijklmn"))},
				Offsets: []uint32{0},
			},
		},
		{
			name: "Test04",
			src_args: &types.Bytes{
				Data:    []byte("abcdefghijklmn"),
				Lengths: []uint32{uint32(len("abcdefghijklmn"))},
				Offsets: []uint32{0},
			},
			start_args: []uint64{5},
			start_type: types.T_uint64,
			want: &types.Bytes{
				Data:    []byte("efghijklmn"),
				Lengths: []uint32{uint32(len("efghijklmn"))},
				Offsets: []uint32{0},
			},
		},
		{
			name: "Test05",
			src_args: &types.Bytes{
				Data:    []byte("abcdefghijklmn"),
				Lengths: []uint32{uint32(len("abcdefghijklmn"))},
				Offsets: []uint32{0},
			},
			start_args: []int8{-10},
			start_type: types.T_int8,
			want: &types.Bytes{
				Data:    []byte("efghijklmn"),
				Lengths: []uint32{uint32(len("efghijklmn"))},
				Offsets: []uint32{0},
			},
		},
		{
			name: "Test06",
			src_args: &types.Bytes{
				Data:    []byte("abcdefghijklmn"),
				Lengths: []uint32{uint32(len("abcdefghijklmn"))},
				Offsets: []uint32{0},
			},
			start_args: []int16{-10},
			start_type: types.T_int16,
			want: &types.Bytes{
				Data:    []byte("efghijklmn"),
				Lengths: []uint32{uint32(len("efghijklmn"))},
				Offsets: []uint32{0},
			},
		},
		{
			name: "Test07",
			src_args: &types.Bytes{
				Data:    []byte("abcdefghijklmn"),
				Lengths: []uint32{uint32(len("abcdefghijklmn"))},
				Offsets: []uint32{0},
			},
			start_args: []int32{-10},
			start_type: types.T_int32,
			want: &types.Bytes{
				Data:    []byte("efghijklmn"),
				Lengths: []uint32{uint32(len("efghijklmn"))},
				Offsets: []uint32{0},
			},
		},
		{
			name: "Test08",
			src_args: &types.Bytes{
				Data:    []byte("abcdefghijklmn"),
				Lengths: []uint32{uint32(len("abcdefghijklmn"))},
				Offsets: []uint32{0},
			},
			start_args: []int64{-10},
			start_type: types.T_int64,
			want: &types.Bytes{
				Data:    []byte("efghijklmn"),
				Lengths: []uint32{uint32(len("efghijklmn"))},
				Offsets: []uint32{0},
			},
		},
		{
			name: "Test09",
			src_args: &types.Bytes{
				Data:    []byte("abcdefghijklmn"),
				Lengths: []uint32{uint32(len("abcdefghijklmn"))},
				Offsets: []uint32{0},
			},
			start_args: []uint64{0},
			start_type: types.T_uint64,
			want: &types.Bytes{
				Data:    []byte(""),
				Lengths: []uint32{uint32(len(""))},
				Offsets: []uint32{0},
			},
		},
		{
			name: "Test10",
			src_args: &types.Bytes{
				Data:    []byte("abcdefghijklmn"),
				Lengths: []uint32{uint32(len("abcdefghijklmn"))},
				Offsets: []uint32{0},
			},
			start_args: []int32{0},
			start_type: types.T_int32,
			want: &types.Bytes{
				Data:    []byte(""),
				Lengths: []uint32{uint32(len(""))},
				Offsets: []uint32{0},
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			out := &types.Bytes{
				Data:    make([]byte, len(c.src_args.Data)),
				Lengths: make([]uint32, len(c.src_args.Lengths)),
				Offsets: make([]uint32, len(c.src_args.Offsets)),
			}

			got := SliceDynamicOffsetUnbounded(c.src_args, out, c.start_args, c.start_type)
			require.Equal(t, c.want.String(), got.String())
		})
	}

}

//-------------------------------------------------------------

func TestSliceFromLeftConstantOffsetBounded(t *testing.T) {
	cases := []struct {
		name   string
		args   *types.Bytes
		start  int64
		length int64
		want   *types.Bytes
	}{
		{
			name: "Test01",
			args: &types.Bytes{
				Data:    []byte("abcdefghijklmn"),
				Lengths: []uint32{uint32(len("abcdefghijklmn"))},
				Offsets: []uint32{0},
			},
			start:  5,
			length: 6,
			want: &types.Bytes{
				Data:    []byte("efghij"),
				Lengths: []uint32{uint32(len("efghij"))},
				Offsets: []uint32{0},
			},
		},
		{
			name: "Test02",
			args: &types.Bytes{
				Data:    []byte("abcdefghijklmn"),
				Lengths: []uint32{uint32(len("abcdefghijklmn"))},
				Offsets: []uint32{0},
			},
			start:  5,
			length: 10,
			want: &types.Bytes{
				Data:    []byte("efghijklmn"),
				Lengths: []uint32{uint32(len("efghijklmn"))},
				Offsets: []uint32{0},
			},
		},
		{
			name: "Test03",
			args: &types.Bytes{
				Data:    []byte("abcdefghijklmn"),
				Lengths: []uint32{uint32(len("abcdefghijklmn"))},
				Offsets: []uint32{0},
			},
			start:  5,
			length: 0,
			want: &types.Bytes{
				Data:    []byte(""),
				Lengths: []uint32{uint32(len(""))},
				Offsets: []uint32{0},
			},
		},
		{
			name: "Test04",
			args: &types.Bytes{
				Data:    []byte("abcdefghijklmn"),
				Lengths: []uint32{uint32(len("abcdefghijklmn"))},
				Offsets: []uint32{0},
			},
			start:  5,
			length: -3,
			want: &types.Bytes{
				Data:    []byte("efghijk"),
				Lengths: []uint32{uint32(len("efghijk"))},
				Offsets: []uint32{0},
			},
		},
		{
			name: "Test05",
			args: &types.Bytes{
				Data:    []byte("abcdefghijklmn"),
				Lengths: []uint32{uint32(len("abcdefghijklmn"))},
				Offsets: []uint32{0},
			},
			start:  5,
			length: -8,
			want: &types.Bytes{
				Data:    []byte("ef"),
				Lengths: []uint32{uint32(len("ef"))},
				Offsets: []uint32{0},
			},
		},
		{
			name: "Test06",
			args: &types.Bytes{
				Data:    []byte("abcdefghijklmn"),
				Lengths: []uint32{uint32(len("abcdefghijklmn"))},
				Offsets: []uint32{0},
			},
			start:  5,
			length: -10,
			want: &types.Bytes{
				Data:    []byte(""),
				Lengths: []uint32{uint32(len(""))},
				Offsets: []uint32{0},
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			out := &types.Bytes{
				Data:    make([]byte, len(c.args.Data)),
				Lengths: make([]uint32, len(c.args.Lengths)),
				Offsets: make([]uint32, len(c.args.Offsets)),
			}
			got := SliceFromLeftConstantOffsetBounded(c.args, out, c.start-1, c.length)
			require.Equal(t, c.want.String(), got.String())
		})
	}
}

func TestSliceFromRightConstantOffsetBounded(t *testing.T) {
	cases := []struct {
		name   string
		args   *types.Bytes
		start  int64
		length int64
		want   *types.Bytes
	}{
		{
			name: "Test03",
			args: &types.Bytes{
				Data:    []byte("abcdefghijklmn"),
				Lengths: []uint32{uint32(len("abcdefghijklmn"))},
				Offsets: []uint32{0},
			},
			start:  -10,
			length: 5,
			want: &types.Bytes{
				Data:    []byte("efghi"),
				Lengths: []uint32{uint32(len("efghi"))},
				Offsets: []uint32{0},
			},
		},
		{
			name: "Test04",
			args: &types.Bytes{
				Data:    []byte("abcdefghijklmn"),
				Lengths: []uint32{uint32(len("abcdefghijklmn"))},
				Offsets: []uint32{0},
			},
			start:  -10,
			length: 0,
			want: &types.Bytes{
				Data:    []byte(""),
				Lengths: []uint32{uint32(len(""))},
				Offsets: []uint32{0},
			},
		},
		{
			name: "Test05",
			args: &types.Bytes{
				Data:    []byte("abcdefghijklmn"),
				Lengths: []uint32{uint32(len("abcdefghijklmn"))},
				Offsets: []uint32{0},
			},
			start:  -10,
			length: 12,
			want: &types.Bytes{
				Data:    []byte("efghijklmn"),
				Lengths: []uint32{uint32(len("efghijklmn"))},
				Offsets: []uint32{0},
			},
		},
		{
			name: "Test06",
			args: &types.Bytes{
				Data:    []byte("abcdefghijklmn"),
				Lengths: []uint32{uint32(len("abcdefghijklmn"))},
				Offsets: []uint32{0},
			},
			start:  -14,
			length: 12,
			want: &types.Bytes{
				Data:    []byte("abcdefghijkl"),
				Lengths: []uint32{uint32(len("abcdefghijkl"))},
				Offsets: []uint32{0},
			},
		},
		{
			name: "Test07",
			args: &types.Bytes{
				Data:    []byte("abcdefghijklmn"),
				Lengths: []uint32{uint32(len("abcdefghijklmn"))},
				Offsets: []uint32{0},
			},
			start:  -20,
			length: 4,
			want: &types.Bytes{
				Data:    []byte(""),
				Lengths: []uint32{uint32(len(""))},
				Offsets: []uint32{0},
			},
		},
		{
			name: "Test08",
			args: &types.Bytes{
				Data:    []byte("abcdefghijklmn"),
				Lengths: []uint32{uint32(len("abcdefghijklmn"))},
				Offsets: []uint32{0},
			},
			start:  -16,
			length: 10,
			want: &types.Bytes{
				Data:    []byte("abcdefgh"),
				Lengths: []uint32{uint32(len("abcdefgh"))},
				Offsets: []uint32{0},
			},
		},
		{
			name: "Test09",
			args: &types.Bytes{
				Data:    []byte("abcdefghijklmn"),
				Lengths: []uint32{uint32(len("abcdefghijklmn"))},
				Offsets: []uint32{0},
			},
			start:  -16,
			length: 20,
			want: &types.Bytes{
				Data:    []byte("abcdefghijklmn"),
				Lengths: []uint32{uint32(len("abcdefghijklmn"))},
				Offsets: []uint32{0},
			},
		},
		{
			name: "Test10",
			args: &types.Bytes{
				Data:    []byte("abcdefghijklmn"),
				Lengths: []uint32{uint32(len("abcdefghijklmn"))},
				Offsets: []uint32{0},
			},
			start:  -8,
			length: -4,
			want: &types.Bytes{
				Data:    []byte("gh"),
				Lengths: []uint32{uint32(len("gh"))},
				Offsets: []uint32{0},
			},
		},
		{
			name: "Test11",
			args: &types.Bytes{
				Data:    []byte("abcdefghijklmn"),
				Lengths: []uint32{uint32(len("abcdefghijklmn"))},
				Offsets: []uint32{0},
			},
			start:  -8,
			length: -6,
			want: &types.Bytes{
				Data:    []byte(""),
				Lengths: []uint32{uint32(len(""))},
				Offsets: []uint32{0},
			},
		},
		{
			name: "Test11",
			args: &types.Bytes{
				Data:    []byte("abcdefghijklmn"),
				Lengths: []uint32{uint32(len("abcdefghijklmn"))},
				Offsets: []uint32{0},
			},
			start:  -14,
			length: -6,
			want: &types.Bytes{
				Data:    []byte(""),
				Lengths: []uint32{uint32(len(""))},
				Offsets: []uint32{0},
			},
		},
		{
			name: "Test12",
			args: &types.Bytes{
				Data:    []byte("abcdefghijklmn"),
				Lengths: []uint32{uint32(len("abcdefghijklmn"))},
				Offsets: []uint32{0},
			},
			start:  -16,
			length: -6,
			want: &types.Bytes{
				Data:    []byte(""),
				Lengths: []uint32{uint32(len(""))},
				Offsets: []uint32{0},
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			out := &types.Bytes{
				Data:    make([]byte, len(c.args.Data)),
				Lengths: make([]uint32, len(c.args.Lengths)),
				Offsets: make([]uint32, len(c.args.Offsets)),
			}
			got := SliceFromRightConstantOffsetBounded(c.args, out, -c.start, c.length)
			require.Equal(t, c.want.String(), got.String())
		})
	}
}

func TestSliceFromZeroConstantOffsetBounded(t *testing.T) {
	cases := []struct {
		name   string
		args1  *types.Bytes
		start  int64
		length int64
		want   *types.Bytes
	}{
		{
			name: "Test01",
			args1: &types.Bytes{
				Data:    []byte("abcdefghijklmn"),
				Lengths: []uint32{uint32(len("abcdefghijklmn"))},
				Offsets: []uint32{0},
			},
			start:  0,
			length: 20,
			want: &types.Bytes{
				Data:    []byte(""),
				Lengths: []uint32{uint32(len(""))},
				Offsets: []uint32{0},
			},
		},
		{
			name: "Test02",
			args1: &types.Bytes{
				Data:    []byte("abcd132456"),
				Lengths: []uint32{uint32(len("abcd132456"))},
				Offsets: []uint32{0},
			},
			start:  0,
			length: -5,
			want: &types.Bytes{
				Data:    []byte(""),
				Lengths: []uint32{uint32(len(""))},
				Offsets: []uint32{0},
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			out := &types.Bytes{
				Data:    make([]byte, len(c.args1.Data)),
				Lengths: make([]uint32, len(c.args1.Lengths)),
				Offsets: make([]uint32, len(c.args1.Offsets)),
			}

			got := SliceFromZeroConstantOffsetBounded(c.args1, out)
			require.Equal(t, c.want.String(), got.String())
		})
	}
}

func TestSliceDynamicOffsetBounded(t *testing.T) {
	cases := []struct {
		name        string
		src_args    *types.Bytes
		start_args  interface{}
		start_type  types.T
		lenght_args interface{}
		length_type types.T
		cs          []bool
		want        *types.Bytes
	}{
		{
			name: "Test01",
			src_args: &types.Bytes{
				Data:    []byte("abcdefghijklmn"),
				Lengths: []uint32{uint32(len("abcdefghijklmn"))},
				Offsets: []uint32{0},
			},
			start_args:  []uint8{5},
			start_type:  types.T_uint8,
			lenght_args: []uint8{6},
			length_type: types.T_uint8,
			cs:          []bool{false, false, false},
			want: &types.Bytes{
				Data:    []byte("efghij"),
				Lengths: []uint32{uint32(len("efghij"))},
				Offsets: []uint32{0},
			},
		},
		{
			name: "Test02",
			src_args: &types.Bytes{
				Data:    []byte("abcdefghijklmn"),
				Lengths: []uint32{uint32(len("abcdefghijklmn"))},
				Offsets: []uint32{0},
			},
			start_args:  []uint16{5},
			start_type:  types.T_uint16,
			lenght_args: []uint16{6},
			length_type: types.T_uint16,
			cs:          []bool{false, false, false},
			want: &types.Bytes{
				Data:    []byte("efghij"),
				Lengths: []uint32{uint32(len("efghij"))},
				Offsets: []uint32{0},
			},
		},
		{
			name: "Test03",
			src_args: &types.Bytes{
				Data:    []byte("abcdefghijklmn"),
				Lengths: []uint32{uint32(len("abcdefghijklmn"))},
				Offsets: []uint32{0},
			},
			start_args:  []uint32{5},
			start_type:  types.T_uint32,
			lenght_args: []uint32{6},
			length_type: types.T_uint32,
			cs:          []bool{false, false, false},
			want: &types.Bytes{
				Data:    []byte("efghij"),
				Lengths: []uint32{uint32(len("efghij"))},
				Offsets: []uint32{0},
			},
		},
		{
			name: "Test04",
			src_args: &types.Bytes{
				Data:    []byte("abcdefghijklmn"),
				Lengths: []uint32{uint32(len("abcdefghijklmn"))},
				Offsets: []uint32{0},
			},
			start_args:  []uint64{5},
			start_type:  types.T_uint64,
			lenght_args: []uint64{6},
			length_type: types.T_uint64,
			cs:          []bool{false, false, false},
			want: &types.Bytes{
				Data:    []byte("efghij"),
				Lengths: []uint32{uint32(len("efghij"))},
				Offsets: []uint32{0},
			},
		},
		{
			name: "Test05",
			src_args: &types.Bytes{
				Data:    []byte("abcdefghijklmn"),
				Lengths: []uint32{uint32(len("abcdefghijklmn"))},
				Offsets: []uint32{0},
			},
			start_args:  []uint32{5},
			start_type:  types.T_uint32,
			lenght_args: []int64{6},
			length_type: types.T_int64,
			cs:          []bool{false, false, false},
			want: &types.Bytes{
				Data:    []byte("efghij"),
				Lengths: []uint32{uint32(len("efghij"))},
				Offsets: []uint32{0},
			},
		},
		{
			name: "Test06",
			src_args: &types.Bytes{
				Data:    []byte("abcdefghijklmn"),
				Lengths: []uint32{uint32(len("abcdefghijklmn"))},
				Offsets: []uint32{0},
			},
			start_args:  []uint32{5},
			start_type:  types.T_uint32,
			lenght_args: []int64{-6},
			length_type: types.T_int64,
			cs:          []bool{false, false, false},
			want: &types.Bytes{
				Data:    []byte("efgh"),
				Lengths: []uint32{uint32(len("efgh"))},
				Offsets: []uint32{0},
			},
		},
		{
			name: "Test07",
			src_args: &types.Bytes{
				Data:    []byte("abcdefghijklmn"),
				Lengths: []uint32{uint32(len("abcdefghijklmn"))},
				Offsets: []uint32{0},
			},
			start_args:  []uint32{5},
			start_type:  types.T_uint32,
			lenght_args: []int8{-6},
			length_type: types.T_int8,
			cs:          []bool{false, false, false},
			want: &types.Bytes{
				Data:    []byte("efgh"),
				Lengths: []uint32{uint32(len("efgh"))},
				Offsets: []uint32{0},
			},
		},
		{
			name: "Test08",
			src_args: &types.Bytes{
				Data:    []byte("abcdefghijklmn"),
				Lengths: []uint32{uint32(len("abcdefghijklmn"))},
				Offsets: []uint32{0},
			},
			start_args:  []uint32{5},
			start_type:  types.T_uint32,
			lenght_args: []int8{0},
			length_type: types.T_int8,
			cs:          []bool{false, false, false},
			want: &types.Bytes{
				Data:    []byte(""),
				Lengths: []uint32{uint32(len(""))},
				Offsets: []uint32{0},
			},
		},
		{
			name: "Test09",
			src_args: &types.Bytes{
				Data:    []byte("abcdefghijklmn"),
				Lengths: []uint32{uint32(len("abcdefghijklmn"))},
				Offsets: []uint32{0},
			},
			start_args:  []uint32{5},
			start_type:  types.T_uint32,
			lenght_args: []int16{-10},
			length_type: types.T_int16,
			cs:          []bool{false, false, false},
			want: &types.Bytes{
				Data:    []byte(""),
				Lengths: []uint32{uint32(len(""))},
				Offsets: []uint32{0},
			},
		},
		{
			name: "Test10",
			src_args: &types.Bytes{
				Data:    []byte("abcdefghijklmn"),
				Lengths: []uint32{uint32(len("abcdefghijklmn"))},
				Offsets: []uint32{0},
			},
			start_args:  []int32{0},
			start_type:  types.T_int32,
			lenght_args: []int16{-10},
			length_type: types.T_int16,
			cs:          []bool{false, false, false},
			want: &types.Bytes{
				Data:    []byte(""),
				Lengths: []uint32{uint32(len(""))},
				Offsets: []uint32{0},
			},
		},
		//----------------------------------------
		{
			name: "Test11",
			src_args: &types.Bytes{
				Data:    []byte("abcdefghijklmn"),
				Lengths: []uint32{uint32(len("abcdefghijklmn"))},
				Offsets: []uint32{0},
			},
			start_args:  []int16{-10},
			start_type:  types.T_int16,
			lenght_args: []int16{-2},
			length_type: types.T_int16,
			cs:          []bool{false, false, false},
			want: &types.Bytes{
				Data:    []byte("efghijkl"),
				Lengths: []uint32{uint32(len("efghijkl"))},
				Offsets: []uint32{0},
			},
		},
		{
			name: "Test12",
			src_args: &types.Bytes{
				Data:    []byte("abcdefghijklmn"),
				Lengths: []uint32{uint32(len("abcdefghijklmn"))},
				Offsets: []uint32{0},
			},
			start_args:  []int32{-10},
			start_type:  types.T_int32,
			lenght_args: []int64{-6},
			length_type: types.T_int64,
			cs:          []bool{false, false, false},
			want: &types.Bytes{
				Data:    []byte("efgh"),
				Lengths: []uint32{uint32(len("efgh"))},
				Offsets: []uint32{0},
			},
		},
		{
			name: "Test13",
			src_args: &types.Bytes{
				Data:    []byte("abcdefghijklmn"),
				Lengths: []uint32{uint32(len("abcdefghijklmn"))},
				Offsets: []uint32{0},
			},
			start_args:  []int8{-10},
			start_type:  types.T_int8,
			lenght_args: []int32{-12},
			length_type: types.T_int32,
			cs:          []bool{false, false, false},
			want: &types.Bytes{
				Data:    []byte(""),
				Lengths: []uint32{uint32(len(""))},
				Offsets: []uint32{0},
			},
		},
		{
			name: "Test14",
			src_args: &types.Bytes{
				Data:    []byte("abcdefghijklmn"),
				Lengths: []uint32{uint32(len("abcdefghijklmn"))},
				Offsets: []uint32{0},
			},
			start_args:  []int16{-10},
			start_type:  types.T_int16,
			lenght_args: []int32{0},
			length_type: types.T_int32,
			cs:          []bool{false, false, false},
			want: &types.Bytes{
				Data:    []byte(""),
				Lengths: []uint32{uint32(len(""))},
				Offsets: []uint32{0},
			},
		},
		{
			name: "Test15",
			src_args: &types.Bytes{
				Data:    []byte("abcdefghijklmn"),
				Lengths: []uint32{uint32(len("abcdefghijklmn"))},
				Offsets: []uint32{0},
			},
			start_args:  []int32{-10},
			start_type:  types.T_int32,
			lenght_args: []int64{-9},
			length_type: types.T_int64,
			cs:          []bool{false, false, false},
			want: &types.Bytes{
				Data:    []byte("e"),
				Lengths: []uint32{uint32(len("e"))},
				Offsets: []uint32{0},
			},
		},
		{
			name: "Test16",
			src_args: &types.Bytes{
				Data:    []byte("abcdefghijklmn"),
				Lengths: []uint32{uint32(len("abcdefghijklmn"))},
				Offsets: []uint32{0},
			},
			start_args:  []int32{-10},
			start_type:  types.T_int32,
			lenght_args: []int64{-10},
			length_type: types.T_int64,
			cs:          []bool{false, false, false},
			want: &types.Bytes{
				Data:    []byte(""),
				Lengths: []uint32{uint32(len(""))},
				Offsets: []uint32{0},
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			out := &types.Bytes{
				Data:    make([]byte, len(c.src_args.Data)),
				Lengths: make([]uint32, len(c.src_args.Lengths)),
				Offsets: make([]uint32, len(c.src_args.Offsets)),
			}
			// func SliceDynamicOffsetBounded(src *types.Bytes, res *types.Bytes,
			// 	start_column interface{}, start_column_type types.T,
			// 	length_column interface{}, length_column_type types.T) *types.Bytes

			got := SliceDynamicOffsetBounded(c.src_args, out, c.start_args, c.start_type, c.lenght_args, c.length_type, c.cs)
			require.Equal(t, c.want.String(), got.String())
		})
	}
}
