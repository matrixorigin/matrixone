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

var (
	SubstringFromLeftConstOffsetUnbounded  func(*types.Bytes, *types.Bytes, int64) *types.Bytes
	SubstringFromRightConstOffsetUnbounded func(*types.Bytes, *types.Bytes, int64) *types.Bytes
	SubstringFromZeroConstOffsetUnbounded  func(*types.Bytes, *types.Bytes) *types.Bytes
	SubstringFromZeroConstOffsetBounded    func(*types.Bytes, *types.Bytes) *types.Bytes
	SubstringDynamicOffsetUnbounded        func(*types.Bytes, *types.Bytes, interface{}, types.T) *types.Bytes
	SubstringFromLeftConstOffsetBounded    func(*types.Bytes, *types.Bytes, int64, int64) *types.Bytes
	SubstringFromRightConstOffsetBounded   func(*types.Bytes, *types.Bytes, int64, int64) *types.Bytes
	SubstringDynamicOffsetBounded          func(*types.Bytes, *types.Bytes, interface{}, types.T, interface{}, types.T, []bool) *types.Bytes
)

func init() {
	SubstringFromLeftConstOffsetUnbounded = substringFromLeftConstOffsetUnbounded
	SubstringFromRightConstOffsetUnbounded = substringFromRightConstOffsetUnbounded
	SubstringFromZeroConstOffsetUnbounded = substringFromZeroConstOffsetUnbounded
	SubstringDynamicOffsetUnbounded = substringDynamicOffsetUnbounded
	SubstringFromZeroConstOffsetBounded = substringFromZeroConstOffsetBounded
	SubstringFromLeftConstOffsetBounded = substringFromLeftConstOffsetBounded
	SubstringFromRightConstOffsetBounded = substringFromRightConstOffsetBounded
	SubstringDynamicOffsetBounded = substringDynamicOffsetBounded
}

func TestSubstringFromLeftConstOffsetUnbounded(t *testing.T) {
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

			got := SubstringFromLeftConstOffsetUnbounded(c.args1, out, c.start-1)
			require.Equal(t, c.want.String(), got.String())
		})
	}
}

func TestSubstringFromRightConstOffsetUnbounded(t *testing.T) {
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

			got := SubstringFromRightConstOffsetUnbounded(c.args1, out, -c.start)
			require.Equal(t, c.want.String(), got.String())
		})
	}
}

func TestSubstringFromZeroConstOffsetUnbounded(t *testing.T) {
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

			got := SubstringFromZeroConstOffsetUnbounded(c.args1, out)
			require.Equal(t, c.want.String(), got.String())
		})
	}
}

func TestSubstringDynamicOffsetUnbounded(t *testing.T) {
	cases := []struct {
		name      string
		srcArgs   *types.Bytes
		startArgs interface{}
		startType types.T
		want      *types.Bytes
	}{
		{
			name: "Test01",
			srcArgs: &types.Bytes{
				Data:    []byte("abcdefghijklmn"),
				Lengths: []uint32{uint32(len("abcdefghijklmn"))},
				Offsets: []uint32{0},
			},
			startArgs: []uint8{5},
			startType: types.T_uint8,
			want: &types.Bytes{
				Data:    []byte("efghijklmn"),
				Lengths: []uint32{uint32(len("efghijklmn"))},
				Offsets: []uint32{0},
			},
		},
		{
			name: "Test02",
			srcArgs: &types.Bytes{
				Data:    []byte("abcdefghijklmn"),
				Lengths: []uint32{uint32(len("abcdefghijklmn"))},
				Offsets: []uint32{0},
			},
			startArgs: []uint16{5},
			startType: types.T_uint16,
			want: &types.Bytes{
				Data:    []byte("efghijklmn"),
				Lengths: []uint32{uint32(len("efghijklmn"))},
				Offsets: []uint32{0},
			},
		},
		{
			name: "Test03",
			srcArgs: &types.Bytes{
				Data:    []byte("abcdefghijklmn"),
				Lengths: []uint32{uint32(len("abcdefghijklmn"))},
				Offsets: []uint32{0},
			},
			startArgs: []uint32{5},
			startType: types.T_uint32,
			want: &types.Bytes{
				Data:    []byte("efghijklmn"),
				Lengths: []uint32{uint32(len("efghijklmn"))},
				Offsets: []uint32{0},
			},
		},
		{
			name: "Test04",
			srcArgs: &types.Bytes{
				Data:    []byte("abcdefghijklmn"),
				Lengths: []uint32{uint32(len("abcdefghijklmn"))},
				Offsets: []uint32{0},
			},
			startArgs: []uint64{5},
			startType: types.T_uint64,
			want: &types.Bytes{
				Data:    []byte("efghijklmn"),
				Lengths: []uint32{uint32(len("efghijklmn"))},
				Offsets: []uint32{0},
			},
		},
		{
			name: "Test05",
			srcArgs: &types.Bytes{
				Data:    []byte("abcdefghijklmn"),
				Lengths: []uint32{uint32(len("abcdefghijklmn"))},
				Offsets: []uint32{0},
			},
			startArgs: []int8{-10},
			startType: types.T_int8,
			want: &types.Bytes{
				Data:    []byte("efghijklmn"),
				Lengths: []uint32{uint32(len("efghijklmn"))},
				Offsets: []uint32{0},
			},
		},
		{
			name: "Test06",
			srcArgs: &types.Bytes{
				Data:    []byte("abcdefghijklmn"),
				Lengths: []uint32{uint32(len("abcdefghijklmn"))},
				Offsets: []uint32{0},
			},
			startArgs: []int16{-10},
			startType: types.T_int16,
			want: &types.Bytes{
				Data:    []byte("efghijklmn"),
				Lengths: []uint32{uint32(len("efghijklmn"))},
				Offsets: []uint32{0},
			},
		},
		{
			name: "Test07",
			srcArgs: &types.Bytes{
				Data:    []byte("abcdefghijklmn"),
				Lengths: []uint32{uint32(len("abcdefghijklmn"))},
				Offsets: []uint32{0},
			},
			startArgs: []int32{-10},
			startType: types.T_int32,
			want: &types.Bytes{
				Data:    []byte("efghijklmn"),
				Lengths: []uint32{uint32(len("efghijklmn"))},
				Offsets: []uint32{0},
			},
		},
		{
			name: "Test08",
			srcArgs: &types.Bytes{
				Data:    []byte("abcdefghijklmn"),
				Lengths: []uint32{uint32(len("abcdefghijklmn"))},
				Offsets: []uint32{0},
			},
			startArgs: []int64{-10},
			startType: types.T_int64,
			want: &types.Bytes{
				Data:    []byte("efghijklmn"),
				Lengths: []uint32{uint32(len("efghijklmn"))},
				Offsets: []uint32{0},
			},
		},
		{
			name: "Test09",
			srcArgs: &types.Bytes{
				Data:    []byte("abcdefghijklmn"),
				Lengths: []uint32{uint32(len("abcdefghijklmn"))},
				Offsets: []uint32{0},
			},
			startArgs: []uint64{0},
			startType: types.T_uint64,
			want: &types.Bytes{
				Data:    []byte(""),
				Lengths: []uint32{uint32(len(""))},
				Offsets: []uint32{0},
			},
		},
		{
			name: "Test10",
			srcArgs: &types.Bytes{
				Data:    []byte("abcdefghijklmn"),
				Lengths: []uint32{uint32(len("abcdefghijklmn"))},
				Offsets: []uint32{0},
			},
			startArgs: []int32{0},
			startType: types.T_int32,
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
				Data:    make([]byte, len(c.srcArgs.Data)),
				Lengths: make([]uint32, len(c.srcArgs.Lengths)),
				Offsets: make([]uint32, len(c.srcArgs.Offsets)),
			}

			got := SubstringDynamicOffsetUnbounded(c.srcArgs, out, c.startArgs, c.startType)
			require.Equal(t, c.want.String(), got.String())
		})
	}

}

//-------------------------------------------------------------

func TestSubstringFromLeftConstOffsetBounded(t *testing.T) {
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
			got := SubstringFromLeftConstOffsetBounded(c.args, out, c.start-1, c.length)
			require.Equal(t, c.want.String(), got.String())
		})
	}
}

func TestSubstringFromRightConstOffsetBounded(t *testing.T) {
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
			got := SubstringFromRightConstOffsetBounded(c.args, out, -c.start, c.length)
			require.Equal(t, c.want.String(), got.String())
		})
	}
}

func TestSubstringFromZeroConstOffsetBounded(t *testing.T) {
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

			got := SubstringFromZeroConstOffsetBounded(c.args1, out)
			require.Equal(t, c.want.String(), got.String())
		})
	}
}

func TestSubstringDynamicOffsetBounded(t *testing.T) {
	cases := []struct {
		name       string
		srcArgs    *types.Bytes
		startArgs  interface{}
		startType  types.T
		lengthArgs interface{}
		lengthType types.T
		cs         []bool
		want       *types.Bytes
	}{
		{
			name: "Test01",
			srcArgs: &types.Bytes{
				Data:    []byte("abcdefghijklmn"),
				Lengths: []uint32{uint32(len("abcdefghijklmn"))},
				Offsets: []uint32{0},
			},
			startArgs:  []uint8{5},
			startType:  types.T_uint8,
			lengthArgs: []uint8{6},
			lengthType: types.T_uint8,
			cs:         []bool{false, false, false},
			want: &types.Bytes{
				Data:    []byte("efghij"),
				Lengths: []uint32{uint32(len("efghij"))},
				Offsets: []uint32{0},
			},
		},
		{
			name: "Test02",
			srcArgs: &types.Bytes{
				Data:    []byte("abcdefghijklmn"),
				Lengths: []uint32{uint32(len("abcdefghijklmn"))},
				Offsets: []uint32{0},
			},
			startArgs:  []uint16{5},
			startType:  types.T_uint16,
			lengthArgs: []uint16{6},
			lengthType: types.T_uint16,
			cs:         []bool{false, false, false},
			want: &types.Bytes{
				Data:    []byte("efghij"),
				Lengths: []uint32{uint32(len("efghij"))},
				Offsets: []uint32{0},
			},
		},
		{
			name: "Test03",
			srcArgs: &types.Bytes{
				Data:    []byte("abcdefghijklmn"),
				Lengths: []uint32{uint32(len("abcdefghijklmn"))},
				Offsets: []uint32{0},
			},
			startArgs:  []uint32{5},
			startType:  types.T_uint32,
			lengthArgs: []uint32{6},
			lengthType: types.T_uint32,
			cs:         []bool{false, false, false},
			want: &types.Bytes{
				Data:    []byte("efghij"),
				Lengths: []uint32{uint32(len("efghij"))},
				Offsets: []uint32{0},
			},
		},
		{
			name: "Test04",
			srcArgs: &types.Bytes{
				Data:    []byte("abcdefghijklmn"),
				Lengths: []uint32{uint32(len("abcdefghijklmn"))},
				Offsets: []uint32{0},
			},
			startArgs:  []uint64{5},
			startType:  types.T_uint64,
			lengthArgs: []uint64{6},
			lengthType: types.T_uint64,
			cs:         []bool{false, false, false},
			want: &types.Bytes{
				Data:    []byte("efghij"),
				Lengths: []uint32{uint32(len("efghij"))},
				Offsets: []uint32{0},
			},
		},
		{
			name: "Test05",
			srcArgs: &types.Bytes{
				Data:    []byte("abcdefghijklmn"),
				Lengths: []uint32{uint32(len("abcdefghijklmn"))},
				Offsets: []uint32{0},
			},
			startArgs:  []uint32{5},
			startType:  types.T_uint32,
			lengthArgs: []int64{6},
			lengthType: types.T_int64,
			cs:         []bool{false, false, false},
			want: &types.Bytes{
				Data:    []byte("efghij"),
				Lengths: []uint32{uint32(len("efghij"))},
				Offsets: []uint32{0},
			},
		},
		{
			name: "Test06",
			srcArgs: &types.Bytes{
				Data:    []byte("abcdefghijklmn"),
				Lengths: []uint32{uint32(len("abcdefghijklmn"))},
				Offsets: []uint32{0},
			},
			startArgs:  []uint32{5},
			startType:  types.T_uint32,
			lengthArgs: []int64{-6},
			lengthType: types.T_int64,
			cs:         []bool{false, false, false},
			want: &types.Bytes{
				Data:    []byte("efgh"),
				Lengths: []uint32{uint32(len("efgh"))},
				Offsets: []uint32{0},
			},
		},
		{
			name: "Test07",
			srcArgs: &types.Bytes{
				Data:    []byte("abcdefghijklmn"),
				Lengths: []uint32{uint32(len("abcdefghijklmn"))},
				Offsets: []uint32{0},
			},
			startArgs:  []uint32{5},
			startType:  types.T_uint32,
			lengthArgs: []int8{-6},
			lengthType: types.T_int8,
			cs:         []bool{false, false, false},
			want: &types.Bytes{
				Data:    []byte("efgh"),
				Lengths: []uint32{uint32(len("efgh"))},
				Offsets: []uint32{0},
			},
		},
		{
			name: "Test08",
			srcArgs: &types.Bytes{
				Data:    []byte("abcdefghijklmn"),
				Lengths: []uint32{uint32(len("abcdefghijklmn"))},
				Offsets: []uint32{0},
			},
			startArgs:  []uint32{5},
			startType:  types.T_uint32,
			lengthArgs: []int8{0},
			lengthType: types.T_int8,
			cs:         []bool{false, false, false},
			want: &types.Bytes{
				Data:    []byte(""),
				Lengths: []uint32{uint32(len(""))},
				Offsets: []uint32{0},
			},
		},
		{
			name: "Test09",
			srcArgs: &types.Bytes{
				Data:    []byte("abcdefghijklmn"),
				Lengths: []uint32{uint32(len("abcdefghijklmn"))},
				Offsets: []uint32{0},
			},
			startArgs:  []uint32{5},
			startType:  types.T_uint32,
			lengthArgs: []int16{-10},
			lengthType: types.T_int16,
			cs:         []bool{false, false, false},
			want: &types.Bytes{
				Data:    []byte(""),
				Lengths: []uint32{uint32(len(""))},
				Offsets: []uint32{0},
			},
		},
		{
			name: "Test10",
			srcArgs: &types.Bytes{
				Data:    []byte("abcdefghijklmn"),
				Lengths: []uint32{uint32(len("abcdefghijklmn"))},
				Offsets: []uint32{0},
			},
			startArgs:  []int32{0},
			startType:  types.T_int32,
			lengthArgs: []int16{-10},
			lengthType: types.T_int16,
			cs:         []bool{false, false, false},
			want: &types.Bytes{
				Data:    []byte(""),
				Lengths: []uint32{uint32(len(""))},
				Offsets: []uint32{0},
			},
		},
		//----------------------------------------
		{
			name: "Test11",
			srcArgs: &types.Bytes{
				Data:    []byte("abcdefghijklmn"),
				Lengths: []uint32{uint32(len("abcdefghijklmn"))},
				Offsets: []uint32{0},
			},
			startArgs:  []int16{-10},
			startType:  types.T_int16,
			lengthArgs: []int16{-2},
			lengthType: types.T_int16,
			cs:         []bool{false, false, false},
			want: &types.Bytes{
				Data:    []byte("efghijkl"),
				Lengths: []uint32{uint32(len("efghijkl"))},
				Offsets: []uint32{0},
			},
		},
		{
			name: "Test12",
			srcArgs: &types.Bytes{
				Data:    []byte("abcdefghijklmn"),
				Lengths: []uint32{uint32(len("abcdefghijklmn"))},
				Offsets: []uint32{0},
			},
			startArgs:  []int32{-10},
			startType:  types.T_int32,
			lengthArgs: []int64{-6},
			lengthType: types.T_int64,
			cs:         []bool{false, false, false},
			want: &types.Bytes{
				Data:    []byte("efgh"),
				Lengths: []uint32{uint32(len("efgh"))},
				Offsets: []uint32{0},
			},
		},
		{
			name: "Test13",
			srcArgs: &types.Bytes{
				Data:    []byte("abcdefghijklmn"),
				Lengths: []uint32{uint32(len("abcdefghijklmn"))},
				Offsets: []uint32{0},
			},
			startArgs:  []int8{-10},
			startType:  types.T_int8,
			lengthArgs: []int32{-12},
			lengthType: types.T_int32,
			cs:         []bool{false, false, false},
			want: &types.Bytes{
				Data:    []byte(""),
				Lengths: []uint32{uint32(len(""))},
				Offsets: []uint32{0},
			},
		},
		{
			name: "Test14",
			srcArgs: &types.Bytes{
				Data:    []byte("abcdefghijklmn"),
				Lengths: []uint32{uint32(len("abcdefghijklmn"))},
				Offsets: []uint32{0},
			},
			startArgs:  []int16{-10},
			startType:  types.T_int16,
			lengthArgs: []int32{0},
			lengthType: types.T_int32,
			cs:         []bool{false, false, false},
			want: &types.Bytes{
				Data:    []byte(""),
				Lengths: []uint32{uint32(len(""))},
				Offsets: []uint32{0},
			},
		},
		{
			name: "Test15",
			srcArgs: &types.Bytes{
				Data:    []byte("abcdefghijklmn"),
				Lengths: []uint32{uint32(len("abcdefghijklmn"))},
				Offsets: []uint32{0},
			},
			startArgs:  []int32{-10},
			startType:  types.T_int32,
			lengthArgs: []int64{-9},
			lengthType: types.T_int64,
			cs:         []bool{false, false, false},
			want: &types.Bytes{
				Data:    []byte("e"),
				Lengths: []uint32{uint32(len("e"))},
				Offsets: []uint32{0},
			},
		},
		{
			name: "Test16",
			srcArgs: &types.Bytes{
				Data:    []byte("abcdefghijklmn"),
				Lengths: []uint32{uint32(len("abcdefghijklmn"))},
				Offsets: []uint32{0},
			},
			startArgs:  []int32{-10},
			startType:  types.T_int32,
			lengthArgs: []int64{-10},
			lengthType: types.T_int64,
			cs:         []bool{false, false, false},
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
				Data:    make([]byte, len(c.srcArgs.Data)),
				Lengths: make([]uint32, len(c.srcArgs.Lengths)),
				Offsets: make([]uint32, len(c.srcArgs.Offsets)),
			}

			got := SubstringDynamicOffsetBounded(c.srcArgs, out, c.startArgs, c.startType, c.lengthArgs, c.lengthType, c.cs)
			require.Equal(t, c.want.String(), got.String())
		})
	}
}
