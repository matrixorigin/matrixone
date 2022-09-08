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

func TestSubstringFromLeftConstOffsetUnbounded(t *testing.T) {
	cases := []struct {
		name  string
		args1 []string
		start int64
		want  []string
	}{
		{
			name:  "Test01",
			args1: []string{"abcdefghijklmn"},
			start: 5,
			want:  []string{"efghijklmn"},
		},
		{
			name:  "Test02",
			args1: []string{"abcdefghijklmn"},
			start: 12,
			want:  []string{"lmn"},
		},
		{
			name:  "Test03",
			args1: []string{"abcdefghijklmn"},
			start: 15,
			want:  []string{""},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			out := make([]string, len(c.args1))
			got := SubstringFromLeftConstOffsetUnbounded(c.args1, out, c.start-1)
			require.Equal(t, c.want, got)
		})
	}
}

func TestSubstringFromRightConstOffsetUnbounded(t *testing.T) {
	cases := []struct {
		name  string
		args1 []string
		start int64
		want  []string
	}{
		{
			name:  "Test01",
			args1: []string{"abcdefghijklmn"},
			start: -5,
			want:  []string{"jklmn"},
		},
		{
			name:  "Test02",
			args1: []string{"abcdefghijklmn"},
			start: -14,
			want:  []string{"abcdefghijklmn"},
		},
		{
			name:  "Test03",
			args1: []string{"abcdefghijklmn"},
			start: -16,
			want:  []string{""},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			out := make([]string, len(c.args1))
			got := SubstringFromRightConstOffsetUnbounded(c.args1, out, -c.start)
			require.Equal(t, c.want, got)
		})
	}
}

func TestSubstringFromZeroConstOffsetUnbounded(t *testing.T) {
	cases := []struct {
		name  string
		args1 []string
		start int64
		want  []string
	}{
		{
			name:  "Test01",
			args1: []string{"abcdefghijklmn"},
			start: 0,
			want:  []string{""},
		},
		{
			name:  "Test02",
			args1: []string{"abcd132456"},
			start: 0,
			want:  []string{""},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			out := make([]string, len(c.args1))
			got := SubstringFromZeroConstOffsetUnbounded(c.args1, out)
			require.Equal(t, c.want, got)
		})
	}
}

func tTestSubstringDynamicOffsetUnbounded[T types.BuiltinNumber](t *testing.T, name string, arg []string, startArgs []T, want []string) {
	t.Run(name, func(t *testing.T) {
		out := make([]string, len(arg))
		got := SubstringDynamicOffsetUnbounded(arg, out, startArgs, []bool{false, false})
		require.Equal(t, want, got)
	})
}

func TestSubstringDynamicOffsetUnbounded(t *testing.T) {
	tTestSubstringDynamicOffsetUnbounded(t, "Testu1", []string{"abcdefghijklmn"}, []uint8{5}, []string{"efghijklmn"})
	tTestSubstringDynamicOffsetUnbounded(t, "Testu2", []string{"abcdefghijklmn"}, []uint16{5}, []string{"efghijklmn"})
	tTestSubstringDynamicOffsetUnbounded(t, "Testu3", []string{"abcdefghijklmn"}, []uint32{5}, []string{"efghijklmn"})
	tTestSubstringDynamicOffsetUnbounded(t, "Testu4", []string{"abcdefghijklmn"}, []uint64{5}, []string{"efghijklmn"})
	tTestSubstringDynamicOffsetUnbounded(t, "Testi1", []string{"abcdefghijklmn"}, []int8{5}, []string{"efghijklmn"})
	tTestSubstringDynamicOffsetUnbounded(t, "Testi2", []string{"abcdefghijklmn"}, []int16{5}, []string{"efghijklmn"})
	tTestSubstringDynamicOffsetUnbounded(t, "Testi3", []string{"abcdefghijklmn"}, []int32{5}, []string{"efghijklmn"})
	tTestSubstringDynamicOffsetUnbounded(t, "Testi4", []string{"abcdefghijklmn"}, []int64{5}, []string{"efghijklmn"})
	tTestSubstringDynamicOffsetUnbounded(t, "Testf3", []string{"abcdefghijklmn"}, []float32{5}, []string{"efghijklmn"})
	tTestSubstringDynamicOffsetUnbounded(t, "Testf4", []string{"abcdefghijklmn"}, []float64{5}, []string{"efghijklmn"})
}

//-------------------------------------------------------------

func TestSubstringFromLeftConstOffsetBounded(t *testing.T) {
	cases := []struct {
		name   string
		args   []string
		start  int64
		length int64
		want   []string
	}{
		{
			name:   "Test01",
			args:   []string{"abcdefghijklmn"},
			start:  5,
			length: 6,
			want:   []string{"efghij"},
		},
		{
			name:   "Test02",
			args:   []string{"abcdefghijklmn"},
			start:  5,
			length: 10,
			want:   []string{"efghijklmn"},
		},
		{
			name:   "Test03",
			args:   []string{"abcdefghijklmn"},
			start:  5,
			length: 0,
			want:   []string{""},
		},
		{
			name:   "Test04",
			args:   []string{"abcdefghijklmn"},
			start:  5,
			length: -3,
			want:   []string{""},
		},
		{
			name:   "Test05",
			args:   []string{"abcdefghijklmn"},
			start:  5,
			length: -8,
			want:   []string{""},
		},
		{
			name:   "Test06",
			args:   []string{"abcdefghijklmn"},
			start:  5,
			length: -10,
			want:   []string{""},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			out := make([]string, len(c.args))
			got := SubstringFromLeftConstOffsetBounded(c.args, out, c.start-1, c.length)
			require.Equal(t, c.want, got)
		})
	}
}

func TestSubstringFromRightConstOffsetBounded(t *testing.T) {
	cases := []struct {
		name   string
		args   []string
		start  int64
		length int64
		want   []string
	}{
		{
			name:   "Test03",
			args:   []string{"abcdefghijklmn"},
			start:  -10,
			length: 5,
			want:   []string{"efghi"},
		},
		{
			name:   "Test04",
			args:   []string{"abcdefghijklmn"},
			start:  -10,
			length: 0,
			want:   []string{""},
		},
		{
			name:   "Test05",
			args:   []string{"abcdefghijklmn"},
			start:  -10,
			length: 12,
			want:   []string{"efghijklmn"},
		},
		{
			name:   "Test06",
			args:   []string{"abcdefghijklmn"},
			start:  -14,
			length: 12,
			want:   []string{"abcdefghijkl"},
		},
		{
			name:   "Test07",
			args:   []string{"abcdefghijklmn"},
			start:  -20,
			length: 4,
			want:   []string{""},
		},
		{
			name:   "Test08",
			args:   []string{"abcdefghijklmn"},
			start:  -16,
			length: 10,
			want:   []string{""},
		},
		{
			name:   "Test09",
			args:   []string{"abcdefghijklmn"},
			start:  -16,
			length: 20,
			want:   []string{""},
		},
		{
			name:   "Test10",
			args:   []string{"abcdefghijklmn"},
			start:  -8,
			length: -4,
			want:   []string{""},
		},
		{
			name:   "Test11",
			args:   []string{"abcdefghijklmn"},
			start:  -8,
			length: -6,
			want:   []string{""},
		},
		{
			name:   "Test11",
			args:   []string{"abcdefghijklmn"},
			start:  -14,
			length: -6,
			want:   []string{""},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			out := make([]string, len(c.args))
			got := SubstringFromRightConstOffsetBounded(c.args, out, -c.start, c.length)
			require.Equal(t, c.want, got)
		})
	}
}

func TestSubstringFromZeroConstOffsetBounded(t *testing.T) {
	cases := []struct {
		name   string
		args1  []string
		start  int64
		length int64
		want   []string
	}{
		{
			name:   "Test01",
			args1:  []string{"abcdefghijklmn"},
			start:  0,
			length: 20,
			want:   []string{""},
		},
		{
			name:   "Test02",
			args1:  []string{"abcd132456"},
			start:  0,
			length: -5,
			want:   []string{""}},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			out := make([]string, len(c.args1))
			got := SubstringFromZeroConstOffsetBounded(c.args1, out)
			require.Equal(t, c.want, got)
		})
	}
}

func tTestSubstringDynamicOffsetBounded[T1, T2 types.BuiltinNumber](
	t *testing.T, name string, srcArgs []string, startArgs []T1, lengthArgs []T2, cs []bool, want []string) {
	t.Run(name, func(t *testing.T) {
		out := make([]string, len(srcArgs))
		got := SubstringDynamicOffsetBounded(srcArgs, out, startArgs, lengthArgs, cs)
		require.Equal(t, want, got)
	})
}

func TestSubstringDynamicOffsetBounded(t *testing.T) {
	tTestSubstringDynamicOffsetBounded(t, "Test01", []string{"abcdefghijklmn"},
		[]uint8{5}, []uint8{6}, []bool{false, false, false},
		[]string{"efghij"})

	tTestSubstringDynamicOffsetBounded(t, "Test02", []string{"abcdefghijklmn"},
		[]uint16{5}, []uint16{6}, []bool{false, false, false},
		[]string{"efghij"})

	tTestSubstringDynamicOffsetBounded(t, "Test03", []string{"abcdefghijklmn"},
		[]uint32{5}, []uint32{6}, []bool{false, false, false},
		[]string{"efghij"})

	tTestSubstringDynamicOffsetBounded(t, "Test04", []string{"abcdefghijklmn"},
		[]uint64{5}, []uint64{6}, []bool{false, false, false},
		[]string{"efghij"})

	tTestSubstringDynamicOffsetBounded(t, "Test04", []string{"abcdefghijklmn"},
		[]uint64{5}, []uint64{6}, []bool{false, false, false},
		[]string{"efghij"})

	tTestSubstringDynamicOffsetBounded(t, "Test05", []string{"abcdefghijklmn"},
		[]uint32{5}, []int64{6}, []bool{false, false, false},
		[]string{"efghij"})

	tTestSubstringDynamicOffsetBounded(t, "Test06", []string{"abcdefghijklmn"},
		[]uint32{5}, []int64{-6}, []bool{false, false, false},
		[]string{""})

	tTestSubstringDynamicOffsetBounded(t, "Test07", []string{"abcdefghijklmn"},
		[]uint32{5}, []int8{-6}, []bool{false, false, false},
		[]string{""})

	tTestSubstringDynamicOffsetBounded(t, "Test08", []string{"abcdefghijklmn"},
		[]uint32{5}, []int8{0}, []bool{false, false, false},
		[]string{""})

	tTestSubstringDynamicOffsetBounded(t, "Test09", []string{"abcdefghijklmn"},
		[]uint32{5}, []int16{-10}, []bool{false, false, false},
		[]string{""})

	tTestSubstringDynamicOffsetBounded(t, "Test10", []string{"abcdefghijklmn"},
		[]uint32{0}, []int16{-10}, []bool{false, false, false},
		[]string{""})

	tTestSubstringDynamicOffsetBounded(t, "Test11", []string{"abcdefghijklmn"},
		[]int32{-10}, []int16{-2}, []bool{false, false, false},
		[]string{""})

	tTestSubstringDynamicOffsetBounded(t, "Test12", []string{"abcdefghijklmn"},
		[]int32{-10}, []int64{-6}, []bool{false, false, false},
		[]string{""})

	tTestSubstringDynamicOffsetBounded(t, "Test13", []string{"abcdefghijklmn"},
		[]int32{-10}, []int64{-12}, []bool{false, false, false},
		[]string{""})

	tTestSubstringDynamicOffsetBounded(t, "Test14", []string{"abcdefghijklmn"},
		[]int32{-10}, []int64{0}, []bool{false, false, false},
		[]string{""})

	tTestSubstringDynamicOffsetBounded(t, "Test15", []string{"abcdefghijklmn"},
		[]int32{-10}, []int64{-9}, []bool{false, false, false},
		[]string{""})

	tTestSubstringDynamicOffsetBounded(t, "Test16", []string{"abcdefghijklmn"},
		[]int32{-10}, []int64{-10}, []bool{false, false, false},
		[]string{""})
}
