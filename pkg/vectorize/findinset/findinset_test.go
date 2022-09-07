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

package findinset

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFindInSet(t *testing.T) {
	tt := []struct {
		str     string
		strlist string
		idx     int
	}{
		{"abc", "abc,uzi", 1},
		{"xyz", "dec,xyz,abc", 2},
		{"z", "a,e,c,z", 4},
		// not match
		{"a,", "a,d,", 0},
		{"", "a,d,", 3},
		{" xxx ", "wwww, xxx ", 2},
		{"dbs", "abc,def", 0},
		{"测试", "测试1,测试", 2},
	}
	strs := make([]string, len(tt))
	strlists := make([]string, len(tt))
	want := make([]uint64, len(tt))
	for i, tc := range tt {
		rs := findInStrList(tc.str, tc.strlist)
		if rs != uint64(tc.idx) {
			t.Fatalf("findInStrList(%s, %s) = %d, want %d", tc.str, tc.strlist, rs, tc.idx)
		}
		strs[i] = tc.str
		strlists[i] = tc.strlist
		want[i] = uint64(tc.idx)
	}

	lv := strs
	rv := strlists
	got := make([]uint64, len(tt))
	got = FindInSet(lv, rv, got)
	require.Equal(t, want, got)
}

func TestFindInSetWithLeftConst(t *testing.T) {
	tt := []struct {
		str      string
		strlists []string
		want     []uint64
		got      []uint64
	}{
		{
			str:      "U",
			strlists: []string{"I,U,O", "U,P,V", "V,W,Z", "W,X,U"},
			want:     []uint64{2, 1, 0, 3},
			got:      make([]uint64, 4),
		},
	}

	for _, tc := range tt {
		lv := tc.str
		rv := tc.strlists
		got := FindInSetWithLeftConst(lv, rv, tc.got)
		require.Equal(t, tc.want, got)
	}
}

func TestFindInSetWithRightConst(t *testing.T) {
	tt := []struct {
		str      []string
		strlists string
		want     []uint64
		got      []uint64
	}{
		{
			str:      []string{"10", "2", "6", "8"},
			strlists: "1,2,3,4,5,6,7,8,9,10",
			want:     []uint64{10, 2, 6, 8},
			got:      make([]uint64, 4),
		},
	}

	for _, tc := range tt {
		lv := tc.str
		rv := tc.strlists
		got := FindInSetWithRightConst(lv, rv, tc.got)
		require.Equal(t, tc.want, got)
	}
}
