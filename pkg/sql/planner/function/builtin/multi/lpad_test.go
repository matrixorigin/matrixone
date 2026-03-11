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

package multi

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestLpadVarchar(t *testing.T) {
	cases := []struct {
		name      string
		vecs      []*vector.Vector
		wantBytes []byte
	}{
		{
			name:      "TEST01",
			vecs:      makeLpadVectors("hello", 1, "#", []int{1, 1, 1}),
			wantBytes: []byte("h"),
		},
		{
			name:      "TEST02",
			vecs:      makeLpadVectors("hello", 10, "#", []int{1, 1, 1}),
			wantBytes: []byte("#####hello"),
		},
		{
			name:      "TEST03",
			vecs:      makeLpadVectors("hello", 15, "#@&", []int{1, 1, 1}),
			wantBytes: []byte("#@&#@&#@&#hello"),
		},
		{
			name:      "TEST04",
			vecs:      makeLpadVectors("12345678", 10, "abcdefgh", []int{1, 1, 1}),
			wantBytes: []byte("ab12345678"),
		},
		{
			name:      "TEST05",
			vecs:      makeLpadVectors("hello", 0, "#@&", []int{1, 1, 1}),
			wantBytes: []byte(""),
		},
		{
			name:      "TEST06",
			vecs:      makeLpadVectors("hello", -1, "#@&", []int{1, 1, 1}),
			wantBytes: []byte(nil),
		},
		{
			name:      "Tx",
			vecs:      makeLpadVectors("hello", 1, "", []int{1, 1, 1}),
			wantBytes: []byte("h"),
		},
		{
			name:      "Tx2",
			vecs:      makeLpadVectors("", 5, "x", []int{1, 1, 1}),
			wantBytes: []byte("xxxxx"),
		},
		{
			name:      "Tx3",
			vecs:      makeLpadVectors("你好", 10, "再见", []int{1, 1, 1}),
			wantBytes: []byte("再见再见再见再见你好"),
		},
		{
			name:      "tx4",
			vecs:      makeLpadVectors("hello", -1, "#@&", []int{0, 0, 0}),
			wantBytes: []byte(nil),
		},
		{
			name:      "tx5",
			vecs:      makeLpadVectors("hello", -1, "#@&", []int{0, 0, 1}),
			wantBytes: []byte(nil),
		},
		{
			name:      "tx6",
			vecs:      makeLpadVectors("hello", -1, "#@&", []int{0, 1, 0}),
			wantBytes: []byte(nil),
		},
		{
			name:      "tx6",
			vecs:      makeLpadVectors("hello", -1, "#@&", []int{1, 0, 0}),
			wantBytes: []byte(nil),
		},
		{
			name:      "tx6",
			vecs:      makeLpadVectors("hello", -1, "#@&", []int{1, 1, 0}),
			wantBytes: []byte(nil),
		},
		{
			name:      "tx6",
			vecs:      makeLpadVectors("hello", -1, "#@&", []int{1, 0, 1}),
			wantBytes: []byte(nil),
		},
		{
			name:      "tx6",
			vecs:      makeLpadVectors("hello", -1, "#@&", []int{0, 1, 1}),
			wantBytes: []byte(nil),
		},
		{
			name:      "tx6",
			vecs:      makeLpadVectors("hello", -1, "#@&", []int{1, 1, 1}),
			wantBytes: []byte(nil),
		},
		{
			name:      "tx6",
			vecs:      makeLpadVectors("a你", 15, "见", []int{1, 1, 1}),
			wantBytes: []byte("见见见见见见见见见见见见见a你"),
		},
		{
			name:      "tx6",
			vecs:      makeLpadVectors("a你a", 15, "见a", []int{1, 1, 1}),
			wantBytes: []byte("见a见a见a见a见a见aa你a"),
		},
		{
			name:      "tx6",
			vecs:      makeLpadVectors("a你aa", 15, "见aa", []int{1, 1, 1}),
			wantBytes: []byte("见aa见aa见aa见aa你aa"),
		},
		{
			name:      "tx6",
			vecs:      makeLpadVectors("a你aaa", 15, "见aaa", []int{1, 1, 1}),
			wantBytes: []byte("见aaa见aaa见aa你aaa"),
		},
		{
			name:      "tx6",
			vecs:      makeLpadVectors("a你aaaa", 15, "见aaaa", []int{1, 1, 1}),
			wantBytes: []byte("见aaaa见aaaa你aaaa"),
		},
		{
			name:      "tx6",
			vecs:      makeLpadVectors("aaaaaaaa", 4, "bbb", []int{1, 1, 1}),
			wantBytes: []byte("aaaa"),
		},
	}

	proc := testutil.NewProcess()
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			lpad, err := Lpad(c.vecs, proc)
			if err != nil {
				t.Fatal(err)
			}
			if c.wantBytes == nil {
				require.Equal(t, lpad.IsConstNull(), true)
			} else {
				require.Equal(t, c.wantBytes, lpad.GetBytesAt(0))
			}

		})
	}

}

func makeLpadVectors(src string, length int64, pad string, nils []int) []*vector.Vector {
	vec := make([]*vector.Vector, 3)
	vec[0] = vector.NewConstBytes(types.T_varchar.ToType(), []byte(src), 1, testutil.TestUtilMp)
	vec[1] = vector.NewConstFixed(types.T_int64.ToType(), length, 1, testutil.TestUtilMp)
	vec[2] = vector.NewConstBytes(types.T_varchar.ToType(), []byte(pad), 1, testutil.TestUtilMp)
	for i, n := range nils {
		if n == 0 {
			vector.SetConstNull(vec[i], 1, testutil.TestUtilMp)
		}
	}
	return vec
}
