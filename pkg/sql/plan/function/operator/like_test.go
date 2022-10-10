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

package operator

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestLikeVarchar(t *testing.T) {
	cases := []struct {
		name      string
		vecs      []*vector.Vector
		proc      *process.Process
		wantBytes []bool
	}{
		{
			name:      "TEST01",
			vecs:      makeLikeVectors("RUNOOB.COM", "%COM", true, true),
			proc:      testutil.NewProcessWithMPool(mpool.MustNewZero()),
			wantBytes: []bool{true},
		},
		{
			name:      "TEST02",
			vecs:      makeLikeVectors("aaa", "aaa", true, true),
			proc:      testutil.NewProcessWithMPool(mpool.MustNewZero()),
			wantBytes: []bool{true},
		},
		{
			name:      "TEST03",
			vecs:      makeLikeVectors("123", "1%", true, true),
			proc:      testutil.NewProcessWithMPool(mpool.MustNewZero()),
			wantBytes: []bool{true},
		},
		{
			name:      "TEST04",
			vecs:      makeLikeVectors("SALESMAN", "%SAL%", true, true),
			proc:      testutil.NewProcessWithMPool(mpool.MustNewZero()),
			wantBytes: []bool{true},
		},
		{
			name:      "TEST05",
			vecs:      makeLikeVectors("MANAGER@@@", "MAN_", true, true),
			proc:      testutil.NewProcessWithMPool(mpool.MustNewZero()),
			wantBytes: []bool{false},
		},
		{
			name:      "TEST06",
			vecs:      makeLikeVectors("MANAGER@@@", "_", true, true),
			proc:      testutil.NewProcessWithMPool(mpool.MustNewZero()),
			wantBytes: []bool{false},
		},
		{
			name:      "TEST07",
			vecs:      makeLikeVectors("hello@world", "hello_world", true, true),
			proc:      testutil.NewProcessWithMPool(mpool.MustNewZero()),
			wantBytes: []bool{true},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			likeRes, err := Like(c.vecs, c.proc)
			if err != nil {
				t.Fatal(err)
			}
			require.Equal(t, c.wantBytes, likeRes.Col.([]bool))
		})
	}
}

func TestLikeVarchar2(t *testing.T) {
	procs := testutil.NewProc()
	cases := []struct {
		name       string
		vecs       []*vector.Vector
		proc       *process.Process
		wantBytes  []bool
		wantScalar bool
	}{
		{
			name:       "TEST01",
			vecs:       makeLikeVectors("RUNOOB.COM", "%COM", false, true),
			proc:       procs,
			wantBytes:  []bool{true},
			wantScalar: false,
		},
		{
			name:       "TEST02",
			vecs:       makeLikeVectors("aaa", "aaa", false, true),
			proc:       procs,
			wantBytes:  []bool{true},
			wantScalar: false,
		},
		{
			name:       "TEST03",
			vecs:       makeLikeVectors("123", "1%", false, true),
			proc:       procs,
			wantBytes:  []bool{true},
			wantScalar: false,
		},
		{
			name:       "TEST04",
			vecs:       makeLikeVectors("SALESMAN", "%SAL%", false, true),
			proc:       procs,
			wantBytes:  []bool{true},
			wantScalar: false,
		},
		{
			name:       "TEST05",
			vecs:       makeLikeVectors("MANAGER@@@", "MAN_", false, true),
			proc:       procs,
			wantBytes:  []bool{false},
			wantScalar: false,
		},
		{
			name:       "TEST06",
			vecs:       makeLikeVectors("MANAGER@@@", "_", false, true),
			proc:       procs,
			wantBytes:  []bool{false},
			wantScalar: false,
		},
		{
			name:       "TEST07",
			vecs:       makeLikeVectors("hello@world", "hello_world", false, true),
			proc:       procs,
			wantBytes:  []bool{true},
			wantScalar: false,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			likeRes, err := Like(c.vecs, c.proc)
			if err != nil {
				t.Fatal(err)
			}
			require.Equal(t, c.wantBytes, likeRes.Col.([]bool))
			require.Equal(t, c.wantScalar, likeRes.IsScalar())
		})
	}
}

func makeStrVec(s string, isConst bool, n int) *vector.Vector {
	if isConst {
		return vector.NewConstString(types.T_varchar.ToType(), n, s, testutil.TestUtilMp)
	} else {
		return vector.NewWithStrings(types.T_varchar.ToType(), []string{s}, nil, testutil.TestUtilMp)
	}
}

func makeLikeVectors(src string, pat string, isSrcConst bool, isPatConst bool) []*vector.Vector {
	resVectors := make([]*vector.Vector, 2)
	resVectors[0] = makeStrVec(src, isSrcConst, 10)
	resVectors[1] = makeStrVec(pat, isPatConst, 10)
	return resVectors
}
