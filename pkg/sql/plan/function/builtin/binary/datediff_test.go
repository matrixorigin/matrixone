// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package binary

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestDateDiff(t *testing.T) {
	cases := []struct {
		name string
		vecs []*vector.Vector
		proc *process.Process
		want int64
	}{
		{
			name: "TEST01",
			vecs: makeDateDiffVectors("2017-08-17", "2017-08-17", procs.Mp()),
			proc: testutil.NewProc(),
			want: 0,
		},
		{
			name: "TEST02",
			vecs: makeDateDiffVectors("2017-08-17", "2017-08-08", procs.Mp()),
			proc: testutil.NewProc(),
			want: 9,
		},
		{
			name: "TEST03",
			vecs: makeDateDiffVectors("2017-08-08", "2017-08-17", procs.Mp()),
			proc: testutil.NewProc(),
			want: -9,
		},
		{
			name: "TEST04",
			vecs: makeDateDiffVectors("2022-10-9", "2012-10-11", procs.Mp()),
			proc: testutil.NewProc(),
			want: 3650,
		},
		{
			name: "TEST05",
			vecs: makeDateDiffVectors("2022-10-9", "2004-04-24", procs.Mp()),
			proc: testutil.NewProc(),
			want: 6742,
		},
		{
			name: "TEST06",
			vecs: makeDateDiffVectors("2022-10-9", "2008-12-04", procs.Mp()),
			proc: testutil.NewProc(),
			want: 5057,
		},
		{
			name: "TEST07",
			vecs: makeDateDiffVectors("2022-10-9", "2012-03-23", procs.Mp()),
			proc: testutil.NewProc(),
			want: 3852,
		},
		{
			name: "TEST08",
			vecs: makeDateDiffVectors("2022-10-9", "2000-03-23", procs.Mp()),
			proc: testutil.NewProc(),
			want: 8235,
		},
		{
			name: "TEST09",
			vecs: makeDateDiffVectors("2022-10-9", "2030-03-23", procs.Mp()),
			proc: testutil.NewProc(),
			want: -2722,
		},
		{
			name: "TEST03",
			vecs: makeDateDiffVectors("2022-10-9", "2040-03-23", procs.Mp()),
			proc: testutil.NewProc(),
			want: -6375,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			date, err := DateDiff(c.vecs, c.proc)
			if err != nil {
				t.Fatal(err)
			}
			require.Equal(t, c.want, date.Col.([]int64)[0])
		})
	}
}

func makeDateDiffVectors(firstStr, secondStr string, mp *mpool.MPool) []*vector.Vector {
	vec := make([]*vector.Vector, 2)

	firstDate, _ := types.ParseDateCast(firstStr)
	secondDate, _ := types.ParseDateCast(secondStr)

	vec[0] = vector.NewConstFixed(types.T_date.ToType(), 1, firstDate, mp)
	vec[1] = vector.NewConstFixed(types.T_date.ToType(), 1, secondDate, mp)
	return vec
}
