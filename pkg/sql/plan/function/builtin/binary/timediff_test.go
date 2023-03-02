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

func TestTimeDiffInTime(t *testing.T) {
	procs := testutil.NewProc()
	cases := []struct {
		name string
		vecs []*vector.Vector
		proc *process.Process
		want *vector.Vector
	}{
		{
			name: "TEST01",
			vecs: makeTimeVectors("22:22:22", "11:11:11", procs.Mp()),
			proc: testutil.NewProc(),
			want: makeResultVector("11:11:11", procs),
		},
		{
			name: "TEST02",
			vecs: makeTimeVectors("22:22:22", "-11:11:11", procs.Mp()),
			proc: testutil.NewProc(),
			want: makeResultVector("33:33:33", procs),
		},
		{
			name: "TEST03",
			vecs: makeTimeVectors("-22:22:22", "11:11:11", procs.Mp()),
			proc: testutil.NewProc(),
			want: makeResultVector("-33:33:33", procs),
		},
		{
			name: "TEST04",
			vecs: makeTimeVectors("-22:22:22", "-11:11:11", procs.Mp()),
			proc: testutil.NewProc(),
			want: makeResultVector("-11:11:11", procs),
		},
		{
			name: "TEST05",
			vecs: makeTimeVectors("11:11:11", "22:22:22", procs.Mp()),
			proc: testutil.NewProc(),
			want: makeResultVector("-11:11:11", procs),
		},
		{
			name: "TEST06",
			vecs: makeTimeVectors("11:11:11", "-22:22:22", procs.Mp()),
			proc: testutil.NewProc(),
			want: makeResultVector("33:33:33", procs),
		},
		{
			name: "TEST07",
			vecs: makeTimeVectors("-11:11:11", "22:22:22", procs.Mp()),
			proc: testutil.NewProc(),
			want: makeResultVector("-33:33:33", procs),
		},
		{
			name: "TEST08",
			vecs: makeTimeVectors("-11:11:11", "-22:22:22", procs.Mp()),
			proc: testutil.NewProc(),
			want: makeResultVector("11:11:11", procs),
		},
		{
			name: "TEST09",
			vecs: makeTimeVectors("-2562047787:59:59", "-2562047787:59:59", procs.Mp()),
			proc: testutil.NewProc(),
			want: makeResultVector("00:00:00", procs),
		},
		{
			name: "TEST10",
			vecs: makeTimeVectors("2562047787:59:59", "2562047787:59:59", procs.Mp()),
			proc: testutil.NewProc(),
			want: makeResultVector("00:00:00", procs),
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			diff, err := TimeDiff[types.Time](c.vecs, c.proc)
			if err != nil {
				t.Fatal(err)
			}
			require.Equal(t, vector.MustFixedCol[types.Time](c.want), vector.MustFixedCol[types.Time](diff))
		})
	}
}

func TestTimeDiffInDateTime(t *testing.T) {
	procs := testutil.NewProc()
	cases := []struct {
		name string
		vecs []*vector.Vector
		proc *process.Process
		want *vector.Vector
	}{
		{
			name: "TEST01",
			vecs: makeDateTimeVectors("2012-12-12 22:22:22", "2012-12-12 11:11:11", procs.Mp()),
			proc: testutil.NewProc(),
			want: makeResultVector("11:11:11", procs),
		},
		{
			name: "TEST02",
			vecs: makeDateTimeVectors("2012-12-12 11:11:11", "2012-12-12 22:22:22", procs.Mp()),
			proc: testutil.NewProc(),
			want: makeResultVector("-11:11:11", procs),
		},
		{
			name: "TEST03",
			vecs: makeDateTimeVectors("2012-12-12 22:22:22", "2000-12-12 11:11:11", procs.Mp()),
			proc: testutil.NewProc(),
			want: makeResultVector("105203:11:11", procs),
		},
		{
			name: "TEST04",
			vecs: makeDateTimeVectors("2000-12-12 11:11:11", "2012-12-12 22:22:22", procs.Mp()),
			proc: testutil.NewProc(),
			want: makeResultVector("-105203:11:11", procs),
		},
		{
			name: "TEST05",
			vecs: makeDateTimeVectors("2012-12-12 22:22:22", "2012-10-10 11:11:11", procs.Mp()),
			proc: testutil.NewProc(),
			want: makeResultVector("1523:11:11", procs),
		},
		{
			name: "TEST06",
			vecs: makeDateTimeVectors("2012-10-10 11:11:11", "2012-12-12 22:22:22", procs.Mp()),
			proc: testutil.NewProc(),
			want: makeResultVector("-1523:11:11", procs),
		},
		{
			name: "TEST07",
			vecs: makeDateTimeVectors("2012-12-12 22:22:22", "2012-12-10 11:11:11", procs.Mp()),
			proc: testutil.NewProc(),
			want: makeResultVector("59:11:11", procs),
		},
		{
			name: "TEST08",
			vecs: makeDateTimeVectors("2012-12-10 11:11:11", "2012-12-12 22:22:22", procs.Mp()),
			proc: testutil.NewProc(),
			want: makeResultVector("-59:11:11", procs),
		},
		{
			name: "TEST09",
			vecs: makeDateTimeVectors("2012-12-10 11:11:11", "2012-12-10 11:11:11", procs.Mp()),
			proc: testutil.NewProc(),
			want: makeResultVector("00:00:00", procs),
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			diff, err := TimeDiff[types.Datetime](c.vecs, c.proc)
			if err != nil {
				t.Fatal(err)
			}
			require.Equal(t, vector.MustFixedCol[types.Time](c.want), vector.MustFixedCol[types.Time](diff))
		})
	}
}

func makeDateTimeVectors(firstStr, secondStr string, mp *mpool.MPool) []*vector.Vector {
	vec := make([]*vector.Vector, 2)

	firstDate, _ := types.ParseDatetime(firstStr, 0)
	secondDate, _ := types.ParseDatetime(secondStr, 0)

	vec[0] = vector.NewVec(types.T_datetime.ToType())
	vector.AppendFixed(vec[0], firstDate, false, mp)
	vec[1] = vector.NewVec(types.T_datetime.ToType())
	vector.AppendFixed(vec[1], secondDate, false, mp)
	return vec
}

func makeTimeVectors(firstStr, secondStr string, mp *mpool.MPool) []*vector.Vector {
	vec := make([]*vector.Vector, 2)

	firstDate, _ := types.ParseTime(firstStr, 0)
	secondDate, _ := types.ParseTime(secondStr, 0)

	vec[0] = vector.NewVec(types.T_time.ToType())
	vector.AppendFixed(vec[0], firstDate, false, mp)
	vec[1] = vector.NewVec(types.T_time.ToType())
	vector.AppendFixed(vec[1], secondDate, false, mp)
	return vec
}

func makeResultVector(res string, proc *process.Process) *vector.Vector {

	resData, _ := types.ParseTime(res, 0)
	vec := vector.NewVec(types.T_time.ToType())
	vector.AppendFixed(vec, resData, false, proc.Mp())
	return vec
}
