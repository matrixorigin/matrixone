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
			vecs: makeTimeStampDiffVectors("2017-12-01 12:15:12", "2018-01-01 7:18:20", "microsecond", proc.Mp()),
			proc: testutil.NewProc(),
			want: 2660588000000,
		},
		{
			name: "TEST02",
			vecs: makeTimeStampDiffVectors("2017-12-01 12:15:12", "2018-01-01 7:18:20", "second", proc.Mp()),
			proc: testutil.NewProc(),
			want: 2660588,
		},
		{
			name: "TEST03",
			vecs: makeTimeStampDiffVectors("2017-12-01 12:15:12", "2018-01-01 7:18:20", "minute", proc.Mp()),
			proc: testutil.NewProc(),
			want: 44343,
		},
		{
			name: "TEST04",
			vecs: makeTimeStampDiffVectors("2017-12-01 12:15:12", "2018-01-01 7:18:20", "hour", proc.Mp()),
			proc: testutil.NewProc(),
			want: 739,
		},
		{
			name: "TEST05",
			vecs: makeTimeStampDiffVectors("2017-12-01 12:15:12", "2018-01-01 7:18:20", "day", proc.Mp()),
			proc: testutil.NewProc(),
			want: 30,
		},
		{
			name: "TEST06",
			vecs: makeTimeStampDiffVectors("2017-12-01 12:15:12", "2018-01-08 12:15:12", "week", proc.Mp()),
			proc: testutil.NewProc(),
			want: 5,
		},
		{
			name: "TEST07",
			vecs: makeTimeStampDiffVectors("2017-11-01 12:15:12", "2018-01-01 12:15:12", "month", proc.Mp()),
			proc: testutil.NewProc(),
			want: 2,
		},
		{
			name: "TEST08",
			vecs: makeTimeStampDiffVectors("2017-01-01 12:15:12", "2018-01-01 12:15:12", "quarter", proc.Mp()),
			proc: testutil.NewProc(),
			want: 4,
		},
		{
			name: "TEST09",
			vecs: makeTimeStampDiffVectors("2017-01-01 12:15:12", "2018-01-01 12:15:12", "year", proc.Mp()),
			proc: testutil.NewProc(),
			want: 1,
		},
		{
			name: "TEST10",
			vecs: makeTimeStampDiffReverseVectors("2017-12-01 12:15:12", "2018-01-01 7:18:20", "microsecond", proc.Mp()),
			proc: testutil.NewProc(),
			want: -2660588000000,
		},
		{
			name: "TEST11",
			vecs: makeTimeStampDiffReverseVectors("2017-12-01 12:15:12", "2018-01-01 7:18:20", "second", proc.Mp()),
			proc: testutil.NewProc(),
			want: -2660588,
		},
		{
			name: "TEST12",
			vecs: makeTimeStampDiffReverseVectors("2017-12-01 12:15:12", "2018-01-01 7:18:20", "minute", proc.Mp()),
			proc: testutil.NewProc(),
			want: -44343,
		},
		{
			name: "TEST13",
			vecs: makeTimeStampDiffReverseVectors("2017-12-01 12:15:12", "2018-01-01 7:18:20", "hour", proc.Mp()),
			proc: testutil.NewProc(),
			want: -739,
		},
		{
			name: "TEST14",
			vecs: makeTimeStampDiffReverseVectors("2017-12-01 12:15:12", "2018-01-01 7:18:20", "day", proc.Mp()),
			proc: testutil.NewProc(),
			want: -30,
		},
		{
			name: "TEST15",
			vecs: makeTimeStampDiffReverseVectors("2017-12-01 12:15:12", "2018-01-08 12:15:12", "week", proc.Mp()),
			proc: testutil.NewProc(),
			want: -5,
		},
		{
			name: "TEST16",
			vecs: makeTimeStampDiffReverseVectors("2017-11-01 12:15:12", "2018-01-01 12:15:12", "month", proc.Mp()),
			proc: testutil.NewProc(),
			want: -2,
		},
		{
			name: "TEST17",
			vecs: makeTimeStampDiffReverseVectors("2017-01-01 12:15:12", "2018-01-01 12:15:12", "quarter", proc.Mp()),
			proc: testutil.NewProc(),
			want: -4,
		},
		{
			name: "TEST18",
			vecs: makeTimeStampDiffReverseVectors("2017-01-01 12:15:12", "2018-01-01 12:15:12", "year", proc.Mp()),
			proc: testutil.NewProc(),
			want: -1,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			date, err := TimeStampDiff(c.vecs, c.proc)
			if err != nil {
				t.Fatal(err)
			}
			require.Equal(t, c.want, date.Col.([]int64)[0])
		})
	}
}

func makeTimeStampDiffVectors(firstStr, secondStr, unit string, mp *mpool.MPool) []*vector.Vector {
	vec := make([]*vector.Vector, 3)

	firstDate, _ := types.ParseDatetime(firstStr, 0)
	secondDate, _ := types.ParseDatetime(secondStr, 0)

	vec[1] = vector.NewConstFixed(types.T_datetime.ToType(), 1, firstDate, mp)
	vec[2] = vector.NewConstFixed(types.T_datetime.ToType(), 1, secondDate, mp)
	vec[0] = vector.NewConstString(types.T_varchar.ToType(), 1, unit, mp)
	return vec
}

func makeTimeStampDiffReverseVectors(firstStr, secondStr, unit string, mp *mpool.MPool) []*vector.Vector {
	vec := make([]*vector.Vector, 3)

	firstDate, _ := types.ParseDatetime(firstStr, 0)
	secondDate, _ := types.ParseDatetime(secondStr, 0)

	vec[1] = vector.NewConstFixed(types.T_datetime.ToType(), 1, secondDate, mp)
	vec[2] = vector.NewConstFixed(types.T_datetime.ToType(), 1, firstDate, mp)
	vec[0] = vector.NewConstString(types.T_varchar.ToType(), 1, unit, mp)
	return vec
}
