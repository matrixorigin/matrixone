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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestDateAdd(t *testing.T) {
	cases := []struct {
		name string
		vecs []*vector.Vector
		proc *process.Process
		want string
	}{
		{
			name: "TEST01",
			vecs: makeDateAddVectors("2022-01-01", true, 1, types.Day, proc.Mp()),
			proc: testutil.NewProc(),
			want: "2022-01-02",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			date, err := DateAdd(c.vecs, c.proc)
			if err != nil {
				t.Fatal(err)
			}
			require.Equal(t, c.want, date.Col.([]types.Date)[0].String())
		})
	}

}

func TestDatetimeAdd(t *testing.T) {
	cases := []struct {
		name string
		vecs []*vector.Vector
		proc *process.Process
		want string
	}{
		{
			name: "TEST01",
			vecs: makeDatetimeAddVectors("2022-01-01 00:00:00", true, 1, types.Day, proc.Mp()),
			proc: testutil.NewProc(),
			want: "2022-01-02 00:00:00",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			date, err := DatetimeAdd(c.vecs, c.proc)
			if err != nil {
				t.Fatal(err)
			}
			require.Equal(t, c.want, date.Col.([]types.Datetime)[0].String())
		})
	}

}

func TestDateStringAdd(t *testing.T) {
	cases := []struct {
		name string
		vecs []*vector.Vector
		proc *process.Process
		want string
		err  uint16
	}{
		{
			name: "TEST01",
			vecs: makeDateStringAddVectors("2022-01-01", true, 1, types.Day, proc.Mp()),
			proc: testutil.NewProc(),
			want: "2022-01-02 00:00:00",
			err:  0,
		},
		{
			name: "TEST02",
			vecs: makeDateStringAddVectors("2022-01-01 00:00:00", true, 1, types.Day, proc.Mp()),
			proc: testutil.NewProc(),
			want: "2022-01-02 00:00:00",
			err:  0,
		},
		{
			name: "TEST03",
			vecs: makeDateStringAddVectors("2022-01-01", true, 1, types.Second, proc.Mp()),
			proc: testutil.NewProc(),
			want: "2022-01-01 00:00:01",
			err:  0,
		},
		{
			name: "TEST04",
			vecs: makeDateStringAddVectors("xxxx", true, 1, types.Second, proc.Mp()),
			proc: testutil.NewProc(),
			want: "0001-01-01 00:00:00",
			err:  moerr.ErrInvalidInput,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			date, err := DateStringAdd(c.vecs, c.proc)
			require.Equal(t, c.want, date.Col.([]types.Datetime)[0].String())
			require.True(t, moerr.IsMoErrCode(err, c.err))
		})
	}

}

func makeDateAddVectors(str string, isConst bool, num int64, unit types.IntervalType, mp *mpool.MPool) []*vector.Vector {
	vec := make([]*vector.Vector, 3)

	date, _ := types.ParseDateCast(str)

	vec[0] = vector.NewConstFixed(types.T_date.ToType(), 1, date, mp)
	vec[1] = vector.NewConstFixed(types.T_int64.ToType(), 1, num, mp)
	vec[2] = vector.NewConstFixed(types.T_int64.ToType(), 1, int64(unit), mp)
	return vec
}

func makeDatetimeAddVectors(str string, isConst bool, num int64, unit types.IntervalType, mp *mpool.MPool) []*vector.Vector {
	vec := make([]*vector.Vector, 3)

	datetime, _ := types.ParseDatetime(str, 0)

	vec[0] = vector.NewConstFixed(types.T_datetime.ToType(), 1, datetime, mp)
	vec[1] = vector.NewConstFixed(types.T_int64.ToType(), 1, num, mp)
	vec[2] = vector.NewConstFixed(types.T_int64.ToType(), 1, int64(unit), mp)
	return vec
}

func makeDateStringAddVectors(str string, isConst bool, num int64, unit types.IntervalType, mp *mpool.MPool) []*vector.Vector {
	vec := make([]*vector.Vector, 3)
	vec[0] = vector.NewConstString(types.T_varchar.ToType(), 1, str, mp)
	vec[1] = vector.NewConstFixed(types.T_int64.ToType(), 1, num, mp)
	vec[2] = vector.NewConstFixed(types.T_int64.ToType(), 1, int64(unit), mp)
	return vec
}
