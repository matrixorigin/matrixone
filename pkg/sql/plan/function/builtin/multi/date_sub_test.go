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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestDateSub(t *testing.T) {
	cases := []struct {
		name string
		vecs []*vector.Vector
		proc *process.Process
		want string
	}{
		{
			name: "TEST01",
			vecs: makeDateSubVectors("2022-01-02", true, 1, types.Day),
			proc: testutil.NewProc(),
			want: "2022-01-01",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			date, err := DateSub(c.vecs, c.proc)
			if err != nil {
				t.Fatal(err)
			}
			require.Equal(t, c.want, vector.MustFixedCol[types.Date](date)[0].String())
		})
	}

}

func TestDatetimeSub(t *testing.T) {
	cases := []struct {
		name string
		vecs []*vector.Vector
		proc *process.Process
		want string
	}{
		{
			name: "TEST01",
			vecs: makeDatetimeSubVectors("2022-01-02 00:00:00", true, 1, types.Day),
			proc: testutil.NewProc(),
			want: "2022-01-01 00:00:00",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			date, err := DatetimeSub(c.vecs, c.proc)
			if err != nil {
				t.Fatal(err)
			}
			require.Equal(t, c.want, vector.MustFixedCol[types.Datetime](date)[0].String())
		})
	}

}

func TestDateStringSub(t *testing.T) {
	cases := []struct {
		name string
		vecs []*vector.Vector
		proc *process.Process
		want string
		err  uint16
	}{
		{
			name: "TEST01",
			vecs: makeDateStringSubVectors("2022-01-02", true, 1, types.Day),
			proc: testutil.NewProc(),
			want: "2022-01-01 00:00:00",
			err:  0,
		},
		{
			name: "TEST02",
			vecs: makeDateStringSubVectors("2022-01-02 00:00:00", true, 1, types.Day),
			proc: testutil.NewProc(),
			want: "2022-01-01 00:00:00",
			err:  0,
		},
		{
			name: "TEST03",
			vecs: makeDateStringSubVectors("2022-01-01", true, 1, types.Second),
			proc: testutil.NewProc(),
			want: "2021-12-31 23:59:59",
			err:  0,
		},
		//{
		//	name: "TEST04",
		//	vecs: makeDateStringSubVectors("xxxx", true, 1, types.Second),
		//	proc: testutil.NewProc(),
		//	want: "0001-01-01 00:00:00",
		//	err:  moerr.ErrInvalidInput,
		//},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			date, err := DateStringSub(c.vecs, c.proc)
			require.Equal(t, c.want, vector.MustFixedCol[types.Datetime](date)[0].String())
			require.True(t, moerr.IsMoErrCode(err, c.err))
		})
	}

}

func makeDateSubVectors(str string, isConst bool, num int64, unit types.IntervalType) []*vector.Vector {
	vec := make([]*vector.Vector, 3)

	date, _ := types.ParseDateCast(str)

	vec[0] = vector.NewConstFixed(types.T_date.ToType(), date, 1, testutil.TestUtilMp)
	vec[1] = vector.NewConstFixed(types.T_int64.ToType(), num, 1, testutil.TestUtilMp)
	vec[2] = vector.NewConstFixed(types.T_int64.ToType(), int64(unit), 1, testutil.TestUtilMp)
	return vec
}

func makeDatetimeSubVectors(str string, isConst bool, num int64, unit types.IntervalType) []*vector.Vector {
	vec := make([]*vector.Vector, 3)

	datetime, _ := types.ParseDatetime(str, 0)

	vec[0] = vector.NewConstFixed(types.T_datetime.ToType(), datetime, 1, testutil.TestUtilMp)
	vec[1] = vector.NewConstFixed(types.T_int64.ToType(), num, 1, testutil.TestUtilMp)
	vec[2] = vector.NewConstFixed(types.T_int64.ToType(), int64(unit), 1, testutil.TestUtilMp)
	return vec
}

func makeDateStringSubVectors(str string, isConst bool, num int64, unit types.IntervalType) []*vector.Vector {
	vec := make([]*vector.Vector, 3)
	vec[0] = vector.NewConstBytes(types.T_varchar.ToType(), []byte(str), 1, testutil.TestUtilMp)
	vec[1] = vector.NewConstFixed(types.T_int64.ToType(), num, 1, testutil.TestUtilMp)
	vec[2] = vector.NewConstFixed(types.T_int64.ToType(), int64(unit), 1, testutil.TestUtilMp)
	return vec
}
