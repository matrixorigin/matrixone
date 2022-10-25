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

package unary

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestTime(t *testing.T) {
	cases := []struct {
		name    string
		input   string
		precise int32
		isConst bool
		testTyp types.Type
		vecs    []*vector.Vector
		proc    *process.Process
		want    []types.Time
	}{
		//============================== Date ==============================
		{
			name:    "TimeTest-FromDate01",
			input:   "2022-01-01",
			precise: 0,
			isConst: false,
			testTyp: types.T_date.ToType(),
			proc:    testutil.NewProc(),
			want:    []types.Time{types.FromTimeClock(false, 0, 0, 0, 0)},
		},
		{
			name:    "TimeTest-FromDate02",
			input:   "20110101",
			precise: 0,
			isConst: false,
			testTyp: types.T_date.ToType(),
			proc:    testutil.NewProc(),
			want:    []types.Time{types.FromTimeClock(false, 0, 0, 0, 0)},
		},

		//============================== Datetime ==============================
		{
			name:    "TimeTest-FromDatetime01",
			input:   "2022-01-01 16:22:44",
			precise: 0,
			isConst: false,
			testTyp: types.T_datetime.ToType(),
			proc:    testutil.NewProc(),
			want:    []types.Time{types.FromTimeClock(false, 16, 22, 44, 0)},
		},
		{
			name:    "TimeTest-FromDatetime02",
			input:   "2022-01-01 16:22:44.123456",
			precise: 4,
			isConst: false,
			testTyp: types.T_datetime.ToType(),
			proc:    testutil.NewProc(),
			want:    []types.Time{types.FromTimeClock(false, 16, 22, 44, 123500)},
		},
		{
			name:    "TimeTest-FromDatetime03",
			input:   "20220101224433.123456",
			precise: 6,
			isConst: false,
			testTyp: types.T_datetime.ToType(),
			proc:    testutil.NewProc(),
			want:    []types.Time{types.FromTimeClock(false, 22, 44, 33, 123456)},
		},

		//============================== DateString ==============================
		// precise is default 6 when input
		{
			name:    "TimeTest-FromDateString01",
			input:   "20110101112233",
			precise: 6,
			isConst: false,
			testTyp: types.T_varchar.ToType(),
			proc:    testutil.NewProc(),
			want:    []types.Time{types.FromTimeClock(false, 11, 22, 33, 0)},
		},
		{
			name:    "TimeTest-FromDateString02",
			input:   "2022-01-01 16:22:44.1235",
			precise: 6,
			isConst: false,
			testTyp: types.T_varchar.ToType(),
			proc:    testutil.NewProc(),
			want:    []types.Time{types.FromTimeClock(false, 16, 22, 44, 123500)},
		},
		{
			name:    "TimeTest-FromDateString03",
			input:   "2022-01-01 16:22:44",
			precise: 6,
			isConst: false,
			testTyp: types.T_varchar.ToType(),
			proc:    testutil.NewProc(),
			want:    []types.Time{types.FromTimeClock(false, 16, 22, 44, 0)},
		},
		{
			name:    "TimeTest-FromDateString04",
			input:   "-112233",
			precise: 6,
			isConst: false,
			testTyp: types.T_varchar.ToType(),
			proc:    testutil.NewProc(),
			want:    []types.Time{types.FromTimeClock(true, 11, 22, 33, 0)},
		},
		{
			name:    "TimeTest-FromDateString05",
			input:   "-233.123",
			precise: 6,
			isConst: false,
			testTyp: types.T_varchar.ToType(),
			proc:    testutil.NewProc(),
			want:    []types.Time{types.FromTimeClock(true, 0, 2, 33, 123000)},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			vec, err1 := makeVectorForTimeTest(c.input, c.precise, c.isConst, c.testTyp, c.proc)
			require.Equal(t, uint16(0), err1)

			var result *vector.Vector
			var err error
			switch c.testTyp.Oid {
			case types.T_date:
				result, err = DateToTime(vec, c.proc)
			case types.T_datetime:
				result, err = DatetimeToTime(vec, c.proc)
			case types.T_char, types.T_varchar:
				result, err = DateStringToTime(vec, c.proc)
			}
			require.NoError(t, err)
			require.Equal(t, c.want, result.Col.([]types.Time))
		})
	}

}

func makeVectorForTimeTest(str string, precision int32, isConst bool, typ types.Type, proc *process.Process) ([]*vector.Vector, uint16) {
	vec := make([]*vector.Vector, 1)
	if isConst {
		switch typ.Oid {
		case types.T_date:
			data, err := types.ParseDate(str)
			if err != nil {
				return nil, moerr.ErrInvalidInput
			}
			vec[0] = vector.NewConstFixed(types.T_date.ToType(), 1, data, testutil.TestUtilMp)
		case types.T_datetime:
			data, err := types.ParseDatetime(str, precision)
			if err != nil {
				return nil, moerr.ErrInvalidInput
			}
			vec[0] = vector.NewConstFixed(types.T_datetime.ToType(), 1, data, testutil.TestUtilMp)
			vec[0].Typ.Precision = precision
		case types.T_char, types.T_varchar:
			vec[0] = vector.NewConstString(types.Type{Oid: types.T_varchar, Size: 26}, 1, str, testutil.TestUtilMp)
		}
	} else {
		input := make([]string, 0)
		input = append(input, str)
		switch typ.Oid {
		case types.T_date:
			vec[0] = testutil.MakeDateVector(input, nil)
		case types.T_datetime:
			vec[0] = testutil.MakeDateTimeVector(input, nil)
			vec[0].Typ.Precision = precision
		case types.T_char:
			vec[0] = testutil.MakeCharVector(input, nil)
		case types.T_varchar:
			vec[0] = testutil.MakeVarcharVector(input, nil)
		default:
			return vec, moerr.ErrInvalidInput
		}
	}
	return vec, 0
}
