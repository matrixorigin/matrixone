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
	"strconv"
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
		scale   int32
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
			scale:   0,
			isConst: false,
			testTyp: types.T_date.ToType(),
			proc:    testutil.NewProc(),
			want:    []types.Time{types.TimeFromClock(false, 0, 0, 0, 0)},
		},
		{
			name:    "TimeTest-FromDate02",
			input:   "20110101",
			scale:   0,
			isConst: false,
			testTyp: types.T_date.ToType(),
			proc:    testutil.NewProc(),
			want:    []types.Time{types.TimeFromClock(false, 0, 0, 0, 0)},
		},

		//============================== Datetime ==============================
		{
			name:    "TimeTest-FromDatetime01",
			input:   "2022-01-01 16:22:44",
			scale:   0,
			isConst: false,
			testTyp: types.T_datetime.ToType(),
			proc:    testutil.NewProc(),
			want:    []types.Time{types.TimeFromClock(false, 16, 22, 44, 0)},
		},
		{
			name:    "TimeTest-FromDatetime02",
			input:   "2022-01-01 16:22:44.123456",
			scale:   4,
			isConst: false,
			testTyp: types.T_datetime.ToType(),
			proc:    testutil.NewProc(),
			want:    []types.Time{types.TimeFromClock(false, 16, 22, 44, 123500)},
		},

		//============================== DateString ==============================
		// precise is default 6 when input
		{
			name:    "TimeTest-FromDateString01",
			input:   "20110101112233",
			scale:   6,
			isConst: false,
			testTyp: types.T_varchar.ToType(),
			proc:    testutil.NewProc(),
			want:    []types.Time{types.TimeFromClock(false, 2011010111, 22, 33, 0)},
		},
		{
			name:    "TimeTest-FromDateString02",
			input:   "2022-01-01 16:22:44.1235",
			scale:   6,
			isConst: false,
			testTyp: types.T_varchar.ToType(),
			proc:    testutil.NewProc(),
			want:    []types.Time{types.TimeFromClock(false, 16, 22, 44, 123500)},
		},
		{
			name:    "TimeTest-FromDateString03",
			input:   "2022-01-01 16:22:44",
			scale:   6,
			isConst: false,
			testTyp: types.T_varchar.ToType(),
			proc:    testutil.NewProc(),
			want:    []types.Time{types.TimeFromClock(false, 16, 22, 44, 0)},
		},
		{
			name:    "TimeTest-FromDateString04",
			input:   "-112233",
			scale:   6,
			isConst: false,
			testTyp: types.T_varchar.ToType(),
			proc:    testutil.NewProc(),
			want:    []types.Time{types.TimeFromClock(true, 11, 22, 33, 0)},
		},
		{
			name:    "TimeTest-FromDateString05",
			input:   "-233.123",
			scale:   6,
			isConst: false,
			testTyp: types.T_varchar.ToType(),
			proc:    testutil.NewProc(),
			want:    []types.Time{types.TimeFromClock(true, 0, 2, 33, 123000)},
		},
		//============================== Int64 ==============================
		{
			name:    "TimeTest-FromInt64-01",
			input:   "112233",
			scale:   0,
			isConst: false,
			testTyp: types.T_int64.ToType(),
			proc:    testutil.NewProc(),
			want:    []types.Time{types.TimeFromClock(false, 11, 22, 33, 0)},
		},
		{
			name:    "TimeTest-FromInt64-02",
			input:   "20221212112233",
			scale:   0,
			isConst: false,
			testTyp: types.T_int64.ToType(),
			proc:    testutil.NewProc(),
			want:    []types.Time{types.TimeFromClock(false, 2022121211, 22, 33, 0)},
		},
		{
			name:    "TimeTest-FromInt64-03",
			input:   "-20221212112233",
			scale:   0,
			isConst: false,
			testTyp: types.T_int64.ToType(),
			proc:    testutil.NewProc(),
			want:    []types.Time{types.TimeFromClock(true, 2022121211, 22, 33, 0)},
		},
		//============================== Decimal128 ==============================
		{
			name:    "TimeTest-FromDecimal128-01",
			input:   "20221212112233.4444",
			scale:   3,
			isConst: false,
			testTyp: types.T_decimal128.ToType(),
			proc:    testutil.NewProc(),
			want:    []types.Time{types.TimeFromClock(false, 2022121211, 22, 33, 444000)},
		},
		{
			name:    "TimeTest-FromDecimal128-02",
			input:   "20221212112233.4446",
			scale:   3,
			isConst: false,
			testTyp: types.T_decimal128.ToType(),
			proc:    testutil.NewProc(),
			want:    []types.Time{types.TimeFromClock(false, 2022121211, 22, 33, 445000)},
		},
		{
			name:    "TimeTest-FromDecimal128-03",
			input:   "-20221212112233.4444",
			scale:   3,
			isConst: false,
			testTyp: types.T_decimal128.ToType(),
			proc:    testutil.NewProc(),
			want:    []types.Time{types.TimeFromClock(true, 2022121211, 22, 33, 444000)},
		},
		{
			name:    "TimeTest-FromDecimal128-04",
			input:   "-20221212112233.4446",
			scale:   3,
			isConst: false,
			testTyp: types.T_decimal128.ToType(),
			proc:    testutil.NewProc(),
			want:    []types.Time{types.TimeFromClock(true, 2022121211, 22, 33, 445000)},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			vec, err1 := makeVectorForTimeTest(c.input, c.scale, c.isConst, c.testTyp, c.proc)
			require.Equal(t, uint16(0), err1)

			var result *vector.Vector
			var err error
			switch c.testTyp.Oid {
			case types.T_int64:
				result, err = Int64ToTime(vec, c.proc)
			case types.T_decimal128:
				result, err = Decimal128ToTime(vec, c.proc)
			case types.T_date:
				result, err = DateToTime(vec, c.proc)
			case types.T_datetime:
				result, err = DatetimeToTime(vec, c.proc)
			case types.T_char, types.T_varchar:
				result, err = DateStringToTime(vec, c.proc)
			}
			require.NoError(t, err)
			require.Equal(t, c.want, vector.MustFixedCol[types.Time](result))
		})
	}

}

func makeVectorForTimeTest(str string, scale int32, isConst bool, typ types.Type, proc *process.Process) ([]*vector.Vector, uint16) {
	vec := make([]*vector.Vector, 1)
	if isConst {
		switch typ.Oid {
		case types.T_int64:
			data, err := strconv.Atoi(str)
			if err != nil {
				return nil, moerr.ErrInvalidInput
			}
			vec[0] = vector.NewConstFixed(types.T_int64.ToType(), data, 1, testutil.TestUtilMp)
		case types.T_decimal128:
			data, err := types.ParseStringToDecimal128(str, 34, scale, false)
			if err != nil {
				return nil, moerr.ErrInvalidInput
			}
			vec[0] = vector.NewConstFixed(types.T_int128.ToType(), data, 1, testutil.TestUtilMp)
		case types.T_date:
			data, err := types.ParseDateCast(str)
			if err != nil {
				return nil, moerr.ErrInvalidInput
			}
			vec[0] = vector.NewConstFixed(types.T_date.ToType(), data, 1, testutil.TestUtilMp)
		case types.T_datetime:
			data, err := types.ParseDatetime(str, scale)
			if err != nil {
				return nil, moerr.ErrInvalidInput
			}
			vec[0] = vector.NewConstFixed(types.T_date.ToType(), data, 1, testutil.TestUtilMp)
			vec[0].GetType().Scale = scale
		case types.T_char, types.T_varchar:
			vec[0] = vector.NewConstBytes(types.Type{Oid: types.T_varchar, Size: 26}, []byte(str), 1, testutil.TestUtilMp)
		}
	} else {
		input := make([]string, 0)
		input = append(input, str)
		switch typ.Oid {
		case types.T_int64:
			input := make([]int64, 0)
			tmp, err := strconv.Atoi(str)
			if err != nil {
				return nil, moerr.ErrInvalidInput
			}
			input = append(input, int64(tmp))
			vec[0] = testutil.MakeInt64Vector(input, nil)
		case types.T_decimal128:
			input := make([]types.Decimal128, 0)
			tmp, err := types.ParseStringToDecimal128(str, 34, scale, false)
			if err != nil {
				return nil, moerr.ErrInvalidInput
			}
			input = append(input, tmp)
			vec[0] = vector.NewVec(typ)
			vector.AppendFixedList(vec[0], input, nil, testutil.TestUtilMp)

		case types.T_date:
			vec[0] = testutil.MakeDateVector(input, nil)
		case types.T_datetime:
			vec[0] = testutil.MakeDateTimeVector(input, nil)
			vec[0].GetType().Scale = scale
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
