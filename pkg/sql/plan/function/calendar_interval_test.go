// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package function

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
	"math"
	"testing"
)

func TestCalendarIntervalNormalization(t *testing.T) {
	for _, tc := range []struct {
		text    string
		unit    types.IntervalType
		decimal bool
		want    int64
		state   timeIntervalState
	}{
		{"1.4", types.Day, true, 1, 0}, {"1.5", types.Day, true, 2, 0}, {"1.9", types.Day, true, 2, 0}, {"-1.5", types.Day, true, -2, 0},
		{"0.5", types.Day, true, 1, 0}, {"-0.5", types.Day, true, -1, 0}, {"1.4999999999999999999999999999999999999999", types.Day, true, 1, 0},
		{"1.5", types.Day, false, 1, 0}, {"-1.5", types.Day, false, -1, 0}, {"-0.999", types.Day, false, 0, 0},
		{"1.1234567", types.Second, true, 1123456, 0}, {"-1.1234567", types.Second, true, -1123456, 0},
		{"0.0000009", types.Second, true, 0, 0}, {"-0.0000009", types.Second, false, 0, 0},
		{"1.123456", types.Second, false, 1123456, 0}, {"1", types.Second, false, 1000000, 0},
		{"9223372036854.7758079", types.Second, true, math.MaxInt64, 0}, {"-9223372036854.7758089", types.Second, true, math.MinInt64, 0},
		{"9223372036854.775808", types.Second, true, 0, timeIntervalOverflow}, {"9223372036854775807.5", types.Day, true, 0, timeIntervalOverflow},
		{"9223372036854775807.9", types.Day, false, math.MaxInt64, 0}, {"18446744073709551616", types.Day, false, 0, timeIntervalOverflow},
		{"", types.Day, false, 0, timeIntervalInvalid}, {"NaN", types.Second, false, 0, timeIntervalInvalid}, {"1-2", types.Day, false, 0, timeIntervalInvalid},
		{"1.2.3", types.Second, false, 0, timeIntervalInvalid}, {"1.5junk", types.Second, false, 0, timeIntervalInvalid},
		// The pre-existing text digit-field grammar remains permissive.
		{"foo1", types.Day, false, 1, 0}, {".5", types.Day, false, 5, 0},
	} {
		t.Run(tc.text+tc.unit.String(), func(t *testing.T) {
			got := normalizeCalendarInterval(tc.text, tc.unit, tc.decimal)
			require.Equal(t, tc.state, got.state)
			if tc.state == timeIntervalValid {
				require.Equal(t, tc.want, got.value)
			}
		})
	}
}

func TestToCalendarIntervalTypedVectors(t *testing.T) {
	proc := testutil.NewProcess(t)
	for _, oid := range []types.T{types.T_decimal64, types.T_decimal128, types.T_decimal256} {
		for _, unit := range []types.IntervalType{types.Day, types.Second} {
			t.Run(oid.String()+unit.String(), func(t *testing.T) {
				texts := []string{"1.4", "1.5", "1.9", "-1.5", "0.5", "-0.5"}
				if unit == types.Second {
					texts = []string{"1.1234567", "-1.1234567", "0.0000009", "-0.0000009", "1.0000010", "-0.0000000"}
				}
				var input FunctionTestInput
				switch oid {
				case types.T_decimal64:
					values := make([]types.Decimal64, len(texts))
					for i, s := range texts {
						v, e := types.ParseDecimal64(s, 18, 7)
						require.NoError(t, e)
						values[i] = v
					}
					input = NewFunctionTestInput(types.New(oid, 18, 7), values, nil)
				case types.T_decimal128:
					values := make([]types.Decimal128, len(texts))
					for i, s := range texts {
						v, e := types.ParseDecimal128(s, 38, 7)
						require.NoError(t, e)
						values[i] = v
					}
					input = NewFunctionTestInput(types.New(oid, 38, 7), values, nil)
				case types.T_decimal256:
					values := make([]types.Decimal256, len(texts))
					for i, s := range texts {
						v, e := types.ParseDecimal256(s, 76, 7)
						require.NoError(t, e)
						values[i] = v
					}
					input = NewFunctionTestInput(types.New(oid, 76, 7), values, nil)
				}
				want := []int64{1, 2, 2, -2, 1, -1}
				if unit == types.Second {
					want = []int64{1123456, -1123456, 0, 0, 1000001, 0}
				}
				tc := NewFunctionTestCase(proc, []FunctionTestInput{input, NewFunctionTestConstInput(types.T_int64.ToType(), []int64{int64(unit)}, nil)}, NewFunctionTestResult(types.T_int64.ToType(), false, want, nil), ToCalendarInterval)
				ok, info := tc.RunAndFree()
				require.True(t, ok, info)
			})
		}
	}
	for _, tc := range []struct {
		name  string
		input FunctionTestInput
		unit  types.IntervalType
		want  []int64
		nulls []bool
	}{
		{"double day", NewFunctionTestInput(types.T_float64.ToType(), []float64{1.5, -1.5, math.NaN(), math.Inf(1), math.MaxFloat64, 0}, []bool{false, false, false, false, false, true}), types.Day, []int64{1, -1, 0, 0, math.MinInt64, 0}, []bool{false, false, true, true, false, true}},
		{"double second", NewFunctionTestInput(types.T_float64.ToType(), []float64{1.000009, 1.1234567, -0.0000009, -1.1234567, 1.000001}, nil), types.Second, []int64{1000009, 1123456, 0, -1123456, 1000001}, nil},
		{"float second", NewFunctionTestInput(types.T_float32.ToType(), []float32{1.000001, 1.1234567, -1.1234567}, nil), types.Second, []int64{1000000, 1123456, -1123456}, nil},
		{"text day", NewFunctionTestInput(types.T_text.ToType(), []string{"1.5", "-1.5", "bad", "9223372036854775808", "0"}, []bool{false, false, false, false, true}), types.Day, []int64{1, -1, 0, math.MinInt64, 0}, []bool{false, false, true, false, true}},
		{"constant second", NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"1.1234567"}, nil), types.Second, []int64{1123456}, nil},
		{"integer specialization", NewFunctionTestInput(types.T_int64.ToType(), []int64{1, -1, math.MaxInt64}, nil), types.Second, []int64{1000000, -1000000, math.MinInt64}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			def := NewFunctionTestCase(proc, []FunctionTestInput{tc.input, NewFunctionTestConstInput(types.T_int64.ToType(), []int64{int64(tc.unit)}, nil)}, NewFunctionTestResult(types.T_int64.ToType(), false, tc.want, tc.nulls), ToCalendarInterval)
			ok, info := def.RunAndFree()
			require.True(t, ok, info)
		})
	}
	masked := NewFunctionTestCase(proc, []FunctionTestInput{
		NewFunctionTestInput(types.T_varchar.ToType(), []string{"1.5", "999999999999999999999"}, nil),
		NewFunctionTestConstInput(types.T_int64.ToType(), []int64{int64(types.Day)}, nil)}, NewFunctionTestResult(types.T_int64.ToType(), false, []int64{1, 0}, []bool{false, true}), ToCalendarInterval).WithSelectList(&FunctionSelectList{AnyNull: true, SelectList: []bool{true, false}})
	ok, info := masked.RunAndFree()
	require.True(t, ok, info)
}
