// Copyright 2026 Matrix Origin
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

package function

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

// ADDTIME/SUBTIME TIME-domain overflow must clamp to the MySQL TIME boundary,
// exactly like the CAST-to-TIME path, instead of silently returning NULL.
func TestAddSubTimeClampsToMySQLTimeBoundary(t *testing.T) {
	max := types.MySQLTimeMax
	sec := types.Time(types.MicroSecsPerSec)

	t.Run("clamp helper keeps value and flag", func(t *testing.T) {
		got, clamped := timeArithmeticResult(max, sec, false)
		require.Equal(t, max, got)
		require.True(t, clamped)

		got, clamped = timeArithmeticResult(-max, -sec, false)
		require.Equal(t, -max, got)
		require.True(t, clamped)

		got, clamped = timeArithmeticResult(-max, sec, false)
		require.Equal(t, -max+sec, got)
		require.False(t, clamped)

		got, clamped = timeArithmeticResult(max, sec, true)
		require.Equal(t, max-sec, got)
		require.False(t, clamped)

		got, clamped = timeArithmeticResult(max, -sec, true)
		require.Equal(t, max, got)
		require.True(t, clamped)

		// 0 - MinInt64 == +2^63 exceeds the TIME ceiling: the clamped result
		// keeps the positive sign of the exact mathematical difference.
		got, clamped = timeArithmeticResult(types.Time(0), types.Time(math.MinInt64), true)
		require.Equal(t, max, got)
		require.True(t, clamped)

		// MinInt64 - MinInt64 cancels exactly to zero; no clamping happens.
		got, clamped = timeArithmeticResult(types.Time(math.MinInt64), types.Time(math.MinInt64), true)
		require.Equal(t, types.Time(0), got)
		require.False(t, clamped)

		// (MinInt64 + max) - MinInt64 lands exactly on the upper boundary.
		got, clamped = timeArithmeticResult(types.Time(math.MinInt64+int64(max)), types.Time(math.MinInt64), true)
		require.Equal(t, max, got)
		require.False(t, clamped)

		// max + MinInt64 stays a huge negative value below the TIME floor.
		got, clamped = timeArithmeticResult(max, types.Time(math.MinInt64), false)
		require.Equal(t, -max, got)
		require.True(t, clamped)
	})

	t.Run("typed operands time result", func(t *testing.T) {
		proc := newTmpProcess(t)
		warnings := &numericWarningSession{}
		proc.WarningSink = warnings

		left := NewFunctionTestInput(types.T_time.ToType(), []types.Time{
			max - sec, -max, 0, max, 0,
		}, nil)
		right := NewFunctionTestInput(types.T_varchar.ToType(), []string{
			"00:00:02", "00:00:01", "838:59:59", "-00:00:01", "1:00:00",
		}, []bool{false, false, false, false, true})
		want := NewFunctionTestResult(types.T_time.ToType(), false, []types.Time{
			max, -max + sec, max, max - sec, 0,
		}, []bool{false, false, false, false, true})
		caseDef := NewFunctionTestCase(proc, []FunctionTestInput{left, right}, want, AddTime)
		ok, info := caseDef.RunAndFree()
		require.True(t, ok, info)

		// SUBTIME keeps the same boundary semantics for typed TIME input.
		leftSub := NewFunctionTestInput(types.T_time.ToType(), []types.Time{-max, 0, 0}, nil)
		rightSub := NewFunctionTestInput(types.T_varchar.ToType(), []string{"00:00:01", "-838:59:59", "838:59:59"}, nil)
		wantSub := NewFunctionTestResult(types.T_time.ToType(), false, []types.Time{-max, max, -max}, []bool{false, false, false})
		caseSub := NewFunctionTestCase(proc, []FunctionTestInput{leftSub, rightSub}, wantSub, SubTime)
		ok, info = caseSub.RunAndFree()
		require.True(t, ok, info)

		require.Len(t, warnings.warnings, 2)
		for _, w := range warnings.warnings {
			require.Equal(t, moerr.ER_DATETIME_FUNCTION_OVERFLOW, w.code)
		}
	})

	t.Run("string operands time result", func(t *testing.T) {
		proc := newTmpProcess(t)
		warnings := &numericWarningSession{}
		proc.WarningSink = warnings

		left := NewFunctionTestInput(types.T_varchar.ToType(), []string{
			"00:00:01", "00:00:01", "-838:59:59", "-838:59:59", "10:00:00", "", "garbage",
		}, []bool{false, false, false, false, false, true, false})
		right := NewFunctionTestInput(types.T_varchar.ToType(), []string{
			"838:59:59", "839:00:00", "-00:00:01", "00:00:01", "10:00:00", "1:00:00", "1:00:00",
		}, nil)
		want := NewFunctionTestResult(types.T_time.ToType(), false, []types.Time{
			max, max, -max, -max + sec, types.TimeFromClock(false, 20, 0, 0, 0), 0, 0,
		}, []bool{false, false, false, false, false, true, true})
		caseDef := NewFunctionTestCase(proc, []FunctionTestInput{left, right}, want, AddTime)
		ok, info := caseDef.RunAndFree()
		require.True(t, ok, info)

		require.Len(t, warnings.warnings, 3)
		for _, w := range warnings.warnings {
			require.Equal(t, moerr.ER_DATETIME_FUNCTION_OVERFLOW, w.code)
		}

		proc2 := newTmpProcess(t)
		leftSub := NewFunctionTestInput(types.T_varchar.ToType(), []string{"-838:59:59", "00:00:01", "00:00:01"}, nil)
		rightSub := NewFunctionTestInput(types.T_varchar.ToType(), []string{"00:00:01", "-838:59:59", "838:59:59"}, nil)
		wantSub := NewFunctionTestResult(types.T_time.ToType(), false, []types.Time{-max, max, -max + sec}, []bool{false, false, false})
		caseSub := NewFunctionTestCase(proc2, []FunctionTestInput{leftSub, rightSub}, wantSub, SubTime)
		ok, info = caseSub.RunAndFree()
		require.True(t, ok, info)
	})

	t.Run("string operands varchar result", func(t *testing.T) {
		proc := newTmpProcess(t)
		left := NewFunctionTestInput(types.T_varchar.ToType(), []string{
			"00:00:01", "-838:59:59", "00:00:01.500000", "10:00:00", "bad",
		}, nil)
		right := NewFunctionTestInput(types.T_varchar.ToType(), []string{
			"839:00:00", "-00:00:01", "838:59:58.600000", "10:00:00", "1:00:00",
		}, nil)
		want := NewFunctionTestResult(types.New(types.T_varchar, 29, 6), false, []string{
			"838:59:59", "-838:59:59", "838:59:59.000000", "20:00:00", "",
		}, []bool{false, false, false, false, true})
		caseDef := NewFunctionTestCase(proc, []FunctionTestInput{left, right}, want, AddTime)
		ok, info := caseDef.RunAndFree()
		require.True(t, ok, info)

		leftSub := NewFunctionTestInput(types.T_varchar.ToType(), []string{"-838:59:59", "00:00:01"}, nil)
		rightSub := NewFunctionTestInput(types.T_varchar.ToType(), []string{"00:00:01", "-838:59:59"}, nil)
		wantSub := NewFunctionTestResult(types.New(types.T_varchar, 29, 6), false, []string{"-838:59:59", "838:59:59"}, []bool{false, false})
		caseSub := NewFunctionTestCase(proc, []FunctionTestInput{leftSub, rightSub}, wantSub, SubTime)
		ok, info = caseSub.RunAndFree()
		require.True(t, ok, info)
	})
}

// TIME() on an invalid string must yield a row-local NULL with a truncation
// warning instead of failing the whole statement with an out-of-range error.
func TestDateStringToTimeInvalidInputBecomesRowLocalNull(t *testing.T) {
	proc := newTmpProcess(t)
	warnings := &numericWarningSession{}
	proc.WarningSink = warnings

	input := NewFunctionTestInput(types.T_varchar.ToType(), []string{
		"25:00:00", "100:00:00", "838:59:59",
		"2024-01-15 12:34:56", "2024-01-15 25:00:00", "not-a-time", "839:00:00",
	}, nil)
	want := NewFunctionTestResult(types.T_time.ToTypeWithScale(6), false, []types.Time{
		types.TimeFromClock(false, 25, 0, 0, 0),
		types.TimeFromClock(false, 100, 0, 0, 0),
		types.MySQLTimeMax,
		types.TimeFromClock(false, 12, 34, 56, 0),
		0,
		0,
		types.MySQLTimeMax,
	}, []bool{false, false, false, false, true, true, false})
	caseDef := NewFunctionTestCase(proc, []FunctionTestInput{input}, want, DateStringToTime)
	ok, info := caseDef.RunAndFree()
	require.True(t, ok, info)

	// Only the two invalid rows and the over-boundary clamp warn.
	require.Len(t, warnings.warnings, 3)
	for _, w := range warnings.warnings {
		require.Equal(t, moerr.ER_TRUNCATED_WRONG_VALUE, w.code)
	}

	proc2 := newTmpProcess(t)
	inNull := NewFunctionTestInput(types.T_varchar.ToType(), []string{"25:00:00"}, []bool{true})
	wantNull := NewFunctionTestResult(types.T_time.ToTypeWithScale(6), false, []types.Time{0}, []bool{true})
	caseNull := NewFunctionTestCase(proc2, []FunctionTestInput{inNull}, wantNull, DateStringToTime)
	ok, info = caseNull.RunAndFree()
	require.True(t, ok, info)

	proc3 := newTmpProcess(t)
	inConst := NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"2024-01-15 25:00:00"}, nil)
	wantConst := NewFunctionTestResult(types.T_time.ToTypeWithScale(6), false, []types.Time{0}, []bool{true})
	caseConst := NewFunctionTestCase(proc3, []FunctionTestInput{inConst}, wantConst, DateStringToTime)
	ok, info = caseConst.RunAndFree()
	require.True(t, ok, info)
}
