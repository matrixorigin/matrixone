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
	"strconv"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

// Issue 31154: ROUND/TRUNCATE on finite DOUBLE values near the boundary of
// the float64 range must keep exact decimal rounding semantics at extreme
// decimal positions. The blanket digits<=-308 shortcut cut scale -308 to
// zero; math.Pow(10,k) carries a few-ULP deviation that moved the quotient
// across rounding boundaries near scale -300..-307; and the positive-scale
// intermediate multiplication x*10^digits manufactured +/-Inf. Expected
// values are the exact decimal roundings of the operands' exact binary
// values, i.e. the reference-engine behavior.
func TestRoundTruncateFloat64ExtremeScales(t *testing.T) {
	cases := []struct {
		name   string
		fn     func(float64, int64) (float64, error)
		value  float64
		digits int64
		want   float64
	}{
		// Reference repro matrix (issue 31154, MySQL 8.3 oracle).
		{"truncate(1e308,-308)", truncateFloat64, 1e308, -308, 1e308},
		{"truncate(-1e308,-308)", truncateFloat64, -1e308, -308, -1e308},
		{"truncate(1e308,-309) coarser is zero", truncateFloat64, 1e308, -309, 0},
		{"truncate(1e200,-308) smaller is zero", truncateFloat64, 1e200, -308, 0},
		{"round(1e308,-308)", roundFloat64, 1e308, -308, 1e308},
		{"round(-1e308,-308)", roundFloat64, -1e308, -308, -1e308},
		{"round(1e308,-309) coarser is zero", roundFloat64, 1e308, -309, 0},
		{"round(1e200,-308) smaller is zero", roundFloat64, 1e200, -308, 0},
		{"truncate(1e308,-307)", truncateFloat64, 1e308, -307, 1e308},
		{"truncate(-1e308,-307)", truncateFloat64, -1e308, -307, -1e308},
		{"round(1e308,-307)", roundFloat64, 1e308, -307, 1e308},
		{"round(-1e308,-307)", roundFloat64, -1e308, -307, -1e308},
		// Positive-scale intermediate x*10^digits must never make Inf.
		{"truncate(1e308,10)", truncateFloat64, 1e308, 10, 1e308},
		{"truncate(-1e308,10)", truncateFloat64, -1e308, 10, -1e308},
		{"round(1e308,10)", roundFloat64, 1e308, 10, 1e308},
		{"round(-1e308,10)", roundFloat64, -1e308, 10, -1e308},
		{"truncate(1e308,100)", truncateFloat64, 1e308, 100, 1e308},
		{"round(math.MaxFloat64,10)", roundFloat64, math.MaxFloat64, 10, math.MaxFloat64},
		// Exact-decimal contract at the boundary: fl(5e307) sits just above
		// the 0.5 rounding boundary, so exact semantics rounds away from zero
		// (no tie), and fl(9.9999e307) truncates to 9*10^307.
		{"round(5e307,-308)", roundFloat64, 5e307, -308, 1e308},
		{"truncate(9.9999e307,-307)", truncateFloat64, 9.9999e307, -307, 0x1.005419221015dp+1023},
		// TRUNCATE can never amplify the input: coarser multipliers of the
		// largest double still round to a finite multiple.
		{"truncate(math.MaxFloat64,-308)", truncateFloat64, math.MaxFloat64, -308, 1e308},
		{"truncate(math.MaxFloat64,-307)", truncateFloat64, math.MaxFloat64, -307, 1.7e308},
		// Ordinary controls: unchanged behavior at common scales.
		{"round(123.456,-1)", roundFloat64, 123.456, -1, 120},
		{"round(135,-1) ties to even", roundFloat64, 135, -1, 140},
		{"round(125,-1) ties to even", roundFloat64, 125, -1, 120},
		{"truncate(127,-1)", truncateFloat64, 127, -1, 120},
		{"truncate(-127,-1)", truncateFloat64, -127, -1, -120},
		{"round(2.5,0) round to even", roundFloat64, 2.5, 0, 2},
		{"round(3.5,0) round to even", roundFloat64, 3.5, 0, 4},
		{"truncate(-2.5,0)", truncateFloat64, -2.5, 0, -2},
		{"round(1e308,0)", roundFloat64, 1e308, 0, 1e308},
		{"round(1e300,10) finite fast path", roundFloat64, 1e300, 10, 1e300},
		{"round(0.3,308) huge digits identity", roundFloat64, 0.3, 308, 0.3},
		{"truncate(0.3,308) huge digits identity", truncateFloat64, 0.3, 308, 0.3},
		{"round(-0.5,-309) far coarser negative zero", roundFloat64, -0.5, -309, 0},
		{"truncate(1e15,-15)", truncateFloat64, 1e15, -15, 1e15},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.fn(tc.value, tc.digits)
			require.NoError(t, err)
			require.Equal(t, tc.want, got, "%s: bits %x, want bits %x", tc.name, math.Float64bits(got), math.Float64bits(tc.want))
		})
	}
}

// The -307 misround measured in issue 31154: divided back by 1e307, the
// TRUNCATE result must be exactly 10, not the 9.000000000000005 produced by
// the few-ULP-high math.Pow(10,307) scale.
func TestTruncateFloat64ExtremeScaleRatio(t *testing.T) {
	tv, tErr := truncateFloat64(1e308, -307)
	require.NoError(t, tErr)
	require.Equal(t, 10.0, tv/1e307)
	rv, rErr := roundFloat64(1e308, -307)
	require.NoError(t, rErr)
	require.Equal(t, 10.0, rv/1e307)
}

// Rounding that pushes a finite DOUBLE out of range (for example
// ROUND(1.8e308, -308) requiring 2e308) raises ER_DATA_OUT_OF_RANGE, like the
// other overflow-checked math functions, instead of returning +Inf. The
// error is returned through the regular op error channel: no panic, so no
// build-machine source paths can leak into the client-facing error text
// (issue 31154). TRUNCATE can never amplify, so the same inputs stay finite.
func TestRoundFloat64ExtremeScaleOverflow(t *testing.T) {
	for _, tc := range []struct {
		name   string
		value  float64
		digits int64
	}{
		{"half-up beyond max double", 1.5e308, -308},
		{"max double at -308", math.MaxFloat64, -308},
		{"round-up at -307", math.MaxFloat64, -307},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Direct production scalar callback (locks the error origin). The
			// public wrapper error channel (generalMathMultiWithError ->
			// op*WithErrorCheck) is separately locked by the
			// TestRoundFloat64Wrapper* tests below.
			_, rErr := roundFloat64(tc.value, tc.digits)
			require.NotNil(t, rErr, "expected ER_DATA_OUT_OF_RANGE, got success")
			require.True(t, moerr.IsMoErrCode(rErr, moerr.ErrOutOfRange))
		})
	}
	tv1, tErr1 := truncateFloat64(1.5e308, -308)
	require.NoError(t, tErr1)
	require.Equal(t, 1e308, tv1)
	tv2, tErr2 := truncateFloat64(math.MaxFloat64, -308)
	require.NoError(t, tErr2)
	require.Equal(t, 1e308, tv2)
}

// Column-vector path with a const digits argument (opUnaryFixedToFixed):
// the scalar scale -308 fix must hold for every row.
func TestTruncateFloat64VectorExtremeScales(t *testing.T) {
	proc := newMemoryFunctionTestProcess(t)
	t.Cleanup(func() { proc.Free(); require.Zero(t, proc.Mp().CurrNB()) })

	values := NewFunctionTestInput(types.T_float64.ToType(), []float64{1e308, -1e308, 1e200}, nil)
	digits := NewFunctionTestConstInput(types.T_int64.ToType(), []int64{-308}, nil)
	expected := NewFunctionTestResult(types.T_float64.ToType(), false, []float64{1e308, -1e308, 0}, nil)
	tc := NewFunctionTestCase(proc, []FunctionTestInput{values, digits}, expected, TruncateFloat64)
	defer tc.Free()
	ok, info := tc.Run()
	require.True(t, ok, info)
}

// Column-vector path with a per-row digits argument
// (opBinaryFixedFixedToFixed): the fix must not depend on const folding.
func TestRoundFloat64VectorDynamicDigitsExtremeScales(t *testing.T) {
	proc := newMemoryFunctionTestProcess(t)
	t.Cleanup(func() { proc.Free(); require.Zero(t, proc.Mp().CurrNB()) })

	values := NewFunctionTestInput(types.T_float64.ToType(), []float64{1e308, 1e308, 1e308}, nil)
	digits := NewFunctionTestInput(types.T_int64.ToType(), []int64{-308, -307, 10}, nil)
	expected := NewFunctionTestResult(types.T_float64.ToType(), false, []float64{1e308, 1e308, 1e308}, nil)
	tc := NewFunctionTestCase(proc, []FunctionTestInput{values, digits}, expected, RoundFloat64)
	defer tc.Free()
	tc.fnLength = tc.parameters[1].Length()
	tc.parameters[0].SetLength(tc.fnLength)
	ok, info := tc.Run()
	require.True(t, ok, info)
}

// Public wrapper error channel (const digits -> opUnaryFixedToFixedWithErrorCheck):
// the first out-of-range row aborts the call with ER_DATA_OUT_OF_RANGE (1690),
// which the SQL executor surfaces as a stable client error (no panic, no
// stack, no partial row output; BVT/SQL evidence in the issue batch).
func TestRoundFloat64WrapperConstDigitsErrorChannel(t *testing.T) {
	proc := newMemoryFunctionTestProcess(t)
	t.Cleanup(func() { proc.Free(); require.Zero(t, proc.Mp().CurrNB()) })

	values := NewFunctionTestInput(types.T_float64.ToType(), []float64{1e308, 1.5e308}, nil)
	digits := NewFunctionTestConstInput(types.T_int64.ToType(), []int64{-308}, nil)
	tcc := NewFunctionTestCase(proc, []FunctionTestInput{values, digits},
		NewFunctionTestResult(types.T_float64.ToType(), true, nil, nil), RoundFloat64)
	defer tcc.Free()
	require.NoError(t, tcc.result.PreExtendAndReset(2))
	err := tcc.fn(tcc.parameters, tcc.result, proc, 2, nil)
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrOutOfRange), "expected ER_DATA_OUT_OF_RANGE, got %v", err)
}

// Same contract through the per-row digits (non-const) binary op
// (opBinaryFixedFixedToFixedWithErrorCheck): rows are evaluated in order, so a
// success-then-overflow mix must abort with the same error rather than
// emitting the partial row set.
func TestRoundFloat64WrapperRowwiseDigitsErrorChannel(t *testing.T) {
	proc := newMemoryFunctionTestProcess(t)
	t.Cleanup(func() { proc.Free(); require.Zero(t, proc.Mp().CurrNB()) })

	values := NewFunctionTestInput(types.T_float64.ToType(), []float64{1e308, 1.5e308}, nil)
	digits := NewFunctionTestInput(types.T_int64.ToType(), []int64{-308, -308}, nil)
	tcc := NewFunctionTestCase(proc, []FunctionTestInput{values, digits},
		NewFunctionTestResult(types.T_float64.ToType(), true, nil, nil), RoundFloat64)
	defer tcc.Free()
	require.NoError(t, tcc.result.PreExtendAndReset(2))
	err := tcc.fn(tcc.parameters, tcc.result, proc, 2, nil)
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrOutOfRange), "expected ER_DATA_OUT_OF_RANGE, got %v", err)
}

// Rows masked out by selectList (CASE/IF short-circuit) must not be evaluated,
// so an out-of-range masked row cannot fail the call. The generic skip
// semantics of this op error channel are framework-covered by
// Test_DivFn_SelectListSkipsMaskedRows in arithmetic_overflow_boundary_test.go;
// this test locks them for the round/truncate registration.
func TestRoundFloat64WrapperMaskedOverflowRowSkipped(t *testing.T) {
	proc := newMemoryFunctionTestProcess(t)
	t.Cleanup(func() { proc.Free(); require.Zero(t, proc.Mp().CurrNB()) })

	maskRow1 := &FunctionSelectList{AnyNull: true, SelectList: []bool{true, false}}
	values := NewFunctionTestInput(types.T_float64.ToType(), []float64{1e308, 1.5e308}, nil)
	digits := NewFunctionTestConstInput(types.T_int64.ToType(), []int64{-308}, nil)
	tcc := NewFunctionTestCase(proc, []FunctionTestInput{values, digits},
		NewFunctionTestResult(types.T_float64.ToType(), false, nil, nil), RoundFloat64)
	defer tcc.Free()
	require.NoError(t, tcc.result.PreExtendAndReset(2))
	err := tcc.fn(tcc.parameters, tcc.result, proc, 2, maskRow1)
	require.NoError(t, err, "masked overflow row must not fail the call")
	rsVec := tcc.GetResultVectorDirectly()
	require.True(t, rsVec.GetNulls().Contains(1), "masked row must be NULL in the result")
	require.False(t, rsVec.GetNulls().Contains(0), "unmasked row must be computed")
	require.Equal(t, 1e308, vector.GetAny(rsVec, 0, false))
}

// A NULL input row is likewise never evaluated: the would-be-overflow value
// hidden by NULL must not fail the call.
func TestRoundFloat64WrapperNullRowSkipped(t *testing.T) {
	proc := newMemoryFunctionTestProcess(t)
	t.Cleanup(func() { proc.Free(); require.Zero(t, proc.Mp().CurrNB()) })

	values := NewFunctionTestInput(types.T_float64.ToType(), []float64{1e308, 1.5e308}, []bool{false, true})
	digits := NewFunctionTestConstInput(types.T_int64.ToType(), []int64{-308}, nil)
	tcc := NewFunctionTestCase(proc, []FunctionTestInput{values, digits},
		NewFunctionTestResult(types.T_float64.ToType(), false, nil, nil), RoundFloat64)
	defer tcc.Free()
	require.NoError(t, tcc.result.PreExtendAndReset(2))
	err := tcc.fn(tcc.parameters, tcc.result, proc, 2, nil)
	require.NoError(t, err, "NULL input row must not be evaluated")
	rsVec := tcc.GetResultVectorDirectly()
	require.True(t, rsVec.GetNulls().Contains(1))
	require.False(t, rsVec.GetNulls().Contains(0))
	require.Equal(t, 1e308, vector.GetAny(rsVec, 0, false))
}

// PR review 5668724880 (item 1): mid-magnitude negative digits must keep
// exact decimal semantics as well; forcing them into the exact path must
// still return the exact decimal result, never zero. 1e24 and 1e25 are
// exactly representable float64 values on the respective 10^k grids.
func TestTruncateFloat64MidGridSemantics(t *testing.T) {
	cases := []struct {
		name   string
		fn     func(float64, int64) (float64, error)
		value  float64
		digits int64
		want   float64
	}{
		{"truncate(1e24,-24) on grid", truncateFloat64, 1e24, -24, 1e24},
		{"truncate(-1e24,-24) on grid", truncateFloat64, -1e24, -24, -1e24},
		{"truncate(1.1e24,-24)", truncateFloat64, 1.1e24, -24, 1e24},
		{"truncate(-1.1e24,-24)", truncateFloat64, -1.1e24, -24, -1e24},
		{"round(1.24e24,-24)", roundFloat64, 1.24e24, -24, 1e24},
		{"round(1.25e25,-25) rounds down", roundFloat64, 1.25e25, -25, 1e25},
		{"round(1.75e25,-25) rounds up", roundFloat64, 1.75e25, -25, 2e25},
		{"round(-1.25e25,-25)", roundFloat64, -1.25e25, -25, -1e25},
		{"round(1e24,-24) on grid", roundFloat64, 1e24, -24, 1e24},
		// Repeated truncation must be stable: the inner result is on grid.
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.fn(tc.value, tc.digits)
			require.NoError(t, err)
			require.Equal(t, tc.want, got, "%s: bits %x, want bits %x", tc.name, math.Float64bits(got), math.Float64bits(tc.want))
		})
	}
	inner, err := truncateFloat64(1.1e24, -24)
	require.NoError(t, err)
	outer, err := truncateFloat64(inner, -24)
	require.NoError(t, err)
	require.Equal(t, 1e24, outer, "truncate(truncate(1.1e24,-24),-24)")
}

// Same mid-grid contract through the production vector wrapper with a const
// digits argument (opUnaryFixedToFixedWithErrorCheck lane).
func TestTruncateFloat64VectorMidGridConstDigits(t *testing.T) {
	proc := newMemoryFunctionTestProcess(t)
	t.Cleanup(func() { proc.Free(); require.Zero(t, proc.Mp().CurrNB()) })

	values := NewFunctionTestInput(types.T_float64.ToType(), []float64{1e24, -1e24, 1.1e24}, nil)
	digits := NewFunctionTestConstInput(types.T_int64.ToType(), []int64{-24}, nil)
	expected := NewFunctionTestResult(types.T_float64.ToType(), false, []float64{1e24, -1e24, 1e24}, nil)
	tc := NewFunctionTestCase(proc, []FunctionTestInput{values, digits}, expected, TruncateFloat64)
	defer tc.Free()
	ok, info := tc.Run()
	require.True(t, ok, info)
}

// Same mid-grid contract with a per-row (dynamic) digits column
// (opBinaryFixedFixedToFixedWithErrorCheck lane).
func TestTruncateFloat64VectorDynamicDigitsMidGrid(t *testing.T) {
	proc := newMemoryFunctionTestProcess(t)
	t.Cleanup(func() { proc.Free(); require.Zero(t, proc.Mp().CurrNB()) })

	values := NewFunctionTestInput(types.T_float64.ToType(), []float64{1e24, -1e24, 1.1e24}, nil)
	digits := NewFunctionTestInput(types.T_int64.ToType(), []int64{-24, -24, -24}, nil)
	expected := NewFunctionTestResult(types.T_float64.ToType(), false, []float64{1e24, -1e24, 1e24}, nil)
	tc := NewFunctionTestCase(proc, []FunctionTestInput{values, digits}, expected, TruncateFloat64)
	defer tc.Free()
	tc.fnLength = tc.parameters[1].Length()
	tc.parameters[0].SetLength(tc.fnLength)
	ok, info := tc.Run()
	require.True(t, ok, info)
}

// PR review 5668724880 (item 2): ordinary magnitudes must stay on the
// allocation-free double lane; only quotients within the ULP guard of a
// rounding decision boundary may pay the exact decimal path.
var roundTruncateAllocFreeSink float64

func TestRoundTruncateFloat64OrdinaryMagnitudeAllocFree(t *testing.T) {
	cases := []struct {
		value  float64
		digits int64
	}{
		{1e20, -1},          // review repro: on-grid identity magnitude
		{1.23456789e20, -1}, // off-grid ordinary magnitude
		{-8.76e19, -2},
		{3.14159e21, -3},
		// PR review 5670921927: the boundary-window rescale snap must likewise
		// stay allocation-free (it runs before the exact decimal delegate).
		{1.5e24, -23},
		{1.51e24, -23},
		{-1.5e24, -23},
	}
	got := testing.AllocsPerRun(1024, func() {
		for _, tc := range cases {
			v1, err1 := truncateFloat64(tc.value, tc.digits)
			if err1 != nil {
				t.Fatalf("truncate(%v,%v): %v", tc.value, tc.digits, err1)
			}
			roundTruncateAllocFreeSink += v1
			v2, err2 := roundFloat64(tc.value, tc.digits)
			if err2 != nil {
				t.Fatalf("round(%v,%v): %v", tc.value, tc.digits, err2)
			}
			roundTruncateAllocFreeSink += v2
		}
	})
	require.Equal(t, 0.0, got, "ordinary magnitudes must not allocate (PR review 5668724880 item 2)")
}

// PR review 5670921927 (item 1): the boundary-window quotient of a value that
// already sits on the intended decimal grid (for example 1.5e24 at digits -23,
// whose double quotient is 15.000000000000002) must snap back to the adjacent
// grid multiple that rescales to the input bitwise, instead of deciding from
// the ULP-noisy quotient and dropping to the neighboring multiple. Repeated
// truncation/rounding of an already-processed value must be idempotent.
// Expected values follow the exact decimal result converted to the nearest
// float64; TRUNCATE(1.49e24,-23) therefore reaches 14*fl(10^23), which is the
// double one ULP below fl(1.4e24).
func TestRoundTruncateFloat64GridSnapSemanticsR22(t *testing.T) {
	grid14 := 14 * pow10NearestFloatTable[23]
	require.Equal(t, 1.3999999999999998e24, grid14)
	cases := []struct {
		name   string
		fn     func(float64, int64) (float64, error)
		value  float64
		digits int64
		want   float64
	}{
		{"truncate(1e24*1.5,-23) on-grid repro", truncateFloat64, 1.5e24, -23, 1.5e24},
		{"truncate(-1e24*1.5,-23) on-grid repro", truncateFloat64, -1.5e24, -23, -1.5e24},
		{"truncate(1e24*1.6,-23) integral quotient", truncateFloat64, 1.6e24, -23, 1.6e24},
		{"truncate(-1e24*1.6,-23) integral quotient", truncateFloat64, -1.6e24, -23, -1.6e24},
		{"truncate(1e24*1.4,-23)", truncateFloat64, 1.4e24, -23, 1.4e24},
		{"truncate(-1e24*1.4,-23)", truncateFloat64, -1.4e24, -23, -1.4e24},
		{"truncate(1e24*1.49,-23)", truncateFloat64, 1.49e24, -23, grid14},
		{"truncate(-1e24*1.49,-23)", truncateFloat64, -1.49e24, -23, -grid14},
		{"truncate(1e24*1.51,-23)", truncateFloat64, 1.51e24, -23, 1.5e24},
		{"truncate(-1e24*1.51,-23)", truncateFloat64, -1.51e24, -23, -1.5e24},
		{"truncate(1e24*1.55,-23)", truncateFloat64, 1.55e24, -23, 1.5e24},
		{"truncate(-1e24*1.55,-23)", truncateFloat64, -1.55e24, -23, -1.5e24},
		{"truncate(1e24*1.65,-23)", truncateFloat64, 1.65e24, -23, 1.6e24},
		{"truncate(-1e24*1.65,-23)", truncateFloat64, -1.65e24, -23, -1.6e24},
		{"round(1e24*1.5,-23) on-grid", roundFloat64, 1.5e24, -23, 1.5e24},
		{"round(-1e24*1.5,-23) on-grid", roundFloat64, -1.5e24, -23, -1.5e24},
		// PR review 5673058412 re-canonicalization: 1.4e24 IS the float64
		// nearest to 14·10^23 (RN rep), so the near-grid snap now returns the
		// input bit-for-bit, matching the exact-decimal oracle and the
		// TRUNCATE result on the same operand instead of the old 1-ULP-away
		// lattice artifact 14*fl(10^23) (verified: q=14.00000000000002, lattice
		// 44f287626ee52197 != x=44f287626ee52198).
		{"round(1e24*1.4,-23) RN rep identity", roundFloat64, 1.4e24, -23, 1.4e24},
		{"round(-1e24*1.4,-23) RN rep identity", roundFloat64, -1.4e24, -23, -1.4e24},
		{"round(1e24*1.49,-23)", roundFloat64, 1.49e24, -23, 1.5e24},
		{"round(-1e24*1.49,-23)", roundFloat64, -1.49e24, -23, -1.5e24},
		{"round(1e24*1.55,-23)", roundFloat64, 1.55e24, -23, 1.6e24},
		{"round(-1e24*1.55,-23)", roundFloat64, -1.55e24, -23, -1.6e24},
		{"round(1e24*1.65,-23)", roundFloat64, 1.65e24, -23, 1.6e24},
		{"round(-1e24*1.65,-23)", roundFloat64, -1.65e24, -23, -1.6e24},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.fn(tc.value, tc.digits)
			require.NoError(t, err)
			require.Equal(t, tc.want, got, "%s: bits %x, want bits %x", tc.name, math.Float64bits(got), math.Float64bits(tc.want))

			// Idempotence: applying the same operation to the result must be
			// a bitwise no-op (PR review 5670921927 nested repro).
			again, err2 := tc.fn(got, tc.digits)
			require.NoError(t, err2)
			require.Equal(t, math.Float64bits(got), math.Float64bits(again),
				"%s must be idempotent: first %x, second %x", tc.name, math.Float64bits(got), math.Float64bits(again))
		})
	}
}

// PR review 5670921927 (item 2): near the finite DOUBLE limit the quotient's
// last-ULP rounding must not manufacture Inf: the true decimal rescale of
// math.MaxFloat64 at digits -200 is representable, so ROUND and TRUNCATE
// return the input on both sign mirrors instead of raising
// ER_DATA_OUT_OF_RANGE. The genuine overflow at digits -308
// (ROUND(1.7e308,-308) needs 2e308) must still fail.
func TestRoundTruncateFloat64FlMaxFiniteRescaleR22(t *testing.T) {
	for _, tc := range []struct {
		name  string
		fn    func(float64, int64) (float64, error)
		value float64
	}{
		{"truncate(max double,-200)", truncateFloat64, math.MaxFloat64},
		{"truncate(-max double,-200)", truncateFloat64, -math.MaxFloat64},
		{"round(max double,-200)", roundFloat64, math.MaxFloat64},
		{"round(-max double,-200)", roundFloat64, -math.MaxFloat64},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.fn(tc.value, -200)
			require.NoError(t, err)
			require.Equal(t, tc.value, got, "%s: bits %x, want %x", tc.name, math.Float64bits(got), math.Float64bits(tc.value))
		})
	}
	for _, x := range []float64{1.7e308, -1.7e308} {
		_, err := roundFloat64(x, -308)
		require.NotNil(t, err, "round(%v,-308) overflow through the exact-arbitration lane must still fail", x)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrOutOfRange))
	}
}

// Same grid-snap contract through the production vector wrapper with a const
// digits argument (opUnaryFixedToFixedWithErrorCheck lane), including the
// negative mirrors and the reviewer nested repro's inner fragment.
func TestTruncateFloat64VectorGridSnapConstDigitsR22(t *testing.T) {
	proc := newMemoryFunctionTestProcess(t)
	t.Cleanup(func() { proc.Free(); require.Zero(t, proc.Mp().CurrNB()) })

	values := NewFunctionTestInput(types.T_float64.ToType(),
		[]float64{1.5e24, -1.5e24, 1.51e24, -1.51e24, 1.49e24, -1.49e24, 1.6e24, -1.6e24}, nil)
	digits := NewFunctionTestConstInput(types.T_int64.ToType(), []int64{-23}, nil)
	grid14 := 14 * pow10NearestFloatTable[23]
	expected := NewFunctionTestResult(types.T_float64.ToType(), false,
		[]float64{1.5e24, -1.5e24, 1.5e24, -1.5e24, grid14, -grid14, 1.6e24, -1.6e24}, nil)
	tc := NewFunctionTestCase(proc, []FunctionTestInput{values, digits}, expected, TruncateFloat64)
	defer tc.Free()
	ok, info := tc.Run()
	require.True(t, ok, info)

	// Idempotence on the same vector lane: re-truncate the result. The
	// already-truncated rows must come back bitwise unchanged, so verifying
	// the twice-applied case equals the once-applied case locks stability.
	values2 := NewFunctionTestInput(types.T_float64.ToType(),
		[]float64{1.5e24, -1.5e24, 1.5e24, -1.5e24, grid14, -grid14, 1.6e24, -1.6e24}, nil)
	expected2 := NewFunctionTestResult(types.T_float64.ToType(), false,
		[]float64{1.5e24, -1.5e24, 1.5e24, -1.5e24, grid14, -grid14, 1.6e24, -1.6e24}, nil)
	tc2 := NewFunctionTestCase(proc, []FunctionTestInput{values2, digits}, expected2, TruncateFloat64)
	defer tc2.Free()
	ok2, info2 := tc2.Run()
	require.True(t, ok2, info2)
}

// Grid-snap + idempotence through the per-row (dynamic) digits column
// (opBinaryFixedFixedToFixedWithErrorCheck lane).
func TestTruncateFloat64VectorGridSnapDynamicDigitsR22(t *testing.T) {
	proc := newMemoryFunctionTestProcess(t)
	t.Cleanup(func() { proc.Free(); require.Zero(t, proc.Mp().CurrNB()) })

	values := NewFunctionTestInput(types.T_float64.ToType(),
		[]float64{1.5e24, -1.5e24, 1.51e24, -1.51e24}, nil)
	digits := NewFunctionTestInput(types.T_int64.ToType(), []int64{-23, -23, -23, -23}, nil)
	expected := NewFunctionTestResult(types.T_float64.ToType(), false,
		[]float64{1.5e24, -1.5e24, 1.5e24, -1.5e24}, nil)
	tc := NewFunctionTestCase(proc, []FunctionTestInput{values, digits}, expected, TruncateFloat64)
	defer tc.Free()
	tc.fnLength = tc.parameters[1].Length()
	tc.parameters[0].SetLength(tc.fnLength)
	ok, info := tc.Run()
	require.True(t, ok, info)
}

// The finite rescale at digits -200 near the finite DOUBLE limit through the
// vector wrapper (opUnaryFixedToFixedWithErrorCheck lane) must not raise
// ER_DATA_OUT_OF_RANGE for either function or either sign.
func TestRoundTruncateFloat64VectorFlMaxFiniteConstDigitsR22(t *testing.T) {
	proc := newMemoryFunctionTestProcess(t)
	t.Cleanup(func() { proc.Free(); require.Zero(t, proc.Mp().CurrNB()) })

	digits := NewFunctionTestConstInput(types.T_int64.ToType(), []int64{-200}, nil)
	in := NewFunctionTestInput(types.T_float64.ToType(), []float64{math.MaxFloat64, -math.MaxFloat64}, nil)
	expVal := NewFunctionTestResult(types.T_float64.ToType(), false, []float64{math.MaxFloat64, -math.MaxFloat64}, nil)
	tfc := NewFunctionTestCase(proc, []FunctionTestInput{in, digits}, expVal, TruncateFloat64)
	defer tfc.Free()
	ok, info := tfc.Run()
	require.True(t, ok, info)

	in2 := NewFunctionTestInput(types.T_float64.ToType(), []float64{math.MaxFloat64, -math.MaxFloat64}, nil)
	expVal2 := NewFunctionTestResult(types.T_float64.ToType(), false, []float64{math.MaxFloat64, -math.MaxFloat64}, nil)
	rfc := NewFunctionTestCase(proc, []FunctionTestInput{in2, digits}, expVal2, RoundFloat64)
	defer rfc.Free()
	ok2, info2 := rfc.Run()
	require.True(t, ok2, info2)
}

// ---------------------------------------------------------------------------
// PR review 5673058412: the near-grid snap must also recognize the exact
// fallback's representation. The delegate emits the float64 nearest to an
// exact decimal grid multiple (fl(1.4e26) for 14·10^25), which differs by one
// ULP from the double-domain lattice value n*fl(10^k) == 1.4000000000000002e26.
// Before this fix TRUNCATE(1.5e26,-25) routed to the exact delegate and
// returned 1.4e26, and each repeated truncation at unchanged digits dropped
// another full 1e25 step (1.5e26 -> 1.4e26 -> 1.3e26), for both signs and for
// const and dynamic digits. Bit constants below are from the recorded
// bit-level probe.
var (
	r48x15 = math.Float64frombits(0x455f04ef12cb04cf) // fl(1.5e26) == RN(15·10^25)
	r48x14 = math.Float64frombits(0x455cf389cd46047d) // fl(1.4e26) == RN(14·10^25)
	r48x13 = math.Float64frombits(0x455ae22487c1042b) // fl(1.3e26) == RN(13·10^25)
	// One-ULP neighbours of r48x14: the lower one's exact decimal value is
	// below 14·10^25, so genuine exact truncation selects 13·10^25; the upper
	// one IS the double-domain lattice value 14*fl(10^25), a fixed point of
	// the float lane.
	r48x14lo = math.Float64frombits(0x455cf389cd46047c)
	r48x14hi = math.Float64frombits(0x455cf389cd46047e)
	// m=17 at scale 23: the same decimal multiple 17·10^23 has lattice and
	// exact-delegate representations one ULP apart, and dividing the RN
	// representation by fl(10^23) lands exactly on the integral quotient, so
	// the on-grid lane must also recognize the exact-decimal form (the
	// idempotence sweep caught x -> RN rep -> lattice rep before the on-grid
	// lane was hardened).
	r48lat17 = math.Float64frombits(0x44f67fd2f45f5fa5) // 17*fl(10^23) = 1.6999999999999998e24
	r48rn17  = math.Float64frombits(0x44f67fd2f45f5fa6) // fl(17e23) == RN(17·10^23) = 1.7e24
	r48rn17u = math.Float64frombits(0x44f67fd2f45f5fa7) // RN(17·10^23) + 1 ULP
)

// Bit-exact scalar matrix: both representations of adjacent grid multiples
// are recognized as identity at digits -25, both signs, for RNG and TRUNC,
// and the genuinely off-grid one-ULP neighbours keep the exact boundary
// decision. Every row additionally asserts five nested applications never
// move the result (idempotence).
func TestRoundTruncateFloat64ExactGridSnapSemanticsR48(t *testing.T) {
	cases := []struct {
		name   string
		fn     func(float64, int64) (float64, error)
		value  float64
		digits int64
		want   float64
	}{
		{"truncate(RN(15e25),-25) identity", truncateFloat64, r48x15, -25, r48x15},
		{"truncate(-RN(15e25),-25) identity", truncateFloat64, -r48x15, -25, -r48x15},
		{"truncate(RN(14e25),-25) identity (nested repro input)", truncateFloat64, r48x14, -25, r48x14},
		{"truncate(-RN(14e25),-25) identity", truncateFloat64, -r48x14, -25, -r48x14},
		{"truncate(RN(13e25),-25) identity", truncateFloat64, r48x13, -25, r48x13},
		{"truncate(-RN(13e25),-25) identity", truncateFloat64, -r48x13, -25, -r48x13},
		{"truncate(14*fl(1e25) lattice,-25) identity", truncateFloat64, r48x14hi, -25, r48x14hi},
		{"truncate(-14*fl(1e25) lattice,-25) identity", truncateFloat64, -r48x14hi, -25, -r48x14hi},
		{"truncate(1ULP below RN(14e25),-25) genuine floor -> RN(13e25)", truncateFloat64, r48x14lo, -25, r48x13},
		{"truncate(-1ULP below RN(14e25),-25) genuine floor -> -RN(13e25)", truncateFloat64, -r48x14lo, -25, -r48x13},
		{"round(RN(15e25),-25) identity", roundFloat64, r48x15, -25, r48x15},
		{"round(-RN(15e25),-25) identity", roundFloat64, -r48x15, -25, -r48x15},
		{"round(RN(14e25),-25) identity", roundFloat64, r48x14, -25, r48x14},
		{"round(-RN(14e25),-25) identity", roundFloat64, -r48x14, -25, -r48x14},
		{"round(RN(13e25),-25) identity", roundFloat64, r48x13, -25, r48x13},
		{"round(-RN(13e25),-25) identity", roundFloat64, -r48x13, -25, -r48x13},
		{"round(14*fl(1e25) lattice,-25) identity", roundFloat64, r48x14hi, -25, r48x14hi},
		{"round(1ULP below RN(14e25),-25) -> lattice 14*fl(1e25)", roundFloat64, r48x14lo, -25, r48x14hi},
		{"round(-1ULP below RN(14e25),-25) -> lattice -14*fl(1e25)", roundFloat64, -r48x14lo, -25, -r48x14hi},
		{"truncate(17*fl(1e23) lattice,-23) identity", truncateFloat64, r48lat17, -23, r48lat17},
		{"truncate(-17*fl(1e23) lattice,-23) identity", truncateFloat64, -r48lat17, -23, -r48lat17},
		{"truncate(RN(17e23) integral-quotient,-23) identity", truncateFloat64, r48rn17, -23, r48rn17},
		{"truncate(-RN(17e23),-23) identity", truncateFloat64, -r48rn17, -23, -r48rn17},
		{"truncate(RN(17e23)+1ULP,-23) exact delegate -> RN(17e23)", truncateFloat64, r48rn17u, -23, r48rn17},
		{"truncate(-RN(17e23)+1ULP,-23) exact delegate -> -RN(17e23)", truncateFloat64, -r48rn17u, -23, -r48rn17},
		{"round(17*fl(1e23) lattice,-23) identity", roundFloat64, r48lat17, -23, r48lat17},
		{"round(-17*fl(1e23) lattice,-23) identity", roundFloat64, -r48lat17, -23, -r48lat17},
		{"round(RN(17e23),-23) identity", roundFloat64, r48rn17, -23, r48rn17},
		{"round(-RN(17e23),-23) identity", roundFloat64, -r48rn17, -23, -r48rn17},
		{"round(RN(17e23)+1ULP,-23) float lane -> lattice", roundFloat64, r48rn17u, -23, r48lat17},
		{"round(-RN(17e23)+1ULP,-23) float lane -> -lattice", roundFloat64, -r48rn17u, -23, -r48lat17},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.fn(tc.value, tc.digits)
			require.NoError(t, err)
			require.Equal(t, math.Float64bits(tc.want), math.Float64bits(got),
				"%s bits: got %x want %x", tc.name, math.Float64bits(got), math.Float64bits(tc.want))
			prev := got
			for i := 0; i < 5; i++ {
				next, err := tc.fn(prev, tc.digits)
				require.NoError(t, err)
				require.Equal(t, math.Float64bits(prev), math.Float64bits(next),
					"%s iteration %d moved: prev %x next %x", tc.name, i, math.Float64bits(prev), math.Float64bits(next))
			}
		})
	}
}

// Idempotence is a property, not a witness: sweep a deterministic grid of
// scales and multiples in both float representations (the double-domain
// lattice n*fl(10^k) and the exact-decimal RN(n*10^k)) plus one-ULP
// neighbours and both signs, and require F(F(x)) == F(x) bitwise for ROUND
// and TRUNCATE. The reference RN values use strconv's correctly rounded
// parsing, independent of the production bit path.
func TestRoundTruncateFloat64IdempotenceSweepR48(t *testing.T) {
	ks := []int64{16, 17, 21, 22, 23, 24, 25, 26, 27, 31, 47, 53, 64, 100, 101}
	ms := []int64{11, 13, 14, 15, 17, 19, 25, 49, 99}
	rnGrid := func(m, k int64) float64 {
		v, err := strconv.ParseFloat(strconv.FormatInt(m, 10)+"e"+strconv.FormatInt(k, 10), 64)
		if err != nil {
			t.Fatalf("sweep rn(%d,%d): %v", m, k, err)
		}
		return v
	}
	for _, k := range ks {
		for _, m := range ms {
			lattice := float64(m) * pow10NearestFloatTable[k]
			rn := rnGrid(m, k)
			for _, base := range []float64{rn, lattice, math.Nextafter(rn, math.Inf(1)), math.Nextafter(rn, math.Inf(-1))} {
				for _, s := range []float64{1, -1} {
					x := base * s
					t1, err := truncateFloat64(x, -k)
					require.NoError(t, err, "truncate(%v,-%d)", x, k)
					t2, err := truncateFloat64(t1, -k)
					require.NoError(t, err)
					require.Equal(t, math.Float64bits(t1), math.Float64bits(t2),
						"truncate idempotence: x=%v k=%d t1=%x t2=%x", x, k, math.Float64bits(t1), math.Float64bits(t2))
					r1, err := roundFloat64(x, -k)
					if err != nil {
						continue // genuine out-of-range; no fixed point to check
					}
					r2, err := roundFloat64(r1, -k)
					require.NoError(t, err, "round(%v,-%d)->%v then re-round errored", x, k, r1)
					require.Equal(t, math.Float64bits(r1), math.Float64bits(r2),
						"round idempotence: x=%v k=%d r1=%x r2=%x", x, k, math.Float64bits(r1), math.Float64bits(r2))
				}
			}
		}
	}
}

// Same snap contract through the production vector wrapper at digits -25:
// const digits, dynamic per-row digits, both signs, both grid
// representations, a one-ULP off-grid row, and nested applications on the
// first output vector (PR review 5673058412 asked for scalar/vector and
// const/dynamic nested regressions).
func TestTruncateFloat64VectorGridSnapR48(t *testing.T) {
	proc := newMemoryFunctionTestProcess(t)
	t.Cleanup(func() { proc.Free(); require.Zero(t, proc.Mp().CurrNB()) })

	vals := []float64{r48x15, -r48x15, r48x14, -r48x14, r48x14hi, r48x14lo, r48x13, -r48x13}
	exp := []float64{r48x15, -r48x15, r48x14, -r48x14, r48x14hi, r48x13, r48x13, -r48x13}

	constDigits := NewFunctionTestConstInput(types.T_int64.ToType(), []int64{-25}, nil)
	tc := NewFunctionTestCase(proc,
		[]FunctionTestInput{NewFunctionTestInput(types.T_float64.ToType(), vals, nil), constDigits},
		NewFunctionTestResult(types.T_float64.ToType(), false, exp, nil), TruncateFloat64)
	defer tc.Free()
	ok, info := tc.Run()
	require.True(t, ok, info)

	// Nested: the once-processed rows (RN and lattice representations mixed)
	// must be fixed points on the same const-digits lane.
	tcN := NewFunctionTestCase(proc,
		[]FunctionTestInput{NewFunctionTestInput(types.T_float64.ToType(), exp, nil), constDigits},
		NewFunctionTestResult(types.T_float64.ToType(), false, exp, nil), TruncateFloat64)
	defer tcN.Free()
	okN, infoN := tcN.Run()
	require.True(t, okN, infoN)

	// Dynamic per-row digits: the same -25 rows plus neighbouring scales are
	// all evaluated row-wise; each row's own grid scale decides.
	dynVals := []float64{r48x15, -r48x15, r48x14, r48x14lo, 1.5e24, -1.5e24, 1e20, -1e20}
	dynDig := []int64{-25, -25, -25, -25, -23, -23, -1, -1}
	dynExp := []float64{r48x15, -r48x15, r48x14, r48x13, 1.5e24, -1.5e24, 1e20, -1e20}
	tcD := NewFunctionTestCase(proc,
		[]FunctionTestInput{NewFunctionTestInput(types.T_float64.ToType(), dynVals, nil),
			NewFunctionTestInput(types.T_int64.ToType(), dynDig, nil)},
		NewFunctionTestResult(types.T_float64.ToType(), false, dynExp, nil), TruncateFloat64)
	defer tcD.Free()
	tcD.fnLength = tcD.parameters[1].Length()
	tcD.parameters[0].SetLength(tcD.fnLength)
	okD, infoD := tcD.Run()
	require.True(t, okD, infoD)
}

// ROUND through the vector wrapper at const digits -25: RN-representation
// grid values come back bit-identical (before this fix the float lane
// remapped fl(N·10^25) to the lattice value N*fl(10^25)); lattice values and
// the one-ULP off-grid row follow the float-domain lane exactly as before.
func TestRoundFloat64VectorGridSnapR48(t *testing.T) {
	proc := newMemoryFunctionTestProcess(t)
	t.Cleanup(func() { proc.Free(); require.Zero(t, proc.Mp().CurrNB()) })

	vals := []float64{r48x15, -r48x15, r48x14, -r48x14, r48x14hi, r48x14lo, r48x13}
	exp := []float64{r48x15, -r48x15, r48x14, -r48x14, r48x14hi, r48x14hi, r48x13}
	digits := NewFunctionTestConstInput(types.T_int64.ToType(), []int64{-25}, nil)
	tc := NewFunctionTestCase(proc,
		[]FunctionTestInput{NewFunctionTestInput(types.T_float64.ToType(), vals, nil), digits},
		NewFunctionTestResult(types.T_float64.ToType(), false, exp, nil), RoundFloat64)
	defer tc.Free()
	ok, info := tc.Run()
	require.True(t, ok, info)

	// Nested: all expected outputs are fixed points (lane-1 lattice values
	// pass through the on-grid lane; RN values through the exact-grid snap).
	tcN := NewFunctionTestCase(proc,
		[]FunctionTestInput{NewFunctionTestInput(types.T_float64.ToType(), exp, nil), digits},
		NewFunctionTestResult(types.T_float64.ToType(), false, exp, nil), RoundFloat64)
	defer tcN.Free()
	okN, infoN := tcN.Run()
	require.True(t, okN, infoN)
}
