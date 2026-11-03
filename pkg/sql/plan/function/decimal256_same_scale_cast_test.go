// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package function

import (
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

// TestCastDecimal256SameScaleCapacity covers the issue-#30938 capacity-cast
// kernel: a same-scale DECIMAL256->DECIMAL256 cast admits every coefficient
// ParseDecimal256's own width check admits (|v| < 10^width, limb comparison)
// and rejects exactly the coefficients the Format+Parse path rejects, with
// the identical checked error. Every expected value below is produced by the
// pre-existing Format+Parse channel, so the test pins behavior identity
// between the two paths row by row.
func TestCastDecimal256SameScaleCapacity(t *testing.T) {
	proc := testutil.NewProcess(t)
	var zero types.Decimal256

	parse := func(s string, w, sc int32) types.Decimal256 {
		v, err := types.ParseDecimal256(s, w, sc)
		require.NoError(t, err)
		return v
	}
	repeat := func(d string, n int) string { return strings.Repeat(d, n) }

	t.Run("integer_domain_boundaries", func(t *testing.T) {
		fromType := types.New(types.T_decimal256, 76, 0)
		toType := types.New(types.T_decimal256, 65, 0)
		// 10^64 (65 digits) fits; 10^65-1 is the largest fitting coefficient;
		// their negatives fit symmetrically; a NULL row passes through.
		fit64 := parse("1"+repeat("0", 64), 76, 0)
		maxFit := parse(repeat("9", 65), 76, 0)
		negFit64 := parse("-1"+repeat("0", 64), 76, 0)
		negMaxFit := parse("-"+repeat("9", 65), 76, 0)
		tc := NewFunctionTestCase(
			proc,
			[]FunctionTestInput{
				NewFunctionTestInput(fromType,
					[]types.Decimal256{fit64, maxFit, negFit64, negMaxFit, zero, zero},
					[]bool{false, false, false, false, false, true}),
				NewFunctionTestInput(toType, []types.Decimal256{}, nil),
			},
			NewFunctionTestResult(toType, false,
				[]types.Decimal256{fit64, maxFit, negFit64, negMaxFit, zero, zero},
				[]bool{false, false, false, false, false, true}),
			NewCast,
		)
		ok, info := tc.RunAndFree()
		require.True(t, ok, info)
	})

	t.Run("integer_domain_rejections", func(t *testing.T) {
		fromType := types.New(types.T_decimal256, 76, 0)
		toType := types.New(types.T_decimal256, 65, 0)
		// 10^65 and -10^65 are the first rejected magnitudes (66 digits);
		// the issue's 76-digit product is rejected by the same check.
		issue76 := parse("9999999999999999999999999999999999999800000000000000000000000000000000000001", 76, 0)
		pow65 := parse("1"+repeat("0", 65), 76, 0)
		negPow65 := parse("-1"+repeat("0", 65), 76, 0)
		for name, v := range map[string]types.Decimal256{
			"pow65":     pow65,
			"neg_pow65": negPow65,
			"issue_76":  issue76,
		} {
			t.Run(name, func(t *testing.T) {
				tc := NewFunctionTestCase(
					proc,
					[]FunctionTestInput{
						NewFunctionTestInput(fromType, []types.Decimal256{v}, []bool{false}),
						NewFunctionTestInput(toType, []types.Decimal256{}, nil),
					},
					NewFunctionTestResult(toType, true, nil, nil),
					NewCast,
				)
				ok, info := tc.RunAndFree()
				require.True(t, ok, info)
			})
		}
	})

	t.Run("scaled_same_scale", func(t *testing.T) {
		fromType := types.New(types.T_decimal256, 76, 30)
		toType := types.New(types.T_decimal256, 65, 30)
		// Coefficients of 65 digits fit; 66-digit coefficients reject. The
		// rendered value keeps the full scale on both paths.
		fit := parse("12345678901234567890123456789012345."+repeat("9", 30), 76, 30)
		fitNeg := parse("-12345678901234567890123456789012345."+repeat("9", 30), 76, 30)
		tcFit := NewFunctionTestCase(
			proc,
			[]FunctionTestInput{
				NewFunctionTestInput(fromType, []types.Decimal256{fit, fitNeg}, []bool{false, false}),
				NewFunctionTestInput(toType, []types.Decimal256{}, nil),
			},
			NewFunctionTestResult(toType, false, []types.Decimal256{fit, fitNeg}, []bool{false, false}),
			NewCast,
		)
		ok, info := tcFit.RunAndFree()
		require.True(t, ok, info)

		reject := parse("123456789012345678901234567890123456."+repeat("9", 30), 76, 30)
		tcRej := NewFunctionTestCase(
			proc,
			[]FunctionTestInput{
				NewFunctionTestInput(fromType, []types.Decimal256{reject}, []bool{false}),
				NewFunctionTestInput(toType, []types.Decimal256{}, nil),
			},
			NewFunctionTestResult(toType, true, nil, nil),
			NewCast,
		)
		ok, info = tcRej.RunAndFree()
		require.True(t, ok, info)
	})

	t.Run("narrow_target", func(t *testing.T) {
		fromType := types.New(types.T_decimal256, 76, 0)
		toType := types.New(types.T_decimal256, 20, 0)
		fit := parse(repeat("9", 20), 76, 0)
		tcFit := NewFunctionTestCase(
			proc,
			[]FunctionTestInput{
				NewFunctionTestInput(fromType, []types.Decimal256{fit}, []bool{false}),
				NewFunctionTestInput(toType, []types.Decimal256{}, nil),
			},
			NewFunctionTestResult(toType, false, []types.Decimal256{fit}, []bool{false}),
			NewCast,
		)
		ok, info := tcFit.RunAndFree()
		require.True(t, ok, info)

		reject := parse("1"+repeat("0", 20), 76, 0)
		tcRej := NewFunctionTestCase(
			proc,
			[]FunctionTestInput{
				NewFunctionTestInput(fromType, []types.Decimal256{reject}, []bool{false}),
				NewFunctionTestInput(toType, []types.Decimal256{}, nil),
			},
			NewFunctionTestResult(toType, true, nil, nil),
			NewCast,
		)
		ok, info = tcRej.RunAndFree()
		require.True(t, ok, info)
	})

	t.Run("carrier_width_ceiling", func(t *testing.T) {
		// (76,0)->(76,0) same-scale: the 76-digit maximum coefficient fits,
		// mirroring ParseDecimal256's width cap at the 256-bit carrier.
		fromType := types.New(types.T_decimal256, 76, 0)
		toType := types.New(types.T_decimal256, 76, 0)
		maxCoef := parse(repeat("9", 76), 76, 0)
		negMaxCoef := parse("-"+repeat("9", 76), 76, 0)
		tc := NewFunctionTestCase(
			proc,
			[]FunctionTestInput{
				NewFunctionTestInput(fromType, []types.Decimal256{maxCoef, negMaxCoef}, []bool{false, false}),
				NewFunctionTestInput(toType, []types.Decimal256{}, nil),
			},
			NewFunctionTestResult(toType, false, []types.Decimal256{maxCoef, negMaxCoef}, []bool{false, false}),
			NewCast,
		)
		ok, info := tc.RunAndFree()
		require.True(t, ok, info)
	})

	t.Run("non_same_scale_controls_unchanged", func(t *testing.T) {
		// scaleDown keeps the scale-then-reparse path with its rounding.
		fromDown := types.New(types.T_decimal256, 76, 10)
		toDown := types.New(types.T_decimal256, 65, 5)
		down := parse("12345678901234567890.1234567890", 76, 10)
		downWant, err := types.ParseDecimal256("12345678901234567890.12346", 65, 5)
		require.NoError(t, err)
		tcDown := NewFunctionTestCase(
			proc,
			[]FunctionTestInput{
				NewFunctionTestInput(fromDown, []types.Decimal256{down}, []bool{false}),
				NewFunctionTestInput(toDown, []types.Decimal256{}, nil),
			},
			NewFunctionTestResult(toDown, false, []types.Decimal256{downWant}, []bool{false}),
			NewCast,
		)
		ok, info := tcDown.RunAndFree()
		require.True(t, ok, info)

		// Unsafe scale growth (integer capacity shrinks) keeps the reparse
		// path: the coefficient is rescaled and re-checked, not limb-admitted.
		fromGrow := types.New(types.T_decimal256, 65, 0)
		toGrow := types.New(types.T_decimal256, 65, 5)
		grow := parse("12345678901234567890", 65, 0)
		growWant, err := types.ParseDecimal256("12345678901234567890.00000", 65, 5)
		require.NoError(t, err)
		tcGrow := NewFunctionTestCase(
			proc,
			[]FunctionTestInput{
				NewFunctionTestInput(fromGrow, []types.Decimal256{grow}, []bool{false}),
				NewFunctionTestInput(toGrow, []types.Decimal256{}, nil),
			},
			NewFunctionTestResult(toGrow, false, []types.Decimal256{growWant}, []bool{false}),
			NewCast,
		)
		ok, info = tcGrow.RunAndFree()
		require.True(t, ok, info)
	})

	t.Run("constant_vector", func(t *testing.T) {
		// A const vector carries one row through the same branch; the fitting
		// constant is returned coefficient-identically.
		fromType := types.New(types.T_decimal256, 76, 0)
		toType := types.New(types.T_decimal256, 65, 0)
		v := parse(repeat("9", 65), 76, 0)
		tc := NewFunctionTestCase(
			proc,
			[]FunctionTestInput{
				NewFunctionTestInput(fromType, []types.Decimal256{v}, []bool{false}),
				NewFunctionTestInput(toType, []types.Decimal256{}, nil),
			},
			NewFunctionTestResult(toType, false, []types.Decimal256{v}, []bool{false}),
			NewCast,
		)
		ok, info := tc.RunAndFree()
		require.True(t, ok, info)
	})
}
