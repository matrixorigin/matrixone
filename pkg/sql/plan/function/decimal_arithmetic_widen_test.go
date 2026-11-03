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
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestDecimal128ArithmeticWidensToDecimal256(t *testing.T) {
	for _, test := range []struct {
		name     string
		operator string
		input    types.Type
		want     types.Type
	}{
		{name: "add 38 digits", operator: "+", input: types.New(types.T_decimal128, 38, 0), want: types.New(types.T_decimal256, 39, 0)},
		{name: "subtract scaled 38 digits", operator: "-", input: types.New(types.T_decimal128, 38, 18), want: types.New(types.T_decimal256, 39, 18)},
		{name: "multiply 20 digits", operator: "*", input: types.New(types.T_decimal128, 20, 0), want: types.New(types.T_decimal256, 40, 0)},
	} {
		t.Run(test.name, func(t *testing.T) {
			resolved, err := GetFunctionByName(context.Background(), test.operator, []types.Type{test.input, test.input})
			require.NoError(t, err)
			require.True(t, resolved.needCast)
			require.Equal(t, []types.Type{
				types.New(types.T_decimal256, test.input.Width, test.input.Scale),
				types.New(types.T_decimal256, test.input.Width, test.input.Scale),
			}, resolved.targetTypes)
			require.Equal(t, test.want, resolved.retType)
		})
	}
}

func TestDecimal128ArithmeticKeepsExistingFastPathWhenResultFits(t *testing.T) {
	input := types.New(types.T_decimal128, 19, 0)
	resolved, err := GetFunctionByName(context.Background(), "*", []types.Type{input, input})
	require.NoError(t, err)
	require.NotEqual(t, types.T_decimal256, resolved.targetTypes[0].Oid)
	require.Equal(t, types.New(types.T_decimal128, 38, 0), resolved.retType)
}

func TestMixedDecimalArithmeticWidensFromOriginalDomains(t *testing.T) {
	decimal38 := types.New(types.T_decimal128, 38, 0)
	decimal20 := types.New(types.T_decimal128, 20, 0)
	decimal18 := types.New(types.T_decimal64, 18, 0)
	for _, test := range []struct {
		name       string
		operator   string
		inputs     []types.Type
		wantTarget []types.Type
		wantResult types.Type
	}{
		{
			name:       "decimal38 plus signed bigint",
			operator:   "+",
			inputs:     []types.Type{decimal38, types.T_int64.ToType()},
			wantTarget: []types.Type{types.New(types.T_decimal256, 38, 0), types.New(types.T_decimal256, 19, 0)},
			wantResult: types.New(types.T_decimal256, 39, 0),
		},
		{
			name:       "decimal38 minus unsigned bigint",
			operator:   "-",
			inputs:     []types.Type{decimal38, types.T_uint64.ToType()},
			wantTarget: []types.Type{types.New(types.T_decimal256, 38, 0), types.New(types.T_decimal256, 20, 0)},
			wantResult: types.New(types.T_decimal256, 39, 0),
		},
		{
			name:       "decimal38 times unsigned bigint",
			operator:   "*",
			inputs:     []types.Type{decimal38, types.T_uint64.ToType()},
			wantTarget: []types.Type{types.New(types.T_decimal256, 39, 0), types.New(types.T_decimal256, 20, 0)},
			wantResult: types.New(types.T_decimal256, 59, 0),
		},
		{
			name:       "decimal64 plus decimal128",
			operator:   "+",
			inputs:     []types.Type{decimal18, decimal38},
			wantTarget: []types.Type{types.New(types.T_decimal256, 19, 0), types.New(types.T_decimal256, 38, 0)},
			wantResult: types.New(types.T_decimal256, 39, 0),
		},
		{
			name:       "bit64 plus decimal38",
			operator:   "+",
			inputs:     []types.Type{types.T_bit.ToType(), decimal38},
			wantTarget: []types.Type{types.New(types.T_decimal256, 20, 0), types.New(types.T_decimal256, 38, 0)},
			wantResult: types.New(types.T_decimal256, 39, 0),
		},
		{
			name:       "decimal38 plus bit64",
			operator:   "+",
			inputs:     []types.Type{decimal38, types.T_bit.ToType()},
			wantTarget: []types.Type{types.New(types.T_decimal256, 38, 0), types.New(types.T_decimal256, 20, 0)},
			wantResult: types.New(types.T_decimal256, 39, 0),
		},
		{
			name:       "bit64 times decimal38",
			operator:   "*",
			inputs:     []types.Type{types.T_bit.ToType(), decimal38},
			wantTarget: []types.Type{types.New(types.T_decimal256, 20, 0), types.New(types.T_decimal256, 39, 0)},
			wantResult: types.New(types.T_decimal256, 59, 0),
		},
		{
			name:       "decimal38 times bit64",
			operator:   "*",
			inputs:     []types.Type{decimal38, types.T_bit.ToType()},
			wantTarget: []types.Type{types.New(types.T_decimal256, 39, 0), types.New(types.T_decimal256, 20, 0)},
			wantResult: types.New(types.T_decimal256, 59, 0),
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			resolved, err := GetFunctionByName(context.Background(), test.operator, test.inputs)
			require.NoError(t, err)
			require.True(t, resolved.needCast)
			require.Equal(t, test.wantTarget, resolved.targetTypes)
			require.Equal(t, test.wantResult, resolved.retType)
		})
	}

	resolved, err := GetFunctionByName(context.Background(), "*", []types.Type{decimal20, decimal18})
	require.NoError(t, err)
	// Preserve the released checked carrier when nominal precisions fit it;
	// subsequent MUL treats the precision-38 result as a carrier envelope.
	require.Equal(t, types.T_decimal128, resolved.targetTypes[0].Oid)
	require.Equal(t, types.New(types.T_decimal128, 38, 0), resolved.retType)
}

// Published precision is owned by the common arithmetic wrapper, independently
// of kernel dispatch. Inputs below are legal in their declared SQL domains.
func TestDecimal256ArithmeticPublishedPrecision(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	whole := "-57896044618658097711785492504343953926634992332820282019728792003"
	fraction := "-0.956564819968"
	max65 := strings.Repeat("9", 65)
	for _, tc := range []struct {
		name                            string
		fn                              executeLogicOfOverload
		leftType, rightType, resultType types.Type
		left, right, want               []string
		leftNulls, wantNulls            []bool
		leftConst, rightConst           bool
		selection                       *FunctionSelectList
		rawWant                         []types.Decimal256
		wantErr                         string
	}{
		{name: "multiply 65 digits", fn: multiFn,
			leftType: types.New(types.T_decimal256, 38, 0), rightType: types.New(types.T_decimal256, 27, 0), resultType: types.New(types.T_decimal256, 65, 0),
			left: []string{strings.Repeat("9", 38)}, right: []string{strings.Repeat("9", 27)}, want: []string{"99999999999999999999999999899999999999000000000000000000000000001"}},
		{name: "multiply overflow", fn: multiFn,
			leftType: types.New(types.T_decimal256, 38, 0), rightType: types.New(types.T_decimal256, 28, 0), resultType: types.New(types.T_decimal256, 65, 0),
			left: []string{strings.Repeat("9", 38)}, right: []string{strings.Repeat("9", 28)}, wantErr: "exceeds DECIMAL(65,0)"},
		{name: "negative boundary succeeds", fn: plusFn,
			leftType: types.New(types.T_decimal256, 65, 0), rightType: types.New(types.T_decimal256, 65, 0), resultType: types.New(types.T_decimal256, 65, 0),
			left: []string{"-" + max65}, right: []string{"0"}, want: []string{"-" + max65}},
		{name: "positive boundary overflows", fn: plusFn,
			leftType: types.New(types.T_decimal256, 65, 0), rightType: types.New(types.T_decimal256, 65, 0), resultType: types.New(types.T_decimal256, 65, 0),
			left: []string{max65}, right: []string{"1"}, wantErr: "exceeds DECIMAL(65,0)"},
		{name: "negative boundary overflows", fn: minusFn,
			leftType: types.New(types.T_decimal256, 65, 0), rightType: types.New(types.T_decimal256, 65, 0), resultType: types.New(types.T_decimal256, 65, 0),
			left: []string{"-" + max65}, right: []string{"1"}, wantErr: "exceeds DECIMAL(65,0)"},
		{name: "minimum vector vector", fn: plusFn,
			leftType: types.New(types.T_decimal256, 65, 0), rightType: types.New(types.T_decimal256, 65, 12), resultType: types.New(types.T_decimal256, 65, 12),
			left: []string{"0", whole}, right: []string{"0", fraction}, wantErr: "exceeds DECIMAL(65,12)"},
		{name: "minimum scalar vector", fn: plusFn, leftConst: true,
			leftType: types.New(types.T_decimal256, 65, 12), rightType: types.New(types.T_decimal256, 65, 0), resultType: types.New(types.T_decimal256, 65, 12),
			left: []string{fraction}, right: []string{"0", whole}, wantErr: "exceeds DECIMAL(65,12)"},
		{name: "minimum vector scalar", fn: minusFn, rightConst: true,
			leftType: types.New(types.T_decimal256, 65, 0), rightType: types.New(types.T_decimal256, 65, 12), resultType: types.New(types.T_decimal256, 65, 12),
			left: []string{"0", whole}, right: []string{"0.956564819968"}, wantErr: "exceeds DECIMAL(65,12)"},
		{name: "physical carrier preserves minimum", fn: plusFn,
			leftType: types.New(types.T_decimal256, 65, 0), rightType: types.New(types.T_decimal256, 65, 12), resultType: types.New(types.T_decimal256, 76, 12),
			left: []string{"0", whole}, right: []string{"0", fraction}, rawWant: []types.Decimal256{{}, {B192_255: uint64(1) << 63}}},
		{name: "minimum NULL row ignored", fn: plusFn,
			leftType: types.New(types.T_decimal256, 65, 0), rightType: types.New(types.T_decimal256, 65, 12), resultType: types.New(types.T_decimal256, 65, 12),
			left: []string{"1", whole}, right: []string{"0", fraction}, leftNulls: []bool{false, true}, want: []string{"1", "0"}, wantNulls: []bool{false, true}},
		{name: "minimum filtered row ignored", fn: plusFn,
			leftType: types.New(types.T_decimal256, 65, 0), rightType: types.New(types.T_decimal256, 65, 12), resultType: types.New(types.T_decimal256, 65, 12),
			left: []string{"1", whole}, right: []string{"0", fraction}, selection: &FunctionSelectList{AnyNull: true, SelectList: []bool{true, false}}, want: []string{"1", "0"}, wantNulls: []bool{false, true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			parse := func(values []string, typ types.Type) []types.Decimal256 {
				out := make([]types.Decimal256, len(values))
				for i, value := range values {
					var err error
					out[i], err = types.ParseDecimal256(value, typ.Width, typ.Scale)
					require.NoError(t, err)
				}
				return out
			}
			left := NewFunctionTestInput(tc.leftType, parse(tc.left, tc.leftType), tc.leftNulls)
			left.isConst = tc.leftConst
			right := NewFunctionTestInput(tc.rightType, parse(tc.right, tc.rightType), nil)
			right.isConst = tc.rightConst
			want := tc.rawWant
			if want == nil {
				want = parse(tc.want, tc.resultType)
			}
			fc := NewFunctionTestCase(proc, []FunctionTestInput{left, right},
				NewFunctionTestResult(tc.resultType, tc.wantErr != "", want, tc.wantNulls), tc.fn).WithSelectList(tc.selection)
			defer fc.Free()
			fc.fnLength = max(len(tc.left), len(tc.right))
			if tc.wantErr != "" {
				_, err := fc.DebugRun()
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrOutOfRange), "error: %v", err)
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			ok, info := fc.Run()
			require.True(t, ok, info)
			require.Equal(t, tc.resultType, *fc.GetResultVectorDirectly().GetType())
		})
	}
}

// TestDecimalMultiplicationPublishesPhysicalCarrierWidth covers issue #30938:
// multiplication resolves its expression result into the Decimal256 physical
// carrier domain (up to 76 digits) instead of clamping to the 65-digit DECIMAL
// declaration limit, matching MySQL's intermediate-result width. The declared
// storage domain stays at 65 digits and is enforced by checked casts; results
// whose declared precision fits below the carrier bound keep their existing
// inference.
func TestDecimalMultiplicationPublishesPhysicalCarrierWidth(t *testing.T) {
	for _, test := range []struct {
		name        string
		left, right types.Type
		wantCast    []types.Type // nil means the pair resolves without casts
		want        types.Type
	}{
		{
			// A width-38 DECIMAL128 operand counts 39 coefficient digits
			// (decimalMultiplyCoefficientWidth), so the coercion targets carry
			// the adjusted width and 39 + 39 = 78 saturates the 76-digit
			// carrier publication.
			name:     "38 digit square reaches 76",
			left:     types.New(types.T_decimal128, 38, 0),
			right:    types.New(types.T_decimal128, 38, 0),
			wantCast: []types.Type{types.New(types.T_decimal256, 39, 0), types.New(types.T_decimal256, 39, 0)},
			want:     types.New(types.T_decimal256, 76, 0),
		},
		{
			name:     "38 by 28 digits reaches 67",
			left:     types.New(types.T_decimal128, 38, 0),
			right:    types.New(types.T_decimal128, 28, 0),
			wantCast: []types.Type{types.New(types.T_decimal256, 39, 0), types.New(types.T_decimal256, 28, 0)},
			want:     types.New(types.T_decimal256, 67, 0),
		},
		{
			name:     "65 digit square saturates the carrier",
			left:     types.New(types.T_decimal256, 65, 0),
			right:    types.New(types.T_decimal256, 65, 0),
			wantCast: []types.Type{types.New(types.T_decimal256, 65, 0), types.New(types.T_decimal256, 65, 0)},
			want:     types.New(types.T_decimal256, 76, 0),
		},
		{
			// 39 + 27 = 66 exceeds the 65-digit declaration limit but fits
			// the physical carrier, so the pair widens past the declared
			// domain (storage admission stays a separate checked cast).
			name:     "38 by 27 digits widens beyond the declared domain",
			left:     types.New(types.T_decimal128, 38, 0),
			right:    types.New(types.T_decimal128, 27, 0),
			wantCast: []types.Type{types.New(types.T_decimal256, 39, 0), types.New(types.T_decimal256, 27, 0)},
			want:     types.New(types.T_decimal256, 66, 0),
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			resolved, err := GetFunctionByName(context.Background(), "*", []types.Type{test.left, test.right})
			require.NoError(t, err)
			if test.wantCast == nil {
				require.False(t, resolved.needCast)
			} else {
				require.True(t, resolved.needCast)
				require.Len(t, resolved.targetTypes, 2)
				require.Equal(t, test.wantCast[0], resolved.targetTypes[0])
				require.Equal(t, test.wantCast[1], resolved.targetTypes[1])
			}
			require.Equal(t, test.want, resolved.retType)
		})
	}
}

// TestDecimal256MultiplyPhysicalCarrierValues pins the exact values that the
// Decimal256 kernel and the published-precision gate must return for products
// beyond the 65-digit declaration limit (issue #30938). The result metadata is
// the physical carrier width, so checkDecimal256ResultPrecision defers to the
// kernel's own range check and the exact coefficient is returned.
func TestDecimal256MultiplyPhysicalCarrierValues(t *testing.T) {
	proc := testutil.NewProcess(t)
	nines38 := strings.Repeat("9", 38)
	cases := []struct {
		name                string
		leftType, rightType types.Type
		resultType          types.Type
		left, right         string
		want                string
	}{
		{
			name:       "issue 30938 38x38 literal pair",
			leftType:   types.New(types.T_decimal256, 38, 0),
			rightType:  types.New(types.T_decimal256, 38, 0),
			resultType: types.New(types.T_decimal256, 76, 0),
			left:       nines38,
			right:      nines38,
			want:       "9999999999999999999999999999999999999800000000000000000000000000000000000001",
		},
		{
			name:       "issue 30938 65 digit by 2",
			leftType:   types.New(types.T_decimal256, 65, 0),
			rightType:  types.New(types.T_decimal256, 19, 0),
			resultType: types.New(types.T_decimal256, 76, 0),
			left:       strings.Repeat("9", 65),
			right:      "2",
			want:       "199999999999999999999999999999999999999999999999999999999999999998",
		},
		{
			name:       "38x28 saturates 66",
			leftType:   types.New(types.T_decimal256, 38, 0),
			rightType:  types.New(types.T_decimal256, 28, 0),
			resultType: types.New(types.T_decimal256, 66, 0),
			left:       nines38,
			right:      strings.Repeat("9", 28),
			want:       "999999999999999999999999999899999999990000000000000000000000000001",
		},
		{
			name:       "negative 38 digit square",
			leftType:   types.New(types.T_decimal256, 38, 0),
			rightType:  types.New(types.T_decimal256, 38, 0),
			resultType: types.New(types.T_decimal256, 76, 0),
			left:       "-" + nines38,
			right:      nines38,
			want:       "-9999999999999999999999999999999999999800000000000000000000000000000000000001",
		},
		{
			name:       "scaled coefficients fill the carrier",
			leftType:   types.New(types.T_decimal256, 38, 2),
			rightType:  types.New(types.T_decimal256, 38, 2),
			resultType: types.New(types.T_decimal256, 76, 4),
			left:       "111111111111111111111111111111111111.11",
			right:      "111111111111111111111111111111111111.11",
			want:       "12345679012345679012345679012345679012098765432098765432098765432098765.4321",
		},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			left, parseErr := types.ParseDecimal256(test.left, test.leftType.Width, test.leftType.Scale)
			require.NoError(t, parseErr)
			right, parseErr := types.ParseDecimal256(test.right, test.rightType.Width, test.rightType.Scale)
			require.NoError(t, parseErr)
			expected, parseErr := types.ParseDecimal256(test.want, test.resultType.Width, test.resultType.Scale)
			require.NoError(t, parseErr)

			inputs := []FunctionTestInput{
				NewFunctionTestInput(test.leftType, []types.Decimal256{left, left}, nil),
				NewFunctionTestInput(test.rightType, []types.Decimal256{right, right}, nil),
			}
			result := NewFunctionTestResult(test.resultType, false, []types.Decimal256{expected, expected}, nil)
			testCase := NewFunctionTestCase(proc, inputs, result, multiFn)
			succeeded, info := testCase.RunAndFree()
			require.True(t, succeeded, info)
		})
	}
}

func TestDecimal256MultiplyPreservesWideSignedOperand(t *testing.T) {
	proc := testutil.NewProcess(t)
	leftType := types.New(types.T_decimal256, 19, 0)
	rightType := types.New(types.T_decimal256, 32, 16)
	resultType := types.New(types.T_decimal256, 51, 16)
	left, err := types.ParseDecimal256("1235467899687894561", leftType.Width, leftType.Scale)
	require.NoError(t, err)
	for _, test := range []struct {
		name     string
		right    string
		expected string
	}{
		{
			name:     "positive",
			right:    "2733892455124775.7851878942123454",
			expected: "3377636369505588272411432821735887.9920303797133694",
		},
		{
			name:     "negative",
			right:    "-2733892455124775.7851878942123454",
			expected: "-3377636369505588272411432821735887.9920303797133694",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			right, parseErr := types.ParseDecimal256(test.right, rightType.Width, rightType.Scale)
			require.NoError(t, parseErr)
			expected, parseErr := types.ParseDecimal256(test.expected, resultType.Width, resultType.Scale)
			require.NoError(t, parseErr)
			require.False(t, d256AllFitInt64([]types.Decimal256{right}, 1))
			require.False(t, d256AllFitInt32([]types.Decimal256{right}, 1))

			for _, input := range []struct {
				name  string
				right FunctionTestInput
			}{
				{
					name:  "literal scalar",
					right: NewFunctionTestConstInput(rightType, []types.Decimal256{right}, nil),
				},
				{
					name:  "typed column",
					right: NewFunctionTestInput(rightType, []types.Decimal256{right, right}, nil),
				},
			} {
				t.Run(input.name, func(t *testing.T) {
					testCase := NewFunctionTestCase(proc,
						[]FunctionTestInput{
							NewFunctionTestInput(leftType, []types.Decimal256{left, left}, nil),
							input.right,
						},
						NewFunctionTestResult(resultType, false, []types.Decimal256{expected, expected}, nil),
						multiFn)
					succeeded, info := testCase.RunAndFree()
					require.True(t, succeeded, info)
				})
			}
		})
	}
}

// inventoryRule names the section 4.1 rule (or the documented exclusion) a
// consumer's inventory membership relies on. The pinned sets below implement
// section 4.2 item 2's machine-checked exhaustiveness (revision 5
// F-CAPPED-CONSUMERS, revision 8 F-TRANSPARENT-CONSUMERS, revision 29
// F-MOD-STORAGE-ENVELOPE) against the live registry: they fail
// deterministically on any forgotten or newly added consumer.
type inventoryRule string

const (
	ruleMultiply     inventoryRule = "section 4.1 widened multiplication publication (the seam subject itself)"
	ruleModMirror    inventoryRule = "section 4.1 mod/% envelope mirror (revision 29: DecimalModReturnType rule-for-rule, maximum operand width and scale)"
	ruleTransparent  inventoryRule = "section 4.1 transparent scalar selector (verbatim copy or binder merge over the value-producing input types)"
	ruleValueArg     inventoryRule = "section 4.1 value-arg rule (non-value arguments — ordering keys, the NAME_CONST label — never produce result values)"
	ruleLagLead      inventoryRule = "section 4.1 lag/lead merge rule"
	ruleCast         inventoryRule = "section 4.1 cast declared-target rule (retType returns the target type)"
	ruleRoundMirror  inventoryRule = "section 4.1 round/truncate/floor/ceil envelope mirror (refineDecimalRoundingReturnType rule-for-rule)"
	ruleAddSubMirror inventoryRule = "section 4.1 add/subtract envelope mirror (decimalAddSubReturnType rule-for-rule)"
	ruleDivMirror    inventoryRule = "section 4.1 divide envelope mirror (decimalDivisionReturnType rule-for-rule)"
	ruleAvgAggregate inventoryRule = "section 5 envelope-bound aggregate argument (avgDecimal256ReturnType, width-sensitive at every width)"
	ruleSumAggregate inventoryRule = "section 5 envelope-bound aggregate argument (SumReturnType min(w+22,65), width-sensitive below argument width 43)"
	ruleValuesCopy   inventoryRule = "family T verbatim copy of the value-producing input (the VALUES overload retType returns parameters[0]); production binding is the ON DUPLICATE KEY UPDATE path (base_binder.go values dispatch) whose argument is a declared target-table column"
	rulePythonUdf    inventoryRule = "documented exclusion: the result is the UDF's declared return-type input (pythonUdfRetType returns the trailing parameter), a SQL declaration capped at 65 digits, so no >65 output is constructible; UDF semantics are outside this change's scope"
)

// inventoryCheckOne is the section 4.2 item 1 inventory: the exact set of
// consumers whose production-resolution result can exceed 65 digits for the
// synthesized carrier inputs (76,0)/(76,38), each naming its section 4.1
// rule. percentile_disc joins through its tailored (value, p) shape pinned in
// the test body; ifnull's SQL surface is the binder's case-when-isnull
// rewrite, also pinned in the body.
var inventoryCheckOne = map[string]inventoryRule{
	"*": ruleMultiply,

	"%":   ruleModMirror,
	"mod": ruleModMirror,

	"abs":         ruleTransparent,
	"unary_plus":  ruleTransparent,
	"unary_minus": ruleTransparent,
	"coalesce":    ruleTransparent,
	"case":        ruleTransparent,
	"if":          ruleTransparent,
	"iff":         ruleTransparent,
	"greatest":    ruleTransparent,
	"least":       ruleTransparent,
	"max":         ruleTransparent,
	"min":         ruleTransparent,
	"name_const":  ruleValueArg,

	"any_value":       ruleValueArg,
	"max_by":          ruleValueArg,
	"max_by_non_null": ruleValueArg,
	"first_value":     ruleValueArg,
	"last_value":      ruleValueArg,
	"nth_value":       ruleValueArg,

	"lag":  ruleLagLead,
	"lead": ruleLagLead,

	"cast":        ruleCast,
	"cast_assign": ruleCast,
	"cast_ignore": ruleCast,
	"cast_strict": ruleCast,

	"round":    ruleRoundMirror,
	"truncate": ruleRoundMirror,
	"floor":    ruleRoundMirror,
	"ceil":     ruleRoundMirror,
	"ceiling":  ruleRoundMirror,

	"values": ruleValuesCopy,

	"python_user_defined_function": rulePythonUdf,
}

// inventoryFamilyT is check two's transparent-propagation family: consumers
// whose registered result is a verbatim copy of, or a binder merge over, the
// types of their value-producing inputs, so propagation across the seam is
// sound without admission.
var inventoryFamilyT = map[string]inventoryRule{
	"abs":         ruleTransparent,
	"unary_plus":  ruleTransparent,
	"unary_minus": ruleTransparent,
	"coalesce":    ruleTransparent,
	"case":        ruleTransparent,
	"if":          ruleTransparent,
	"iff":         ruleTransparent,
	"greatest":    ruleTransparent,
	"least":       ruleTransparent,
	"max":         ruleTransparent,
	"min":         ruleTransparent,
	"name_const":  ruleValueArg,

	"any_value":       ruleValueArg,
	"max_by":          ruleValueArg,
	"max_by_non_null": ruleValueArg,
	"first_value":     ruleValueArg,
	"last_value":      ruleValueArg,
	"nth_value":       ruleValueArg,

	"lag":  ruleLagLead,
	"lead": ruleLagLead,

	"cast":        ruleCast,
	"cast_assign": ruleCast,
	"cast_ignore": ruleCast,
	"cast_strict": ruleCast,

	"values": ruleValuesCopy,
}

// inventoryFamilyD is check two's domain-arithmetic family: consumers that
// derive their result by a width/scale computing rule over the input types.
// sum joins through the below-width-43 pin in the test body because
// min(w+22, 65) saturates on section 4.2's seam pairs.
var inventoryFamilyD = map[string]inventoryRule{
	"%":   ruleModMirror,
	"mod": ruleModMirror,

	"+": ruleAddSubMirror,
	"-": ruleAddSubMirror,

	"/": ruleDivMirror,

	"avg": ruleAvgAggregate,

	"round":    ruleRoundMirror,
	"truncate": ruleRoundMirror,
	"floor":    ruleRoundMirror,
	"ceil":     ruleRoundMirror,
	"ceiling":  ruleRoundMirror,
}

func inventorySortedKeys[V any](set map[string]V) []string {
	keys := make([]string, 0, len(set))
	for key := range set {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// TestDecimalConsumerInventoryExhaustiveIssue30938 is section 4.2 item 2's
// machine-checked exhaustiveness proof. It walks every registered function
// name through the production resolution path (GetFunctionByNameWithoutError,
// the same resolution the binder uses, including the decimal metadata
// finalizers) and pins both checks against the live registry:
//
// Check one (section 4.2 item 1): for the synthesized carrier inputs (76,0)
// and (76,38), any resolution result wider than 65 must name a section 4.1
// rule; the observed wide set must equal the inventory exactly, so a
// forgotten or newly added consumer fails deterministically.
//
// Check two (revision 5, re-founded in revision 8): across section 4.2's
// seam pairs - widened products against their base publications, the
// mid-scale wrapper pairs (64,0)/(53,0) and (65,0)/(54,0), and the division
// pair (65,4)/(59,4) - every decimal consumer whose result type differs
// between a pair classifies into exactly one owned family: family T
// (transparent propagation) or family D (domain arithmetic), with the one
// documented exclusion justified by pinned registry facts.
func TestDecimalConsumerInventoryExhaustiveIssue30938(t *testing.T) {
	carriers := []types.Type{
		types.New(types.T_decimal256, 76, 0),
		types.New(types.T_decimal256, 76, 38),
	}
	seamPairs := [][2]types.Type{
		{types.New(types.T_decimal256, 76, 0), types.New(types.T_decimal256, 65, 0)},
		{types.New(types.T_decimal256, 66, 0), types.New(types.T_decimal256, 65, 0)},
		{types.New(types.T_decimal256, 76, 12), types.New(types.T_decimal256, 65, 12)},
		{types.New(types.T_decimal256, 70, 4), types.New(types.T_decimal256, 65, 4)},
		{types.New(types.T_decimal256, 64, 0), types.New(types.T_decimal256, 53, 0)},
		{types.New(types.T_decimal256, 65, 0), types.New(types.T_decimal256, 54, 0)},
		{types.New(types.T_decimal256, 65, 4), types.New(types.T_decimal256, 59, 4)},
	}
	vec := func(carrier types.Type, arity int) []types.Type {
		inputs := make([]types.Type, arity)
		for i := range inputs {
			inputs[i] = carrier
		}
		return inputs
	}
	resolve := func(name string, inputs []types.Type) (rt types.Type, ok bool, err error) {
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("panic: %v", r)
				rt, ok = types.Type{}, false
			}
		}()
		r, resolved := GetFunctionByNameWithoutError(name, inputs)
		if !resolved {
			return types.Type{}, false, nil
		}
		return r.retType, true, nil
	}

	wide := map[string]types.Type{}
	divergent := map[string][2]types.Type{}
	for name := range functionIdRegister {
		for _, carrier := range carriers {
			for arity := 1; arity <= 4; arity++ {
				rt, ok, err := resolve(name, vec(carrier, arity))
				require.NoErrorf(t, err, "check one resolution panicked for %s arity %d", name, arity)
				if ok && rt.Oid.IsDecimal() && rt.Width > 65 {
					wide[name] = rt
				}
			}
		}
		for _, pair := range seamPairs {
			for arity := 1; arity <= 4; arity++ {
				rtA, okA, errA := resolve(name, vec(pair[0], arity))
				rtB, okB, errB := resolve(name, vec(pair[1], arity))
				require.NoErrorf(t, errA, "check two resolution panicked for %s arity %d", name, arity)
				require.NoErrorf(t, errB, "check two resolution panicked for %s arity %d", name, arity)
				if okA && okB && rtA.Oid.IsDecimal() && rtB.Oid.IsDecimal() &&
					(rtA.Oid != rtB.Oid || rtA.Width != rtB.Width || rtA.Scale != rtB.Scale) {
					divergent[name] = [2]types.Type{rtA, rtB}
				}
			}
		}
	}

	// Check one: the observed wide set equals the inventory exactly, and
	// every member names its section 4.1 rule.
	require.Equal(t, inventorySortedKeys(inventoryCheckOne), inventorySortedKeys(wide),
		"check one: every >65 resolution-path result must name a section 4.1 rule and the inventory must be exact (section 4.2 items 1-2)")
	for _, name := range []string{"*", "mod", "%", "round", "coalesce", "max", "cast"} {
		require.Contains(t, wide, name, "non-vacuous pin: %s must stay >65-capable", name)
	}
	// The widened multiplication owner publishes >65 at every width, so it is
	// wide but cannot diverge across section 4.2's seam pairs on this tree;
	// its parent-side divergence is the issue's own red/green proof.
	require.NotContains(t, divergent, "*")

	// Check two: families T and D stay disjoint, and every divergent consumer
	// classifies into exactly one owned family or the documented exclusion.
	classified := map[string]inventoryRule{}
	for name, rule := range inventoryFamilyT {
		classified[name] = rule
	}
	for name, rule := range inventoryFamilyD {
		_, clash := inventoryFamilyT[name]
		require.False(t, clash, "families T and D must stay disjoint: %s", name)
		classified[name] = rule
	}
	classified["python_user_defined_function"] = rulePythonUdf
	require.Equal(t, inventorySortedKeys(classified), inventorySortedKeys(divergent),
		"check two: every divergent consumer must classify into family T, family D, or the documented exclusion (section 4.2 item 2)")
	for _, name := range []string{"+", "mod", "round", "avg", "coalesce", "max"} {
		require.Contains(t, divergent, name, "non-vacuous pin: %s must stay seam-divergent", name)
	}

	t.Run("percentile_disc tailored shape joins family T", func(t *testing.T) {
		// Ordered-set plan arguments are (order value, percentile p)
		// (ordered_set.go header); p must be numeric and never decimal256
		// (orderedSetPercentileCheckWithMode), which is why the uniform
		// same-type synthesis cannot resolve percentile_disc at all.
		p := types.T_float64.ToType()
		rt, ok, err := resolve("percentile_disc", []types.Type{types.New(types.T_decimal256, 76, 0), p})
		require.NoError(t, err)
		require.True(t, ok, "percentile_disc must resolve its tailored (value, p) shape")
		require.Equal(t, types.New(types.T_decimal256, 76, 0), rt,
			"percentile_disc returns args[0] verbatim (aggexec PercentileDiscReturnType), the section 4.1 percentile_disc rule")
		rtNarrow, ok, err := resolve("percentile_disc", []types.Type{types.New(types.T_decimal256, 65, 0), p})
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, types.New(types.T_decimal256, 65, 0), rtNarrow)
		_, ok, err = resolve("percentile_disc", []types.Type{types.New(types.T_decimal256, 76, 0), types.New(types.T_decimal256, 76, 0)})
		require.NoError(t, err)
		require.False(t, ok, "a decimal256 percentile argument is rejected by the ordered-set check")
	})

	t.Run("percentile_cont stays class (i) width-insensitive", func(t *testing.T) {
		p := types.T_float64.ToType()
		_, ok, err := resolve("percentile_cont", []types.Type{types.New(types.T_decimal256, 76, 0), p})
		require.NoError(t, err)
		require.False(t, ok, "percentile_cont rejects decimal256 and >=38-wide decimal values, so no carrier input resolves")
		rt, ok, err := resolve("percentile_cont", []types.Type{types.New(types.T_decimal128, 20, 10), p})
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, types.New(types.T_decimal128, 38, 11), rt,
			"percentile_cont returns decimal128(38, s+1), a pure function of scale (class (i))")
	})

	t.Run("sum joins family D below argument width 43", func(t *testing.T) {
		rt34, ok, err := resolve("sum", []types.Type{types.New(types.T_decimal128, 34, 0)})
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, types.New(types.T_decimal256, 56, 0), rt34)
		rt42, ok, err := resolve("sum", []types.Type{types.New(types.T_decimal128, 42, 0)})
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, types.New(types.T_decimal256, 64, 0), rt42,
			"SumReturnType min(w+22,65) is width-sensitive below argument width 43 (family D, section 5 subject)")
		// min(w+22, 65) saturates at 65 on section 4.2's seam pairs, so sum
		// is absent from the uniform-synthesis divergent set by construction.
		rtWide, ok, err := resolve("sum", []types.Type{types.New(types.T_decimal256, 76, 0)})
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, types.New(types.T_decimal256, 65, 0), rtWide)
		rtBase, ok, err := resolve("sum", []types.Type{types.New(types.T_decimal256, 65, 0)})
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, rtWide, rtBase)
	})

	t.Run("ifnull registry facts", func(t *testing.T) {
		// The registry maps the name ifnull to the ISNULL function id
		// (function_id.go), a one-argument null predicate returning bool;
		// SQL ifnull(a, b) is rewritten by the binder to
		// case when isnull(a) then b else null (base_binder.go), so the SQL
		// surface is covered by family T's case rule.
		_, ok, err := resolve("ifnull", []types.Type{types.New(types.T_decimal256, 76, 0), types.New(types.T_decimal256, 76, 0)})
		require.NoError(t, err)
		require.False(t, ok, "ifnull resolves to the one-argument ISNULL predicate id, never a two-argument decimal consumer")
		rt, ok, err := resolve("ifnull", []types.Type{types.New(types.T_decimal256, 76, 0)})
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, types.T_bool.ToType(), rt)
	})

	t.Run("values is a family T verbatim copy", func(t *testing.T) {
		rt, ok, err := resolve("values", []types.Type{types.New(types.T_decimal256, 76, 0)})
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, types.New(types.T_decimal256, 76, 0), rt,
			"VALUES returns parameters[0] verbatim, family T's defining property")
	})

	t.Run("python udf result is its declared return type input", func(t *testing.T) {
		rt, ok, err := resolve("python_user_defined_function", []types.Type{types.T_varchar.ToType(), types.New(types.T_decimal256, 65, 0)})
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, types.New(types.T_decimal256, 65, 0), rt,
			"the result is the trailing declared-return-type input, a SQL declaration capped at 65 digits; the all-carrier synthesis feeding a carrier as the return-type slot is not a production shape")
	})

	t.Run("dml cast variants follow the declared-target rule", func(t *testing.T) {
		// cast_strict is the upstream internal assignment conversion
		// (list_operator.go): like cast_assign/cast_ignore its result is the
		// declared target parameters[1], so it names the same section 4.1
		// cast rule.
		for _, name := range []string{"cast_assign", "cast_ignore", "cast_strict"} {
			rt, ok, err := resolve(name, []types.Type{types.New(types.T_decimal256, 76, 0), types.New(types.T_decimal256, 65, 1)})
			require.NoError(t, err)
			require.True(t, ok)
			require.Equal(t, types.New(types.T_decimal256, 65, 1), rt,
				"%s returns the declared target parameters[1], the section 4.1 cast rule", name)
		}
	})
}
