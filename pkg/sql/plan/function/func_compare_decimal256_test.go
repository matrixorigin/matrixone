// Copyright 2021 - 2022 Matrix Origin
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
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

// issue #24565: a CASE/IFF whose branches need more than 38 digits is promoted
// to decimal256; comparing such a value with a decimal128 must work instead of
// failing with "bad value [DECIMAL256 DECIMAL128]".

func Test_FixedTypeCastRule1_Decimal256Promotion(t *testing.T) {
	// decimal256 vs decimal128 -> both promoted to decimal256, each keeps scale/width.
	has, t1, t2 := fixedTypeCastRule1(
		types.New(types.T_decimal256, 58, 20),
		types.New(types.T_decimal128, 38, 20),
	)
	require.True(t, has)
	require.Equal(t, types.T_decimal256, t1.Oid)
	require.Equal(t, types.T_decimal256, t2.Oid)
	require.Equal(t, int32(20), t1.Scale)
	require.Equal(t, int32(58), t1.Width)
	require.Equal(t, int32(20), t2.Scale)
	require.Equal(t, int32(38), t2.Width)

	// decimal128 vs decimal256 (reversed) -> still both decimal256.
	has, t1, t2 = fixedTypeCastRule1(
		types.New(types.T_decimal128, 38, 2),
		types.New(types.T_decimal256, 58, 20),
	)
	require.True(t, has)
	require.Equal(t, types.T_decimal256, t1.Oid)
	require.Equal(t, types.T_decimal256, t2.Oid)

	// decimal256 vs decimal256 -> both decimal256.
	has, t1, t2 = fixedTypeCastRule1(
		types.New(types.T_decimal256, 58, 20),
		types.New(types.T_decimal256, 40, 5),
	)
	require.True(t, has)
	require.Equal(t, types.T_decimal256, t1.Oid)
	require.Equal(t, types.T_decimal256, t2.Oid)

	// decimal256 vs a non-decimal/non-integer type (varchar) must NOT be
	// handled by the promotion shortcut.
	has, _, _ = fixedTypeCastRule1(
		types.New(types.T_decimal256, 58, 20),
		types.T_varchar.ToType(),
	)
	require.False(t, has)
}

// Test_FixedTypeCastRule1_Decimal256VsInteger covers the decimal256-vs-integer
// promotion path: a bare integer literal compared with a decimal256 value must
// promote both sides to decimal256 so the comparison binds (issue #24565
// review). The integer side becomes decimal256(integralWidth, 0).
func Test_FixedTypeCastRule1_Decimal256VsInteger(t *testing.T) {
	has, t1, t2 := fixedTypeCastRule1(
		types.New(types.T_decimal256, 58, 20),
		types.T_int64.ToType(),
	)
	require.True(t, has)
	require.Equal(t, types.T_decimal256, t1.Oid)
	require.Equal(t, int32(20), t1.Scale)
	require.Equal(t, types.T_decimal256, t2.Oid)
	require.Equal(t, int32(0), t2.Scale)
	require.Equal(t, int32(19), t2.Width) // int64 integral width

	// reversed: integer on the left.
	has, t1, t2 = fixedTypeCastRule1(
		types.T_uint32.ToType(),
		types.New(types.T_decimal256, 40, 5),
	)
	require.True(t, has)
	require.Equal(t, types.T_decimal256, t1.Oid)
	require.Equal(t, int32(0), t1.Scale)
	require.Equal(t, int32(10), t1.Width) // uint32 integral width
	require.Equal(t, types.T_decimal256, t2.Oid)
	require.Equal(t, int32(5), t2.Scale)
}

// Test_IsNumericType_Decimal256 verifies decimal256 is recognised by the
// runtime type-mismatch fallback path (issue #24565 review).
func Test_IsNumericType_Decimal256(t *testing.T) {
	require.True(t, isNumericType(types.T_decimal256))
	require.True(t, isNumericType(types.T_decimal128))
	require.False(t, isNumericType(types.T_varchar))
}

// Test_DecimalHelpers covers the small decimal helper funcs used by the BETWEEN
// and comparison binding paths.
func Test_DecimalHelpers(t *testing.T) {
	require.True(t, isDecimalOrInteger(types.New(types.T_decimal128, 38, 2)))
	require.True(t, isDecimalOrInteger(types.T_int64.ToType()))
	require.False(t, isDecimalOrInteger(types.T_float64.ToType()))
	require.False(t, isDecimalOrInteger(types.T_varchar.ToType()))

	require.Equal(t, types.T_decimal64, widestDecimalFamily([]types.Type{
		types.New(types.T_decimal64, 18, 2), types.T_int64.ToType()}))
	require.Equal(t, types.T_decimal128, widestDecimalFamily([]types.Type{
		types.New(types.T_decimal64, 18, 2), types.New(types.T_decimal128, 38, 2)}))
	require.Equal(t, types.T_decimal256, widestDecimalFamily([]types.Type{
		types.New(types.T_decimal128, 38, 2), types.New(types.T_decimal256, 76, 2)}))

	// decimal256FromSource: decimal256 unchanged, decimal preserves width/scale,
	// integer becomes (integralWidth, 0).
	d256 := types.New(types.T_decimal256, 50, 10)
	require.Equal(t, d256, decimal256FromSource(d256))
	got := decimal256FromSource(types.New(types.T_decimal128, 38, 6))
	require.Equal(t, types.T_decimal256, got.Oid)
	require.Equal(t, int32(38), got.Width)
	require.Equal(t, int32(6), got.Scale)
	got = decimal256FromSource(types.T_int64.ToType())
	require.Equal(t, types.T_decimal256, got.Oid)
	require.Equal(t, int32(19), got.Width)
	require.Equal(t, int32(0), got.Scale)
}

// Test_BetweenCheckFn_Decimal256Promotion verifies the BETWEEN checkFn computes
// a common decimal type preserving max integral width + max scale and promotes
// to decimal256 when decimal128 would overflow (issue #24565 review). Example:
// DECIMAL(38,0) BETWEEN DECIMAL(38,30) AND DECIMAL(38,30) needs 38 integral + 30
// scale = 68 digits, i.e. DECIMAL256(68,30) (scale 30 is the 3.0-dev max).
func Test_BetweenCheckFn_Decimal256Promotion(t *testing.T) {
	var betweenFunc *FuncNew
	for i := range supportedOperators {
		if supportedOperators[i].functionId == BETWEEN {
			betweenFunc = &supportedOperators[i]
			break
		}
	}
	require.NotNil(t, betweenFunc, "BETWEEN operator should be defined")

	// DECIMAL(38,0) BETWEEN DECIMAL(38,30) AND DECIMAL(38,30) -> DECIMAL256(68,30).
	res := betweenFunc.checkFn(betweenFunc.Overloads, []types.Type{
		types.New(types.T_decimal128, 38, 0),
		types.New(types.T_decimal128, 38, 30),
		types.New(types.T_decimal128, 38, 30),
	})
	require.Equal(t, succeedWithCast, res.status)
	require.Len(t, res.finalType, 3)
	for _, ft := range res.finalType {
		require.Equal(t, types.T_decimal256, ft.Oid)
		require.Equal(t, int32(30), ft.Scale)
		require.Equal(t, int32(68), ft.Width)
	}

	// mixed family (dec64 value, dec128 bounds) must not be rejected; all three
	// aligned to a common decimal type.
	res = betweenFunc.checkFn(betweenFunc.Overloads, []types.Type{
		types.New(types.T_decimal64, 18, 2),
		types.New(types.T_decimal128, 38, 4),
		types.New(types.T_decimal128, 38, 4),
	})
	require.Equal(t, succeedWithCast, res.status)
	require.Len(t, res.finalType, 3)
	for _, ft := range res.finalType {
		require.True(t, ft.Oid.IsDecimal())
		require.Equal(t, res.finalType[0].Oid, ft.Oid)
		require.Equal(t, int32(4), ft.Scale)
	}

	// decimal value with integer bounds stays numeric and binds.
	res = betweenFunc.checkFn(betweenFunc.Overloads, []types.Type{
		types.New(types.T_decimal128, 20, 2),
		types.T_int64.ToType(),
		types.T_int64.ToType(),
	})
	require.Equal(t, succeedWithCast, res.status)
	require.Len(t, res.finalType, 3)
	for _, ft := range res.finalType {
		require.True(t, ft.Oid.IsDecimal())
	}
}

func Test_CompareOperatorSupports_Decimal256(t *testing.T) {
	d256 := types.New(types.T_decimal256, 58, 20)
	require.True(t, equalAndNotEqualOperatorSupports(d256, d256))
	require.True(t, otherCompareOperatorSupports(d256, d256))
}

func Test_EqualFn_Decimal256(t *testing.T) {
	proc := testutil.NewProcess(t)

	// equal, same scale
	tc := tcTemp{
		info: "decimal256 = decimal256 (same scale)",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(types.New(types.T_decimal256, 58, 0),
				[]types.Decimal256{types.Decimal256FromInt64(5), types.Decimal256FromInt64(6)},
				[]bool{false, false}),
			NewFunctionTestInput(types.New(types.T_decimal256, 58, 0),
				[]types.Decimal256{types.Decimal256FromInt64(5), types.Decimal256FromInt64(7)},
				[]bool{false, false}),
		},
		expect: NewFunctionTestResult(types.T_bool.ToType(), false,
			[]bool{true, false}, []bool{false, false}),
	}
	fcTC := NewFunctionTestCase(proc, tc.inputs, tc.expect, equalFn)
	s, info := fcTC.Run()
	require.True(t, s, info, tc.info)
}

func Test_EqualFn_Decimal256_DifferentScale(t *testing.T) {
	proc := testutil.NewProcess(t)

	// 5 (scale 0) compared to 500 (scale 2) == 5.00 -> equal.
	tc := tcTemp{
		info: "decimal256 = decimal256 (different scale)",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(types.New(types.T_decimal256, 58, 0),
				[]types.Decimal256{types.Decimal256FromInt64(5)}, []bool{false}),
			NewFunctionTestInput(types.New(types.T_decimal256, 58, 2),
				[]types.Decimal256{types.Decimal256FromInt64(500)}, []bool{false}),
		},
		expect: NewFunctionTestResult(types.T_bool.ToType(), false,
			[]bool{true}, []bool{false}),
	}
	fcTC := NewFunctionTestCase(proc, tc.inputs, tc.expect, equalFn)
	s, info := fcTC.Run()
	require.True(t, s, info, tc.info)
}

func Test_GreatThanFn_Decimal256(t *testing.T) {
	proc := testutil.NewProcess(t)
	tc := tcTemp{
		info: "decimal256 > decimal256",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(types.New(types.T_decimal256, 58, 0),
				[]types.Decimal256{types.Decimal256FromInt64(5), types.Decimal256FromInt64(1)},
				[]bool{false, false}),
			NewFunctionTestInput(types.New(types.T_decimal256, 58, 0),
				[]types.Decimal256{types.Decimal256FromInt64(1), types.Decimal256FromInt64(5)},
				[]bool{false, false}),
		},
		expect: NewFunctionTestResult(types.T_bool.ToType(), false,
			[]bool{true, false}, []bool{false, false}),
	}
	fcTC := NewFunctionTestCase(proc, tc.inputs, tc.expect, greatThanFn)
	s, info := fcTC.Run()
	require.True(t, s, info, tc.info)
}

func Test_NotEqualFn_Decimal256(t *testing.T) {
	proc := testutil.NewProcess(t)
	tc := tcTemp{
		info: "decimal256 != decimal256",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(types.New(types.T_decimal256, 58, 0),
				[]types.Decimal256{types.Decimal256FromInt64(5), types.Decimal256FromInt64(6)},
				[]bool{false, false}),
			NewFunctionTestInput(types.New(types.T_decimal256, 58, 0),
				[]types.Decimal256{types.Decimal256FromInt64(5), types.Decimal256FromInt64(7)},
				[]bool{false, false}),
		},
		expect: NewFunctionTestResult(types.T_bool.ToType(), false,
			[]bool{false, true}, []bool{false, false}),
	}
	fcTC := NewFunctionTestCase(proc, tc.inputs, tc.expect, notEqualFn)
	s, info := fcTC.Run()
	require.True(t, s, info, tc.info)
}

func Test_NullSafeEqualFn_Decimal256(t *testing.T) {
	proc := testutil.NewProcess(t)
	tc := tcTemp{
		info: "decimal256 <=> decimal256 with nulls",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(types.New(types.T_decimal256, 58, 0),
				[]types.Decimal256{types.Decimal256FromInt64(5), types.Decimal256FromInt64(0)},
				[]bool{false, true}),
			NewFunctionTestInput(types.New(types.T_decimal256, 58, 0),
				[]types.Decimal256{types.Decimal256FromInt64(5), types.Decimal256FromInt64(0)},
				[]bool{false, true}),
		},
		expect: NewFunctionTestResult(types.T_bool.ToType(), false,
			[]bool{true, true}, []bool{false, false}),
	}
	fcTC := NewFunctionTestCase(proc, tc.inputs, tc.expect, nullSafeEqualFn)
	s, info := fcTC.Run()
	require.True(t, s, info, tc.info)
}

// Test_ValueDec256Compare_AllBranches exercises the const/variable, null and
// scale-direction (m>=0 / m<0) branches of valueDec256Compare directly, so the
// decimal256 comparison helper is covered beyond the simple variable path.
func Test_ValueDec256Compare_AllBranches(t *testing.T) {
	proc := testutil.NewProcess(t)
	mp := proc.Mp()
	eq := func(a, b types.Decimal256) bool { return a == b }

	mkVar := func(scale int32, vals []int64, nullList []bool) *vector.Vector {
		vec := vector.NewVec(types.New(types.T_decimal256, 76, scale))
		ds := make([]types.Decimal256, len(vals))
		for i, v := range vals {
			ds[i] = types.Decimal256FromInt64(v)
		}
		require.NoError(t, vector.AppendFixedList(vec, ds, nullList, mp))
		return vec
	}
	mkConst := func(scale int32, val int64, length int) *vector.Vector {
		vec, err := vector.NewConstFixed(types.New(types.T_decimal256, 76, scale),
			types.Decimal256FromInt64(val), length, mp)
		require.NoError(t, err)
		return vec
	}
	run := func(p1, p2 *vector.Vector, length int) ([]bool, *nulls.Nulls) {
		w := vector.NewFunctionResultWrapper(types.T_bool.ToType(), mp)
		require.NoError(t, w.PreExtendAndReset(length))
		rs := vector.MustFunctionResult[bool](w)
		require.NoError(t, valueDec256Compare([]*vector.Vector{p1, p2}, rs, uint64(length), eq, nil))
		rsVec := rs.GetResultVector()
		col := vector.MustFixedColWithTypeCheck[bool](rsVec)
		out := make([]bool, length)
		copy(out, col)
		return out, rsVec.GetNulls()
	}

	// const vs const: m>=0 then m<0 (5 == 5.00).
	r, _ := run(mkConst(0, 5, 2), mkConst(2, 500, 2), 2)
	require.Equal(t, []bool{true, true}, r)
	r, _ = run(mkConst(2, 500, 2), mkConst(0, 5, 2), 2)
	require.Equal(t, []bool{true, true}, r)

	// c1 (p1 const) m>=0: with null then without null.
	r, ns := run(mkConst(0, 5, 2), mkVar(2, []int64{500, 0}, []bool{false, true}), 2)
	require.True(t, r[0])
	require.True(t, ns.Contains(1))
	r, _ = run(mkConst(0, 5, 2), mkVar(2, []int64{500, 400}, nil), 2)
	require.Equal(t, []bool{true, false}, r)

	// c1 m<0: with null then without null.
	r, ns = run(mkConst(2, 500, 2), mkVar(0, []int64{5, 0}, []bool{false, true}), 2)
	require.True(t, r[0])
	require.True(t, ns.Contains(1))
	r, _ = run(mkConst(2, 500, 2), mkVar(0, []int64{5, 6}, nil), 2)
	require.Equal(t, []bool{true, false}, r)

	// c2 (p2 const) m>=0: with null then without null.
	r, ns = run(mkVar(0, []int64{5, 0}, []bool{false, true}), mkConst(2, 500, 2), 2)
	require.True(t, r[0])
	require.True(t, ns.Contains(1))
	r, _ = run(mkVar(0, []int64{5, 4}, nil), mkConst(2, 500, 2), 2)
	require.Equal(t, []bool{true, false}, r)

	// c2 m<0: with null then without null.
	r, ns = run(mkVar(2, []int64{500, 0}, []bool{false, true}), mkConst(0, 5, 2), 2)
	require.True(t, r[0])
	require.True(t, ns.Contains(1))
	r, _ = run(mkVar(2, []int64{500, 600}, nil), mkConst(0, 5, 2), 2)
	require.Equal(t, []bool{true, false}, r)

	// var vs var with null: m>=0 then m<0.
	r, ns = run(mkVar(0, []int64{5, 0}, []bool{false, true}), mkVar(2, []int64{500, 500}, nil), 2)
	require.True(t, r[0])
	require.True(t, ns.Contains(1))
	r, ns = run(mkVar(2, []int64{500, 0}, []bool{false, true}), mkVar(0, []int64{5, 5}, nil), 2)
	require.True(t, r[0])
	require.True(t, ns.Contains(1))

	// var vs var without null: m>=0 then m<0.
	r, _ = run(mkVar(0, []int64{5, 6}, nil), mkVar(2, []int64{500, 500}, nil), 2)
	require.Equal(t, []bool{true, false}, r)
	r, _ = run(mkVar(2, []int64{500, 600}, nil), mkVar(0, []int64{5, 7}, nil), 2)
	require.Equal(t, []bool{true, false}, r)
}

// Test_ValueDec256Compare_ScaleOverflowError verifies that an overflow from
// Decimal256.Scale() (extreme scale span) is propagated as an error instead of
// comparing un-scaled raw values (issue #24565 review).
func Test_ValueDec256Compare_ScaleOverflowError(t *testing.T) {
	proc := testutil.NewProcess(t)
	mp := proc.Mp()
	big, err := types.ParseDecimal256(strings.Repeat("9", 76), 76, 0)
	require.NoError(t, err)
	p1, err := vector.NewConstFixed(types.New(types.T_decimal256, 76, 0), big, 1, mp)
	require.NoError(t, err)
	p2, err := vector.NewConstFixed(types.New(types.T_decimal256, 76, 30), types.Decimal256FromInt64(1), 1, mp)
	require.NoError(t, err)
	w := vector.NewFunctionResultWrapper(types.T_bool.ToType(), mp)
	require.NoError(t, w.PreExtendAndReset(1))
	rs := vector.MustFunctionResult[bool](w)
	err = valueDec256Compare([]*vector.Vector{p1, p2}, rs, 1,
		func(a, b types.Decimal256) bool { return a == b }, nil)
	require.Error(t, err)
}

func Test_LessThanFn_Decimal256(t *testing.T) {
	proc := testutil.NewProcess(t)
	tc := tcTemp{
		info: "decimal256 < decimal256",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(types.New(types.T_decimal256, 58, 0),
				[]types.Decimal256{types.Decimal256FromInt64(1), types.Decimal256FromInt64(5)},
				[]bool{false, false}),
			NewFunctionTestInput(types.New(types.T_decimal256, 58, 0),
				[]types.Decimal256{types.Decimal256FromInt64(5), types.Decimal256FromInt64(1)},
				[]bool{false, false}),
		},
		expect: NewFunctionTestResult(types.T_bool.ToType(), false,
			[]bool{true, false}, []bool{false, false}),
	}
	fcTC := NewFunctionTestCase(proc, tc.inputs, tc.expect, lessThanFn)
	s, info := fcTC.Run()
	require.True(t, s, info, tc.info)
}

func Test_LessEqualFn_Decimal256(t *testing.T) {
	proc := testutil.NewProcess(t)
	tc := tcTemp{
		info: "decimal256 <= decimal256",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(types.New(types.T_decimal256, 58, 0),
				[]types.Decimal256{types.Decimal256FromInt64(5), types.Decimal256FromInt64(6)},
				[]bool{false, false}),
			NewFunctionTestInput(types.New(types.T_decimal256, 58, 0),
				[]types.Decimal256{types.Decimal256FromInt64(5), types.Decimal256FromInt64(1)},
				[]bool{false, false}),
		},
		expect: NewFunctionTestResult(types.T_bool.ToType(), false,
			[]bool{true, false}, []bool{false, false}),
	}
	fcTC := NewFunctionTestCase(proc, tc.inputs, tc.expect, lessEqualFn)
	s, info := fcTC.Run()
	require.True(t, s, info, tc.info)
}

func Test_GreatEqualFn_Decimal256(t *testing.T) {
	proc := testutil.NewProcess(t)
	tc := tcTemp{
		info: "decimal256 >= decimal256",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(types.New(types.T_decimal256, 58, 0),
				[]types.Decimal256{types.Decimal256FromInt64(5), types.Decimal256FromInt64(1)},
				[]bool{false, false}),
			NewFunctionTestInput(types.New(types.T_decimal256, 58, 0),
				[]types.Decimal256{types.Decimal256FromInt64(5), types.Decimal256FromInt64(5)},
				[]bool{false, false}),
		},
		expect: NewFunctionTestResult(types.T_bool.ToType(), false,
			[]bool{true, false}, []bool{false, false}),
	}
	fcTC := NewFunctionTestCase(proc, tc.inputs, tc.expect, greatEqualFn)
	s, info := fcTC.Run()
	require.True(t, s, info, tc.info)
}

// Test_BetweenImpl_Decimal256_NonConst exercises the runtime decimal256 path of
// BETWEEN with a non-constant left operand, which previously fell through to
// panic("unreached code") because betweenImpl had no decimal256 branch
// (issue #24565 review).
func Test_BetweenImpl_Decimal256_NonConst(t *testing.T) {
	proc := testutil.NewProcess(t)
	mp := proc.Mp()

	// p0: non-constant decimal256 column; p1/p2: constant bounds [1, 100].
	p0 := vector.NewVec(types.New(types.T_decimal256, 76, 0))
	require.NoError(t, vector.AppendFixedList(p0, []types.Decimal256{
		types.Decimal256FromInt64(5),
		types.Decimal256FromInt64(50),
		types.Decimal256FromInt64(500),
	}, nil, mp))
	p1, err := vector.NewConstFixed(types.New(types.T_decimal256, 76, 0), types.Decimal256FromInt64(1), 3, mp)
	require.NoError(t, err)
	p2, err := vector.NewConstFixed(types.New(types.T_decimal256, 76, 0), types.Decimal256FromInt64(100), 3, mp)
	require.NoError(t, err)

	w := vector.NewFunctionResultWrapper(types.T_bool.ToType(), mp)
	require.NoError(t, w.PreExtendAndReset(3))
	require.NoError(t, betweenImpl([]*vector.Vector{p0, p1, p2}, w, proc, 3, nil))

	rs := vector.MustFunctionResult[bool](w)
	col := vector.MustFixedColWithTypeCheck[bool](rs.GetResultVector())
	require.Equal(t, []bool{true, true, false}, col[:3])
}
