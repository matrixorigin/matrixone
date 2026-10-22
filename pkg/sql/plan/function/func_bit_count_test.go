// Copyright 2021 Matrix Origin
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
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestBitCountInteger(t *testing.T) {
	proc := testutil.NewProcess(t)
	tc := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_int64.ToType(), []int64{0, 1, 64, -1, -2}, nil),
		},
		NewFunctionTestResult(types.T_uint64.ToType(), false, []uint64{0, 1, 1, 64, 63}, nil),
		BitCountInteger[int64])
	ok, info := tc.RunAndFree()
	require.True(t, ok, info)
}

func TestBitCountString(t *testing.T) {
	proc := testutil.NewProcess(t)
	tc := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_varchar.ToType(), []string{
				"", "   ", "64", "+1", "-1", "-2", "255",
				"9223372036854775808",
				"18446744073709551615",
				"18446744073709551616",
				"-9223372036854775808",
				"-18446744073709551616",
			}, nil),
		},
		NewFunctionTestResult(types.T_uint64.ToType(), false, []uint64{
			0, 0, 1, 1, 64, 63, 8,
			1,
			64,
			64,
			1,
			1,
		}, nil),
		BitCountNonBinaryString)
	ok, info := tc.RunAndFree()
	require.True(t, ok, info)
}

func TestBitCountStringNumericPrefixCoercion(t *testing.T) {
	for _, tc := range []struct {
		input string
		want  uint64
	}{
		{input: "abc", want: 0},
		{input: "12x", want: 2},
		{input: "123abc", want: 6},
		{input: "1.9", want: 1},
		{input: "+x", want: 0},
		{input: "-12x", want: 61},
	} {
		got, err := bitCountFromMysqlIntegerString(tc.input)
		require.NoError(t, err, tc.input)
		require.Equal(t, tc.want, got, tc.input)
	}
}

func TestBitCountFloat(t *testing.T) {
	proc := testutil.NewProcess(t)
	values := []float64{
		1.4, 1.5, 2.5, 3.5, 3.9, 64.9,
		-1.4, -1.5, -2.5, -3.5, -3.9,
		0, math.Copysign(0, -1), 0.9, -0.9, 3, -3,
		-9223372036854775808.0, 18446744073709551616.0,
		math.Inf(1), math.Inf(-1), math.NaN(),
	}
	want := []uint64{
		1, 1, 1, 2, 2, 1,
		64, 64, 63, 63, 63,
		0, 0, 0, 0, 2, 63,
		1, 64, 64, 1, 0,
	}
	nulls := make([]bool, len(values))
	nulls[len(values)-1] = true // A NULL NaN must not enter the conversion helper.
	float32Values := make([]float32, len(values))
	for i, value := range values {
		float32Values[i] = float32(value)
	}
	for _, tc := range []struct {
		typ    types.T
		values any
		fn     executeLogicOfOverload
	}{
		{types.T_float32, float32Values, BitCountFloat[float32]},
		{types.T_float64, values, BitCountFloat[float64]},
	} {
		t.Run(tc.typ.String(), func(t *testing.T) {
			fc := NewFunctionTestCase(proc,
				[]FunctionTestInput{NewFunctionTestInput(tc.typ.ToType(), tc.values, nulls)},
				NewFunctionTestResult(types.T_uint64.ToType(), false, want, nulls), tc.fn)
			ok, info := fc.RunAndFree()
			require.True(t, ok, info)
		})
	}

	t.Run("bounds", func(t *testing.T) {
		const upper = 18446744073709549568.0
		for _, tc := range []struct {
			value float64
			want  uint64
		}{
			{math.Nextafter(float64(math.MinInt64), math.Inf(-1)), 1},
			{float64(math.MinInt64), 1},
			{math.Nextafter(float64(math.MinInt64), 0), 2},
			{math.Nextafter(float64(math.MaxInt64), 0), 53},
			{float64(math.MaxInt64), 1},
			{math.Nextafter(upper, 0), 52},
			{upper, 64},
			{math.Nextafter(upper, math.Inf(1)), 64},
		} {
			got, err := bitCountFromFloat(tc.value, proc)
			require.NoError(t, err)
			require.Equal(t, tc.want, got, "input %g", tc.value)
		}
	})

	_, err := bitCountFromFloat(math.NaN(), proc)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))
	_, err = bitCountFromFloat(float32(math.NaN()), proc)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))
}

func TestBitCountDecimal64(t *testing.T) {
	proc := testutil.NewProcess(t)
	typ := types.New(types.T_decimal64, 18, 1)
	inputs := []string{"1.4", "1.5", "2.5", "-1.4", "-1.5", "-2.5"}
	values := make([]types.Decimal64, len(inputs))
	for i, input := range inputs {
		v, err := types.ParseDecimal64(input, typ.Width, typ.Scale)
		require.NoError(t, err)
		values[i] = v
	}

	tc := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(typ, values, nil),
		},
		NewFunctionTestResult(types.T_uint64.ToType(), false, []uint64{1, 1, 2, 64, 63, 63}, nil),
		BitCountDecimal64)
	ok, info := tc.RunAndFree()
	require.True(t, ok, info)

	got, err := bitCountFromDecimal64(types.Decimal64Max, 0)
	require.NoError(t, err)
	require.Equal(t, uint64(63), got)

	got, err = bitCountFromDecimal64(types.Decimal64Min, 0)
	require.NoError(t, err)
	require.Equal(t, uint64(1), got)
}

func TestBitCountDecimal128(t *testing.T) {
	proc := testutil.NewProcess(t)
	typ := types.New(types.T_decimal128, 38, 0)
	inputs := []string{
		"9223372036854775807",
		"9223372036854775808",
		"18446744073709551615",
		"-1",
		"-9223372036854775808",
		"-18446744073709551616",
	}
	values := make([]types.Decimal128, len(inputs))
	for i, input := range inputs {
		v, err := types.ParseDecimal128(input, typ.Width, typ.Scale)
		require.NoError(t, err)
		values[i] = v
	}

	tc := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(typ, values, nil),
		},
		NewFunctionTestResult(types.T_uint64.ToType(), false, []uint64{63, 1, 64, 64, 1, 1}, nil),
		BitCountDecimal128)
	ok, info := tc.RunAndFree()
	require.True(t, ok, info)

	overflow, err := types.ParseDecimal128("18446744073709551616", typ.Width, typ.Scale)
	require.NoError(t, err)
	_, err = bitCountFromDecimal128(overflow, typ.Scale)
	require.Error(t, err)
}

func TestBitCountDecimal256(t *testing.T) {
	proc := testutil.NewProcess(t)
	typ := types.New(types.T_decimal256, 65, 1)
	values := []types.Decimal256{
		mustParseDecimal256(t, "1.5", 1),
		mustParseDecimal256(t, "2.5", 1),
		mustParseDecimal256(t, "-1.5", 1),
		mustParseDecimal256(t, "9223372036854775807.0", 1),
		mustParseDecimal256(t, "9223372036854775808.0", 1),
		mustParseDecimal256(t, "18446744073709551615.0", 1),
		mustParseDecimal256(t, "-1.0", 1),
		mustParseDecimal256(t, "-18446744073709551616.0", 1),
	}

	tc := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(typ, values, nil),
		},
		NewFunctionTestResult(types.T_uint64.ToType(), false, []uint64{1, 2, 63, 63, 1, 64, 64, 1}, nil),
		BitCountDecimal256)
	ok, info := tc.RunAndFree()
	require.True(t, ok, info)

	overflow := mustParseDecimal256(t, "18446744073709551616.0", typ.Scale)
	_, err := bitCountFromDecimal256(overflow, typ.Scale)
	require.Error(t, err)
}

func TestBitCountBinaryString(t *testing.T) {
	proc := testutil.NewProcess(t)
	tc := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_varbinary.ToType(), []string{"64", "@", "\xff"}, nil),
		},
		NewFunctionTestResult(types.T_uint64.ToType(), false, []uint64{7, 1, 8}, nil),
		BitCountBinaryString)
	ok, info := tc.RunAndFree()
	require.True(t, ok, info)
}

func TestBitCountVarcharWithIsBin(t *testing.T) {
	proc := testutil.NewProcess(t)
	mp := proc.Mp()
	input := testutil.MakeVarlenaVector(
		[][]byte{[]byte("64"), []byte("@"), {0x00}, {0xff}},
		nil,
		types.T_varchar.ToType(),
		mp,
	)
	defer input.Free(mp)
	input.SetIsBin(true)

	result := vector.NewFunctionResultWrapper(types.T_uint64.ToType(), mp)
	defer result.Free()
	require.NoError(t, result.PreExtendAndReset(input.Length()))

	require.NoError(t, BitCountNonBinaryString([]*vector.Vector{input}, result, proc, input.Length(), nil))
	require.Equal(t, []uint64{7, 1, 0, 8}, vector.MustFixedColNoTypeCheck[uint64](result.GetResultVector()))
}

func TestBitCountVarcharUsesRowStringDomain(t *testing.T) {
	proc := testutil.NewProcess(t)
	mp := proc.Mp()
	input := testutil.MakeVarlenaVector(
		[][]byte{[]byte("64"), []byte("64"), []byte("64")},
		nil,
		types.T_varchar.ToType(),
		mp,
	)
	defer input.Free(mp)
	require.NoError(t, input.SetRuntimeStringDomainAtWithMP(0, types.RuntimeStringBinary, mp))
	require.NoError(t, input.SetRuntimeStringDomainAtWithMP(1, types.RuntimeStringText, mp))

	result := vector.NewFunctionResultWrapper(types.T_uint64.ToType(), mp)
	defer result.Free()
	require.NoError(t, result.PreExtendAndReset(input.Length()))

	require.NoError(t, BitCountNonBinaryString([]*vector.Vector{input}, result, proc, input.Length(), nil))
	require.Equal(t, []uint64{7, 1, 1}, vector.MustFixedColNoTypeCheck[uint64](result.GetResultVector()))
}

func TestBitCountTypeCheck(t *testing.T) {
	ctx := context.Background()

	get, err := GetFunctionByName(ctx, "bit_count", []types.Type{types.T_varchar.ToType()})
	require.NoError(t, err)
	require.Equal(t, int32(BIT_COUNT), get.fid)
	require.Equal(t, int32(13), get.overloadId)
	overload, err := GetFunctionById(ctx, get.GetEncodedOverloadID())
	require.NoError(t, err)
	require.False(t, overload.IsVolatile(), "text diagnostics do not make the result nondeterministic")
	require.True(t, overload.CannotFold(), "folding must preserve row diagnostics")

	get, err = GetFunctionByName(ctx, "bit_count", []types.Type{types.T_any.ToType()})
	require.NoError(t, err)
	require.Equal(t, int32(14), get.overloadId)
	require.True(t, get.needCast)
	require.Equal(t, types.T_varbinary, get.targetTypes[0].Oid)

	get, err = GetFunctionByName(ctx, "bit_count", []types.Type{types.T_binary.ToType()})
	require.NoError(t, err)
	require.Equal(t, int32(14), get.overloadId)

	get, err = GetFunctionByName(ctx, "bit_count", []types.Type{types.T_varbinary.ToType()})
	require.NoError(t, err)
	require.Equal(t, int32(14), get.overloadId)

	get, err = GetFunctionByName(ctx, "bit_count", []types.Type{types.T_blob.ToType()})
	require.NoError(t, err)
	require.Equal(t, int32(14), get.overloadId)
}

func TestBitCountStringWarnings(t *testing.T) {
	for _, tc := range []struct {
		name                        string
		values                      []string
		want                        []uint64
		nulls                       []bool
		mask                        *FunctionSelectList
		binary, rowBinary, constant bool
		warnings                    int
	}{
		{name: "text", values: []string{"3x", "2.9", "3 ", "", "bad", "3x"}, want: []uint64{2, 1, 2, 0, 0, 0}, nulls: []bool{false, false, false, false, false, true}, warnings: 3},
		{name: "binary_literal", values: []string{"3x"}, want: []uint64{8}, binary: true},
		{name: "runtime_domains", values: []string{"3x", "3x", "3"}, want: []uint64{8, 2, 2}, rowBinary: true, warnings: 1},
		{name: "partial_selection", values: []string{"bad", "3x", "bad"}, want: []uint64{0, 2, 0}, mask: &FunctionSelectList{AnyNull: true, SelectList: []bool{false, true, false}}, warnings: 1},
		{name: "constant_rows", values: []string{"3x"}, want: []uint64{2, 2, 2}, constant: true, warnings: 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			session := &numericWarningSession{}
			proc.Session = session
			outputNulls := tc.nulls
			if tc.mask != nil {
				outputNulls = []bool{true, false, true}
			}
			fc := NewFunctionTestCase(proc, []FunctionTestInput{NewFunctionTestInput(types.T_varchar.ToType(), tc.values, tc.nulls)}, NewFunctionTestResult(types.T_uint64.ToType(), false, tc.want, outputNulls), BitCountNonBinaryString).WithSelectList(tc.mask)
			defer fc.Free()
			fc.parameters[0].SetIsBin(tc.binary)
			if tc.rowBinary {
				require.NoError(t, fc.parameters[0].SetRuntimeStringDomainAtWithMP(0, types.RuntimeStringBinary, proc.Mp()))
				require.NoError(t, fc.parameters[0].SetRuntimeStringDomainAtWithMP(1, types.RuntimeStringText, proc.Mp()))
			}
			if tc.constant {
				fc.parameters[0].SetClass(vector.CONSTANT)
				fc.fnLength = 3
			}
			ok, info := fc.RunAndFree()
			require.True(t, ok, info)
			require.Len(t, session.warnings, tc.warnings)
			for _, warning := range session.warnings {
				require.Equal(t, uint16(moerr.ER_TRUNCATED_WRONG_VALUE), warning.code)
				require.Contains(t, warning.msg, "Truncated incorrect INTEGER value")
			}
		})
	}
}

func TestBitCountDirectDiagnostics(t *testing.T) {
	for _, tc := range []struct {
		name, value string
		null, warns bool
	}{
		{"malformed", "3x", false, true}, {"valid", "3 ", false, false}, {"null", "", true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			session := &numericWarningSession{}
			proc.Session = session
			input, err := vector.NewConstBytes(types.T_varchar.ToType(), []byte(tc.value), 3, proc.Mp())
			require.NoError(t, err)
			defer input.Free(proc.Mp())
			if tc.null {
				input.SetNull(0)
			}
			for _, length := range []int{3, 0} {
				session.warnings = nil
				func() {
					result, err := RunFunctionDirectly(proc, EncodeOverloadID(BIT_COUNT, 13), []*vector.Vector{input}, length)
					require.NoError(t, err)
					defer result.Free(proc.Mp())
					require.Equal(t, length, result.Length())
					if tc.warns {
						require.Len(t, session.warnings, length)
					} else {
						require.Empty(t, session.warnings)
					}
					if length > 0 {
						require.True(t, result.IsConst())
					}
					for row := 0; row < length; row++ {
						require.Equal(t, tc.null, result.IsNull(uint64(row)))
						if !tc.null {
							require.Equal(t, uint64(2), vector.GetFixedAtNoTypeCheck[uint64](result, row))
						}
						if tc.warns {
							require.Equal(t, numericWarning{code: moerr.ER_TRUNCATED_WRONG_VALUE, msg: "Truncated incorrect INTEGER value: '3x'"}, session.warnings[row])
						}
					}
				}()
			}
		})
	}
}

func TestBitCountFoldCandidatesPreserveDiagnosticOwnership(t *testing.T) {
	for _, value := range []string{"3", "3x"} {
		source := &plan.Expr{Typ: plan.Type{Id: int32(types.T_varchar)}, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Sval{Sval: value}}}}
		expr := &plan.Expr{Typ: plan.Type{Id: int32(types.T_uint64)}, Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{Obj: EncodeOverloadID(BIT_COUNT, 13)}, Args: []*plan.Expr{source},
		}}}
		require.True(t, IsFoldCandidate(expr, false))
		require.False(t, IsConstant(expr, false))
		require.False(t, IsStatementConstant(expr))
		require.False(t, IsRuntimeConstant(expr))
		require.False(t, IsStatementConstantInput(expr))
		require.Equal(t, ZoneMapUnsupported, GetZoneMapEvaluation(expr.GetF()))
		expr.GetF().Args[0] = &plan.Expr{Typ: source.Typ, Expr: &plan.Expr_P{P: &plan.ParamRef{}}}
		require.False(t, IsFoldCandidate(expr, false))
		require.True(t, IsFoldCandidate(expr, true))
		expr.GetF().Args[0] = &plan.Expr{Typ: source.Typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{}}}
		require.False(t, IsFoldCandidate(expr, true))
		expr.GetF().Func.Obj = EncodeOverloadID(RANDOM, 0)
		expr.GetF().Args = nil
		require.False(t, IsFoldCandidate(expr, true))
	}
}
