// Copyright 2021 - 2026 Matrix Origin
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

package rule

import (
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
)

func TestFoldedDecimalDependencySurvivesPackedVector(t *testing.T) {
	for _, marked := range []bool{false, true} {
		for _, null := range []bool{false, true} {
			source := &plan.Expr{Expr: &plan.Expr_Vec{Vec: &plan.LiteralVec{DecimalLiteralRequiresV82: marked}}}
			result := &plan.Literal{Isnull: null, Value: &plan.Literal_Bval{Bval: true}}
			PreserveFoldedDecimalLiteralSemantics(source, result)
			require.Equal(t, marked, result.DecimalLiteralRequiresV82)
			require.Equal(t, null, result.Isnull)
			require.True(t, result.GetBval())
		}
	}
}

func TestGetConstantValue2AppendsEnumLiteralWithEnumWidth(t *testing.T) {
	proc := testutil.NewProcess(t)
	vec := vector.NewVec(types.T_enum.ToType())
	defer vec.Free(proc.Mp())

	for _, value := range []uint32{0, 1, 3} {
		expr := &plan.Expr{
			Typ: plan.Type{Id: int32(types.T_enum), Enumvalues: "a,b,"},
			Expr: &plan.Expr_Lit{Lit: &plan.Literal{
				Value: &plan.Literal_EnumVal{EnumVal: value},
			}},
		}
		constant, err := GetConstantValue2(proc, expr, vec)
		require.NoError(t, err)
		require.True(t, constant)
	}
	require.Equal(t, []types.Enum{0, 1, 3}, vector.MustFixedColNoTypeCheck[types.Enum](vec))
}

func TestConstantFoldRuleRetainsStandaloneInterval(t *testing.T) {
	expr := &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_interval)},
		Expr: &plan.Expr_List{List: &plan.ExprList{List: []*plan.Expr{
			{Typ: plan.Type{Id: int32(types.T_int64)}},
			{Typ: plan.Type{Id: int32(types.T_varchar)}},
		}}},
	}
	require.Same(t, expr, NewConstantFold(false).constantFold(expr, testutil.NewProcess(t)))
}

func TestGetConstantValue2PreservesAndValidatesLiteralStringSource(t *testing.T) {
	proc := testutil.NewProcess(t)
	vec := vector.NewVec(types.T_varchar.ToType())
	defer vec.Free(proc.Mp())
	for _, test := range []struct {
		source types.StringSource
		null   bool
	}{
		{source: types.StringSourceExpression},
		{source: types.StringSourceLiteral},
		{source: types.StringSourceUserVariable},
		{source: types.StringSourceSQLPrepare, null: true},
		{source: types.StringSourceCOMStmt},
	} {
		encoded := uint32(test.source) + 1
		if test.source == types.StringSourceLiteral {
			encoded = 0
		}
		literal := &plan.Literal{
			Value:        &plan.Literal_Sval{Sval: "value"},
			Isnull:       test.null,
			StringSource: encoded,
		}
		constant, err := GetConstantValue2(proc, &plan.Expr{
			Typ:  plan.Type{Id: int32(types.T_varchar)},
			Expr: &plan.Expr_Lit{Lit: literal},
		}, vec)
		require.NoError(t, err)
		require.True(t, constant)
		row := vec.Length() - 1
		require.Equal(t, test.source, vec.GetStringSourceAt(row))
		require.Equal(t, test.null, vec.IsNull(uint64(row)))
	}

	length := vec.Length()
	for _, rawSource := range []uint32{257, ^uint32(0)} {
		constant, err := GetConstantValue2(proc, &plan.Expr{
			Typ: plan.Type{Id: int32(types.T_varchar)},
			Expr: &plan.Expr_Lit{Lit: &plan.Literal{
				Value: &plan.Literal_Sval{Sval: "invalid"}, StringSource: rawSource,
			}},
		}, vec)
		require.False(t, constant)
		require.ErrorContains(t, err, "invalid literal string source")
		require.Equal(t, length, vec.Length())
	}
}

func TestGetConstantValue2PhysicalFamilySource(t *testing.T) {
	for _, test := range []struct {
		oid     types.T
		literal *plan.Literal
	}{
		{types.T_bool, &plan.Literal{Value: &plan.Literal_Bval{Bval: true}}},
		{types.T_bit, &plan.Literal{Value: &plan.Literal_U64Val{U64Val: 3}}},
		{types.T_int8, &plan.Literal{Value: &plan.Literal_I8Val{I8Val: 3}}},
		{types.T_int16, &plan.Literal{Value: &plan.Literal_I16Val{I16Val: 3}}},
		{types.T_int32, &plan.Literal{Value: &plan.Literal_I32Val{I32Val: 3}}},
		{types.T_int64, &plan.Literal{Value: &plan.Literal_I64Val{I64Val: 3}}},
		{types.T_uint8, &plan.Literal{Value: &plan.Literal_U8Val{U8Val: 3}}},
		{types.T_uint16, &plan.Literal{Value: &plan.Literal_U16Val{U16Val: 3}}},
		{types.T_uint32, &plan.Literal{Value: &plan.Literal_U32Val{U32Val: 3}}},
		{types.T_uint64, &plan.Literal{Value: &plan.Literal_U64Val{U64Val: 3}}},
		{types.T_float32, &plan.Literal{Value: &plan.Literal_Fval{Fval: 3}}},
		{types.T_float64, &plan.Literal{Value: &plan.Literal_Dval{Dval: 3}}},
		{types.T_varchar, &plan.Literal{Value: &plan.Literal_Sval{Sval: "value"}}},
		{types.T_array_float32, &plan.Literal{Value: &plan.Literal_VecVal{VecVal: "\x00\x00\x00\x00"}}},
		{types.T_timestamp, &plan.Literal{Value: &plan.Literal_Timestampval{Timestampval: 3}}},
		{types.T_datetime, &plan.Literal{Value: &plan.Literal_Datetimeval{Datetimeval: 3}}},
		{types.T_enum, &plan.Literal{Value: &plan.Literal_EnumVal{EnumVal: 3}}},
		{types.T_decimal64, &plan.Literal{Value: &plan.Literal_Decimal64Val{Decimal64Val: &plan.Decimal64{A: 3}}}},
		{types.T_decimal128, &plan.Literal{Value: &plan.Literal_Decimal128Val{Decimal128Val: &plan.Decimal128{A: 3}}}},
	} {
		t.Run(test.oid.String(), func(t *testing.T) {
			proc := testutil.NewProcess(t)
			vec := vector.NewVec(test.oid.ToType())
			t.Cleanup(func() { vec.Free(proc.Mp()); require.Zero(t, proc.Mp().CurrNB()) })
			expr := &plan.Expr{Typ: plan.Type{Id: int32(test.oid)}, Expr: &plan.Expr_Lit{Lit: test.literal}}
			for i := 0; i < 2; i++ {
				ok, err := GetConstantValue2(proc, expr, vec)
				require.NoError(t, err)
				require.True(t, ok)
				require.False(t, vec.IsNull(uint64(i)))
				require.Equal(t, types.StringSourceLiteral, vec.GetStringSourceAt(i))
			}
			test.literal.Isnull = true
			ok, err := GetConstantValue2(proc, expr, vec)
			require.NoError(t, err)
			require.True(t, ok)
			require.Equal(t, 3, vec.Length())
			require.True(t, vec.IsNull(2))
			require.Equal(t, types.StringSourceLiteral, vec.GetStringSourceAt(2))
			require.Nil(t, vec.GetStringSources())

			// A missing payload keeps the established fallback placeholder
			// behavior; the DATETIME branch historically reports a constant NULL.
			expr.Expr = &plan.Expr_Lit{Lit: &plan.Literal{}}
			ok, err = GetConstantValue2(proc, expr, vec)
			require.NoError(t, err)
			require.Equal(t, test.oid == types.T_datetime, ok)
			require.Equal(t, 4, vec.Length())
			wantSource := types.StringSourceExpression
			if ok {
				wantSource = types.StringSourceLiteral
			}
			require.Equal(t, wantSource, vec.GetStringSourceAt(3))
			require.Equal(t, types.StringSourceLiteral, vec.GetStringSourceAt(2))
			require.Equal(t, !vec.GetType().IsVarlen(), vec.IsNull(3))
		})
	}
}

func TestGetConstantValue2TemporalFallback(t *testing.T) {
	// Canonical DATE/TIME literals are not handled by this fast path. Preserve
	// the placeholder and fallback contract used by constructValueScan.
	for _, test := range []struct {
		oid     types.T
		literal *plan.Literal
	}{
		{types.T_date, &plan.Literal{Value: &plan.Literal_Dateval{Dateval: 3}}},
		{types.T_time, &plan.Literal{Value: &plan.Literal_Timeval{Timeval: 3}}},
	} {
		t.Run(test.oid.String(), func(t *testing.T) {
			proc := testutil.NewProcess(t)
			vec := vector.NewVec(test.oid.ToType())
			t.Cleanup(func() { vec.Free(proc.Mp()); require.Zero(t, proc.Mp().CurrNB()) })
			expr := &plan.Expr{Typ: plan.Type{Id: int32(test.oid)}, Expr: &plan.Expr_Lit{Lit: test.literal}}
			ok, err := GetConstantValue2(proc, expr, vec)
			require.NoError(t, err)
			require.False(t, ok)
			require.Equal(t, 1, vec.Length())
			require.True(t, vec.IsNull(0))
			require.Equal(t, types.StringSourceExpression, vec.GetStringSourceAt(0))
		})
	}
}

// Payload is reserved before measuring: a uniform literal stream must not
// allocate a row-source sidecar, regardless of its length or physical type.
func TestGetConstantValue2UniformSourceAllocation(t *testing.T) {
	for _, oid := range []types.T{types.T_int32, types.T_varchar} {
		for _, n := range []int{100, 1000, 10000} {
			t.Run(fmt.Sprintf("%s/%d", oid, n), func(t *testing.T) {
				proc := testutil.NewProcess(t)
				vec := vector.NewVec(oid.ToType())
				t.Cleanup(func() {
					vec.Free(proc.Mp())
					require.Zero(t, proc.Mp().CurrNB())
				})
				require.NoError(t, vec.PreExtend(n, proc.Mp()))
				literal := &plan.Literal{Value: &plan.Literal_I32Val{I32Val: 42}}
				if oid == types.T_varchar {
					literal.Value = &plan.Literal_Sval{Sval: "inline"}
				}
				expr := &plan.Expr{Typ: plan.Type{Id: int32(oid)}, Expr: &plan.Expr_Lit{Lit: literal}}
				stats := proc.Mp().Stats()
				allocs, bytes := stats.NumAlloc.Load(), stats.NumAllocBytes.Load()
				for i := 0; i < n; i++ {
					ok, err := GetConstantValue2(proc, expr, vec)
					require.NoError(t, err)
					require.True(t, ok)
				}
				t.Logf("rows=%d allocations=%d bytes=%d", n, stats.NumAlloc.Load()-allocs, stats.NumAllocBytes.Load()-bytes)
				require.Equal(t, allocs, stats.NumAlloc.Load())
				require.Equal(t, bytes, stats.NumAllocBytes.Load())
				require.Nil(t, vec.GetStringSources())
				require.Equal(t, n, vec.Length())
				for i := 0; i < n; i++ {
					require.Equal(t, types.StringSourceLiteral, vec.GetStringSourceAt(i))
					if oid == types.T_varchar {
						require.Equal(t, "inline", vec.GetStringAt(i))
					} else {
						require.Equal(t, int32(42), vector.GetFixedAtNoTypeCheck[int32](vec, i))
					}
				}
			})
		}
	}
}

func BenchmarkGetConstantValue2StringSource(b *testing.B) {
	for _, n := range []int{100, 1000, 10000} {
		for _, mixed := range []bool{false, true} {
			b.Run(fmt.Sprintf("rows=%d/mixed=%v", n, mixed), func(b *testing.B) {
				proc := testutil.NewProcess(b)
				literal := &plan.Literal{Value: &plan.Literal_I32Val{I32Val: 42}}
				expr := &plan.Expr{Typ: plan.Type{Id: int32(types.T_int32)}, Expr: &plan.Expr_Lit{Lit: literal}}
				b.ReportAllocs()
				b.ResetTimer()
				for iteration := 0; iteration < b.N; iteration++ {
					vec := vector.NewVec(types.T_int32.ToType())
					if err := vec.PreExtend(n, proc.Mp()); err != nil {
						b.Fatal(err)
					}
					for row := 0; row < n; row++ {
						literal.StringSource = 0
						if mixed && row >= n/2 {
							literal.StringSource = uint32(types.StringSourceCOMStmt) + 1
						}
						ok, err := GetConstantValue2(proc, expr, vec)
						if err != nil || !ok {
							vec.Free(proc.Mp())
							b.Fatalf("constant=%v err=%v", ok, err)
						}
					}
					vec.Free(proc.Mp())
				}
				b.StopTimer()
				require.Zero(b, proc.Mp().CurrNB())
			})
		}
	}
}

func makeConstantCastExpr(t *testing.T, name string, sourceType, targetType types.Type, value string) *plan.Expr {
	t.Helper()
	f, err := function.GetFunctionByName(context.Background(), name, []types.Type{sourceType, targetType})
	require.NoError(t, err)

	targetPlanType := plan.Type{
		Id:    int32(targetType.Oid),
		Width: targetType.Width,
		Scale: targetType.Scale,
	}
	return &plan.Expr{
		Typ: targetPlanType,
		Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{Obj: f.GetEncodedOverloadID(), ObjName: name},
			Args: []*plan.Expr{
				{
					Typ:  plan.Type{Id: int32(sourceType.Oid), Width: sourceType.Width, Scale: sourceType.Scale},
					Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Sval{Sval: value}}},
				},
				{
					Typ:  targetPlanType,
					Expr: &plan.Expr_T{T: &plan.TargetType{}},
				},
			},
		}},
	}
}

func TestPreparedConstantFoldKeepsSqlModeDependentTemporalCast(t *testing.T) {
	proc := testutil.NewProcess(t)
	stringType := types.New(types.T_varchar, 32, 0)

	for _, targetType := range []types.Type{
		types.T_date.ToType(),
		types.New(types.T_datetime, 0, 6),
		types.New(types.T_timestamp, 0, 6),
	} {
		t.Run(targetType.Oid.String(), func(t *testing.T) {
			expr := makeConstantCastExpr(t, "cast", stringType, targetType, "0000-00-00 00:00:00")
			folded := NewConstantFold(true).constantFold(expr, proc)
			require.NotNil(t, folded.GetF())
			valid := makeConstantCastExpr(t, "cast", stringType, targetType, "2024-01-02 03:04:05")
			validFolded := NewConstantFold(true).constantFold(valid, proc)
			if targetType.Oid == types.T_timestamp {
				require.NotNil(t, validFolded.GetF()) // session time_zone changes at EXECUTE
			} else {
				require.NotNil(t, validFolded.GetLit())
			}
		})
	}
}

func TestConstantFoldStillFoldsUnaffectedCasts(t *testing.T) {
	proc := testutil.NewProcess(t)
	stringType := types.New(types.T_varchar, 32, 0)

	nonPreparedTemporal := makeConstantCastExpr(t, "cast", stringType, types.T_date.ToType(), "2024-01-02")
	require.NotNil(t, NewConstantFold(false).constantFold(nonPreparedTemporal, proc).GetLit())

	preparedNumeric := makeConstantCastExpr(t, "cast", stringType, types.T_int64.ToType(), "42")
	require.NotNil(t, NewConstantFold(true).constantFold(preparedNumeric, proc).GetLit())

	preparedStrictTemporal := makeConstantCastExpr(t, "cast_strict", stringType, types.T_date.ToType(), "2024-01-02")
	require.NotNil(t, NewConstantFold(true).constantFold(preparedStrictTemporal, proc).GetLit())

	ordinaryStrictTime := makeConstantCastExpr(t, "cast_strict", stringType, types.T_time.ToTypeWithScale(6), "12:34:56")
	require.NotNil(t, NewConstantFold(false).constantFold(ordinaryStrictTime, proc).GetLit())
}

func TestConstantFoldDefersLegacyTimeAssignmentCast(t *testing.T) {
	proc := testutil.NewProcess(t)
	expr := makeConstantCastExpr(
		t,
		"cast_strict",
		types.T_varchar.ToType(),
		types.T_time.ToTypeWithScale(6),
		"2562047788:00:00",
	)

	folded := NewConstantFold(false).constantFold(expr, proc)
	require.NotNil(t, folded.GetF())
	require.Equal(t, "cast_strict", folded.GetF().GetFunc().GetObjName())
}

func TestPreparedConstantFoldKeepsExactDecimalBelowImplicitFloatCast(t *testing.T) {
	proc := testutil.NewProcess(t)
	ctx := context.Background()
	stringType := types.New(types.T_varchar, 64, 0)
	decimalType := types.New(types.T_decimal128, 38, 10)
	floatType := types.T_float64.ToType()

	explicit, err := function.GetFunctionByNameWithOverload(
		ctx, "cast", []types.Type{stringType, decimalType}, 1)
	require.NoError(t, err)
	makeExplicitDecimal := func() *plan.Expr {
		return &plan.Expr{
			Typ: plan.Type{Id: int32(decimalType.Oid), Width: decimalType.Width, Scale: decimalType.Scale},
			Expr: &plan.Expr_F{F: &plan.Function{
				Func: &plan.ObjectRef{Obj: explicit.GetEncodedOverloadID(), ObjName: "cast"},
				Args: []*plan.Expr{
					{
						Typ: plan.Type{Id: int32(stringType.Oid), Width: stringType.Width},
						Expr: &plan.Expr_Lit{Lit: &plan.Literal{
							Value: &plan.Literal_Sval{Sval: "9007199254740992.0000000002"},
						}},
					},
					{
						Typ: plan.Type{
							Id: int32(decimalType.Oid), Width: decimalType.Width, Scale: decimalType.Scale,
						},
						Expr: &plan.Expr_T{T: &plan.TargetType{}},
					},
				},
			}},
		}
	}
	implicit, err := function.GetFunctionByNameWithOverload(
		ctx, "cast", []types.Type{decimalType, floatType}, 0)
	require.NoError(t, err)
	makeOuter := func() *plan.Expr {
		return &plan.Expr{
			Typ: plan.Type{Id: int32(floatType.Oid)},
			Expr: &plan.Expr_F{F: &plan.Function{
				Func: &plan.ObjectRef{Obj: implicit.GetEncodedOverloadID(), ObjName: "cast"},
				Args: []*plan.Expr{
					makeExplicitDecimal(),
					{Typ: plan.Type{Id: int32(floatType.Oid)}, Expr: &plan.Expr_T{T: &plan.TargetType{}}},
				},
			}},
		}
	}

	prepared := NewConstantFold(true).constantFold(makeOuter(), proc)
	require.NotNil(t, prepared.GetF())
	require.NotNil(t, prepared.GetF().Args[0].GetF(), "explicit DECIMAL source must remain recoverable")
	require.NotNil(t, NewConstantFold(false).constantFold(makeOuter(), proc).GetLit(),
		"ordinary non-prepared constant folding remains unchanged")
}

func TestConstantFoldPreservesSerializedResultProvenance(t *testing.T) {
	proc := testutil.NewProcess(t)
	inputType := types.T_bool.ToType()

	for _, name := range []string{function.SerialFunctionName, function.SerialFullFunctionName} {
		t.Run(name, func(t *testing.T) {
			registered, err := function.GetFunctionByName(context.Background(), name, []types.Type{inputType})
			require.NoError(t, err)
			resultType := registered.GetReturnType()
			require.Equal(t, types.CharsetBinary, resultType.Charset)

			expr := &plan.Expr{
				Typ: plan.Type{
					Id:      int32(resultType.Oid),
					Width:   resultType.Width,
					Scale:   resultType.Scale,
					Charset: uint32(resultType.Charset),
				},
				Expr: &plan.Expr_F{F: &plan.Function{
					Func: &plan.ObjectRef{Obj: registered.GetEncodedOverloadID(), ObjName: name},
					Args: []*plan.Expr{{
						Typ:  plan.Type{Id: int32(types.T_bool)},
						Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Bval{Bval: true}}},
					}},
				}},
			}

			folded := NewConstantFold(false).constantFold(expr, proc)
			literal := folded.GetLit()
			require.NotNil(t, literal)
			require.Equal(t, uint32(types.CharsetBinary), folded.Typ.Charset)
			require.Equal(t, string([]byte{0x27}), literal.GetSval())
			require.False(t, literal.GetIsBin(), "serial folding must not acquire SQL hex/bit semantics")
			require.True(t, literal.GetIsSerialized(), "serialized bytes lost their diagnostic provenance")
		})
	}

	t.Run("serial null remains an ordinary null", func(t *testing.T) {
		registered, err := function.GetFunctionByName(
			context.Background(), function.SerialFunctionName, []types.Type{inputType},
		)
		require.NoError(t, err)

		expr := &plan.Expr{
			Typ: plan.Type{Id: int32(types.T_varchar)},
			Expr: &plan.Expr_F{F: &plan.Function{
				Func: &plan.ObjectRef{
					Obj:     registered.GetEncodedOverloadID(),
					ObjName: function.SerialFunctionName,
				},
				Args: []*plan.Expr{{
					Typ:  plan.Type{Id: int32(types.T_bool)},
					Expr: &plan.Expr_Lit{Lit: &plan.Literal{Isnull: true}},
				}},
			}},
		}

		literal := NewConstantFold(false).constantFold(expr, proc).GetLit()
		require.NotNil(t, literal)
		require.True(t, literal.GetIsnull())
		require.False(t, literal.GetIsBin(), "NULL must not acquire binary identity metadata")
		require.False(t, literal.GetIsSerialized(), "NULL must not acquire serialized provenance")
	})
}

type foldTestWarnings struct{ count int }

func (s *foldTestWarnings) AppendWarningDiagnostic(uint16, string) { s.count++ }

func TestConstantFoldDefersDiagnosticExpressions(t *testing.T) {
	for _, prepared := range []bool{false, true} {
		for _, value := range []string{"838:59:59", "12:00:00"} {
			proc := testutil.NewProcess(t)
			sink := &foldTestWarnings{}
			proc.WarningSink = sink
			expr := &plan.Expr{Typ: plan.Type{Id: int32(types.T_varchar), Width: 29, Scale: 6}, Expr: &plan.Expr_F{F: &plan.Function{
				Func: &plan.ObjectRef{Obj: function.EncodeOverloadID(function.ADDTIME, 9)},
				Args: []*plan.Expr{
					{Typ: plan.Type{Id: int32(types.T_varchar)}, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Sval{Sval: value}}}},
					{Typ: plan.Type{Id: int32(types.T_varchar)}, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Sval{Sval: "00:00:01"}}}},
				},
			}}}
			got := NewConstantFold(prepared).constantFold(expr, proc)
			require.Zero(t, sink.count)
			require.Same(t, sink, proc.GetWarningSink())
			if value == "838:59:59" {
				require.NotNil(t, got.GetF())
			} else {
				require.NotNil(t, got.GetLit())
			}
		}
	}
}

func TestConstantFoldDefersExplicitCastRangeWarnings(t *testing.T) {
	for _, prepared := range []bool{false, true} {
		for _, tc := range []struct {
			target            types.Type
			overflow, control string
		}{
			{types.T_int64.ToType(), "9223372036854775808", "9223372036854775807"},
			{types.T_uint64.ToType(), "-1", "18446744073709551615"},
			{types.New(types.T_decimal64, 5, 2), "999.995", "999.994"},
			{types.New(types.T_decimal128, 20, 2), "999999999999999999.995", "999999999999999999.994"},
			{types.New(types.T_decimal256, 40, 2), "99999999999999999999999999999999999999.995", "99999999999999999999999999999999999999.994"},
		} {
			for _, value := range []string{tc.overflow, tc.control} {
				t.Run(fmt.Sprintf("%v/%s/%s", prepared, tc.target, value), func(t *testing.T) {
					proc := testutil.NewProcess(t)
					sink := &foldTestWarnings{}
					proc.WarningSink = sink
					expr := makeConstantCastExpr(t, "cast", types.T_varchar.ToType(), tc.target, value)
					expr.GetF().Func.Obj = function.EncodeOverloadID(function.CAST, 1)
					got := NewConstantFold(prepared).constantFold(expr, proc)
					require.Zero(t, sink.count, "speculative diagnostics must not reach the real session")
					if value == tc.overflow {
						require.NotNil(t, got.GetF(), "warning cast must remain executable")
					} else if tc.target.Oid != types.T_decimal256 {
						require.NotNil(t, got.GetLit(), "silent boundary control remains foldable")
					} else {
						require.NotNil(t, got.GetVec(), "silent wide values use a constant vector")
						require.True(t, got.GetVec().DecimalLiteralRequiresV82)
					}
					if tc.target.Oid == types.T_decimal256 {
						vec, free, warned, err := EvaluateConstantExpression(proc, expr, batch.EmptyForConstFoldBatch)
						if free != nil {
							t.Cleanup(free)
						}
						require.NoError(t, err)
						require.NotNil(t, vec)
						require.Equal(t, value == tc.overflow, warned)
						require.Zero(t, sink.count)
					}
				})
			}
		}
	}
}

func TestConstantVectorFoldRetainsRecoverySource(t *testing.T) {
	for _, prepared := range []bool{false, true} {
		t.Run(fmt.Sprint(prepared), func(t *testing.T) {
			proc := testutil.NewProcess(t)
			wide := types.New(types.T_decimal256, 65, 1)
			makeCast := func(recoverSource bool) *plan.Expr {
				expr := makeConstantCastExpr(t, "cast", types.T_varchar.ToType(), wide, "900")
				expr.GetF().Func.Obj = function.EncodeOverloadID(function.CAST, 1)
				if recoverSource {
					expr.GetF().Args[0].GetLit().Src = &plan.Expr{
						Typ:  types.T_varchar.ToType().PlanType(),
						Expr: &plan.Expr_V{V: &plan.VarRef{Name: "later_execution_value"}},
					}
				}
				return expr
			}
			cast := NewConstantFold(prepared).constantFold(makeCast(true), proc)
			require.NotNil(t, cast.GetF())
			require.NotNil(t, cast.GetF().Args[0].GetLit().Src)
			plus, err := function.GetFunctionByName(proc.Ctx, "+", []types.Type{wide, wide})
			require.NoError(t, err)
			parent := &plan.Expr{Typ: wide.PlanType(), Expr: &plan.Expr_F{F: &plan.Function{
				Func: &plan.ObjectRef{ObjName: "+", Obj: plus.GetEncodedOverloadID()},
				Args: []*plan.Expr{makeCast(false), makeCast(true)},
			}}}
			got := NewConstantFold(prepared).constantFold(parent, proc)
			require.NotNil(t, got.GetF(), "a new constant-vector child must not erase its sibling's recovery source")
			require.NotNil(t, got.GetF().Args[0].GetVec())
			require.NotNil(t, got.GetF().Args[1].GetF().Args[0].GetLit().Src)
		})
	}
}

func TestDecimal256ScalarConstantAdmission(t *testing.T) {
	proc := testutil.NewProcess(t)
	wide := types.New(types.T_decimal256, 65, 1)
	for _, tc := range []struct {
		name                           string
		null, flat, mismatch, unmarked bool
	}{
		{name: "scalar"}, {name: "NULL", null: true},
		{name: "flat singleton", flat: true}, {name: "payload type mismatch", mismatch: true},
		{name: "unmarked", unmarked: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var value *vector.Vector
			var err error
			switch {
			case tc.null:
				value = vector.NewConstNull(wide, 1, proc.Mp())
			case tc.flat:
				value = vector.NewVec(wide)
				err = vector.AppendFixed(value, types.Decimal256{}, false, proc.Mp())
			case tc.mismatch:
				value, err = vector.NewConstFixed(types.T_int64.ToType(), int64(0), 1, proc.Mp())
			default:
				value, err = vector.NewConstFixed(wide, types.Decimal256{}, 1, proc.Mp())
			}
			if value != nil {
				defer value.Free(proc.Mp())
			}
			require.NoError(t, err)
			data, err := value.MarshalBinary()
			require.NoError(t, err)
			expr := &plan.Expr{Typ: wide.PlanType(), Expr: &plan.Expr_Vec{Vec: &plan.LiteralVec{
				Len: 1, Data: data, DecimalLiteralRequiresV82: !tc.unmarked,
			}}}
			require.Equal(t, tc.name == "scalar", IsDecimal256ScalarConstant(expr))
			expr.Typ.Id = int32(types.T_int64)
			require.False(t, IsDecimal256ScalarConstant(expr), "the enclosing type must agree too")
		})
	}
	require.False(t, IsDecimal256ScalarConstant(nil))
	require.False(t, IsDecimal256ScalarConstant(&plan.Expr{Typ: wide.PlanType(), Expr: &plan.Expr_Vec{Vec: &plan.LiteralVec{
		Len: 1, Data: []byte{1}, DecimalLiteralRequiresV82: true,
	}}}))
}

func TestScalarConstantVectorAdmission(t *testing.T) {
	proc := testutil.NewProcess(t)
	for _, tc := range []struct {
		name                  string
		constant              bool
		length, encodedLength int
		want                  bool
	}{
		{"scalar", true, 1, 1, true},
		{"flat singleton", false, 1, 1, false},
		{"flat list", false, 2, 2, false},
		{"constant multiple rows", true, 2, 2, false},
		{"payload length mismatch", true, 2, 1, false},
		{"encoded length mismatch", true, 1, 2, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var value *vector.Vector
			if tc.constant {
				var err error
				value, err = vector.NewConstFixed(types.T_int64.ToType(), int64(900), tc.length, proc.Mp())
				if value != nil {
					defer value.Free(proc.Mp())
				}
				require.NoError(t, err)
			} else {
				value = vector.NewVec(types.T_int64.ToType())
				defer value.Free(proc.Mp())
				for range tc.length {
					require.NoError(t, vector.AppendFixed(value, int64(900), false, proc.Mp()))
				}
			}
			data, err := value.MarshalBinary()
			require.NoError(t, err)
			expr := &plan.Expr{Expr: &plan.Expr_Vec{Vec: &plan.LiteralVec{Len: int32(tc.encodedLength), Data: data}}}
			require.Equal(t, tc.want, IsScalarConstantVector(expr))
		})
	}
	require.False(t, IsScalarConstantVector(nil))
	require.False(t, IsScalarConstantVector(&plan.Expr{Expr: &plan.Expr_Vec{Vec: &plan.LiteralVec{Len: 1, Data: []byte{1}}}}))
}

func TestEvaluateConstantExpressionKernelFailure(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []int64
		fail bool
	}{
		{"round", []int64{5000000000000000000, -19}, true},
		{"abs", []int64{math.MinInt64}, true},
		{"round", []int64{-11, -1}, false},
	} {
		t.Run(fmt.Sprintf("%s/fail=%t", tc.name, tc.fail), func(t *testing.T) {
			proc := testutil.NewProcess(t)
			t.Cleanup(func() {
				defer proc.Free()
				require.Equal(t, [2]int64{}, [2]int64{proc.Mp().CurrNB(), proc.Mp().OnHeapCurrNB()})
			})
			args := make([]*plan.Expr, len(tc.args))
			argTypes := make([]types.Type, len(tc.args))
			for i, value := range tc.args {
				argTypes[i] = types.T_int64.ToType()
				args[i] = &plan.Expr{Typ: plan.Type{Id: int32(types.T_int64)}, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I64Val{I64Val: value}}}}
			}
			fn, err := function.GetFunctionByName(proc.Ctx, tc.name, argTypes)
			require.NoError(t, err)
			expr := &plan.Expr{Typ: plan.Type{Id: int32(fn.GetReturnType().Oid)}, Expr: &plan.Expr_F{F: &plan.Function{Func: &plan.ObjectRef{Obj: fn.GetEncodedOverloadID(), ObjName: tc.name}, Args: args}}}
			var vec *vector.Vector
			var free func()
			var warned bool
			var escaped any
			originalSink := &foldTestWarnings{count: 7}
			proc.WarningSink = originalSink
			func() {
				defer func() { escaped = recover() }()
				vec, free, warned, err = EvaluateConstantExpression(proc, expr, batch.EmptyForConstFoldBatch)
			}()
			if free != nil {
				t.Cleanup(free)
			}
			require.Nil(t, escaped, "speculative failure must use the folder error channel")
			require.Same(t, originalSink, proc.WarningSink)
			require.Equal(t, 7, originalSink.count)
			require.False(t, warned)
			if tc.fail {
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrOutOfRange))
				require.Nil(t, vec)
				require.Nil(t, free)
			} else {
				require.NoError(t, err)
				require.NotNil(t, free)
				require.Equal(t, int64(-10), vector.MustFixedColNoTypeCheck[int64](vec)[0])
			}
		})
	}
}
