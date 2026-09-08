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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
)

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
			expr := makeConstantCastExpr(t, "cast", stringType, targetType, "2024-01-02 03:04:05")
			folded := NewConstantFold(true).constantFold(expr, proc)
			require.NotNil(t, folded.GetF())
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
