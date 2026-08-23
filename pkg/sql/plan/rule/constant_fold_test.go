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
