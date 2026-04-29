// Copyright 2024 Matrix Origin
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

package plan

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
)

func Test_replaceFuncId(t *testing.T) {
	case1 := &Expr{
		Expr: &plan.Expr_F{
			F: &plan.Function{
				Func: &ObjectRef{
					ObjName: "current_timestamp",
					Obj:     function.CURRENT_TIMESTAMP,
				},
				Args: []*Expr{
					{
						Expr: &plan.Expr_Col{
							Col: &plan.ColRef{
								RelPos: 1,
								ColPos: 10,
								Name:   "a",
							},
						},
					},
				},
			},
		},
	}

	err := replaceFuncId(context.Background(), case1)
	assert.NoError(t, err)

	case1ColDef := &plan.ColDef{
		Default: &plan.Default{
			Expr: case1,
		},
	}
	case1Expr, err := getDefaultExpr(context.Background(), case1ColDef)
	assert.NoError(t, err)
	assert.NotNil(t, case1Expr)
}

func TestGetTypeFromAstMySQLCompatibilityTypes(t *testing.T) {
	ctx := context.Background()

	yearType, err := getTypeFromAst(ctx, &tree.T{InternalType: tree.InternalType{
		Oid: uint32(defines.MYSQL_TYPE_YEAR),
	}})
	require.NoError(t, err)
	require.Equal(t, int32(types.T_year), yearType.Id)
	require.Equal(t, int32(4), yearType.Width)

	decimalType, err := getTypeFromAst(ctx, &tree.T{InternalType: tree.InternalType{
		Oid:         uint32(defines.MYSQL_TYPE_DECIMAL),
		DisplayWith: 65,
		Scale:       30,
	}})
	require.NoError(t, err)
	require.Equal(t, int32(types.T_decimal256), decimalType.Id)
	require.Equal(t, int32(65), decimalType.Width)
	require.Equal(t, int32(30), decimalType.Scale)

	setType, err := getTypeFromAst(ctx, &tree.T{InternalType: tree.InternalType{
		Oid:        uint32(defines.MYSQL_TYPE_SET),
		EnumValues: []string{"read", "write", "execute"},
	}})
	require.NoError(t, err)
	require.Equal(t, int32(types.T_uint64), setType.Id)
	require.Equal(t, "read,write,execute", setType.Enumvalues)
	require.Equal(t, "SET('read','write','execute')", FormatColType(setType))

	geometryType, err := getTypeFromAst(ctx, &tree.T{InternalType: tree.InternalType{
		Oid:          uint32(defines.MYSQL_TYPE_GEOMETRY),
		FamilyString: "geometry",
	}})
	require.NoError(t, err)
	require.Equal(t, int32(types.T_geometry), geometryType.Id)
	require.Equal(t, "GEOMETRY", FormatColType(geometryType))

	pointType, err := getTypeFromAst(ctx, &tree.T{InternalType: tree.InternalType{
		Oid:          uint32(defines.MYSQL_TYPE_GEOMETRY),
		FamilyString: "point",
	}})
	require.NoError(t, err)
	require.Equal(t, int32(types.T_geometry), pointType.Id)
	require.Equal(t, "POINT", pointType.Enumvalues)
	require.Equal(t, "POINT", FormatColType(pointType))
}

func TestMakePlan2DecimalExprWithTypeUsesDecimal256(t *testing.T) {
	expr, err := makePlan2DecimalExprWithType(context.Background(), "123456789012345678901234567890123456789")
	require.NoError(t, err)
	require.Equal(t, int32(types.T_decimal256), expr.Typ.Id)
	require.Equal(t, int32(65), expr.Typ.Width)
	require.Equal(t, int32(0), expr.Typ.Scale)
}

// TestMakePlan2DecimalExprWithTypeMalformedInputNotRecovered verifies that
// when Parse128 fails for a non-range reason (e.g. malformed input), we do
// NOT silently fall through to Parse256 and present its "beyond the range"
// error to the user. The original Decimal128 "illegal string" error is the
// right diagnostic.
func TestMakePlan2DecimalExprWithTypeMalformedInputNotRecovered(t *testing.T) {
	_, err := makePlan2DecimalExprWithType(context.Background(), "not-a-number")
	require.Error(t, err)
	require.Contains(t, err.Error(), "illegal string",
		"malformed decimal literal should surface the parser's illegal-string error, not a Decimal256 range error; got %v", err)
	require.NotContains(t, err.Error(), "Decimal256",
		"malformed decimal literal should not have been retried against Decimal256")
}

func TestDefaultBinderUntypedDecimalLiteralUsesDecimal256(t *testing.T) {
	binder := NewDefaultBinder(context.Background(), nil, nil, plan.Type{}, nil)
	decimal := "123456789012345678901234567890123456789"
	expr, err := binder.BindExpr(tree.NewNumVal(decimal, decimal, false, tree.P_decimal), 0, true)
	require.NoError(t, err)
	require.Equal(t, int32(types.T_decimal256), expr.Typ.Id)
	require.Equal(t, int32(65), expr.Typ.Width)
	require.Equal(t, int32(0), expr.Typ.Scale)
}

func TestSetDefaultAndOnUpdateUseSetMembers(t *testing.T) {
	proc := testutil.NewProcess(t)
	setType := plan.Type{Id: int32(types.T_uint64), Enumvalues: "read,write"}

	defaultCol := tree.NewColumnTableDef(
		tree.NewUnresolvedColName("perm"),
		nil,
		[]tree.ColumnAttribute{
			&tree.AttributeDefault{Expr: tree.NewNumVal("read,write", "read,write", false, tree.P_char)},
		},
	)
	defaultValue, err := buildDefaultExpr(defaultCol, setType, proc)
	require.NoError(t, err)
	require.Equal(t, uint64(3), defaultValue.Expr.GetLit().GetU64Val())

	onUpdateCol := tree.NewColumnTableDef(
		tree.NewUnresolvedColName("perm"),
		nil,
		[]tree.ColumnAttribute{
			&tree.AttributeOnUpdate{Expr: tree.NewNumVal("write", "write", false, tree.P_char)},
		},
	)
	onUpdate, err := buildOnUpdate(onUpdateCol, setType, proc)
	require.NoError(t, err)
	require.NotNil(t, onUpdate.Expr)
}

func TestBuildDefaultExprCoversErrorBranches(t *testing.T) {
	proc := testutil.NewProcess(t)

	// No default attribute at all: non-null column with nullAbility=true.
	noDefaultCol := tree.NewColumnTableDef(
		tree.NewUnresolvedColName("c"),
		nil,
		nil,
	)
	defaultVal, err := buildDefaultExpr(noDefaultCol, plan.Type{Id: int32(types.T_int32)}, proc)
	require.NoError(t, err)
	require.Nil(t, defaultVal.Expr)
	require.True(t, defaultVal.NullAbility)

	// JSON column with explicit default value is rejected.
	jsonCol := tree.NewColumnTableDef(
		tree.NewUnresolvedColName("c"),
		nil,
		[]tree.ColumnAttribute{
			&tree.AttributeDefault{Expr: tree.NewNumVal("{}", "{}", false, tree.P_char)},
		},
	)
	_, err = buildDefaultExpr(jsonCol, plan.Type{Id: int32(types.T_json)}, proc)
	require.Error(t, err)

	// NOT NULL column with an explicit NULL default is rejected.
	notNullCol := tree.NewColumnTableDef(
		tree.NewUnresolvedColName("c"),
		nil,
		[]tree.ColumnAttribute{
			&tree.AttributeNull{Is: false},
			&tree.AttributeDefault{Expr: tree.NewNumVal("null", "null", false, tree.P_null)},
		},
	)
	_, err = buildDefaultExpr(notNullCol, plan.Type{Id: int32(types.T_int32)}, proc)
	require.Error(t, err)

	// uuid() default on a non-UUID column is rejected.
	uuidCol := tree.NewColumnTableDef(
		tree.NewUnresolvedColName("c"),
		nil,
		[]tree.ColumnAttribute{
			&tree.AttributeDefault{Expr: &tree.FuncExpr{
				Func:  tree.FuncName2ResolvableFunctionReference(tree.NewUnresolvedColName("uuid")),
				Exprs: tree.Exprs{},
			}},
		},
	)
	_, err = buildDefaultExpr(uuidCol, plan.Type{Id: int32(types.T_varchar), Width: 36}, proc)
	require.Error(t, err)
}

func TestBuildOnUpdateNoAttribute(t *testing.T) {
	proc := testutil.NewProcess(t)

	noAttrCol := tree.NewColumnTableDef(
		tree.NewUnresolvedColName("c"),
		nil,
		nil,
	)
	onUpdate, err := buildOnUpdate(noAttrCol, plan.Type{Id: int32(types.T_int32)}, proc)
	require.NoError(t, err)
	require.Nil(t, onUpdate)
}

func TestSetDefaultAndOnUpdateFoldNonLiteral(t *testing.T) {
	proc := testutil.NewProcess(t)
	setType := plan.Type{Id: int32(types.T_uint64), Enumvalues: "read,write,execute"}

	// Default: concat('read','') folds to the literal "read" and should resolve
	// against the SET members, giving bit 0b001.
	defaultCol := tree.NewColumnTableDef(
		tree.NewUnresolvedColName("perm"),
		nil,
		[]tree.ColumnAttribute{
			&tree.AttributeDefault{Expr: &tree.FuncExpr{
				Func: tree.FuncName2ResolvableFunctionReference(tree.NewUnresolvedColName("concat")),
				Exprs: tree.Exprs{
					tree.NewNumVal("read", "read", false, tree.P_char),
					tree.NewNumVal("", "", false, tree.P_char),
				},
			}},
		},
	)
	d, err := buildDefaultExpr(defaultCol, setType, proc)
	require.NoError(t, err)
	require.Equal(t, uint64(1), d.Expr.GetLit().GetU64Val(),
		"folded non-literal default should be resolved by SET members")

	// Default: concat('read','qq') folds to "readqq" which is not a set member;
	// should be rejected as an invalid default instead of silently cast to 0.
	badCol := tree.NewColumnTableDef(
		tree.NewUnresolvedColName("perm"),
		nil,
		[]tree.ColumnAttribute{
			&tree.AttributeDefault{Expr: &tree.FuncExpr{
				Func: tree.FuncName2ResolvableFunctionReference(tree.NewUnresolvedColName("concat")),
				Exprs: tree.Exprs{
					tree.NewNumVal("read", "read", false, tree.P_char),
					tree.NewNumVal("qq", "qq", false, tree.P_char),
				},
			}},
		},
	)
	_, err = buildDefaultExpr(badCol, setType, proc)
	require.Error(t, err)

	// On update: concat('read,write','') folds to "read,write", bits 0b011.
	onUpdateCol := tree.NewColumnTableDef(
		tree.NewUnresolvedColName("perm"),
		nil,
		[]tree.ColumnAttribute{
			&tree.AttributeOnUpdate{Expr: &tree.FuncExpr{
				Func: tree.FuncName2ResolvableFunctionReference(tree.NewUnresolvedColName("concat")),
				Exprs: tree.Exprs{
					tree.NewNumVal("read,write", "read,write", false, tree.P_char),
					tree.NewNumVal("", "", false, tree.P_char),
				},
			}},
		},
	)
	u, err := buildOnUpdate(onUpdateCol, setType, proc)
	require.NoError(t, err)
	require.Equal(t, uint64(3), u.Expr.GetLit().GetU64Val())
}

func TestSetDefaultRejectsSignedNumericOverflow(t *testing.T) {
	setType := plan.Type{Id: int32(types.T_uint64), Enumvalues: "read,write"}
	expr := makePlan2Int64ConstExprWithType(8)
	_, err := funcCastForSetType(context.Background(), expr, setType)
	require.Error(t, err)

	expr = makePlan2Int64ConstExprWithType(-1)
	_, err = funcCastForSetType(context.Background(), expr, setType)
	require.Error(t, err)

	expr = makePlan2Int64ConstExprWithType(3)
	casted, err := funcCastForSetType(context.Background(), expr, setType)
	require.NoError(t, err)
	require.Equal(t, uint64(3), casted.GetLit().GetU64Val())
}

func TestFuncCastForSetTypeCoversAllLiteralShapes(t *testing.T) {
	setType := plan.Type{Id: int32(types.T_uint64), Enumvalues: "read,write,execute"}

	// string -> ParseSet
	out, err := funcCastForSetType(context.Background(), makePlan2StringConstExprWithType("read,execute"), setType)
	require.NoError(t, err)
	require.Equal(t, uint64(5), out.GetLit().GetU64Val())

	// u64 value
	out, err = funcCastForSetType(context.Background(), makePlan2Uint64ConstExprWithType(3), setType)
	require.NoError(t, err)
	require.Equal(t, uint64(3), out.GetLit().GetU64Val())

	// narrower unsigned ints (uint8/16/32)
	out, err = funcCastForSetType(context.Background(), makePlan2Uint8ConstExprWithType(1), setType)
	require.NoError(t, err)
	require.Equal(t, uint64(1), out.GetLit().GetU64Val())
	out, err = funcCastForSetType(context.Background(), makePlan2Uint16ConstExprWithType(2), setType)
	require.NoError(t, err)
	require.Equal(t, uint64(2), out.GetLit().GetU64Val())
	out, err = funcCastForSetType(context.Background(), makePlan2Uint32ConstExprWithType(4), setType)
	require.NoError(t, err)
	require.Equal(t, uint64(4), out.GetLit().GetU64Val())

	// signed narrower ints
	out, err = funcCastForSetType(context.Background(), makePlan2Int8ConstExprWithType(1), setType)
	require.NoError(t, err)
	require.Equal(t, uint64(1), out.GetLit().GetU64Val())
	out, err = funcCastForSetType(context.Background(), makePlan2Int16ConstExprWithType(2), setType)
	require.NoError(t, err)
	require.Equal(t, uint64(2), out.GetLit().GetU64Val())
	out, err = funcCastForSetType(context.Background(), makePlan2Int32ConstExprWithType(4), setType)
	require.NoError(t, err)
	require.Equal(t, uint64(4), out.GetLit().GetU64Val())

	// non-set type returns the expression untouched
	intType := plan.Type{Id: int32(types.T_int64)}
	pass, err := funcCastForSetType(context.Background(), makePlan2Int64ConstExprWithType(5), intType)
	require.NoError(t, err)
	require.Equal(t, int64(5), pass.GetLit().GetI64Val())

	// null literal passes through unchanged
	nullExpr := &plan.Expr{Typ: setType, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Isnull: true}}}
	passNull, err := funcCastForSetType(context.Background(), nullExpr, setType)
	require.NoError(t, err)
	require.True(t, passNull.GetLit().Isnull)

	// out-of-range value bubbles up an error
	_, err = funcCastForSetType(context.Background(), makePlan2Uint64ConstExprWithType(16), setType)
	require.Error(t, err)
}

func TestSetEmptyMemberFormatting(t *testing.T) {
	setValues, err := types.NormalizeSetValues([]string{"", "a"})
	require.NoError(t, err)
	encoded := types.EncodeSetValues(setValues)
	require.NotEqual(t, ",a", encoded)

	setType := plan.Type{Id: int32(types.T_uint64), Enumvalues: encoded}
	require.Equal(t, "SET('','a')", FormatColType(setType))
}
