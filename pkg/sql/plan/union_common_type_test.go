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

package plan

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/stretchr/testify/require"
)

func TestUnionDecimalLiteralCommonType(t *testing.T) {
	for _, test := range []struct {
		name     string
		sql      string
		oid      types.T
		width    int32
		scale    int32
		nullable bool
	}{
		{
			name:  "small positive integer literal",
			sql:   "select 1 as x union all select 2.5 as x",
			oid:   types.T_decimal64,
			width: 2,
			scale: 1,
		},
		{
			name:  "negative integer literal",
			sql:   "select -123 as x union select 2.50 as x",
			oid:   types.T_decimal64,
			width: 5,
			scale: 2,
		},
		{
			name:  "bigint boundary literal",
			sql:   "select 9223372036854775807 as x union all select 0.1 as x",
			oid:   types.T_decimal128,
			width: 20,
			scale: 1,
		},
		{
			name:  "explicit signed domain is not narrowed",
			sql:   "select cast(1 as signed) as x union all select 2.5 as x",
			oid:   types.T_decimal128,
			width: 20,
			scale: 1,
		},
		{
			name:     "pure null remains neutral and nullable",
			sql:      "select 1 as x union all select null union all select 2.5",
			oid:      types.T_decimal64,
			width:    2,
			scale:    1,
			nullable: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			typ := buildFirstQueryResultType(t, test.sql)
			require.Equal(t, int32(test.oid), typ.Id)
			require.Equal(t, test.width, typ.Width)
			require.Equal(t, test.scale, typ.Scale)
			require.Equal(t, !test.nullable, typ.NotNullable)
		})
	}
}

func TestUnionSignedUnsignedIntegerCommonType(t *testing.T) {
	for _, test := range []struct {
		name string
		sql  string
	}{
		{
			name: "union all signed first",
			sql:  "select cast(-1 as signed) as x union all select cast(18446744073709551615 as unsigned) as x",
		},
		{
			name: "union all unsigned first",
			sql:  "select cast(18446744073709551615 as unsigned) as x union all select cast(-1 as signed) as x",
		},
		{
			name: "union distinct",
			sql:  "select cast(-1 as signed) as x union select cast(18446744073709551615 as unsigned) as x",
		},
		{
			name: "intersect",
			sql:  "select cast(-1 as signed) as x intersect select cast(18446744073709551615 as unsigned) as x",
		},
		{
			name: "intersect all",
			sql:  "select cast(-1 as signed) as x intersect all select cast(18446744073709551615 as unsigned) as x",
		},
		{
			name: "except",
			sql:  "select cast(-1 as signed) as x except select cast(18446744073709551615 as unsigned) as x",
		},
		{
			name: "minus",
			sql:  "select cast(-1 as signed) as x minus select cast(18446744073709551615 as unsigned) as x",
		},
		{
			name: "null branch",
			sql:  "select null as x union all select cast(-1 as signed) as x union all select cast(18446744073709551615 as unsigned) as x",
		},
		{
			name: "direct coalesce",
			sql:  "select coalesce(cast(-1 as signed), cast(18446744073709551615 as unsigned)) as x",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			typ := buildFirstQueryResultType(t, test.sql)
			require.Equal(t, int32(types.T_decimal128), typ.Id)
			require.Equal(t, int32(20), typ.Width)
			require.Zero(t, typ.Scale)
			if test.name == "null branch" {
				require.False(t, typ.NotNullable)
			} else {
				require.True(t, typ.NotNullable)
			}
		})
	}
}

func TestUnionSignedUnsignedIntegerColumnCommonType(t *testing.T) {
	const tableName = "union_signed_unsigned_columns"
	ctx := NewMockCompilerContext(true)
	ctx.dbs["test"] = true
	ctx.objects[tableName] = &planpb.ObjectRef{DbName: "test", ObjName: tableName, Obj: 1}
	ctx.tables[tableName] = &planpb.TableDef{
		Name:   tableName,
		DbName: "test",
		Cols: []*planpb.ColDef{
			{Name: "s", Typ: planpb.Type{Id: int32(types.T_int64)}},
			{Name: "u", Typ: planpb.Type{Id: int32(types.T_uint64)}},
		},
	}

	stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL,
		"select s as x from test.union_signed_unsigned_columns union all select u from test.union_signed_unsigned_columns", 1)
	require.NoError(t, err)
	defer stmt.Free()

	logicPlan, err := BuildPlan(ctx, stmt, false)
	require.NoError(t, err)
	query := logicPlan.GetQuery()
	require.NotEmpty(t, query.Steps)

	var setNode *planpb.Node
	for _, node := range query.Nodes {
		if node.NodeType == planpb.Node_UNION_ALL {
			setNode = node
			break
		}
	}
	require.NotNil(t, setNode)
	require.Len(t, setNode.ProjectList, 1)
	require.Equal(t, int32(types.T_decimal128), setNode.ProjectList[0].Typ.Id)
	require.Equal(t, int32(20), setNode.ProjectList[0].Typ.Width)
	require.Zero(t, setNode.ProjectList[0].Typ.Scale)
	require.False(t, setNode.ProjectList[0].Typ.NotNullable)

	root := query.Nodes[query.Steps[len(query.Steps)-1]]
	require.NotEmpty(t, root.ProjectList)
	require.Equal(t, int32(types.T_decimal128), root.ProjectList[0].Typ.Id)
	require.Equal(t, int32(20), root.ProjectList[0].Typ.Width)
	require.Zero(t, root.ProjectList[0].Typ.Scale)
	require.False(t, root.ProjectList[0].Typ.NotNullable)
}

func TestCTASUnionSignedUnsignedIntegerMetadata(t *testing.T) {
	stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL,
		"create table t_ctas_union_signed_unsigned as select cast(-1 as signed) as x union all select cast(18446744073709551615 as unsigned) as x", 1)
	require.NoError(t, err)
	defer stmt.Free()

	logicPlan, err := BuildPlan(NewMockCompilerContext(true), stmt, false)
	require.NoError(t, err)

	var visible []*planpb.ColDef
	for _, col := range logicPlan.GetDdl().GetCreateTable().GetTableDef().GetCols() {
		if !col.Hidden {
			visible = append(visible, col)
		}
	}
	require.Len(t, visible, 1)
	require.Equal(t, "x", visible[0].Name)
	require.Equal(t, int32(types.T_decimal128), visible[0].Typ.Id)
	require.Equal(t, int32(20), visible[0].Typ.Width)
	require.Zero(t, visible[0].Typ.Scale)
	require.True(t, visible[0].Typ.NotNullable)
}

func TestCTASUnionDecimalLiteralMetadata(t *testing.T) {
	stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL,
		"create table t_ctas_union_decimal as select 1 as x union all select 2.5 as x", 1)
	require.NoError(t, err)
	defer stmt.Free()

	logicPlan, err := BuildPlan(NewMockCompilerContext(true), stmt, false)
	require.NoError(t, err)

	var visible []*planpb.ColDef
	for _, col := range logicPlan.GetDdl().GetCreateTable().GetTableDef().GetCols() {
		if !col.Hidden {
			visible = append(visible, col)
		}
	}
	require.Len(t, visible, 1)
	require.Equal(t, "x", visible[0].Name)
	require.Equal(t, int32(types.T_decimal64), visible[0].Typ.Id)
	require.Equal(t, int32(2), visible[0].Typ.Width)
	require.Equal(t, int32(1), visible[0].Typ.Scale)
	require.True(t, visible[0].Typ.NotNullable)
	require.NotNil(t, visible[0].Default)
	require.False(t, visible[0].Default.NullAbility)
}

func TestSetOperationIntegerLiteralDecimalType(t *testing.T) {
	literalExpr := func(literal *planpb.Literal) *planpb.Expr {
		return &planpb.Expr{Expr: &planpb.Expr_Lit{Lit: literal}}
	}

	for _, test := range []struct {
		name  string
		expr  *planpb.Expr
		oid   types.T
		width int32
		ok    bool
	}{
		{
			name:  "int8",
			expr:  literalExpr(&planpb.Literal{Value: &planpb.Literal_I8Val{I8Val: -12}}),
			oid:   types.T_decimal64,
			width: 2,
			ok:    true,
		},
		{
			name:  "int16",
			expr:  literalExpr(&planpb.Literal{Value: &planpb.Literal_I16Val{I16Val: -123}}),
			oid:   types.T_decimal64,
			width: 3,
			ok:    true,
		},
		{
			name:  "int32",
			expr:  literalExpr(&planpb.Literal{Value: &planpb.Literal_I32Val{I32Val: 12345}}),
			oid:   types.T_decimal64,
			width: 5,
			ok:    true,
		},
		{
			name: "int64 minimum",
			expr: literalExpr(&planpb.Literal{Value: &planpb.Literal_I64Val{
				I64Val: int64(-9223372036854775807 - 1),
			}}),
			oid:   types.T_decimal128,
			width: 19,
			ok:    true,
		},
		{
			name:  "uint8",
			expr:  literalExpr(&planpb.Literal{Value: &planpb.Literal_U8Val{U8Val: 255}}),
			oid:   types.T_decimal64,
			width: 3,
			ok:    true,
		},
		{
			name:  "uint16",
			expr:  literalExpr(&planpb.Literal{Value: &planpb.Literal_U16Val{U16Val: 65535}}),
			oid:   types.T_decimal64,
			width: 5,
			ok:    true,
		},
		{
			name:  "uint32",
			expr:  literalExpr(&planpb.Literal{Value: &planpb.Literal_U32Val{U32Val: 4294967295}}),
			oid:   types.T_decimal64,
			width: 10,
			ok:    true,
		},
		{
			name: "uint64 maximum",
			expr: literalExpr(&planpb.Literal{Value: &planpb.Literal_U64Val{
				U64Val: uint64(18446744073709551615),
			}}),
			oid:   types.T_decimal128,
			width: 20,
			ok:    true,
		},
		{
			name: "nested unary operators",
			expr: &planpb.Expr{Expr: &planpb.Expr_F{F: &planpb.Function{
				Func: &planpb.ObjectRef{ObjName: "unary_plus"},
				Args: []*planpb.Expr{{Expr: &planpb.Expr_F{F: &planpb.Function{
					Func: &planpb.ObjectRef{ObjName: "unary_minus"},
					Args: []*planpb.Expr{literalExpr(&planpb.Literal{Value: &planpb.Literal_I8Val{I8Val: 7}})},
				}}}},
			}}},
			oid:   types.T_decimal64,
			width: 1,
			ok:    true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			actual, ok := setOperationIntegerLiteralDecimalType(test.expr)
			require.Equal(t, test.ok, ok)
			require.Equal(t, types.New(test.oid, test.width, 0), actual)
		})
	}

	invalidExprs := []*planpb.Expr{
		nil,
		{},
		literalExpr(nil),
		literalExpr(&planpb.Literal{Isnull: true}),
		literalExpr(&planpb.Literal{Value: &planpb.Literal_Sval{Sval: "1"}}),
		{Expr: &planpb.Expr_F{F: &planpb.Function{}}},
		{Expr: &planpb.Expr_F{F: &planpb.Function{Func: &planpb.ObjectRef{ObjName: "unary_plus"}}}},
		{Expr: &planpb.Expr_F{F: &planpb.Function{
			Func: &planpb.ObjectRef{ObjName: "unary_plus"},
			Args: []*planpb.Expr{nil},
		}}},
		{Expr: &planpb.Expr_F{F: &planpb.Function{
			Func: &planpb.ObjectRef{ObjName: "abs"},
			Args: []*planpb.Expr{literalExpr(&planpb.Literal{Value: &planpb.Literal_I8Val{I8Val: 1}})},
		}}},
	}
	for i, expr := range invalidExprs {
		actual, ok := setOperationIntegerLiteralDecimalType(expr)
		require.False(t, ok, "invalid expression %d", i)
		require.Equal(t, types.Type{}, actual)
	}
}

func buildFirstQueryResultType(t *testing.T, sql string) planpb.Type {
	t.Helper()
	stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, sql, 1)
	require.NoError(t, err)
	defer stmt.Free()

	logicPlan, err := BuildPlan(NewMockCompilerContext(true), stmt, false)
	require.NoError(t, err)
	query := logicPlan.GetQuery()
	require.NotEmpty(t, query.Steps)
	root := query.Nodes[query.Steps[len(query.Steps)-1]]
	require.NotEmpty(t, root.ProjectList)
	return root.ProjectList[0].Typ
}
