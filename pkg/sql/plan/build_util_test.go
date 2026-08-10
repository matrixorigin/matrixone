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
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
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

// TestRewriteCountNotNullColToStarcount ensures plan-level rewrite sets both ObjName and Obj
// so runtime uses countStarExec; regression test for count(not_null_col) performance fix.
func TestRewriteCountNotNullColToStarcount(t *testing.T) {
	wantObj := function.EncodeOverloadID(int32(function.STARCOUNT), 0)

	node := &plan.Node{
		AggList: []*plan.Expr{
			{
				Expr: &plan.Expr_F{
					F: &plan.Function{
						Func: &plan.ObjectRef{ObjName: "count"},
						Args: []*plan.Expr{
							{Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}}},
						},
					},
				},
			},
		},
	}
	tableDef := &plan.TableDef{
		Cols: []*plan.ColDef{
			{Name: "a", Typ: plan.Type{NotNullable: true}},
		},
	}

	RewriteCountNotNullColToStarcount(node, tableDef)

	agg := node.AggList[0].GetF()
	require.NotNil(t, agg)
	require.NotNil(t, agg.Func)
	assert.Equal(t, "starcount", agg.Func.ObjName, "ObjName must be starcount so compile treats as single starcount")
	assert.Equal(t, wantObj, agg.Func.Obj, "Obj must be CountStar overload so runtime uses countStarExec")
}

func TestConvertCharBinaryTypeResolution(t *testing.T) {
	cases := []struct {
		sql   string
		oid   types.T
		width int32
		scale int32
	}{
		{
			sql:   "select convert(12345, char)",
			oid:   types.T_varchar,
			width: types.MaxVarcharLen,
		},
		{
			sql:   "select convert('ABCDE', char)",
			oid:   types.T_varchar,
			width: types.MaxVarcharLen,
		},
		{
			sql:   "select convert(12345, char(3))",
			oid:   types.T_char,
			width: 3,
		},
		{
			sql:   "select convert('AZ', binary)",
			oid:   types.T_binary,
			width: -1,
			scale: -1,
		},
		{
			sql:   "select convert('AZ', binary(1))",
			oid:   types.T_binary,
			width: 1,
			scale: -1,
		},
		{
			sql:   "select convert(12345, binary(3))",
			oid:   types.T_binary,
			width: 3,
			scale: -1,
		},
	}

	for _, tc := range cases {
		t.Run(tc.sql, func(t *testing.T) {
			logicPlan, err := runOneStmt(NewMockOptimizer(false), t, tc.sql)
			require.NoError(t, err)
			require.NotNil(t, logicPlan.GetQuery())

			var expr *plan.Expr
			for _, node := range logicPlan.GetQuery().Nodes {
				if node.NodeType == plan.Node_PROJECT && len(node.ProjectList) > 0 {
					expr = node.ProjectList[0]
					break
				}
			}
			require.NotNil(t, expr)
			require.Equal(t, int32(tc.oid), expr.Typ.Id)
			require.Equal(t, tc.width, expr.Typ.Width)
			require.Equal(t, tc.scale, expr.Typ.Scale)
		})
	}
}

func TestGetTypeFromAstAssignsExplicitStringCharset(t *testing.T) {
	testCases := []struct {
		definition  string
		wantOID     types.T
		wantCharset uint32
	}{
		{definition: "char(10)", wantOID: types.T_char, wantCharset: uint32(types.CharsetUTF8)},
		{definition: "varchar(10)", wantOID: types.T_varchar, wantCharset: uint32(types.CharsetUTF8)},
		{definition: "text", wantOID: types.T_text, wantCharset: uint32(types.CharsetUTF8)},
		{definition: "binary(10)", wantOID: types.T_binary, wantCharset: uint32(types.CharsetBinary)},
		{definition: "varbinary(10)", wantOID: types.T_varbinary, wantCharset: uint32(types.CharsetBinary)},
		{definition: "blob", wantOID: types.T_blob, wantCharset: uint32(types.CharsetBinary)},
	}

	for _, testCase := range testCases {
		t.Run(testCase.definition, func(t *testing.T) {
			stmt, err := mysql.ParseOne(context.Background(),
				"create table t (v "+testCase.definition+")", 1)
			require.NoError(t, err)
			defer stmt.Free()

			column := stmt.(*tree.CreateTable).Defs[0].(*tree.ColumnTableDef)
			typ, err := getTypeFromAst(context.Background(), column.Type)
			require.NoError(t, err)
			require.Equal(t, int32(testCase.wantOID), typ.Id)
			require.Equal(t, testCase.wantCharset, typ.Charset)
		})
	}
}

func TestGetTypeFromAstGeometrySubtype(t *testing.T) {
	stmt, err := mysql.ParseOne(context.Background(), "create table t (g point)", 1)
	require.NoError(t, err)

	createTable, ok := stmt.(*tree.CreateTable)
	require.True(t, ok)
	require.Len(t, createTable.Defs, 1)

	colDef, ok := createTable.Defs[0].(*tree.ColumnTableDef)
	require.True(t, ok)

	typ, err := getTypeFromAst(context.Background(), colDef.Type)
	require.NoError(t, err)
	require.Equal(t, int32(types.T_geometry), typ.Id)
	require.Equal(t, "POINT", geometrySubtypeName(&typ))
	require.NoError(t, applyColumnAttributesToType(context.Background(), &typ, colDef.Attributes))
	require.Equal(t, "POINT", geometrySubtypeName(&typ))
	_, sridDefined := geometrySRIDValue(&typ)
	require.False(t, sridDefined)

	stmt, err = mysql.ParseOne(context.Background(), "create table t (g geometry)", 1)
	require.NoError(t, err)
	createTable, ok = stmt.(*tree.CreateTable)
	require.True(t, ok)
	colDef, ok = createTable.Defs[0].(*tree.ColumnTableDef)
	require.True(t, ok)

	typ, err = getTypeFromAst(context.Background(), colDef.Type)
	require.NoError(t, err)
	require.Equal(t, int32(types.T_geometry), typ.Id)
	require.Equal(t, "", geometrySubtypeName(&typ))
	_, sridDefined = geometrySRIDValue(&typ)
	require.False(t, sridDefined)

	stmt, err = mysql.ParseOne(context.Background(), "create table t (g point srid 4326)", 1)
	require.NoError(t, err)
	createTable, ok = stmt.(*tree.CreateTable)
	require.True(t, ok)
	colDef, ok = createTable.Defs[0].(*tree.ColumnTableDef)
	require.True(t, ok)

	typ, err = getTypeFromAst(context.Background(), colDef.Type)
	require.NoError(t, err)
	require.NoError(t, applyColumnAttributesToType(context.Background(), &typ, colDef.Attributes))
	require.Equal(t, "POINT", geometrySubtypeName(&typ))
	srid, sridDefined := geometrySRIDValue(&typ)
	require.True(t, sridDefined)
	require.Equal(t, uint32(4326), srid)

	stmt, err = mysql.ParseOne(context.Background(), "create table t (g geometry srid 0)", 1)
	require.NoError(t, err)
	createTable, ok = stmt.(*tree.CreateTable)
	require.True(t, ok)
	colDef, ok = createTable.Defs[0].(*tree.ColumnTableDef)
	require.True(t, ok)

	typ, err = getTypeFromAst(context.Background(), colDef.Type)
	require.NoError(t, err)
	require.NoError(t, applyColumnAttributesToType(context.Background(), &typ, colDef.Attributes))
	require.Equal(t, "", geometrySubtypeName(&typ))
	srid, sridDefined = geometrySRIDValue(&typ)
	require.True(t, sridDefined)
	require.Equal(t, uint32(0), srid)
}

func TestGetTypeFromAstGeometryAliases(t *testing.T) {
	cases := []struct {
		col         string
		wantOid     types.T
		wantSubtype string
		wantSRID    uint32
		sridDefined bool
	}{
		{"point", types.T_geometry, "POINT", 0, false},
		{"geometry32", types.T_geometry32, "", 0, false},
		{"point32", types.T_geometry32, "POINT", 0, false},
		{"geography", types.T_geometry, "", 4326, true},
		{"geography32", types.T_geometry32, "", 4326, true},
		{"multipolygon32", types.T_geometry32, "MULTIPOLYGON", 0, false},
	}
	for _, c := range cases {
		t.Run(c.col, func(t *testing.T) {
			stmt, err := mysql.ParseOne(context.Background(), "create table t (g "+c.col+")", 1)
			require.NoError(t, err)
			createTable := stmt.(*tree.CreateTable)
			colDef := createTable.Defs[0].(*tree.ColumnTableDef)

			typ, err := getTypeFromAst(context.Background(), colDef.Type)
			require.NoError(t, err)
			require.Equal(t, int32(c.wantOid), typ.Id)
			require.Equal(t, c.wantSubtype, geometrySubtypeName(&typ))
			srid, defined := geometrySRIDValue(&typ)
			require.Equal(t, c.sridDefined, defined)
			if c.sridDefined {
				require.Equal(t, c.wantSRID, srid)
			}
		})
	}
}

func TestGetTypeFromAstLongStringAliases(t *testing.T) {
	tests := []struct {
		typeSQL string
		want    types.T
	}{
		{typeSQL: "long varchar", want: types.T_text},
		{typeSQL: "long varbinary", want: types.T_blob},
	}

	for _, test := range tests {
		t.Run(test.typeSQL, func(t *testing.T) {
			stmt, err := mysql.ParseOne(context.Background(), "create table t (value "+test.typeSQL+")", 1)
			require.NoError(t, err)
			defer stmt.Free()

			createTable, ok := stmt.(*tree.CreateTable)
			require.True(t, ok)
			require.Len(t, createTable.Defs, 1)
			colDef, ok := createTable.Defs[0].(*tree.ColumnTableDef)
			require.True(t, ok)

			typ, err := getTypeFromAst(context.Background(), colDef.Type)
			require.NoError(t, err)
			require.Equal(t, int32(test.want), typ.Id)
		})
	}
}

func TestGetTypeFromAstArrayAsJson(t *testing.T) {
	stmt, err := mysql.ParseOne(context.Background(), "create table t (tags array(varchar(20)))", 1)
	require.NoError(t, err)

	createTable, ok := stmt.(*tree.CreateTable)
	require.True(t, ok)
	require.Len(t, createTable.Defs, 1)
	colDef, ok := createTable.Defs[0].(*tree.ColumnTableDef)
	require.True(t, ok)

	typ, err := getTypeFromAst(context.Background(), colDef.Type)
	require.NoError(t, err)
	require.Equal(t, int32(types.T_json), typ.Id)
	require.Equal(t, "array(varchar(20))", typ.Enumvalues)
}

func TestGetTypeFromAstTinyTextPreservesByteLimit(t *testing.T) {
	stmt, err := mysql.ParseOne(context.Background(), "create table t (value tinytext)", 1)
	require.NoError(t, err)

	createTable := stmt.(*tree.CreateTable)
	colDef := createTable.Defs[0].(*tree.ColumnTableDef)
	typ, err := getTypeFromAst(context.Background(), colDef.Type)
	require.NoError(t, err)
	require.Equal(t, int32(types.T_text), typ.Id)
	require.Equal(t, int32(types.MaxTinyTextLen), typ.Width)
}

func TestGetTypeFromAstArrayValidatesElementType(t *testing.T) {
	stmt, err := mysql.ParseOne(context.Background(), "create table t (tags array(varchar(16777217)))", 1)
	require.NoError(t, err)

	createTable, ok := stmt.(*tree.CreateTable)
	require.True(t, ok)
	colDef, ok := createTable.Defs[0].(*tree.ColumnTableDef)
	require.True(t, ok)

	_, err = getTypeFromAst(context.Background(), colDef.Type)
	require.Error(t, err)
	require.Contains(t, err.Error(), "typeLen is over the MaxVarcharLen")
}

func TestGetTypeFromAstArrayRejectsUnsupportedElementType(t *testing.T) {
	tests := []string{
		"create table t (tags array(bit))",
		"create table t (tags array(enum('a','b')))",
		"create table t (tags array(vecf32(3)))",
		"create table t (tags array(array(bit)))",
	}

	for _, sql := range tests {
		t.Run(sql, func(t *testing.T) {
			stmt, err := mysql.ParseOne(context.Background(), sql, 1)
			require.NoError(t, err)

			createTable, ok := stmt.(*tree.CreateTable)
			require.True(t, ok)
			colDef, ok := createTable.Defs[0].(*tree.ColumnTableDef)
			require.True(t, ok)

			_, err = getTypeFromAst(context.Background(), colDef.Type)
			require.Error(t, err)
			require.Contains(t, err.Error(), "unsupported ARRAY element type")
		})
	}
}

func TestApplyColumnAttributesToTypeRejectsNonGeometrySRID(t *testing.T) {
	tests := []string{
		"create table t (a int srid 4326)",
		"create table t (a varchar(20) srid 4326)",
		"create table t (a decimal(10,2) srid 4326)",
	}

	for _, sql := range tests {
		t.Run(sql, func(t *testing.T) {
			stmt, err := mysql.ParseOne(context.Background(), sql, 1)
			require.NoError(t, err)

			createTable, ok := stmt.(*tree.CreateTable)
			require.True(t, ok)
			colDef, ok := createTable.Defs[0].(*tree.ColumnTableDef)
			require.True(t, ok)

			typ, err := getTypeFromAst(context.Background(), colDef.Type)
			require.NoError(t, err)
			err = applyColumnAttributesToType(context.Background(), &typ, colDef.Attributes)
			require.Error(t, err)
			require.Contains(t, err.Error(), "SRID is only supported for GEOMETRY columns")
		})
	}
}

func TestBuildDefaultExprGeometryDisallowsNonNullDefault(t *testing.T) {
	proc := testutil.NewProcess(t)

	stmt, err := mysql.ParseOne(context.Background(), "create table t (g geometry default 'POINT(1 1)')", 1)
	require.NoError(t, err)

	createTable, ok := stmt.(*tree.CreateTable)
	require.True(t, ok)
	colDef, ok := createTable.Defs[0].(*tree.ColumnTableDef)
	require.True(t, ok)

	typ, err := getTypeFromAst(context.Background(), colDef.Type)
	require.NoError(t, err)

	_, err = buildDefaultExpr(colDef, typ, proc)
	require.Error(t, err)
	require.Contains(t, err.Error(), "GEOMETRY column 'g' cannot have default value")
}

func TestBuildDefaultExprGeometryAllowsNullDefault(t *testing.T) {
	proc := testutil.NewProcess(t)

	stmt, err := mysql.ParseOne(context.Background(), "create table t (g geometry default null)", 1)
	require.NoError(t, err)

	createTable, ok := stmt.(*tree.CreateTable)
	require.True(t, ok)
	colDef, ok := createTable.Defs[0].(*tree.ColumnTableDef)
	require.True(t, ok)

	typ, err := getTypeFromAst(context.Background(), colDef.Type)
	require.NoError(t, err)

	def, err := buildDefaultExpr(colDef, typ, proc)
	require.NoError(t, err)
	require.NotNil(t, def)
}

func TestBuildDefaultExprParenthesizedNullMatchesNullDefault(t *testing.T) {
	proc := testutil.NewProcess(t)

	tests := []struct {
		name    string
		sql     string
		wantErr string
	}{
		{
			name:    "not null rejects parenthesized null",
			sql:     "create table t (a int not null default (null))",
			wantErr: "invalid default value for column 'a'",
		},
		{
			name: "json allows parenthesized null",
			sql:  "create table t (j json default (null))",
		},
		{
			name: "geometry allows parenthesized null",
			sql:  "create table t (g geometry default (null))",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := mysql.ParseOne(context.Background(), tt.sql, 1)
			require.NoError(t, err)

			createTable, ok := stmt.(*tree.CreateTable)
			require.True(t, ok)
			colDef, ok := createTable.Defs[0].(*tree.ColumnTableDef)
			require.True(t, ok)

			typ, err := getTypeFromAst(context.Background(), colDef.Type)
			require.NoError(t, err)

			def, err := buildDefaultExpr(colDef, typ, proc)
			if tt.wantErr != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, def)
		})
	}
}

func TestBuildDefaultExprAllowsParenthesizedUuidForStringDefault(t *testing.T) {
	proc := testutil.NewProcess(t)

	stmt, err := mysql.ParseOne(context.Background(), "create table t (id varchar(191) not null default (uuid()))", 1)
	require.NoError(t, err)

	createTable, ok := stmt.(*tree.CreateTable)
	require.True(t, ok)
	colDef, ok := createTable.Defs[0].(*tree.ColumnTableDef)
	require.True(t, ok)

	typ, err := getTypeFromAst(context.Background(), colDef.Type)
	require.NoError(t, err)

	def, err := buildDefaultExpr(colDef, typ, proc)
	require.NoError(t, err)
	require.NotNil(t, def)
	require.NotNil(t, def.Expr)
	require.Equal(t, "(uuid())", def.OriginString)
}

func TestBuildDefaultExprKeepsBareUuidTypeGuard(t *testing.T) {
	proc := testutil.NewProcess(t)

	stmt, err := mysql.ParseOne(context.Background(), "create table t (a int default uuid())", 1)
	require.NoError(t, err)

	createTable, ok := stmt.(*tree.CreateTable)
	require.True(t, ok)
	colDef, ok := createTable.Defs[0].(*tree.ColumnTableDef)
	require.True(t, ok)

	typ, err := getTypeFromAst(context.Background(), colDef.Type)
	require.NoError(t, err)

	_, err = buildDefaultExpr(colDef, typ, proc)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid default value for column 'a'")
}

// Column DEFAULT / ON UPDATE validation must use the strict assignment cast for
// width-constrained string targets: an over-length value is rejected, not silently truncated.
func TestBuildDefaultAndOnUpdateRejectOversizedCharVarchar(t *testing.T) {
	proc := testutil.NewProcess(t)

	for _, oid := range []types.T{types.T_varchar, types.T_char} {
		typ := plan.Type{Id: int32(oid), Width: 3}

		defaultCol := tree.NewColumnTableDef(
			tree.NewUnresolvedColName("a"),
			nil,
			[]tree.ColumnAttribute{
				&tree.AttributeDefault{Expr: tree.NewNumVal("abcdef", "abcdef", false, tree.P_char)},
			},
		)
		_, err := buildDefaultExpr(defaultCol, typ, proc)
		require.Error(t, err, "oversized DEFAULT for %v(3) must be rejected", oid)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidDefault))

		onUpdateCol := tree.NewColumnTableDef(
			tree.NewUnresolvedColName("a"),
			nil,
			[]tree.ColumnAttribute{
				&tree.AttributeOnUpdate{Expr: tree.NewNumVal("abcdef", "abcdef", false, tree.P_char)},
			},
		)
		_, err = buildOnUpdate(onUpdateCol, typ, proc)
		require.Error(t, err, "oversized ON UPDATE for %v(3) must be rejected", oid)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidDefault))
	}
}

func TestBuildDefaultExprRejectsOversizedTinyText(t *testing.T) {
	proc := testutil.NewProcess(t)
	value := strings.Repeat("a", types.MaxTinyTextLen+1)
	defaultCol := tree.NewColumnTableDef(
		tree.NewUnresolvedColName("a"),
		nil,
		[]tree.ColumnAttribute{
			&tree.AttributeDefault{Expr: tree.NewNumVal(value, value, false, tree.P_char)},
		},
	)

	_, err := buildDefaultExpr(defaultCol, plan.Type{
		Id: int32(types.T_text), Width: types.MaxTinyTextLen,
	}, proc)
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidDefault))
}

// A value that fits the CHAR/VARCHAR width is accepted as a column DEFAULT.
func TestBuildDefaultExprFitsVarchar(t *testing.T) {
	proc := testutil.NewProcess(t)

	defaultCol := tree.NewColumnTableDef(
		tree.NewUnresolvedColName("a"),
		nil,
		[]tree.ColumnAttribute{
			&tree.AttributeDefault{Expr: tree.NewNumVal("abc", "abc", false, tree.P_char)},
		},
	)
	defaultValue, err := buildDefaultExpr(defaultCol, plan.Type{Id: int32(types.T_varchar), Width: 3}, proc)
	require.NoError(t, err)
	require.Equal(t, "abc", defaultValue.Expr.GetLit().GetSval())
}

func TestMapDDLAssignmentCastErrorOnlyMapsStringWidthFailures(t *testing.T) {
	ctx := t.Context()
	invalidInput := moerr.NewInvalidInput(ctx, "bad default")
	require.Same(t, invalidInput, mapDDLAssignmentCastError(
		ctx,
		plan.Type{Id: int32(types.T_varchar), Width: 3},
		"a",
		invalidInput,
	))

	internal := moerr.NewInternalError(ctx, "cast failed")
	require.Same(t, internal, mapDDLAssignmentCastError(
		ctx,
		plan.Type{Id: int32(types.T_int64)},
		"a",
		internal,
	))
}

// makePlan2AssignmentCastExpr routes assignment-only targets through cast_strict,
// while explicit casts via makePlan2CastExpr keep the lenient generic cast.
func TestMakePlan2AssignmentCastExprUsesStrictForAssignmentTargets(t *testing.T) {
	ctx := context.Background()
	srcText := &Expr{Typ: plan.Type{Id: int32(types.T_text)}}

	targets := []plan.Type{
		{Id: int32(types.T_varchar), Width: 3},
		{Id: int32(types.T_char), Width: 3},
		{Id: int32(types.T_text), Width: types.MaxTinyTextLen},
		{Id: int32(types.T_date), Width: 3},
		{Id: int32(types.T_datetime), Width: 3},
		{Id: int32(types.T_timestamp), Width: 3},
	}
	for _, target := range targets {

		strictExpr, err := makePlan2AssignmentCastExpr(ctx, DeepCopyExpr(srcText), target)
		require.NoError(t, err)
		require.Equal(t, "cast_strict", strictExpr.GetF().GetFunc().GetObjName())

		genericExpr, err := makePlan2CastExpr(ctx, DeepCopyExpr(srcText), target)
		require.NoError(t, err)
		require.Equal(t, "cast", genericExpr.GetF().GetFunc().GetObjName())
	}

	// Non-string targets stay on the generic cast even for assignment.
	intExpr, err := makePlan2AssignmentCastExpr(ctx, DeepCopyExpr(srcText), plan.Type{Id: int32(types.T_int64)})
	require.NoError(t, err)
	require.Equal(t, "cast", intExpr.GetF().GetFunc().GetObjName())
}

func TestForceAssignmentCastExprUsesAssignmentSemantics(t *testing.T) {
	ctx := context.Background()
	srcText := &Expr{Typ: plan.Type{Id: int32(types.T_text)}}

	targets := []plan.Type{
		{Id: int32(types.T_varchar), Width: 3},
		{Id: int32(types.T_char), Width: 3},
		{Id: int32(types.T_text), Width: types.MaxTinyTextLen},
		{Id: int32(types.T_date), Width: 3},
		{Id: int32(types.T_datetime), Width: 3},
		{Id: int32(types.T_timestamp), Width: 3},
	}
	for _, target := range targets {
		strictExpr, err := forceAssignmentCastExpr(ctx, DeepCopyExpr(srcText), target)
		require.NoError(t, err)
		want := "cast_strict"
		if useSqlModeStringAssignmentCast(target) {
			want = "cast_assign"
		}
		require.Equal(t, want, strictExpr.GetF().GetFunc().GetObjName())

		genericExpr, err := forceCastExpr(ctx, DeepCopyExpr(srcText), target)
		require.NoError(t, err)
		require.Equal(t, "cast", genericExpr.GetF().GetFunc().GetObjName())
	}
}

func TestTinyTextSameTypeAssignmentStillValidates(t *testing.T) {
	ctx := context.Background()
	tinyText := plan.Type{Id: int32(types.T_text), Width: types.MaxTinyTextLen}
	source := &Expr{Typ: tinyText}

	for _, funcName := range []string{"cast_assign", "cast_ignore", "cast_strict"} {
		casted, err := forceAssignmentCastExprWithName(ctx, DeepCopyExpr(source), tinyText, funcName)
		require.NoError(t, err)
		require.Equal(t, funcName, casted.GetF().GetFunc().GetObjName())
	}

	// Generic casts retain the ordinary same-type no-op behavior. Only a
	// constrained assignment boundary must revalidate recovered legacy rows.
	generic, err := forceCastExprWithName(ctx, DeepCopyExpr(source), tinyText, "cast")
	require.NoError(t, err)
	require.Nil(t, generic.GetF())

	proc := testutil.NewProcess(t)
	rt := moruntime.ServiceRuntime(proc.GetService())
	defer rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
	target := &plan.Expr{Typ: tinyText, Expr: &plan.Expr_T{T: &plan.TargetType{}}}
	for _, test := range []struct {
		version int64
		ignore  bool
		want    string
	}{
		{version: defines.MORPCVersion4, want: "cast_strict"},
		{version: defines.MORPCVersion4, ignore: true, want: "cast"},
		{version: defines.MORPCVersion5, want: "cast_assign"},
		{version: defines.MORPCVersion5, ignore: true, want: "cast_ignore"},
	} {
		rt.SetGlobalVariables(moruntime.MOProtocolVersion, test.version)
		casted, err := forceCastExpr2WithProcess(
			ctx,
			DeepCopyExpr(source),
			makeTypeByPlan2Type(tinyText),
			DeepCopyExpr(target),
			test.ignore,
			proc,
		)
		require.NoError(t, err)
		require.Equal(t, test.want, casted.GetF().GetFunc().GetObjName())

		casted, err = forceAssignmentCastExprWithProcess(
			ctx,
			DeepCopyExpr(source),
			tinyText,
			test.ignore,
			proc,
		)
		require.NoError(t, err)
		require.Equal(t, test.want, casted.GetF().GetFunc().GetObjName())
	}
}

func TestAssignmentCastPreservesNestedExplicitTemporalCast(t *testing.T) {
	ctx := context.Background()
	target := plan.Type{Id: int32(types.T_date)}
	srcText := &Expr{Typ: plan.Type{Id: int32(types.T_text)}}

	explicit, err := forceCastExpr(ctx, srcText, target)
	require.NoError(t, err)
	require.Equal(t, "cast", explicit.GetF().GetFunc().GetObjName())

	assignment, err := forceAssignmentCastExpr(ctx, explicit, target)
	require.NoError(t, err)
	require.Same(t, explicit, assignment)
	require.Equal(t, "cast", assignment.GetF().GetFunc().GetObjName())
}

// A generated CHAR/VARCHAR column is materialized as a real column write, so
// buildGeneratedExpr must wrap its expression with the strict assignment cast
// (cast_strict): an over-length value is rejected, not silently truncated.
func TestBuildGeneratedExprUsesStrictForCharVarchar(t *testing.T) {
	proc := testutil.NewProcess(t)
	rt := moruntime.ServiceRuntime(proc.GetService())
	original, hadOriginal := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	t.Cleanup(func() {
		if hadOriginal {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, original)
		} else {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
		}
	})
	rt.SetGlobalVariables(
		moruntime.MOProtocolVersion,
		defines.MORPCVersion5,
	)
	stmt, err := mysql.ParseOne(context.Background(),
		"create table t (t text, g varchar(1) generated always as (coalesce(t, '')) stored)", 1)
	require.NoError(t, err)
	createTable, ok := stmt.(*tree.CreateTable)
	require.True(t, ok)

	var genCol *tree.ColumnTableDef
	for _, def := range createTable.Defs {
		if cd, ok := def.(*tree.ColumnTableDef); ok && cd.Name.ColNameOrigin() == "g" {
			genCol = cd
		}
	}
	require.NotNil(t, genCol)

	existingCols := []*ColDef{{Name: "t", Typ: plan.Type{Id: int32(types.T_text)}}}
	gen, err := buildGeneratedExpr(genCol, plan.Type{Id: int32(types.T_varchar), Width: 1}, existingCols, proc)
	require.NoError(t, err)
	require.NotNil(t, gen)
	require.Equal(t, "cast_strict", gen.Expr.GetF().GetFunc().GetObjName())
	fid, _ := function.DecodeOverloadID(gen.Expr.GetF().GetFunc().GetObj())
	require.Equal(t, int32(function.CAST_STRICT), fid)
	require.Equal(t, int32(types.T_varchar), gen.Expr.Typ.Id) // type still resolves to the column type

	// A non-CHAR/VARCHAR generated target keeps the generic cast.
	genInt, err := buildGeneratedExpr(genCol, plan.Type{Id: int32(types.T_int64)}, existingCols, proc)
	require.NoError(t, err)
	require.Equal(t, "cast", genInt.Expr.GetF().GetFunc().GetObjName())
}

func TestApplyGeneratedColumnAssignmentCastCompatibility(t *testing.T) {
	builder := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)
	proc := builder.compCtx.GetProcess()
	rt := moruntime.ServiceRuntime(proc.GetService())
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion5)
	defer rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
	source := &Expr{Typ: plan.Type{Id: int32(types.T_text)}}
	target := plan.Type{Id: int32(types.T_varchar), Width: 3}

	for _, storedName := range []string{"cast_strict", "cast_assign"} {
		stored, err := forceCastExprWithName(context.Background(), DeepCopyExpr(source), target, storedName)
		require.NoError(t, err)

		normal := builder.applyGeneratedColumnAssignmentCast(DeepCopyExpr(stored), false)
		require.Equal(t, "cast_assign", normal.GetF().GetFunc().GetObjName())

		ignore := builder.applyGeneratedColumnAssignmentCast(DeepCopyExpr(stored), true)
		require.Equal(t, "cast_ignore", ignore.GetF().GetFunc().GetObjName())
	}

	require.Nil(t, builder.applyGeneratedColumnAssignmentCast(nil, false))
	require.Same(t, source, builder.applyGeneratedColumnAssignmentCast(source, false))

	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion4)
	stored, err := forceCastExprWithName(context.Background(), DeepCopyExpr(source), target, "cast_assign")
	require.NoError(t, err)
	normal := builder.applyGeneratedColumnAssignmentCast(DeepCopyExpr(stored), false)
	require.Equal(t, "cast_strict", normal.GetF().GetFunc().GetObjName())
	ignore := builder.applyGeneratedColumnAssignmentCast(DeepCopyExpr(stored), true)
	require.Equal(t, "cast", ignore.GetF().GetFunc().GetObjName())
}

func TestAssignmentCastProtocolGate(t *testing.T) {
	proc := testutil.NewProcess(t)
	rt := moruntime.ServiceRuntime(proc.GetService())
	defer rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
	target := plan.Type{Id: int32(types.T_varchar), Width: 3}

	tests := []struct {
		version int64
		ignore  bool
		want    string
	}{
		{version: defines.MORPCVersion4, want: "cast_strict"},
		{version: defines.MORPCVersion4, ignore: true, want: "cast"},
		{version: defines.MORPCVersion5, want: "cast_assign"},
		{version: defines.MORPCVersion5, ignore: true, want: "cast_ignore"},
	}
	for _, test := range tests {
		rt.SetGlobalVariables(moruntime.MOProtocolVersion, test.version)
		require.Equal(t, test.want, assignmentCastFunctionName(target, test.ignore, proc))

		source := makePlan2Int64ConstExprWithType(1)
		targetType := &plan.Expr{
			Typ:  target,
			Expr: &plan.Expr_T{T: &plan.TargetType{}},
		}
		casted, err := forceCastExpr2WithProcess(
			t.Context(),
			source,
			makeTypeByPlan2Type(target),
			targetType,
			test.ignore,
			proc,
		)
		require.NoError(t, err)
		require.Equal(t, test.want, casted.GetF().GetFunc().GetObjName())
	}
	require.Equal(t, "cast", assignmentCastFunctionName(plan.Type{Id: int32(types.T_int64)}, false, proc))
	require.Equal(t, "cast_assign", assignmentCastFunctionName(plan.Type{
		Id: int32(types.T_text), Width: types.MaxTinyTextLen,
	}, false, proc))
	require.Equal(t, "cast_ignore", assignmentCastFunctionName(plan.Type{
		Id: int32(types.T_text), Width: types.MaxTinyTextLen,
	}, true, proc))
	require.Equal(t, "cast", assignmentCastFunctionName(plan.Type{Id: int32(types.T_text)}, false, proc))
}

func TestSubstituteColRefsInExprPreservesAggregateConfig(t *testing.T) {
	source := &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_text)},
		Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{ObjName: NameGroupConcat},
			Args: []*plan.Expr{{
				Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 0, ColPos: 0}},
			}},
			AggConfig:     []byte{1, 2, 3},
			AggConfigType: plan.AggregateConfigType_AGG_CONFIG_GROUP_CONCAT_ORDER,
		}},
	}

	rewritten := substituteColRefsInExpr(
		source,
		[]*plan.Expr{makePlan2Int64ConstExprWithType(7)},
		0,
	)
	require.Equal(t, int64(7), rewritten.GetF().Args[0].GetLit().GetI64Val())
	require.Equal(t, source.GetF().AggConfig, rewritten.GetF().AggConfig)
	require.Equal(t, source.GetF().AggConfigType, rewritten.GetF().AggConfigType)

	rewritten.GetF().AggConfig[0] = 9
	require.Equal(t, byte(1), source.GetF().AggConfig[0])
}
