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

	"github.com/matrixorigin/matrixone/pkg/container/types"
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
// CHAR/VARCHAR targets: an over-length value is rejected, not silently truncated.
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

		onUpdateCol := tree.NewColumnTableDef(
			tree.NewUnresolvedColName("a"),
			nil,
			[]tree.ColumnAttribute{
				&tree.AttributeOnUpdate{Expr: tree.NewNumVal("abcdef", "abcdef", false, tree.P_char)},
			},
		)
		_, err = buildOnUpdate(onUpdateCol, typ, proc)
		require.Error(t, err, "oversized ON UPDATE for %v(3) must be rejected", oid)
	}
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

// Assignment expressions use cast_strict for every target type, while generic
// planner casts remain on cast.
func TestMakePlan2AssignmentCastExprUsesAssignmentOperator(t *testing.T) {
	ctx := context.Background()
	srcText := &Expr{Typ: plan.Type{Id: int32(types.T_text)}}

	for _, oid := range []types.T{types.T_varchar, types.T_char} {
		target := plan.Type{Id: int32(oid), Width: 3}

		strictExpr, err := makePlan2AssignmentCastExpr(ctx, DeepCopyExpr(srcText), target)
		require.NoError(t, err)
		require.Equal(t, "cast_strict", strictExpr.GetF().GetFunc().GetObjName())

		genericExpr, err := makePlan2CastExpr(ctx, DeepCopyExpr(srcText), target)
		require.NoError(t, err)
		require.Equal(t, "cast", genericExpr.GetF().GetFunc().GetObjName())
	}

	// Numeric targets also use assignment semantics rather than generic implicit
	// cast semantics.
	intExpr, err := makePlan2AssignmentCastExpr(ctx, DeepCopyExpr(srcText), plan.Type{Id: int32(types.T_int64)})
	require.NoError(t, err)
	require.Equal(t, "cast_strict", intExpr.GetF().GetFunc().GetObjName())
}

func TestForceCastExpr2UsesAssignmentCastForNumericTarget(t *testing.T) {
	ctx := context.Background()
	source := makePlan2StringConstExprWithType("7e2")
	targetType := types.T_int32.ToType()
	target := &plan.Expr{
		Typ: makePlan2Type(&targetType),
		Expr: &plan.Expr_T{
			T: &plan.TargetType{},
		},
	}

	expr, err := forceCastExpr2(ctx, source, targetType, target)
	require.NoError(t, err)
	require.Equal(t, "cast_strict", expr.GetF().GetFunc().GetObjName())
}

// A generated CHAR/VARCHAR column is materialized as a real column write, so
// buildGeneratedExpr must wrap its expression with the strict assignment cast
// (cast_strict): an over-length value is rejected, not silently truncated.
func TestBuildGeneratedExprUsesStrictForCharVarchar(t *testing.T) {
	proc := testutil.NewProcess(t)
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
}
