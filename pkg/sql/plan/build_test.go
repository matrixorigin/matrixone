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

package plan

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"slices"
	"strings"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_lock "github.com/matrixorigin/matrixone/pkg/frontend/test/mock_lock"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	lockpb "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	txnpb "github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/features"
	"github.com/matrixorigin/matrixone/pkg/sql/internal/materialized"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/util/toml"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type txnModeTestOperator struct {
	client.TxnOperator
	meta txnpb.TxnMeta
}

func (o txnModeTestOperator) Txn() txnpb.TxnMeta {
	return o.meta
}

func setMockTxnMode(mock *MockOptimizer, mode txnpb.TxnMode) {
	proc := testutil.NewProc(nil)
	proc.Base.TxnOperator = txnModeTestOperator{meta: txnpb.TxnMeta{Mode: mode}}
	mock.ctxt.GetProcessFunc = func() *process.Process { return proc }
}

type sqlModeMockCompilerContext struct {
	*MockCompilerContext
	sqlMode string
}

type cancelAfterGetContextCompilerContext struct {
	CompilerContext
	ctx       context.Context
	cancel    context.CancelFunc
	remaining int
}

func (c *cancelAfterGetContextCompilerContext) GetContext() context.Context {
	c.remaining--
	if c.remaining == 0 {
		c.cancel()
	}
	return c.ctx
}

func (c *sqlModeMockCompilerContext) ResolveVariable(varName string, isSystemVar, isGlobalVar bool) (interface{}, error) {
	if varName == "sql_mode" {
		return c.sqlMode, nil
	}
	return c.MockCompilerContext.ResolveVariable(varName, isSystemVar, isGlobalVar)
}

func BenchmarkInsert(b *testing.B) {
	typ := types.T_varchar.ToType()
	typ.Width = 1024
	targetType := makePlan2Type(&typ)
	targetType.Width = 1024

	originStr := "0123456789"
	testExpr := tree.NewNumVal(originStr, originStr, false, tree.P_char)
	targetT := &plan.Expr{
		Typ: targetType,
		Expr: &plan.Expr_T{
			T: &plan.TargetType{},
		},
	}
	ctx := context.TODO()
	for i := 0; i < b.N; i++ {
		binder := NewDefaultBinder(ctx, nil, nil, targetType, nil)
		expr, err := binder.BindExpr(testExpr, 0, true)
		if err != nil {
			break
		}
		_, err = forceCastExpr2(ctx, expr, typ, targetT)
		if err != nil {
			break
		}
	}
}

func TestBuildPrepareStringUsesSessionSQLMode(t *testing.T) {
	ctx := &sqlModeMockCompilerContext{
		MockCompilerContext: NewMockCompilerContext(true),
		sqlMode:             "PIPES_AS_CONCAT",
	}
	p, err := buildPrepare(tree.NewPrepareString("stmt_sql_mode", "select 'a'||'b'"), ctx)
	require.NoError(t, err)
	require.NotNil(t, p.GetDcl().GetPrepare().GetPlan())
}

func TestPreparedSetVariablesCollectParamsInAssignmentOrder(t *testing.T) {
	mock := NewMockOptimizer(false)
	p, err := runOneStmt(mock, t,
		"prepare stmt1 from 'set @first = ? + 1, @second = ?'")
	require.NoError(t, err)
	prepare := p.GetDcl().GetPrepare()
	require.NotNil(t, prepare)
	require.Len(t, prepare.ParamTypes, 2)

	setVars := prepare.Plan.GetDcl().GetSetVariables()
	require.NotNil(t, setVars)
	require.Len(t, setVars.Items, 2)
	require.Equal(t, int32(0), findFirstParamPos(setVars.Items[0].Value))
	require.Equal(t, int32(1), findFirstParamPos(setVars.Items[1].Value))
}

func TestPreparedSetVariablesCollectScalarSubqueryParams(t *testing.T) {
	mock := NewMockOptimizer(false)
	p, err := runOneStmt(mock, t,
		"prepare stmt1 from 'set @answer = (select ?)'")
	require.NoError(t, err)

	prepare := p.GetDcl().GetPrepare()
	require.NotNil(t, prepare)
	require.Len(t, prepare.ParamTypes, 1)
}

func TestPreparedSetVariablesCollectScalarAggregateParams(t *testing.T) {
	mock := NewMockOptimizer(false)
	p, err := runOneStmt(mock, t,
		"prepare stmt1 from 'set @answer = (select sum(cast(? as signed)))'")
	require.NoError(t, err)

	prepare := p.GetDcl().GetPrepare()
	require.NotNil(t, prepare)
	require.Len(t, prepare.ParamTypes, 1)
}

func TestPreparedSetVariablesCollectScalarGroupByParams(t *testing.T) {
	mock := NewMockOptimizer(false)
	p, err := runOneStmt(mock, t,
		"prepare stmt1 from 'set @answer = (select max(1) group by cast(? as signed))'")
	require.NoError(t, err)

	prepare := p.GetDcl().GetPrepare()
	require.NotNil(t, prepare)
	require.Len(t, prepare.ParamTypes, 1)
}

func TestPreparedSetVariablesCollectWindowParams(t *testing.T) {
	mock := NewMockOptimizer(false)
	p, err := runOneStmt(mock, t,
		"prepare stmt1 from 'set @answer = (select sum(cast(? as signed)) over (partition by cast(? as signed) order by cast(? as signed)))'")
	require.NoError(t, err)

	prepare := p.GetDcl().GetPrepare()
	require.NotNil(t, prepare)
	require.Len(t, prepare.ParamTypes, 3)
}

func TestPreparedSetVariablesKeepGlobalParamOrderAcrossSubqueries(t *testing.T) {
	mock := NewMockOptimizer(false)
	p, err := runOneStmt(mock, t,
		"prepare stmt1 from 'set @first = ?, @nested = (select (select ?)), @third = ?'")
	require.NoError(t, err)

	prepare := p.GetDcl().GetPrepare()
	require.NotNil(t, prepare)
	require.Len(t, prepare.ParamTypes, 3)

	setVars := prepare.Plan.GetDcl().GetSetVariables()
	require.NotNil(t, setVars)
	require.Len(t, setVars.Items, 3)
	require.Equal(t, int32(0), findFirstParamPos(setVars.Items[0].Value))
	require.Equal(t, int32(2), findFirstParamPos(setVars.Items[2].Value))
}

func TestPreparedSetVariablesCollectScalarSubquerySchemas(t *testing.T) {
	mock := NewMockOptimizer(false)
	p, err := runOneStmt(mock, t,
		"prepare stmt1 from 'set @answer = (select n_nationkey from nation where n_nationkey = ?)'")
	require.NoError(t, err)

	prepare := p.GetDcl().GetPrepare()
	require.NotNil(t, prepare)
	require.Len(t, prepare.ParamTypes, 1)
	require.Len(t, prepare.Schemas, 1)
	require.Equal(t, "nation", prepare.Schemas[0].ObjName)
}

func TestPreparedLiteralSetHasNoParams(t *testing.T) {
	mock := NewMockOptimizer(false)
	p, err := runOneStmt(mock, t,
		"prepare stmt1 from 'set @answer = 41 + 1'")
	require.NoError(t, err)
	require.Empty(t, p.GetDcl().GetPrepare().ParamTypes)
}

func findFirstParamPos(expr *plan.Expr) int32 {
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_P:
		return exprImpl.P.Pos
	case *plan.Expr_F:
		for _, arg := range exprImpl.F.Args {
			if pos := findFirstParamPos(arg); pos >= 0 {
				return pos
			}
		}
	case *plan.Expr_List:
		for _, item := range exprImpl.List.List {
			if pos := findFirstParamPos(item); pos >= 0 {
				return pos
			}
		}
	}
	return -1
}

func TestBuildViewPersistsSessionSQLMode(t *testing.T) {
	ctx := &sqlModeMockCompilerContext{
		MockCompilerContext: NewMockCompilerContext(true),
		sqlMode:             "ANSI_QUOTES",
	}
	stmt, err := mysql.ParseOneWithSQLMode(
		context.Background(),
		`create view v_sql_mode as select 1 as "c"`,
		1,
		ctx.sqlMode,
	)
	require.NoError(t, err)
	defer stmt.Free()

	p, err := BuildPlan(ctx, stmt, false)
	require.NoError(t, err)
	viewDef := p.GetDdl().GetCreateView().GetTableDef().GetViewSql()
	require.NotNil(t, viewDef)

	var viewData ViewData
	require.NoError(t, json.Unmarshal([]byte(viewDef.GetView()), &viewData))
	require.NotNil(t, viewData.SQLMode)
	require.Equal(t, ctx.sqlMode, *viewData.SQLMode)
}

func TestPerformRejectsNestedSelectIntoOutfile(t *testing.T) {
	tests := []string{
		"perform select 1 into outfile 'direct.csv'",
		"perform with c as (select 1 into outfile 'cte.csv') select * from c",
		"perform select (select 1 into outfile 'projection.csv')",
		"perform select 1 where exists (select 1 into outfile 'predicate.csv')",
	}

	for _, sql := range tests {
		t.Run(sql, func(t *testing.T) {
			stmt, err := mysql.ParseOne(t.Context(), sql, 1)
			require.NoError(t, err)
			defer stmt.Free()

			_, err = BuildPlan(NewMockCompilerContext(true), stmt, false)
			require.ErrorContains(t, err, "PERFORM SELECT INTO OUTFILE")
		})
	}
}

func TestPerformAllowsNestedSelectWithoutOutfile(t *testing.T) {
	tests := []string{
		"perform with c as (select 1) select * from c",
		"perform select (select 1)",
		"perform select 1 where exists (select 1)",
	}

	for _, sql := range tests {
		t.Run(sql, func(t *testing.T) {
			stmt, err := mysql.ParseOne(t.Context(), sql, 1)
			require.NoError(t, err)
			defer stmt.Free()

			_, err = BuildPlan(NewMockCompilerContext(true), stmt, false)
			require.NoError(t, err)
		})
	}
}

// only use in developing
func TestSingleSQL(t *testing.T) {
	// sql := "INSERT INTO NATION VALUES (1, 'NAME1',21, 'COMMENT1'), (2, 'NAME2', 22, 'COMMENT2')"
	// sql := "insert into dept values (11, 'aa', 'bb')"
	// sql := "delete from dept where deptno > 10"
	// sql := "delete from nation where n_nationkey > 10"
	// sql := "delete nation, nation2 from nation join nation2 on nation.n_name = nation2.n_name"
	// sql := "update nation set n_name ='a' where n_nationkey > 10"
	// sql := "update dept set deptno = 11 where deptno = 10"
	sqls := []string{"prepare stmt1 from update nation set n_name = ? where n_nationkey = ?",
		"prepare stmt1 from insert into  nation values (?, ?, ?, ?) ON DUPLICATE KEY UPDATE n_name=?"}
	mock := NewMockOptimizer(true)

	for _, sql := range sqls {
		logicPlan, err := runOneStmt(mock, t, sql)
		if err != nil {
			t.Fatalf("%+v", err)
		}
		outPutPlan(logicPlan, true, t)
	}
}

func addTextCastTableForTest(mock *MockOptimizer) {
	const tableName = "text_cast_t"
	idType := plan.Type{Id: int32(types.T_int32), NotNullable: true}
	textType := plan.Type{Id: int32(types.T_text)}
	varcharType := plan.Type{Id: int32(types.T_varchar), Width: 255}
	rowIDType := plan.Type{Id: int32(types.T_Rowid), NotNullable: true, Width: 16}

	cols := []*ColDef{
		{ColId: 0, Name: "id", OriginName: "id", Typ: idType, Primary: true, Pkidx: 1, Default: &plan.Default{}},
		{ColId: 1, Name: "txt", OriginName: "txt", Typ: textType, Default: &plan.Default{NullAbility: true}},
		{ColId: 2, Name: "vc", OriginName: "vc", Typ: varcharType, Default: &plan.Default{NullAbility: true}},
		{ColId: 3, Name: catalog.Row_ID, OriginName: catalog.Row_ID, Typ: rowIDType, Hidden: true, Default: &plan.Default{}},
	}
	tableDef := &TableDef{
		TableType: catalog.SystemOrdinaryRel,
		TblId:     23176,
		Name:      tableName,
		Cols:      cols,
		Pkey: &plan.PrimaryKeyDef{
			PkeyColName: "id",
			Cols:        []uint64{0},
			Names:       []string{"id"},
			CompPkeyCol: cols[0],
		},
		Defs: []*plan.TableDef_DefType{
			{
				Def: &plan.TableDef_DefType_Properties{
					Properties: &plan.PropertiesDef{
						Properties: []*plan.Property{
							{Key: catalog.SystemRelAttr_Kind, Value: catalog.SystemOrdinaryRel},
						},
					},
				},
			},
		},
	}
	mock.ctxt.objects[tableName] = &ObjectRef{SchemaName: "tpch", ObjName: tableName, Obj: 23176}
	mock.ctxt.tables[tableName] = tableDef
	mock.ctxt.id2name[23176] = tableName
	mock.ctxt.pks[tableName] = []int{0}
}

// resolveQueryPlan unwraps a PREPARE plan to the inner prepared query plan so
// prepare-specific assertions inspect the real query instead of the outer DCL
// node (whose GetQuery() is nil).
func resolveQueryPlan(p *Plan) *Plan {
	if p == nil {
		return nil
	}
	if p.GetQuery() != nil {
		return p
	}
	if prep := p.GetDcl().GetPrepare(); prep != nil {
		return prep.GetPlan()
	}
	return p
}

func planHasTextToCharOrVarcharCast(p *Plan) bool {
	p = resolveQueryPlan(p)
	if p == nil || p.GetQuery() == nil {
		return false
	}
	for _, node := range p.GetQuery().Nodes {
		if nodeHasTextToCharOrVarcharCast(node) {
			return true
		}
	}
	return false
}

func nodeHasTextToCharOrVarcharCast(node *plan.Node) bool {
	if node == nil {
		return false
	}
	for _, expr := range node.ProjectList {
		if exprHasTextToCharOrVarcharCast(expr) {
			return true
		}
	}
	for _, expr := range node.OnList {
		if exprHasTextToCharOrVarcharCast(expr) {
			return true
		}
	}
	for _, expr := range node.FilterList {
		if exprHasTextToCharOrVarcharCast(expr) {
			return true
		}
	}
	for _, expr := range node.GroupBy {
		if exprHasTextToCharOrVarcharCast(expr) {
			return true
		}
	}
	for _, expr := range node.AggList {
		if exprHasTextToCharOrVarcharCast(expr) {
			return true
		}
	}
	if node.DedupJoinCtx != nil {
		for _, expr := range node.DedupJoinCtx.UpdateColExprList {
			if exprHasTextToCharOrVarcharCast(expr) {
				return true
			}
		}
	}
	for _, expr := range node.OnUpdateExprs {
		if exprHasTextToCharOrVarcharCast(expr) {
			return true
		}
	}
	if node.RowsetData != nil {
		for _, col := range node.RowsetData.Cols {
			for _, data := range col.Data {
				if exprHasTextToCharOrVarcharCast(data.Expr) {
					return true
				}
			}
		}
	}
	return false
}

func exprHasTextToCharOrVarcharCast(expr *plan.Expr) bool {
	if expr == nil {
		return false
	}
	if f := expr.GetF(); f != nil {
		if (f.Func.GetObjName() == "cast" || f.Func.GetObjName() == "cast_strict") && len(f.Args) > 0 &&
			f.Args[0].Typ.Id == int32(types.T_text) &&
			(expr.Typ.Id == int32(types.T_char) || expr.Typ.Id == int32(types.T_varchar)) {
			return true
		}
		for _, arg := range f.Args {
			if exprHasTextToCharOrVarcharCast(arg) {
				return true
			}
		}
	}
	if list := expr.GetList(); list != nil {
		for _, item := range list.List {
			if exprHasTextToCharOrVarcharCast(item) {
				return true
			}
		}
	}
	return false
}

func planHasTextToVarcharCastWithWidth(p *Plan, width int32) bool {
	return planHasTextToVarcharCastWithNameAndWidth(p, "", width)
}

func planHasTextToVarcharAssignCastWithWidth(p *Plan, width int32) bool {
	return planHasTextToVarcharCastWithNameAndWidth(p, "cast_assign", width)
}

func planHasTextToVarcharCastWithNameAndWidth(p *Plan, funcName string, width int32) bool {
	p = resolveQueryPlan(p)
	if p == nil || p.GetQuery() == nil {
		return false
	}
	var visit func(expr *plan.Expr) bool
	visit = func(expr *plan.Expr) bool {
		if expr == nil {
			return false
		}
		if f := expr.GetF(); f != nil {
			nameMatches := f.Func.GetObjName() == funcName
			if funcName == "" {
				name := f.Func.GetObjName()
				nameMatches = name == "cast" || name == "cast_strict" || name == "cast_assign"
			}
			if nameMatches && len(f.Args) > 0 &&
				f.Args[0].Typ.Id == int32(types.T_text) &&
				expr.Typ.Id == int32(types.T_varchar) &&
				expr.Typ.Width == width {
				return true
			}
			for _, arg := range f.Args {
				if visit(arg) {
					return true
				}
			}
		}
		if list := expr.GetList(); list != nil {
			for _, item := range list.List {
				if visit(item) {
					return true
				}
			}
		}
		return false
	}
	for _, node := range p.GetQuery().Nodes {
		for _, expr := range node.ProjectList {
			if visit(expr) {
				return true
			}
		}
		if node.DedupJoinCtx != nil {
			for _, expr := range node.DedupJoinCtx.UpdateColExprList {
				if visit(expr) {
					return true
				}
			}
		}
		for _, expr := range node.OnUpdateExprs {
			if visit(expr) {
				return true
			}
		}
		if node.RowsetData != nil {
			for _, col := range node.RowsetData.Cols {
				for _, data := range col.Data {
					if visit(data.Expr) {
						return true
					}
				}
			}
		}
	}
	return false
}

func planHasUnboundedTextToTinyTextCast(p *Plan) bool {
	p = resolveQueryPlan(p)
	if p == nil || p.GetQuery() == nil {
		return false
	}
	var visit func(expr *plan.Expr) bool
	visit = func(expr *plan.Expr) bool {
		if expr == nil {
			return false
		}
		if f := expr.GetF(); f != nil {
			name := f.Func.GetObjName()
			if (name == "cast" || name == "cast_strict" || name == "cast_assign") && len(f.Args) > 0 &&
				f.Args[0].Typ.Id == int32(types.T_text) && f.Args[0].Typ.Width == 0 &&
				expr.Typ.Id == int32(types.T_text) && expr.Typ.Width == types.MaxTinyTextLen {
				return true
			}
			for _, arg := range f.Args {
				if visit(arg) {
					return true
				}
			}
		}
		if list := expr.GetList(); list != nil {
			for _, item := range list.List {
				if visit(item) {
					return true
				}
			}
		}
		return false
	}
	for _, node := range p.GetQuery().Nodes {
		for _, expr := range node.ProjectList {
			if visit(expr) {
				return true
			}
		}
	}
	return false
}

func TestUpdateTextConcatCoalesceKeepsTextAssignmentCast(t *testing.T) {
	mock := NewMockOptimizer(true)
	addTextCastTableForTest(mock)

	logicPlan, err := runOneStmt(mock, t, "update text_cast_t set txt = concat(coalesce(vc, txt, ''), ' suffix') where id = 1")
	assert.NoError(t, err)
	assert.False(t, planHasTextToCharOrVarcharCast(logicPlan))
	assert.False(t, planHasUnboundedTextToTinyTextCast(logicPlan))
}

func TestPrepareUpdateTextConcatCoalesceKeepsTextAssignmentCast(t *testing.T) {
	mock := NewMockOptimizer(true)
	addTextCastTableForTest(mock)

	logicPlan, err := runOneStmt(mock, t, "prepare stmt1 from update text_cast_t set txt = concat(coalesce(txt, ''), ?) where id = ?")
	assert.NoError(t, err)
	assert.False(t, planHasTextToCharOrVarcharCast(logicPlan))
}

func TestUpdateTextCaseKeepsTextAssignmentCast(t *testing.T) {
	mock := NewMockOptimizer(true)
	addTextCastTableForTest(mock)

	logicPlan, err := runOneStmt(mock, t, "update text_cast_t set txt = case when id = 1 then txt else '' end where id = 1")
	assert.NoError(t, err)
	assert.False(t, planHasTextToCharOrVarcharCast(logicPlan))
}

func TestUpdateTextIfKeepsTextAssignmentCast(t *testing.T) {
	mock := NewMockOptimizer(true)
	addTextCastTableForTest(mock)

	logicPlan, err := runOneStmt(mock, t, "update text_cast_t set txt = if(id = 1, txt, '') where id = 1")
	assert.NoError(t, err)
	assert.False(t, planHasTextToCharOrVarcharCast(logicPlan))
}

func TestUpdateVarcharFromTextKeepsVarcharWidthCast(t *testing.T) {
	mock := NewMockOptimizer(true)
	addTextCastTableForTest(mock)

	logicPlan, err := runOneStmt(mock, t, "update text_cast_t set vc = txt where id = 1")
	assert.NoError(t, err)
	assert.True(t, planHasTextToVarcharCastWithWidth(logicPlan, 255))
}

func TestInsertSelectVarcharFromTextUsesAssignmentCast(t *testing.T) {
	mock := NewMockOptimizer(true)
	addTextCastTableForTest(mock)

	// INSERT ... SELECT is an assignment path: it routes CHAR/VARCHAR targets
	// through cast_assign, which enforces width at runtime per sql_mode.
	logicPlan, err := runOneStmt(mock, t, "insert into text_cast_t(id, vc) select id, txt from text_cast_t")
	assert.NoError(t, err)
	assert.True(t, planHasTextToVarcharAssignCastWithWidth(logicPlan, 255))
}

func TestInsertSelectEnumToJSONQuotesDisplayValue(t *testing.T) {
	mock := NewMockOptimizer(true)
	source := mock.ctxt.tables["nation"]
	source.Cols[1].Typ = plan.Type{
		Id:         int32(types.T_enum),
		Enumvalues: `alpha,{"a":1}`,
	}

	const tableName = "enum_json_destination"
	idType := plan.Type{Id: int32(types.T_int32), NotNullable: true}
	jsonType := plan.Type{Id: int32(types.T_json)}
	rowIDType := plan.Type{Id: int32(types.T_Rowid), NotNullable: true, Width: 16}
	cols := []*ColDef{
		{ColId: 0, Name: "id", OriginName: "id", Typ: idType, Primary: true, Pkidx: 1, Default: &plan.Default{}},
		{ColId: 1, Name: "j", OriginName: "j", Typ: jsonType, Default: &plan.Default{NullAbility: true}},
		{ColId: 2, Name: catalog.Row_ID, OriginName: catalog.Row_ID, Typ: rowIDType, Hidden: true, Default: &plan.Default{}},
	}
	tableDef := &TableDef{
		TableType: catalog.SystemOrdinaryRel,
		TblId:     23177,
		Name:      tableName,
		Cols:      cols,
		Pkey: &plan.PrimaryKeyDef{
			PkeyColName: "id",
			Cols:        []uint64{0},
			Names:       []string{"id"},
			CompPkeyCol: cols[0],
		},
	}
	mock.ctxt.objects[tableName] = &ObjectRef{SchemaName: "tpch", ObjName: tableName, Obj: 23177}
	mock.ctxt.tables[tableName] = tableDef
	mock.ctxt.id2name[23177] = tableName
	mock.ctxt.pks[tableName] = []int{0}

	for _, tc := range []struct {
		sql       string
		wantQuote bool
	}{
		{sql: "insert into enum_json_destination(id, j) select n_nationkey, n_name from nation", wantQuote: true},
		{sql: "insert into enum_json_destination(id, j) select id, name from (select n_nationkey as id, n_name as name from nation) src", wantQuote: true},
		{sql: "insert into enum_json_destination(id, j) select id, name from (select n_nationkey as id, n_name as name from nation union all select n_nationkey, n_name from nation) src", wantQuote: true},
		{sql: "insert into enum_json_destination(id, j) select id, name from (select n_nationkey as id, n_name as name from nation union select n_nationkey, n_name from nation) src", wantQuote: true},
		{sql: "insert into enum_json_destination(id, j) select id, name from (select n_nationkey as id, n_name as name from nation intersect select n_nationkey, cast('{\"a\":1}' as varchar) from nation) src", wantQuote: true},
		{sql: "insert into enum_json_destination(id, j) select id, name from (select n_nationkey as id, n_name as name from nation intersect all select n_nationkey, cast('{\"a\":1}' as varchar) from nation) src", wantQuote: true},
		{sql: "insert into enum_json_destination(id, j) select id, name from (select n_nationkey as id, n_name as name from nation minus select n_nationkey, cast('{\"a\":1}' as varchar) from nation) src", wantQuote: true},
		{sql: "insert into enum_json_destination(id, j) select id, name from (select n_nationkey as id, n_name as name from nation union all select n_nationkey, n_comment from nation) src", wantQuote: false},
		{sql: "insert into enum_json_destination(id, j) select id, name from (select n_nationkey as id, n_comment as name from nation union all select n_nationkey, n_name from nation) src", wantQuote: false},
		{sql: "insert into enum_json_destination(id, j) select id, name from (select n_nationkey as id, n_name as name from nation union select n_nationkey, n_comment from nation) src", wantQuote: false},
		{sql: "insert into enum_json_destination(id, j) select id, name from (select n_nationkey as id, n_comment as name from nation union select n_nationkey, n_name from nation) src", wantQuote: false},
		{sql: "insert into enum_json_destination(id, j) select id, name from (select n_nationkey as id, cast('{\"a\":1}' as varchar) as name from nation intersect select n_nationkey, n_name from nation) src", wantQuote: false},
		{sql: "insert into enum_json_destination(id, j) select id, name from (select n_nationkey as id, cast('{\"a\":1}' as varchar) as name from nation intersect all select n_nationkey, n_name from nation) src", wantQuote: false},
		{sql: "insert into enum_json_destination(id, j) select id, name from (select n_nationkey as id, cast('{\"a\":1}' as varchar) as name from nation minus select n_nationkey, n_name from nation) src", wantQuote: false},
	} {
		logicPlan, err := runOneStmt(mock, t, tc.sql)
		require.NoError(t, err, tc.sql)

		foundJSONQuote := false
		for _, node := range logicPlan.GetQuery().Nodes {
			for _, expr := range node.ProjectList {
				if exprContainsFuncName(expr, "json_quote") {
					foundJSONQuote = true
				}
			}
		}
		require.Equal(t, tc.wantQuote, foundJSONQuote, "unexpected ENUM display quoting decision: %s", tc.sql)
	}
}

func TestProjectedEnumToJSONExplicitCastQuotesDisplayValue(t *testing.T) {
	mock := NewMockOptimizer(true)
	source := mock.ctxt.tables["nation"]
	source.Cols[1].Typ = plan.Type{
		Id:         int32(types.T_enum),
		Enumvalues: `alpha,{"a":1}`,
	}

	for _, tc := range []struct {
		sql       string
		wantQuote bool
	}{
		{sql: "select convert(name, json) from (select n_name as name from nation) src", wantQuote: true},
		{sql: "select cast(name as json) from (select n_name as name from nation union all select n_name from nation) src", wantQuote: true},
		{sql: "select convert(name, json) from (select n_name as name from nation union select n_name from nation) src", wantQuote: true},
		{sql: "select convert(name, json) from (select n_name as name from nation union all select n_comment from nation) src", wantQuote: false},
	} {
		logicPlan, err := runOneStmt(mock, t, tc.sql)
		require.NoError(t, err, tc.sql)

		foundJSONQuote := false
		for _, node := range logicPlan.GetQuery().Nodes {
			for _, expr := range node.ProjectList {
				if exprContainsFuncName(expr, "json_quote") {
					foundJSONQuote = true
				}
			}
		}
		require.Equal(t, tc.wantQuote, foundJSONQuote, "unexpected ENUM display quoting decision: %s", tc.sql)
	}
}

func TestUpdateProjectedEnumToJSONQuotesDisplayValue(t *testing.T) {
	mock := NewMockOptimizer(true)
	table := mock.ctxt.tables["nation"]
	table.Cols[1].Typ = plan.Type{
		Id:         int32(types.T_enum),
		Enumvalues: `alpha,{"a":1}`,
	}
	for _, col := range table.Cols {
		if col.Name == "n_comment" {
			col.Typ = plan.Type{Id: int32(types.T_json)}
			break
		}
	}

	for _, tc := range []struct {
		sql       string
		wantQuote bool
	}{
		{
			sql:       "update nation n join (select n_nationkey as id, n_name as value from nation) src on n.n_nationkey = src.id set n.n_comment = src.value",
			wantQuote: true,
		},
		{
			sql:       "update nation n join (select n_nationkey as id, n_name as value from nation union all select n_nationkey, n_name from nation) src on n.n_nationkey = src.id set n.n_comment = src.value",
			wantQuote: true,
		},
		{
			sql:       "update nation n join (select n_nationkey as id, n_name as value from nation union all select n_nationkey, cast('{\"a\":1}' as varchar) from nation) src on n.n_nationkey = src.id set n.n_comment = src.value",
			wantQuote: false,
		},
	} {
		logicPlan, err := runOneStmt(mock, t, tc.sql)
		require.NoError(t, err, tc.sql)

		foundJSONQuote := false
		for _, node := range logicPlan.GetQuery().Nodes {
			for _, expr := range node.ProjectList {
				if exprContainsFuncName(expr, "json_quote") {
					foundJSONQuote = true
				}
			}
		}
		require.Equal(t, tc.wantQuote, foundJSONQuote, "unexpected ENUM display quoting decision: %s", tc.sql)
	}
}

func TestSetDisplayValueToJSONQuotesAcrossPlannerPaths(t *testing.T) {
	mock := NewMockOptimizer(true)
	table := mock.ctxt.tables["nation"]
	table.Cols[1].Typ = plan.Type{
		Id:         int32(types.T_uint64),
		Enumvalues: "alpha,beta",
	}

	const tableName = "set_json_destination"
	idType := plan.Type{Id: int32(types.T_int32), NotNullable: true}
	jsonType := plan.Type{Id: int32(types.T_json)}
	rowIDType := plan.Type{Id: int32(types.T_Rowid), NotNullable: true, Width: 16}
	cols := []*ColDef{
		{ColId: 0, Name: "id", OriginName: "id", Typ: idType, Primary: true, Pkidx: 1, Default: &plan.Default{}},
		{ColId: 1, Name: "j", OriginName: "j", Typ: jsonType, Default: &plan.Default{NullAbility: true}},
		{ColId: 2, Name: catalog.Row_ID, OriginName: catalog.Row_ID, Typ: rowIDType, Hidden: true, Default: &plan.Default{}},
	}
	mock.ctxt.objects[tableName] = &ObjectRef{SchemaName: "tpch", ObjName: tableName, Obj: 23178}
	mock.ctxt.tables[tableName] = &TableDef{
		TableType: catalog.SystemOrdinaryRel,
		TblId:     23178,
		Name:      tableName,
		Cols:      cols,
		Pkey: &plan.PrimaryKeyDef{
			PkeyColName: "id",
			Cols:        []uint64{0},
			Names:       []string{"id"},
			CompPkeyCol: cols[0],
		},
	}
	mock.ctxt.id2name[23178] = tableName
	mock.ctxt.pks[tableName] = []int{0}

	for _, sql := range []string{
		"select convert(n_name, json) from nation",
		"select cast(n_name as json) from nation",
		"select convert(name, json) from (select n_name as name from nation) src",
		"select cast(name as json) from (select n_name as name from nation union all select n_name from nation) src",
		"insert into set_json_destination(id, j) select n_nationkey, n_name from nation",
		"update set_json_destination dst join nation src on dst.id = src.n_nationkey set dst.j = src.n_name",
	} {
		logicPlan, err := runOneStmt(mock, t, sql)
		require.NoError(t, err, sql)

		foundJSONQuote := false
		for _, node := range logicPlan.GetQuery().Nodes {
			for _, expr := range node.ProjectList {
				if exprContainsFuncName(expr, "json_quote") {
					foundJSONQuote = true
				}
			}
		}
		require.True(t, foundJSONQuote, "SET display value must be quoted as JSON: %s", sql)
	}
}

func TestProjectedSetNumericCastUsesStoredBitmap(t *testing.T) {
	mock := NewMockOptimizer(true)
	mock.ctxt.tables["nation"].Cols[1].Typ = plan.Type{Id: int32(types.T_uint64), Enumvalues: ",a"}

	for _, tc := range []struct {
		name            string
		sql             string
		wantStringCast  bool
		wantBitmapCarry bool
	}{
		{
			name:            "derived table",
			sql:             "select cast(name as unsigned) from (select n_name as name from nation) src",
			wantBitmapCarry: true,
		},
		{
			name:            "pure set union all",
			sql:             "select cast(name as unsigned) from (select n_name as name from nation union all select n_name from nation) src",
			wantBitmapCarry: true,
		},
		{
			name:            "three-way pure set union all",
			sql:             "select cast(name as unsigned) from (select n_name as name from nation union all select n_name from nation union all select n_name from nation) src",
			wantBitmapCarry: true,
		},
		{
			name:            "pure set intersect",
			sql:             "select cast(name as unsigned) from (select n_name as name from nation intersect select n_name from nation) src",
			wantBitmapCarry: true,
		},
		{
			name:            "pure set minus",
			sql:             "select cast(name as unsigned) from (select n_name as name from nation minus select n_name from nation) src",
			wantBitmapCarry: true,
		},
		{
			name:           "mixed set and varchar union all",
			sql:            "select cast(name as unsigned) from (select n_name as name from nation union all select n_comment from nation) src",
			wantStringCast: true,
		},
		{
			name:           "three-way mixed set and varchar union all",
			sql:            "select cast(name as unsigned) from (select n_name as name from nation union all select n_comment from nation union all select n_name from nation) src",
			wantStringCast: true,
		},
		{
			name:            "nested intersect precedence",
			sql:             "select cast(name as unsigned) from (select n_name as name from nation union all select n_name from nation intersect select n_name from nation) src",
			wantBitmapCarry: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logicPlan, err := runOneStmt(mock, t, tc.sql)
			require.NoError(t, err)
			require.Equal(t, tc.wantStringCast, planHasVarcharToIntegerCast(logicPlan))
			require.Equal(t, tc.wantBitmapCarry, planHasPlainUint64ColRef(logicPlan))
			requireSetOperationProjectionWidths(t, logicPlan)
		})
	}
}

func requireSetOperationProjectionWidths(t *testing.T, p *Plan) {
	t.Helper()
	p = resolveQueryPlan(p)
	require.NotNil(t, p)
	query := p.GetQuery()
	require.NotNil(t, query)
	for nodeID, node := range query.Nodes {
		switch node.NodeType {
		case plan.Node_UNION, plan.Node_UNION_ALL,
			plan.Node_MINUS, plan.Node_MINUS_ALL,
			plan.Node_INTERSECT, plan.Node_INTERSECT_ALL:
			for _, childID := range node.Children {
				require.GreaterOrEqual(t, childID, int32(0), "set node %d has invalid child", nodeID)
				require.Less(t, int(childID), len(query.Nodes), "set node %d has invalid child", nodeID)
				require.Len(t, query.Nodes[childID].ProjectList, len(node.ProjectList),
					"set node %d child %d projection width", nodeID, childID)
			}
		}
	}
}

func TestInsertSelectProjectedSetUsesStoredBitmap(t *testing.T) {
	mock := NewMockOptimizer(true)
	mock.ctxt.tables["nation"].Cols[1].Typ = plan.Type{Id: int32(types.T_uint64), Enumvalues: ",a"}
	addSetBitmapDestinationForTest(mock)

	logicPlan, err := runOneStmt(
		mock,
		t,
		"insert into set_bitmap_destination(id, bitmap) select n_nationkey, n_name from nation",
	)
	require.NoError(t, err)
	require.False(t, planHasVarcharToIntegerCast(logicPlan))
	require.True(t, planHasPlainUint64ColRef(logicPlan))
}

func TestInsertSelectSetTargetRejectsUnknownSourceColumn(t *testing.T) {
	mock := NewMockOptimizer(true)
	addSetBitmapDestinationForTest(mock)
	mock.ctxt.tables["set_bitmap_destination"].Cols[1].Typ.Enumvalues = "a,b"

	_, err := runOneStmt(
		mock,
		t,
		"insert into set_bitmap_destination(id, bitmap) select n_nationkey, missing from nation",
	)
	require.ErrorContains(t, err, "column missing does not exist")
}

func addSetBitmapDestinationForTest(mock *MockOptimizer) {
	const tableName = "set_bitmap_destination"
	idType := plan.Type{Id: int32(types.T_int32), NotNullable: true}
	bitmapType := plan.Type{Id: int32(types.T_uint64)}
	rowIDType := plan.Type{Id: int32(types.T_Rowid), NotNullable: true, Width: 16}
	cols := []*ColDef{
		{ColId: 0, Name: "id", OriginName: "id", Typ: idType, Primary: true, Pkidx: 1, Default: &plan.Default{}},
		{ColId: 1, Name: "bitmap", OriginName: "bitmap", Typ: bitmapType, Default: &plan.Default{NullAbility: true}},
		{ColId: 2, Name: catalog.Row_ID, OriginName: catalog.Row_ID, Typ: rowIDType, Hidden: true, Default: &plan.Default{}},
	}
	mock.ctxt.objects[tableName] = &ObjectRef{SchemaName: "tpch", ObjName: tableName, Obj: 23179}
	mock.ctxt.tables[tableName] = &TableDef{
		TableType: catalog.SystemOrdinaryRel,
		TblId:     23179,
		Name:      tableName,
		Cols:      cols,
		Pkey: &plan.PrimaryKeyDef{
			PkeyColName: "id",
			Cols:        []uint64{0},
			Names:       []string{"id"},
			CompPkeyCol: cols[0],
		},
	}
	mock.ctxt.id2name[23179] = tableName
	mock.ctxt.pks[tableName] = []int{0}
}

func planHasVarcharToIntegerCast(p *Plan) bool {
	return planHasExpr(p, func(expr *plan.Expr) bool {
		fn := expr.GetF()
		if fn == nil || fn.Func == nil || len(fn.Args) == 0 {
			return false
		}
		name := fn.Func.ObjName
		return (name == "cast" || name == "cast_strict" || name == "cast_assign") &&
			fn.Args[0].Typ.Id == int32(types.T_varchar) && types.T(expr.Typ.Id).IsInteger()
	})
}

func planHasPlainUint64ColRef(p *Plan) bool {
	return planHasExpr(p, func(expr *plan.Expr) bool {
		return expr.GetCol() != nil && expr.Typ.Id == int32(types.T_uint64) && expr.Typ.Enumvalues == ""
	})
}

func planHasExpr(p *Plan, match func(*plan.Expr) bool) bool {
	p = resolveQueryPlan(p)
	if p == nil || p.GetQuery() == nil {
		return false
	}
	var visit func(*plan.Expr) bool
	visit = func(expr *plan.Expr) bool {
		if expr == nil {
			return false
		}
		if match(expr) {
			return true
		}
		if fn := expr.GetF(); fn != nil {
			for _, arg := range fn.Args {
				if visit(arg) {
					return true
				}
			}
		}
		if list := expr.GetList(); list != nil {
			for _, item := range list.List {
				if visit(item) {
					return true
				}
			}
		}
		return false
	}
	for _, node := range p.GetQuery().Nodes {
		for _, expr := range node.ProjectList {
			if visit(expr) {
				return true
			}
		}
	}
	return false
}

func TestOnDuplicateUpdateVarcharFromTextUsesAssignmentCast(t *testing.T) {
	mock := NewMockOptimizer(true)
	addTextCastTableForTest(mock)

	// ON DUPLICATE KEY UPDATE is an assignment path (not INSERT IGNORE), so it
	// routes the CHAR/VARCHAR target through the sql_mode-gated cast_assign.
	logicPlan, err := runOneStmt(mock, t, "insert into text_cast_t(id, txt, vc) values (1, repeat('a', 260), '') on duplicate key update vc = txt")
	assert.NoError(t, err)
	assert.True(t, planHasTextToVarcharAssignCastWithWidth(logicPlan, 255))
}

// test single table plan building
func TestSingleTableSQLBuilder(t *testing.T) {
	mock := NewMockOptimizer(false)
	// should pass
	sqls := []string{
		"SELECT '1900-01-01 00:00:00' + INTERVAL 2147483648 SECOND",
		"SELECT N_NAME, N_REGIONKEY FROM NATION WHERE N_REGIONKEY > 0 AND N_NAME LIKE '%AA' ORDER BY N_NAME DESC, N_REGIONKEY LIMIT 10, 20",
		"SELECT N_NAME, N_REGIONKEY a FROM NATION WHERE N_REGIONKEY > 0 ORDER BY a DESC", //test alias
		"SELECT NATION.N_NAME FROM NATION",                                       //test alias
		"SELECT * FROM NATION",                                                   //test star
		"SELECT a.* FROM NATION a",                                               //test star
		"SELECT count(*) FROM NATION",                                            //test star
		"SELECT count(*) FROM NATION group by N_NAME",                            //test star
		"SELECT N_NAME, count(distinct N_REGIONKEY) FROM NATION group by N_NAME", //test distinct agg function
		"SELECT N_NAME, MAX(N_REGIONKEY) FROM NATION GROUP BY N_NAME HAVING MAX(N_REGIONKEY) > 10", //test agg
		"SELECT DISTINCT N_NAME FROM NATION", //test distinct
		"select sum(n_nationkey) as s from nation order by s",
		"select date_add(date '2001-01-01', interval 1 day) as a",
		"select date_sub(date '2001-01-01', interval '1' day) as a",
		"select date_add('2001-01-01', interval '1' day) as a",
		"select n_name, count(*) from nation group by n_name order by 2 asc",
		"select count(distinct 12)",
		"select nullif(n_name, n_comment), ifnull(n_comment, n_name) from nation",

		"select 18446744073709551500",
		"select 0xffffffffffffffff",
		"select 0xffff",

		"SELECT N_REGIONKEY + 2 as a, N_REGIONKEY/2, N_REGIONKEY* N_NATIONKEY, N_REGIONKEY % N_NATIONKEY, N_REGIONKEY - N_NATIONKEY FROM NATION WHERE -N_NATIONKEY < -20", //test more expr
		"SELECT N_REGIONKEY FROM NATION where N_REGIONKEY >= N_NATIONKEY or (N_NAME like '%ddd' and N_REGIONKEY >0.5)",                                                    //test more expr
		"SELECT N_REGIONKEY FROM NATION where N_REGIONKEY between 2 and 2 OR N_NATIONKEY not between 3 and 10",                                                            //test more expr
		// "SELECT N_REGIONKEY FROM NATION where N_REGIONKEY is null and N_NAME is not null",
		"SELECT N_REGIONKEY FROM NATION where N_REGIONKEY IN (1, 2)",  //test more expr
		"SELECT N_REGIONKEY FROM NATION where N_REGIONKEY NOT IN (1)", //test more expr
		"select N_REGIONKEY from nation group by N_REGIONKEY having abs(nation.N_REGIONKEY - 1) >10",

		"SELECT -1",
		"select date_add('1997-12-31 23:59:59',INTERVAL 100000 SECOND)",
		"select date_sub('1997-12-31 23:59:59',INTERVAL 2 HOUR)",
		"select @str_var, @int_var, @bool_var, @float_var, @null_var",
		"select @str_var, @@global.int_var, @@session.bool_var",
		"select n_name from nation where n_name != @str_var and n_regionkey > @int_var",
		"select n_name from nation where n_name != @@global.str_var and n_regionkey > @@session.int_var",
		"select distinct(n_name), ((abs(n_regionkey))) from nation",
		"SET @var = abs(-1), @@session.string_var = 'aaa'",
		"SET NAMES 'utf8mb4' COLLATE 'utf8mb4_general_ci'",
		"SELECT DISTINCT N_NAME FROM NATION ORDER BY N_NAME", //test distinct with order by

		"prepare stmt1 from select * from nation",
		"prepare stmt1 from select * from nation where n_name = ?",
		"prepare stmt1 from 'select * from nation where n_name = ?'",
		"prepare stmt1 from 'insert into nation select * from nation2 where n_name = ?'",
		"prepare stmt1 from 'select * from nation where n_name = ?'",
		"prepare stmt1 from 'drop table if exists t1'",
		"prepare stmt1 from 'create table t1 (a int)'",
		"prepare stmt1 from select N_REGIONKEY from nation group by N_REGIONKEY having abs(nation.N_REGIONKEY - ?) > ?",
		"execute stmt1",
		"execute stmt1 using @str_var, @@global.int_var",
		"deallocate prepare stmt1",
		"drop prepare stmt1",
		"select count(n_name) from nation limit 10",
		"select l_shipdate + interval '1' day from lineitem",
		"select interval '1' day + l_shipdate  from lineitem",
		"select interval '1' day + cast('2022-02-02 00:00:00' as datetime)",
		"select cast('2022-02-02 00:00:00' as datetime) + interval '1' day",
		"select true is unknown",
		"select null is not unknown",
		"select 1 as c,  1/2, abs(-2)",

		"select date('2022-01-01'), adddate(time'00:00:00', interval 1 day), subdate(time'00:00:00', interval 1 week), '2007-01-01' + interval 1 month, '2007-01-01' -  interval 1 hour",
		"SELECT '2024-01-01' + INTERVAL n_nationkey DAY FROM nation",
		"SELECT '2024-01-01' - INTERVAL n_nationkey HOUR FROM nation",
		"SELECT '2024-01-01' + INTERVAL n_nationkey % 365 DAY FROM nation",
		"SELECT '2024-01-01' + INTERVAL (n_nationkey % 365) DAY FROM nation",
		"SELECT 20260515 + INTERVAL 7 DAY",
		"SELECT 20260515 - INTERVAL 7 DAY",
		"SELECT INTERVAL 7 DAY + 20260515",
		"SELECT MAX(n_nationkey) + INTERVAL 7 DAY FROM nation",
		"SELECT MAX(n_nationkey) - INTERVAL 7 DAY FROM nation",
		"select 2222332222222223333333333333333333, 0x616263,-10, bit_and(2), bit_or(2), 'aaa' like '%a',str_to_date('04/31/2004', '%m/%d/%Y'),unix_timestamp(from_unixtime(2147483647))",
		"select max(n_nationkey) over  (partition by N_REGIONKEY) from nation",
		"select * from generate_series(1, 5) g",
		"prepare stmt1 from select * from nation where n_name like ? or n_nationkey > 10 order by 2 limit '10'",

		"values row(1,1), row(2,2), row(3,3) order by column_0 limit 2",
		"select * from (values row(1,1), row(2,2), row(3,3)) a (c1, c2)",
		"prepare stmt1 from select * from nation where n_name like ? or n_nationkey > 10 order by 2 limit '10' for update",

		// get_format: DATE/TIME/DATETIME/TIMESTAMP should be treated as type keywords, not column names
		"select get_format(DATE, 'USA')",
		"select get_format(TIME, 'EUR')",
		"select get_format(DATETIME, 'JIS')",
		"select get_format(TIMESTAMP, 'ISO')",

		"select count(n_name) from nation limit 10 for update", // aggregate + limit + for update (issue 23131 family)

		// uuid family: INTERVAL shift rewrite, datetime boundary form, extraction
		"select uuid(interval 1 minute), uuid_v7(interval 1 hour), uuid_v1(interval 1 day), uuid_v6(interval 1 month)",
		"select uuid_v7('2026-01-01 00:00:00'), uuid_v1('2026-01-01 00:00:00'), uuid_v6('2026-01-01 00:00:00')",
		"select uuid_extract_version(uuid_v4()), uuid_extract_timestamp(uuid_v7())",
	}
	runTestShouldPass(mock, t, sqls, false, false)

	// should error
	sqls = []string{
		"SELECT N_NAME, N_REGIONKEY FROM table_not_exist",                   //table not exist
		"SELECT N_NAME, column_not_exist FROM NATION",                       //column not exist
		"SELECT N_NAME, N_REGIONKEY a FROM NATION ORDER BY cccc",            //column alias not exist
		"SELECT N_NAME, b.N_REGIONKEY FROM NATION a ORDER BY b.N_REGIONKEY", //table alias not exist
		"SELECT N_NAME FROM NATION WHERE ffff(N_REGIONKEY) > 0",             //function name not exist
		"SELECT NATION.N_NAME FROM NATION a",                                // mysql should error, but i don't think it is necesssary
		"select n_nationkey, sum(n_nationkey) from nation",
		"SET @var = abs(a)", // can't use column
		"SET @var = avg(2)", // can't use agg function

		"SELECT DISTINCT N_NAME FROM NATION GROUP BY N_REGIONKEY", //test distinct with group by
		"SELECT DISTINCT N_NAME FROM NATION ORDER BY N_REGIONKEY", //test distinct with order by
		//"select 18446744073709551500",                             //over int64
		//"select 0xffffffffffffffff",                               //over int64

		"select uuid_v7(5, 3)", // internal (count, unit) uuid form is not directly callable
		"select uuid(1, 2, 3)", // uuid family takes zero or one arg
	}
	runTestShouldError(mock, t, sqls)
}

func TestRollupWindowRanksAfterRollupUnion(t *testing.T) {
	mock := NewMockOptimizer(false)
	for _, tc := range []struct {
		name             string
		sql              string
		expectedHeadings []string
	}{
		{
			name: "aliased aggregate output",
			sql: `
				select
					l_returnflag,
					l_linestatus,
					sum(l_quantity) as total_qty,
					row_number() over (order by sum(l_quantity) desc, l_returnflag, l_linestatus) as row_num,
					rank() over (order by sum(l_quantity) desc) as rank_num,
					dense_rank() over (order by sum(l_quantity) desc) as dense_rank_num
				from lineitem
				group by l_returnflag, l_linestatus with rollup
				having total_qty > 0
				order by total_qty desc, l_returnflag, l_linestatus`,
			expectedHeadings: []string{"l_returnflag", "l_linestatus", "total_qty", "row_num", "rank_num", "dense_rank_num"},
		},
		{
			name: "aggregate output without alias",
			sql: `
				select
					l_returnflag,
					l_linestatus,
					sum(l_quantity),
					row_number() over (order by sum(l_quantity) desc) as row_num,
					rank() over (order by sum(l_quantity) desc) as rank_num,
					dense_rank() over (order by sum(l_quantity) desc) as dense_rank_num
				from lineitem
				group by l_returnflag, l_linestatus with rollup
				order by sum(l_quantity) desc, l_returnflag, l_linestatus`,
			expectedHeadings: []string{"l_returnflag", "l_linestatus", "sum(l_quantity)", "row_num", "rank_num", "dense_rank_num"},
		},
		{
			name: "aggregate used only by windows",
			sql: `
				select
					l_returnflag,
					l_linestatus,
					row_number() over (order by sum(l_quantity) desc) as row_num,
					rank() over (order by sum(l_quantity) desc) as rank_num,
					dense_rank() over (order by sum(l_quantity) desc) as dense_rank_num
				from lineitem
				group by l_returnflag, l_linestatus with rollup
				order by row_num`,
			expectedHeadings: []string{"l_returnflag", "l_linestatus", "row_num", "rank_num", "dense_rank_num"},
		},
		{
			name: "window outputs only",
			sql: `
				select
					row_number() over (order by 1) as row_num,
					rank() over (order by 1) as rank_num,
					dense_rank() over (order by 1) as dense_rank_num
				from lineitem
				group by l_returnflag with rollup`,
			expectedHeadings: []string{"row_num", "rank_num", "dense_rank_num"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logicPlan, err := runOneStmt(mock, t, tc.sql)
			require.NoError(t, err)

			query := logicPlan.GetQuery()
			require.NotNil(t, query)
			require.Equal(t, tc.expectedHeadings, query.Headings)

			windowCount := 0
			windowAfterUnionCount := 0
			for _, node := range query.Nodes {
				if node.NodeType == plan.Node_WINDOW {
					windowCount++
					require.Len(t, node.Children, 1)
					if query.Nodes[node.Children[0]].NodeType == plan.Node_UNION_ALL {
						windowAfterUnionCount++
					}
				}
			}

			require.Equal(t, 3, windowCount)
			require.Equal(t, 1, windowAfterUnionCount)
		})
	}
}

func TestRollupWindowAliasCollisionsPreserveSourceScope(t *testing.T) {
	mock := NewMockOptimizer(false)
	tests := []struct {
		name               string
		sql                string
		expectedHeadings   []string
		expectedProjectLen int
		expectedHavingType int32
	}{
		{
			name: "select alias collides with source column",
			sql: `
				select n_name as n_regionkey, n_regionkey, sum(n_nationkey),
				       row_number() over (order by n_regionkey) as rn
				from nation
				group by n_name, n_regionkey with rollup`,
			expectedHeadings:   []string{"n_regionkey", "n_regionkey", "sum(n_nationkey)", "rn"},
			expectedProjectLen: 4,
		},
		{
			name: "window output alias collides with source column",
			sql: `
				select n_name, n_regionkey, sum(n_nationkey),
				       row_number() over (order by n_regionkey) as n_regionkey
				from nation
				group by n_name, n_regionkey with rollup`,
			expectedHeadings:   []string{"n_name", "n_regionkey", "sum(n_nationkey)", "n_regionkey"},
			expectedProjectLen: 4,
		},
		{
			name: "final order by window keeps source scope",
			sql: `
				select n_name as n_regionkey, n_regionkey, sum(n_nationkey)
				from nation
				group by n_name, n_regionkey with rollup
				order by row_number() over (order by n_regionkey)`,
			expectedHeadings:   []string{"n_regionkey", "n_regionkey", "sum(n_nationkey)"},
			expectedProjectLen: 3,
		},
		{
			name: "having alias collision keeps source scope",
			sql: `
				select sum(n_nationkey) as n_regionkey, n_regionkey,
				       row_number() over (order by n_regionkey) as rn
				from nation
				group by n_regionkey with rollup
				having N_REGIONKEY > 0`,
			expectedHeadings:   []string{"n_regionkey", "n_regionkey", "rn"},
			expectedProjectLen: 3,
			expectedHavingType: int32(types.T_int32),
		},
		{
			name: "qualified grouped source keeps bare having source scope",
			sql: `
				select sum(t.n_nationkey) as n_regionkey, t.n_regionkey,
				       row_number() over (order by t.n_regionkey) as rn
				from nation t
				group by t.n_regionkey with rollup
				having n_regionkey > 0`,
			expectedHeadings:   []string{"n_regionkey", "n_regionkey", "rn"},
			expectedProjectLen: 3,
			expectedHavingType: int32(types.T_int32),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			logicPlan, err := runOneStmt(mock, t, test.sql)
			require.NoError(t, err)
			query := logicPlan.GetQuery()
			require.NotNil(t, query)
			require.Equal(t, test.expectedHeadings, query.Headings)
			require.NotEmpty(t, query.Steps)

			root := query.Nodes[query.Steps[len(query.Steps)-1]]
			require.Len(t, root.ProjectList, test.expectedProjectLen)
			require.Equal(t, int32(types.T_int32), root.ProjectList[1].Typ.Id)

			foundRowNumber := false
			for _, node := range query.Nodes {
				if node.NodeType != plan.Node_WINDOW {
					continue
				}
				for _, winExpr := range node.WinSpecList {
					spec := winExpr.GetW()
					if spec == nil || spec.Name != "row_number" {
						continue
					}
					foundRowNumber = true
					require.Len(t, spec.OrderBy, 1)
					require.Equal(t, int32(types.T_int32), spec.OrderBy[0].Expr.Typ.Id)
				}
			}
			require.True(t, foundRowNumber)

			if test.expectedHavingType != 0 {
				foundHaving := false
				for _, node := range query.Nodes {
					if node.NodeType != plan.Node_FILTER || !node.RollupFilter {
						continue
					}
					for _, filter := range node.FilterList {
						fn := filter.GetF()
						require.NotNil(t, fn)
						require.Equal(t, ">", fn.Func.ObjName)
						require.NotEmpty(t, fn.Args)
						require.Equal(t, test.expectedHavingType, fn.Args[0].Typ.Id)
						foundHaving = true
					}
				}
				require.True(t, foundHaving)
			}
		})
	}
}

func TestRollupWindowHavingAliasCollisionWithHiddenGroup(t *testing.T) {
	mock := NewMockOptimizer(false)
	logicPlan, err := runOneStmt(mock, t, `
		select sum(n_nationkey) as n_regionkey,
		       row_number() over (order by sum(n_nationkey)) as rn
		from nation
		group by n_regionkey with rollup
		having N_REGIONKEY > 0`)
	require.NoError(t, err)

	query := logicPlan.GetQuery()
	require.NotNil(t, query)
	foundHaving := false
	for _, node := range query.Nodes {
		if node.NodeType != plan.Node_FILTER || !node.RollupFilter {
			continue
		}
		for _, filter := range node.FilterList {
			fn := filter.GetF()
			require.NotNil(t, fn)
			require.Equal(t, ">", fn.Func.ObjName)
			require.NotEmpty(t, fn.Args)
			require.Equal(t, int32(types.T_int32), fn.Args[0].Typ.Id)
			foundHaving = true
		}
	}
	require.True(t, foundHaving)
}

func TestRollupWindowHavingPreservesFromScopeErrors(t *testing.T) {
	mock := NewMockOptimizer(false)
	tests := []struct {
		name      string
		sql       string
		wantError string
	}{
		{
			name: "ambiguous source without window",
			sql: `
				select n1.n_regionkey
				from nation n1
				join nation n2 on n1.n_nationkey = n2.n_nationkey
				group by n1.n_regionkey with rollup
				having n_regionkey > 0`,
			wantError: "ambiguous column reference",
		},
		{
			name: "ambiguous source with window",
			sql: `
				select n1.n_regionkey,
				       row_number() over (order by n1.n_regionkey) as rn
				from nation n1
				join nation n2 on n1.n_nationkey = n2.n_nationkey
				group by n1.n_regionkey with rollup
				having n_regionkey > 0`,
			wantError: "ambiguous column reference",
		},
		{
			name: "non-grouped source without window",
			sql: `
				select sum(n_nationkey) as n_name
				from nation
				group by n_regionkey with rollup
				having n_name <> ''`,
			wantError: "must appear in the GROUP BY clause",
		},
		{
			name: "non-grouped source with window",
			sql: `
				select sum(n_nationkey) as n_name,
				       row_number() over (order by sum(n_nationkey)) as rn
				from nation
				group by n_regionkey with rollup
				having n_name <> ''`,
			wantError: "must appear in the GROUP BY clause",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := runOneStmt(mock, t, test.sql)
			require.Error(t, err)
			require.Contains(t, err.Error(), test.wantError)
		})
	}
}

func TestRollupWindowHavingAvoidsUnsafeASTVisitor(t *testing.T) {
	for _, test := range []struct {
		name string
		sql  string
	}{
		{
			name: "searched case",
			sql: `
				select n_regionkey,
				       row_number() over (order by n_regionkey) as rn
				from nation
				group by n_regionkey with rollup
				having case when n_regionkey > 0 then 1 else 0 end = 1`,
		},
		{
			name: "prepared parameter",
			sql: `
				prepare stmt1 from select n_regionkey,
				       row_number() over (order by n_regionkey) as rn
				from nation
				group by n_regionkey with rollup
				having sum(n_nationkey) > ?`,
		},
		{
			name: "case insensitive name in expression",
			sql: `
				select n_regionkey,
				       row_number() over (order by n_regionkey) as rn
				from nation
				group by n_regionkey with rollup
				having abs(N_REGIONKEY) >= 0`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			mock := NewMockOptimizer(false)
			_, err := runOneStmt(mock, t, test.sql)
			require.NoError(t, err)
		})
	}
}

func TestRollupWindowVolatileExprOccurrencesStayIndependent(t *testing.T) {
	stmt := mustParseRollupWindowSelect(t, `
		select rand() as r1, rand() as r2,
		       row_number() over (order by rand()) as rn
		from nation
		group by n_regionkey with rollup`)
	clause := stmt.Select.(*tree.SelectClause)
	expandRollupGroupByForTest(clause.GroupBy)

	rewritten, ok := rewriteRollupWindowSelect(clause, stmt.OrderBy, stmt.Limit, stmt.RankOption)
	require.True(t, ok)
	require.NotNil(t, rewritten)

	outerClause := rewritten.Select.(*tree.SelectClause)
	require.Len(t, outerClause.Exprs, 3)
	firstRand, ok := outerClause.Exprs[0].Expr.(*tree.UnresolvedName)
	require.True(t, ok)
	secondRand, ok := outerClause.Exprs[1].Expr.(*tree.UnresolvedName)
	require.True(t, ok)
	require.NotEqual(t, firstRand.ColNameOrigin(), secondRand.ColNameOrigin())

	rowNumber, ok := outerClause.Exprs[2].Expr.(*tree.FuncExpr)
	require.True(t, ok)
	require.NotNil(t, rowNumber.WindowSpec)
	require.Len(t, rowNumber.WindowSpec.OrderBy, 1)
	_, ok = rowNumber.WindowSpec.OrderBy[0].Expr.(*tree.FuncExpr)
	require.True(t, ok, "the window's rand occurrence must not reuse either SELECT projection")
}

func TestRewriteRollupWindowSelectHelpers(t *testing.T) {
	stmt := mustParseRollupWindowSelect(t, `
		select
			l_returnflag as flag,
			l_linestatus as status,
			sum(l_quantity) as total_qty,
			row_number() over (order by sum(l_quantity) desc) as rn
		from lineitem
		group by l_returnflag, l_linestatus with rollup
		having sum(l_quantity) > 0
		order by sum(l_quantity) desc
		limit 10`)
	clause := stmt.Select.(*tree.SelectClause)
	expandRollupGroupByForTest(clause.GroupBy)

	rewritten, ok := rewriteRollupWindowSelect(clause, stmt.OrderBy, stmt.Limit, stmt.RankOption)
	require.True(t, ok)
	require.NotNil(t, rewritten)
	require.NotNil(t, rewritten.Limit)
	require.Len(t, rewritten.OrderBy, 1)
	require.Contains(t, tree.String(rewritten, dialect.MYSQL), "__mo_rollup_window")
	require.Contains(t, tree.String(rewritten, dialect.MYSQL), "total_qty")

	state := newRollupWindowRewriteState(clause.Exprs)
	outerExprs, ok := buildRollupWindowSelectExprs(clause.Exprs, state)
	require.True(t, ok)
	require.Len(t, state.innerExprs, 4)
	require.Len(t, outerExprs, 4)
	for _, innerExpr := range state.innerExprs {
		require.NotNil(t, innerExpr.As)
		require.True(t, strings.HasPrefix(innerExpr.As.Origin(), rollupWindowInternalAliasPrefix))
	}

	alias, ok := state.lookupExprAlias(clause.Exprs[0].Expr)
	require.True(t, ok)
	require.True(t, strings.HasPrefix(alias, rollupWindowInternalAliasPrefix))
	_, ok = state.lookupExprAlias(tree.NewUnresolvedColName("l_returnflag"))
	require.False(t, ok)
	_, ok = state.lookupExprAlias(tree.NewUnresolvedColName("flag"))
	require.False(t, ok)

	state.addHavingAliasExprs(clause.Exprs)
	require.Len(t, state.innerExprs, 7)
	require.Equal(t, "flag", state.innerExprs[4].As.Origin())
	require.Equal(t, "status", state.innerExprs[5].As.Origin())
	require.Equal(t, "total_qty", state.innerExprs[6].As.Origin())
}

func TestRewriteRollupWindowSelectGuards(t *testing.T) {
	_, ok := rewriteRollupWindowSelect(nil, nil, nil, nil)
	require.False(t, ok)

	noGroup := mustParseRollupWindowSelect(t, "select a, row_number() over () from t")
	_, ok = rewriteRollupWindowSelect(noGroup.Select.(*tree.SelectClause), noGroup.OrderBy, noGroup.Limit, noGroup.RankOption)
	require.True(t, ok)

	distinct := mustParseRollupWindowSelect(t, "select distinct a, row_number() over () from t group by a, b with rollup")
	distinctClause := distinct.Select.(*tree.SelectClause)
	expandRollupGroupByForTest(distinctClause.GroupBy)
	rewritten, ok := rewriteRollupWindowSelect(distinctClause, distinct.OrderBy, distinct.Limit, distinct.RankOption)
	require.True(t, ok)
	require.Nil(t, rewritten)

	havingWindow := mustParseRollupWindowSelect(t, "select a, b, row_number() over () from t group by a, b with rollup having row_number() over () > 0")
	havingWindowClause := havingWindow.Select.(*tree.SelectClause)
	expandRollupGroupByForTest(havingWindowClause.GroupBy)
	rewritten, ok = rewriteRollupWindowSelect(havingWindowClause, havingWindow.OrderBy, havingWindow.Limit, havingWindow.RankOption)
	require.True(t, ok)
	require.Nil(t, rewritten)

	oneGroup := mustParseRollupWindowSelect(t, "select a, row_number() over () from t group by a")
	_, ok = rewriteRollupWindowSelect(oneGroup.Select.(*tree.SelectClause), oneGroup.OrderBy, oneGroup.Limit, oneGroup.RankOption)
	require.True(t, ok)

	state := newRollupWindowRewriteState(nil)
	_, ok = buildRollupWindowSelectExprs(nil, state)
	require.False(t, ok)
	star := mustParseRollupWindowSelect(t, "select *, row_number() over () from t")
	starExprs := star.Select.(*tree.SelectClause).Exprs
	state = newRollupWindowRewriteState(starExprs)
	_, ok = buildRollupWindowSelectExprs(starExprs, state)
	require.False(t, ok)

	noWindow := mustParseRollupWindowSelect(t, "select a from t")
	_, ok = rewriteRollupWindowSelect(noWindow.Select.(*tree.SelectClause), noWindow.OrderBy, noWindow.Limit, noWindow.RankOption)
	require.False(t, ok)

	complexInnerExpr := mustParseRollupWindowSelect(t, "select a + b, row_number() over () from t")
	complexExprs := complexInnerExpr.Select.(*tree.SelectClause).Exprs
	state = newRollupWindowRewriteState(complexExprs)
	_, ok = buildRollupWindowSelectExprs(complexExprs, state)
	require.True(t, ok)
	require.Len(t, state.innerExprs, 1)

	windowBeforeAlias := mustParseRollupWindowSelect(t, "select row_number() over (order by total) as rn, sum(a) as total from t")
	windowBeforeAliasExprs := windowBeforeAlias.Select.(*tree.SelectClause).Exprs
	state = newRollupWindowRewriteState(windowBeforeAliasExprs)
	outerExprs, ok := buildRollupWindowSelectExprs(windowBeforeAliasExprs, state)
	require.True(t, ok)
	require.Len(t, state.innerExprs, 2)
	aggregateAlias := state.innerExprs[0].As.Origin()
	sourceAlias := state.innerExprs[1].As.Origin()
	rewrittenWindow := tree.String(outerExprs[0].Expr, dialect.MYSQL)
	require.Contains(t, rewrittenWindow, sourceAlias)
	require.NotContains(t, rewrittenWindow, aggregateAlias)

	duplicateAlias := mustParseRollupWindowSelect(t, "select a as x, b as x, row_number() over () from t")
	duplicateExprs := duplicateAlias.Select.(*tree.SelectClause).Exprs
	state = newRollupWindowRewriteState(duplicateExprs)
	_, ok = buildRollupWindowSelectExprs(duplicateExprs, state)
	require.True(t, ok)
	require.Len(t, state.innerExprs, 2)

	state = newRollupWindowRewriteState(nil)
	upperLiteralAlias, ok := state.ensureInnerExpr(mustParseRollupWindowExpr(t, "'A'"))
	require.True(t, ok)
	lowerLiteralAlias, ok := state.ensureInnerExpr(mustParseRollupWindowExpr(t, "'a'"))
	require.True(t, ok)
	require.NotEqual(t, upperLiteralAlias, lowerLiteralAlias)

	parenAggregate := mustParseRollupWindowSelect(t, "select (sum(a)), row_number() over () from t")
	require.Equal(t, "sum(a)", rollupWindowOutputAlias(parenAggregate.Select.(*tree.SelectClause).Exprs[0]).Origin())
}

func TestRewriteRollupWindowUnsupportedDoesNotFallback(t *testing.T) {
	mock := NewMockOptimizer(false)
	_, err := runOneStmt(
		mock,
		t,
		"select distinct a, row_number() over () from nation group by a, n_regionkey with rollup",
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "window functions with ROLLUP or CUBE for this expression")
}

func TestRewriteRollupWindowExprSupportedShapes(t *testing.T) {
	state := rollupWindowAliasMap(
		"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m",
	)

	complexExpr := mustParseRollupWindowExpr(t, `
		case a
			when 1 then not (
				((b + -c) > cast(d as signed)) and (e like f escape g)
				xor h = i
				or (j, k) = (l, m)
			)
			else 'fallback'
		end`)
	rewritten, ok := rewriteRollupWindowExpr(complexExpr, state)
	require.True(t, ok)
	rewrittenSQL := tree.String(rewritten, dialect.MYSQL)
	require.Contains(t, rewrittenSQL, "a_alias")
	require.Contains(t, rewrittenSQL, "m_alias")

	windowExpr := mustParseRollupWindowExpr(t, `
		sum(a) over (
			partition by b
			order by c
			rows between 1 preceding and 2 following
		)`)
	windowState := newRollupWindowRewriteState(nil)
	rewritten, ok = rewriteRollupWindowExpr(windowExpr, windowState)
	require.True(t, ok)
	rewrittenSQL = tree.String(rewritten, dialect.MYSQL)
	require.Len(t, windowState.innerExprs, 3)
	require.Contains(t, rewrittenSQL, windowState.innerExprs[0].As.Origin())
	require.Contains(t, rewrittenSQL, "partition by "+windowState.innerExprs[1].As.Origin())
	require.Contains(t, rewrittenSQL, "order by "+windowState.innerExprs[2].As.Origin())

	state = newRollupWindowRewriteState(nil)
	aggregateExpr := mustParseRollupWindowExpr(t, "sum(missing)")
	rewritten, ok = rewriteRollupWindowExpr(aggregateExpr, state)
	require.True(t, ok)
	require.True(t, strings.HasPrefix(tree.String(rewritten, dialect.MYSQL), rollupWindowInternalAliasPrefix))
	require.Len(t, state.innerExprs, 1)

	state = rollupWindowAliasMap("a", "b")
	tupleExprs, ok := rewriteRollupWindowExprs(tree.Exprs{tree.NewUnresolvedColName("a"), tree.NewUnresolvedColName("b")}, state)
	require.True(t, ok)
	require.Len(t, tupleExprs, 2)

	emptyExprs, ok := rewriteRollupWindowExprs(nil, state)
	require.True(t, ok)
	require.Nil(t, emptyExprs)

	state = newRollupWindowRewriteState(nil)
	orderBy, ok := rewriteRollupWindowOrderBy(tree.OrderBy{{Expr: tree.NewUnresolvedColName("another_missing")}}, state)
	require.True(t, ok)
	require.Len(t, orderBy, 1)
	require.True(t, strings.HasPrefix(tree.String(orderBy[0].Expr, dialect.MYSQL), rollupWindowInternalAliasPrefix))
}

func TestRollupWindowExprContainsWindowRecursiveShapes(t *testing.T) {
	for _, tc := range []struct {
		name string
		expr string
	}{
		{name: "func args", expr: "coalesce(sum(a) over (), b)"},
		{name: "func order by", expr: "group_concat(a order by sum(b) over ())"},
		{name: "binary", expr: "a + sum(b) over ()"},
		{name: "unary", expr: "-sum(a) over ()"},
		{name: "comparison", expr: "sum(a) over () > 0"},
		{name: "and", expr: "sum(a) over () and b"},
		{name: "xor", expr: "a xor sum(b) over ()"},
		{name: "or", expr: "a or sum(b) over ()"},
		{name: "not", expr: "not sum(a) over ()"},
		{name: "is null", expr: "sum(a) over () is null"},
		{name: "is not null", expr: "sum(a) over () is not null"},
		{name: "paren", expr: "(sum(a) over ())"},
		{name: "cast", expr: "cast(sum(a) over () as signed)"},
		{name: "tuple", expr: "(a, sum(b) over ())"},
		{name: "between", expr: "sum(a) over () between 1 and 2"},
		{name: "case expr", expr: "case sum(a) over () when 1 then b else c end"},
		{name: "case when", expr: "case when a then sum(b) over () else c end"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.True(t, rollupWindowExprContainsWindow(mustParseRollupWindowExpr(t, tc.expr)))
		})
	}

	require.False(t, rollupWindowExprContainsWindow(mustParseRollupWindowExpr(t, "case when a then b else c end")))
}

func mustParseRollupWindowSelect(t *testing.T, sql string) *tree.Select {
	t.Helper()
	stmts, err := mysql.Parse(context.Background(), sql, 1)
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	stmt, ok := stmts[0].(*tree.Select)
	require.True(t, ok)
	return stmt
}

func mustParseRollupWindowExpr(t *testing.T, expr string) tree.Expr {
	t.Helper()
	stmt := mustParseRollupWindowSelect(t, "select "+expr)
	clause, ok := stmt.Select.(*tree.SelectClause)
	require.True(t, ok)
	require.Len(t, clause.Exprs, 1)
	return clause.Exprs[0].Expr
}

func rollupWindowAliasMap(cols ...string) *rollupWindowRewriteState {
	state := newRollupWindowRewriteState(nil)
	state.activeNameAliases = make(map[string]string, len(cols))
	for _, col := range cols {
		state.activeNameAliases[col] = col + "_alias"
	}
	return state
}

func expandRollupGroupByForTest(groupBy *tree.GroupByClause) {
	if groupBy == nil || !groupBy.Rollup || len(groupBy.GroupByExprsList) == 0 {
		return
	}
	for i := len(groupBy.GroupByExprsList[0]) - 1; i > 0; i-- {
		groupBy.GroupByExprsList = append(groupBy.GroupByExprsList, groupBy.GroupByExprsList[0][0:i])
	}
	groupBy.GroupByExprsList = append(groupBy.GroupByExprsList, nil)
}

func TestOnlyFullGroupByAllowsCorrelatedSubqueryOnGroupedColumn(t *testing.T) {
	sqls := []string{
		`
		SELECT n_name,
		       (SELECT COUNT(*) FROM nation2 n2 WHERE n2.n_name = nation.n_name) AS c
		FROM nation
		GROUP BY n_name
		ORDER BY n_name`,
		`
		SELECT n_name
		FROM nation
		GROUP BY n_name
		HAVING (SELECT COUNT(*) FROM nation2 n2 WHERE n2.n_name = nation.n_name) > 0`,
		`
		SELECT n_name
		FROM nation
		GROUP BY n_name
		ORDER BY (SELECT COUNT(*) FROM nation2 n2 WHERE n2.n_name = nation.n_name)`,
		`
		SELECT n.n_name,
		       (SELECT COUNT(*) FROM nation2 n2 WHERE n2.n_name = n.n_name) AS c
		FROM nation n
		GROUP BY n.n_name`,
		`
		SELECT n_name,
		       (SELECT COUNT(*) FROM nation2 n2 WHERE n2.n_name = nation.n_name) AS c
		FROM nation
		GROUP BY 1`,
		`
		SELECT n_name AS name,
		       (SELECT COUNT(*) FROM nation2 n2 WHERE n2.n_name = nation.n_name) AS c
		FROM nation
		GROUP BY name`,
		`
			SELECT n_regionkey
			FROM nation
			GROUP BY n_regionkey
			HAVING EXISTS (
				SELECT n_name
				FROM nation2
				GROUP BY n_name
				HAVING COUNT(*) > nation.n_regionkey
			)`,
		`
			SELECT n_nationkey,
			       EXISTS (
			           SELECT n_name
			           FROM nation2
			           GROUP BY n_name
			           HAVING COUNT(*) > nation.n_regionkey
			       )
			FROM nation
			GROUP BY n_nationkey`,
		`
			SELECT n_name,
			       EXISTS (
			           SELECT n_name
			           FROM nation2
			           GROUP BY n_name
			           HAVING COUNT(*) > nation.n_regionkey
			       )
			FROM nation
			WHERE n_regionkey = 1
			GROUP BY n_name`,
	}

	for _, sql := range sqls {
		mock := NewMockOptimizer(false)
		_, err := runOneStmt(mock, t, sql)
		require.NoError(t, err, sql)
	}
}

func TestOnlyFullGroupByAllowsCorrelatedHavingOnUngroupedOuterQuery(t *testing.T) {
	sqls := []string{
		`
		SELECT nation.n_regionkey,
		       EXISTS (
		           SELECT nation2.n_name
		           FROM nation2
		           GROUP BY nation2.n_name
		           HAVING COUNT(*) >= nation.n_regionkey
		       ) AS ex
		FROM nation
		ORDER BY nation.n_regionkey`,
		`
		SELECT nation.n_regionkey,
		       EXISTS (
		           SELECT 1
		           FROM nation2
		           HAVING COUNT(*) >= nation.n_regionkey
		       ) AS ex
		FROM nation
		ORDER BY nation.n_regionkey`,
		`
			SELECT nation.n_regionkey,
			       EXISTS (
			           SELECT n_nationkey
			           FROM nation2
			           GROUP BY n_nationkey
			           HAVING n_nationkey >= nation.n_regionkey
			       ) AS ex
			FROM nation
			ORDER BY nation.n_regionkey`,
		`
			SELECT n_regionkey
			FROM nation
			HAVING EXISTS (
			    SELECT n_name
			    FROM nation2
			    GROUP BY n_name
			    HAVING COUNT(*) > nation.n_regionkey
			)`,
		`
			SELECT MAX(EXISTS (
			    SELECT n_nationkey
			    FROM nation2
			    GROUP BY n_nationkey
			    HAVING n_nationkey >= nation.n_regionkey
			))
			FROM nation`,
		`
			SELECT 1
			FROM nation
			HAVING MAX(EXISTS (
			    SELECT n_nationkey
			    FROM nation2
			    GROUP BY n_nationkey
			    HAVING n_nationkey >= nation.n_regionkey
			))`,
		`
			SELECT n_nationkey,
			       MAX(EXISTS (
			           SELECT n_name
			           FROM nation2
			           GROUP BY n_name
			           HAVING COUNT(*) >= nation.n_regionkey
			       ))
			FROM nation
			GROUP BY n_nationkey`,
		`
			SELECT n_nationkey
			FROM nation
			GROUP BY n_nationkey
			HAVING MAX(EXISTS (
			    SELECT n_name
			    FROM nation2
			    GROUP BY n_name
			    HAVING COUNT(*) >= nation.n_regionkey
			))`,
		`
			SELECT SUM(n_nationkey),
			       EXISTS (
			           SELECT n_name
			           FROM nation2
			           GROUP BY n_name
			           HAVING COUNT(*) > nation.n_regionkey
			       )
			FROM nation
			WHERE n_regionkey = 1`,
		`
			SELECT EXISTS (
			           SELECT n_name
			           FROM nation2
			           GROUP BY n_name
			           HAVING COUNT(*) > nation.n_regionkey
			       ),
			       SUM(n_nationkey)
			FROM nation
			WHERE n_regionkey = 1`,
		`
			SELECT MAX(EXISTS (
			    SELECT n_name
			    FROM nation2
			    GROUP BY n_name
			    HAVING COUNT(*) >= nation.n_regionkey
			))
			FROM nation
			WHERE n_regionkey = 1`,
		`
			SELECT SUM(n_nationkey)
			FROM nation
			WHERE EXISTS (
			    SELECT n_name
			    FROM nation2
			    GROUP BY n_name
			    HAVING COUNT(*) > nation.n_regionkey
			)`,
		`
			SELECT SUM(n.n_nationkey)
			FROM nation n
			JOIN region r ON EXISTS (
			    SELECT n2.n_name
			    FROM nation2 n2
			    GROUP BY n2.n_name
			    HAVING COUNT(*) > n.n_regionkey
			)`,
		`
			SELECT 1
			FROM nation
			WHERE n_regionkey = 1
			HAVING MAX(EXISTS (
			    SELECT n_name
			    FROM nation2
			    GROUP BY n_name
			    HAVING COUNT(*) >= nation.n_regionkey
			))`,
	}

	for _, sql := range sqls {
		mock := NewMockOptimizer(false)
		_, err := runOneStmt(mock, t, sql)
		require.NoError(t, err, sql)
	}
}

func TestOnlyFullGroupByNonAggregateHavingBuildsFilter(t *testing.T) {
	mock := NewMockOptimizer(false)
	p, err := runOneStmt(mock, t, `
		SELECT n_regionkey
		FROM nation
		HAVING EXISTS (
		    SELECT n_name
		    FROM nation2
		    GROUP BY n_name
		    HAVING COUNT(*) > nation.n_regionkey
		)`)
	require.NoError(t, err)

	found := false
	for _, node := range p.GetQuery().Nodes {
		if node.NodeType == plan.Node_FILTER && len(node.FilterList) > 0 {
			found = true
			break
		}
	}
	require.True(t, found)
}

func TestOnlyFullGroupByAllowsNonAggregateHavingOnInformationSchemaView(t *testing.T) {
	mock := NewMockOptimizer(false)
	mock.ctxt.SetSqlModeOverride("ONLY_FULL_GROUP_BY")
	p, err := runOneStmt(mock, t, `
		SELECT TABLE_SCHEMA AS TABLE_CAT,
		       NULL AS TABLE_SCHEM,
		       TABLE_NAME,
		       CASE
		           WHEN TABLE_TYPE = 'BASE TABLE' THEN
		               CASE
		                   WHEN TABLE_SCHEMA = 'mysql'
		                       OR TABLE_SCHEMA = 'performance_schema'
		                   THEN 'SYSTEM TABLE'
		                   ELSE 'TABLE'
		               END
		           WHEN TABLE_TYPE = 'TEMPORARY' THEN 'LOCAL_TEMPORARY'
		           ELSE TABLE_TYPE
		       END AS TABLE_TYPE,
		       TABLE_COMMENT AS REMARKS,
		       NULL AS TYPE_CAT,
		       NULL AS TYPE_SCHEM,
		       NULL AS TYPE_NAME,
		       NULL AS SELF_REFERENCING_COL_NAME,
		       NULL AS REF_GENERATION
		FROM information_schema.tables
		WHERE TABLE_SCHEMA = 'benchbase'
		HAVING TABLE_TYPE IN ('TABLE', NULL, NULL, NULL, NULL)
		ORDER BY TABLE_TYPE, TABLE_SCHEMA, TABLE_NAME`)
	require.NoError(t, err)
	for _, node := range p.GetQuery().Nodes {
		require.NotEqual(t, plan.Node_AGG, node.NodeType)
	}
}

func TestOnlyFullGroupByRejectsNonAggregateHavingAnonymousExpression(t *testing.T) {
	mock := NewMockOptimizer(false)
	mock.ctxt.SetSqlModeOverride("ONLY_FULL_GROUP_BY")
	_, err := runOneStmt(mock, t, `
		SELECT n_regionkey + 1
		FROM nation
		HAVING n_regionkey + 1 > 1`)
	require.ErrorContains(t, err, "must appear in the GROUP BY clause")
}

func TestOnlyFullGroupByAllowsNonAggregateHavingDirectAlias(t *testing.T) {
	mock := NewMockOptimizer(false)
	mock.ctxt.SetSqlModeOverride("ONLY_FULL_GROUP_BY")
	_, err := runOneStmt(mock, t, `
		SELECT n_regionkey AS region_key
		FROM nation
		HAVING region_key > 0`)
	require.NoError(t, err)
}

func TestOnlyFullGroupByAllowsNonAggregateHavingDirectColumn(t *testing.T) {
	mock := NewMockOptimizer(false)
	mock.ctxt.SetSqlModeOverride("ONLY_FULL_GROUP_BY")
	_, err := runOneStmt(mock, t, `
		SELECT n_regionkey
		FROM nation
		HAVING n_regionkey > 0`)
	require.NoError(t, err)
}

func TestOnlyFullGroupByRejectsExplicitImplicitHavingNameCollision(t *testing.T) {
	mock := NewMockOptimizer(false)
	mock.ctxt.SetSqlModeOverride("ONLY_FULL_GROUP_BY")
	_, err := runOneStmt(mock, t, `
		SELECT n_nationkey AS n_regionkey, n_regionkey
		FROM nation
		HAVING n_regionkey > 0`)
	require.ErrorContains(t, err, "Column 'n_regionkey' in having clause is ambiguous")
}

func TestOnlyFullGroupByAllowsEquivalentExplicitImplicitHavingName(t *testing.T) {
	mock := NewMockOptimizer(false)
	mock.ctxt.SetSqlModeOverride("ONLY_FULL_GROUP_BY")
	_, err := runOneStmt(mock, t, `
		SELECT n_regionkey AS n_regionkey, n_regionkey
		FROM nation
		HAVING n_regionkey > 0`)
	require.NoError(t, err)
}

func TestOnlyFullGroupByExplicitHavingAliasPrecedesUnprojectedSource(t *testing.T) {
	mock := NewMockOptimizer(false)
	mock.ctxt.SetSqlModeOverride("ONLY_FULL_GROUP_BY")
	_, err := runOneStmt(mock, t, `
		SELECT n_nationkey AS n_regionkey
		FROM nation
		HAVING n_regionkey > 0`)
	require.NoError(t, err)
}

func TestOnlyFullGroupByAllowsProjectedSourceWithDifferentHavingAlias(t *testing.T) {
	mock := NewMockOptimizer(false)
	mock.ctxt.SetSqlModeOverride("ONLY_FULL_GROUP_BY")
	_, err := runOneStmt(mock, t, `
		SELECT n_regionkey AS region_key
		FROM nation
		HAVING n_regionkey > 0`)
	require.NoError(t, err)
}

func TestOnlyFullGroupByRejectsUnprojectedOrEmbeddedHavingSource(t *testing.T) {
	for _, sql := range []string{
		`SELECT n_regionkey AS region_key
		 FROM nation
		 HAVING n_nationkey > 0`,
		`SELECT n_regionkey + 1 AS region_key
		 FROM nation
		 HAVING n_regionkey > 0`,
	} {
		mock := NewMockOptimizer(false)
		mock.ctxt.SetSqlModeOverride("ONLY_FULL_GROUP_BY")
		_, err := runOneStmt(mock, t, sql)
		require.ErrorContains(t, err, "must appear in the GROUP BY clause", sql)
	}
}

func TestOnlyFullGroupByRejectsAmbiguousProjectedSourceName(t *testing.T) {
	mock := NewMockOptimizer(false)
	mock.ctxt.SetSqlModeOverride("ONLY_FULL_GROUP_BY")
	_, err := runOneStmt(mock, t, `
		SELECT n1.n_regionkey AS region_key
		FROM nation n1
		JOIN nation n2 ON n1.n_nationkey = n2.n_nationkey
		HAVING n_regionkey > 0`)
	require.ErrorContains(t, err, "ambiguous column reference")
}

func TestOnlyFullGroupByAllowsUnaryPlusImplicitHavingName(t *testing.T) {
	mock := NewMockOptimizer(false)
	mock.ctxt.SetSqlModeOverride("ONLY_FULL_GROUP_BY")
	_, err := runOneStmt(mock, t, `
		SELECT +n_regionkey
		FROM nation
		HAVING n_regionkey > 0`)
	require.NoError(t, err)
}

func TestOnlyFullGroupByAllowsNestedUnaryPlusImplicitHavingName(t *testing.T) {
	mock := NewMockOptimizer(false)
	mock.ctxt.SetSqlModeOverride("ONLY_FULL_GROUP_BY")
	_, err := runOneStmt(mock, t, `
		SELECT ++n_regionkey
		FROM nation
		HAVING n_regionkey > 0`)
	require.NoError(t, err)
}

func TestOnlyFullGroupByAllowsUnaryPlusQualifiedHavingProjectedColumn(t *testing.T) {
	mock := NewMockOptimizer(false)
	mock.ctxt.SetSqlModeOverride("ONLY_FULL_GROUP_BY")
	_, err := runOneStmt(mock, t, `
		SELECT +nation.n_regionkey
		FROM nation
		HAVING nation.n_regionkey > 0`)
	require.NoError(t, err)
}

func TestOnlyFullGroupByAllowsEquivalentUnaryPlusHavingOutputs(t *testing.T) {
	mock := NewMockOptimizer(false)
	mock.ctxt.SetSqlModeOverride("ONLY_FULL_GROUP_BY")
	_, err := runOneStmt(mock, t, `
		SELECT +n_regionkey AS n_regionkey, n_regionkey
		FROM nation
		HAVING n_regionkey > 0`)
	require.NoError(t, err)
}

func TestOnlyFullGroupByAllowsQualifiedHavingProjectedColumn(t *testing.T) {
	for _, sql := range []string{
		`SELECT nation.n_regionkey
		 FROM nation
		 HAVING nation.n_regionkey > 0`,
		`SELECT n_regionkey AS region_key
		 FROM nation
		 HAVING nation.n_regionkey > 0`,
		`SELECT nation.n_regionkey
		 FROM tpch.nation
		 HAVING tpch.nation.n_regionkey > 0`,
		`SELECT tpch.nation.n_regionkey
		 FROM tpch.nation
		 HAVING nation.n_regionkey > 0`,
	} {
		mock := NewMockOptimizer(false)
		mock.ctxt.SetSqlModeOverride("ONLY_FULL_GROUP_BY")
		_, err := runOneStmt(mock, t, sql)
		require.NoError(t, err, sql)
	}
}

func TestOnlyFullGroupByRejectsQualifiedColumnInsideAnonymousProjection(t *testing.T) {
	mock := NewMockOptimizer(false)
	mock.ctxt.SetSqlModeOverride("ONLY_FULL_GROUP_BY")
	_, err := runOneStmt(mock, t, `
		SELECT n_regionkey + 1 AS region_key
		FROM nation
		HAVING nation.n_regionkey > 0`)
	require.ErrorContains(t, err, "must appear in the GROUP BY clause")
}

func TestOnlyFullGroupByRejectsAmbiguousHavingAlias(t *testing.T) {
	mock := NewMockOptimizer(false)
	mock.ctxt.SetSqlModeOverride("ONLY_FULL_GROUP_BY")
	_, err := runOneStmt(mock, t, `
		SELECT n_regionkey AS x, n_nationkey AS x
		FROM nation
		HAVING x > 0`)
	require.ErrorContains(t, err, "Column 'x' in having clause is ambiguous")
}

func TestOnlyFullGroupByAllowsEquivalentDuplicateHavingAlias(t *testing.T) {
	mock := NewMockOptimizer(false)
	mock.ctxt.SetSqlModeOverride("ONLY_FULL_GROUP_BY")
	_, err := runOneStmt(mock, t, `
		SELECT n_regionkey AS x, n_regionkey AS x
		FROM nation
		HAVING x > 0`)
	require.NoError(t, err)
}

func TestMatrixOneNativeStillRejectsNonAggregateHavingColumn(t *testing.T) {
	mock := NewMockOptimizer(false)
	mock.ctxt.SetSqlModeOverride("ONLY_FULL_GROUP_BY,MATRIXONE_NATIVE")
	_, err := runOneStmt(mock, t, `
		SELECT n_regionkey
		FROM nation
		HAVING n_nationkey > 0`)
	require.ErrorContains(t, err, "must appear in the GROUP BY clause")
}

func TestOnlyFullGroupByWindowOnlyHavingBuildsPreWindowFilter(t *testing.T) {
	mock := NewMockOptimizer(false)
	p, err := runOneStmt(mock, t, `
		SELECT ROW_NUMBER() OVER ()
		FROM nation
		HAVING rand() > -1`)
	require.NoError(t, err)

	query := p.GetQuery()
	found := false
	for _, node := range query.Nodes {
		if node.NodeType != plan.Node_WINDOW || len(node.Children) == 0 {
			continue
		}
		if planSubtreeHasFilter(query, node.Children[0]) {
			found = true
			break
		}
	}
	require.True(t, found)
}

func TestForUpdateLocksAfterCorrelatedNonAggregateHaving(t *testing.T) {
	mock := NewMockOptimizer(false)
	p, err := runOneStmt(mock, t, `
		SELECT n_regionkey
		FROM nation
		HAVING EXISTS (
		    SELECT n_name
		    FROM nation2
		    GROUP BY n_name
		    HAVING COUNT(*) > nation.n_regionkey
		)
		FOR UPDATE`)
	require.NoError(t, err)

	query := p.GetQuery()
	var lockNode *plan.Node
	for _, node := range query.Nodes {
		if node.NodeType == plan.Node_LOCK_OP {
			lockNode = node
			break
		}
	}
	require.NotNil(t, lockNode)
	require.Len(t, lockNode.Children, 1)

	// The correlated HAVING is flattened into a MARK JOIN and a FILTER.  The
	// lock must consume that filtered row set, rather than the raw outer scan.
	lockedInput := query.Nodes[lockNode.Children[0]]
	require.Equal(t, plan.Node_FILTER, lockedInput.NodeType)
	require.True(t, lockedInput.FilterIsBarrier)
	require.NotEmpty(t, lockedInput.Children)
	require.True(t, planSubtreeHasNodeType(query, lockedInput.Children[0], plan.Node_JOIN))
	require.True(t, planSubtreeHasJoinType(query, lockedInput.Children[0], plan.Node_MARK))
}

func planSubtreeHasNodeType(query *plan.Query, nodeID int32, nodeType plan.Node_NodeType) bool {
	if nodeID < 0 || int(nodeID) >= len(query.Nodes) {
		return false
	}
	node := query.Nodes[nodeID]
	if node.NodeType == nodeType {
		return true
	}
	for _, childID := range node.Children {
		if planSubtreeHasNodeType(query, childID, nodeType) {
			return true
		}
	}
	return false
}

func planSubtreeHasJoinType(query *plan.Query, nodeID int32, joinType plan.Node_JoinType) bool {
	if nodeID < 0 || int(nodeID) >= len(query.Nodes) {
		return false
	}
	node := query.Nodes[nodeID]
	if node.NodeType == plan.Node_JOIN && node.JoinType == joinType {
		return true
	}
	for _, childID := range node.Children {
		if planSubtreeHasJoinType(query, childID, joinType) {
			return true
		}
	}
	return false
}

func planSubtreeHasFilter(query *plan.Query, nodeID int32) bool {
	if nodeID < 0 || int(nodeID) >= len(query.Nodes) {
		return false
	}
	node := query.Nodes[nodeID]
	if len(node.FilterList) > 0 {
		return true
	}
	for _, childID := range node.Children {
		if planSubtreeHasFilter(query, childID) {
			return true
		}
	}
	return false
}

func TestOnlyFullGroupByRejectsCorrelatedSubqueryOnUngroupedColumn(t *testing.T) {
	sqls := []struct {
		sql         string
		errContains string
	}{
		{`
		SELECT n_name,
		       (SELECT COUNT(*) FROM nation2 n2 WHERE n2.n_name = nation.n_comment) AS c
		FROM nation
		GROUP BY n_name`, "nation.n_comment"},
		{`
		SELECT n_name
			FROM nation
			GROUP BY n_name
			HAVING (SELECT COUNT(*) FROM nation2 n2 WHERE n2.n_name = nation.n_comment) > 0`, "nation.n_comment"},
		{`
			SELECT SUM(n_regionkey),
			       EXISTS (
			           SELECT n_name
			           FROM nation2
			           GROUP BY n_name
			           HAVING COUNT(*) > nation.n_regionkey
			       )
			FROM nation`, "nation.n_regionkey"},
		{`
			SELECT EXISTS (
			           SELECT n_name
			           FROM nation2
			           GROUP BY n_name
			           HAVING COUNT(*) > nation.n_regionkey
			       ),
			       SUM(n_regionkey)
			FROM nation`, "nation.n_regionkey"},
		{`
			SELECT SUM(n_regionkey)
			FROM nation
			HAVING EXISTS (
			    SELECT n_name
			    FROM nation2
			    GROUP BY n_name
			    HAVING COUNT(*) > nation.n_comment
			)`, "nation.n_comment"},
		{`
			SELECT 1
			FROM nation
			HAVING EXISTS (
			    SELECT n_name
			    FROM nation2
			    GROUP BY n_name
			    HAVING COUNT(*) > nation.n_comment
			)
			ORDER BY SUM(n_regionkey)`, "nation.n_comment"},
		{`
			SELECT 1
			FROM nation
			HAVING EXISTS (
			    SELECT n_name
			    FROM nation2
			    GROUP BY n_name
			    HAVING COUNT(*) > nation.n_comment
			) AND SUM(n_regionkey) > 0`, "nation.n_comment"},
		{`
			SELECT SUM(SUM(n_regionkey)) OVER ()
			FROM nation
			HAVING EXISTS (
			    SELECT n_name
			    FROM nation2
			    GROUP BY n_name
			    HAVING COUNT(*) > nation.n_comment
			)`, "nation.n_comment"},
	}

	for _, tt := range sqls {
		mock := NewMockOptimizer(false)
		_, err := runOneStmt(mock, t, tt.sql)
		require.Error(t, err, tt.sql)
		require.Contains(t, err.Error(), tt.errContains)
	}
}

func TestOnlyFullGroupByPreservesCorrelatedAggregateNYI(t *testing.T) {
	sqls := []string{
		`
		SELECT (SELECT COUNT(DISTINCT nation.n_comment))
		FROM nation
		GROUP BY n_name`,
		`
		SELECT n_name
		FROM nation
		WHERE (SELECT AVG(nation.n_regionkey) FROM nation2) = 1`,
		`
		SELECT n_name,
		       (SELECT COUNT(nation.n_name) FROM nation2) AS c
		FROM nation
		GROUP BY n_name`,
		`
		SELECT n_regionkey
		FROM nation
		GROUP BY n_regionkey
		HAVING EXISTS (
			SELECT n_name
			FROM nation2
			GROUP BY n_name
			HAVING SUM(nation.n_regionkey) > 0
		)`,
	}

	for _, sql := range sqls {
		mock := NewMockOptimizer(false)
		_, err := runOneStmt(mock, t, sql)
		require.Error(t, err, sql)
		require.Contains(t, err.Error(), "correlated columns in aggregate function")
	}
}

// test join table plan building
func TestJoinTableSqlBuilder(t *testing.T) {
	mock := NewMockOptimizer(false)

	// should pass
	sqls := []string{
		"SELECT N_NAME,N_REGIONKEY FROM NATION join REGION on NATION.N_REGIONKEY = REGION.R_REGIONKEY",
		"SELECT N_NAME, N_REGIONKEY FROM NATION join REGION on NATION.N_REGIONKEY = REGION.R_REGIONKEY WHERE NATION.N_REGIONKEY > 0",
		"SELECT N_NAME, NATION2.R_REGIONKEY FROM NATION2 join REGION using(R_REGIONKEY) WHERE NATION2.R_REGIONKEY > 0",
		"SELECT N_NAME, NATION2.R_REGIONKEY FROM NATION2 NATURAL JOIN REGION WHERE NATION2.R_REGIONKEY > 0",
		"SELECT N_NAME FROM NATION NATURAL JOIN REGION",                                                                                                     //have no same column name but it's ok
		"SELECT N_NAME,N_REGIONKEY FROM NATION a join REGION b on a.N_REGIONKEY = b.R_REGIONKEY WHERE a.N_REGIONKEY > 0",                                    //test alias
		"SELECT l.L_ORDERKEY a FROM CUSTOMER c, ORDERS o, LINEITEM l WHERE c.C_CUSTKEY = o.O_CUSTKEY and l.L_ORDERKEY = o.O_ORDERKEY and o.O_ORDERKEY < 10", //join three tables
		"SELECT c.* FROM CUSTOMER c, ORDERS o, LINEITEM l WHERE c.C_CUSTKEY = o.O_CUSTKEY and l.L_ORDERKEY = o.O_ORDERKEY",                                  //test star
		"SELECT * FROM CUSTOMER c, ORDERS o, LINEITEM l WHERE c.C_CUSTKEY = o.O_CUSTKEY and l.L_ORDERKEY = o.O_ORDERKEY",                                    //test star
		"SELECT a.* FROM NATION a join REGION b on a.N_REGIONKEY = b.R_REGIONKEY WHERE a.N_REGIONKEY > 0",                                                   //test star
		"SELECT * FROM NATION a join REGION b on a.N_REGIONKEY = b.R_REGIONKEY WHERE a.N_REGIONKEY > 0",
		"SELECT N_NAME, R_REGIONKEY FROM NATION2 join REGION using(R_REGIONKEY)",
		"select nation.n_name from nation join nation2 on nation.n_name !='a' join region on nation.n_regionkey = region.r_regionkey",
		"select * from nation, nation2, region",
		"select n_name from nation dedup join region on n_regionkey = r_regionkey",
		"SELECT * FROM NATION a join REGION b on a.N_REGIONKEY = b.R_REGIONKEY WHERE a.N_REGIONKEY > 0 for update", //join for update
		"select * from nation, nation2, region for update",                                                         //multi-table for update
		"with target as (select n_nationkey from NATION order by n_nationkey limit 5) select t.n_nationkey from NATION t join target on t.n_nationkey = target.n_nationkey for update", // cte + join + for update (issue 23131)
		"select * from (select n_nationkey from NATION order by n_nationkey limit 5) t for update",                                                                                     // derived table + for update (issue 23132)
		"select n_nationkey from NATION t where exists (select 1 from REGION r where r.r_regionkey = t.n_regionkey) for update",                                                        // exists subquery + for update (issue 23133)
		"select n_nationkey from NATION where n_regionkey in (select r_regionkey from REGION) for update",                                                                              // in subquery + for update (issue 23133)
		"select n_regionkey, count(*) from NATION group by n_regionkey for update",                                                                                                     // aggregate + for update
	}
	runTestShouldPass(mock, t, sqls, false, false)

	// should error
	sqls = []string{
		"SELECT N_NAME,N_REGIONKEY FROM NATION join REGION on NATION.N_REGIONKEY = REGION.NotExistColumn",                    //column not exist
		"SELECT N_NAME, R_REGIONKEY FROM NATION join REGION using(R_REGIONKEY)",                                              //column not exist
		"SELECT N_NAME,N_REGIONKEY FROM NATION a join REGION b on a.N_REGIONKEY = b.R_REGIONKEY WHERE aaaaa.N_REGIONKEY > 0", //table alias not exist
		"select *", //No table used
	}
	runTestShouldError(mock, t, sqls)
}

func TestMySQLJoinSyntaxVariantsPlan(t *testing.T) {
	mock := NewMockOptimizer(false)
	sqls := []string{
		"SELECT * FROM { OJ NATION left outer join NATION2 on NATION.N_NATIONKEY = NATION2.N_NATIONKEY }",
		"SELECT * FROM NATION straight_join NATION2 using(N_NATIONKEY)",
	}
	runTestShouldPass(mock, t, sqls, false, false)
}

// test derived table plan building
func TestDerivedTableSqlBuilder(t *testing.T) {
	mock := NewMockOptimizer(false)
	// should pass
	sqls := []string{
		"select c_custkey from (select c_custkey from CUSTOMER ) a",
		"select c_custkey from (select c_custkey from CUSTOMER group by c_custkey ) a",
		"select col1 from (select c_custkey from CUSTOMER group by c_custkey ) a(col1)",
		"select c_custkey from (select c_custkey, count(C_NATIONKEY) ff from CUSTOMER group by c_custkey ) a where ff > 0 order by c_custkey",
		"select col1 from (select c_custkey, count(C_NATIONKEY) ff from CUSTOMER group by c_custkey ) a(col1, col2) where col2 > 0 order by col1",
		"select c_custkey from (select c_custkey, count(C_NATIONKEY) ff from CUSTOMER group by c_custkey ) a join NATION b on a.c_custkey = b.N_REGIONKEY where b.N_NATIONKEY > 10",
		"select a.* from (select c_custkey, count(C_NATIONKEY) ff from CUSTOMER group by c_custkey ) a join NATION b on a.c_custkey = b.N_REGIONKEY where b.N_NATIONKEY > 10",
		"select * from (select c_custkey, count(C_NATIONKEY) ff from CUSTOMER group by c_custkey ) a join NATION b on a.c_custkey = b.N_REGIONKEY where b.N_NATIONKEY > 10",
	}
	runTestShouldPass(mock, t, sqls, false, false)

	// should error
	sqls = []string{
		"select C_NAME from (select c_custkey from CUSTOMER) a",                               //column not exist
		"select c_custkey2222 from (select c_custkey from CUSTOMER group by c_custkey ) a",    //column not exist
		"select col1 from (select c_custkey from CUSTOMER group by c_custkey ) a(col1, col2)", //column length not match
		"select c_custkey from (select c_custkey from CUSTOMER group by c_custkey) a(col1)",   //column not exist
	}
	runTestShouldError(mock, t, sqls)
}

func TestDerivedTableAliasValidation(t *testing.T) {
	tests := []struct {
		name      string
		sql       string
		mysqlCode uint16
		sqlState  string
		message   string
	}{
		{
			name:      "missing alias",
			sql:       "select * from (select c_custkey from CUSTOMER)",
			mysqlCode: moerr.ER_DERIVED_MUST_HAVE_ALIAS,
			sqlState:  "42000",
			message:   "Every derived table must have its own alias",
		},
		{
			name:      "missing alias for values",
			sql:       "select * from (values row(1))",
			mysqlCode: moerr.ER_DERIVED_MUST_HAVE_ALIAS,
			sqlState:  "42000",
			message:   "Every derived table must have its own alias",
		},
		{
			name:      "missing alias through parentheses",
			sql:       "select * from ((select c_custkey from CUSTOMER))",
			mysqlCode: moerr.ER_DERIVED_MUST_HAVE_ALIAS,
			sqlState:  "42000",
			message:   "Every derived table must have its own alias",
		},
		{
			name:      "too few column aliases",
			sql:       "select * from (select c_custkey, c_name from CUSTOMER) as d(a)",
			mysqlCode: moerr.ER_VIEW_WRONG_LIST,
			sqlState:  "HY000",
			message:   "In definition of view, derived table or common table expression, SELECT list and column names list have different column counts",
		},
		{
			name:      "too many column aliases",
			sql:       "select * from (select c_custkey from CUSTOMER) as d(a, b)",
			mysqlCode: moerr.ER_VIEW_WRONG_LIST,
			sqlState:  "HY000",
			message:   "In definition of view, derived table or common table expression, SELECT list and column names list have different column counts",
		},
		{
			name:      "too few column aliases inside cte body",
			sql:       "with c as (select * from (select c_custkey, c_name from CUSTOMER) as d(a)) select * from c",
			mysqlCode: moerr.ER_VIEW_WRONG_LIST,
			sqlState:  "HY000",
			message:   "In definition of view, derived table or common table expression, SELECT list and column names list have different column counts",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mock := NewMockOptimizer(false)
			_, err := runOneStmt(mock, t, test.sql)
			require.Error(t, err)

			var moErr *moerr.Error
			require.ErrorAs(t, err, &moErr)
			require.Equal(t, test.mysqlCode, moErr.MySQLCode())
			require.Equal(t, test.sqlState, moErr.SqlState())
			require.Equal(t, test.message, moErr.Error())
		})
	}
}

// test derived table plan building
func TestUnionSqlBuilder(t *testing.T) {
	mock := NewMockOptimizer(false)
	// should pass
	sqls := []string{
		"(select 1) union (select 1)",
		"(((select n_nationkey from nation order by n_nationkey))) union (((select n_nationkey from nation order by n_nationkey)))",
		"select 1 union select 2",
		"select 1 union (select 2 union select 3)",
		"(select 1 union select 2) union select 3 intersect select 4 order by 1",
		"select 1 union select null",
		"select n_name from nation intersect select n_name from nation2",
		"select n_name from nation minus select n_name from nation2",
		"select 1 union select 2 intersect select 2 union all select 1.1 minus select 22222",
		"select 1 as a union select 2 order by a limit 1",
		"select n_name from nation union select n_comment from nation order by n_name",
		"with qn (foo, bar) as (select 1 as col, 2 as coll union select 4, 5) select qn1.bar from qn qn1",
		"select n_name, n_comment from nation union all select n_name, n_comment from nation2",
		"select n_name from nation intersect all select n_name from nation2",
		"(select n_name from nation for update) union all (select n_name from nation2 for update)",
		"(select n_name from nation for update) union all (select n_name from nation2)",
		"with qn as (select n_nationkey from nation union all select n_nationkey from nation2) select * from qn for update",
		"with qn as (select n_nationkey from nation union all select n_nationkey from nation2) select * from qn limit 6 for update",
	}
	runTestShouldPass(mock, t, sqls, false, false)

	forUpdateUnionPlan, err := runOneStmt(mock, t, "(select n_name from nation for update) union all (select n_name from nation2 for update)")
	require.NoError(t, err)
	require.Equal(t, 2, countLockOpNodes(forUpdateUnionPlan))

	forUpdateUnionOneBranchPlan, err := runOneStmt(mock, t, "(select n_name from nation for update) union all (select n_name from nation2)")
	require.NoError(t, err)
	require.Equal(t, 1, countLockOpNodes(forUpdateUnionOneBranchPlan))

	cteOuterForUpdatePlan, err := runOneStmt(mock, t, "with qn as (select n_nationkey from nation union all select n_nationkey from nation2) select * from qn for update")
	require.NoError(t, err)
	require.Equal(t, 0, countLockOpNodes(cteOuterForUpdatePlan))

	cteOuterForUpdateLimitPlan, err := runOneStmt(mock, t, "with qn as (select n_nationkey from nation union all select n_nationkey from nation2) select * from qn limit 6 for update")
	require.NoError(t, err)
	require.Equal(t, 0, countLockOpNodes(cteOuterForUpdateLimitPlan))

	// should error
	sqls = []string{
		"select 1 union select 2, 'a'",
		"select n_name as a from nation union select n_comment from nation order by n_name",
		"select n_name from nation minus all select n_name from nation2", // not support
		"select n_name from nation union all select n_name from nation2 for update",
	}
	runTestShouldError(mock, t, sqls)
}

func countLockOpNodes(logicPlan *Plan) int {
	query := logicPlan.GetQuery()
	if query == nil {
		return 0
	}

	count := 0
	for _, node := range query.Nodes {
		if node.NodeType == plan.Node_LOCK_OP {
			count++
		}
	}
	return count
}

func TestSelectSharedLockMode(t *testing.T) {
	mock := NewMockOptimizer(false)
	tests := []struct {
		name            string
		sql             string
		mode            lockpb.LockMode
		lockTargetCount int
	}{
		{
			name:            "for share",
			sql:             "select n_nationkey from nation where n_nationkey = 1 for share",
			mode:            lockpb.LockMode_Shared,
			lockTargetCount: 1,
		},
		{
			name:            "lock in share mode",
			sql:             "select n_nationkey from nation where n_nationkey = 1 lock in share mode",
			mode:            lockpb.LockMode_Shared,
			lockTargetCount: 1,
		},
		{
			name:            "for share inside nested parentheses",
			sql:             "((select n_nationkey from nation for share))",
			mode:            lockpb.LockMode_Shared,
			lockTargetCount: 1,
		},
		{
			name:            "for share across rollup window rewrite",
			sql:             "select n_regionkey, row_number() over (order by n_regionkey) from nation group by n_regionkey with rollup for share",
			mode:            lockpb.LockMode_Shared,
			lockTargetCount: 2,
		},
		{
			name:            "for update remains exclusive",
			sql:             "select n_nationkey from nation where n_nationkey = 1 for update",
			mode:            lockpb.LockMode_Exclusive,
			lockTargetCount: 1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			logicPlan, err := runOneStmt(mock, t, test.sql)
			require.NoError(t, err)

			var lockTargets []*plan.LockTarget
			for _, node := range logicPlan.GetQuery().Nodes {
				if node.NodeType == plan.Node_LOCK_OP {
					lockTargets = append(lockTargets, node.LockTargets...)
				}
			}
			require.Len(t, lockTargets, test.lockTargetCount)
			for _, target := range lockTargets {
				require.Equal(t, test.mode, target.Mode)
			}
		})
	}
}

// test CTE plan building
func TestCTESqlBuilder(t *testing.T) {
	mock := NewMockOptimizer(false)

	// should pass
	sqls := []string{
		"WITH qn AS (SELECT * FROM nation) SELECT * FROM qn;",
		"with qn0 as (select 1), qn1 as (select * from qn0), qn2 as (select 1), qn3 as (select 1 from qn1, qn2) select 1 from qn3",

		`WITH qn AS (select "outer" as a)
		SELECT (WITH qn AS (SELECT "inner" as a) SELECT a from qn),
		qn.a
		FROM qn`,
	}
	runTestShouldPass(mock, t, sqls, false, false)

	// should error
	sqls = []string{
		"WITH qn(a, b) AS (SELECT * FROM nation) SELECT * FROM qn;",
		`with qn1 as (with qn3 as (select * from qn2) select * from qn3),
		qn2 as (select 1)
		select * from qn1`,

		`WITH qn2 AS (SELECT a FROM qn WHERE a IS NULL or a>0),
		qn AS (SELECT b as a FROM qn2)
		SELECT qn.a  FROM qn`,
	}
	runTestShouldError(mock, t, sqls)
}

func TestInsert(t *testing.T) {
	mock := NewMockOptimizer(false)
	// should pass
	sqls := []string{
		"INSERT INTO NATION VALUES (1, 'NAME1',21, 'COMMENT1'), (2, 'NAME2', 22, 'COMMENT2')",
		"INSERT INTO NATION (N_NATIONKEY, N_REGIONKEY, N_NAME, N_COMMENT) VALUES (1, 21, 'NAME1','comment1'), (2, 22, 'NAME2', 'comment2')",
		"INSERT INTO NATION SELECT * FROM NATION2",
	}
	runTestShouldPass(mock, t, sqls, false, false)

	// should error
	sqls = []string{
		"INSERT NATION VALUES (1, 'NAME1',21, 'COMMENT1'), ('NAME2', 22, 'COMMENT2')",                                // doesn't match value count
		"INSERT NATION (N_NATIONKEY, N_REGIONKEY, N_NAME) VALUES (1, 'NAME1'), (2, 22, 'NAME2')",                     // doesn't match value count
		"INSERT NATION (N_NATIONKEY, N_REGIONKEY, N_NAME2222) VALUES (1, 21, 'NAME1'), (2, 22, 'NAME2')",             // column not exist
		"INSERT NATION333 (N_NATIONKEY, N_REGIONKEY, N_NAME2222) VALUES (1, 2, 'NAME1'), (2, 22, 'NAME2')",           // table not exist
		"INSERT NATION (N_NATIONKEY, N_REGIONKEY, N_NAME2222) VALUES (1, 'should int32', 'NAME1'), (2, 22, 'NAME2')", // column type not match
		"INSERT NATION (N_NATIONKEY, N_REGIONKEY, N_NAME2222) VALUES (1, 2.22, 'NAME1'), (2, 22, 'NAME2')",           // column type not match
		"INSERT NATION (N_NATIONKEY, N_REGIONKEY, N_NAME2222) VALUES (1, 2, 'NAME1'), (2, 22, 'NAME2')",              // function expr not support now
		"INSERT INTO region SELECT * FROM NATION2",                                                                   // column length not match
		"INSERT INTO region SELECT 1, 2, 3, 4, 5, 6 FROM NATION2",                                                    // column length not match
		"INSERT NATION333 (N_NATIONKEY, N_REGIONKEY, N_NAME2222) SELECT 1, 2, 3 FROM NATION2",                        // table not exist
	}
	runTestShouldError(mock, t, sqls)
}

func TestLoadPlanUsesSingleTableLockTarget(t *testing.T) {
	mock := NewMockOptimizer(true)
	logicPlan, err := runOneStmt(
		mock,
		t,
		"LOAD DATA INLINE FORMAT='csv', DATA='1,n,1,c' INTO TABLE nation FIELDS TERMINATED BY ','",
	)
	require.NoError(t, err)

	query := logicPlan.GetQuery()
	require.NotNil(t, query)
	require.Equal(t, plan.Query_INSERT, query.StmtType)
	require.True(t, query.LoadTag)

	var lockTargets []*plan.LockTarget
	for _, node := range query.Nodes {
		if node.NodeType == plan.Node_LOCK_OP {
			lockTargets = append(lockTargets, node.LockTargets...)
		}
	}
	require.Len(t, lockTargets, 1)
	require.True(t, lockTargets[0].LockTable)
	require.Equal(t, mock.ctxt.tables["nation"].TblId, lockTargets[0].TableId)
}

func TestLoadPlanKeepsUniqueIndexRowLockTarget(t *testing.T) {
	mock := NewMockOptimizer(true)
	logicPlan, err := runOneStmt(
		mock,
		t,
		"LOAD DATA INLINE FORMAT='csv', DATA='1,d,l' INTO TABLE dept FIELDS TERMINATED BY ','",
	)
	require.NoError(t, err)

	query := logicPlan.GetQuery()
	require.NotNil(t, query)
	require.True(t, query.LoadTag)

	var lockTargets []*plan.LockTarget
	for _, node := range query.Nodes {
		if node.NodeType == plan.Node_LOCK_OP {
			lockTargets = append(lockTargets, node.LockTargets...)
		}
	}
	require.Len(t, lockTargets, 2)
	baseTableTargets := 0
	indexRowTargets := 0
	for _, target := range lockTargets {
		if target.TableId == mock.ctxt.tables["dept"].TblId {
			require.True(t, target.LockTable)
			baseTableTargets++
			continue
		}
		require.False(t, target.LockTable)
		indexRowTargets++
	}
	require.Equal(t, 1, baseTableTargets)
	require.Equal(t, 1, indexRowTargets)
}

func TestLargeDMLKeepsRowScopedLockTarget(t *testing.T) {
	sqls := []string{
		"INSERT INTO NATION SELECT * FROM NATION2",
		"DELETE FROM NATION",
		"REPLACE INTO NATION SELECT * FROM NATION2",
		"SELECT N_NATIONKEY FROM NATION FOR UPDATE",
	}

	for _, sql := range sqls {
		t.Run(sql, func(t *testing.T) {
			mock := NewMockOptimizer(true)
			proc := testutil.NewProc(t)
			lockService := mock_lock.NewMockLockService(gomock.NewController(t))
			lockService.EXPECT().GetConfig().Return(lockservice.Config{
				ServiceID:       "plan-test",
				MaxLockRowCount: 1,
			}).AnyTimes()
			proc.Base.LockService = lockService
			rt := moruntime.ServiceRuntime(proc.GetService())
			if rt == nil {
				rt = moruntime.DefaultRuntime()
				moruntime.SetupServiceBasedRuntime(proc.GetService(), rt)
			}
			rt.SetGlobalVariables("optimizer_hints", "")
			mock.ctxt.GetProcessFunc = func() *process.Process { return proc }

			logicPlan, err := runOneStmt(mock, t, sql)
			require.NoError(t, err)

			lockNodeCount := 0
			for _, node := range logicPlan.GetQuery().Nodes {
				if node.NodeType != plan.Node_LOCK_OP {
					continue
				}
				lockNodeCount++
				for _, target := range node.LockTargets {
					require.False(t, target.LockTable,
						"large DML must retain row/range lock target: %s", sql)
				}
			}
			require.NotZero(t, lockNodeCount, "expected a lock operator: %s", sql)
		})
	}
}

func TestLargeUpdateTableLockRequiresUnrestrictedSingleTarget(t *testing.T) {
	tests := []struct {
		name          string
		sql           string
		maxRows       uint64
		wantTableLock bool
		prepare       func(*MockOptimizer)
	}{
		{
			name:          "unfiltered single target",
			sql:           "UPDATE NATION SET N_NAME = 'updated'",
			maxRows:       1,
			wantTableLock: true,
		},
		{
			name:          "unfiltered primary key update",
			sql:           "UPDATE NATION SET N_NATIONKEY = N_NATIONKEY + 100",
			maxRows:       1,
			wantTableLock: true,
		},
		{
			name:          "literal true is statically unrestricted",
			sql:           "UPDATE NATION SET N_NAME = 'updated' WHERE TRUE",
			maxRows:       1,
			wantTableLock: true,
		},
		{
			name:          "partitioned full update",
			sql:           "UPDATE NATION SET N_NAME = 'updated'",
			maxRows:       1,
			wantTableLock: true,
			prepare: func(mock *MockOptimizer) {
				mock.ctxt.tables["nation"].FeatureFlag |= features.Partitioned
				mock.ctxt.tables["nation"].Partition = &plan.Partition{
					PartitionDefs: []*plan.PartitionDef{{
						Def: &plan.Expr{Expr: &plan.Expr_F{F: &plan.Function{Args: []*plan.Expr{
							{Expr: &plan.Expr_Col{Col: &plan.ColRef{Name: "n_nationkey"}}},
						}}}},
					}},
				}
			},
		},
		{
			name:    "bounded predicate stays row scoped",
			sql:     "UPDATE NATION SET N_NAME = 'updated' WHERE N_NATIONKEY >= 0",
			maxRows: 1,
		},
		{
			name:    "constant false stays row scoped",
			sql:     "UPDATE NATION SET N_NAME = 'updated' WHERE FALSE",
			maxRows: 1,
		},
		{
			name:    "nonliteral tautology is conservatively row scoped",
			sql:     "UPDATE NATION SET N_NAME = 'updated' WHERE 1 = 1",
			maxRows: 1,
		},
		{
			name:    "ordered limit stays row scoped",
			sql:     "UPDATE NATION SET N_NAME = 'updated' ORDER BY N_NATIONKEY LIMIT 10",
			maxRows: 1,
		},
		{
			name: "joined source stays row scoped",
			sql: "UPDATE NATION n JOIN NATION2 n2 ON n.N_NATIONKEY = n2.N_NATIONKEY " +
				"SET n.N_NAME = 'updated'",
			maxRows: 1,
		},
		{
			name:    "update from stays row scoped",
			sql:     "UPDATE NATION n SET n.N_NAME = 'updated' FROM NATION2 n2 WHERE n.N_NATIONKEY = n2.N_NATIONKEY",
			maxRows: 1,
		},
		{
			name:          "small full update stays row scoped",
			sql:           "UPDATE NATION SET N_NAME = 'updated'",
			maxRows:       1 << 30,
			wantTableLock: false,
		},
		{
			name:          "float64 full keyspace can use table lock",
			sql:           "UPDATE NATION SET N_NAME = 'updated'",
			maxRows:       1,
			wantTableLock: true,
			prepare: func(mock *MockOptimizer) {
				tableDef := mock.ctxt.tables["nation"]
				pkPos := tableDef.Name2ColIndex[tableDef.Pkey.PkeyColName]
				tableDef.Cols[pkPos].Typ = plan.Type{Id: int32(types.T_float64)}
			},
		},
		{
			name:          "float32 full keyspace can use table lock",
			sql:           "UPDATE NATION SET N_NAME = 'updated'",
			maxRows:       1,
			wantTableLock: true,
			prepare: func(mock *MockOptimizer) {
				tableDef := mock.ctxt.tables["nation"]
				pkPos := tableDef.Name2ColIndex[tableDef.Pkey.PkeyColName]
				tableDef.Cols[pkPos].Typ = plan.Type{Id: int32(types.T_float32)}
			},
		},
		{
			name:    "affected foreign key preserves lock order",
			sql:     "UPDATE replace_fk_c SET pid = pid",
			maxRows: 1,
		},
		{
			name:          "unrelated column on foreign key table",
			sql:           "UPDATE replace_fk_c SET id = id + 100",
			maxRows:       1,
			wantTableLock: true,
		},
		{
			name:    "locking scalar subquery preserves lock order",
			sql:     "UPDATE NATION SET N_NAME = (SELECT N_NAME FROM NATION2 LIMIT 1 FOR UPDATE)",
			maxRows: 1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mock := NewMockOptimizer(true)
			if test.prepare != nil {
				test.prepare(mock)
			}
			proc := testutil.NewProc(t)
			lockService := mock_lock.NewMockLockService(gomock.NewController(t))
			lockService.EXPECT().GetConfig().Return(lockservice.Config{
				ServiceID:       "plan-test",
				MaxLockRowCount: toml.ByteSize(test.maxRows),
			}).AnyTimes()
			proc.Base.LockService = lockService
			rt := moruntime.ServiceRuntime(proc.GetService())
			if rt == nil {
				rt = moruntime.DefaultRuntime()
				moruntime.SetupServiceBasedRuntime(proc.GetService(), rt)
			}
			rt.SetGlobalVariables("optimizer_hints", "")
			mock.ctxt.GetProcessFunc = func() *process.Process { return proc }

			logicPlan, err := runOneStmt(mock, t, test.sql)
			require.NoError(t, err)

			exclusiveTargets := 0
			for _, node := range logicPlan.GetQuery().Nodes {
				if node.NodeType != plan.Node_LOCK_OP {
					continue
				}
				for _, target := range node.LockTargets {
					if target.Mode != lockpb.LockMode_Exclusive {
						continue
					}
					exclusiveTargets++
					require.Equal(t, test.wantTableLock, target.LockTable)
				}
			}
			require.NotZero(t, exclusiveTargets)
		})
	}
}

func TestLargeUnrestrictedIndexedUpdateLocksEveryWrittenNamespace(t *testing.T) {
	for _, test := range []struct {
		name          string
		sql           string
		wantTableLock bool
	}{
		{
			name:          "full update",
			sql:           "UPDATE index_hint_t SET a = a + 1",
			wantTableLock: true,
		},
		{
			name: "bounded update",
			sql:  "UPDATE index_hint_t SET a = a + 1 WHERE id >= 0",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			mock := NewMockOptimizer(true)
			addIndexHintChoiceTableForTest(mock)
			proc := testutil.NewProc(t)
			lockService := mock_lock.NewMockLockService(gomock.NewController(t))
			lockService.EXPECT().GetConfig().Return(lockservice.Config{
				ServiceID:       "plan-test",
				MaxLockRowCount: 1,
			}).AnyTimes()
			proc.Base.LockService = lockService
			rt := moruntime.ServiceRuntime(proc.GetService())
			if rt == nil {
				rt = moruntime.DefaultRuntime()
				moruntime.SetupServiceBasedRuntime(proc.GetService(), rt)
			}
			rt.SetGlobalVariables("optimizer_hints", "")
			mock.ctxt.GetProcessFunc = func() *process.Process { return proc }

			logicPlan, err := runOneStmt(mock, t, test.sql)
			require.NoError(t, err)

			targetTables := make(map[uint64]struct{})
			exclusiveTargets := 0
			for _, node := range logicPlan.GetQuery().Nodes {
				if node.NodeType != plan.Node_LOCK_OP {
					continue
				}
				for _, target := range node.LockTargets {
					if target.Mode != lockpb.LockMode_Exclusive {
						continue
					}
					exclusiveTargets++
					targetTables[target.TableId] = struct{}{}
					require.Equal(t, test.wantTableLock, target.LockTable)
				}
			}
			require.GreaterOrEqual(t, exclusiveTargets, 2,
				"base and affected unique-index namespaces must both be locked")
			require.GreaterOrEqual(t, len(targetTables), 2)
		})
	}
}

func TestLargeSharedLockTargetsKeepBoundedFallback(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		prepare func(*MockOptimizer)
	}{
		{
			name: "select for share",
			sql:  "SELECT N_NATIONKEY FROM NATION FOR SHARE",
		},
		{
			name: "lock in share mode",
			sql:  "SELECT N_NATIONKEY FROM NATION LOCK IN SHARE MODE",
		},
		{
			name: "foreign key validation",
			sql:  "INSERT INTO replace_fk_c VALUES (10, 1), (11, 1)",
		},
		{
			name: "float32 select for share",
			sql:  "SELECT N_NATIONKEY FROM NATION FOR SHARE",
			prepare: func(mock *MockOptimizer) {
				tableDef := mock.ctxt.tables["nation"]
				pkPos := tableDef.Name2ColIndex[tableDef.Pkey.PkeyColName]
				tableDef.Cols[pkPos].Typ = plan.Type{Id: int32(types.T_float32)}
			},
		},
		{
			name: "float64 lock in share mode",
			sql:  "SELECT N_NATIONKEY FROM NATION LOCK IN SHARE MODE",
			prepare: func(mock *MockOptimizer) {
				tableDef := mock.ctxt.tables["nation"]
				pkPos := tableDef.Name2ColIndex[tableDef.Pkey.PkeyColName]
				tableDef.Cols[pkPos].Typ = plan.Type{Id: int32(types.T_float64)}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mock := NewMockOptimizer(true)
			if test.prepare != nil {
				test.prepare(mock)
			}
			proc := testutil.NewProc(t)
			lockService := mock_lock.NewMockLockService(gomock.NewController(t))
			lockService.EXPECT().GetConfig().Return(lockservice.Config{
				ServiceID:       "plan-test",
				MaxLockRowCount: 1,
			}).AnyTimes()
			proc.Base.LockService = lockService
			rt := moruntime.ServiceRuntime(proc.GetService())
			if rt == nil {
				rt = moruntime.DefaultRuntime()
				moruntime.SetupServiceBasedRuntime(proc.GetService(), rt)
			}
			rt.SetGlobalVariables("optimizer_hints", "")
			mock.ctxt.GetProcessFunc = func() *process.Process { return proc }

			logicPlan, err := runOneStmt(mock, t, test.sql)
			require.NoError(t, err)

			sharedTargets := 0
			for _, node := range logicPlan.GetQuery().Nodes {
				if node.NodeType != plan.Node_LOCK_OP {
					continue
				}
				for _, target := range node.LockTargets {
					if target.Mode != lockpb.LockMode_Shared {
						continue
					}
					sharedTargets++
					require.True(t, target.LockTable,
						"large shared target must retain the planner fallback: %s", test.sql)
				}
			}
			require.NotZero(t, sharedTargets, "expected a shared lock target: %s", test.sql)
		})
	}
}

func TestApplyLockTableFallbackGuardsAndModes(t *testing.T) {
	mock := NewMockOptimizer(true)
	markedAtBoundary := &plan.LockTarget{Mode: lockpb.LockMode_Exclusive}
	markedAboveBoundary := &plan.LockTarget{Mode: lockpb.LockMode_Exclusive}
	builder := &QueryBuilder{
		compCtx: &mock.ctxt,
		fullTableUpdateLockTargets: map[*plan.LockTarget]struct{}{
			markedAtBoundary:    {},
			markedAboveBoundary: {},
		},
		qry: &plan.Query{Nodes: []*plan.Node{
			{NodeType: plan.Node_TABLE_SCAN, Stats: &plan.Stats{Outcnt: 100}},
			{NodeType: plan.Node_LOCK_OP},
			{
				NodeType: plan.Node_LOCK_OP,
				Stats:    &plan.Stats{Outcnt: 3},
				LockTargets: []*plan.LockTarget{
					markedAtBoundary,
				},
			},
			{
				NodeType: plan.Node_LOCK_OP,
				Stats:    &plan.Stats{Outcnt: 4},
				LockTargets: []*plan.LockTarget{
					markedAboveBoundary,
					{Mode: lockpb.LockMode_Shared},
					{Mode: lockpb.LockMode_Exclusive},
				},
			},
		}},
	}

	// Planning without a process or without a real lock service is valid for
	// internal and mock compiler contexts.
	mock.ctxt.GetProcessFunc = func() *process.Process { return nil }
	applyLockTableFallback(builder)
	proc := testutil.NewProc(t)
	mock.ctxt.GetProcessFunc = func() *process.Process { return proc }
	applyLockTableFallback(builder)

	lockService := mock_lock.NewMockLockService(gomock.NewController(t))
	gomock.InOrder(
		lockService.EXPECT().GetConfig().Return(lockservice.Config{}),
		lockService.EXPECT().GetConfig().Return(lockservice.Config{MaxLockRowCount: 3}),
	)
	proc.Base.LockService = lockService
	applyLockTableFallback(builder)
	applyLockTableFallback(builder)

	require.False(t, builder.qry.Nodes[2].LockTargets[0].LockTable,
		"the configured budget is inclusive")
	require.True(t, builder.qry.Nodes[3].LockTargets[0].LockTable,
		"a proven full-table update upgrades above the configured budget")
	require.True(t, builder.qry.Nodes[3].LockTargets[1].LockTable,
		"cardinality-known shared targets must upgrade before acquisition")
	require.False(t, builder.qry.Nodes[3].LockTargets[2].LockTable,
		"unmarked exclusive targets retain owner-side range escalation")
}

func TestInsertIntoMarkedTemporaryTableUsesModernPath(t *testing.T) {
	mock := NewMockOptimizer(true)
	catalog.MarkTableDefTemporary(mock.ctxt.tables["nation"])
	// Resolve sets this session-scoped bit when the logical temporary-table
	// alias is mapped to its physical table.
	mock.ctxt.tables["nation"].IsTemporary = true

	tests := []struct {
		name string
		sql  string
	}{
		{
			name: "plain insert",
			sql:  "insert into nation values (1, 'n', 2, 'plain')",
		},
		{
			name: "insert ignore",
			sql:  "insert ignore into nation values (1, 'n', 2, 'ignore')",
		},
		{
			name: "on duplicate key update",
			sql: "insert into nation values (1, 'n', 2, 'upsert') " +
				"on duplicate key update n_comment = values(n_comment)",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			logicPlan, err := runOneStmt(mock, t, test.sql)
			require.NoError(t, err)

			hasMultiUpdate := false
			for _, node := range logicPlan.GetQuery().Nodes {
				if node.NodeType == plan.Node_MULTI_UPDATE {
					hasMultiUpdate = true
					break
				}
			}
			require.True(t, hasMultiUpdate,
				"temporary-table %s should stay on the modern insert path", test.name)
		})
	}
}

const clusterGeneratedInsertTable = "cluster_generated_insert"

func addClusterGeneratedInsertTableForTest(mock *MockOptimizer) {
	intType := plan.Type{Id: int32(types.T_int32)}
	accountType := plan.Type{Id: int32(types.T_uint32), NotNullable: true}
	cols := []*plan.ColDef{
		{ColId: 0, Name: "id", OriginName: "id", Typ: intType, NotNull: true,
			Default: &plan.Default{NullAbility: false}},
		{ColId: 1, Name: "base_value", OriginName: "base_value", Typ: intType,
			Default: &plan.Default{NullAbility: true}},
		{ColId: 2, Name: "stored_value", OriginName: "stored_value", Typ: intType,
			Default: &plan.Default{NullAbility: true}, GeneratedCol: &plan.GeneratedCol{
				Expr: &plan.Expr{Typ: intType, Expr: &plan.Expr_Col{Col: &plan.ColRef{
					RelPos: 0, ColPos: 1, Name: "base_value",
				}}},
				IsStored: true,
			}},
		{ColId: 3, Name: "virtual_value", OriginName: "virtual_value", Typ: intType,
			Default: &plan.Default{NullAbility: true}, GeneratedCol: &plan.GeneratedCol{
				Expr: &plan.Expr{Typ: intType, Expr: &plan.Expr_Col{Col: &plan.ColRef{
					RelPos: 0, ColPos: 1, Name: "base_value",
				}}},
				IsStored: false,
			}},
		{ColId: 4, Name: "account_id", OriginName: "account_id", Typ: accountType, NotNull: true,
			Default: &plan.Default{NullAbility: false, Expr: makePlan2Uint32ConstExprWithType(catalog.System_Account)}},
	}
	compPkey := MakeHiddenColDefByName(catalog.CPrimaryKeyColName)
	compPkey.ColId = 5
	compPkey.OriginName = catalog.CPrimaryKeyColName
	compPkey.Primary = true
	rowID := MakeRowIdColDef()
	rowID.ColId = 6
	rowID.OriginName = catalog.Row_ID
	cols = append(cols, compPkey, rowID)

	name2ColIndex := make(map[string]int32, len(cols))
	for i, col := range cols {
		name2ColIndex[col.Name] = int32(i)
	}
	tableDef := &plan.TableDef{
		TableType:     catalog.SystemClusterRel,
		TblId:         27923,
		Name:          clusterGeneratedInsertTable,
		Cols:          cols,
		Name2ColIndex: name2ColIndex,
		Pkey: &plan.PrimaryKeyDef{
			Names:       []string{"id", "account_id"},
			Cols:        []uint64{0, 4},
			PkeyColName: catalog.CPrimaryKeyColName,
			CompPkeyCol: compPkey,
		},
	}
	mock.ctxt.objects[clusterGeneratedInsertTable] = &plan.ObjectRef{
		SchemaName: "tpch", ObjName: clusterGeneratedInsertTable, Obj: 27923,
	}
	mock.ctxt.tables[clusterGeneratedInsertTable] = tableDef
	mock.ctxt.id2name[tableDef.TblId] = clusterGeneratedInsertTable
	mock.ctxt.pks[clusterGeneratedInsertTable] = []int{0, 4}
}

func exprContainsTypedNull(expr *plan.Expr) bool {
	if expr == nil {
		return false
	}
	if lit := expr.GetLit(); lit != nil {
		return lit.Isnull
	}
	if f := expr.GetF(); f != nil {
		for _, arg := range f.Args {
			if exprContainsTypedNull(arg) {
				return true
			}
		}
	}
	return false
}

func exprContainsIntegerLiteral(expr *plan.Expr, want int64) bool {
	if expr == nil {
		return false
	}
	if lit := expr.GetLit(); lit != nil && !lit.Isnull {
		switch value := lit.Value.(type) {
		case *plan.Literal_I32Val:
			return int64(value.I32Val) == want
		case *plan.Literal_I64Val:
			return value.I64Val == want
		case *plan.Literal_U32Val:
			return int64(value.U32Val) == want
		case *plan.Literal_U64Val:
			return value.U64Val <= uint64(^uint64(0)>>1) && int64(value.U64Val) == want
		}
	}
	if f := expr.GetF(); f != nil {
		for _, arg := range f.Args {
			if exprContainsIntegerLiteral(arg, want) {
				return true
			}
		}
	}
	return false
}

func requireModernClusterInsertPlan(
	t *testing.T,
	query *plan.Query,
	wantAccountID *int64,
	wantIgnoreDedup bool,
) {
	t.Helper()

	var multiUpdate *plan.Node
	hasIgnoreDedup := false
	for _, node := range query.Nodes {
		require.NotEqual(t, plan.Node_INSERT, node.NodeType,
			"cluster-table writes must not fall back to the legacy INSERT path")
		if node.NodeType == plan.Node_MULTI_UPDATE {
			multiUpdate = node
		}
		if node.NodeType == plan.Node_JOIN && node.JoinType == plan.Node_DEDUP &&
			node.OnDuplicateAction == plan.Node_IGNORE {
			hasIgnoreDedup = true
		}
	}
	require.NotNil(t, multiUpdate)
	if wantIgnoreDedup {
		require.True(t, hasIgnoreDedup)
	}

	var tableCtx *plan.UpdateCtx
	for _, updateCtx := range multiUpdate.UpdateCtxList {
		if updateCtx.TableDef != nil && updateCtx.TableDef.Name == clusterGeneratedInsertTable {
			tableCtx = updateCtx
			break
		}
	}
	require.NotNil(t, tableCtx)

	var preInsert *plan.Node
	for _, node := range query.Nodes {
		if node.NodeType == plan.Node_PRE_INSERT && node.PreInsertCtx != nil &&
			node.PreInsertCtx.TableDef.GetName() == clusterGeneratedInsertTable {
			preInsert = node
			break
		}
	}
	require.NotNil(t, preInsert)
	require.Len(t, preInsert.Children, 1)
	rowImage := query.Nodes[preInsert.Children[0]]

	writeExpr := func(colName string) *plan.Expr {
		colPos, ok := tableCtx.TableDef.Name2ColIndex[colName]
		require.True(t, ok)
		require.Less(t, int(colPos), len(tableCtx.InsertCols))
		ref := tableCtx.InsertCols[colPos]
		require.Equal(t, colPos, ref.ColPos)
		require.Less(t, int(colPos), len(rowImage.ProjectList))
		return rowImage.ProjectList[colPos]
	}

	for _, generated := range []struct {
		name     string
		isStored bool
	}{
		{name: "stored_value", isStored: true},
		{name: "virtual_value", isStored: false},
	} {
		col := tableCtx.TableDef.Cols[tableCtx.TableDef.Name2ColIndex[generated.name]]
		require.NotNil(t, col.GeneratedCol)
		require.Equal(t, generated.isStored, col.GeneratedCol.IsStored)
		require.False(t, exprContainsTypedNull(writeExpr(generated.name)),
			"generated column %s must not reach the physical write as a typed NULL", generated.name)
	}

	if wantAccountID != nil {
		accountExpr := writeExpr("account_id")
		require.True(t, exprContainsIntegerLiteral(accountExpr, *wantAccountID),
			"account_id must remain in its target column position: %s", accountExpr.String())
	}

	compPkeyExpr := preInsert.PreInsertCtx.CompPkeyExpr
	require.NotNil(t, compPkeyExpr)
	require.Equal(t, "serial", compPkeyExpr.GetF().GetFunc().GetObjName())
	require.Len(t, compPkeyExpr.GetF().Args, 2)
	require.Equal(t, int32(0), compPkeyExpr.GetF().Args[0].GetCol().ColPos)
	require.Equal(t, int32(4), compPkeyExpr.GetF().Args[1].GetCol().ColPos)
}

func TestClusterTableInsertUsesModernPath(t *testing.T) {
	tests := []struct {
		name            string
		sql             string
		prepared        bool
		wantAccountID   int64
		wantIgnoreDedup bool
	}{
		{
			name: "values",
			sql:  "insert into cluster_generated_insert (id, base_value) values (1, 4)",
		},
		{
			name:          "insert select with explicit account",
			sql:           "insert into cluster_generated_insert (id, base_value, account_id) select 2, 6, 17",
			wantAccountID: 17,
		},
		{
			name:     "prepared values",
			sql:      "prepare cluster_insert from 'insert into cluster_generated_insert (id, base_value) values (?, ?)'",
			prepared: true,
		},
		{
			name:            "insert ignore",
			sql:             "insert ignore into cluster_generated_insert (id, base_value) values (1, 4)",
			wantIgnoreDedup: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mock := NewMockOptimizer(true)
			addClusterGeneratedInsertTableForTest(mock)

			logicPlan, err := runOneStmt(mock, t, test.sql)
			require.NoError(t, err)
			query := logicPlan.GetQuery()
			if test.prepared {
				prepare := logicPlan.GetDcl().GetPrepare()
				require.NotNil(t, prepare)
				query = prepare.Plan.GetQuery()
			}
			require.NotNil(t, query)
			wantAccountID := test.wantAccountID
			requireModernClusterInsertPlan(t, query, &wantAccountID, test.wantIgnoreDedup)
		})
	}
}

func TestClusterTableInsertRejectsUnsupportedSyntax(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr string
	}{
		{
			name:    "overwrite",
			sql:     "insert overwrite cluster_generated_insert (id, base_value) values (1, 4)",
			wantErr: "not supported: INSERT OVERWRITE currently supports Iceberg table mappings",
		},
		{
			name:    "partition values",
			sql:     "insert into cluster_generated_insert partition(p = 1) (id, base_value) values (1, 4)",
			wantErr: "not supported: INSERT PARTITION value syntax currently supports Iceberg INSERT OVERWRITE only",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mock := NewMockOptimizer(true)
			addClusterGeneratedInsertTableForTest(mock)

			_, err := runOneStmt(mock, t, test.sql)
			require.EqualError(t, err, test.wantErr)
		})
	}
}

func TestClusterTableLoadUsesModernPath(t *testing.T) {
	mock := NewMockOptimizer(true)
	addClusterGeneratedInsertTableForTest(mock)

	logicPlan, err := runOneStmt(mock, t,
		"load data inline format='csv', data='1,4,0' into table cluster_generated_insert fields terminated by ',' "+
			"(id, base_value, account_id)")
	require.NoError(t, err)
	query := logicPlan.GetQuery()
	require.NotNil(t, query)
	require.True(t, query.LoadTag)
	requireModernClusterInsertPlan(t, query, nil, false)
}

func TestInsertIgnoreIntoInternalIndexTableRemainsUnsupported(t *testing.T) {
	mock := NewMockOptimizer(true)
	_, err := runOneStmt(mock, t,
		"insert ignore into `__mo_index_secondary_meta` (`__mo_index_key`, `__mo_index_val`) "+
			"values ('version', '0')")
	require.ErrorContains(t, err, "insert into vector/text index table")
}

func TestInsertIgnoreWithMultipleUniqueConstraintsUsesCoordinatedDedup(t *testing.T) {
	mock := NewMockOptimizer(true)
	logicPlan, err := runOneStmt(mock, t,
		"INSERT IGNORE INTO dept VALUES (1, 'Sales', 'NY'), (1, 'Marketing', 'SF')")
	require.NoError(t, err)

	coordinated := 0
	legacyIgnoreDedups := 0
	for _, node := range logicPlan.GetQuery().Nodes {
		if node.NodeType == plan.Node_PRE_INSERT_UK &&
			node.PreInsertUkCtx.GetInsertIgnoreMultiDedup() {
			coordinated++
			require.Len(t, node.PreInsertUkCtx.KeyColumns, 2)
			require.Len(t, node.PreInsertUkCtx.ConflictColumns, 2)
			require.Equal(t, node.PreInsertUkCtx.OutputColumns, node.PreInsertUkCtx.KeyColumns[0])
			for i := range node.PreInsertUkCtx.KeyColumns {
				require.Equal(t, node.PreInsertUkCtx.KeyColumns[i]+1,
					node.PreInsertUkCtx.ConflictColumns[i])
			}
		}
		if node.NodeType == plan.Node_JOIN && node.JoinType == plan.Node_DEDUP &&
			node.OnDuplicateAction == plan.Node_IGNORE {
			legacyIgnoreDedups++
		}
	}
	require.Equal(t, 1, coordinated)
	require.Zero(t, legacyIgnoreDedups,
		"independent per-key IGNORE joins would discard fallback rows before all constraints are known")
}

func TestInsertIgnoreSingleUniqueConstraintKeepsExistingDedupPath(t *testing.T) {
	mock := NewMockOptimizer(true)
	logicPlan, err := runOneStmt(mock, t,
		"INSERT IGNORE INTO fake_pk_t VALUES (1, 'x'), (1, 'y')")
	require.NoError(t, err)

	coordinated := 0
	legacyIgnoreDedups := 0
	for _, node := range logicPlan.GetQuery().Nodes {
		if node.NodeType == plan.Node_PRE_INSERT_UK &&
			node.PreInsertUkCtx.GetInsertIgnoreMultiDedup() {
			coordinated++
		}
		if node.NodeType == plan.Node_JOIN && node.JoinType == plan.Node_DEDUP &&
			node.OnDuplicateAction == plan.Node_IGNORE {
			legacyIgnoreDedups++
		}
	}
	require.Zero(t, coordinated)
	require.Equal(t, 1, legacyIgnoreDedups)
}

func TestUpdate(t *testing.T) {
	mock := NewMockOptimizer(true)
	// should pass
	sqls := []string{
		"UPDATE NATION SET N_NAME ='U1', N_REGIONKEY=2",
		"UPDATE NATION SET N_NAME ='U1', N_REGIONKEY=2 WHERE N_NATIONKEY > 10 LIMIT 20",
		"UPDATE NATION SET N_NAME ='U1', N_REGIONKEY=N_REGIONKEY+2 WHERE N_NATIONKEY > 10 LIMIT 20",
		"update NATION a join NATION2 b on a.N_REGIONKEY = b.R_REGIONKEY set a.N_NAME = 'aa'",
		// PostgreSQL-style UPDATE ... FROM
		"UPDATE NATION a SET a.N_NAME = 'aa' FROM NATION2 b WHERE a.N_REGIONKEY = b.R_REGIONKEY",
		"UPDATE NATION SET N_NAME = 'bb' FROM REGION WHERE NATION.N_REGIONKEY = REGION.R_REGIONKEY",
		"UPDATE NATION a SET a.N_NAME = 'cc' FROM NATION2 b, REGION c WHERE a.N_REGIONKEY = b.R_REGIONKEY AND b.R_REGIONKEY = c.R_REGIONKEY",
		// Unqualified SET LHS must bind to the target only; both NATION and
		// NATION2 expose N_NAME but this should NOT be reported as ambiguous.
		"UPDATE NATION SET N_NAME = NATION2.N_NAME FROM NATION2 WHERE NATION.N_REGIONKEY = NATION2.R_REGIONKEY",
		// FROM-clause join tree (JOIN ... ON ...) must round-trip without
		// changing associativity.
		"UPDATE NATION a SET a.N_NAME = 'dd' FROM NATION2 b JOIN REGION c ON b.R_REGIONKEY = c.R_REGIONKEY WHERE a.N_REGIONKEY = b.R_REGIONKEY",
		// Self-join: target and source are the same table.
		"UPDATE NATION a SET a.N_NAME = b.N_NAME FROM NATION b WHERE a.N_REGIONKEY = b.N_REGIONKEY",
		"prepare stmt1 from 'update nation set n_name = ? where n_nationkey > ?'",
		"drop index idx1 on test_idx",
	}
	runTestShouldPass(mock, t, sqls, false, false)

	// should error
	sqls = []string{
		"UPDATE NATION SET N_NAME2 ='U1', N_REGIONKEY=2",                                         // column not exist
		"UPDATE NATION2222 SET N_NAME ='U1', N_REGIONKEY=2",                                      // table not exist
		"UPDATE NATION a SET a.N_NAME = 'x' FROM NOTEXIST b WHERE a.N_REGIONKEY = b.R_REGIONKEY", // FROM table not exist
		"UPDATE NATION a SET a.N_NAME = 'x' FROM NATION2 b WHERE a.N_REGIONKEY = b.NOT_A_COL",    // FROM column not exist
	}
	runTestShouldError(mock, t, sqls)
}

func TestUpdateIgnoreUsesIgnoreDedupAction(t *testing.T) {
	mock := NewMockOptimizer(true)
	logicPlan, err := runOneStmt(mock, t,
		"UPDATE IGNORE NATION SET N_NATIONKEY = N_NATIONKEY + 1")
	require.NoError(t, err)

	found := false
	for _, node := range logicPlan.GetQuery().Nodes {
		if node.JoinType == plan.Node_DEDUP {
			found = true
			require.Equal(t, plan.Node_IGNORE, node.OnDuplicateAction)
		}
	}
	require.True(t, found, "UPDATE IGNORE of a primary key should include a DEDUP join")
}

func TestUpdateIgnoreUsesAssignmentIgnoreCast(t *testing.T) {
	mock := NewMockOptimizer(true)
	logicPlan, err := runOneStmt(mock, t,
		"UPDATE IGNORE NATION SET N_NAME = CAST('abcdefghijklmnopqrstuvwxyz' AS TEXT)")
	require.NoError(t, err)
	require.True(t,
		planHasTextToVarcharCastWithNameAndWidth(logicPlan, "cast_ignore", 25),
		"UPDATE IGNORE assignment should use cast_ignore",
	)

	logicPlan, err = runOneStmt(mock, t,
		"UPDATE NATION SET N_NAME = CAST('abcdefghijklmnopqrstuvwxyz' AS TEXT)")
	require.NoError(t, err)
	require.True(t,
		planHasTextToVarcharCastWithNameAndWidth(logicPlan, "cast_assign", 25),
		"ordinary UPDATE assignment should use cast_assign",
	)
}

func TestUpdateRecomputesCompositeClusterByKey(t *testing.T) {
	testCases := []struct {
		name             string
		sql              string
		expectRecomputed bool
	}{
		{
			name:             "first component",
			sql:              "update constraint_test.products set pid = pid + 8 where pid = 1",
			expectRecomputed: true,
		},
		{
			name:             "last component",
			sql:              "update constraint_test.products set pname = 'new' where pid = 1",
			expectRecomputed: true,
		},
		{
			name:             "all components",
			sql:              "update constraint_test.products set pid = 9, pname = 'new' where pid = 1",
			expectRecomputed: true,
		},
		{
			name:             "non cluster column",
			sql:              "update constraint_test.products set description = 'new' where pid = 1",
			expectRecomputed: false,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			mock := NewMockOptimizer(true)
			configureProductsAsCompositeClusterByTable(t, mock)

			logicPlan, err := runOneStmt(mock, t, testCase.sql)
			require.NoError(t, err)
			query := logicPlan.GetQuery()

			var multiUpdate *plan.Node
			var originUpdateCtx *plan.UpdateCtx
			for _, node := range query.Nodes {
				if node.NodeType != plan.Node_MULTI_UPDATE {
					continue
				}
				multiUpdate = node
				for _, updateCtx := range node.UpdateCtxList {
					if updateCtx.TableDef != nil && updateCtx.TableDef.Name == "products" {
						originUpdateCtx = updateCtx
						break
					}
				}
			}
			require.NotNil(t, multiUpdate)
			require.NotNil(t, originUpdateCtx)

			clusterByName := originUpdateCtx.TableDef.ClusterBy.Name
			clusterByPos := originUpdateCtx.TableDef.Name2ColIndex[clusterByName]
			clusterByInsertCol := originUpdateCtx.InsertCols[clusterByPos]

			require.Len(t, multiUpdate.Children, 1)
			lockNode := query.Nodes[multiUpdate.Children[0]]
			require.Equal(t, plan.Node_LOCK_OP, lockNode.NodeType)
			require.Len(t, lockNode.Children, 1)
			finalProject := query.Nodes[lockNode.Children[0]]
			require.Equal(t, plan.Node_PROJECT, finalProject.NodeType)
			clusterByExpr := finalProject.ProjectList[clusterByInsertCol.ColPos]

			if !testCase.expectRecomputed {
				require.NotNil(t, clusterByExpr.GetCol())
				return
			}

			clusterByFunc := clusterByExpr.GetF()
			require.NotNil(t, clusterByFunc)
			require.Equal(t, "serial_full", clusterByFunc.Func.ObjName)
			require.Len(t, clusterByFunc.Args, 2)

			for i, componentName := range []string{"pid", "pname"} {
				componentPos := originUpdateCtx.TableDef.Name2ColIndex[componentName]
				componentInsertCol := originUpdateCtx.InsertCols[componentPos]
				componentExpr := finalProject.ProjectList[componentInsertCol.ColPos]
				require.Equal(t, componentExpr.GetCol(), clusterByFunc.Args[i].GetCol())
			}
		})
	}
}

func configureProductsAsCompositeClusterByTable(t *testing.T, mock *MockOptimizer) {
	t.Helper()
	tableDef := mock.ctxt.tables["products"]
	require.NotNil(t, tableDef)
	require.Len(t, tableDef.Cols, 6)

	clusterByCol := tableDef.Cols[4]
	clusterByCol.Hidden = true
	tableDef.ClusterBy.CompCbkeyCol = clusterByCol

	fakePrimaryKey := DeepCopyColDef(mock.ctxt.tables["fake_pk_t"].Cols[2])
	tableDef.Cols = append(tableDef.Cols, nil)
	copy(tableDef.Cols[5:], tableDef.Cols[4:])
	tableDef.Cols[4] = fakePrimaryKey
	for i, col := range tableDef.Cols {
		col.ColId = uint64(i)
	}
	tableDef.Pkey = &plan.PrimaryKeyDef{
		Names:       []string{catalog.FakePrimaryKeyColName},
		PkeyColName: catalog.FakePrimaryKeyColName,
		Cols:        []uint64{4},
		CompPkeyCol: fakePrimaryKey,
	}
}

func TestDropIndexIfExistsMissingIndex(t *testing.T) {
	mock := NewMockOptimizer(true)

	logicPlan, err := runOneStmt(mock, t, "drop index if exists nonexist on test_idx")
	require.NoError(t, err)
	testDeepCopy(logicPlan)
	dropIndex := logicPlan.GetDdl().GetDropIndex()
	require.NotNil(t, dropIndex)
	require.Equal(t, "", dropIndex.GetIndexName())

	_, err = runOneStmt(mock, t, "drop index nonexist on test_idx")
	require.Error(t, err)
	require.Contains(t, err.Error(), "not found index: nonexist")
}

func TestUpdatePgStyleFromDedupsDuplicateSourceMatchesOnNewPath(t *testing.T) {
	mock := NewMockOptimizer(true)

	logicPlan, err := runOneStmt(mock, t,
		"UPDATE NATION SET N_NAME = NATION2.N_NAME FROM NATION2 WHERE NATION.N_REGIONKEY = NATION2.R_REGIONKEY")
	if err != nil {
		t.Fatalf("build UPDATE FROM plan: %v", err)
	}

	query := logicPlan.GetQuery()
	tableDef := mock.ctxt.tables["nation"]
	if hasUpdateFromDedupAnyValueAgg(query, len(tableDef.Cols)) {
		t.Fatalf("UPDATE FROM dedup should not aggregate update columns with any_value")
	}
	if !hasUpdateFromDedupWindow(query, 1) {
		t.Fatalf("UPDATE FROM should dedup duplicate source matches with row_number window partitioned by row_id")
	}
	if !hasUpdateFromDedupInt64Selector(query) {
		t.Fatalf("UPDATE FROM dedup selector should explicitly cast row_number to int64")
	}
}

func TestMultiTargetUpdateUsesIndependentModernSelectors(t *testing.T) {
	mock := NewMockOptimizer(true)
	logicPlan, err := runOneStmt(
		mock,
		t,
		"UPDATE nation n JOIN nation2 n2 ON n.n_nationkey = n2.n_nationkey "+
			"SET n.n_name = n2.n_name, n2.n_comment = n.n_comment",
	)
	require.NoError(t, err)

	query := logicPlan.GetQuery()
	var multiUpdate *plan.Node
	rowNumberWindows := 0
	guardedAssignmentProjects := 0
	for _, node := range query.Nodes {
		if node.NodeType == plan.Node_WINDOW {
			for _, specExpr := range node.WinSpecList {
				if spec := specExpr.GetW(); spec != nil &&
					spec.Name == "row_number" &&
					len(spec.PartitionBy) == 2 {
					rowNumberWindows++
				}
			}
		}
		if node.NodeType == plan.Node_MULTI_UPDATE {
			multiUpdate = node
		}
		if node.NodeType == plan.Node_PROJECT && len(node.Children) == 1 &&
			query.Nodes[node.Children[0]].NodeType == plan.Node_WINDOW {
			for _, expr := range node.ProjectList {
				if expr.GetF() != nil && expr.GetF().GetFunc().GetObjName() == "if" {
					guardedAssignmentProjects++
				}
			}
		}
	}
	require.NotNil(t, multiUpdate)
	// Each target has one pre-assignment selector for lazy evaluation and one
	// post-branch selector for physical-row deduplication.
	require.Equal(t, 4, rowNumberWindows)
	require.GreaterOrEqual(t, guardedAssignmentProjects, 2,
		"target-local assignments must be lazily evaluated above the target row-number windows")

	mainCtxs := make(map[string]*plan.UpdateCtx)
	for _, updateCtx := range multiUpdate.UpdateCtxList {
		if updateCtx.TableDef == nil {
			continue
		}
		if updateCtx.TableDef.Name == "nation" || updateCtx.TableDef.Name == "nation2" {
			mainCtxs[updateCtx.TableDef.Name] = updateCtx
		}
	}
	require.Len(t, mainCtxs, 2)
	for _, name := range []string{"nation", "nation2"} {
		updateCtx := mainCtxs[name]
		require.NotNil(t, updateCtx)
		require.True(t, updateCtx.DedupByTargetRowId)
		require.Len(t, updateCtx.DeleteCols, 4)
		require.NotEqual(t, updateCtx.DeleteCols[0].ColPos, updateCtx.DeleteCols[2].ColPos)
	}
	require.NotEqual(
		t,
		mainCtxs["nation"].DeleteCols[0].ColPos,
		mainCtxs["nation2"].DeleteCols[0].ColPos,
	)

	require.Len(t, multiUpdate.Children, 1)
	lockNode := query.Nodes[multiUpdate.Children[0]]
	require.Equal(t, plan.Node_LOCK_OP, lockNode.NodeType)
	for i := 1; i < len(lockNode.LockTargets); i++ {
		previous := lockNode.LockTargets[i-1]
		current := lockNode.LockTargets[i]
		require.True(t,
			previous.TableId < current.TableId ||
				(previous.TableId == current.TableId &&
					previous.PrimaryColIdxInBat <= current.PrimaryColIdxInBat),
		)
	}
}

func TestMultiTargetUpdateIgnoreUsesIndependentTargetBranches(t *testing.T) {
	mock := NewMockOptimizer(true)
	logicPlan, err := runOneStmt(
		mock,
		t,
		"UPDATE IGNORE emp JOIN dept ON emp.deptno = dept.deptno "+
			"SET emp.empno = dept.deptno, dept.loc = emp.ename",
	)
	require.NoError(t, err)

	unionCount := 0
	var multiUpdate *plan.Node
	for _, node := range logicPlan.GetQuery().Nodes {
		if node.NodeType == plan.Node_UNION_ALL {
			unionCount++
		}
		if node.NodeType == plan.Node_MULTI_UPDATE {
			multiUpdate = node
		}
	}
	require.GreaterOrEqual(t, unionCount, 1)
	require.NotNil(t, multiUpdate)
	for _, updateCtx := range multiUpdate.UpdateCtxList {
		if updateCtx.DedupByTargetRowId {
			require.GreaterOrEqual(t, len(updateCtx.DeleteCols), 4)
		}
	}
}

func TestMultiTargetUpdateSupportsTwoAutoIncrementTargets(t *testing.T) {
	mock := NewMockOptimizer(true)
	for _, tableName := range []string{"nation", "nation2"} {
		tableDef := mock.ctxt.tables[tableName]
		pkPos := tableDef.Name2ColIndex[tableDef.Pkey.PkeyColName]
		tableDef.Cols[pkPos].Typ.AutoIncr = true
	}

	logicPlan, err := runOneStmt(
		mock,
		t,
		"UPDATE nation n JOIN nation2 n2 ON n.n_nationkey = n2.n_nationkey "+
			"SET n.n_nationkey = DEFAULT, n2.n_nationkey = DEFAULT",
	)
	require.NoError(t, err)

	preInsertCount := 0
	for _, node := range logicPlan.GetQuery().Nodes {
		if node.NodeType != plan.Node_PRE_INSERT {
			continue
		}
		preInsertCount++
		require.True(t, node.PreInsertCtx.HasTargetSelector)
	}
	require.Equal(t, 2, preInsertCount)
}

func TestPartitionedMultiTargetUpdateUsesModernPlan(t *testing.T) {
	for _, test := range []struct {
		sql                  string
		partitionColumnCount int
	}{
		{
			sql: "UPDATE nation n JOIN nation2 n2 ON n.n_nationkey = n2.n_nationkey " +
				"SET n.n_name = n2.n_name, n2.n_comment = n.n_comment",
			partitionColumnCount: 1,
		},
		{
			sql: "UPDATE nation2 n2 JOIN nation n ON n.n_nationkey = n2.n_nationkey " +
				"SET n2.n_comment = n.n_comment, n.n_name = n2.n_name",
			partitionColumnCount: 1,
		},
		{
			sql: "UPDATE nation n JOIN nation2 n2 ON n.n_nationkey = n2.n_nationkey " +
				"SET n.n_nationkey = n.n_nationkey + 10, n2.n_comment = n.n_comment",
			partitionColumnCount: 2,
		},
	} {
		mock := NewMockOptimizer(true)
		mock.ctxt.tables["nation"].FeatureFlag |= features.Partitioned
		mock.ctxt.tables["nation"].Partition = &plan.Partition{
			PartitionDefs: []*plan.PartitionDef{{
				Def: &plan.Expr{Expr: &plan.Expr_F{F: &plan.Function{Args: []*plan.Expr{
					{Expr: &plan.Expr_Col{Col: &plan.ColRef{Name: "n_nationkey"}}},
				}}}},
			}},
		}
		logicPlan, err := runOneStmt(mock, t, test.sql)
		require.NoError(t, err)

		multiUpdates := 0
		for _, node := range logicPlan.GetQuery().Nodes {
			if node.NodeType == plan.Node_MULTI_UPDATE {
				multiUpdates++
				require.Len(t, node.UpdateCtxList, 2)
				for _, updateCtx := range node.UpdateCtxList {
					if updateCtx.TableDef.Name == "nation" {
						require.Len(t, updateCtx.PartitionCols, test.partitionColumnCount)
						require.NotEqual(t, int32(-1), updateCtx.PartitionCols[0].ColPos)
					} else {
						require.Empty(t, updateCtx.PartitionCols)
					}
				}
			}
		}
		require.Equal(t, 1, multiUpdates)
	}
}

func TestReadOnlySiblingAliasIsNotWritableTarget(t *testing.T) {
	mock := NewMockOptimizer(true)
	logicPlan, err := runOneStmt(
		mock,
		t,
		"UPDATE nation a JOIN nation b ON a.n_nationkey = b.n_nationkey "+
			"SET a.n_name = b.n_name",
	)
	require.NoError(t, err)
	require.NotNil(t, logicPlan.GetQuery())
}

func TestModernMultiTargetOnUpdateColumnsKeepActiveSelectorsTyped(t *testing.T) {
	for _, sql := range []string{
		"UPDATE emp, dept SET emp.job = 'a', dept.loc = 'b' " +
			"WHERE emp.deptno = dept.deptno",
	} {
		mock := NewMockOptimizer(true)
		setMockOnUpdateExpr(t, mock, "nation", "n_regionkey", "1")
		setMockOnUpdateExpr(t, mock, "emp", "sal", "1")
		setMockOnUpdateExpr(t, mock, "dept", "dname", "'updated'")

		logicPlan, err := runOneStmt(mock, t, sql)
		require.NoError(t, err, sql)
		multiUpdates := 0
		for _, node := range logicPlan.GetQuery().Nodes {
			if node.NodeType == plan.Node_MULTI_UPDATE {
				multiUpdates++
			}
		}
		require.Equal(t, 1, multiUpdates)
	}
}

func TestUpdatePgStyleFromDedupPicksWholeSourceRow(t *testing.T) {
	mock := NewMockOptimizer(true)

	logicPlan, err := runOneStmt(mock, t,
		"UPDATE NATION SET N_NAME = NATION2.N_NAME, N_COMMENT = NATION2.N_COMMENT FROM NATION2 WHERE NATION.N_REGIONKEY = NATION2.R_REGIONKEY")
	if err != nil {
		t.Fatalf("build UPDATE FROM plan: %v", err)
	}

	query := logicPlan.GetQuery()
	if hasUpdateFromDedupAnyValueAgg(query, len(mock.ctxt.tables["nation"].Cols)) {
		t.Fatalf("UPDATE FROM dedup must pick a whole source row, not aggregate each update column with any_value")
	}
	if !hasUpdateFromDedupWindow(query, 1) {
		t.Fatalf("UPDATE FROM dedup should use row_number window partitioned by target row_id")
	}
}

func TestUpdatePgStyleFromDedupFKTablePicksWholeSourceRow(t *testing.T) {
	mock := NewMockOptimizer(true)

	logicPlan, err := runOneStmt(mock, t,
		"UPDATE emp SET sal = dept.deptno, comm = dept.deptno FROM dept WHERE emp.deptno = dept.deptno")
	if err != nil {
		t.Fatalf("build UPDATE FROM plan: %v", err)
	}

	query := logicPlan.GetQuery()
	if hasUpdateFromDedupAnyValueAgg(query, len(mock.ctxt.tables["emp"].Cols)) {
		t.Fatalf("UPDATE FROM dedup must pick a whole source row, not aggregate each update column with any_value")
	}
	if !hasUpdateFromDedupWindow(query, 1) {
		t.Fatalf("UPDATE FROM dedup should use row_number window partitioned by target row_id")
	}
}

// TestUpdatePgStyleFromDedupPartitionsByRowIDNotGeometry32 guards the new
// bindUpdate path against the GEOMETRY32 partition-key crash: T_geometry32 has
// no comparator in pkg/compare, so a row_number window partitioned on a
// GEOMETRY32 target column would build a nil comparator and crash at runtime.
// The dedup key must be row_id, never the geometry column.
func TestUpdatePgStyleFromDedupPartitionsByRowIDNotGeometry32(t *testing.T) {
	mock := NewMockOptimizer(true)
	geoTyp := plan.Type{Id: int32(types.T_geometry32)}
	setMockColumnType(t, mock, "nation", "n_comment", geoTyp)

	logicPlan, err := runOneStmt(mock, t,
		"UPDATE NATION SET N_NAME = NATION2.N_NAME FROM NATION2 WHERE NATION.N_REGIONKEY = NATION2.R_REGIONKEY")
	if err != nil {
		t.Fatalf("build UPDATE FROM with GEOMETRY32 column: %v", err)
	}

	query := logicPlan.GetQuery()
	if !hasUpdateFromDedupWindow(query, 1) {
		t.Fatalf("UPDATE FROM dedup must partition by row_id, not by a GEOMETRY32 target column")
	}
	if updateFromDedupPartitionsColName(query, "n_comment") {
		t.Fatalf("UPDATE FROM dedup must not include the GEOMETRY32 column in the partition key")
	}
}

// TestUpdatePgStyleFromDedupFKTablePartitionsByRowIDNotGeometry32 guards the
// modern path for an FK-bearing target against the same GEOMETRY32
// partition-key crash.
func TestUpdatePgStyleFromDedupFKTablePartitionsByRowIDNotGeometry32(t *testing.T) {
	mock := NewMockOptimizer(true)
	geoTyp := plan.Type{Id: int32(types.T_geometry32)}
	setMockColumnType(t, mock, "emp", "hiredate", geoTyp)

	logicPlan, err := runOneStmt(mock, t,
		"UPDATE emp SET sal = dept.deptno, comm = dept.deptno FROM dept WHERE emp.deptno = dept.deptno")
	if err != nil {
		t.Fatalf("build FK-table UPDATE FROM with GEOMETRY32 column: %v", err)
	}

	query := logicPlan.GetQuery()
	if !hasUpdateFromDedupWindow(query, 1) {
		t.Fatalf("FK-table UPDATE FROM dedup must partition by row_id, not by a GEOMETRY32 target column")
	}
	if updateFromDedupPartitionsColName(query, "hiredate") {
		t.Fatalf("FK-table UPDATE FROM dedup must not include the GEOMETRY32 column in the partition key")
	}
}

// TestUpdatePgStyleFromFKTableUsesModernDedup guards the new FK-table route:
// unrelated child columns stay on the modern row_number dedup path.
func TestUpdatePgStyleFromFKTableUsesModernDedup(t *testing.T) {
	mock := NewMockOptimizer(true)

	logicPlan, err := runOneStmt(mock, t,
		"UPDATE emp SET sal = dept.deptno, comm = dept.deptno FROM dept WHERE emp.deptno = dept.deptno")
	if err != nil {
		t.Fatalf("build FK-table UPDATE FROM plan: %v", err)
	}

	query := logicPlan.GetQuery()
	if hasAnyValueAgg(query) {
		t.Fatalf("modern FK-table UPDATE FROM dedup must not use any_value aggregation")
	}
	if !hasUpdateFromDedupWindow(query, 1) {
		t.Fatalf("modern FK-table UPDATE FROM must use row_number partitioned by target row_id")
	}
}

func TestUpdatePgStyleFromDedupExpandsDefaultBeforeDedup(t *testing.T) {
	mock := NewMockOptimizer(true)
	setMockDefaultExpr(t, mock, "nation", "n_name", "name-default")

	logicPlan, err := runOneStmt(mock, t,
		"UPDATE NATION SET N_NAME = DEFAULT FROM NATION2 WHERE NATION.N_REGIONKEY = NATION2.R_REGIONKEY")
	if err != nil {
		t.Fatalf("build UPDATE FROM with DEFAULT: %v", err)
	}

	query := logicPlan.GetQuery()
	if queryContainsDefaultVal(query) {
		t.Fatalf("UPDATE FROM dedup should run after DEFAULT expansion")
	}
	if !queryContainsStringLiteral(query, "name-default") {
		t.Fatalf("UPDATE FROM dedup should retain the expanded DEFAULT expression")
	}
	if hasUpdateFromDedupAnyValueAgg(query, len(mock.ctxt.tables["nation"].Cols)) {
		t.Fatalf("UPDATE FROM dedup should not wrap DEFAULT with any_value")
	}
}

func TestUpdatePgStyleFromDedupAllowsVectorUpdateColumn(t *testing.T) {
	mock := NewMockOptimizer(true)
	vecTyp := plan.Type{Id: int32(types.T_array_float32), Width: 4}
	setMockColumnType(t, mock, "nation", "n_comment", vecTyp)
	setMockColumnType(t, mock, "nation2", "n_comment", vecTyp)

	_, err := runOneStmt(mock, t,
		"UPDATE NATION SET N_COMMENT = NATION2.N_COMMENT FROM NATION2 WHERE NATION.N_REGIONKEY = NATION2.R_REGIONKEY")
	if err != nil {
		t.Fatalf("UPDATE FROM should allow vector update columns through row-level dedup: %v", err)
	}
}

func TestUpdatePgStyleFromDedupKeepsGeneratedColumnsAfterDedup(t *testing.T) {
	mock := NewMockOptimizer(true)
	setMockGeneratedColumn(t, mock, "nation", "n_comment", "n_name")

	logicPlan, err := runOneStmt(mock, t,
		"UPDATE NATION SET N_NAME = NATION2.N_NAME FROM NATION2 WHERE NATION.N_REGIONKEY = NATION2.R_REGIONKEY")
	if err != nil {
		t.Fatalf("build UPDATE FROM with generated column: %v", err)
	}

	query := logicPlan.GetQuery()
	if hasUpdateFromDedupAnyValueAgg(query, len(mock.ctxt.tables["nation"].Cols)) {
		t.Fatalf("dedup should not aggregate generated or update columns with any_value")
	}
	if !hasUpdateFromDedupWindow(query, 1) {
		t.Fatalf("UPDATE FROM with generated column should still use row-level dedup")
	}
}

func TestUpdatePgStyleFromDedupAllowsDecimal256AndEnumUpdateColumns(t *testing.T) {
	tests := []struct {
		name string
		typ  plan.Type
		sql  string
	}{
		{
			name: "decimal256",
			typ:  plan.Type{Id: int32(types.T_decimal256), Width: 65, Scale: 30},
			sql:  "UPDATE NATION SET N_COMMENT = REGION.R_COMMENT FROM REGION WHERE NATION.N_REGIONKEY = REGION.R_REGIONKEY",
		},
		{
			name: "enum",
			typ:  plan.Type{Id: int32(types.T_enum), Enumvalues: "small,medium,large"},
			sql:  "UPDATE NATION SET N_COMMENT = CASE WHEN 1 > 0 THEN 'small' ELSE 'medium' END FROM REGION WHERE NATION.N_REGIONKEY = REGION.R_REGIONKEY",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mock := NewMockOptimizer(true)
			setMockColumnType(t, mock, "nation", "n_comment", tt.typ)
			setMockColumnType(t, mock, "region", "r_comment", tt.typ)

			_, err := runOneStmt(mock, t, tt.sql)
			if err != nil {
				t.Fatalf("UPDATE FROM should allow %s update columns through row-level dedup: %v", tt.name, err)
			}
		})
	}
}

func TestModernMultiTargetGeneratedColumnsKeepTargetContexts(t *testing.T) {
	mock := NewMockOptimizer(true)
	setMockGeneratedColumn(t, mock, "emp", "ename", "job")
	setMockGeneratedColumn(t, mock, "dept", "dname", "loc")

	logicPlan, err := runOneStmt(mock, t,
		"UPDATE emp, dept SET emp.job = dept.loc, dept.loc = emp.job WHERE emp.deptno = dept.deptno")
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"dept", "emp"}, modernBaseUpdateContextNames(logicPlan.GetQuery()))
}

// TestModernMultiTargetUpdateContextLayoutDeterministic guards the stable
// per-target physical write layout. A fresh optimizer per iteration rebuilds
// assignment maps, so accidental map-order dependence remains observable.
func TestModernMultiTargetUpdateContextLayoutDeterministic(t *testing.T) {
	const sql = "UPDATE emp, dept SET emp.mgr = 1, emp.sal = 2, dept.loc = 'x' WHERE emp.deptno = dept.deptno"
	var want []string
	for iter := 0; iter < 16; iter++ {
		mock := NewMockOptimizer(true)
		logicPlan, err := runOneStmt(mock, t, sql)
		require.NoError(t, err, "iteration %d", iter)
		got := modernUpdateContextLayout(logicPlan.GetQuery())
		require.NotEmpty(t, got, "iteration %d", iter)
		if iter == 0 {
			want = got
			continue
		}
		assert.Equal(t, want, got,
			"modern UPDATE context layout must be deterministic across builds (iter %d)", iter)
	}
}

func modernBaseUpdateContextNames(query *Query) []string {
	var names []string
	for _, node := range query.Nodes {
		if node.NodeType != plan.Node_MULTI_UPDATE {
			continue
		}
		for _, updateCtx := range node.UpdateCtxList {
			if updateCtx.TableDef != nil &&
				(updateCtx.TableDef.Name == "emp" || updateCtx.TableDef.Name == "dept") {
				names = append(names, updateCtx.TableDef.Name)
			}
		}
	}
	return names
}

func modernUpdateContextLayout(query *Query) []string {
	var layout []string
	for _, node := range query.Nodes {
		if node.NodeType != plan.Node_MULTI_UPDATE {
			continue
		}
		for _, updateCtx := range node.UpdateCtxList {
			if updateCtx.TableDef == nil {
				continue
			}
			layout = append(layout, fmt.Sprintf(
				"%s:insert=%v:delete=%v:partition=%v:target=%d",
				updateCtx.TableDef.Name,
				updateCtx.InsertCols,
				updateCtx.DeleteCols,
				updateCtx.PartitionCols,
				updateCtx.TargetUpdateCtxIdx,
			))
		}
	}
	return layout
}

func TestModernMultiTargetGeneratedColumnsUseDefault(t *testing.T) {
	mock := NewMockOptimizer(true)
	setMockDefaultExpr(t, mock, "emp", "job", "job-default")
	setMockGeneratedColumn(t, mock, "emp", "ename", "job")

	logicPlan, err := runOneStmt(mock, t,
		"UPDATE emp, dept SET emp.job = DEFAULT, dept.loc = 'default-marker' WHERE emp.deptno = dept.deptno")
	require.NoError(t, err)
	require.True(t, queryContainsStringLiteral(logicPlan.GetQuery(), "job-default"))
	require.True(t, queryContainsStringLiteral(logicPlan.GetQuery(), "default-marker"))
}

func TestModernMultiTargetGeneratedColumnsUseOnUpdate(t *testing.T) {
	mock := NewMockOptimizer(true)
	setMockOnUpdateExpr(t, mock, "emp", "job", "job-on-update")
	setMockGeneratedColumn(t, mock, "emp", "ename", "job")

	logicPlan, err := runOneStmt(mock, t,
		"UPDATE emp, dept SET emp.comm = 1, dept.loc = 'on-update-marker' WHERE emp.deptno = dept.deptno")
	require.NoError(t, err)
	require.True(t, queryContainsStringLiteral(logicPlan.GetQuery(), "job-on-update"))
	require.True(t, queryContainsStringLiteral(logicPlan.GetQuery(), "on-update-marker"))
}

func TestModernMultiTargetGeneratedColumnChainBuildsCompleteContexts(t *testing.T) {
	mock := NewMockOptimizer(true)
	setMockGeneratedColumn(t, mock, "emp", "mgr", "empno")
	setMockGeneratedColumn(t, mock, "emp", "deptno", "mgr")
	emp := mock.ctxt.tables["emp"]
	var empnoPos, mgrPos, deptnoPos int32
	for pos, col := range emp.Cols {
		switch col.Name {
		case "empno":
			empnoPos = int32(pos)
		case "mgr":
			mgrPos = int32(pos)
		case "deptno":
			deptnoPos = int32(pos)
		}
	}

	logicPlan, err := runOneStmt(mock, t,
		"UPDATE emp, dept SET emp.comm = 1, dept.loc = 'chain-marker' WHERE emp.deptno = dept.deptno")
	require.NoError(t, err)
	query := logicPlan.GetQuery()
	require.ElementsMatch(t, []string{"dept", "emp"}, modernBaseUpdateContextNames(logicPlan.GetQuery()))
	require.True(t, queryContainsStringLiteral(logicPlan.GetQuery(), "chain-marker"))

	var chainNodeID int32 = -1
	for nodeID, node := range query.Nodes {
		if node.NodeType != plan.Node_PROJECT || len(node.ProjectList) <= int(deptnoPos) {
			continue
		}
		mgrRewrite := node.ProjectList[mgrPos].GetF()
		deptnoRewrite := node.ProjectList[deptnoPos].GetF()
		if mgrRewrite == nil || mgrRewrite.Func.GetObjName() != "if" || len(mgrRewrite.Args) != 3 ||
			deptnoRewrite == nil || deptnoRewrite.Func.GetObjName() != "if" || len(deptnoRewrite.Args) != 3 {
			continue
		}
		freshMgr := mgrRewrite.Args[1]
		if freshMgr.GetCol() == nil || freshMgr.GetCol().ColPos != empnoPos {
			continue
		}
		// deptno is generated from mgr. Its active-row branch must consume the
		// complete freshly recomputed mgr row image, not the stale input column.
		require.Equal(t, node.ProjectList[mgrPos], deptnoRewrite.Args[1])
		chainNodeID = int32(nodeID)
		break
	}
	require.NotEqual(t, int32(-1), chainNodeID,
		"the modern plan must preserve the two-layer generated-column row-image dependency")
	require.True(t, slices.ContainsFunc(query.Nodes, func(node *plan.Node) bool {
		return node.NodeType == plan.Node_MULTI_UPDATE && len(node.Children) == 1 &&
			planNodeDependsOn(query, node.Children[0], chainNodeID, make(map[int32]struct{}))
	}), "the generated-column chain must feed the physical MULTI_UPDATE")
}

func TestPreparedForeignKeyActionsMarkQueryUncacheable(t *testing.T) {
	t.Run("ordinary child update marks prepare uncacheable", func(t *testing.T) {
		mock := NewMockOptimizer(true)
		setMockEmpDeptForeignKeyAction(t, mock, plan.ForeignKeyDef_SET_NULL, plan.ForeignKeyDef_CASCADE)

		query := buildPreparedQuery(t, mock, "prepare stmt1 from update emp set deptno = ? where empno = ?")
		require.True(t, query.GetHasForeignKeyAction())
	})

	t.Run("unrelated child update remains cacheable", func(t *testing.T) {
		mock := NewMockOptimizer(true)
		setMockEmpDeptForeignKeyAction(t, mock, plan.ForeignKeyDef_SET_NULL, plan.ForeignKeyDef_CASCADE)

		query := buildPreparedQuery(t, mock, "prepare stmt1 from update emp set ename = ? where empno = ?")
		require.False(t, query.GetHasForeignKeyAction())
	})

	t.Run("parent update cascade marks prepare uncacheable", func(t *testing.T) {
		mock := NewMockOptimizer(true)
		setMockEmpDeptForeignKeyAction(t, mock, plan.ForeignKeyDef_RESTRICT, plan.ForeignKeyDef_CASCADE)

		query := buildPreparedQuery(t, mock, "prepare stmt1 from update dept set deptno = deptno + 10 where deptno = ?")
		require.True(t, query.GetHasForeignKeyAction())
	})

	t.Run("unrelated parent update remains cacheable", func(t *testing.T) {
		mock := NewMockOptimizer(true)
		setMockEmpDeptForeignKeyAction(t, mock, plan.ForeignKeyDef_RESTRICT, plan.ForeignKeyDef_CASCADE)

		query := buildPreparedQuery(t, mock, "prepare stmt1 from update dept set loc = ? where deptno = ?")
		require.False(t, query.GetHasForeignKeyAction())
	})

	t.Run("child update remains uncacheable with checks disabled", func(t *testing.T) {
		mock := NewMockOptimizer(true)
		setMockEmpDeptForeignKeyAction(t, mock, plan.ForeignKeyDef_SET_NULL, plan.ForeignKeyDef_CASCADE)
		mock.ctxt.ResolveVariableFunc = func(name string, _, _ bool) (interface{}, error) {
			switch name {
			case "foreign_key_checks":
				return int64(0), nil
			case "sql_mode":
				return "", nil
			default:
				return nil, moerr.NewInternalError(context.Background(), "unexpected variable")
			}
		}

		query := buildPreparedQuery(t, mock, "prepare stmt1 from update emp set deptno = ? where empno = ?")
		require.True(t, query.GetHasForeignKeyAction())
	})

	t.Run("parent delete set null marks prepare uncacheable", func(t *testing.T) {
		mock := NewMockOptimizer(true)
		setMockEmpDeptForeignKeyAction(t, mock, plan.ForeignKeyDef_SET_NULL, plan.ForeignKeyDef_RESTRICT)

		query := buildPreparedQuery(t, mock, "prepare stmt1 from delete from dept where deptno = ?")
		require.True(t, query.GetHasForeignKeyAction())
	})

	t.Run("parent delete restrict keeps prepare cacheable", func(t *testing.T) {
		mock := NewMockOptimizer(true)
		setMockEmpDeptForeignKeyAction(t, mock, plan.ForeignKeyDef_RESTRICT, plan.ForeignKeyDef_RESTRICT)

		query := buildPreparedQuery(t, mock, "prepare stmt1 from delete from dept where deptno = ?")
		require.False(t, query.GetHasForeignKeyAction())
	})
}

func TestDeleteSetNullMaintainsCompositeSecondaryIndexEntry(t *testing.T) {
	mock := NewMockOptimizer(true)
	setMockEmpDeptForeignKeyAction(t, mock, plan.ForeignKeyDef_SET_NULL, plan.ForeignKeyDef_RESTRICT)

	emp := mock.ctxt.tables["emp"]
	require.Len(t, emp.Indexes, 2)
	emp.Indexes = emp.Indexes[1:]
	require.False(t, emp.Indexes[0].Unique)
	emp.Indexes[0].Parts = []string{"deptno", "ename", catalog.AliasPrefix + "empno"}

	logicPlan, err := runOneStmt(mock, t, "delete from dept where deptno = 10")
	require.NoError(t, err)
	query := logicPlan.GetQuery()
	require.Equal(t, 1, countUpdateFkPlanNodes(query, plan.Node_PRE_INSERT_SK),
		"a composite secondary index retains a row whose key has a NULL component")
}

func TestDeleteSetNullDropsSingleColumnSecondaryIndexEntry(t *testing.T) {
	mock := NewMockOptimizer(true)
	setMockEmpDeptForeignKeyAction(t, mock, plan.ForeignKeyDef_SET_NULL, plan.ForeignKeyDef_RESTRICT)

	emp := mock.ctxt.tables["emp"]
	require.Len(t, emp.Indexes, 2)
	emp.Indexes = emp.Indexes[1:]
	require.False(t, emp.Indexes[0].Unique)
	emp.Indexes[0].Parts = []string{"deptno", catalog.AliasPrefix + "empno"}

	logicPlan, err := runOneStmt(mock, t, "delete from dept where deptno = 10")
	require.NoError(t, err)
	require.Zero(t, countUpdateFkPlanNodes(logicPlan.GetQuery(), plan.Node_PRE_INSERT_SK),
		"a single-column secondary index compacts the NULL replacement key")
}

func TestPreparedInsertForeignKeyPlansRemainSensitiveAcrossChecks(t *testing.T) {
	statements := []struct {
		name string
		sql  string
	}{
		{name: "plain insert", sql: "prepare stmt1 from insert into replace_fk_c values (?, ?)"},
		{name: "insert ignore", sql: "prepare stmt1 from insert ignore into replace_fk_c values (?, ?)"},
		{name: "on duplicate key update", sql: "prepare stmt1 from insert into replace_fk_c values (?, ?) on duplicate key update pid = values(pid)"},
		{
			name: "no real key on duplicate key update fallback",
			sql:  "prepare stmt1 from insert into insert_fk_no_key_c values (?, ?) on duplicate key update pid = values(pid)",
		},
	}

	for _, checks := range []int64{0, 1} {
		for _, statement := range statements {
			t.Run(fmt.Sprintf("%s/checks=%d", statement.name, checks), func(t *testing.T) {
				mock := NewMockOptimizer(true)
				mock.ctxt.ResolveVariableFunc = func(name string, _, _ bool) (interface{}, error) {
					switch name {
					case "foreign_key_checks":
						return checks, nil
					case "sql_mode":
						return "", nil
					default:
						return nil, moerr.NewInternalError(context.Background(), "unexpected variable")
					}
				}

				query := buildPreparedQuery(t, mock, statement.sql)
				require.True(t, query.GetHasForeignKeyAction(),
					"prepared INSERT into an FK child must observe foreign_key_checks at each execution")
			})
		}
	}
}

func buildPreparedQuery(t *testing.T, mock *MockOptimizer, sql string) *plan.Query {
	t.Helper()

	logicPlan, err := runOneStmt(mock, t, sql)
	require.NoError(t, err)
	queryPlan := resolveQueryPlan(logicPlan)
	require.NotNil(t, queryPlan.GetQuery())
	return queryPlan.GetQuery()
}

func setMockEmpDeptForeignKeyAction(
	t *testing.T,
	mock *MockOptimizer,
	onDelete plan.ForeignKeyDef_RefAction,
	onUpdate plan.ForeignKeyDef_RefAction,
) {
	t.Helper()

	const (
		mockDeptTableID uint64 = 88888
		mockEmpTableID  uint64 = 88889
	)

	empTable := mock.ctxt.tables["emp"]
	require.NotNil(t, empTable)
	require.NotEmpty(t, empTable.Fkeys)

	deptTable := mock.ctxt.tables["dept"]
	require.NotNil(t, deptTable)

	delete(mock.ctxt.id2name, empTable.TblId)
	delete(mock.ctxt.id2name, deptTable.TblId)
	empTable.TblId = mockEmpTableID
	deptTable.TblId = mockDeptTableID
	mock.ctxt.id2name[mockEmpTableID] = "emp"
	mock.ctxt.id2name[mockDeptTableID] = "dept"
	require.NotNil(t, mock.ctxt.objects["emp"])
	require.NotNil(t, mock.ctxt.objects["dept"])
	mock.ctxt.objects["emp"].Obj = int64(mockEmpTableID)
	mock.ctxt.objects["dept"].Obj = int64(mockDeptTableID)

	empTable.Fkeys[0].ForeignTbl = deptTable.TblId
	empTable.Fkeys[0].ForeignCols = []uint64{0}
	empTable.Fkeys[0].OnDelete = onDelete
	empTable.Fkeys[0].OnUpdate = onUpdate

	deptTable.RefChildTbls = []uint64{empTable.TblId}
}

func TestModernMultiTargetNonFirstTableGeneratedColumn(t *testing.T) {
	mock := NewMockOptimizer(true)
	// Generate dname from loc on the second table (dept).
	setMockGeneratedColumn(t, mock, "dept", "dname", "loc")

	logicPlan, err := runOneStmt(mock, t,
		"UPDATE emp, dept SET emp.comm = 1, dept.loc = 'non-first-gen' WHERE emp.deptno = dept.deptno")
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"dept", "emp"}, modernBaseUpdateContextNames(logicPlan.GetQuery()))
	require.True(t, queryContainsStringLiteral(logicPlan.GetQuery(), "non-first-gen"))
}

func TestMultiTargetUpdateGeneratedColumnGuardUsesProjectInput(t *testing.T) {
	mock := NewMockOptimizer(true)
	setMockGeneratedColumn(t, mock, "dept", "dname", "loc")

	logicPlan, err := runOneStmt(mock, t,
		"UPDATE emp, dept SET emp.comm = 1, dept.loc = 'modern-gen' WHERE emp.deptno = dept.deptno")
	require.NoError(t, err)
	require.True(t, queryContainsStringLiteral(logicPlan.GetQuery(), "modern-gen"))
}

func TestModernMultiTargetGeneratedColumnChainSurvivesOptimize(t *testing.T) {
	mock := NewMockOptimizer(true)
	// Chain: sal depends on comm, comm is a SET column.
	// After optimization and rewrite, sal's generated expr should use the SET value of comm.
	setMockGeneratedColumn(t, mock, "emp", "sal", "comm")

	logicPlan, err := runOneStmt(mock, t,
		"UPDATE emp, dept SET emp.comm = 1, dept.loc = 'chain-opt-marker' WHERE emp.deptno = dept.deptno")
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"dept", "emp"}, modernBaseUpdateContextNames(logicPlan.GetQuery()))
	require.True(t, queryContainsStringLiteral(logicPlan.GetQuery(), "chain-opt-marker"))
}

func TestUpdateGeneratedColumnDerivedTableSourceOnFKTable(t *testing.T) {
	mock := NewMockOptimizer(true)
	setMockGeneratedColumn(t, mock, "emp", "sal", "comm")

	logicPlan, err := runOneStmt(mock, t,
		"UPDATE emp SET comm = 1, ename = 'derived-src-marker' FROM (SELECT deptno, loc FROM dept) AS d WHERE emp.deptno = d.deptno")
	if err != nil {
		t.Fatalf("build modern FK-table update with derived source and generated column: %v", err)
	}

	query := logicPlan.GetQuery()
	hasMultiUpdate := false
	for _, node := range query.Nodes {
		if node.NodeType == plan.Node_MULTI_UPDATE {
			hasMultiUpdate = true
			break
		}
	}
	if !hasMultiUpdate {
		t.Fatal("FK-table UPDATE with an unrelated child key must use MULTI_UPDATE")
	}
	if !queryContainsStringLiteral(query, "derived-src-marker") {
		t.Fatal("modern UPDATE must retain the derived-source assignment")
	}
}

func setMockGeneratedColumn(t *testing.T, mock *MockOptimizer, tableName, generatedName, sourceName string) {
	tableDef := mock.ctxt.tables[tableName]
	if tableDef == nil {
		t.Fatalf("missing mock table %s", tableName)
	}

	var generatedCol *ColDef
	var sourceCol *ColDef
	var sourcePos int32
	for idx, col := range tableDef.Cols {
		switch col.Name {
		case generatedName:
			generatedCol = col
		case sourceName:
			sourceCol = col
			sourcePos = int32(idx)
		}
	}
	if generatedCol == nil {
		t.Fatalf("missing generated column %s.%s", tableName, generatedName)
	}
	if sourceCol == nil {
		t.Fatalf("missing generated source column %s.%s", tableName, sourceName)
	}

	generatedCol.GeneratedCol = &plan.GeneratedCol{
		Expr: &plan.Expr{
			Typ: sourceCol.Typ,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: 0,
					ColPos: sourcePos,
					Name:   sourceName,
				},
			},
		},
		IsStored: true,
	}
}

func setMockDefaultExpr(t *testing.T, mock *MockOptimizer, tableName, colName, value string) {
	col := requireMockColumn(t, mock, tableName, colName)
	col.Default = &plan.Default{
		Expr:         makeStringConstExpr(col.Typ, value),
		OriginString: value,
		NullAbility:  true,
	}
}

func setMockOnUpdateExpr(t *testing.T, mock *MockOptimizer, tableName, colName, value string) {
	col := requireMockColumn(t, mock, tableName, colName)
	col.OnUpdate = &plan.OnUpdate{
		Expr:         makeStringConstExpr(col.Typ, value),
		OriginString: value,
	}
}

func setMockColumnType(t *testing.T, mock *MockOptimizer, tableName, colName string, typ plan.Type) {
	col := requireMockColumn(t, mock, tableName, colName)
	col.Typ = typ
}

func requireMockColumn(t *testing.T, mock *MockOptimizer, tableName, colName string) *ColDef {
	tableDef := mock.ctxt.tables[tableName]
	if tableDef == nil {
		t.Fatalf("missing mock table %s", tableName)
	}
	for _, col := range tableDef.Cols {
		if col.Name == colName {
			return col
		}
	}
	t.Fatalf("missing mock column %s.%s", tableName, colName)
	return nil
}

func makeStringConstExpr(typ plan.Type, value string) *plan.Expr {
	return &plan.Expr{
		Typ: typ,
		Expr: &plan.Expr_Lit{
			Lit: &plan.Literal{
				Value: &plan.Literal_Sval{Sval: value},
			},
		},
	}
}

func hasUpdateFromDedupAnyValueAgg(query *Query, groupByLen int) bool {
	for _, node := range query.Nodes {
		if node.NodeType != plan.Node_AGG || len(node.GroupBy) != groupByLen {
			continue
		}
		for _, aggExpr := range node.AggList {
			if fn := aggExpr.GetF(); fn != nil && fn.Func.ObjName == "any_value" {
				return true
			}
		}
	}
	return false
}

// hasAnyValueAgg reports whether the plan contains any AGG node aggregating with
// any_value, regardless of GROUP BY shape.
func hasAnyValueAgg(query *Query) bool {
	for _, node := range query.Nodes {
		if node.NodeType != plan.Node_AGG {
			continue
		}
		for _, aggExpr := range node.AggList {
			if fn := aggExpr.GetF(); fn != nil && fn.Func.ObjName == "any_value" {
				return true
			}
		}
	}
	return false
}

// hasUpdateFromDedupWindow reports whether the plan contains a row_number window
// used for UPDATE ... FROM dedup, partitioned on exactly partitionByLen row_id
// columns. The dedup key must be the target row's physical identity (row_id),
// not the whole old target row, so every partition expr must reference row_id.
func hasUpdateFromDedupWindow(query *Query, partitionByLen int) bool {
	for _, node := range query.Nodes {
		if node.NodeType != plan.Node_WINDOW {
			continue
		}
		for _, winExpr := range node.WinSpecList {
			spec := winExpr.GetW()
			if spec == nil || spec.Name != "row_number" || len(spec.PartitionBy) != partitionByLen {
				continue
			}
			allRowID := true
			for _, partExpr := range spec.PartitionBy {
				if !exprContainsColName(partExpr, catalog.Row_ID) {
					allRowID = false
					break
				}
			}
			if allRowID {
				return true
			}
		}
	}
	return false
}

// hasUpdateFromDedupInt64Selector verifies that the internal UPDATE ... FROM
// dedup consumer converts ROW_NUMBER's public unsigned result to its signed
// selector contract at the projection boundary.
func hasUpdateFromDedupInt64Selector(query *Query) bool {
	for _, node := range query.Nodes {
		if node.NodeType != plan.Node_PROJECT {
			continue
		}
		for _, expr := range node.ProjectList {
			if expr.Typ.Id != int32(types.T_int64) {
				continue
			}
			fn := expr.GetF()
			if fn == nil || fn.Func == nil || fn.Func.ObjName != "cast" || len(fn.Args) != 2 {
				continue
			}
			col := fn.Args[0].GetCol()
			if col != nil && col.Name == "__mo_update_from_dedup_row_number" &&
				fn.Args[0].Typ.Id == int32(types.T_uint64) {
				return true
			}
		}
	}
	return false
}

// updateFromDedupPartitionsColName reports whether any row_number dedup window
// partitions on the given column name. Used to assert that columns without a
// stable comparator (e.g. GEOMETRY32) never end up in the dedup partition key.
func updateFromDedupPartitionsColName(query *Query, colName string) bool {
	for _, node := range query.Nodes {
		if node.NodeType != plan.Node_WINDOW {
			continue
		}
		for _, winExpr := range node.WinSpecList {
			spec := winExpr.GetW()
			if spec == nil || spec.Name != "row_number" {
				continue
			}
			for _, partExpr := range spec.PartitionBy {
				if exprContainsColName(partExpr, colName) {
					return true
				}
			}
		}
	}
	return false
}

func queryContainsStringLiteral(query *Query, value string) bool {
	return queryContainsExpr(query, func(expr *plan.Expr) bool {
		return exprContainsStringLiteral(expr, value)
	})
}

func queryContainsDefaultVal(query *Query) bool {
	return queryContainsExpr(query, exprContainsDefaultVal)
}

func queryContainsExpr(query *Query, accept func(*plan.Expr) bool) bool {
	for _, node := range query.Nodes {
		exprLists := [][]*plan.Expr{
			node.ProjectList,
			node.OnList,
			node.FilterList,
			node.GroupBy,
			node.AggList,
			node.WinSpecList,
		}
		for _, exprList := range exprLists {
			for _, expr := range exprList {
				if accept(expr) {
					return true
				}
			}
		}
		for _, order := range node.OrderBy {
			if accept(order.Expr) {
				return true
			}
		}
	}
	return false
}

func exprContainsFuncName(expr *plan.Expr, name string) bool {
	switch e := expr.Expr.(type) {
	case *plan.Expr_F:
		if e.F.Func != nil && e.F.Func.ObjName == name {
			return true
		}
		for _, arg := range e.F.Args {
			if exprContainsFuncName(arg, name) {
				return true
			}
		}
	case *plan.Expr_List:
		for _, item := range e.List.List {
			if exprContainsFuncName(item, name) {
				return true
			}
		}
	}
	return false
}

func exprContainsStringLiteral(expr *plan.Expr, value string) bool {
	switch e := expr.Expr.(type) {
	case *plan.Expr_Lit:
		if sval, ok := e.Lit.Value.(*plan.Literal_Sval); ok {
			return sval.Sval == value
		}
	case *plan.Expr_F:
		for _, arg := range e.F.Args {
			if exprContainsStringLiteral(arg, value) {
				return true
			}
		}
	case *plan.Expr_List:
		for _, item := range e.List.List {
			if exprContainsStringLiteral(item, value) {
				return true
			}
		}
	case *plan.Expr_W:
		if exprContainsStringLiteral(e.W.WindowFunc, value) {
			return true
		}
		for _, partition := range e.W.PartitionBy {
			if exprContainsStringLiteral(partition, value) {
				return true
			}
		}
		for _, order := range e.W.OrderBy {
			if exprContainsStringLiteral(order.Expr, value) {
				return true
			}
		}
	}
	return false
}

func exprContainsDefaultVal(expr *plan.Expr) bool {
	switch e := expr.Expr.(type) {
	case *plan.Expr_Lit:
		_, ok := e.Lit.Value.(*plan.Literal_Defaultval)
		return ok
	case *plan.Expr_F:
		for _, arg := range e.F.Args {
			if exprContainsDefaultVal(arg) {
				return true
			}
		}
	case *plan.Expr_List:
		for _, item := range e.List.List {
			if exprContainsDefaultVal(item) {
				return true
			}
		}
	case *plan.Expr_W:
		if exprContainsDefaultVal(e.W.WindowFunc) {
			return true
		}
		for _, partition := range e.W.PartitionBy {
			if exprContainsDefaultVal(partition) {
				return true
			}
		}
		for _, order := range e.W.OrderBy {
			if exprContainsDefaultVal(order.Expr) {
				return true
			}
		}
	}
	return false
}

func exprContainsColName(expr *plan.Expr, name string) bool {
	switch e := expr.Expr.(type) {
	case *plan.Expr_Col:
		return e.Col.Name == name || strings.HasSuffix(e.Col.Name, "."+name)
	case *plan.Expr_F:
		for _, arg := range e.F.Args {
			if exprContainsColName(arg, name) {
				return true
			}
		}
	case *plan.Expr_List:
		for _, item := range e.List.List {
			if exprContainsColName(item, name) {
				return true
			}
		}
	}
	return false
}

func TestDelete(t *testing.T) {
	mock := NewMockOptimizer(true)
	// should pass
	sqls := []string{
		"DELETE FROM NATION",
		"DELETE FROM NATION WHERE N_NATIONKEY > 10",
		"DELETE FROM NATION WHERE N_NATIONKEY > 10 LIMIT 20",
		"delete nation from nation left join nation2 on nation.n_nationkey = nation2.n_nationkey",
		"delete from nation",
		"delete nation, nation2 from nation join nation2 on nation.n_name = nation2.n_name",
		"prepare stmt1 from 'delete from nation where n_nationkey > ?'",
	}
	runTestShouldPass(mock, t, sqls, false, false)

	// should error
	sqls = []string{
		"DELETE FROM NATION2222",                     // table not exist
		"DELETE FROM NATION WHERE N_NATIONKEY2 > 10", // column not found
	}
	runTestShouldError(mock, t, sqls)
}

func TestReplacePKTable(t *testing.T) {
	mock := NewMockOptimizer(true)
	// REPLACE on tables with real primary keys should pass
	sqls := []string{
		"REPLACE INTO dept VALUES (1, 'Sales', 'New York')",
		"REPLACE INTO dept (deptno, dname, loc) VALUES (2, 'HR', 'London')",
		"REPLACE INTO dept SET deptno = 3, dname = 'Eng', loc = 'SF'",
		"REPLACE INTO dept VALUES (1, 'Sales', 'NY'), (2, 'HR', 'LA')",
	}
	runTestShouldPass(mock, t, sqls, false, false)

	// should error
	sqls = []string{
		"REPLACE INTO nonexistent VALUES (1, 'a')",         // table not exist
		"REPLACE INTO dept (deptno, badcol) VALUES (1, 2)", // column not exist
	}
	runTestShouldError(mock, t, sqls)
}

func TestReplaceRewritesLegacyGeneratedColumnCast(t *testing.T) {
	mock := NewMockOptimizer(true)
	tableDef := mock.ctxt.tables["dept"]
	require.NotNil(t, tableDef)

	var source, generated *plan.ColDef
	var sourcePos int32
	for i, col := range tableDef.Cols {
		switch strings.ToLower(col.Name) {
		case "deptno":
			source = col
			sourcePos = int32(i)
		case "dname":
			generated = col
		}
	}
	require.NotNil(t, source)
	require.NotNil(t, generated)
	sourceExpr := &plan.Expr{
		Typ: source.Typ,
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				RelPos: 0,
				ColPos: sourcePos,
				Name:   source.Name,
			},
		},
	}
	legacyExpr, err := forceCastExprWithName(t.Context(), sourceExpr, generated.Typ, "cast_strict")
	require.NoError(t, err)
	generated.GeneratedCol = &plan.GeneratedCol{Expr: legacyExpr, IsStored: true}

	stmt, err := mysql.ParseOne(t.Context(), "REPLACE INTO dept (deptno, loc) VALUES (1, 'NY')", 1)
	require.NoError(t, err)
	built, err := mock.Optimize(stmt)
	require.NoError(t, err)
	foundGeneratedAssignment := false
	for _, node := range built.Nodes {
		for _, expr := range node.ProjectList {
			f := expr.GetF()
			if f != nil &&
				f.GetFunc().GetObjName() == "cast_assign" &&
				expr.Typ.Width == generated.Typ.Width &&
				len(f.Args) > 0 &&
				f.Args[0].Typ.Id == source.Typ.Id {
				foundGeneratedAssignment = true
			}
		}
	}
	require.True(t, foundGeneratedAssignment)
}

func TestAssignmentCastRollingUpgradePlanGate(t *testing.T) {
	proc := testutil.NewProc(nil)
	rt := moruntime.ServiceRuntime(proc.GetService())
	defer rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)

	build := func(version int64) string {
		rt.SetGlobalVariables(moruntime.MOProtocolVersion, version)
		mock := NewMockOptimizer(true)
		stmt, err := mysql.ParseOne(
			t.Context(),
			"INSERT INTO dept (deptno, dname, loc) SELECT 1, 'Sales', 'NY'",
			1,
		)
		require.NoError(t, err)
		built, err := mock.Optimize(stmt)
		require.NoError(t, err)
		data, err := json.Marshal(built)
		require.NoError(t, err)
		return string(data)
	}

	mixedVersionPlan := build(defines.MORPCVersion4)
	require.Contains(t, mixedVersionPlan, `"obj_name":"cast_strict"`)
	require.NotContains(t, mixedVersionPlan, `"obj_name":"cast_assign"`)
	require.NotContains(t, mixedVersionPlan, `"obj_name":"cast_ignore"`)

	upgradedPlan := build(defines.MORPCVersion5)
	require.Contains(t, upgradedPlan, `"obj_name":"cast_assign"`)
}

func addPositiveCheck(t *testing.T, mock *MockOptimizer, tableName, columnName string) {
	t.Helper()
	tableDef := mock.ctxt.tables[tableName]
	colPos := tableDef.Name2ColIndex[columnName]
	checkExpr, err := BindFuncExprImplByPlanExpr(
		t.Context(),
		">",
		[]*plan.Expr{
			{Typ: tableDef.Cols[colPos].Typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 0, ColPos: colPos}}},
			MakePlan2Int64ConstExprWithType(0),
		},
	)
	require.NoError(t, err)
	tableDef.Checks = []*plan.CheckDef{{Name: "positive_check", Check: checkExpr}}
}

func TestInsertAddsCheckConstraintFilter(t *testing.T) {

	build := func(sql string) *plan.Query {
		mock := NewMockOptimizer(true)
		addPositiveCheck(t, mock, "dept", "deptno")

		stmt, err := mysql.ParseOne(t.Context(), sql, 1)
		require.NoError(t, err)
		built, err := mock.Optimize(stmt)
		require.NoError(t, err)
		return built
	}

	t.Run("regular insert asserts", func(t *testing.T) {
		query := build("insert into dept values (1, 'Sales', 'NY')")
		found := false
		for _, node := range query.Nodes {
			if node.NodeType != plan.Node_ASSERT {
				continue
			}
			for _, expr := range node.FilterList {
				if expr.GetF() != nil &&
					expr.GetF().GetFunc().GetObjName() == "_check_constraint_assert" {
					found = true
				}
			}
		}
		require.True(t, found)
	})

	t.Run("replace rejects mixed-version cluster", func(t *testing.T) {
		mock := NewMockOptimizer(true)
		addPositiveCheck(t, mock, "dept", "deptno")
		proc := testutil.NewProc(nil)
		rt := moruntime.ServiceRuntime(proc.GetService())
		defer rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
		rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion6)
		mock.ctxt.GetProcessFunc = func() *process.Process { return proc }

		stmt, err := mysql.ParseOne(
			t.Context(),
			"replace into dept values (1, 'Sales', 'NY')",
			1,
		)
		require.NoError(t, err)
		_, err = mock.Optimize(stmt)
		require.ErrorContains(t, err, "CHECK constraints require all CNs to support protocol version 7")
	})

	t.Run("insert ignore filters invalid rows", func(t *testing.T) {
		query := build("insert ignore into dept values (1, 'Sales', 'NY')")
		found := false
		for _, node := range query.Nodes {
			if node.NodeType != plan.Node_FILTER {
				continue
			}
			for _, expr := range node.FilterList {
				if expr.GetF() != nil && expr.GetF().GetFunc().GetObjName() == "coalesce" {
					found = true
				}
			}
		}
		require.True(t, found)
	})

	for _, sql := range []string{
		"replace into dept values (1, 'Sales', 'NY')",
		"replace into dept set deptno = 1, dname = 'Sales', loc = 'NY'",
	} {
		t.Run(sql, func(t *testing.T) {
			query := build(sql)
			found := false
			for _, node := range query.Nodes {
				if node.NodeType != plan.Node_ASSERT {
					continue
				}
				for _, expr := range node.FilterList {
					if expr.GetF() != nil &&
						expr.GetF().GetFunc().GetObjName() == "_check_constraint_assert" {
						found = true
					}
				}
			}
			require.True(t, found)
		})
	}

	t.Run("ODKU without unique key asserts on legacy fallback", func(t *testing.T) {
		mock := NewMockOptimizer(true)
		tableDef := mock.ctxt.tables["fake_pk_t"]
		tableDef.Indexes = nil
		colPos := tableDef.Name2ColIndex["a"]
		colExpr := &plan.Expr{
			Typ: tableDef.Cols[colPos].Typ,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{RelPos: 0, ColPos: colPos},
			},
		}
		checkExpr, err := BindFuncExprImplByPlanExpr(
			t.Context(),
			">",
			[]*plan.Expr{colExpr, MakePlan2Int64ConstExprWithType(0)},
		)
		require.NoError(t, err)
		tableDef.Checks = []*plan.CheckDef{{
			Name:  "fake_pk_t_chk_1",
			Check: checkExpr,
		}}

		stmt, err := mysql.ParseOne(
			t.Context(),
			"insert into fake_pk_t(a, b) values (-1, 'x') on duplicate key update b = 'y'",
			1,
		)
		require.NoError(t, err)
		query, err := mock.Optimize(stmt)
		require.NoError(t, err)

		found := false
		for _, node := range query.Nodes {
			if node.NodeType != plan.Node_ASSERT {
				continue
			}
			for _, expr := range node.FilterList {
				if expr.GetF() != nil &&
					expr.GetF().GetFunc().GetObjName() == "_check_constraint_assert" {
					found = true
				}
			}
		}
		require.True(t, found)
	})
}

func TestInsertIgnoreCheckCompositeUniqueNeedsLockKeyProjection(t *testing.T) {
	tableDef := &plan.TableDef{Indexes: []*plan.IndexDef{
		{Unique: true, Parts: []string{"a"}},
		{Unique: true, Parts: []string{"a", "b"}},
	}}

	needsProjection, err := hasMaterializedInsertUniqueLockKey(tableDef, []bool{false, false})
	require.NoError(t, err)
	require.True(t, needsProjection)

	needsProjection, err = hasMaterializedInsertUniqueLockKey(tableDef, []bool{false, true})
	require.NoError(t, err)
	require.False(t, needsProjection)

	_, err = hasMaterializedInsertUniqueLockKey(&plan.TableDef{Indexes: []*plan.IndexDef{{
		Unique:          true,
		Parts:           []string{"a", "b"},
		IndexAlgoParams: "not-json",
	}}}, []bool{false})
	require.Error(t, err)
}

func TestInsertIgnoreCheckCompositeUniqueBuildsPlan(t *testing.T) {
	mock := NewMockOptimizer(true)
	tableDef := mock.ctxt.tables["dept_composite_uk"]
	addPositiveCheck(t, mock, tableDef.Name, "deptno")

	stmt, err := mysql.ParseOne(
		t.Context(),
		"insert ignore into dept_composite_uk values (1, 'Sales', 'NY')",
		1,
	)
	require.NoError(t, err)
	query, err := mock.Optimize(stmt)
	require.NoError(t, err)

	foundCheckFilter := false
	for _, node := range query.Nodes {
		if node.NodeType != plan.Node_FILTER {
			continue
		}
		for _, expr := range node.FilterList {
			foundCheckFilter = foundCheckFilter || exprContainsFuncName(expr, "coalesce")
		}
	}
	require.True(t, foundCheckFilter)
}

func TestReplaceSetColRefAsDefault(t *testing.T) {
	mock := NewMockOptimizer(true)
	// REPLACE ... SET col = <expr referencing columns> must bind the RHS column
	// references as DEFAULT(col) instead of failing with
	// "ambiguous column reference". The exact computed values are covered by BVT.
	sqls := []string{
		// reference the assigned column itself: deptno = DEFAULT(deptno) + 1
		"REPLACE INTO dept SET deptno = deptno + 1, dname = 'Eng'",
		// reference another column: loc = DEFAULT(dname)
		"REPLACE INTO dept SET deptno = 1, loc = dname",
		// reference the assigned column directly: dname = DEFAULT(dname)
		"REPLACE INTO dept SET deptno = 1, dname = dname",
	}
	runTestShouldPass(mock, t, sqls, false, false)

	// An RHS reference to a non-existent column must still error.
	// A qualified RHS reference must NOT be silently resolved to DEFAULT(col) of
	// the destination column; it must error rather than dropping the qualifier.
	sqls = []string{
		"REPLACE INTO dept SET deptno = 1, dname = nosuchcol",
		"REPLACE INTO dept SET deptno = 1, dname = other.dname",
		"REPLACE INTO dept SET deptno = 1, dname = dept.dname",
	}
	runTestShouldError(mock, t, sqls)
}

func TestReplaceSetFunctionColRefAsDefault(t *testing.T) {
	mock := NewMockOptimizer(true)
	runTestShouldPass(mock, t, []string{
		"REPLACE INTO dept SET deptno = 1, dname = upper(dname)",
	}, false, false)
}

func TestReplaceFakePKTable(t *testing.T) {
	mock := NewMockOptimizer(true)
	// REPLACE on table with only unique key (fake PK) should pass
	sqls := []string{
		"REPLACE INTO fake_pk_t VALUES (1, 'hello')",
		"REPLACE INTO fake_pk_t (a, b) VALUES (2, 'world')",
		"REPLACE INTO fake_pk_t SET a = 3, b = 'test'",
	}
	runTestShouldPass(mock, t, sqls, false, false)
}

func TestReplaceFakePKCompositeNullableUKSkipsNullKeyIndex(t *testing.T) {
	mock := NewMockOptimizer(true)
	idxTbl := catalog.UniqueIndexTableNamePrefix + "fake-pk-comp-uk-ab"

	// touchesIdx reports whether the REPLACE plan reads or maintains the uk_ab index
	// table (a TABLE_SCAN on it, or a MULTI_UPDATE UpdateCtx targeting it).
	touchesIdx := func(sql string) bool {
		logicPlan, err := runOneStmt(mock, t, sql)
		if err != nil {
			t.Fatalf("%s: %+v", sql, err)
		}
		query := logicPlan.GetQuery()
		for _, node := range query.Nodes {
			if node.NodeType == plan.Node_TABLE_SCAN && node.TableDef != nil && node.TableDef.Name == idxTbl {
				return true
			}
			if node.NodeType == plan.Node_MULTI_UPDATE {
				for _, uc := range node.UpdateCtxList {
					if uc.TableDef != nil && uc.TableDef.Name == idxTbl {
						return true
					}
				}
			}
		}
		return false
	}

	// fake_pk_comp has a composite UNIQUE(a, b) and no real PK. Omitting column a makes
	// it default to NULL, so serial(a, b) is NULL: the unique key can never conflict and
	// is never stored. Like a plain INSERT (which skips index maintenance for a NULL
	// key), REPLACE must NOT read or maintain the uk_ab index table for this row.
	assert.False(t, touchesIdx("REPLACE INTO fake_pk_comp (b, c) VALUES (1, 'x')"),
		"REPLACE with a statically-NULL composite unique-key part must not maintain the unique index table")

	// With both key parts provided (non-NULL) the unique key can conflict, so REPLACE
	// must maintain the uk_ab index table as usual.
	assert.True(t, touchesIdx("REPLACE INTO fake_pk_comp (a, b, c) VALUES (1, 2, 'x')"),
		"REPLACE with a fully non-NULL composite unique key must maintain the unique index table")
}

func TestReplaceChildParentFKUsesInPlanCheck(t *testing.T) {
	mock := NewMockOptimizer(true)
	// emp has a child->parent foreign key (deptno references dept(deptno)). REPLACE
	// must enforce parent existence in-plan with the per-FK MARK-join assert the modern
	// INSERT path uses, not silently allow an orphan child row. emp has no
	// self-referencing FK, so DetectSqls must be empty.
	logicPlan, err := runOneStmt(mock, t,
		"REPLACE INTO emp VALUES (1, 'Alice', 'DEV', 0, '2020-01-01', 5000.00, 500.00, 1)")
	if err != nil {
		t.Fatalf("%+v", err)
	}
	query := logicPlan.GetQuery()
	hasMark := false
	for _, node := range query.Nodes {
		if node.NodeType == plan.Node_JOIN && node.JoinType == plan.Node_MARK {
			hasMark = true
		}
	}
	assert.True(t, hasMark, "REPLACE on a child FK table must enforce parent existence via an in-plan MARK join")
	assert.Empty(t, query.DetectSqls, "child->parent FK on REPLACE should be enforced in-plan, not via DetectSqls")
}

func TestReplaceFKTable(t *testing.T) {
	mock := NewMockOptimizer(true)
	// REPLACE on table with foreign key should pass (modern path)
	sqls := []string{
		"REPLACE INTO emp VALUES (1, 'Alice', 'DEV', 0, '2020-01-01', 5000.00, 500.00, 1)",
	}
	runTestShouldPass(mock, t, sqls, false, false)
}

func TestReplaceSelfRefFKTable(t *testing.T) {
	mock := NewMockOptimizer(true)
	// REPLACE on self-referencing FK table with RESTRICT should produce assert checks
	sqls := []string{
		"REPLACE INTO self_ref VALUES (1, NULL, 'root')",
		"REPLACE INTO self_ref (id, parent_id, name) VALUES (2, 1, 'child')",
	}
	runTestShouldPass(mock, t, sqls, false, false)
}

func TestReplaceSelfRefFKCascade(t *testing.T) {
	mock := NewMockOptimizer(true)
	// REPLACE on self-referencing FK table with CASCADE should NOT produce assert checks
	sqls := []string{
		"REPLACE INTO self_ref_cascade VALUES (1, NULL)",
	}
	runTestShouldPass(mock, t, sqls, false, false)
}

func TestReplacePlanStructure(t *testing.T) {
	mock := NewMockOptimizer(true)

	// Test that REPLACE produces Query_INSERT statement type
	logicPlan, err := runOneStmt(mock, t, "REPLACE INTO dept VALUES (1, 'Sales', 'NY')")
	if err != nil {
		t.Fatalf("%+v", err)
	}

	query := logicPlan.GetQuery()
	assert.NotNil(t, query)
	assert.Equal(t, plan.Query_INSERT, query.StmtType)

	// Verify plan contains MULTI_UPDATE node
	hasMultiUpdate := false
	hasDedupJoin := false
	for _, node := range query.Nodes {
		if node.NodeType == plan.Node_MULTI_UPDATE {
			hasMultiUpdate = true
		}
		if node.NodeType == plan.Node_JOIN && node.JoinType == plan.Node_DEDUP {
			hasDedupJoin = true
		}
	}
	assert.True(t, hasMultiUpdate, "REPLACE plan should contain MULTI_UPDATE node")
	assert.True(t, hasDedupJoin, "REPLACE plan should contain DEDUP JOIN node")
}

func TestInsertOnDupFakePKUsesModernPath(t *testing.T) {
	mock := NewMockOptimizer(true)

	// fake_pk_t has no real PK, only unique key(a). ON DUPLICATE KEY UPDATE must
	// be planned on the modern DEDUP JOIN + MULTI_UPDATE path (using the unique
	// key for conflict detection), not fall back to the legacy
	// Node_ON_DUPLICATE_KEY operator.
	logicPlan, err := runOneStmt(mock, t,
		"INSERT INTO fake_pk_t VALUES (1, 'x') ON DUPLICATE KEY UPDATE b = 'y'")
	if err != nil {
		t.Fatalf("%+v", err)
	}

	query := logicPlan.GetQuery()
	assert.NotNil(t, query)

	hasMultiUpdate := false
	hasDedupJoin := false
	for _, node := range query.Nodes {
		switch {
		case node.NodeType == plan.Node_MULTI_UPDATE:
			hasMultiUpdate = true
		case node.NodeType == plan.Node_JOIN && node.JoinType == plan.Node_DEDUP:
			hasDedupJoin = true
		}
	}
	assert.True(t, hasMultiUpdate, "fake-PK ODKU plan should contain MULTI_UPDATE node")
	assert.True(t, hasDedupJoin, "fake-PK ODKU plan should contain DEDUP JOIN node")
}

func TestInsertOnDupFKUsesModernPath(t *testing.T) {
	mock := NewMockOptimizer(true)

	// emp has a foreign key (deptno) references dept(deptno). ON DUPLICATE KEY
	// UPDATE on an FK table must be planned on the modern MULTI_UPDATE path, not the
	// legacy Node_ON_DUPLICATE_KEY operator. The child→parent FK is enforced
	// row-scoped in-plan (see TestInsertOnDupChildParentFKUsesInPlanCheck), so emp —
	// which has no self-referencing FK — generates no DetectSqls.
	logicPlan, err := runOneStmt(mock, t,
		"INSERT INTO emp (empno, deptno) VALUES (1, 10) ON DUPLICATE KEY UPDATE comm = 100")
	if err != nil {
		t.Fatalf("%+v", err)
	}

	query := logicPlan.GetQuery()
	assert.NotNil(t, query)

	hasMultiUpdate := false
	for _, node := range query.Nodes {
		if node.NodeType == plan.Node_MULTI_UPDATE {
			hasMultiUpdate = true
		}
	}
	assert.True(t, hasMultiUpdate, "FK-table ODKU plan should contain MULTI_UPDATE node")
	assert.Empty(t, query.DetectSqls, "child→parent FK ODKU should enforce FK in-plan, not via DetectSqls")
}

func TestInsertChildParentFKUsesInPlanCheck(t *testing.T) {
	mock := NewMockOptimizer(true)

	// emp has a child→parent foreign key (deptno references dept(deptno)). A plain
	// INSERT must enforce it with the row-scoped in-plan assert (a FILTER over the
	// new-row image joined against the parent), NOT a whole-table DetectSql — the
	// latter would false-positive on rows inserted earlier under
	// FOREIGN_KEY_CHECKS=0. Since emp has no self-referencing FK, DetectSqls must be
	// empty.
	logicPlan, err := runOneStmt(mock, t, "INSERT INTO emp (empno, deptno) VALUES (1, 10)")
	if err != nil {
		t.Fatalf("%+v", err)
	}

	query := logicPlan.GetQuery()
	assert.NotNil(t, query)
	assert.Empty(t, query.DetectSqls,
		"plain INSERT with only a child→parent FK should enforce it in-plan, not via DetectSqls")

	hasFilter := false
	for _, node := range query.Nodes {
		if node.NodeType == plan.Node_FILTER && len(node.FilterList) > 0 {
			hasFilter = true
			break
		}
	}
	assert.True(t, hasFilter, "child→parent FK INSERT should contain an in-plan assert FILTER node")
}

func TestInsertOnDupChildParentFKUsesInPlanCheck(t *testing.T) {
	mock := NewMockOptimizer(true)

	// ON DUPLICATE KEY UPDATE on emp (deptno references dept) must enforce the
	// child→parent FK with a row-scoped in-plan assert over the final merged image,
	// NOT a whole-table DetectSql — the latter scales with table size and
	// false-positives on rows inserted earlier under FOREIGN_KEY_CHECKS=0. emp has
	// no self-referencing FK, so DetectSqls must be empty.
	logicPlan, err := runOneStmt(mock, t,
		"INSERT INTO emp (empno, deptno) VALUES (1, 10) ON DUPLICATE KEY UPDATE deptno = 20")
	if err != nil {
		t.Fatalf("%+v", err)
	}

	query := logicPlan.GetQuery()
	assert.NotNil(t, query)
	assert.Empty(t, query.DetectSqls,
		"ODKU with only a child→parent FK should enforce it in-plan, not via DetectSqls")

	hasFilter, hasMultiUpdate, hasMark := false, false, false
	for _, node := range query.Nodes {
		if node.NodeType == plan.Node_FILTER && len(node.FilterList) > 0 {
			hasFilter = true
		}
		if node.NodeType == plan.Node_MULTI_UPDATE {
			hasMultiUpdate = true
		}
		if node.NodeType == plan.Node_JOIN && node.JoinType == plan.Node_MARK {
			hasMark = true
		}
	}
	assert.True(t, hasFilter, "child→parent FK ODKU should contain an in-plan assert FILTER node")
	assert.True(t, hasMultiUpdate, "child→parent FK ODKU should stay on the modern MULTI_UPDATE path")
	assert.True(t, hasMark, "child→parent FK ODKU must use a per-FK MARK join (null-aware MATCH SIMPLE), not a global isnotnull pre-filter")
}

func TestInsertIgnoreChildParentFKDropsRows(t *testing.T) {
	mock := NewMockOptimizer(true)

	// INSERT IGNORE on emp (deptno references dept) must drop the rows whose parent
	// does not exist (MySQL row-skip), not assert. On the modern path that is a MARK
	// join against the parent (the existence check) plus a FILTER that keeps only the
	// matching rows, feeding the MULTI_UPDATE. emp has no self-referencing FK, so
	// DetectSqls must be empty.
	logicPlan, err := runOneStmt(mock, t, "INSERT IGNORE INTO emp (empno, deptno) VALUES (1, 10)")
	if err != nil {
		t.Fatalf("%+v", err)
	}

	query := logicPlan.GetQuery()
	assert.NotNil(t, query)
	assert.Empty(t, query.DetectSqls,
		"INSERT IGNORE with only a child→parent FK should enforce it in-plan, not via DetectSqls")

	hasParentJoin, hasFilter, hasMultiUpdate := false, false, false
	for _, node := range query.Nodes {
		// The parent-existence check is a MARK join (the optimizer may also rewrite
		// the underlying join shape), so accept MARK / SEMI / LEFT / RIGHT.
		if node.NodeType == plan.Node_JOIN &&
			(node.JoinType == plan.Node_MARK || node.JoinType == plan.Node_SEMI ||
				node.JoinType == plan.Node_LEFT || node.JoinType == plan.Node_RIGHT) {
			hasParentJoin = true
		}
		if node.NodeType == plan.Node_FILTER && len(node.FilterList) > 0 {
			hasFilter = true
		}
		if node.NodeType == plan.Node_MULTI_UPDATE {
			hasMultiUpdate = true
		}
	}
	assert.True(t, hasParentJoin, "INSERT IGNORE FK row-skip should outer-join the parent table")
	assert.True(t, hasFilter, "INSERT IGNORE FK row-skip should contain the parent-existence FILTER node")
	assert.True(t, hasMultiUpdate, "INSERT IGNORE FK should stay on the modern MULTI_UPDATE path")
}

func TestCheckConstraintWithChildForeignKey(t *testing.T) {
	var exprContainsFunction func(*plan.Expr, string) bool
	exprContainsFunction = func(expr *plan.Expr, name string) bool {
		fn := expr.GetF()
		if fn == nil {
			return false
		}
		if fn.GetFunc().GetObjName() == name {
			return true
		}
		for _, arg := range fn.Args {
			if exprContainsFunction(arg, name) {
				return true
			}
		}
		return false
	}

	build := func(sql string) *plan.Query {
		mock := NewMockOptimizer(true)
		tableDef := mock.ctxt.tables["emp"]
		colPos := tableDef.Name2ColIndex["deptno"]
		colExpr := &plan.Expr{
			Typ: tableDef.Cols[colPos].Typ,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{RelPos: 0, ColPos: colPos},
			},
		}
		checkExpr, err := BindFuncExprImplByPlanExpr(
			t.Context(),
			">",
			[]*plan.Expr{colExpr, MakePlan2Int64ConstExprWithType(0)},
		)
		require.NoError(t, err)
		tableDef.Checks = []*plan.CheckDef{{
			Name:  "positive_deptno",
			Check: checkExpr,
		}}

		logicPlan, err := runOneStmt(mock, t, sql)
		require.NoError(t, err)
		query := logicPlan.GetQuery()
		require.NotNil(t, query)
		return query
	}

	assertPlanShape := func(t *testing.T, query *plan.Query, nodeType plan.Node_NodeType, checkFunc string) int32 {
		t.Helper()
		hasCheck, hasParentJoin, hasMultiUpdate := false, false, false
		checkNodeID := int32(-1)
		for nodeID, node := range query.Nodes {
			if node.NodeType == nodeType {
				for _, expr := range node.FilterList {
					if exprContainsFunction(expr, checkFunc) {
						hasCheck = true
						checkNodeID = int32(nodeID)
						if nodeType == plan.Node_FILTER && checkFunc == "coalesce" {
							require.True(t, node.FilterIsBarrier,
								"IGNORE CHECK must remain above the final-row producer")
						}
						if nodeType == plan.Node_ASSERT {
							require.Len(t, node.Children, 1)
							require.LessOrEqual(t, len(node.ProjectList), len(query.Nodes[node.Children[0]].ProjectList),
								"ASSERT output projection must not retain stale pre-pruning child positions")
						}
					}
				}
			}
			if node.NodeType == plan.Node_JOIN && node.JoinType == plan.Node_MARK {
				hasParentJoin = true
			}
			if node.NodeType == plan.Node_MULTI_UPDATE {
				hasMultiUpdate = true
			}
		}
		require.True(t, hasCheck)
		require.True(t, hasParentJoin)
		require.True(t, hasMultiUpdate)
		return checkNodeID
	}

	t.Run("replace", func(t *testing.T) {
		query := build("REPLACE INTO emp (empno, deptno) VALUES (1, 10)")
		assertPlanShape(t, query, plan.Node_ASSERT, "_check_constraint_assert")
	})

	t.Run("insert ignore", func(t *testing.T) {
		query := build("INSERT IGNORE INTO emp (empno, deptno) VALUES (1, 10)")
		assertPlanShape(t, query, plan.Node_FILTER, "coalesce")
	})

	t.Run("update", func(t *testing.T) {
		query := build("UPDATE emp SET deptno = deptno + 1")
		assertPlanShape(t, query, plan.Node_ASSERT, "_check_constraint_assert")
	})

	t.Run("update ignore", func(t *testing.T) {
		query := build("UPDATE IGNORE emp SET deptno = 0")
		assertPlanShape(t, query, plan.Node_FILTER, "coalesce")
	})

	t.Run("joined update", func(t *testing.T) {
		query := build("UPDATE emp e JOIN dept d ON e.deptno = d.deptno SET e.deptno = e.deptno + 1")
		hasCheckAssert := false
		for _, node := range query.Nodes {
			if node.NodeType != plan.Node_ASSERT {
				continue
			}
			for _, expr := range node.FilterList {
				if expr.GetF() != nil && expr.GetF().GetFunc().GetObjName() == "_check_constraint_assert" {
					hasCheckAssert = true
				}
			}
		}
		require.True(t, hasCheckAssert,
			"joined UPDATE must validate each target's final row image")
	})

	t.Run("joined update does not validate read-only source", func(t *testing.T) {
		mock := NewMockOptimizer(true)
		addCheck := func(tableName, checkName, colName string) {
			tableDef := mock.ctxt.tables[tableName]
			colPos := tableDef.Name2ColIndex[colName]
			colExpr := &plan.Expr{
				Typ:  tableDef.Cols[colPos].Typ,
				Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 0, ColPos: colPos}},
			}
			checkExpr, err := BindFuncExprImplByPlanExpr(
				t.Context(), ">", []*plan.Expr{colExpr, MakePlan2Int64ConstExprWithType(0)})
			require.NoError(t, err)
			tableDef.Checks = []*plan.CheckDef{{Name: checkName, Check: checkExpr}}
		}
		addCheck("emp", "target_check", "deptno")
		addCheck("dept", "source_check", "deptno")

		logicPlan, err := runOneStmt(mock, t,
			"UPDATE emp e JOIN dept d ON e.deptno = d.deptno SET e.deptno = e.deptno + 1")
		require.NoError(t, err)
		assertNames := make([]string, 0, 2)
		for _, node := range logicPlan.GetQuery().Nodes {
			if node.NodeType != plan.Node_ASSERT {
				continue
			}
			for _, expr := range node.FilterList {
				if expr.GetF() == nil || expr.GetF().GetFunc().GetObjName() != "_check_constraint_assert" {
					continue
				}
				args := expr.GetF().Args
				if len(args) == 2 && args[1].GetLit() != nil {
					assertNames = append(assertNames, args[1].GetLit().GetSval())
				}
			}
		}
		require.Len(t, assertNames, 1)
		require.Contains(t, assertNames[0], "target_check")
		require.NotContains(t, assertNames[0], "source_check")
	})

	t.Run("nullable joined target check is guarded by eligibility", func(t *testing.T) {
		query := build("UPDATE dept d LEFT JOIN emp e ON e.deptno = d.deptno " +
			"SET e.deptno = e.deptno + 1")
		guarded := false
		for _, node := range query.Nodes {
			if node.NodeType != plan.Node_ASSERT {
				continue
			}
			for _, expr := range node.FilterList {
				if exprContainsFunction(expr, "_check_constraint_assert") &&
					exprContainsFunction(expr, "or") &&
					exprContainsFunction(expr, "isnotnull") {
					guarded = true
				}
			}
		}
		require.True(t, guarded,
			"CHECK must pass rows whose nullable update target has no Rowid")
	})

	t.Run("multi target check uses the complete selected candidate", func(t *testing.T) {
		query := build("UPDATE emp e JOIN dept d ON e.deptno = d.deptno " +
			"SET e.deptno = e.deptno + 1, d.loc = e.ename")
		guarded := false
		for _, node := range query.Nodes {
			if node.NodeType != plan.Node_ASSERT {
				continue
			}
			for _, expr := range node.FilterList {
				if exprContainsFunction(expr, "_check_constraint_assert") &&
					exprContainsFunction(expr, "or") &&
					exprContainsFunction(expr, "and") &&
					exprContainsFunction(expr, "=") {
					guarded = true
				}
			}
		}
		require.True(t, guarded,
			"CHECK eligibility must include both target active and row_number = 1")
	})

	t.Run("on duplicate key update", func(t *testing.T) {
		query := build("INSERT INTO emp (empno, deptno) VALUES (1, 10) ON DUPLICATE KEY UPDATE deptno = 0")
		assertNodeID := assertPlanShape(t, query, plan.Node_ASSERT, "_check_constraint_assert")

		var containsNode func(int32, int32) bool
		containsNode = func(rootID, wantedID int32) bool {
			if rootID == wantedID {
				return true
			}
			node := query.Nodes[rootID]
			for _, childID := range node.Children {
				if containsNode(childID, wantedID) {
					return true
				}
			}
			for _, sourceStep := range node.SourceStep {
				if containsNode(query.Steps[sourceStep], wantedID) {
					return true
				}
			}
			return false
		}
		require.Len(t, query.Nodes[assertNodeID].Children, 1)
		require.Equal(t, plan.Node_PROJECT, query.Nodes[query.Nodes[assertNodeID].Children[0]].NodeType,
			"ODKU CHECK must be attached directly to the final merged projection")
		hasDedupUpdateBelowAssert := false
		for nodeID, node := range query.Nodes {
			if node.NodeType == plan.Node_JOIN && node.JoinType == plan.Node_DEDUP &&
				node.OnDuplicateAction == plan.Node_UPDATE &&
				containsNode(query.Nodes[assertNodeID].Children[0], int32(nodeID)) {
				hasDedupUpdateBelowAssert = true
				break
			}
		}
		require.True(t, hasDedupUpdateBelowAssert,
			"CHECK assertion must remain above the DEDUP UPDATE final-row mutation")
	})
}

func TestUpdateWithoutCheckConstraintAddsNoAssert(t *testing.T) {
	mock := NewMockOptimizer(true)
	logicPlan, err := runOneStmt(mock, t, "UPDATE emp SET deptno = deptno + 1")
	require.NoError(t, err)
	for _, node := range logicPlan.GetQuery().Nodes {
		require.NotEqual(t, plan.Node_ASSERT, node.NodeType,
			"tables without CHECK constraints must not pay for an ASSERT operator")
	}
}

func TestInsertOnDupSelfReferFKUsesModernPath(t *testing.T) {
	mock := NewMockOptimizer(true)

	// self_ref has a self-referencing foreign key (parent_id references
	// self_ref(id)). ON DUPLICATE KEY UPDATE must be planned on the modern
	// MULTI_UPDATE path, and the self-referencing FK must be enforced via a
	// generated DetectSql produced by genSqlsForCheckFKSelfRefer, not by falling
	// back to the legacy Node_ON_DUPLICATE_KEY operator.
	logicPlan, err := runOneStmt(mock, t,
		"INSERT INTO self_ref (id, parent_id, name) VALUES (1, NULL, 'x') ON DUPLICATE KEY UPDATE name = 'y'")
	if err != nil {
		t.Fatalf("%+v", err)
	}

	query := logicPlan.GetQuery()
	assert.NotNil(t, query)

	hasMultiUpdate := false
	for _, node := range query.Nodes {
		if node.NodeType == plan.Node_MULTI_UPDATE {
			hasMultiUpdate = true
		}
	}
	assert.True(t, hasMultiUpdate, "self-refer FK ODKU plan should contain MULTI_UPDATE node")
	assert.NotEmpty(t, query.DetectSqls, "self-refer FK insert should generate FK constraint DetectSqls")
	for _, detectSQL := range query.DetectSqls {
		assert.Contains(t, detectSQL, ") as __mo_fk_check_source",
			"generated FK constraint SQL must alias its derived table")
	}
}

func TestInsertOnDupRealPKUniqueKeyConflictUpdates(t *testing.T) {
	mock := NewMockOptimizer(true)

	// dept has a real PK (deptno) and a unique key (dname). To align with MySQL,
	// a unique-key conflict on a real-PK table must trigger an UPDATE of the
	// conflicting row instead of raising a duplicate-entry error.
	//
	// The modern plan achieves this by resolving a single UPDATE target row up
	// front: target_pk = coalesce(pk-existence-probe, uk1_pri, uk2_pri, ...),
	// treating PRIMARY as the 0th index. The main DEDUP-update join then keys on
	// target_pk so a cross-row UK conflict lands on the existing row's UPDATE.
	// The per-UK FAIL dedup join is intentionally kept as in-batch protection
	// (two brand-new rows sharing a new UK value still error, avoiding a
	// duplicated unique-index entry).
	logicPlan, err := runOneStmt(mock, t,
		"INSERT INTO dept VALUES (1, 'Sales', 'NY') ON DUPLICATE KEY UPDATE loc = 'LA'")
	if err != nil {
		t.Fatalf("%+v", err)
	}

	query := logicPlan.GetQuery()
	assert.NotNil(t, query)

	hasMultiUpdate := false
	hasUpdateDedupJoin := false
	hasTargetPkResolve := false
	for _, node := range query.Nodes {
		if node.NodeType == plan.Node_MULTI_UPDATE {
			hasMultiUpdate = true
		}
		if node.NodeType == plan.Node_JOIN && node.JoinType == plan.Node_DEDUP &&
			node.OnDuplicateAction == plan.Node_UPDATE {
			hasUpdateDedupJoin = true
		}
		for _, expr := range node.ProjectList {
			if exprContainsFuncName(expr, "coalesce") {
				hasTargetPkResolve = true
			}
		}
	}
	assert.True(t, hasMultiUpdate, "real-PK ODKU plan should contain MULTI_UPDATE node")
	assert.True(t, hasUpdateDedupJoin,
		"real-PK ODKU plan should contain a DEDUP JOIN with OnDuplicateAction=UPDATE")
	assert.True(t, hasTargetPkResolve,
		"real-PK ODKU must resolve a coalesce(pk, uk...) target so unique-key "+
			"conflicts update the existing row (MySQL-aligned), not just dedup on PK")
}

func TestInsertOnDupRealPKCompositeUniqueKeyConflict(t *testing.T) {
	mock := NewMockOptimizer(true)

	// dept_ck has a real PK (deptno) and a composite unique key (dname, loc),
	// plus a free column note. The target_pk resolution must serialize the
	// composite unique-key value to probe its index table, so a composite
	// unique-key conflict also resolves into the UPDATE target (MySQL-aligned).
	logicPlan, err := runOneStmt(mock, t,
		"INSERT INTO dept_ck VALUES (1, 'Sales', 'NY', 'n') ON DUPLICATE KEY UPDATE note = 'x'")
	if err != nil {
		t.Fatalf("%+v", err)
	}

	query := logicPlan.GetQuery()
	assert.NotNil(t, query)

	hasMultiUpdate := false
	hasTargetPkResolve := false
	for _, node := range query.Nodes {
		if node.NodeType == plan.Node_MULTI_UPDATE {
			hasMultiUpdate = true
		}
		for _, expr := range node.ProjectList {
			if exprContainsFuncName(expr, "coalesce") {
				hasTargetPkResolve = true
			}
		}
	}
	assert.True(t, hasMultiUpdate, "composite-UK real-PK ODKU should contain MULTI_UPDATE node")
	assert.True(t, hasTargetPkResolve,
		"composite-UK real-PK ODKU should resolve a coalesce(pk, composite-uk) target")
}

// TestInsertOnDupIndexMetaTableUsesModernPath guards the regression where
// dropping the legacy ODKU operator broke ivfflat/hnsw/cagra/fulltext index
// creation: index maintenance upserts a version counter into the index metadata
// table via ON DUPLICATE KEY UPDATE. That table carries an algo-specific
// TableType ("metadata") and a secondary-index name, so it is neither
// SystemOrdinaryRel nor SystemIndexRel and canSkipDedup would skip dedup. The
// modern path must still handle this ODKU (build a MULTI_UPDATE with the
// dedup-update join) instead of rejecting it with "insert into vector/text
// index table".
func TestInsertOnDupIndexMetaTableUsesModernPath(t *testing.T) {
	mock := NewMockOptimizer(true)

	// Mirrors the internal SQL generated by handleIvfIndexMetaTable.
	logicPlan, err := runOneStmt(mock, t,
		"INSERT INTO `__mo_index_secondary_meta` (`__mo_index_key`, `__mo_index_val`) "+
			"VALUES ('version', '0') ON DUPLICATE KEY UPDATE "+
			"`__mo_index_val` = CAST( (CAST(`__mo_index_val` AS BIGINT) + 1) AS CHAR)")
	if err != nil {
		t.Fatalf("ODKU into index metadata table must be supported by the modern path: %+v", err)
	}

	query := logicPlan.GetQuery()
	assert.NotNil(t, query)

	hasMultiUpdate := false
	for _, node := range query.Nodes {
		if node.NodeType == plan.Node_MULTI_UPDATE {
			hasMultiUpdate = true
			break
		}
	}
	assert.True(t, hasMultiUpdate,
		"ODKU into index metadata table should build a MULTI_UPDATE node")
}

func TestReplaceNonUniqueSingleIndexDeleteUsesIndexRowID(t *testing.T) {
	mock := NewMockOptimizer(true)

	logicPlan, err := runOneStmt(mock, t,
		"REPLACE INTO single_idx_t VALUES (1, 100)")
	if err != nil {
		t.Fatalf("%+v", err)
	}
	query := logicPlan.GetQuery()
	assert.NotNil(t, query)

	var multiUpdate *plan.Node
	for _, node := range query.Nodes {
		if node.NodeType == plan.Node_MULTI_UPDATE {
			multiUpdate = node
			break
		}
	}
	if multiUpdate == nil {
		t.Fatal("REPLACE plan should contain MULTI_UPDATE node")
	}

	var idxUpdateCtx *plan.UpdateCtx
	for _, updateCtx := range multiUpdate.UpdateCtxList {
		if updateCtx.TableDef == nil {
			continue
		}
		if strings.HasPrefix(updateCtx.TableDef.Name, catalog.SecondaryIndexTableNamePrefix) {
			idxUpdateCtx = updateCtx
			break
		}
	}
	if idxUpdateCtx == nil {
		t.Fatal("REPLACE plan should contain UpdateCtx for the secondary index table")
	}
	if len(idxUpdateCtx.DeleteCols) < 2 {
		t.Fatal("secondary index UpdateCtx should contain delete columns")
	}
	if len(multiUpdate.Children) != 1 {
		t.Fatalf("MULTI_UPDATE should have one child, got %d", len(multiUpdate.Children))
	}

	oldRowIDDeleteCol := idxUpdateCtx.DeleteCols[0]
	oldIdxDeleteCol := idxUpdateCtx.DeleteCols[1]
	child := query.Nodes[multiUpdate.Children[0]]
	if oldRowIDDeleteCol.ColPos < 0 || int(oldRowIDDeleteCol.ColPos) >= len(child.ProjectList) {
		t.Fatalf("DeleteCols[0] ColPos %d out of child project range %d",
			oldRowIDDeleteCol.ColPos, len(child.ProjectList))
	}
	if oldIdxDeleteCol.ColPos < 0 || int(oldIdxDeleteCol.ColPos) >= len(child.ProjectList) {
		t.Fatalf("DeleteCols[1] ColPos %d out of child project range %d",
			oldIdxDeleteCol.ColPos, len(child.ProjectList))
	}
	wantRowIDName := idxUpdateCtx.TableDef.Name + "." + catalog.Row_ID
	wantIdxName := idxUpdateCtx.TableDef.Name + "." + catalog.IndexTableIndexColName
	assert.Equal(t, wantRowIDName, oldRowIDDeleteCol.Name,
		"DeleteCols[0] should read Row_ID from the secondary index table")
	assert.Equal(t, wantIdxName, oldIdxDeleteCol.Name,
		"DeleteCols[1] should read the secondary index key column")
	assert.Equal(t, int32(types.T_Rowid), child.ProjectList[oldRowIDDeleteCol.ColPos].Typ.Id,
		"DeleteCols[0] should point at a Row_ID vector in the MULTI_UPDATE input")
	assert.NotEqual(t, oldRowIDDeleteCol.ColPos, oldIdxDeleteCol.ColPos,
		"Row_ID and index key delete columns must not collapse to the same input column")
	assert.False(t, oldRowIDDeleteCol.RelPos == 0 && oldRowIDDeleteCol.ColPos == 0 &&
		oldRowIDDeleteCol.Name != wantRowIDName,
		"DeleteCols[0] must not fall back to a zero-value ColRef")
}

func findDedupBuildKeepLastFlags(query *plan.Query) []bool {
	flags := make([]bool, 0, len(query.Nodes))
	for _, node := range query.Nodes {
		if node.NodeType != plan.Node_JOIN || node.JoinType != plan.Node_DEDUP {
			continue
		}
		flags = append(flags, node.GetDedupJoinCtx().GetDedupBuildKeepLast())
	}
	return flags
}

func TestDedupBuildKeepLastOnlyForReplace(t *testing.T) {
	mock := NewMockOptimizer(true)

	replacePlan, err := runOneStmt(mock, t, "REPLACE INTO dept VALUES (1, 'Sales', 'NY')")
	if err != nil {
		t.Fatalf("%+v", err)
	}
	replaceFlags := findDedupBuildKeepLastFlags(replacePlan.GetQuery())
	assert.NotEmpty(t, replaceFlags, "REPLACE plan should contain DEDUP JOIN nodes")
	for _, flag := range replaceFlags {
		assert.True(t, flag, "REPLACE DEDUP JOIN should keep the last duplicate build row")
	}

	updatePlan, err := runOneStmt(mock, t, "update dept set deptno = '50' where loc = 'NEW YORK'")
	if err != nil {
		t.Fatalf("%+v", err)
	}
	updateFlags := findDedupBuildKeepLastFlags(updatePlan.GetQuery())
	assert.NotEmpty(t, updateFlags, "UPDATE plan should contain DEDUP JOIN nodes")
	for _, flag := range updateFlags {
		assert.False(t, flag, "UPDATE DEDUP JOIN must preserve duplicate-key failure")
	}
}

func TestReplaceSelfRefPlanStructure(t *testing.T) {
	mock := NewMockOptimizer(true)

	// Self-referencing FK with RESTRICT should build plan successfully
	// FK constraints are enforced via DetectSqls (post-execution), not in-plan asserts
	logicPlan, err := runOneStmt(mock, t, "REPLACE INTO self_ref VALUES (1, NULL, 'root')")
	if err != nil {
		t.Fatalf("%+v", err)
	}

	query := logicPlan.GetQuery()
	assert.NotNil(t, query)
	assert.Equal(t, plan.Query_INSERT, query.StmtType)

	hasMultiUpdate := false
	for _, node := range query.Nodes {
		if node.NodeType == plan.Node_MULTI_UPDATE {
			hasMultiUpdate = true
		}
	}
	assert.True(t, hasMultiUpdate, "self-ref FK REPLACE should contain MULTI_UPDATE node")
}

func TestDeleteSelfReferSetNull(t *testing.T) {
	mock := NewMockOptimizer(true)
	tableDef := mock.ctxt.tables["self_ref_cascade"]
	tableDef.Fkeys[0].OnDelete = plan.ForeignKeyDef_SET_NULL
	tableDef.Fkeys[0].OnUpdate = plan.ForeignKeyDef_SET_NULL

	logicPlan, err := runOneStmt(mock, t, "DELETE FROM self_ref_cascade WHERE id = 2")
	require.NoError(t, err)
	query := logicPlan.GetQuery()
	require.NotNil(t, query)
	assert.True(t, query.GetHasForeignKeyAction())
	assert.True(t, queryUpdatesTable(query, "self_ref_cascade"))
	requireQueryStepDependenciesAcyclic(t, query)
}

func TestDeleteSelfReferCascade(t *testing.T) {
	mock := NewMockOptimizer(true)

	logicPlan, err := runOneStmt(mock, t, "DELETE FROM self_ref_cascade WHERE id = 1")
	require.NoError(t, err)
	query := logicPlan.GetQuery()
	require.NotNil(t, query)
	assert.True(t, query.GetHasForeignKeyAction())
	assert.True(t, queryHasNodeType(query, plan.Node_RECURSIVE_CTE),
		"self-referencing DELETE CASCADE must recursively collect all descendants")
	assert.True(t, slices.ContainsFunc(query.Nodes, func(node *plan.Node) bool {
		return node.NodeType == plan.Node_SINK && node.ExtraOptions == materialized.CTESinkOption
	}), "recursive roots and post-fixpoint exclusion must share drain-safe materialized fanout")
	requireRecursiveCTESources(t, query)
}

func TestDeleteSelfReferCascadeAcrossForeignKeys(t *testing.T) {
	mock := NewMockOptimizer(true)

	logicPlan, err := runOneStmt(mock, t,
		"DELETE FROM self_ref_multi_cascade WHERE id = 1")
	require.NoError(t, err)
	query := logicPlan.GetQuery()
	require.NotNil(t, query)

	recursiveCTEs := 0
	hasMultiEdgeMatch := false
	for _, node := range query.Nodes {
		if node.NodeType == plan.Node_RECURSIVE_CTE {
			recursiveCTEs++
			assert.True(t, node.RecursiveUnionDistinct,
				"self-referencing cascade fixpoint must deduplicate cycles and converging paths")
		}
		if node.NodeType == plan.Node_JOIN && len(node.OnList) == 1 &&
			node.OnList[0].GetF() != nil && node.OnList[0].GetF().Func.ObjName == "or" {
			hasMultiEdgeMatch = true
		}
	}
	assert.Equal(t, 1, recursiveCTEs,
		"all self-referencing CASCADE edges must share one recursive fixpoint")
	assert.True(t, hasMultiEdgeMatch,
		"the recursive fixpoint must expand through either self-referencing FK edge")
	assert.True(t, slices.ContainsFunc(query.Nodes, func(node *plan.Node) bool {
		return node.NodeType == plan.Node_SINK && node.ExtraOptions == materialized.CTESinkOption
	}), "recursive roots and post-fixpoint exclusion must share drain-safe materialized fanout")
	requireRecursiveCTESources(t, query)
}

func TestUpdateSelfReferCascadeUsesModernPlan(t *testing.T) {
	for _, sql := range []string{
		"UPDATE self_ref_cascade SET id = 10 WHERE id = 1",
		"UPDATE self_ref_cascade SET id = id + 10 WHERE id IN (1, 2)",
	} {
		mock := NewMockOptimizer(true)
		mock.CurrentContext().GetProcess().Base.SessionInfo.CountUpdateChangedRows = true
		logicPlan, err := runOneStmt(mock, t, sql)
		require.NoError(t, err)
		query := logicPlan.GetQuery()
		require.NotNil(t, query)
		require.True(t, query.GetHasForeignKeyAction())
		require.Equal(t, 1, countUpdateFkPlanNodes(query, plan.Node_MULTI_UPDATE),
			"the root UPDATE and self-referencing CASCADE must share one physical writer")
		foundAffectedRowsSelector := false
		for _, node := range query.Nodes {
			if node.NodeType != plan.Node_MULTI_UPDATE {
				continue
			}
			for _, updateCtx := range node.UpdateCtxList {
				if updateCtx.TableDef != nil && updateCtx.TableDef.Name == "self_ref_cascade" {
					require.Len(t, updateCtx.AffectedRowsCols, 1,
						"self-cascade rows must not inflate SQL affected-row accounting")
					require.NotNil(t, updateCtx.ChangedRowsCol,
						"default UPDATE semantics must count only changed explicit roots")
					foundAffectedRowsSelector = true
				}
			}
		}
		require.True(t, foundAffectedRowsSelector)
		joinTypes := make([]plan.Node_JoinType, 0)
		for _, node := range query.Nodes {
			if node.NodeType == plan.Node_JOIN {
				joinTypes = append(joinTypes, node.JoinType)
			}
		}
		require.True(t,
			slices.Contains(joinTypes, plan.Node_LEFT) || slices.Contains(joinTypes, plan.Node_RIGHT),
			"root-to-root cascades must be folded into the statement-owned row image")
		require.True(t, slices.ContainsFunc(query.Nodes, func(node *plan.Node) bool {
			return node.NodeType == plan.Node_JOIN && node.JoinType == plan.Node_ANTI
		}), "the cascade branch must exclude statement-owned rows before the streams are merged")
		require.True(t, queryHasNodeType(query, plan.Node_UNION_ALL),
			"root and non-root cascade rows must feed the same MULTI_UPDATE stream")
		require.Equal(t, 0, countUpdateFkPlanNodes(query, plan.Node_PRE_INSERT_UK))
		require.Equal(t, 0, countUpdateFkPlanNodes(query, plan.Node_PRE_INSERT_SK))
		require.GreaterOrEqual(t, len(slices.DeleteFunc(slices.Clone(query.Nodes), func(node *plan.Node) bool {
			return node.NodeType != plan.Node_SINK || node.ExtraOptions != materialized.CTESinkOption
		})), 2, "both the root fold and the shared cascade transition source must be materialized")
		requireQueryStepDependenciesAcyclic(t, query)
	}
}

func requireQueryStepDependenciesAcyclic(t *testing.T, query *plan.Query) {
	t.Helper()
	state := make([]uint8, len(query.Steps))
	var visitStep func(int)
	visitStep = func(step int) {
		require.GreaterOrEqual(t, step, 0)
		require.Less(t, step, len(query.Steps))
		if state[step] == 1 {
			t.Fatalf("query step dependency cycle contains step %d", step)
		}
		if state[step] == 2 {
			return
		}
		state[step] = 1
		seenNodes := make(map[int32]struct{})
		var visitNode func(int32)
		visitNode = func(nodeID int32) {
			require.GreaterOrEqual(t, nodeID, int32(0))
			require.Less(t, int(nodeID), len(query.Nodes))
			if _, ok := seenNodes[nodeID]; ok {
				return
			}
			seenNodes[nodeID] = struct{}{}
			node := query.Nodes[nodeID]
			for _, sourceStep := range node.SourceStep {
				visitStep(int(sourceStep))
			}
			for _, childID := range node.Children {
				visitNode(childID)
			}
		}
		visitNode(query.Steps[step])
		state[step] = 2
	}
	for step := range query.Steps {
		visitStep(step)
	}
}

func requireRecursiveCTESources(t *testing.T, query *plan.Query) {
	t.Helper()
	for _, node := range query.Nodes {
		if node.NodeType != plan.Node_RECURSIVE_CTE {
			continue
		}
		assert.True(t, node.RecursiveUnionDistinct,
			"self-referencing cascade recursion must deduplicate rowids")
		require.GreaterOrEqual(t, len(node.SourceStep), 2)
		for sourceIdx, sourceStep := range node.SourceStep {
			require.GreaterOrEqual(t, sourceStep, int32(0))
			require.Less(t, int(sourceStep), len(query.Steps))
			sink := query.Nodes[query.Steps[sourceStep]]
			require.Equal(t, plan.Node_SINK, sink.NodeType)
			if sourceIdx == 0 {
				assert.False(t, sink.RecursiveCte, "recursive CTE anchor must use a non-recursive sink")
			} else {
				assert.True(t, sink.RecursiveCte, "recursive CTE member must use a recursive sink")
			}
		}
		return
	}
	t.Fatal("recursive CTE node not found")
}

func TestReplaceSelfRefCascade(t *testing.T) {
	mock := NewMockOptimizer(true)

	// Self-referencing FK with CASCADE should also build successfully
	logicPlan, err := runOneStmt(mock, t, "REPLACE INTO self_ref_cascade VALUES (1, NULL)")
	if err != nil {
		t.Fatalf("%+v", err)
	}

	query := logicPlan.GetQuery()
	assert.NotNil(t, query)
	assert.Equal(t, plan.Query_INSERT, query.StmtType)

	// CASCADE FK action must NOT generate a parent→child pre-check; the
	// cascading delete handles child rows.
	for _, sql := range query.DetectSqls {
		assert.False(t, strings.HasPrefix(sql, "REPLACE_PARENT_CHK:"),
			"CASCADE self-ref FK should NOT generate parent-child pre-check, got: %s", sql)
	}
	assert.True(t, queryDeletesTable(query, "self_ref_cascade"),
		"CASCADE self-ref FK must build a descendant delete branch")
	assert.True(t, queryHasNodeType(query, plan.Node_RECURSIVE_CTE),
		"CASCADE self-ref FK must recursively collect the full descendant chain")
	oldRowExclusions := 0
	for _, node := range query.Nodes {
		if node.NodeType == plan.Node_JOIN && node.JoinType == plan.Node_ANTI {
			oldRowExclusions++
		} else if node.NodeType == plan.Node_FILTER && node.FilterIsBarrier &&
			len(node.Children) == 1 && query.Nodes[node.Children[0]].JoinType == plan.Node_MARK {
			oldRowExclusions++
		}
	}
	assert.GreaterOrEqual(t, oldRowExclusions, 1,
		"the completed cascade fixpoint must exclude main REPLACE old rows")
	cascadeLocks := 0
	for _, node := range query.Nodes {
		if node.NodeType != plan.Node_LOCK_OP || len(node.Children) != 1 ||
			query.Nodes[node.Children[0]].NodeType != plan.Node_SINK_SCAN {
			continue
		}
		for _, target := range node.LockTargets {
			if target.TableId == mock.ctxt.tables["self_ref_cascade"].TblId &&
				target.Mode == lockpb.LockMode_Exclusive {
				cascadeLocks++
			}
		}
	}
	assert.GreaterOrEqual(t, cascadeLocks, 2,
		"root and recursively cascaded rows must each lock a materialized source")
	for nodeID, node := range query.Nodes {
		if node.NodeType != plan.Node_SINK_SCAN || len(node.SourceStep) == 0 {
			continue
		}
		sourceSink := query.Nodes[query.Steps[node.SourceStep[0]]]
		for _, expr := range node.ProjectList {
			col, ok := expr.Expr.(*plan.Expr_Col)
			if !ok {
				continue
			}
			require.Less(t, int(col.Col.ColPos), len(sourceSink.ProjectList),
				"sink scan %d column must be remapped to source step %d (sink %d)",
				nodeID, node.SourceStep[0], query.Steps[node.SourceStep[0]])
		}
	}
	for nodeID, node := range query.Nodes {
		if node.NodeType == plan.Node_LOCK_OP && len(node.Children) == 1 {
			require.Len(t, node.ProjectList, len(query.Nodes[node.Children[0]].ProjectList),
				"lock must preserve every physical column requested by the recursive sink")
		}
		if node.NodeType == plan.Node_SINK && len(node.Children) == 1 {
			childWidth := len(query.Nodes[node.Children[0]].ProjectList)
			require.Len(t, node.ProjectList, childWidth,
				"sink %d and child %d must expose the same physical row width",
				nodeID, node.Children[0])
			for _, expr := range node.ProjectList {
				col, ok := expr.Expr.(*plan.Expr_Col)
				if ok {
					require.Less(t, int(col.Col.ColPos), childWidth,
						"sink must not read beyond its child's output")
				}
			}
		}
	}
}

func TestReplaceDetectSqls(t *testing.T) {
	mock := NewMockOptimizer(true)

	// REPLACE on a RESTRICT self-ref FK table must generate a
	// REPLACE_PARENT_CHK: pre-check SQL that references both the FK column
	// and the referred PK column, embedding the user-supplied PK value.
	logicPlan, err := runOneStmt(mock, t, "REPLACE INTO self_ref VALUES (1, NULL, 'root')")
	if err != nil {
		t.Fatalf("%+v", err)
	}

	query := logicPlan.GetQuery()
	assert.NotNil(t, query)
	assert.True(t, query.GetHasForeignKeyAction(), "FK-sensitive REPLACE must not be cached")

	var preCheck string
	for _, sql := range query.DetectSqls {
		if strings.HasPrefix(sql, "REPLACE_PARENT_CHK:") {
			preCheck = strings.TrimPrefix(sql, "REPLACE_PARENT_CHK:")
			break
		}
	}
	assert.NotEmpty(t, preCheck,
		"RESTRICT self-ref FK REPLACE should generate a REPLACE_PARENT_CHK: pre-check SQL")
	assert.Contains(t, preCheck, "self_ref", "pre-check SQL should target self_ref table")
	assert.Contains(t, preCheck, "parent_id", "pre-check SQL should reference the FK column")
	assert.Contains(t, preCheck, "`id`", "pre-check SQL should reference the referred PK column")
	assert.Contains(t, preCheck, "(1)", "pre-check SQL should embed the supplied PK value")
}

func TestReplaceForeignKeyPlanRemainsSensitiveWhenChecksDisabled(t *testing.T) {
	mock := NewMockOptimizer(true)
	mock.ctxt.ResolveVariableFunc = func(name string, _, _ bool) (interface{}, error) {
		switch name {
		case "foreign_key_checks":
			return int64(0), nil
		case "sql_mode":
			return "", nil
		default:
			return nil, moerr.NewInternalError(context.Background(), "unexpected variable")
		}
	}
	logicPlan, err := runOneStmt(mock, t, "REPLACE INTO replace_fk_cp VALUES (1, 'new')")
	require.NoError(t, err)
	query := logicPlan.GetQuery()
	require.True(t, query.GetHasForeignKeyAction())
	require.Empty(t, query.GetDetectSqls())
}

func TestChildInsertSkipsForeignKeyLockBarrierInOptimisticMode(t *testing.T) {
	for _, tc := range []struct {
		name string
		sql  string
	}{
		{name: "insert", sql: "INSERT INTO replace_fk_c VALUES (10, 1), (11, 1)"},
		{name: "insert ignore", sql: "INSERT IGNORE INTO replace_fk_c VALUES (10, 1), (11, 1)"},
		{name: "on duplicate key update", sql: "INSERT INTO replace_fk_c VALUES (10, 1), (11, 1) ON DUPLICATE KEY UPDATE pid = VALUES(pid)"},
		{name: "replace", sql: "REPLACE INTO replace_fk_c VALUES (10, 1), (11, 1)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := NewMockOptimizer(true)
			setMockTxnMode(mock, txnpb.TxnMode_Optimistic)

			logicPlan, err := runOneStmt(mock, t, tc.sql)
			require.NoError(t, err)
			query := logicPlan.GetQuery()
			for _, node := range query.Nodes {
				for _, target := range node.LockTargets {
					assert.NotEqual(t, lockpb.LockMode_Shared, target.Mode,
						"optimistic child FK validation must not plan prerequisite shared locks")
				}
			}
			assert.Len(t, query.Steps, 1,
				"optimistic FK validation must remain in the streaming DML step")
		})
	}
	// The row count is deliberately much larger than the cases above. Plan shape
	// must remain one streaming step; only the VALUE_SCAN payload may grow.
	values := make([]string, 256)
	for i := range values {
		values[i] = fmt.Sprintf("(%d, 1)", i+100)
	}
	mock := NewMockOptimizer(true)
	setMockTxnMode(mock, txnpb.TxnMode_Optimistic)
	logicPlan, err := runOneStmt(mock, t, "INSERT INTO replace_fk_c VALUES "+strings.Join(values, ","))
	require.NoError(t, err)
	assert.Len(t, logicPlan.GetQuery().Steps, 1)
	for _, node := range logicPlan.GetQuery().Nodes {
		for _, target := range node.LockTargets {
			assert.NotEqual(t, lockpb.LockMode_Shared, target.Mode)
		}
	}
}

func TestChildInsertKeepsForeignKeyLockBarrierInPessimisticMode(t *testing.T) {
	mock := NewMockOptimizer(true)
	setMockTxnMode(mock, txnpb.TxnMode_Pessimistic)

	logicPlan, err := runOneStmt(mock, t, "INSERT INTO replace_fk_c VALUES (10, 1), (11, 1)")
	require.NoError(t, err)
	query := logicPlan.GetQuery()
	hasLock := false
	hasSinkScan := false
	for _, node := range query.Nodes {
		hasLock = hasLock || node.NodeType == plan.Node_LOCK_OP
		hasSinkScan = hasSinkScan || node.NodeType == plan.Node_SINK_SCAN
	}
	assert.True(t, hasLock)
	assert.True(t, hasSinkScan)
	assert.Greater(t, len(query.Steps), 1)
}

func TestDeepCopyQueryKeepsReplaceDetectionSQLIndependent(t *testing.T) {
	original := &plan.Query{DetectSqls: []string{
		"REPLACE_PARENT_LOCK:select 1 for update",
		"REPLACE_PARENT_CHK:select true",
	}}
	copied := DeepCopyQuery(original)
	require.Equal(t, original.DetectSqls, copied.DetectSqls)
	copied.DetectSqls[0] = "changed"
	assert.Equal(t, "REPLACE_PARENT_LOCK:select 1 for update", original.DetectSqls[0])
}

func TestReplaceDetectSqlsExplicitColumnsCaseInsensitive(t *testing.T) {
	mock := NewMockOptimizer(true)

	// User-supplied column names use mixed case; lookup must be
	// case-insensitive so the pre-check is still generated.
	logicPlan, err := runOneStmt(mock, t,
		"REPLACE INTO self_ref (ID, PARENT_ID, NAME) VALUES (1, NULL, 'root')")
	if err != nil {
		t.Fatalf("%+v", err)
	}

	query := logicPlan.GetQuery()
	assert.NotNil(t, query)

	hasPreCheck := false
	for _, sql := range query.DetectSqls {
		if strings.HasPrefix(sql, "REPLACE_PARENT_CHK:") {
			hasPreCheck = true
			break
		}
	}
	assert.True(t, hasPreCheck,
		"pre-check should be generated even when explicit columns use mixed case")
}

func TestReplaceDetectSqlsNonLiteralSkip(t *testing.T) {
	mock := NewMockOptimizer(true)

	// Function calls (rand(), uuid(), now(), ...) cannot be safely
	// embedded into the pre-check SQL because they would be
	// re-evaluated and produce a different value than what REPLACE
	// actually writes. The generator must skip pre-check generation in
	// that case rather than emit an unsafe SQL.
	logicPlan, err := runOneStmt(mock, t,
		"REPLACE INTO self_ref VALUES (rand(), NULL, 'r')")
	if err != nil {
		t.Fatalf("%+v", err)
	}

	query := logicPlan.GetQuery()
	assert.NotNil(t, query)

	for _, sql := range query.DetectSqls {
		assert.False(t, strings.HasPrefix(sql, "REPLACE_PARENT_CHK:"),
			"pre-check must NOT be generated for non-literal PK expressions, got: %s", sql)
	}
}

func TestReplaceDetectSqlsMultipleRows(t *testing.T) {
	mock := NewMockOptimizer(true)

	// Multi-row REPLACE: every row's referenced PK value must be
	// embedded into the same pre-check IN list.
	logicPlan, err := runOneStmt(mock, t,
		"REPLACE INTO self_ref VALUES (1, NULL, 'a'), (2, 1, 'b'), (3, 2, 'c')")
	if err != nil {
		t.Fatalf("%+v", err)
	}

	query := logicPlan.GetQuery()
	assert.NotNil(t, query)

	var preCheck string
	for _, sql := range query.DetectSqls {
		if strings.HasPrefix(sql, "REPLACE_PARENT_CHK:") {
			preCheck = strings.TrimPrefix(sql, "REPLACE_PARENT_CHK:")
			break
		}
	}
	assert.NotEmpty(t, preCheck,
		"multi-row RESTRICT self-ref REPLACE should generate a pre-check SQL")
	// All PK values must show up in the IN list.
	assert.Contains(t, preCheck, "1", "pre-check IN list should contain row 1's PK")
	assert.Contains(t, preCheck, "2", "pre-check IN list should contain row 2's PK")
	assert.Contains(t, preCheck, "3", "pre-check IN list should contain row 3's PK")
}

func assertReplaceParentPlanMarker(t *testing.T, query *plan.Query) {
	t.Helper()
	require.Contains(t, query.DetectSqls, "REPLACE_PARENT_PLAN:")
}

func queryHasNodeType(query *plan.Query, typ plan.Node_NodeType) bool {
	for _, node := range query.Nodes {
		if node.NodeType == typ {
			return true
		}
	}
	return false
}

func queryHasFKAssert(query *plan.Query) bool {
	for _, node := range query.Nodes {
		if node.NodeType == plan.Node_FILTER && node.IsEnd {
			return true
		}
		for _, expr := range node.ProjectList {
			if fn := expr.GetF(); fn != nil && fn.Func.ObjName == "assert" {
				return true
			}
		}
	}
	return false
}

func queryDeletesTable(query *plan.Query, table string) bool {
	for _, node := range query.Nodes {
		if node.NodeType == plan.Node_DELETE && node.DeleteCtx != nil &&
			node.DeleteCtx.TableDef != nil && node.DeleteCtx.TableDef.Name == table {
			return true
		}
	}
	return false
}

func queryUpdatesTable(query *plan.Query, table string) bool {
	for _, node := range query.Nodes {
		for _, updateCtx := range node.UpdateCtxList {
			if updateCtx.TableDef != nil && updateCtx.TableDef.Name == table {
				return true
			}
		}
		if node.NodeType == plan.Node_INSERT && node.InsertCtx != nil &&
			node.InsertCtx.TableDef != nil && node.InsertCtx.TableDef.Name == table {
			return true
		}
	}
	return false
}

func assertLockTargetTypesMatchInput(t *testing.T, query *plan.Query) {
	t.Helper()
	for _, node := range query.Nodes {
		if node.NodeType != plan.Node_LOCK_OP {
			continue
		}
		require.Len(t, node.Children, 1)
		input := query.Nodes[node.Children[0]]
		for _, target := range node.LockTargets {
			require.Less(t, int(target.PrimaryColIdxInBat), len(input.ProjectList))
			assert.Equal(t, target.PrimaryColTyp.Id, input.ProjectList[target.PrimaryColIdxInBat].Typ.Id)
		}
	}
}

func TestReplaceParentSideFKRestrict(t *testing.T) {
	mock := NewMockOptimizer(true)

	// REPLACE on a parent table whose PK is referenced by a child with
	// ON DELETE RESTRICT must generate a REPLACE_PARENT_CHK: pre-check SQL
	// against the child table (issue #24951, 3.2 RESTRICT case).
	logicPlan, err := runOneStmt(mock, t, "REPLACE INTO replace_fk_p VALUES (1, 'p1_new')")
	if err != nil {
		t.Fatalf("%+v", err)
	}

	query := logicPlan.GetQuery()
	assert.NotNil(t, query)
	assertReplaceParentPlanMarker(t, query)
	assert.True(t, queryHasNodeType(query, plan.Node_LOCK_OP))
	assertLockTargetTypesMatchInput(t, query)
	assert.True(t, queryHasFKAssert(query), "RESTRICT must assert that no child row references the locked old parent")
}

func TestReplaceParentSideFKCascade(t *testing.T) {
	mock := NewMockOptimizer(true)

	// REPLACE on a parent table whose PK is referenced by a child with
	// ON DELETE CASCADE must generate a REPLACE_PARENT_ACTION: delete SQL
	// against the child table (issue #24951, 3.2 CASCADE case).
	logicPlan, err := runOneStmt(mock, t, "REPLACE INTO replace_fk_cp VALUES (1, 'p1_new')")
	if err != nil {
		t.Fatalf("%+v", err)
	}

	query := logicPlan.GetQuery()
	assert.NotNil(t, query)
	assertReplaceParentPlanMarker(t, query)
	assert.True(t, queryHasNodeType(query, plan.Node_LOCK_OP))
	assert.True(t, queryDeletesTable(query, "replace_fk_cc"), "CASCADE must build a child delete branch")
}

func TestReplaceParentSideFKExplicitColumns(t *testing.T) {
	mock := NewMockOptimizer(true)

	// Explicit column list (mixed case) must still resolve the PK position and
	// generate the parent-side pre-check.
	logicPlan, err := runOneStmt(mock, t,
		"REPLACE INTO replace_fk_p (ID, V) VALUES (1, 'p1_new')")
	if err != nil {
		t.Fatalf("%+v", err)
	}

	query := logicPlan.GetQuery()
	assert.NotNil(t, query)

	assertReplaceParentPlanMarker(t, query)
	assert.True(t, queryHasFKAssert(query))
}

func TestReplaceParentSideFKNoAction(t *testing.T) {
	mock := NewMockOptimizer(true)

	// ON DELETE NO ACTION behaves like RESTRICT: it must generate a
	// REPLACE_PARENT_CHK: pre-check, not a CASCADE/SET NULL action.
	logicPlan, err := runOneStmt(mock, t, "REPLACE INTO replace_fk_np VALUES (1, 'p1_new')")
	if err != nil {
		t.Fatalf("%+v", err)
	}

	query := logicPlan.GetQuery()
	assert.NotNil(t, query)

	assertReplaceParentPlanMarker(t, query)
	assert.True(t, queryHasFKAssert(query))
}

func TestReplaceParentSideFKSetDefault(t *testing.T) {
	mock := NewMockOptimizer(true)

	logicPlan, err := runOneStmt(mock, t, "REPLACE INTO replace_fk_dp VALUES (1, 'p1_new')")
	if err != nil {
		t.Fatalf("%+v", err)
	}

	query := logicPlan.GetQuery()
	assert.NotNil(t, query)

	assertReplaceParentPlanMarker(t, query)
	assert.True(t, queryHasFKAssert(query))
}

func TestReplaceParentSideFKMultiRow(t *testing.T) {
	mock := NewMockOptimizer(true)

	// Multi-row REPLACE: every literal PK value must be embedded into the same
	// parent-side action IN list (issue #24951 data-integrity case).
	logicPlan, err := runOneStmt(mock, t,
		"REPLACE INTO replace_fk_cp VALUES (1, 'a'), (2, 'b')")
	if err != nil {
		t.Fatalf("%+v", err)
	}

	query := logicPlan.GetQuery()
	assert.NotNil(t, query)

	assertReplaceParentPlanMarker(t, query)
	assert.True(t, queryDeletesTable(query, "replace_fk_cc"))
}

func TestReplaceParentSideFKMixedLiteralRows(t *testing.T) {
	mock := NewMockOptimizer(true)

	// Mixed literal/function input is evaluated once by the main row-image plan.
	logicPlan, err := runOneStmt(mock, t,
		"REPLACE INTO replace_fk_cp VALUES (1, 'a'), (rand(), 'b')")
	require.NoError(t, err)
	assertReplaceParentPlanMarker(t, logicPlan.GetQuery())
}

func TestReplaceParentSideFKSetNull(t *testing.T) {
	mock := NewMockOptimizer(true)

	// REPLACE on a parent table whose PK is referenced by a child with
	// ON DELETE SET NULL must generate a REPLACE_PARENT_ACTION: update SQL
	// that nulls the child FK column.
	logicPlan, err := runOneStmt(mock, t, "REPLACE INTO replace_fk_sp VALUES (1, 'p1_new')")
	if err != nil {
		t.Fatalf("%+v", err)
	}

	query := logicPlan.GetQuery()
	assert.NotNil(t, query)

	assertReplaceParentPlanMarker(t, query)
	assert.True(t, queryUpdatesTable(query, "replace_fk_sc"), "SET NULL must build a child update branch")
}

func TestReplaceSelfReferSetNullExcludesMainOldRow(t *testing.T) {
	mock := NewMockOptimizer(true)
	tableDef := mock.ctxt.tables["self_ref_cascade"]
	tableDef.Fkeys[0].OnDelete = plan.ForeignKeyDef_SET_NULL
	tableDef.Fkeys[0].OnUpdate = plan.ForeignKeyDef_SET_NULL

	logicPlan, err := runOneStmt(mock, t, "REPLACE INTO self_ref_cascade VALUES (1, 1)")
	require.NoError(t, err)
	query := logicPlan.GetQuery()
	assert.True(t, queryUpdatesTable(query, "self_ref_cascade"))
	assert.True(t, slices.ContainsFunc(query.Nodes, func(node *plan.Node) bool {
		return node.NodeType == plan.Node_JOIN && node.JoinType == plan.Node_MARK
	}),
		"self-referencing SET NULL must anti-match the complete old-row set owned by the main REPLACE")
}

func TestReplaceCascadeWinsOverSetNullForSameChildRow(t *testing.T) {
	mock := NewMockOptimizer(true)
	child := DeepCopyTableDef(mock.ctxt.tables["replace_fk_sc"], true)
	mock.ctxt.tables["replace_fk_sc"] = child
	if child.Name2ColIndex == nil {
		child.Name2ColIndex = make(map[string]int32, len(child.Cols)+1)
		for i, col := range child.Cols {
			child.Name2ColIndex[col.Name] = int32(i)
		}
	}
	rowIDPos := child.Name2ColIndex[catalog.Row_ID]
	child.Cols = append(child.Cols, nil)
	copy(child.Cols[rowIDPos+1:], child.Cols[rowIDPos:])
	child.Cols[rowIDPos] = &plan.ColDef{
		Name: "cascade_pid", ColId: 10, Typ: plan.Type{Id: int32(types.T_int32), Width: 32},
	}
	for i, col := range child.Cols {
		child.Name2ColIndex[col.Name] = int32(i)
	}
	child.Fkeys = append(child.Fkeys, &plan.ForeignKeyDef{
		Name: "fk_replace_sc_cascade", Cols: []uint64{10}, ForeignTbl: 77005, ForeignCols: []uint64{0},
		OnDelete: plan.ForeignKeyDef_CASCADE, OnUpdate: plan.ForeignKeyDef_CASCADE,
	})

	logicPlan, err := runOneStmt(mock, t, "REPLACE INTO replace_fk_sp VALUES (1, 'p1_new')")
	require.NoError(t, err)
	query := logicPlan.GetQuery()
	assert.True(t, queryUpdatesTable(query, "replace_fk_sc"))
	assert.True(t, queryDeletesTable(query, "replace_fk_sc"))
	assert.True(t, slices.ContainsFunc(query.Nodes, func(node *plan.Node) bool {
		return node.NodeType == plan.Node_JOIN && node.JoinType == plan.Node_ANTI
	}),
		"SET NULL source must anti-join CASCADE-owned child rows")
}

func TestReplaceParentSideFKCombinesSetNullActions(t *testing.T) {
	mock := NewMockOptimizer(true)
	child := DeepCopyTableDef(mock.ctxt.tables["replace_fk_sc"], true)
	mock.ctxt.tables["replace_fk_sc"] = child
	if child.Name2ColIndex == nil {
		child.Name2ColIndex = make(map[string]int32)
		for i, col := range child.Cols {
			child.Name2ColIndex[col.Name] = int32(i)
		}
	}
	rowIDPos := len(child.Cols) - 1
	child.Cols = append(child.Cols, nil)
	copy(child.Cols[rowIDPos+1:], child.Cols[rowIDPos:])
	child.Cols[rowIDPos] = &plan.ColDef{
		Name: "pid2", ColId: 10, Typ: plan.Type{Id: int32(types.T_int32), Width: 32},
	}
	child.Name2ColIndex["pid2"] = int32(rowIDPos)
	child.Name2ColIndex[catalog.Row_ID] = int32(rowIDPos + 1)
	child.Fkeys = append(child.Fkeys, &plan.ForeignKeyDef{
		Name: "fk_replace_sc_2", Cols: []uint64{10}, ForeignTbl: 77005, ForeignCols: []uint64{0},
		OnDelete: plan.ForeignKeyDef_SET_NULL, OnUpdate: plan.ForeignKeyDef_SET_NULL,
	})

	logicPlan, err := runOneStmt(mock, t, "REPLACE INTO replace_fk_sp VALUES (1, 'p1_new')")
	require.NoError(t, err)
	query := logicPlan.GetQuery()
	updates := 0
	foundPhysicalRowGrouping := false
	for _, node := range query.Nodes {
		if node.NodeType == plan.Node_AGG {
			for _, groupExpr := range node.GroupBy {
				if groupExpr.Typ.Id == int32(types.T_Rowid) {
					foundPhysicalRowGrouping = true
				}
			}
		}
		if node.NodeType == plan.Node_INSERT && node.InsertCtx != nil &&
			node.InsertCtx.TableDef != nil && node.InsertCtx.TableDef.Name == "replace_fk_sc" {
			updates++
		}
	}
	assert.True(t, foundPhysicalRowGrouping,
		"combined SET NULL actions must group by Row_ID so physically distinct duplicate rows remain distinct")
	require.Equal(t, 1, updates,
		"all SET NULL columns for one child row must be emitted by one base-table update")
}

func TestReplaceRecursiveCascadeLocksReferencedUniqueIndexKey(t *testing.T) {
	mock := NewMockOptimizer(true)
	cascadeChild := DeepCopyTableDef(mock.ctxt.tables["replace_fk_cc"], true)
	mock.ctxt.tables["replace_fk_cc"] = cascadeChild
	rootObj := mock.ctxt.objects["replace_fk_cp"]

	if cascadeChild.Name2ColIndex == nil {
		cascadeChild.Name2ColIndex = make(map[string]int32, len(cascadeChild.Cols)+1)
	}
	rowIDPos := int32(-1)
	for i, col := range cascadeChild.Cols {
		cascadeChild.Name2ColIndex[col.Name] = int32(i)
		if col.Name == catalog.Row_ID {
			rowIDPos = int32(i)
		}
	}
	require.GreaterOrEqual(t, rowIDPos, int32(0))
	cascadeChild.Cols = append(cascadeChild.Cols, nil)
	copy(cascadeChild.Cols[rowIDPos+1:], cascadeChild.Cols[rowIDPos:])
	cascadeChild.Cols[rowIDPos] = &plan.ColDef{
		Name: "u", ColId: 10, Typ: plan.Type{Id: int32(types.T_varchar), Width: 20},
	}
	for i, col := range cascadeChild.Cols {
		cascadeChild.Name2ColIndex[col.Name] = int32(i)
	}
	const (
		indexTableID = uint64(77911)
		grandchildID = uint64(77912)
	)
	indexTableName := "__mo_index_replace_fk_cc_u"
	cascadeChild.Indexes = append(cascadeChild.Indexes, &plan.IndexDef{
		IndexName: "uk_u", IndexTableName: indexTableName, Parts: []string{"u"},
		Unique: true, TableExist: true, IndexAlgo: catalog.MoIndexDefaultAlgo.ToString(),
	})
	cascadeChild.RefChildTbls = []uint64{grandchildID}

	indexTable := &plan.TableDef{
		TblId: indexTableID, Name: indexTableName,
		Cols: []*plan.ColDef{
			{Name: catalog.IndexTableIndexColName, ColId: 0,
				Typ: plan.Type{Id: int32(types.T_varchar), Width: 20}},
			{Name: catalog.Row_ID, ColId: 1, Hidden: true,
				Typ: plan.Type{Id: int32(types.T_Rowid), Width: 16}},
		},
		Pkey: &plan.PrimaryKeyDef{Names: []string{catalog.IndexTableIndexColName},
			PkeyColName: catalog.IndexTableIndexColName},
		Name2ColIndex: map[string]int32{catalog.IndexTableIndexColName: 0, catalog.Row_ID: 1},
	}
	grandchild := &plan.TableDef{
		TblId: grandchildID, Name: "replace_fk_gc",
		Cols: []*plan.ColDef{
			{Name: "id", ColId: 0, Typ: plan.Type{Id: int32(types.T_int32), Width: 32}},
			{Name: "cu", ColId: 1, Typ: plan.Type{Id: int32(types.T_varchar), Width: 20}},
			{Name: catalog.Row_ID, ColId: 2, Hidden: true, Typ: plan.Type{Id: int32(types.T_Rowid), Width: 16}},
		},
		Pkey: &plan.PrimaryKeyDef{Names: []string{"id"}, PkeyColName: "id"},
		Fkeys: []*plan.ForeignKeyDef{{
			Name: "fk_replace_gc", Cols: []uint64{1}, ForeignTbl: cascadeChild.TblId,
			ForeignCols: []uint64{10}, OnDelete: plan.ForeignKeyDef_RESTRICT,
			OnUpdate: plan.ForeignKeyDef_RESTRICT,
		}},
		Name2ColIndex: map[string]int32{"id": 0, "cu": 1, catalog.Row_ID: 2},
	}
	registerTable := func(tableDef *plan.TableDef) {
		mock.ctxt.tables[tableDef.Name] = tableDef
		mock.ctxt.objects[tableDef.Name] = &plan.ObjectRef{
			Obj: int64(tableDef.TblId), SchemaName: rootObj.SchemaName, ObjName: tableDef.Name,
		}
		mock.ctxt.id2name[tableDef.TblId] = tableDef.Name
	}
	registerTable(indexTable)
	registerTable(grandchild)

	builder := NewQueryBuilder(plan.Query_DELETE, mock.CurrentContext(), false, true)
	bindCtx := NewBindContext(builder, nil)
	sourceTag := builder.genNewBindTag()
	sourceProject := make([]*plan.Expr, len(cascadeChild.Cols))
	for i, col := range cascadeChild.Cols {
		sourceProject[i] = &plan.Expr{Typ: col.Typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{
			RelPos: sourceTag, ColPos: int32(i), Name: col.Name,
		}}}
	}
	sourceNodeID := builder.appendNode(&plan.Node{
		NodeType: plan.Node_TABLE_SCAN, ObjRef: mock.ctxt.objects[cascadeChild.Name],
		TableDef: cascadeChild, ProjectList: sourceProject, BindingTags: []int32{sourceTag},
	}, bindCtx)
	delCtx := &dmlPlanCtx{
		objRef: mock.ctxt.objects[cascadeChild.Name], tableDef: cascadeChild, sourceTag: sourceTag,
	}
	outputNodeID, err := appendRecursiveCascadeLockNode(builder, bindCtx, delCtx, sourceNodeID)
	require.NoError(t, err)
	builder.appendStep(outputNodeID)
	query, err := builder.createQuery()
	require.NoError(t, err)
	foundBaseLock := false
	foundUniqueLock := false
	for _, node := range query.Nodes {
		if node.NodeType != plan.Node_LOCK_OP {
			continue
		}
		for _, target := range node.LockTargets {
			if target.Mode != lockpb.LockMode_Exclusive {
				continue
			}
			if target.TableId == cascadeChild.TblId {
				foundBaseLock = true
			}
			if target.TableId == indexTableID {
				foundUniqueLock = true
				require.Len(t, node.Children, 1)
				lockInput := query.Nodes[node.Children[0]]
				require.Less(t, int(target.PrimaryColIdxInBat), len(lockInput.ProjectList))
				assert.Equal(t, target.PrimaryColTyp.Id,
					lockInput.ProjectList[target.PrimaryColIdxInBat].Typ.Id)
			}
		}
	}
	assert.True(t, foundBaseLock, "recursive cascade must lock the current table primary key")
	assert.True(t, foundUniqueLock,
		"recursive cascade must lock the hidden UNIQUE namespace referenced by the grandchild")
}

func TestReplaceParentSideFKNonLiteralSkip(t *testing.T) {
	mock := NewMockOptimizer(true)

	// Non-literal expressions are evaluated by the main REPLACE row image.
	logicPlan, err := runOneStmt(mock, t, "REPLACE INTO replace_fk_p VALUES (rand(), 'x')")
	require.NoError(t, err)
	assertReplaceParentPlanMarker(t, logicPlan.GetQuery())
}

func TestReplaceParentSideFKUnsupportedSources(t *testing.T) {
	mock := NewMockOptimizer(true)
	preparedSQL := "REPLACE INTO replace_fk_p VALUES (?, 'x')"
	stmts, err := mysql.Parse(mock.CurrentContext().GetContext(), preparedSQL, 1)
	require.NoError(t, err)
	logicPlan, err := BuildPlan(mock.CurrentContext(), stmts[0], true)
	require.NoError(t, err)
	assertReplaceParentPlanMarker(t, logicPlan.GetQuery())

	selectSQL := "REPLACE INTO replace_fk_p SELECT deptno, dname FROM dept"
	logicPlan, err = runOneStmt(mock, t, selectSQL)
	require.NoError(t, err)
	assertReplaceParentPlanMarker(t, logicPlan.GetQuery())
}

func TestChildInsertLocksForeignKeyParentShared(t *testing.T) {
	mock := NewMockOptimizer(true)
	logicPlan, err := runOneStmt(mock, t, "INSERT INTO replace_fk_c VALUES (10, 1)")
	require.NoError(t, err)

	parentID := mock.ctxt.tables["replace_fk_p"].TblId
	query := logicPlan.GetQuery()
	found := false
	lockNodeID := int32(-1)
	parentScanIDs := make([]int32, 0, 1)
	for nodeID, node := range query.Nodes {
		if node.NodeType == plan.Node_TABLE_SCAN && node.TableDef != nil && node.TableDef.TblId == parentID {
			assert.Empty(t, node.LockTargets, "the raw parent scan must not carry a shared lock")
			parentScanIDs = append(parentScanIDs, int32(nodeID))
		}
		for _, target := range node.LockTargets {
			if target.TableId == parentID && target.Mode == lockpb.LockMode_Shared {
				found = true
				lockNodeID = int32(nodeID)
				assert.Equal(t, int32(0), target.PrimaryColRelPos)
				require.Len(t, node.Children, 1)
				lockInput := query.Nodes[node.Children[0]]
				require.Less(t, int(target.PrimaryColIdxInBat), len(lockInput.ProjectList))
				assert.Equal(t, target.PrimaryColTyp.Id, lockInput.ProjectList[target.PrimaryColIdxInBat].Typ.Id)
			}
		}
	}
	assert.True(t, found, "child FK validation must hold a shared lock on its parent row")
	require.NotEmpty(t, parentScanIDs)
	stepContaining := func(target int32) int {
		var contains func(int32) bool
		contains = func(nodeID int32) bool {
			if nodeID == target {
				return true
			}
			for _, childID := range query.Nodes[nodeID].Children {
				if contains(childID) {
					return true
				}
			}
			return false
		}
		for step, rootID := range query.Steps {
			if contains(rootID) {
				return step
			}
		}
		return -1
	}
	var stepDependsOn func(int, int, map[int]bool) bool
	stepDependsOn = func(step, dependency int, visited map[int]bool) bool {
		if step == dependency {
			return true
		}
		if visited[step] {
			return false
		}
		visited[step] = true
		var nodeDependsOn func(int32) bool
		nodeDependsOn = func(nodeID int32) bool {
			node := query.Nodes[nodeID]
			for _, sourceStep := range node.SourceStep {
				if stepDependsOn(int(sourceStep), dependency, visited) {
					return true
				}
			}
			for _, childID := range node.Children {
				if nodeDependsOn(childID) {
					return true
				}
			}
			return false
		}
		return nodeDependsOn(query.Steps[step])
	}
	lockStep := stepContaining(lockNodeID)
	require.GreaterOrEqual(t, lockStep, 0)
	assert.Equal(t, plan.Node_SINK, query.Nodes[query.Steps[lockStep]].NodeType,
		"a dependent SINK_SCAN must consume a materialized lock stage")
	for _, scanID := range parentScanIDs {
		parentStep := stepContaining(scanID)
		require.GreaterOrEqual(t, parentStep, 0)
		assert.True(t, stepDependsOn(parentStep, lockStep, make(map[int]bool)),
			"the parent scan must consume the referenced-key lock step output")
	}
}

func TestChildInsertLockKeyUsesParentDecimalType(t *testing.T) {
	mock := NewMockOptimizer(true)
	parent := mock.ctxt.tables["replace_fk_p"]
	child := mock.ctxt.tables["replace_fk_c"]
	parent.Cols[0].Typ = plan.Type{Id: int32(types.T_decimal64), Width: 5, Scale: 2}
	child.Cols[1].Typ = plan.Type{Id: int32(types.T_decimal64), Width: 5, Scale: 3}

	logicPlan, err := runOneStmt(mock, t, "INSERT INTO replace_fk_c VALUES (10, 1.230)")
	require.NoError(t, err)
	for _, node := range logicPlan.GetQuery().Nodes {
		for _, target := range node.LockTargets {
			if target.TableId != parent.TblId || target.Mode != lockpb.LockMode_Shared {
				continue
			}
			lockInput := logicPlan.GetQuery().Nodes[node.Children[0]]
			require.Less(t, int(target.PrimaryColIdxInBat), len(lockInput.ProjectList))
			lockKey := lockInput.ProjectList[target.PrimaryColIdxInBat]
			assert.Equal(t, int32(types.T_decimal64), lockKey.Typ.Id)
			assert.Equal(t, int32(2), lockKey.Typ.Scale)
			assert.Equal(t, target.PrimaryColTyp, lockKey.Typ)
			require.NotNil(t, lockKey.GetF())
			assert.Equal(t, "cast", lockKey.GetF().Func.ObjName)
			return
		}
	}
	t.Fatal("decimal parent shared lock not found")
}

func TestChildInsertChainsMultipleForeignKeyLocks(t *testing.T) {
	mock := NewMockOptimizer(true)
	child := mock.ctxt.tables["replace_fk_c"]
	fkCopy := *child.Fkeys[0]
	child.Fkeys = append(child.Fkeys, &fkCopy)

	logicPlan, err := runOneStmt(mock, t, "INSERT INTO replace_fk_c VALUES (10, 1)")
	require.NoError(t, err)
	query := logicPlan.GetQuery()
	lockIDs := make([]int32, 0, 2)
	parentID := mock.ctxt.tables["replace_fk_p"].TblId
	for nodeID, node := range query.Nodes {
		for _, target := range node.LockTargets {
			if target.TableId == parentID && target.Mode == lockpb.LockMode_Shared {
				lockIDs = append(lockIDs, int32(nodeID))
			}
		}
	}
	require.Len(t, lockIDs, 2)

	contains := func(root, target int32) bool {
		var visit func(int32) bool
		visit = func(nodeID int32) bool {
			if nodeID == target {
				return true
			}
			for _, childID := range query.Nodes[nodeID].Children {
				if visit(childID) {
					return true
				}
			}
			return false
		}
		return visit(root)
	}
	lockStepRoot := int32(-1)
	for _, stepRoot := range query.Steps {
		if contains(stepRoot, lockIDs[0]) && contains(stepRoot, lockIDs[1]) {
			lockStepRoot = stepRoot
			break
		}
	}
	require.NotEqual(t, int32(-1), lockStepRoot)
	assert.Equal(t, plan.Node_SINK, query.Nodes[lockStepRoot].NodeType)
	assert.True(t, contains(lockIDs[0], lockIDs[1]) || contains(lockIDs[1], lockIDs[0]),
		"foreign-key lock stages must form one serial data pipeline")
}

func TestChildInsertLocksCompositeParentPrimaryKey(t *testing.T) {
	mock := NewMockOptimizer(true)
	parent := mock.ctxt.tables["replace_fk_p"]
	child := mock.ctxt.tables["replace_fk_c"]
	parent.Cols = append(parent.Cols,
		&plan.ColDef{Name: "k", ColId: 3, Typ: plan.Type{Id: int32(types.T_int32), Width: 32}},
		&plan.ColDef{Name: catalog.CPrimaryKeyColName, ColId: 4, Hidden: true,
			Typ: plan.Type{Id: int32(types.T_varchar), Width: 65535}},
	)
	parent.Pkey = &plan.PrimaryKeyDef{Names: []string{"id", "k"}, PkeyColName: catalog.CPrimaryKeyColName}
	if parent.Name2ColIndex == nil {
		parent.Name2ColIndex = make(map[string]int32, len(parent.Cols))
		for i, col := range parent.Cols {
			parent.Name2ColIndex[col.Name] = int32(i)
		}
	}
	parent.Name2ColIndex["k"] = int32(len(parent.Cols) - 2)
	parent.Name2ColIndex[catalog.CPrimaryKeyColName] = int32(len(parent.Cols) - 1)
	child.Fkeys[0].Cols = []uint64{0, 1}
	child.Fkeys[0].ForeignCols = []uint64{0, 3}

	logicPlan, err := runOneStmt(mock, t, "INSERT INTO replace_fk_c VALUES (10, 1)")
	require.NoError(t, err)
	for _, node := range logicPlan.GetQuery().Nodes {
		for _, target := range node.LockTargets {
			if target.TableId != parent.TblId || target.Mode != lockpb.LockMode_Shared {
				continue
			}
			lockInput := logicPlan.GetQuery().Nodes[node.Children[0]]
			require.Less(t, int(target.PrimaryColIdxInBat), len(lockInput.ProjectList),
				"lock input=%+v target=%+v", lockInput, target)
			assert.Equal(t, target.PrimaryColTyp.Id, lockInput.ProjectList[target.PrimaryColIdxInBat].Typ.Id)
			assert.Equal(t, int32(types.T_varchar), target.PrimaryColTyp.Id)
			return
		}
	}
	t.Fatal("composite parent primary key shared lock not found")
}

func TestChildInsertLocksCompositeParentPrimaryKeyPrefixTable(t *testing.T) {
	mock := NewMockOptimizer(true)
	parent := mock.ctxt.tables["replace_fk_p"]
	parent.Cols = append(parent.Cols,
		&plan.ColDef{Name: "k", ColId: 3, Typ: plan.Type{Id: int32(types.T_int32), Width: 32}},
		&plan.ColDef{Name: catalog.CPrimaryKeyColName, ColId: 4, Hidden: true,
			Typ: plan.Type{Id: int32(types.T_varchar), Width: 65535}},
	)
	parent.Pkey = &plan.PrimaryKeyDef{Names: []string{"id", "k"}, PkeyColName: catalog.CPrimaryKeyColName}
	if parent.Name2ColIndex == nil {
		parent.Name2ColIndex = make(map[string]int32, len(parent.Cols))
		for i, col := range parent.Cols {
			parent.Name2ColIndex[col.Name] = int32(i)
		}
	}
	parent.Name2ColIndex["k"] = int32(len(parent.Cols) - 2)
	parent.Name2ColIndex[catalog.CPrimaryKeyColName] = int32(len(parent.Cols) - 1)

	logicPlan, err := runOneStmt(mock, t, "INSERT INTO replace_fk_c VALUES (10, 1)")
	require.NoError(t, err)
	query := logicPlan.GetQuery()
	stepContaining := func(target int32) int {
		var contains func(int32) bool
		contains = func(nodeID int32) bool {
			if nodeID == target {
				return true
			}
			for _, childID := range query.Nodes[nodeID].Children {
				if contains(childID) {
					return true
				}
			}
			return false
		}
		for step, rootID := range query.Steps {
			if contains(rootID) {
				return step
			}
		}
		return -1
	}
	foundParentScan := false
	for _, node := range query.Nodes {
		if node.NodeType == plan.Node_TABLE_SCAN && node.TableDef != nil && node.TableDef.TblId == parent.TblId {
			foundParentScan = true
		}
	}
	for nodeID, node := range query.Nodes {
		for _, target := range node.LockTargets {
			if target.TableId != parent.TblId || target.Mode != lockpb.LockMode_Shared {
				continue
			}
			assert.True(t, target.LockTable)
			lockStep := stepContaining(int32(nodeID))
			require.GreaterOrEqual(t, lockStep, 0)
			assert.Less(t, lockStep, len(query.Steps)-1)
			assert.True(t, foundParentScan)
			return
		}
	}
	t.Fatal("composite parent primary-key prefix shared table lock not found")
}

func TestChildInsertLocksReferencedUniqueIndexKey(t *testing.T) {
	mock := NewMockOptimizer(true)
	parent := mock.ctxt.tables["replace_fk_p"]
	child := mock.ctxt.tables["replace_fk_c"]
	child.Cols[1].Typ = plan.Type{Id: int32(types.T_varchar), Width: 20}
	child.Fkeys[0].ForeignCols = []uint64{1}
	indexName := "__mo_index_fk_parent_v"
	indexID := uint64(77901)
	parent.Indexes = append(parent.Indexes, &plan.IndexDef{
		IndexName: "uk_v", IndexTableName: indexName, Parts: []string{"v"},
		Unique: true, TableExist: true, IndexAlgo: catalog.MoIndexDefaultAlgo.ToString(),
	})
	indexTable := &plan.TableDef{
		TblId: indexID, Name: indexName,
		Cols: []*plan.ColDef{
			{Name: catalog.IndexTableIndexColName, ColId: 0, Typ: plan.Type{Id: int32(types.T_varchar), Width: 20}},
			{Name: catalog.Row_ID, ColId: 1, Hidden: true, Typ: plan.Type{Id: int32(types.T_Rowid)}},
		},
		Pkey: &plan.PrimaryKeyDef{Names: []string{catalog.IndexTableIndexColName},
			PkeyColName: catalog.IndexTableIndexColName},
		Name2ColIndex: map[string]int32{catalog.IndexTableIndexColName: 0, catalog.Row_ID: 1},
	}
	mock.ctxt.tables[indexName] = indexTable
	mock.ctxt.objects[indexName] = &plan.ObjectRef{
		Obj: int64(indexID), SchemaName: mock.ctxt.objects["replace_fk_p"].SchemaName, ObjName: indexName,
	}

	logicPlan, err := runOneStmt(mock, t, "INSERT INTO replace_fk_c VALUES (10, 'x')")
	require.NoError(t, err)
	for _, node := range logicPlan.GetQuery().Nodes {
		for _, target := range node.LockTargets {
			if target.TableId == indexID && target.Mode == lockpb.LockMode_Shared {
				assert.Equal(t, int32(types.T_varchar), target.PrimaryColTyp.Id)
				return
			}
		}
	}
	t.Fatal("referenced unique-index shared lock not found")
}

func TestReplaceAndChildInsertUseCanonicalForeignKeyLockOrder(t *testing.T) {
	mock := NewMockOptimizer(true)
	parent := mock.ctxt.tables["replace_fk_p"]
	child := mock.ctxt.tables["replace_fk_c"]
	if parent.Name2ColIndex == nil {
		parent.Name2ColIndex = make(map[string]int32)
		for i, col := range parent.Cols {
			parent.Name2ColIndex[col.Name] = int32(i)
		}
	}
	if child.Name2ColIndex == nil {
		child.Name2ColIndex = make(map[string]int32)
		for i, col := range child.Cols {
			child.Name2ColIndex[col.Name] = int32(i)
		}
	}
	parentPos := len(parent.Cols) - 1
	parent.Cols = append(parent.Cols, nil)
	copy(parent.Cols[parentPos+1:], parent.Cols[parentPos:])
	parent.Cols[parentPos] = &plan.ColDef{
		Name: "k", ColId: 3, Typ: plan.Type{Id: int32(types.T_varchar), Width: 20},
	}
	parent.Name2ColIndex["k"] = int32(parentPos)
	parent.Name2ColIndex[catalog.Row_ID] = int32(parentPos + 1)
	child.Cols[1].Typ = plan.Type{Id: int32(types.T_varchar), Width: 20}
	childPos := len(child.Cols) - 1
	child.Cols = append(child.Cols, nil)
	copy(child.Cols[childPos+1:], child.Cols[childPos:])
	child.Cols[childPos] = &plan.ColDef{
		Name: "pid2", ColId: 2, Typ: plan.Type{Id: int32(types.T_varchar), Width: 20},
	}
	child.Name2ColIndex["pid2"] = int32(childPos)
	child.Name2ColIndex[catalog.Row_ID] = int32(childPos + 1)
	child.Fkeys[0].ForeignCols = []uint64{1}
	child.Fkeys = append(child.Fkeys, &plan.ForeignKeyDef{
		Cols: []uint64{2}, ForeignTbl: parent.TblId, ForeignCols: []uint64{3},
	})

	addIndex := func(indexName, tableName string, tableID uint64, part string) {
		parent.Indexes = append(parent.Indexes, &plan.IndexDef{
			IndexName: indexName, IndexTableName: tableName, Parts: []string{part},
			Unique: true, TableExist: true, IndexAlgo: catalog.MoIndexDefaultAlgo.ToString(),
		})
		mock.ctxt.tables[tableName] = &plan.TableDef{
			TblId: tableID, Name: tableName,
			Cols: []*plan.ColDef{
				{Name: catalog.IndexTableIndexColName, ColId: 0, Typ: plan.Type{Id: int32(types.T_varchar), Width: 20}},
				{Name: catalog.Row_ID, ColId: 1, Hidden: true, Typ: plan.Type{Id: int32(types.T_Rowid)}},
			},
			Pkey: &plan.PrimaryKeyDef{Names: []string{catalog.IndexTableIndexColName},
				PkeyColName: catalog.IndexTableIndexColName},
			Name2ColIndex: map[string]int32{catalog.IndexTableIndexColName: 0, catalog.Row_ID: 1},
		}
		mock.ctxt.objects[tableName] = &plan.ObjectRef{
			Obj: int64(tableID), SchemaName: mock.ctxt.objects["replace_fk_p"].SchemaName, ObjName: tableName,
		}
	}
	// Declaration order is z then a; physical lock order must be a then z.
	addIndex("uk_v", "__mo_index_z", 77911, "v")
	addIndex("uk_k", "__mo_index_a", 77912, "k")

	logicPlan, err := runOneStmt(mock, t, "INSERT INTO replace_fk_c VALUES (10, 'x', 'y')")
	require.NoError(t, err)
	query := logicPlan.GetQuery()
	lockNode := make(map[uint64]int32)
	for nodeID, node := range query.Nodes {
		for _, target := range node.LockTargets {
			if target.Mode == lockpb.LockMode_Shared {
				lockNode[target.TableId] = int32(nodeID)
			}
		}
	}
	require.Contains(t, lockNode, uint64(77911))
	require.Contains(t, lockNode, uint64(77912))
	var contains func(int32, int32) bool
	contains = func(root, target int32) bool {
		if root == target {
			return true
		}
		for _, childID := range query.Nodes[root].Children {
			if contains(childID, target) {
				return true
			}
		}
		return false
	}
	assert.True(t, contains(lockNode[77911], lockNode[77912]),
		"z lock must depend on the lexically earlier a lock regardless of FK declaration order")

	replacePlan, err := runOneStmt(mock, t, "REPLACE INTO replace_fk_p VALUES (1, 'x', 'y')")
	require.NoError(t, err)
	var replaceLockOrder []uint64
	for _, node := range replacePlan.GetQuery().Nodes {
		if node.NodeType != plan.Node_LOCK_OP || len(node.LockTargets) == 0 {
			continue
		}
		for _, target := range node.LockTargets {
			replaceLockOrder = append(replaceLockOrder, target.TableId)
		}
		break
	}
	require.Equal(t, []uint64{parent.TblId, parent.TblId, 77912, 77912, 77911, 77911}, replaceLockOrder,
		"REPLACE must lock the base table first and hidden unique indexes by physical table name")
}

func TestDeepCopyPreservesSharedLockMode(t *testing.T) {
	assert.Nil(t, DeepCopyLockTarget(nil))
	target := &plan.LockTarget{
		TableId:              42,
		ObjRef:               &plan.ObjectRef{Obj: 42, ObjName: "parent"},
		Mode:                 lockpb.LockMode_Shared,
		PrimaryColRelPos:     11,
		FilterColRelPos:      12,
		PartitionColIdxInBat: 13,
		HasPartitionCol:      true,
		LockRows:             makePlan2Int64ConstExprWithType(7),
	}
	assertScalarFields := func(t *testing.T, copied *plan.LockTarget) {
		t.Helper()
		assert.Equal(t, lockpb.LockMode_Shared, copied.Mode)
		assert.Equal(t, int32(11), copied.PrimaryColRelPos)
		assert.Equal(t, int32(12), copied.FilterColRelPos)
		assert.Equal(t, int32(13), copied.PartitionColIdxInBat)
		assert.True(t, copied.HasPartitionCol)
	}

	direct := DeepCopyLockTarget(target)
	require.NotSame(t, target, direct)
	assertScalarFields(t, direct)
	require.NotSame(t, target.ObjRef, direct.ObjRef)
	require.NotSame(t, target.LockRows, direct.LockRows)

	node := &plan.Node{NodeType: plan.Node_LOCK_OP, LockTargets: []*plan.LockTarget{target}}
	nodeCopy := DeepCopyNode(node)
	require.Len(t, nodeCopy.LockTargets, 1)
	assertScalarFields(t, nodeCopy.LockTargets[0])
	require.NotSame(t, target, nodeCopy.LockTargets[0])

	queryCopy := DeepCopyQuery(&plan.Query{Nodes: []*plan.Node{node}})
	require.Len(t, queryCopy.Nodes, 1)
	require.Len(t, queryCopy.Nodes[0].LockTargets, 1)
	assertScalarFields(t, queryCopy.Nodes[0].LockTargets[0])
	require.NotSame(t, target, queryCopy.Nodes[0].LockTargets[0])
}

func TestReplaceODKU(t *testing.T) {
	mock := NewMockOptimizer(true)
	// INSERT ON DUPLICATE KEY UPDATE should be rewritten to REPLACE path
	sqls := []string{
		"INSERT INTO dept VALUES (1, 'Sales', 'NY') ON DUPLICATE KEY UPDATE loc = VALUES(loc)",
	}
	runTestShouldPass(mock, t, sqls, false, false)
}

func TestSubQuery(t *testing.T) {
	mock := NewMockOptimizer(false)
	// should pass
	sqls := []string{
		"SELECT * FROM NATION where N_REGIONKEY > (select max(R_REGIONKEY) from REGION)",                                 // unrelated
		"SELECT * FROM NATION where N_REGIONKEY in (select max(R_REGIONKEY) from REGION)",                                // unrelated
		"SELECT * FROM NATION where N_REGIONKEY not in (select max(R_REGIONKEY) from REGION)",                            // unrelated
		"SELECT * FROM NATION where exists (select max(R_REGIONKEY) from REGION)",                                        // unrelated
		"SELECT * FROM NATION where N_REGIONKEY > (select max(R_REGIONKEY) from REGION where R_REGIONKEY = N_REGIONKEY)", // related
		//"DELETE FROM NATION WHERE N_NATIONKEY > 10",
		`select
		sum(l_extendedprice) / 7.0 as avg_yearly
	from
		lineitem,
		part
	where
		p_partkey = l_partkey
		and p_brand = 'Brand#54'
		and p_container = 'LG BAG'
		and l_quantity < (
			select
				0.2 * avg(l_quantity)
			from
				lineitem
			where
				l_partkey = p_partkey
		);`, //tpch q17
		"select * from nation where n_regionkey in (select r_regionkey from region) and n_nationkey not in (1,2) and n_nationkey = some (select n_nationkey from nation2)",
		"SELECT * FROM NATION where N_REGIONKEY > (select max(R_REGIONKEY) from REGION where R_REGIONKEY < N_REGIONKEY)",                     // non-eq agg scalar subquery
		"SELECT * FROM NATION where N_REGIONKEY > (select max(R_REGIONKEY) from REGION where N_NAME = R_NAME and R_REGIONKEY < N_REGIONKEY)", // mixed eq + non-eq predicates -> two pullup-added GroupBy entries
		"SELECT * FROM NATION where (select count(*) from REGION where N_NAME = R_NAME and R_REGIONKEY < N_REGIONKEY) = 1",                   // count(*) with mixed eq + non-eq predicates
		"SELECT * FROM NATION where (select avg(R_REGIONKEY) from REGION where N_NAME = R_NAME and R_REGIONKEY < N_REGIONKEY) = 1",           // avg with mixed eq + non-eq predicates
		`SELECT * FROM NATION n1 WHERE EXISTS (
			SELECT 1 FROM NATION n2 WHERE EXISTS (
				SELECT 1 FROM NATION n3
				WHERE n3.N_NATIONKEY = n2.N_NATIONKEY AND n2.N_NATIONKEY = n1.N_NATIONKEY
			)
		)`, // two-level correlated EXISTS subquery
		`SELECT * FROM NATION n1 WHERE NOT EXISTS (
			SELECT 1 FROM NATION n2 WHERE EXISTS (
				SELECT 1 FROM NATION n3
				WHERE n3.N_NATIONKEY = n2.N_NATIONKEY AND n2.N_NATIONKEY = n1.N_NATIONKEY
			)
		)`, // two-level correlated NOT EXISTS subquery
		`SELECT * FROM NATION n1 WHERE n1.N_NATIONKEY IN (
			SELECT n2.N_NATIONKEY FROM NATION n2 WHERE n2.N_NATIONKEY IN (
				SELECT n3.N_NATIONKEY FROM NATION n3
				WHERE n3.N_NATIONKEY = n2.N_NATIONKEY AND n2.N_NATIONKEY = n1.N_NATIONKEY
			)
		)`, // two-level correlated IN subquery
		`SELECT * FROM NATION n1 WHERE n1.N_NATIONKEY NOT IN (
			SELECT n2.N_NATIONKEY FROM NATION n2 WHERE n2.N_NATIONKEY IN (
				SELECT n3.N_NATIONKEY FROM NATION n3
				WHERE n3.N_NATIONKEY = n2.N_NATIONKEY AND n2.N_NATIONKEY = n1.N_NATIONKEY
			)
		)`, // two-level correlated NOT IN subquery
		`SELECT * FROM NATION n1 WHERE n1.N_NATIONKEY = ANY (
			SELECT n2.N_NATIONKEY FROM NATION n2 WHERE n2.N_NATIONKEY = ANY (
				SELECT n3.N_NATIONKEY FROM NATION n3
				WHERE n3.N_NATIONKEY = n2.N_NATIONKEY AND n2.N_NATIONKEY = n1.N_NATIONKEY
			)
		)`, // two-level correlated ANY subquery
		`SELECT * FROM NATION n1 WHERE n1.N_NATIONKEY > ALL (
			SELECT n2.N_NATIONKEY FROM NATION n2 WHERE n2.N_NATIONKEY = ANY (
				SELECT n3.N_NATIONKEY FROM NATION n3
				WHERE n3.N_NATIONKEY = n2.N_NATIONKEY AND n2.N_NATIONKEY < n1.N_NATIONKEY
			)
		)`, // two-level correlated ALL subquery
		`SELECT n1.N_NATIONKEY,
			(SELECT MAX(n2.N_REGIONKEY)
			 FROM NATION n2
			 WHERE n2.N_REGIONKEY = (
				 SELECT MAX(n3.N_REGIONKEY)
				 FROM NATION n3
				 WHERE n3.N_NATIONKEY = n1.N_NATIONKEY
			 ))
		 FROM NATION n1`, // two-level correlated scalar aggregate subquery
	}
	runTestShouldPass(mock, t, sqls, false, false)

	// should error
	sqls = []string{
		"SELECT * FROM NATION where N_REGIONKEY > (select max(R_REGIONKEY) from REGION222)",                                                          // table not exist
		"SELECT * FROM NATION where N_REGIONKEY > (select max(R_REGIONKEY) from REGION where R_REGIONKEY < N_REGIONKEY222)",                          // column not exist
		"SELECT * FROM NATION where N_REGIONKEY > (select max(R_REGIONKEY) from REGION where R_REGIONKEY < N_REGIONKEY group by R_NAME)",             // non-eq agg scalar subquery with GROUP BY
		"SELECT * FROM NATION where N_REGIONKEY > (select max(R_REGIONKEY) from REGION where R_REGIONKEY < N_REGIONKEY having max(R_REGIONKEY) > 0)", // non-eq agg scalar subquery with HAVING
		"SELECT * FROM NATION where N_REGIONKEY > (select max(R_REGIONKEY) + 1 from REGION where R_REGIONKEY < N_REGIONKEY)",                         // non-eq agg scalar subquery with computed projection
	}
	runTestShouldError(mock, t, sqls)

	sql := `SELECT * FROM NATION n1 WHERE n1.N_NATIONKEY > ANY (
		SELECT n2.N_NATIONKEY FROM NATION n2 WHERE n2.N_NATIONKEY = ANY (
			SELECT n3.N_NATIONKEY FROM NATION n3
			WHERE (n3.N_NATIONKEY = n2.N_NATIONKEY AND n2.N_REGIONKEY = n1.N_REGIONKEY)
				OR n3.N_REGIONKEY = 1
		)
	)`
	_, err := runOneStmt(mock, t, sql)
	assert.Error(t, err)
	if err != nil {
		assert.Contains(t, err.Error(), "deep correlated predicate containing inner columns cannot be pulled above mark join")
	}
}

func TestCorrelatedScalarAggregatePushdown(t *testing.T) {
	correlated := `l_quantity < (
		select 0.2 * avg(l_quantity)
		from lineitem
		where l_partkey = p_partkey
	)`
	optimized := []string{
		`select sum(l_extendedprice) / 7.0
		 from lineitem, part
		 where p_partkey = l_partkey
		   and p_brand = 'Brand#54'
		   and p_container = 'LG BAG'
		   and ` + correlated,
		`select sum(l_extendedprice) / 7.0
		 from lineitem, part
		 where ` + correlated + `
		   and p_container = 'LG BAG'
		   and p_brand = 'Brand#54'
		   and p_partkey = l_partkey`,
	}

	for i, sql := range optimized {
		logicPlan, err := runSelectWithValidator(NewMockOptimizer(false), t, sql, func(query *plan.Query) error {
			agg := findAggregateByFunction(query, "avg")
			require.NotNil(t, agg, "case %d: correlated AVG not found before remapping", i)
			require.Len(t, agg.Children, 1)
			semi := query.Nodes[agg.Children[0]]
			require.Equal(t, plan.Node_JOIN, semi.NodeType)
			require.Equal(t, plan.Node_SEMI, semi.JoinType)
			require.Len(t, semi.Children, 2)
			domain := query.Nodes[semi.Children[1]]
			require.Len(t, domain.BindingTags, 1)

			partTags := make([]int32, 0, 2)
			for _, node := range query.Nodes {
				if node.NodeType == plan.Node_TABLE_SCAN && node.TableDef != nil && node.TableDef.Name == "part" {
					require.Len(t, node.BindingTags, 1)
					partTags = append(partTags, node.BindingTags[0])
				}
			}
			require.Len(t, partTags, 2)
			require.NotEqual(t, partTags[0], partTags[1],
				"case %d: cloned scans must have distinct binding tags", i)
			for _, pred := range semi.OnList {
				require.True(t, containsTag(pred, domain.BindingTags[0]),
					"case %d: SEMI predicate must reference the cloned scan binding", i)
			}
			return nil
		})
		require.NoError(t, err)
		query := logicPlan.GetQuery()
		agg := findAggregateByFunction(query, "avg")
		require.NotNil(t, agg, "case %d: correlated AVG not found", i)
		require.Len(t, agg.Children, 1)
		semi := query.Nodes[agg.Children[0]]
		require.Equal(t, plan.Node_JOIN, semi.NodeType)
		require.Equal(t, plan.Node_SEMI, semi.JoinType)
		require.Len(t, semi.Children, 2)
		domain := query.Nodes[semi.Children[1]]
		require.Equal(t, plan.Node_TABLE_SCAN, domain.NodeType)
		require.Equal(t, "part", domain.TableDef.Name)
		require.Len(t, domain.FilterList, 2, "case %d: copied key domain must retain both selective filters", i)
		require.True(t, exprListContainsStringLiteral(domain.FilterList, "Brand#54"))
		require.True(t, exprListContainsStringLiteral(domain.FilterList, "LG BAG"))
	}

	controls := []string{
		`select sum(l_extendedprice) / 7.0
		 from lineitem, part
		 where p_partkey = l_partkey and ` + correlated,
		`select sum(l_extendedprice) / 7.0
		 from lineitem, part
		 where p_partkey = l_partkey
		   and p_partkey > floor(rand() * 100)
		   and ` + correlated,
	}
	for i, sql := range controls {
		logicPlan, err := runOneStmt(NewMockOptimizer(false), t, sql)
		require.NoError(t, err)
		query := logicPlan.GetQuery()
		agg := findAggregateByFunction(query, "avg")
		require.NotNil(t, agg, "control %d: correlated AVG not found", i)
		require.Len(t, agg.Children, 1)
		child := query.Nodes[agg.Children[0]]
		require.False(t, child.NodeType == plan.Node_JOIN && child.JoinType == plan.Node_SEMI,
			"control %d: an unfiltered or volatile domain must not be copied", i)
	}
}

func findAggregateByFunction(query *plan.Query, name string) *plan.Node {
	for _, node := range query.Nodes {
		if node.NodeType != plan.Node_AGG {
			continue
		}
		for _, expr := range node.AggList {
			if exprContainsFuncName(expr, name) {
				return node
			}
		}
	}
	return nil
}

func exprListContainsStringLiteral(exprs []*plan.Expr, value string) bool {
	for _, expr := range exprs {
		if exprContainsStringLiteral(expr, value) {
			return true
		}
	}
	return false
}

func runSelectWithValidator(
	opt Optimizer,
	t *testing.T,
	sql string,
	validate func(*plan.Query) error,
) (*Plan, error) {
	stmts, err := mysql.Parse(opt.CurrentContext().GetContext(), sql, 1)
	require.NoError(t, err)
	stmt, ok := stmts[0].(*tree.Select)
	require.True(t, ok)
	return bindAndOptimizeSelectQueryWithValidator(
		plan.Query_SELECT, opt.CurrentContext(), stmt, false, false, validate,
	)
}

func TestAggregateArgumentScalarSubqueryFlattened(t *testing.T) {
	tests := []string{
		`SELECT AVG((SELECT COUNT(*) FROM REGION r WHERE r.R_REGIONKEY = n.N_NATIONKEY))
		 FROM NATION n`,
		`SELECT n.N_REGIONKEY,
		        COUNT(*),
		        AVG((SELECT COUNT(*) FROM REGION r WHERE r.R_REGIONKEY = n.N_NATIONKEY))
		 FROM NATION n
		 GROUP BY n.N_REGIONKEY`,
		`WITH stats AS (
		     SELECT n.N_REGIONKEY,
		            SUM((SELECT COUNT(*) FROM REGION r WHERE r.R_REGIONKEY = n.N_NATIONKEY)) AS total_regions
		     FROM NATION n
		     GROUP BY n.N_REGIONKEY
		 )
		 SELECT * FROM stats`,
	}

	for _, sql := range tests {
		logicPlan, err := runOneStmt(NewMockOptimizer(false), t, sql)
		require.NoError(t, err, sql)

		foundAgg := false
		for _, node := range logicPlan.GetQuery().Nodes {
			if node.NodeType != plan.Node_AGG {
				continue
			}
			foundAgg = true
			for _, agg := range node.AggList {
				require.False(t, hasSubquery(agg), "AGG contains an executable Expr_Sub: %s", sql)
			}
		}
		require.True(t, foundAgg, sql)
	}
}

func TestIssue23154VectorScalarSubqueryFlattenedEverywhere(t *testing.T) {
	mock := NewMockOptimizer(false)
	vectorCol := mock.ctxt.tables["nation"].Cols[3]
	vectorCol.Typ = plan.Type{Id: int32(types.T_array_float64), Width: 1024}

	sql := `SELECT COUNT(*) AS count,
	               AVG(cosine_similarity(
	                   n_comment,
	                   (SELECT n_comment FROM nation WHERE n_name = 'ref'))) AS avg_similarity,
	               MAX(cosine_similarity(
	                   n_comment,
	                   (SELECT n_comment FROM nation WHERE n_name = 'ref'))) AS max_similarity,
	               MIN(cosine_similarity(
	                   n_comment,
	                   (SELECT n_comment FROM nation WHERE n_name = 'ref'))) AS min_similarity
	          FROM nation
	         WHERE n_comment IS NOT NULL
	           AND n_name != 'ref'
	           AND cosine_similarity(
	                   n_comment,
	                   (SELECT n_comment FROM nation WHERE n_name = 'ref')) >= 0.9`
	logicPlan, err := runOneStmt(mock, t, sql)
	require.NoError(t, err)

	foundAgg := false
	for _, node := range logicPlan.GetQuery().Nodes {
		if node.NodeType == plan.Node_AGG {
			foundAgg = true
		}
		for _, exprs := range [][]*plan.Expr{
			node.AggList,
			node.FilterList,
			node.ProjectList,
			node.OnList,
			node.GroupBy,
		} {
			for _, expr := range exprs {
				require.False(t, hasSubquery(expr),
					"executable plan expression contains Expr_Sub: %s", sql)
			}
		}
		for _, orderBy := range node.OrderBy {
			require.False(t, hasSubquery(orderBy.Expr),
				"executable ORDER BY expression contains Expr_Sub: %s", sql)
		}
	}
	require.True(t, foundAgg)
}

func TestIssue23157VectorScoreScalarSubqueryFlattened(t *testing.T) {
	mock := NewMockOptimizer(false)
	vectorCol := mock.ctxt.tables["nation"].Cols[3]
	vectorCol.Typ = plan.Type{Id: int32(types.T_array_float64), Width: 1024}

	sql := `SELECT n_name,
	               COUNT(*) AS count,
	               AVG(cosine_similarity(
	                   n_comment,
	                   (SELECT n_comment
	                      FROM nation
	                     WHERE n_nationkey = (
	                         SELECT n_nationkey
	                           FROM nation
	                          WHERE n_comment IS NOT NULL
	                          LIMIT 1)))) AS avg_similarity
	          FROM nation
	         WHERE n_comment IS NOT NULL
	           AND n_name IS NOT NULL
	           AND n_name != ''
	         GROUP BY n_name
	        HAVING avg_similarity > 0.6
	         ORDER BY avg_similarity DESC
	         LIMIT 10`
	logicPlan, err := runOneStmt(mock, t, sql)
	require.NoError(t, err)

	foundAgg := false
	for _, node := range logicPlan.GetQuery().Nodes {
		if node.NodeType == plan.Node_AGG {
			foundAgg = true
		}
		for _, exprs := range [][]*plan.Expr{
			node.AggList,
			node.FilterList,
			node.ProjectList,
		} {
			for _, expr := range exprs {
				require.False(t, hasSubquery(expr),
					"executable plan expression contains Expr_Sub: %s", sql)
			}
		}
		for _, orderBy := range node.OrderBy {
			require.False(t, hasSubquery(orderBy.Expr),
				"executable ORDER BY expression contains Expr_Sub: %s", sql)
		}
	}
	require.True(t, foundAgg)
}

func TestAggregateArgumentScalarSubqueryFlattenedBeforeOrderedGroupConcat(t *testing.T) {
	sql := `SELECT n.N_REGIONKEY,
	               GROUP_CONCAT(n.N_NAME ORDER BY n.N_NAME),
	               AVG((SELECT COUNT(*) FROM REGION r WHERE r.R_REGIONKEY = n.N_NATIONKEY))
	        FROM NATION n
	        GROUP BY n.N_REGIONKEY`
	logicPlan, err := runOneStmt(NewMockOptimizer(false), t, sql)
	require.NoError(t, err)

	foundGroupConcatAgg := false
	query := logicPlan.GetQuery()
	for _, node := range query.Nodes {
		if node.NodeType != plan.Node_AGG {
			continue
		}

		var groupConcat *plan.Function
		for _, agg := range node.AggList {
			if f := agg.GetF(); f != nil && f.Func.ObjName == NameGroupConcat {
				groupConcat = f
				break
			}
		}
		if groupConcat == nil {
			continue
		}

		foundGroupConcatAgg = true
		require.False(t, hasSubquery(&plan.Expr{
			Expr: &plan.Expr_F{F: groupConcat},
		}), "GROUP_CONCAT contains an executable Expr_Sub")
		require.Len(t, groupConcat.Args, 2)
		require.Equal(
			t,
			plan.AggregateConfigType_AGG_CONFIG_GROUP_CONCAT_ORDER,
			groupConcat.AggConfigType,
		)
		require.Equal(t, groupConcatOrderConfigVersion, groupConcat.AggConfig[0])

		require.Len(t, node.Children, 1)
		require.Equal(t, plan.Node_JOIN, query.Nodes[node.Children[0]].NodeType)
	}
	require.True(t, foundGroupConcatAgg)
	require.Empty(t, collectReachableSortNodes(query))
}

func TestGroupConcatRejectsOrderBySubquery(t *testing.T) {
	tests := map[string]string{
		"positional": `SELECT n.N_REGIONKEY,
		                     GROUP_CONCAT(
		                         (SELECT r.R_NAME
		                            FROM REGION r
		                           WHERE r.R_REGIONKEY = n.N_NATIONKEY)
		                         ORDER BY 1)
		              FROM NATION n
		              GROUP BY n.N_REGIONKEY`,
		"wrapped positional": `SELECT n.N_REGIONKEY,
		                             GROUP_CONCAT(
		                                 COALESCE((SELECT r.R_NAME
		                                             FROM REGION r
		                                            WHERE r.R_REGIONKEY = n.N_NATIONKEY), '')
		                                 ORDER BY 1)
		                      FROM NATION n
		                      GROUP BY n.N_REGIONKEY`,
		"wrapped explicit": `SELECT n.N_REGIONKEY,
		                           GROUP_CONCAT(
		                               n.N_NAME
		                               ORDER BY COALESCE((SELECT r.R_NAME
		                                                    FROM REGION r
		                                                   WHERE r.R_REGIONKEY = n.N_NATIONKEY), ''))
		                    FROM NATION n
		                    GROUP BY n.N_REGIONKEY`,
	}

	for name, sql := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := runOneStmt(NewMockOptimizer(false), t, sql)
			require.Error(t, err)
			require.Contains(t, err.Error(), "subquery in group_concat ORDER BY")
		})
	}
}

func TestGroupConcatOrdinalReusesArgument(t *testing.T) {
	logicPlan, err := runOneStmt(
		NewMockOptimizer(false),
		t,
		"SELECT GROUP_CONCAT(RAND() ORDER BY 1) FROM NATION",
	)
	require.NoError(t, err)

	var fn *plan.Function
	for _, node := range logicPlan.GetQuery().Nodes {
		if node.NodeType == plan.Node_AGG && len(node.AggList) == 1 {
			fn = node.AggList[0].GetF()
			break
		}
	}
	require.NotNil(t, fn)
	require.Len(t, fn.Args, 1, "ORDER BY ordinal must not add a second RAND evaluator")
	require.Equal(t, groupConcatOrderConfigVersion, fn.AggConfig[0])

	pos := 1 + 4
	require.Equal(t, uint32(1), binary.BigEndian.Uint32(fn.AggConfig[pos:pos+4]))
	pos += 4 + 1
	require.Equal(t, uint32(0), binary.BigEndian.Uint32(fn.AggConfig[pos:pos+4]))
}

func TestGroupConcatAcceptsConstantOrderExpressions(t *testing.T) {
	for _, sql := range []string{
		"SELECT GROUP_CONCAT(N_NAME ORDER BY NULL) FROM NATION",
		"SELECT GROUP_CONCAT(N_NAME ORDER BY 'constant') FROM NATION",
		"SELECT GROUP_CONCAT(N_NAME ORDER BY 1.5) FROM NATION",
		"SELECT GROUP_CONCAT(N_NAME ORDER BY -1) FROM NATION",
	} {
		_, err := runOneStmt(NewMockOptimizer(false), t, sql)
		require.NoError(t, err, sql)
	}
}

func TestGroupConcatRejectsUnsupportedOrderKeyType(t *testing.T) {
	_, err := runOneStmt(
		NewMockOptimizer(false),
		t,
		"SELECT GROUP_CONCAT(N_NAME ORDER BY (N_REGIONKEY, N_NAME)) FROM NATION",
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "group_concat ORDER BY type TUPLE")
	require.NotContains(t, err.Error(), "internal error")
}

func TestOrderedGroupConcatInNonEquiCorrelatedScalarSubqueryKeepsConfig(t *testing.T) {
	logicPlan, err := runOneStmt(
		NewMockOptimizer(false),
		t,
		`SELECT o.N_REGIONKEY, o.N_NAME,
		        (SELECT GROUP_CONCAT(i.N_NAME ORDER BY i.N_NATIONKEY DESC SEPARATOR '~')
		           FROM NATION i
		          WHERE i.N_REGIONKEY < o.N_REGIONKEY)
		   FROM NATION o`,
	)
	require.NoError(t, err)

	found := false
	for _, node := range logicPlan.GetQuery().Nodes {
		for _, agg := range node.AggList {
			fn := agg.GetF()
			if fn == nil || fn.Func.ObjName != NameGroupConcat {
				continue
			}
			found = true
			require.Equal(
				t,
				plan.AggregateConfigType_AGG_CONFIG_GROUP_CONCAT_ORDER,
				fn.AggConfigType,
			)
			require.NotEmpty(t, fn.AggConfig)
		}
	}
	require.True(t, found)
}

func TestMysqlCompatibilityMode(t *testing.T) {
	mock := NewMockOptimizer(false)

	sqls := []string{
		"SELECT n_nationkey FROM NATION group by n_name",
		"SELECT n_nationkey, min(n_name) FROM NATION",
		"SELECT n_nationkey + 100 FROM NATION group by n_name",
	}
	// withou mysql compatibility
	runTestShouldError(mock, t, sqls)
	// with mysql compatibility
	mock.ctxt.mysqlCompatible = true
	runTestShouldPass(mock, t, sqls, false, false)
}

func TestOnlyFullGroupByMySQLAndMatrixOneNativeModes(t *testing.T) {
	const whereConstrained = "select deptno, job, sum(sal) from constraint_test.emp where job = 'clerk' group by deptno"
	const primaryKeyDependent = "select empno, ename, sum(sal) from constraint_test.emp group by empno"
	const unsafeBareColumn = "select deptno, job, sum(sal) from constraint_test.emp group by deptno"
	const volatileWhereValue = "select deptno, empno, sum(sal) from constraint_test.emp where empno = floor(rand() * 100) group by deptno"
	const statementStableWhereValue = "select deptno, hiredate, sum(sal) from constraint_test.emp where hiredate = current_date() group by deptno"
	const whereConstrainedOrderBy = "select deptno, sum(sal) from constraint_test.emp where job = 'clerk' group by deptno order by job"
	const whereConstrainedHaving = "select deptno, sum(sal) from constraint_test.emp where job = 'clerk' group by deptno having job = 'clerk'"
	const primaryKeyDependentHaving = "select empno, sum(sal) from constraint_test.emp group by empno having ename <> ''"
	const primaryKeyDependentRollup = "select empno, ename, sum(sal) from constraint_test.emp group by empno with rollup"
	const primaryKeyDependentCube = "select empno, ename, sum(sal) from constraint_test.emp group by cube(empno)"
	const primaryKeyDependentRollupHaving = "select empno, sum(sal) from constraint_test.emp group by empno with rollup having ename <> ''"
	const primaryKeyDependentRollupOrderBy = "select empno, sum(sal) from constraint_test.emp group by empno with rollup order by ename"
	const whereConstrainedWindow = "select deptno, first_value(job) over (partition by job order by job), sum(sal) from constraint_test.emp where job = 'clerk' group by deptno"
	const whereConstrainedWindowNoSpec = "select deptno, first_value(job) over (), sum(sal) from constraint_test.emp where job = 'clerk' group by deptno"
	const primaryKeyDependentWindow = "select empno, first_value(ename) over (partition by ename order by ename), sum(sal) from constraint_test.emp group by empno"

	tests := []struct {
		name    string
		mode    string
		sql     string
		wantErr bool
	}{
		{
			name: "mysql mode without only full group by stays permissive",
			mode: "STRICT_TRANS_TABLES",
			sql:  unsafeBareColumn,
		},
		{
			name: "mysql only full group by allows where constrained column",
			mode: "ONLY_FULL_GROUP_BY",
			sql:  whereConstrained,
		},
		{
			name: "mysql only full group by allows primary key dependency",
			mode: "ONLY_FULL_GROUP_BY",
			sql:  primaryKeyDependent,
		},
		{
			name: "mysql only full group by keeps where constrained window inputs below window stage",
			mode: "ONLY_FULL_GROUP_BY",
			sql:  whereConstrainedWindow,
		},
		{
			name: "mysql only full group by keeps where constrained window argument below window stage",
			mode: "ONLY_FULL_GROUP_BY",
			sql:  whereConstrainedWindowNoSpec,
		},
		{
			name: "mysql only full group by keeps primary key dependent window inputs below window stage",
			mode: "ONLY_FULL_GROUP_BY",
			sql:  primaryKeyDependentWindow,
		},
		{
			name: "mysql only full group by recognizes mode token case and spacing",
			mode: " strict_trans_tables, only_full_group_by ",
			sql:  primaryKeyDependent,
		},
		{
			name:    "mysql only full group by rejects unconstrained column",
			mode:    "ONLY_FULL_GROUP_BY",
			sql:     unsafeBareColumn,
			wantErr: true,
		},
		{
			name:    "mysql only full group by rejects volatile where value",
			mode:    "ONLY_FULL_GROUP_BY",
			sql:     volatileWhereValue,
			wantErr: true,
		},
		{
			name: "mysql only full group by allows statement stable where value",
			mode: "ONLY_FULL_GROUP_BY",
			sql:  statementStableWhereValue,
		},
		{
			name: "mysql only full group by allows where constrained having column",
			mode: "ONLY_FULL_GROUP_BY",
			sql:  whereConstrainedHaving,
		},
		{
			name: "mysql only full group by allows where constrained order by column",
			mode: "ONLY_FULL_GROUP_BY",
			sql:  whereConstrainedOrderBy,
		},
		{
			name: "mysql only full group by allows primary key dependent having column",
			mode: "ONLY_FULL_GROUP_BY",
			sql:  primaryKeyDependentHaving,
		},
		{
			name:    "mysql only full group by rejects primary key dependency in rollup total",
			mode:    "ONLY_FULL_GROUP_BY",
			sql:     primaryKeyDependentRollup,
			wantErr: true,
		},
		{
			name:    "mysql only full group by rejects primary key dependency in cube total",
			mode:    "ONLY_FULL_GROUP_BY",
			sql:     primaryKeyDependentCube,
			wantErr: true,
		},
		{
			name:    "mysql only full group by rejects primary key dependent rollup having",
			mode:    "ONLY_FULL_GROUP_BY",
			sql:     primaryKeyDependentRollupHaving,
			wantErr: true,
		},
		{
			name:    "mysql only full group by rejects primary key dependent rollup order by",
			mode:    "ONLY_FULL_GROUP_BY",
			sql:     primaryKeyDependentRollupOrderBy,
			wantErr: true,
		},
		{
			name:    "matrixone native keeps strict group by",
			mode:    "ONLY_FULL_GROUP_BY,MATRIXONE_NATIVE",
			sql:     whereConstrained,
			wantErr: true,
		},
		{
			name:    "matrixone native rejects primary key dependency exception",
			mode:    "ONLY_FULL_GROUP_BY,MATRIXONE_NATIVE",
			sql:     primaryKeyDependent,
			wantErr: true,
		},
		{
			name:    "matrixone native rejects where constrained having column",
			mode:    "ONLY_FULL_GROUP_BY,MATRIXONE_NATIVE",
			sql:     whereConstrainedHaving,
			wantErr: true,
		},
		{
			name:    "matrixone native rejects where constrained order by column",
			mode:    "ONLY_FULL_GROUP_BY,MATRIXONE_NATIVE",
			sql:     whereConstrainedOrderBy,
			wantErr: true,
		},
		{
			name: "matrixone native without only full group by stays permissive",
			mode: "MATRIXONE_NATIVE",
			sql:  unsafeBareColumn,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mock := NewMockOptimizer(false)
			mock.ctxt.SetSqlModeOverride(test.mode)
			stmts, err := mysql.Parse(mock.CurrentContext().GetContext(), test.sql, 1)
			require.NoError(t, err)

			_, err = BuildPlan(mock.CurrentContext(), stmts[0], false)
			if test.wantErr {
				require.ErrorContains(t, err, "must appear in the GROUP BY clause")
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestOnlyFullGroupByCompositePrimaryKeyDependency(t *testing.T) {
	builder := &QueryBuilder{
		qry: &plan.Query{
			Nodes: []*plan.Node{
				{
					TableDef: &plan.TableDef{
						Cols: []*plan.ColDef{
							{Name: "tenant_id", Typ: plan.Type{Id: int32(types.T_int64)}},
							{Name: "id", Typ: plan.Type{Id: int32(types.T_int64)}},
						},
						Pkey: &plan.PrimaryKeyDef{
							// MatrixOne stores a composite key in a hidden column while
							// Names retains the user-visible key columns.
							Cols:        []uint64{2},
							PkeyColName: catalog.CPrimaryKeyColName,
							Names:       []string{"tenant_id", "id"},
						},
					},
				},
			},
		},
	}
	binding := NewBinding(1, 0, "", "composite_pk", 0, []string{"tenant_id", "id"}, nil, nil, false, nil)
	ctx := &BindContext{groups: []*plan.Expr{
		{Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: binding.tag, ColPos: 0}}},
		{Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: binding.tag, ColPos: 1}}},
	}, groupingFlag: []bool{true, true}}

	require.True(t, builder.groupByIncludesPrimaryKey(ctx, binding))
	ctx.groupingFlag[1] = false
	require.False(t, builder.groupByIncludesPrimaryKey(ctx, binding))
	ctx.groupingFlag[1] = true
	ctx.groups = ctx.groups[:1]
	ctx.groupingFlag = ctx.groupingFlag[:1]
	require.False(t, builder.groupByIncludesPrimaryKey(ctx, binding))
}

func TestOnlyFullGroupByUsesStructuredBoundColumns(t *testing.T) {
	builder := &QueryBuilder{
		qry: &plan.Query{Nodes: []*plan.Node{
			{TableDef: &plan.TableDef{
				Cols: []*plan.ColDef{
					{Name: "customer.account", Typ: plan.Type{Id: int32(types.T_varchar)}},
					{Name: "unsafe", Typ: plan.Type{Id: int32(types.T_varchar)}},
				},
				Pkey: &plan.PrimaryKeyDef{
					PkeyColName: "customer.account",
					Names:       []string{"customer.account"},
				},
			}},
			{TableDef: &plan.TableDef{}},
		}},
	}
	binding := NewBinding(1, 0, "", "t", 0, []string{"customer.account", "unsafe"}, nil, nil, false, nil)
	unsafeBinding := NewBinding(2, 1, "", "u", 0, []string{"unsafe"}, nil, nil, false, nil)
	ctx := &BindContext{
		bindingByTag: map[int32]*Binding{binding.tag: binding, unsafeBinding.tag: unsafeBinding},
		groups: []*plan.Expr{{Expr: &plan.Expr_Col{Col: &plan.ColRef{
			RelPos: binding.tag,
			ColPos: 0,
		}}}},
		groupingFlag: []bool{true},
	}

	rejected, found := builder.mysqlFullGroupByRejectedColumn(ctx, []boundColumn{
		{name: "t.customer.account", relation: binding.tag, columnPos: 0},
		{name: "u.unsafe", relation: unsafeBinding.tag, columnPos: 0},
	})
	require.True(t, found)
	require.Equal(t, "u.unsafe", rejected)

	convertedColumn := &plan.Expr{Expr: &plan.Expr_F{F: &plan.Function{Args: []*plan.Expr{
		{Expr: &plan.Expr_Lit{Lit: &plan.Literal{}}},
		{Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: binding.tag, ColPos: 0}}},
	}}}}
	require.True(t, builder.mysqlFullGroupByAllowsColRef(ctx, convertedColumn))
	ctx.groupingFlag[0] = false
	require.False(t, builder.mysqlFullGroupByAllowsColRef(ctx, convertedColumn))
}

func TestOnlyFullGroupByEnumColumnValidation(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "rejects unconstrained enum projection",
			sql:     "select deptno, job, sum(sal) from constraint_test.emp group by deptno",
			wantErr: true,
		},
		{
			name: "allows where constrained enum projection",
			sql:  "select deptno, job, sum(sal) from constraint_test.emp where job = 'clerk' group by deptno",
		},
		{
			name: "allows where constrained enum having",
			sql:  "select deptno, sum(sal) from constraint_test.emp where job = 'clerk' group by deptno having job = 'clerk'",
		},
		{
			name: "allows primary key dependent enum projection",
			sql:  "select empno, job, sum(sal) from constraint_test.emp group by empno",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mock := NewMockOptimizer(false)
			mock.ctxt.SetSqlModeOverride("ONLY_FULL_GROUP_BY")
			_, tableDef, err := mock.ctxt.Resolve("constraint_test", "emp", nil)
			require.NoError(t, err)
			tableDef.Cols[2].Typ.Id = int32(types.T_enum)
			tableDef.Cols[2].Typ.Enumvalues = "clerk,manager"

			stmts, err := mysql.Parse(mock.CurrentContext().GetContext(), test.sql, 1)
			require.NoError(t, err)
			_, err = BuildPlan(mock.CurrentContext(), stmts[0], false)
			if test.wantErr {
				require.ErrorContains(t, err, "must appear in the GROUP BY clause")
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestTcl(t *testing.T) {
	mock := NewMockOptimizer(false)
	// should pass
	sqls := []string{
		"start transaction",
		"start transaction read write",
		"begin",
		"commit and chain",
		"commit and chain no release",
		"rollback and chain",
	}
	runTestShouldPass(mock, t, sqls, false, false)

	// should error
	sqls = []string{}
	runTestShouldError(mock, t, sqls)
}

func TestDdl(t *testing.T) {
	mock := NewMockOptimizer(true)
	rt := moruntime.DefaultRuntime()
	moruntime.SetupServiceBasedRuntime("", rt)
	rt.SetGlobalVariables(moruntime.InternalSQLExecutor, executor.NewMemExecutor(func(sql string) (executor.Result, error) {
		return executor.Result{}, nil
	}))
	// should pass
	sqls := []string{
		"create database db_name",               //db not exists and pass
		"create database if not exists db_name", //db not exists but pass
		"create database if not exists tpch",    //db exists and pass
		"drop database if exists db_name",       //db not exists but pass
		"drop database tpch",                    //db exists, pass
		"create view v1 as select * from nation",

		"create table tbl_name (t bool(20) comment 'dd', b int unsigned, c char(20), d varchar(20), primary key(b), index idx_t(c)) comment 'test comment'",
		"create table if not exists tbl_name (b int default 20 primary key, c char(20) default 'ss', d varchar(20) default 'kkk')",
		"create table if not exists nation (t bool(20), b int, c char(20), d varchar(20))",
		"drop table if exists tbl_name",
		"drop table if exists nation",
		"drop table if exists tpch.tbl_not_exist, tpch.tbl_not_exist2",
		"drop table nation",
		"drop table tpch.nation",
		"drop table if exists tpch.tbl_not_exist",
		"drop table if exists db_not_exist.tbl",
		"drop view v1",
		"truncate nation",
		"truncate tpch.nation",
		"truncate table nation",
		"truncate table tpch.nation",
		"create unique index idx_name on nation(n_regionkey)",
		"create view v_nation as select n_nationkey,n_name,n_regionkey,n_comment from nation",
		"CREATE TABLE t1(id INT PRIMARY KEY,name VARCHAR(25),deptId INT,CONSTRAINT fk_t1 FOREIGN KEY(deptId) REFERENCES nation(n_nationkey)) COMMENT='xxxxx'",
		"create table enum_pk_inline (source enum('ACW', 'BT', 'XS3') primary key, last timestamp not null)",
		"create table enum_pk_table (source enum('ACW', 'BT', 'XS3'), primary key (source))",
		"create table t2(empno int unsigned,ename varchar(15),job varchar(10)) cluster by(empno,ename)",
		"lock tables nation read",
		"lock tables nation write, supplier read",
		"unlock tables",
		"alter table emp drop foreign key fk1",
		"alter table nation add FOREIGN KEY fk_t1(n_nationkey) REFERENCES nation2(n_nationkey)",
	}
	runTestShouldPass(mock, t, sqls, false, false)

	// should error
	sqls = []string{
		// "create database tpch",  // check in pipeline now
		// "drop database db_name", // check in pipeline now
		// "create table nation (t bool(20), b int, c char(20), d varchar(20))",             // check in pipeline now
		"create table nation (b int primary key, c char(20) primary key, d varchar(20))", //Multiple primary key
		"drop table tbl_name",           //table not exists in tpch
		"drop table tpch.tbl_not_exist", //database not exists
		"drop table db_not_exist.tbl",   //table not exists
		"create table t6(empno int unsigned,ename varchar(15) auto_increment) cluster by(empno,ename)",
		//"lock tables t3 read",
		"lock tables t1 read, t1 write",
		"lock tables nation read, nation write",
		"alter table nation drop foreign key fk1", //key not exists
		"alter table nation add FOREIGN KEY fk_t1(col_not_exist) REFERENCES nation2(n_nationkey)",
		"alter table nation add FOREIGN KEY fk_t1(n_nationkey) REFERENCES nation2(col_not_exist)",
	}
	runTestShouldError(mock, t, sqls)
}

func TestShow(t *testing.T) {
	mock := NewMockOptimizer(false)
	// should pass
	sqls := []string{
		"show variables",
		//"show create database tpch",
		"show create table nation",
		"show create table tpch.nation",
		"show databases",
		"show databases like '%d'",
		"show databases where `database` = '11'",
		"show databases where `database` = '11' or `database` = 'ddd'",
		"show tables",
		"show tables from tpch",
		"show tables like '%dd'",
		"show tables from tpch where `tables_in_tpch` = 'aa' or `tables_in_tpch` like '%dd'",
		"show columns from nation",
		"show full columns from nation",
		"show columns from nation from tpch",
		"show full columns from nation from tpch",
		"show columns from nation where `field` like '%ff' or `type` = 1 or `null` = 0",
		"show full columns from nation where `field` like '%ff' or `type` = 1 or `null` = 0",
		"show create view v1",
		"show create table v1",
		"show table_number",
		"show table_number from tpch",
		"show column_number from nation",
		"show config",
		"show index from tpch.nation",
		"show locks",
		"show node list",
		"show grants for ROLE role1",
		"show function status",
		"show function status like '%ff'",
		"show snapshots",
		"show snapshots where SNAPSHOT_NAME = 'snapshot_07'",
		// "show procedure status",
		// "show procedure status like '%ff'",
		"show roles",
		"show roles like '%ff'",
		"show stages",
		"show stages like 'my_stage%'",
		// "show grants",
	}
	runTestShouldPass(mock, t, sqls, false, false)

	// should error
	sqls = []string{
		"show create database db_not_exist",                    //db no exist
		"show create table tpch.nation22",                      //table not exist
		"show create view vvv",                                 //view not exist
		"show databases where d ='a'",                          //Column not exist,  show databases only have one column named 'Database'
		"show databases where `Databaseddddd` = '11'",          //column not exist
		"show tables from tpch22222",                           //database not exist
		"show tables from tpch where Tables_in_tpch222 = 'aa'", //column not exist
		"show columns from nation_ddddd",                       //table not exist
		"show full columns from nation_ddddd",
		"show columns from nation_ddddd from tpch", //table not exist
		"show full columns from nation_ddddd from tpch",
		"show columns from nation where `Field22` like '%ff'", //column not exist
		"show full columns from nation where `Field22` like '%ff'",
		"show index from tpch.dddd",
		"show table_number from tpch222",
		"show column_number from nation222",
	}
	runTestShouldError(mock, t, sqls)
}

func TestResultColumns(t *testing.T) {
	mock := NewMockOptimizer(false)
	getColumns := func(sql string) []*ColDef {
		logicPlan, err := runOneStmt(mock, t, sql)
		if err != nil {
			t.Fatalf("sql %s build plan error:%+v", sql, err)
		}
		return GetResultColumnsFromPlan(logicPlan)
	}

	returnNilSQL := []string{
		"begin",
		"commit",
		"rollback",
		"INSERT NATION VALUES (1, 'NAME1',21, 'COMMENT1'), (2, 'NAME2', 22, 'COMMENT2')",
		// "UPDATE NATION SET N_NAME ='U1', N_REGIONKEY=2",
		// "DELETE FROM NATION",
		//"create database db_name",
		//"drop database tpch",
		//"create table tbl_name (b int unsigned, c char(20))",
		//"drop table nation",
	}
	for _, sql := range returnNilSQL {
		columns := getColumns(sql)
		if columns != nil {
			t.Fatalf("sql:%+v, return columns should be nil", sql)
		}
	}

	returnColumnsSQL := map[string]string{
		"SELECT N_NAME, N_REGIONKEY a FROM NATION WHERE N_REGIONKEY > 0 ORDER BY a DESC":            "N_NAME,a",
		"select n_nationkey, sum(n_regionkey) from (select * from nation) sub group by n_nationkey": "n_nationkey,sum(n_regionkey)",
		"show variables":            "Variable_name,Value",
		"show create database tpch": "Database,Create Database",
		"show create table nation":  "Table,Create Table",
		"show databases":            "Database",
		"show tables":               "Tables_in_tpch",
		"show columns from nation":  "Field,Type,Null,Key,Default,Extra,Comment",
	}
	for sql, colsStr := range returnColumnsSQL {
		cols := strings.Split(colsStr, ",")
		columns := getColumns(sql)
		if len(columns) != len(cols) {
			t.Fatalf("sql:%+v, return columns should be [%s]", sql, colsStr)
		}
		for idx, col := range cols {
			// now ast always change col_name to lower string. will be fixed soon
			if !strings.EqualFold(columns[idx].Name, col) {
				t.Fatalf("sql:%+v, return columns should be [%s]", sql, colsStr)
			}
		}
	}
}

func TestResultColumns2(t *testing.T) {
	mock := NewMockOptimizer(true)
	getColumns := func(sql string) []*ColDef {
		logicPlan, err := runOneStmt(mock, t, sql)
		if err != nil {
			t.Fatalf("sql %s build plan error:%+v", sql, err)
		}
		return GetResultColumnsFromPlan(logicPlan)
	}

	returnNilSQL := []string{
		"create database db_name",
		"drop database tpch",
		"create table tbl_name (b int unsigned, c char(20))",
		"drop table nation",
	}
	for _, sql := range returnNilSQL {
		columns := getColumns(sql)
		if columns != nil {
			t.Fatalf("sql:%+v, return columns should be nil", sql)
		}
	}

	returnColumnsSQL := map[string]string{
		"SELECT N_NAME, N_REGIONKEY a FROM NATION WHERE N_REGIONKEY > 0 ORDER BY a DESC":            "N_NAME,a",
		"select n_nationkey, sum(n_regionkey) from (select * from nation) sub group by n_nationkey": "n_nationkey,sum(n_regionkey)",
		"show variables":            "Variable_name,Value",
		"show create database tpch": "Database,Create Database",
		"show create table nation":  "Table,Create Table",
		"show databases":            "Database",
		"show tables":               "Tables_in_tpch",
		"show columns from nation":  "Field,Type,Null,Key,Default,Extra,Comment",
	}
	for sql, colsStr := range returnColumnsSQL {
		cols := strings.Split(colsStr, ",")
		columns := getColumns(sql)
		if len(columns) != len(cols) {
			t.Fatalf("sql:%+v, return columns should be [%s]", sql, colsStr)
		}
		for idx, col := range cols {
			// now ast always change col_name to lower string. will be fixed soon
			if !strings.EqualFold(columns[idx].Name, col) {
				t.Fatalf("sql:%+v, return columns should be [%s]", sql, colsStr)
			}
		}
	}
}

func TestBuildUnnest(t *testing.T) {
	mock := NewMockOptimizer(false)
	sqls := []string{
		`select * from unnest('{"a":1}') as f`,
		`select * from unnest('{"a":1}', '') as f`,
		`select * from unnest('{"a":1}', '$', true) as f`,
	}
	runTestShouldPass(mock, t, sqls, false, false)
	errSqls := []string{
		`select * from unnest(t.t1.a)`,
		`select * from unnest(t.a, "$.b")`,
		`select * from unnest(t.a, "$.b", true)`,
		`select * from unnest(t.a) as f`,
		`select * from unnest(t.a, "$.b") as f`,
		`select * from unnest(t.a, "$.b", true) as f`,
		`select * from unnest('{"a":1}')`,
		`select * from unnest('{"a":1}', "$")`,
		`select * from unnest('{"a":1}', "", true)`,
	}
	runTestShouldError(mock, t, errSqls)
}

func TestVisitRule(t *testing.T) {
	sql := "select * from nation where n_nationkey > 10 or n_nationkey=@int_var or abs(-1) > 1"
	mock := NewMockOptimizer(false)
	ctx := context.TODO()
	plan, err := runOneStmt(mock, t, sql)
	if err != nil {
		t.Fatalf("should not error, sql=%s", sql)
	}
	getParamRule := NewGetParamRule()
	vp := NewVisitPlan(plan, []VisitPlanRule{getParamRule})
	err = vp.Visit(context.TODO())
	if err != nil {
		t.Fatalf("should not error, sql=%s", sql)
	}
	getParamRule.SetParamOrder()
	args := getParamRule.params

	resetParamOrderRule := NewResetParamOrderRule(args)
	vp = NewVisitPlan(plan, []VisitPlanRule{resetParamOrderRule})
	err = vp.Visit(ctx)
	if err != nil {
		t.Fatalf("should not error, sql=%s", sql)
	}

	params := []*Expr{
		makePlan2Int64ConstExprWithType(10),
	}
	resetParamRule := NewResetParamRefRule(ctx, params)
	vp = NewVisitPlan(plan, []VisitPlanRule{resetParamRule})
	err = vp.Visit(ctx)
	if err != nil {
		t.Fatalf("should not error, sql=%s", sql)
	}
}

func TestVisitRule2(t *testing.T) {
	sql := "select * from nation where n_nationkey > 10"
	mock := NewMockOptimizer(false)
	ctx := context.TODO()
	queryPlan, err := runOneStmt(mock, t, sql)
	if err != nil {
		t.Fatalf("should not error, sql=%s", sql)
	}
	getParamRule := NewGetParamRule()
	vp := NewVisitPlan(queryPlan, []VisitPlanRule{getParamRule})
	err = vp.Visit(context.TODO())
	if err != nil {
		t.Fatalf("should not error, sql=%s", sql)
	}
	getParamRule.SetParamOrder()
	args := getParamRule.params

	resetParamOrderRule := NewResetParamOrderRule(args)
	vp = NewVisitPlan(queryPlan, []VisitPlanRule{resetParamOrderRule})
	err = vp.Visit(ctx)
	if err != nil {
		t.Fatalf("should not error, sql=%s", sql)
	}

	if qry, ok := queryPlan.Plan.(*Plan_Query); ok {
		if f, ok := qry.Query.Nodes[1].FilterList[0].Expr.(*plan.Expr_F); ok {
			f.F.Args[1] = &plan.Expr{
				Typ: plan.Type{
					Id:          int32(types.T_int64),
					NotNullable: true,
				},
				Expr: &plan.Expr_P{
					P: &plan.ParamRef{
						Pos: 1,
					},
				},
			}
		}

	}
	params := []*Expr{
		makePlan2Int64ConstExprWithType(10),
	}
	resetParamRule := NewResetParamRefRule(ctx, params)
	vp = NewVisitPlan(queryPlan, []VisitPlanRule{resetParamRule})
	err = vp.Visit(ctx)
	if err == nil {
		t.Fatalf("param 1 not exist, should error")
	}
}

func getJSON(v any, t *testing.T) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		t.Logf("%+v", v)
	}
	var out bytes.Buffer
	err = json.Indent(&out, b, "", "  ")
	if err != nil {
		t.Logf("%+v", v)
	}
	return out.Bytes()
}

func testDeepCopy(logicPlan *Plan) {
	switch logicPlan.Plan.(type) {
	case *plan.Plan_Query:
		_ = DeepCopyPlan(logicPlan)
	case *plan.Plan_Ddl:
		_ = DeepCopyPlan(logicPlan)
	case *plan.Plan_Dcl:
	}
}

func outPutPlan(logicPlan *Plan, toFile bool, t *testing.T) {
	var json []byte
	switch logicPlan.Plan.(type) {
	case *plan.Plan_Query:
		json = getJSON(logicPlan.GetQuery(), t)
	case *plan.Plan_Tcl:
		json = getJSON(logicPlan.GetTcl(), t)
	case *plan.Plan_Ddl:
		json = getJSON(logicPlan.GetDdl(), t)
	case *plan.Plan_Dcl:
		json = getJSON(logicPlan.GetDcl(), t)
	}
	if toFile {
		err := os.WriteFile("/tmp/mo_plan_test.json", json, 0777)
		if err != nil {
			t.Logf("%+v", err)
		}
	} else {
		t.Log(string(json))
	}
}

func runOneStmt(opt Optimizer, t *testing.T, sql string) (*Plan, error) {
	stmts, err := mysql.Parse(opt.CurrentContext().GetContext(), sql, 1)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	// this sql always return one stmt
	ctx := opt.CurrentContext()
	stmt := stmts[0]
	// BuildPlan materializes the plan and does not retain the parser AST. Free
	// it as soon as the plan has been built; runOneStmt is used by thousands of
	// planner tests and retaining every AST until the package test exits can
	// exhaust the coverage runner's memory budget.
	defer stmt.Free()
	return BuildPlan(ctx, stmt, false)
}

func runTestShouldPass(opt Optimizer, t *testing.T, sqls []string, printJSON bool, toFile bool) {
	for _, sql := range sqls {
		logicPlan, err := runOneStmt(opt, t, sql)
		if err != nil {
			t.Fatalf("%+v, sql=%v", err, sql)
		}
		testDeepCopy(logicPlan)
		if printJSON {
			outPutPlan(logicPlan, toFile, t)
		}
	}
}

func runTestShouldError(opt Optimizer, t *testing.T, sqls []string) {
	for _, sql := range sqls {
		_, err := runOneStmt(opt, t, sql)
		if err == nil {
			t.Fatalf("should error, but pass: %v", sql)
		}
	}
}

func Test_mergeContexts(t *testing.T) {
	b1 := NewBinding(0, 1, "db", "a", 0, nil, nil, nil, false, nil)
	bc1 := NewBindContext(nil, nil)
	bc1.bindings = append(bc1.bindings, b1)

	b2 := NewBinding(1, 2, "db", "a", 0, nil, nil, nil, false, nil)
	bc2 := NewBindContext(nil, nil)
	bc2.bindings = append(bc2.bindings, b2)

	bc := NewBindContext(nil, nil)

	//a merge a
	err := bc.mergeContexts(context.Background(), bc1, bc2)
	assert.Error(t, err)
	assert.EqualError(t, err, "invalid input: table 'a' specified more than once")

	//a merge b
	b3 := NewBinding(2, 3, "db", "b", 0, nil, nil, nil, false, nil)
	bc3 := NewBindContext(nil, nil)
	bc3.bindings = append(bc3.bindings, b3)

	err = bc.mergeContexts(context.Background(), bc1, bc3)
	assert.NoError(t, err)

	// a merge a, ctx is  nil
	var ctx context.Context
	err = bc.mergeContexts(ctx, bc1, bc2)
	assert.Error(t, err)
	assert.EqualError(t, err, "invalid input: table 'a' specified more than once")
}

func Test_limitUint64(t *testing.T) {
	sqls := []string{
		"select * from t1 limit 0, 18446744073709551615",
		"select * from t1 limit 18446744073709551615, 18446744073709551615",
		"SELECT IFNULL(CAST(@var AS BIGINT UNSIGNED), 1)",
	}
	testutil.NewProc(t)
	mock := NewMockOptimizer(false)

	for _, sql := range sqls {
		logicPlan, err := runOneStmt(mock, t, sql)
		if err != nil {
			t.Fatalf("%+v", err)
		}
		outPutPlan(logicPlan, true, t)
	}
}

// test canDeleteRewriteToTruncate
func Test_bind_delete(t *testing.T) {
	ctx := context.TODO()
	ctrl := gomock.NewController(t)
	compileCtx := NewMockCompilerContext2(ctrl)
	compileCtx.EXPECT().ResolveVariable(gomock.Any(), gomock.Any(), gomock.Any()).Return("", nil).AnyTimes()
	compileCtx.EXPECT().GetAccountId().Return(catalog.System_Account, moerr.NewInternalError(ctx, "no account id in context")).AnyTimes()
	dmlCtx := &DMLContext{}
	_, err := canDeleteRewriteToTruncate(compileCtx, dmlCtx)
	assert.Error(t, err)
}

// findDedupJoinCaptureList walks the plan looking for the DEDUP JOIN whose
// build side carries the OldColCaptureList — there is at most one in a
// REPLACE plan that took the merged-main-scan path.
func findDedupJoinCaptureList(t *testing.T, query *plan.Query) []plan.OldColCapture {
	t.Helper()
	for _, node := range query.Nodes {
		if node.NodeType != plan.Node_JOIN || node.JoinType != plan.Node_DEDUP {
			continue
		}
		if node.DedupJoinCtx == nil {
			continue
		}
		if len(node.DedupJoinCtx.OldColCaptureList) > 0 {
			return node.DedupJoinCtx.OldColCaptureList
		}
	}
	return nil
}

// TestReplaceCaptureListNarrowed pins the merged-main-scan capture list to
// exactly the columns MULTI_UPDATE actually consumes — Row_ID + PK + (per
// non-serialized index, the leading part column). If the narrowing in
// appendDedupAndMultiUpdateNodesForBindReplace regresses to "capture every
// main-table column", this test will catch it.
//
// We assert by total count rather than by exact ColPos, because the build-side
// projection list may have leading slots prepended before main-table cols
// (cluster keys, etc.) — so absolute positions are layout-sensitive but the
// count formula is not.
func TestReplaceCaptureListNarrowed(t *testing.T) {
	mock := NewMockOptimizer(true)

	// self_ref: id PK + parent_id + name + Row_ID; zero indexes.
	// requiredOldCols = {Row_ID, id} ⇒ capture list length == 2.
	// Pre-narrowing the planner emitted one capture per main-table column
	// (length == 4), which is the regression this test guards against.
	logicPlan, err := runOneStmt(mock, t,
		"REPLACE INTO self_ref VALUES (1, NULL, 'root')")
	if err != nil {
		t.Fatalf("%+v", err)
	}
	query := logicPlan.GetQuery()
	assert.NotNil(t, query)

	captureList := findDedupJoinCaptureList(t, query)
	assert.NotNil(t, captureList,
		"self_ref REPLACE should take the merged-main-scan capture path")

	const totalCols = 4 // 3 user cols + Row_ID
	assert.Less(t, len(captureList), totalCols,
		"capture list must be narrower than the full main-table col set")
	assert.Len(t, captureList, 2,
		"expected Row_ID + PK only; if this changes the formula 1+1+#single_part_idx is wrong")

	relPos := captureList[0].BuildPlaceholder.RelPos
	seen := map[int32]bool{}
	for _, c := range captureList {
		assert.Equal(t, relPos, c.BuildPlaceholder.RelPos,
			"all captures must share one build-side bind tag")
		assert.False(t, seen[c.BuildPlaceholder.ColPos],
			"capture positions must be distinct, got duplicate ColPos=%d",
			c.BuildPlaceholder.ColPos)
		seen[c.BuildPlaceholder.ColPos] = true
	}
}

// TestReplaceCaptureList_NotEmittedWhenMergedScanDisabled documents the
// negative side: tables that fail the useMergedMainScan guard
// (fake PK or any multi-part index) must produce an empty capture list. This
// guards against accidentally enabling capture on a path the optimizer
// can't yet feed correctly.
func TestReplaceCaptureList_NotEmittedWhenMergedScanDisabled(t *testing.T) {
	mock := NewMockOptimizer(true)

	cases := []struct {
		name string
		sql  string
		why  string
	}{
		{
			name: "dept_has_multi_part_idx",
			sql:  "REPLACE INTO dept VALUES (1, 'Sales', 'NY')",
			why:  "dept has a (loc, dname) index → hasMultiPartIdx=true",
		},
		{
			name: "fake_pk_t",
			sql:  "REPLACE INTO fake_pk_t VALUES (1, 'hello')",
			why:  "fake_pk_t has no real PK → isFakePK=true",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			logicPlan, err := runOneStmt(mock, t, c.sql)
			if err != nil {
				t.Fatalf("%+v", err)
			}
			query := logicPlan.GetQuery()
			assert.NotNil(t, query)
			assert.Nil(t, findDedupJoinCaptureList(t, query),
				"%s: %s, no DEDUP JOIN should carry a capture list", c.name, c.why)
		})
	}
}

func TestReplaceCaptureDedupJoinDoesNotShuffle(t *testing.T) {
	mock := NewMockOptimizer(true)
	logicPlan, err := runOneStmt(mock, t,
		"REPLACE INTO self_ref VALUES (1, NULL, 'root')")
	if err != nil {
		t.Fatalf("%+v", err)
	}
	query := logicPlan.GetQuery()
	assert.NotNil(t, query)

	for _, node := range query.Nodes {
		if node.NodeType != plan.Node_JOIN || node.JoinType != plan.Node_DEDUP {
			continue
		}
		if node.DedupJoinCtx == nil || len(node.DedupJoinCtx.OldColCaptureList) == 0 {
			continue
		}

		rightChild := query.Nodes[node.Children[1]]
		rightChild.Stats.Outcnt = 320001
		node.Stats = DefaultStats()

		builder := &QueryBuilder{qry: query}
		determineShuffleForJoin(node, builder)

		assert.False(t, node.Stats.HashmapStats.Shuffle)
		assert.Equal(t, int32(-1), node.Stats.HashmapStats.ShuffleColIdx)
		return
	}

	t.Fatal("expected REPLACE plan to contain a DEDUP JOIN with OldColCaptureList")
}

// A multi-column row subquery as a COUNT(DISTINCT ...) argument binds to an
// Expr_Sub whose Typ.Id is T_tuple. The tuple-expansion guard in BindAggFunc
// must not mistake it for a genuine Expr_List (GetList() returns nil there, so
// the earlier code nil-deref panicked). It must instead reject the query with a
// clear error rather than silently collapsing to the subquery's first column.
func TestCountDistinctRowSubqueryRejected(t *testing.T) {
	mock := NewMockOptimizer(false)
	var (
		plan *Plan
		err  error
	)
	require.NotPanics(t, func() {
		plan, err = runOneStmt(mock, t,
			"select count(distinct (select n_nationkey, n_regionkey from nation)) from nation")
	})
	require.Error(t, err)
	require.Nil(t, plan)
	require.Contains(t, err.Error(), "multi-column subquery")
}

func TestSubqueryInJoinOn(t *testing.T) {
	mock := NewMockOptimizer(false)
	sqls := []string{
		"SELECT a.n_nationkey FROM nation a JOIN nation b ON b.n_nationkey = (SELECT MAX(z.n_nationkey) FROM nation z WHERE z.n_regionkey = a.n_regionkey)",
		"SELECT n_name FROM nation JOIN region ON r_regionkey = (SELECT MAX(r_regionkey) FROM region)",
		"SELECT n_name FROM nation JOIN region ON n_regionkey = r_regionkey AND r_regionkey = (SELECT MAX(r_regionkey) FROM region)",
	}

	for _, sql := range sqls {
		logicPlan, err := runOneStmt(mock, t, sql)
		require.NoError(t, err, sql)

		foundFilter := false
		foundJoinCondition := false
		for _, node := range logicPlan.GetQuery().Nodes {
			if node.NodeType == plan.Node_FILTER {
				foundFilter = true
			}
			for _, expr := range node.OnList {
				foundJoinCondition = true
				require.False(t, hasSubquery(expr), "JOIN OnList contains an executable Expr_Sub: %s", sql)
			}
			for _, expr := range node.FilterList {
				require.False(t, hasSubquery(expr), "FILTER contains an executable Expr_Sub: %s", sql)
			}
		}
		require.True(t, foundFilter, "subquery predicate was not lowered to a FILTER: %s", sql)
		if strings.Contains(sql, "n_regionkey = r_regionkey AND") {
			require.True(t, foundJoinCondition, "ordinary ON predicate was removed from the JOIN: %s", sql)
		}
	}
}

func TestSubqueryInOuterJoinOn(t *testing.T) {
	mock := NewMockOptimizer(false)
	tests := []struct {
		name     string
		sql      string
		wantMark bool
		wantSemi bool
	}{
		{
			name: "left join correlated scalar on preserved input",
			sql: "SELECT a.n_nationkey, b.n_nationkey FROM nation a LEFT JOIN nation b " +
				"ON a.n_regionkey = b.n_regionkey " +
				"AND b.n_nationkey = (SELECT MAX(z.n_nationkey) FROM nation z WHERE z.n_regionkey = a.n_regionkey)",
		},
		{
			name: "right join correlated scalar on preserved input",
			sql: "SELECT a.n_nationkey, b.n_nationkey FROM nation a RIGHT JOIN nation b " +
				"ON a.n_regionkey = b.n_regionkey " +
				"AND a.n_nationkey = (SELECT MIN(z.n_nationkey) FROM nation z WHERE z.n_regionkey = b.n_regionkey)",
		},
		{
			name: "left join uncorrelated scalar",
			sql: "SELECT a.n_nationkey, b.n_nationkey FROM nation a LEFT JOIN nation b " +
				"ON b.n_nationkey = (SELECT MAX(z.n_nationkey) FROM nation z)",
		},
		{
			name: "left join correlated exists on preserved input",
			sql: "SELECT a.n_nationkey, b.n_nationkey FROM nation a LEFT JOIN nation b " +
				"ON a.n_regionkey = b.n_regionkey " +
				"AND EXISTS (SELECT 1 FROM region z WHERE z.r_regionkey = a.n_regionkey)",
			wantMark: true,
		},
		{
			name: "left join correlated exists on nullable input",
			sql: "SELECT a.n_nationkey, b.n_nationkey FROM nation a LEFT JOIN nation b " +
				"ON a.n_regionkey = b.n_regionkey " +
				"AND EXISTS (SELECT 1 FROM region z WHERE z.r_regionkey = b.n_regionkey)",
			wantSemi: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logicPlan, err := runOneStmt(mock, t, tt.sql)
			require.NoError(t, err)

			query := logicPlan.GetQuery()
			foundOuterJoin := false
			foundMarkJoin := false
			foundSemiJoin := false
			joinCount := 0
			for _, node := range query.Nodes {
				if node.NodeType == plan.Node_JOIN {
					joinCount++
					if node.JoinType == plan.Node_LEFT && len(node.OnList) > 0 {
						foundOuterJoin = true
					}
					if node.JoinType == plan.Node_MARK {
						foundMarkJoin = true
					}
					if node.JoinType == plan.Node_SEMI {
						foundSemiJoin = true
					}
				}
				for _, expr := range node.OnList {
					require.False(t, hasSubquery(expr), "JOIN OnList contains Expr_Sub")
					require.False(t, hasCorrCol(expr), "JOIN OnList contains CorrColRef")
				}
				for _, expr := range node.FilterList {
					require.False(t, hasSubquery(expr), "FILTER contains Expr_Sub")
					require.False(t, hasCorrCol(expr), "FILTER contains CorrColRef")
				}
			}

			require.True(t, foundOuterJoin, "outer join disappeared from plan")
			require.GreaterOrEqual(t, joinCount, 2, "subquery was not lowered below the outer join")
			require.Equal(t, tt.wantMark, foundMarkJoin)
			require.Equal(t, tt.wantSemi, foundSemiJoin)
		})
	}

	_, err := runOneStmt(mock, t,
		"SELECT a.n_nationkey FROM nation a LEFT JOIN nation b ON EXISTS ("+
			"SELECT 1 FROM region z WHERE z.r_regionkey = a.n_regionkey AND z.r_regionkey = b.n_regionkey)")
	require.ErrorContains(t, err, "referencing both join inputs")

	_, err = runOneStmt(mock, t,
		"SELECT outer_n.n_nationkey FROM nation outer_n WHERE EXISTS ("+
			"SELECT 1 FROM nation a LEFT JOIN nation b ON EXISTS ("+
			"SELECT 1 FROM region z WHERE z.r_regionkey = outer_n.n_regionkey))")
	require.ErrorContains(t, err, "deeply correlated subquery")
}
func TestSamePhysicalTargetAliasesShareMergedFinalRows(t *testing.T) {
	for _, sql := range []string{
		"UPDATE nation a JOIN nation b ON a.n_nationkey = b.n_nationkey " +
			"SET a.n_name = 'a', b.n_comment = 'b'",
		"UPDATE nation a JOIN nation b ON a.n_nationkey <> b.n_nationkey " +
			"SET a.n_name = 'a', b.n_comment = 'b'",
		"UPDATE nation a JOIN nation b ON a.n_nationkey <> b.n_nationkey " +
			"JOIN nation2 n2 ON n2.n_nationkey = a.n_nationkey " +
			"SET a.n_name = 'a', b.n_comment = 'b', n2.n_name = 'n2'",
	} {
		mock := NewMockOptimizer(true)
		logicPlan, err := runOneStmt(mock, t, sql)
		require.NoError(t, err, sql)

		query := logicPlan.GetQuery()
		var multiUpdate *plan.Node
		mainContexts := 0
		hasUnionAll := false
		hasAggregate := false
		for _, node := range query.Nodes {
			switch node.NodeType {
			case plan.Node_MULTI_UPDATE:
				multiUpdate = node
			case plan.Node_UNION_ALL:
				hasUnionAll = true
			case plan.Node_AGG:
				hasAggregate = true
			}
		}

		require.NotNil(t, multiUpdate)
		require.True(t, hasUnionAll)
		require.True(t, hasAggregate)
		var tableID uint64
		for _, updateCtx := range multiUpdate.UpdateCtxList {
			if updateCtx.TableDef == nil || updateCtx.TableDef.Name != "nation" {
				continue
			}
			mainContexts++
			require.True(t, updateCtx.DedupByTargetRowId)
			require.Len(t, updateCtx.DeleteCols, 4)
			require.Len(t, updateCtx.AffectedRowsCols, 2)
			physicalActivePos := updateCtx.DeleteCols[3].ColPos
			for _, semanticSelector := range updateCtx.AffectedRowsCols {
				require.NotEqual(t, semanticSelector.ColPos, physicalActivePos,
					"repeated aliases must write through the group OR, not one alias selector")
			}
			if tableID == 0 {
				tableID = updateCtx.TableDef.TblId
			} else {
				require.Equal(t, tableID, updateCtx.TableDef.TblId)
			}
		}
		require.Equal(t, 1, mainContexts)
		if strings.Contains(sql, "nation2") {
			require.Len(t, multiUpdate.UpdateCtxList, 2)
		}
	}
}

func TestModernMultiTargetGeneratedColumns(t *testing.T) {
	mock := NewMockOptimizer(true)
	setMockGeneratedColumn(t, mock, "emp", "ename", "job")
	setMockGeneratedColumn(t, mock, "dept", "dname", "loc")

	logicPlan, err := runOneStmt(mock, t,
		"UPDATE emp, dept SET emp.job = dept.loc, dept.loc = emp.job WHERE emp.deptno = dept.deptno")
	require.NoError(t, err)

	multiUpdates := 0
	for _, node := range logicPlan.GetQuery().Nodes {
		if node.NodeType == plan.Node_MULTI_UPDATE {
			multiUpdates++
		}
	}
	require.Equal(t, 1, multiUpdates)
}

func TestUpdateIgnoreChecksRepeatedPhysicalAliasesBeforeFinalRowMerge(t *testing.T) {
	mock := NewMockOptimizer(true)
	tests := []struct {
		name string
		sql  string
	}{
		{
			name: "two aliases",
			sql: "UPDATE IGNORE dept a JOIN dept b ON a.deptno = b.deptno " +
				"SET a.dname = 'conflict', b.loc = 'safe'",
		},
		{
			name: "conflict alias follows safe owner",
			sql: "UPDATE IGNORE dept a JOIN dept b ON a.deptno = b.deptno " +
				"SET a.loc = 'safe', b.dname = 'conflict'",
		},
		{
			name: "three aliases",
			sql: "UPDATE IGNORE dept a JOIN dept b ON a.deptno = b.deptno " +
				"JOIN dept c ON b.deptno = c.deptno " +
				"SET a.dname = 'conflict', b.loc = 'safe-b', c.loc = 'safe-c'",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			logicPlan, err := runOneStmt(mock, t, test.sql)
			require.NoError(t, err)
			query := logicPlan.GetQuery()

			var ignoreDedupIDs []int32
			for nodeID, node := range query.Nodes {
				if node.NodeType == plan.Node_JOIN && node.JoinType == plan.Node_DEDUP &&
					node.OnDuplicateAction == plan.Node_IGNORE {
					ignoreDedupIDs = append(ignoreDedupIDs, int32(nodeID))
				}
			}
			require.NotEmpty(t, ignoreDedupIDs)

			finalMergeAfterIgnore := false
			for nodeID, node := range query.Nodes {
				if node.NodeType != plan.Node_AGG {
					continue
				}
				for _, dedupID := range ignoreDedupIDs {
					if planNodeDependsOn(query, int32(nodeID), dedupID, make(map[int32]struct{})) {
						finalMergeAfterIgnore = true
						break
					}
				}
			}
			require.True(t, finalMergeAfterIgnore,
				"repeated physical aliases must pass alias-level IGNORE checks before RowID merge")
		})
	}
}

func TestUpdateIgnoreRecomputesGeneratedColumnsForRepeatedPhysicalCandidates(t *testing.T) {
	mock := NewMockOptimizer(true)
	setMockGeneratedColumn(t, mock, "dept", "dname", "loc")

	logicPlan, err := runOneStmt(mock, t,
		"UPDATE IGNORE dept a JOIN dept b ON a.deptno = b.deptno "+
			"SET a.loc = 'first', b.loc = 'second'")
	require.NoError(t, err)

	multiUpdates := 0
	for _, node := range logicPlan.GetQuery().Nodes {
		if node.NodeType == plan.Node_MULTI_UPDATE {
			multiUpdates++
		}
	}
	require.Equal(t, 1, multiUpdates)
}

func buildRepeatedAliasUpdateSQL(aliasCount int) string {
	var from strings.Builder
	from.WriteString("nation a0")
	for i := 1; i < aliasCount; i++ {
		fmt.Fprintf(&from, " join nation a%d on a0.n_nationkey=a%d.n_nationkey", i, i)
	}
	assignments := make([]string, aliasCount)
	for i := range assignments {
		assignments[i] = fmt.Sprintf("a%d.n_comment='v%d'", i, i)
	}
	return "update ignore " + from.String() + " set " + strings.Join(assignments, ",")
}

func TestUpdateIgnoreRepeatedAliasPlanningSharesOneMergeAggregate(t *testing.T) {
	const childEnv = "MO_UPDATE_IGNORE_ALIAS_STRESS_CHILD"
	if os.Getenv(childEnv) == "" {
		cmd := exec.CommandContext(t.Context(), os.Args[0],
			"-test.run=^TestUpdateIgnoreRepeatedAliasPlanningSharesOneMergeAggregate$",
			"-test.count=1")
		cmd.Env = append(os.Environ(), childEnv+"=1")
		output, err := cmd.CombinedOutput()
		require.NoError(t, err, string(output))
		return
	}

	for _, aliasCount := range []int{8, 16, 24} {
		t.Run(fmt.Sprintf("%d aliases", aliasCount), func(t *testing.T) {
			mock := NewMockOptimizer(true)
			logicPlan, err := runOneStmt(mock, t, buildRepeatedAliasUpdateSQL(aliasCount))
			require.NoError(t, err)
			aggregates := 0
			for _, node := range logicPlan.GetQuery().Nodes {
				if node.NodeType == plan.Node_AGG {
					aggregates++
				}
			}
			require.Equal(t, 1, aggregates,
				"every greedy stage must reuse the same physical-row contribution aggregate")
			require.Less(t, len(logicPlan.GetQuery().Nodes), 40*aliasCount,
				"greedy candidate/fallback stages must remain linear in the alias count")
		})
	}
}

func TestUpdateIgnoreRepeatedAliasPlanningObservesCancellation(t *testing.T) {
	const aliasCount = 24

	t.Run("filter pushdown stops after in-flight cancellation", func(t *testing.T) {
		stmt, err := mysql.ParseOne(t.Context(), buildRepeatedAliasUpdateSQL(aliasCount), 1)
		require.NoError(t, err)
		defer stmt.Free()
		mock := NewMockOptimizer(true)
		builder := NewQueryBuilder(plan.Query_UPDATE, mock.CurrentContext(), false, true)
		rootID, bindErr := builder.bindUpdate(stmt.(*tree.Update), NewBindContext(builder, nil))
		require.NoError(t, bindErr)

		cancelCtx, cancel := context.WithCancel(t.Context())
		builder.compCtx = &cancelAfterGetContextCompilerContext{
			CompilerContext: builder.compCtx,
			ctx:             cancelCtx,
			cancel:          cancel,
			remaining:       8,
		}
		builder.pushdownFilters(rootID, nil, false)
		require.ErrorIs(t, builder.checkPlanningCanceled(), context.Canceled)
		require.NotEmpty(t, builder.optimizationHistory)
	})

	t.Run("create query returns cancellation", func(t *testing.T) {
		stmt, err := mysql.ParseOne(t.Context(), buildRepeatedAliasUpdateSQL(aliasCount), 1)
		require.NoError(t, err)
		defer stmt.Free()
		mock := NewMockOptimizer(true)
		builder := NewQueryBuilder(plan.Query_UPDATE, mock.CurrentContext(), false, true)
		rootID, bindErr := builder.bindUpdate(stmt.(*tree.Update), NewBindContext(builder, nil))
		require.NoError(t, bindErr)
		builder.qry.Steps = append(builder.qry.Steps, rootID)

		canceledCtx, cancel := context.WithCancel(t.Context())
		cancel()
		mock.ctxt.SetContext(canceledCtx)
		_, createErr := builder.createQuery()
		require.ErrorIs(t, createErr, context.Canceled)
	})
}

func planNodeDependsOn(query *plan.Query, nodeID, dependencyID int32, visited map[int32]struct{}) bool {
	if nodeID == dependencyID {
		return true
	}
	if nodeID < 0 || int(nodeID) >= len(query.Nodes) {
		return false
	}
	if _, ok := visited[nodeID]; ok {
		return false
	}
	visited[nodeID] = struct{}{}
	for _, childID := range query.Nodes[nodeID].Children {
		if planNodeDependsOn(query, childID, dependencyID, visited) {
			return true
		}
	}
	for _, sourceStep := range query.Nodes[nodeID].SourceStep {
		if sourceStep < 0 || int(sourceStep) >= len(query.Steps) {
			continue
		}
		if planNodeDependsOn(query, query.Steps[sourceStep], dependencyID, visited) {
			return true
		}
	}
	return false
}
