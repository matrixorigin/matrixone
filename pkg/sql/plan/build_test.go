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
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

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

func planHasTextToVarcharStrictCastWithWidth(p *Plan, width int32) bool {
	return planHasTextToVarcharCastWithNameAndWidth(p, "cast_strict", width)
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
				nameMatches = f.Func.GetObjName() == "cast" || f.Func.GetObjName() == "cast_strict"
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

func TestUpdateTextConcatCoalesceKeepsTextAssignmentCast(t *testing.T) {
	mock := NewMockOptimizer(true)
	addTextCastTableForTest(mock)

	logicPlan, err := runOneStmt(mock, t, "update text_cast_t set txt = concat(coalesce(txt, ''), ' suffix') where id = 1")
	assert.NoError(t, err)
	assert.False(t, planHasTextToCharOrVarcharCast(logicPlan))
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

func TestInsertSelectVarcharFromTextUsesStrictAssignmentCast(t *testing.T) {
	mock := NewMockOptimizer(true)
	addTextCastTableForTest(mock)

	logicPlan, err := runOneStmt(mock, t, "insert into text_cast_t(id, vc) select id, txt from text_cast_t")
	assert.NoError(t, err)
	assert.True(t, planHasTextToVarcharStrictCastWithWidth(logicPlan, 255))
}

func TestOnDuplicateUpdateVarcharFromTextUsesStrictAssignmentCast(t *testing.T) {
	mock := NewMockOptimizer(true)
	addTextCastTableForTest(mock)

	logicPlan, err := runOneStmt(mock, t, "insert into text_cast_t(id, txt, vc) values (1, repeat('a', 260), '') on duplicate key update vc = txt")
	assert.NoError(t, err)
	assert.True(t, planHasTextToVarcharStrictCastWithWidth(logicPlan, 255))
}

//Test Query Node Tree
// func TestNodeTree(t *testing.T) {
// 	type queryCheck struct {
// 		steps    []int32                    //steps
// 		nodeType map[int]plan.Node_NodeType //node_type in each node
// 		children map[int][]int32            //children in each node
// 	}

// 	// map[sql string]checkData
// 	nodeTreeCheckList := map[string]queryCheck{
// 		"SELECT -1": {
// 			steps: []int32{0},
// 			nodeType: map[int]plan.Node_NodeType{
// 				0: plan.Node_VALUE_SCAN,
// 			},
// 			children: nil,
// 		},
// 		"SELECT -1 from dual": {
// 			steps: []int32{0},
// 			nodeType: map[int]plan.Node_NodeType{
// 				0: plan.Node_VALUE_SCAN,
// 			},
// 			children: nil,
// 		},
// 		// one node
// 		"SELECT N_NAME FROM NATION WHERE N_REGIONKEY = 3": {
// 			steps: []int32{0},
// 			nodeType: map[int]plan.Node_NodeType{
// 				0: plan.Node_TABLE_SCAN,
// 			},
// 			children: nil,
// 		},
// 		// two nodes- SCAN + SORT
// 		"SELECT N_NAME FROM NATION WHERE N_REGIONKEY = 3 Order By N_REGIONKEY": {
// 			steps: []int32{1},
// 			nodeType: map[int]plan.Node_NodeType{
// 				0: plan.Node_TABLE_SCAN,
// 				1: plan.Node_SORT,
// 			},
// 			children: map[int][]int32{
// 				1: {0},
// 			},
// 		},
// 		// two nodes- SCAN + AGG(group by)
// 		"SELECT N_NAME FROM NATION WHERE N_REGIONKEY = 3 Group By N_NAME": {
// 			steps: []int32{1},
// 			nodeType: map[int]plan.Node_NodeType{
// 				0: plan.Node_TABLE_SCAN,
// 				1: plan.Node_AGG,
// 			},
// 			children: map[int][]int32{
// 				1: {0},
// 			},
// 		},
// 		"select sum(n_nationkey) from nation": {
// 			steps: []int32{1},
// 			nodeType: map[int]plan.Node_NodeType{
// 				0: plan.Node_TABLE_SCAN,
// 				1: plan.Node_AGG,
// 			},
// 			children: map[int][]int32{
// 				1: {0},
// 			},
// 		},
// 		"select sum(n_nationkey) from nation order by sum(n_nationkey)": {
// 			steps: []int32{2},
// 			nodeType: map[int]plan.Node_NodeType{
// 				0: plan.Node_TABLE_SCAN,
// 				1: plan.Node_AGG,
// 				2: plan.Node_SORT,
// 			},
// 			children: map[int][]int32{
// 				1: {0},
// 				2: {1},
// 			},
// 		},
// 		// two nodes- SCAN + AGG(distinct)
// 		"SELECT distinct N_NAME FROM NATION": {
// 			steps: []int32{1},
// 			nodeType: map[int]plan.Node_NodeType{
// 				0: plan.Node_TABLE_SCAN,
// 				1: plan.Node_AGG,
// 			},
// 			children: map[int][]int32{
// 				1: {0},
// 			},
// 		},
// 		// three nodes- SCAN + AGG(group by) + SORT
// 		"SELECT N_NAME, count(*) as ttl FROM NATION Group By N_NAME Order By ttl": {
// 			steps: []int32{2},
// 			nodeType: map[int]plan.Node_NodeType{
// 				0: plan.Node_TABLE_SCAN,
// 				1: plan.Node_AGG,
// 				2: plan.Node_SORT,
// 			},
// 			children: map[int][]int32{
// 				1: {0},
// 				2: {1},
// 			},
// 		},
// 		// three nodes - SCAN, SCAN, JOIN
// 		"SELECT N_NAME, N_REGIONKEY FROM NATION join REGION on NATION.N_REGIONKEY = REGION.R_REGIONKEY": {
// 			steps: []int32{3},
// 			nodeType: map[int]plan.Node_NodeType{
// 				0: plan.Node_TABLE_SCAN,
// 				1: plan.Node_TABLE_SCAN,
// 				2: plan.Node_JOIN,
// 				3: plan.Node_PROJECT,
// 			},
// 			children: map[int][]int32{
// 				2: {0, 1},
// 			},
// 		},
// 		// three nodes - SCAN, SCAN, JOIN  //use where for join condition
// 		"SELECT N_NAME, N_REGIONKEY FROM NATION, REGION WHERE NATION.N_REGIONKEY = REGION.R_REGIONKEY": {
// 			steps: []int32{3},
// 			nodeType: map[int]plan.Node_NodeType{
// 				0: plan.Node_TABLE_SCAN,
// 				1: plan.Node_TABLE_SCAN,
// 				2: plan.Node_JOIN,
// 				3: plan.Node_PROJECT,
// 			},
// 			children: map[int][]int32{
// 				2: {0, 1},
// 				3: {2},
// 			},
// 		},
// 		// 5 nodes - SCAN, SCAN, JOIN, SCAN, JOIN  //join three table
// 		"SELECT l.L_ORDERKEY FROM CUSTOMER c, ORDERS o, LINEITEM l WHERE c.C_CUSTKEY = o.O_CUSTKEY and l.L_ORDERKEY = o.O_ORDERKEY and o.O_ORDERKEY < 10": {
// 			steps: []int32{6},
// 			nodeType: map[int]plan.Node_NodeType{
// 				0: plan.Node_TABLE_SCAN,
// 				1: plan.Node_TABLE_SCAN,
// 				2: plan.Node_JOIN,
// 				3: plan.Node_PROJECT,
// 				4: plan.Node_TABLE_SCAN,
// 				5: plan.Node_JOIN,
// 				6: plan.Node_PROJECT,
// 			},
// 			children: map[int][]int32{
// 				2: {0, 1},
// 				3: {2},
// 				5: {3, 4},
// 				6: {5},
// 			},
// 		},
// 		// 6 nodes - SCAN, SCAN, JOIN, SCAN, JOIN, SORT  //join three table
// 		"SELECT l.L_ORDERKEY FROM CUSTOMER c, ORDERS o, LINEITEM l WHERE c.C_CUSTKEY = o.O_CUSTKEY and l.L_ORDERKEY = o.O_ORDERKEY and o.O_ORDERKEY < 10 order by c.C_CUSTKEY": {
// 			steps: []int32{7},
// 			nodeType: map[int]plan.Node_NodeType{
// 				0: plan.Node_TABLE_SCAN,
// 				1: plan.Node_TABLE_SCAN,
// 				2: plan.Node_JOIN,
// 				3: plan.Node_PROJECT,
// 				4: plan.Node_TABLE_SCAN,
// 				5: plan.Node_JOIN,
// 				6: plan.Node_PROJECT,
// 				7: plan.Node_SORT,
// 			},
// 			children: map[int][]int32{
// 				2: {0, 1},
// 				3: {2},
// 				5: {3, 4},
// 				6: {5},
// 				7: {6},
// 			},
// 		},
// 		// 3 nodes  //Derived table
// 		"select c_custkey from (select c_custkey, count(C_NATIONKEY) ff from CUSTOMER group by c_custkey) a where ff > 0": {
// 			steps: []int32{2},
// 			nodeType: map[int]plan.Node_NodeType{
// 				0: plan.Node_TABLE_SCAN,
// 				1: plan.Node_AGG,
// 				2: plan.Node_PROJECT,
// 			},
// 			children: map[int][]int32{
// 				1: {0},
// 				2: {1},
// 			},
// 		},
// 		// 4 nodes  //Derived table
// 		"select c_custkey from (select c_custkey, count(C_NATIONKEY) ff from CUSTOMER group by c_custkey ) a where ff > 0 order by c_custkey": {
// 			steps: []int32{3},
// 			nodeType: map[int]plan.Node_NodeType{
// 				0: plan.Node_TABLE_SCAN,
// 				1: plan.Node_AGG,
// 				2: plan.Node_PROJECT,
// 				3: plan.Node_SORT,
// 			},
// 			children: map[int][]int32{
// 				1: {0},
// 				2: {1},
// 				3: {2},
// 			},
// 		},
// 		// Derived table join normal table
// 		"select c_custkey from (select c_custkey, count(C_NATIONKEY) ff from CUSTOMER group by c_custkey ) a join NATION b on a.c_custkey = b.N_REGIONKEY where b.N_NATIONKEY > 10 order By b.N_REGIONKEY": {
// 			steps: []int32{6},
// 			nodeType: map[int]plan.Node_NodeType{
// 				0: plan.Node_TABLE_SCAN,
// 				1: plan.Node_AGG,
// 				2: plan.Node_PROJECT,
// 				3: plan.Node_TABLE_SCAN,
// 				4: plan.Node_JOIN,
// 				5: plan.Node_PROJECT,
// 				6: plan.Node_SORT,
// 			},
// 			children: map[int][]int32{
// 				1: {0},
// 				2: {1},
// 				4: {2, 3},
// 				5: {4},
// 				6: {5},
// 			},
// 		},
// 		// insert from values
// 		"INSERT NATION (N_NATIONKEY, N_REGIONKEY, N_NAME) VALUES (1, 21, 'NAME1'), (2, 22, 'NAME2')": {
// 			steps: []int32{1},
// 			nodeType: map[int]plan.Node_NodeType{
// 				0: plan.Node_VALUE_SCAN,
// 				1: plan.Node_INSERT,
// 			},
// 			children: map[int][]int32{
// 				1: {0},
// 			},
// 		},
// 		// insert from select
// 		"INSERT NATION SELECT * FROM NATION2": {
// 			steps: []int32{1},
// 			nodeType: map[int]plan.Node_NodeType{
// 				0: plan.Node_TABLE_SCAN,
// 				1: plan.Node_INSERT,
// 			},
// 			children: map[int][]int32{
// 				1: {0},
// 			},
// 		},
// 		// update
// 		"UPDATE NATION SET N_NAME ='U1', N_REGIONKEY=N_REGIONKEY+2 WHERE N_NATIONKEY > 10 LIMIT 20": {
// 			steps: []int32{1},
// 			nodeType: map[int]plan.Node_NodeType{
// 				0: plan.Node_TABLE_SCAN,
// 				1: plan.Node_UPDATE,
// 			},
// 			children: map[int][]int32{
// 				1: {0},
// 			},
// 		},
// 		// delete
// 		"DELETE FROM NATION WHERE N_NATIONKEY > 10 LIMIT 20": {
// 			steps: []int32{1},
// 			nodeType: map[int]plan.Node_NodeType{
// 				0: plan.Node_TABLE_SCAN,
// 				1: plan.Node_DELETE,
// 			},
// 		},
// 		// uncorrelated subquery
// 		"SELECT * FROM NATION where N_REGIONKEY > (select max(R_REGIONKEY) from REGION)": {
// 			steps: []int32{0},
// 			nodeType: map[int]plan.Node_NodeType{
// 				0: plan.Node_TABLE_SCAN, //nodeid = 1  here is the subquery
// 				1: plan.Node_TABLE_SCAN, //nodeid = 0, here is SELECT * FROM NATION where N_REGIONKEY > [subquery]
// 			},
// 			children: map[int][]int32{},
// 		},
// 		// correlated subquery
// 		`SELECT * FROM NATION where N_REGIONKEY >
// 			(select avg(R_REGIONKEY) from REGION where R_REGIONKEY < N_REGIONKEY group by R_NAME)
// 		order by N_NATIONKEY`: {
// 			steps: []int32{3},
// 			nodeType: map[int]plan.Node_NodeType{
// 				0: plan.Node_TABLE_SCAN, //nodeid = 1  subquery node，so,wo pop it to top
// 				1: plan.Node_TABLE_SCAN, //nodeid = 0
// 				2: plan.Node_AGG,        //nodeid = 2  subquery node，so,wo pop it to top
// 				3: plan.Node_SORT,       //nodeid = 3
// 			},
// 			children: map[int][]int32{
// 				2: {1}, //nodeid = 2, have children(NodeId=1, position=0)
// 				3: {0}, //nodeid = 3, have children(NodeId=0, position=2)
// 			},
// 		},
// 		// cte
// 		`with tbl(col1, col2) as (select n_nationkey, n_name from nation) select * from tbl order by col2`: {
// 			steps: []int32{1, 3},
// 			nodeType: map[int]plan.Node_NodeType{
// 				0: plan.Node_TABLE_SCAN,
// 				1: plan.Node_MATERIAL,
// 				2: plan.Node_MATERIAL_SCAN,
// 				3: plan.Node_SORT,
// 			},
// 			children: map[int][]int32{
// 				1: {0},
// 				3: {2},
// 			},
// 		},
// 	}

// 	// run test and check node tree
// 	for sql, check := range nodeTreeCheckList {
// 		mock := NewMockOptimizer(false)
// 		logicPlan, err := runOneStmt(mock, t, sql)
// 		query := logicPlan.GetQuery()
// 		if err != nil {
// 			t.Fatalf("%+v, sql=%v", err, sql)
// 		}
// 		if len(query.Steps) != len(check.steps) {
// 			t.Fatalf("run sql[%+v] error, root should be [%+v] but now is [%+v]", sql, check.steps, query.Steps)
// 		}
// 		for idx, step := range query.Steps {
// 			if step != check.steps[idx] {
// 				t.Fatalf("run sql[%+v] error, root should be [%+v] but now is [%+v]", sql, check.steps, query.Steps)
// 			}
// 		}
// 		for idx, typ := range check.nodeType {
// 			if idx >= len(query.Nodes) {
// 				t.Fatalf("run sql[%+v] error, query.Nodes[%+v].NodeType not exist", sql, idx)
// 			}
// 			if query.Nodes[idx].NodeType != typ {
// 				t.Fatalf("run sql[%+v] error, query.Nodes[%+v].NodeType should be [%+v] but now is [%+v]", sql, idx, typ, query.Nodes[idx].NodeType)
// 			}
// 		}
// 		for idx, children := range check.children {
// 			if idx >= len(query.Nodes) {
// 				t.Fatalf("run sql[%+v] error, query.Nodes[%+v].NodeType not exist", sql, idx)
// 			}
// 			if !reflect.DeepEqual(query.Nodes[idx].Children, children) {
// 				t.Fatalf("run sql[%+v] error, query.Nodes[%+v].Children should be [%+v] but now is [%+v]", sql, idx, children, query.Nodes[idx].Children)
// 			}
// 		}
// 	}
// }

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
	}
	runTestShouldError(mock, t, sqls)
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
	}
	runTestShouldPass(mock, t, sqls, false, false)

	// should error
	sqls = []string{
		"select 1 union select 2, 'a'",
		"select n_name as a from nation union select n_comment from nation order by n_name",
		"select n_name from nation minus all select n_name from nation2", // not support
	}
	runTestShouldError(mock, t, sqls)
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

func TestUpdateFallbackPgStyleFromDedupPicksWholeSourceRow(t *testing.T) {
	mock := NewMockOptimizer(true)

	logicPlan, err := runOneStmt(mock, t,
		"UPDATE emp SET sal = dept.deptno, comm = dept.deptno FROM dept WHERE emp.deptno = dept.deptno")
	if err != nil {
		t.Fatalf("build fallback UPDATE FROM plan: %v", err)
	}

	query := logicPlan.GetQuery()
	if hasUpdateFromDedupAnyValueAgg(query, len(mock.ctxt.tables["emp"].Cols)) {
		t.Fatalf("fallback UPDATE FROM dedup must pick a whole source row, not aggregate each update column with any_value")
	}
	if !hasUpdateFromDedupWindow(query, 1) {
		t.Fatalf("fallback UPDATE FROM dedup should use row_number window partitioned by target row_id")
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

// TestUpdateFallbackPgStyleFromDedupPartitionsByRowIDNotGeometry32 guards the
// fallback (buildTableUpdate) path against the same GEOMETRY32 partition-key
// crash. emp has a foreign key, so UPDATE ... FROM routes through the fallback
// planner.
func TestUpdateFallbackPgStyleFromDedupPartitionsByRowIDNotGeometry32(t *testing.T) {
	mock := NewMockOptimizer(true)
	geoTyp := plan.Type{Id: int32(types.T_geometry32)}
	setMockColumnType(t, mock, "emp", "hiredate", geoTyp)

	logicPlan, err := runOneStmt(mock, t,
		"UPDATE emp SET sal = dept.deptno, comm = dept.deptno FROM dept WHERE emp.deptno = dept.deptno")
	if err != nil {
		t.Fatalf("build fallback UPDATE FROM with GEOMETRY32 column: %v", err)
	}

	query := logicPlan.GetQuery()
	if !hasUpdateFromDedupWindow(query, 1) {
		t.Fatalf("fallback UPDATE FROM dedup must partition by row_id, not by a GEOMETRY32 target column")
	}
	if updateFromDedupPartitionsColName(query, "hiredate") {
		t.Fatalf("fallback UPDATE FROM dedup must not include the GEOMETRY32 column in the partition key")
	}
}

// TestUpdateFallbackPgStyleFromKeepsRowIdNullFilter guards the fallback path
// against losing the join-target NULL-row safeguard. The fallback path's
// needAggFilter drives both the any_value dedup AND an isnotnull(row_id) filter
// that drops NULL rows from left/right-join targets. Replacing the dedup with a
// row_number() window must still keep that NULL filter, otherwise a joined-target
// NULL row could leak into the update pipeline.
func TestUpdateFallbackPgStyleFromKeepsRowIdNullFilter(t *testing.T) {
	mock := NewMockOptimizer(true)

	logicPlan, err := runOneStmt(mock, t,
		"UPDATE emp SET sal = dept.deptno, comm = dept.deptno FROM dept WHERE emp.deptno = dept.deptno")
	if err != nil {
		t.Fatalf("build fallback UPDATE FROM plan: %v", err)
	}

	query := logicPlan.GetQuery()
	if hasAnyValueAgg(query) {
		t.Fatalf("fallback UPDATE FROM dedup must not use any_value aggregation")
	}
	if !hasRowIdIsNotNullFilter(query) {
		t.Fatalf("fallback UPDATE FROM must keep the isnotnull(row_id) join-target NULL-row filter")
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

func TestUpdateFallbackMultiTargetGeneratedColumnsKeepProjectLayout(t *testing.T) {
	mock := NewMockOptimizer(true)
	setMockGeneratedColumn(t, mock, "emp", "ename", "job")
	setMockGeneratedColumn(t, mock, "dept", "dname", "loc")

	logicPlan, err := runOneStmt(mock, t,
		"UPDATE emp, dept SET emp.job = dept.loc, dept.loc = emp.job WHERE emp.deptno = dept.deptno")
	if err != nil {
		t.Fatalf("build fallback multi-target update with generated columns: %v", err)
	}
	query := logicPlan.GetQuery()

	assertFallbackUpdateProjectLength(t, query, len(mock.ctxt.tables["emp"].Cols)+2)
	assertFallbackUpdateProjectLength(t, query, len(mock.ctxt.tables["dept"].Cols)+2)
}

// TestUpdateFallbackProjectLayoutDeterministic guards the per-target column-block
// order of the fallback UPDATE planner. A multi-column SET must produce a
// byte-identical project layout on every build; before the fix, ranging the
// updateKeys map (column -> expr) appended the update expressions to the project
// list in random order across runs. A fresh optimizer per iteration rebuilds the
// maps, and Go randomizes map iteration, so a regression here fails reliably.
func TestUpdateFallbackProjectLayoutDeterministic(t *testing.T) {
	// Multi-target (emp, dept) exercises the table-block order; the two plain
	// (non-indexed) update columns on emp (mgr, sal) exercise the per-target
	// column order. Both were Go-map-ordered before the fix.
	const sql = "UPDATE emp, dept SET emp.mgr = 1, emp.sal = 2, dept.loc = 'x' WHERE emp.deptno = dept.deptno"
	var want []string
	for iter := 0; iter < 16; iter++ {
		mock := NewMockOptimizer(true)
		logicPlan, err := runOneStmt(mock, t, sql)
		if err != nil {
			t.Fatalf("build fallback update (iter %d): %v", iter, err)
		}
		got := fallbackUpdateProjectLayout(logicPlan.GetQuery())
		if len(got) == 0 {
			t.Fatalf("iter %d: no fallback update project node found", iter)
		}
		if iter == 0 {
			want = got
			continue
		}
		assert.Equal(t, want, got,
			"fallback UPDATE project layout must be deterministic across builds (iter %d)", iter)
	}
}

// fallbackUpdateProjectLayout returns a stable signature of every fallback UPDATE
// project node (a PROJECT over a SINK_SCAN): the ordered string form of each
// project expression. query.Nodes is built in a deterministic index order, so
// any cross-build difference reflects nondeterministic plan construction.
func fallbackUpdateProjectLayout(query *Query) []string {
	var layout []string
	for _, node := range query.Nodes {
		if node.NodeType != plan.Node_PROJECT || len(node.Children) != 1 {
			continue
		}
		if query.Nodes[node.Children[0]].NodeType != plan.Node_SINK_SCAN {
			continue
		}
		for _, e := range node.ProjectList {
			layout = append(layout, e.String())
		}
	}
	return layout
}

func TestUpdateFallbackGeneratedColumnsUseDefaultAfterRewrite(t *testing.T) {
	mock := NewMockOptimizer(true)
	setMockDefaultExpr(t, mock, "emp", "job", "job-default")
	setMockGeneratedColumn(t, mock, "emp", "ename", "job")

	logicPlan, err := runOneStmt(mock, t,
		"UPDATE emp, dept SET emp.job = DEFAULT, dept.loc = 'default-marker' WHERE emp.deptno = dept.deptno")
	if err != nil {
		t.Fatalf("build fallback update with generated column over DEFAULT: %v", err)
	}

	node := requireFallbackSourceProjectNode(t, logicPlan.GetQuery(),
		len(mock.ctxt.tables["emp"].Cols)+2+len(mock.ctxt.tables["dept"].Cols)+1, "default-marker")
	if !nodeContainsStringLiteral(node, "job-default") {
		t.Fatalf("generated column should use expanded DEFAULT expression, got %v", node.ProjectList)
	}
}

func TestUpdateFallbackGeneratedColumnsUseOnUpdateAfterRewrite(t *testing.T) {
	mock := NewMockOptimizer(true)
	setMockOnUpdateExpr(t, mock, "emp", "job", "job-on-update")
	setMockGeneratedColumn(t, mock, "emp", "ename", "job")

	logicPlan, err := runOneStmt(mock, t,
		"UPDATE emp, dept SET emp.comm = 1, dept.loc = 'on-update-marker' WHERE emp.deptno = dept.deptno")
	if err != nil {
		t.Fatalf("build fallback update with generated column over ON UPDATE: %v", err)
	}

	node := requireFallbackSourceProjectNode(t, logicPlan.GetQuery(),
		len(mock.ctxt.tables["emp"].Cols)+2+len(mock.ctxt.tables["dept"].Cols)+1, "on-update-marker")
	if !nodeContainsStringLiteral(node, "job-on-update") {
		t.Fatalf("generated column should use ON UPDATE expression, got %v", node.ProjectList)
	}
}

func TestUpdateFallbackGeneratedColumnChainUsesFreshExpr(t *testing.T) {
	mock := NewMockOptimizer(true)
	setMockGeneratedColumn(t, mock, "emp", "mgr", "empno")
	setMockGeneratedColumn(t, mock, "emp", "deptno", "mgr")

	logicPlan, err := runOneStmt(mock, t,
		"UPDATE emp, dept SET emp.comm = 1, dept.loc = 'chain-marker' WHERE emp.deptno = dept.deptno")
	if err != nil {
		t.Fatalf("build fallback update with generated column chain: %v", err)
	}

	query := logicPlan.GetQuery()
	assertFallbackUpdateAggDedupWithAnyValue(t, query)

	// Verify the generated-column chain without depending on the order of the
	// appended update/recompute slots (that order is sensitive to map iteration
	// and was a source of flakiness). emp contributes len(emp.Cols) base columns
	// followed by its appended update + recomputed-generated expressions; both
	// generated columns (mgr, deptno) must be freshly recomputed down to empno,
	// so within that appended region none may reference the stale mgr column and
	// exactly two must reference empno.
	empCols := len(mock.ctxt.tables["emp"].Cols)
	deptCols := len(mock.ctxt.tables["dept"].Cols)
	node := requireFallbackSourceProjectNode(t, query, empCols+3+deptCols+1, "chain-marker")
	empnoRefs := 0
	for pos := empCols; pos < empCols+3; pos++ {
		e := node.ProjectList[pos]
		if exprContainsColName(e, "mgr") {
			t.Fatalf("generated column chain must use freshly recomputed empno, not stale mgr; appended pos %d = %s", pos, e.String())
		}
		if exprContainsColName(e, "empno") {
			empnoRefs++
		}
	}
	if empnoRefs != 2 {
		t.Fatalf("expected both generated columns (mgr, deptno) freshly recomputed to empno, got %d empno refs in emp appended region", empnoRefs)
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

func requireFallbackSourceProjectNode(t *testing.T, query *Query, projectLen int, marker string) *Node {
	for _, node := range query.Nodes {
		if !isFallbackSourceProjectNode(query, node, projectLen, marker) {
			continue
		}
		return node
	}
	t.Fatalf("missing fallback source project with length %d and marker %q", projectLen, marker)
	return nil
}

func isFallbackSourceProjectNode(query *Query, node *Node, projectLen int, marker string) bool {
	if node.NodeType != plan.Node_PROJECT || len(node.ProjectList) != projectLen {
		return false
	}
	if len(node.Children) == 1 {
		childIdx := node.Children[0]
		if childIdx >= 0 && childIdx < int32(len(query.Nodes)) && query.Nodes[childIdx].NodeType == plan.Node_SINK_SCAN {
			return false
		}
	}
	for _, expr := range node.ProjectList {
		if exprContainsStringLiteral(expr, marker) {
			return true
		}
	}
	return false
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

// hasRowIdIsNotNullFilter reports whether the plan filters out joined-target
// NULL rows via isnotnull(row_id). This safeguard must survive on the fallback
// UPDATE ... FROM path even after duplicate matches are deduped by row_number().
func hasRowIdIsNotNullFilter(query *Query) bool {
	for _, node := range query.Nodes {
		if node.NodeType != plan.Node_FILTER {
			continue
		}
		for _, f := range node.FilterList {
			fn := f.GetF()
			if fn == nil || fn.Func.ObjName != "isnotnull" || len(fn.Args) != 1 {
				continue
			}
			if exprContainsColName(fn.Args[0], catalog.Row_ID) {
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

func nodeContainsStringLiteral(node *Node, value string) bool {
	for _, expr := range node.ProjectList {
		if exprContainsStringLiteral(expr, value) {
			return true
		}
	}
	return false
}

func assertFallbackUpdateProjectLength(t *testing.T, query *Query, projectLen int) {
	for _, node := range query.Nodes {
		if node.NodeType != plan.Node_PROJECT || len(node.ProjectList) != projectLen || len(node.Children) != 1 {
			continue
		}
		child := query.Nodes[node.Children[0]]
		if child.NodeType != plan.Node_SINK_SCAN {
			continue
		}
		return
	}
	t.Fatalf("missing fallback update project with length %d", projectLen)
}

func assertFallbackUpdateAggDedupWithAnyValue(t *testing.T, query *Query) {
	foundAgg := false
	foundAnyValue := false
	for _, node := range query.Nodes {
		if node.NodeType != plan.Node_AGG {
			continue
		}
		foundAgg = true
		for _, expr := range node.AggList {
			if exprContainsFuncName(expr, "any_value") {
				foundAnyValue = true
				break
			}
		}
	}
	if !foundAgg || !foundAnyValue {
		t.Fatalf("fallback update should build agg dedup path with any_value, foundAgg=%v foundAnyValue=%v", foundAgg, foundAnyValue)
	}
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
		"create table agg01 (col1 int, col2 enum('egwjqebwq', 'qwewqewqeqewq', 'weueiwqeowqehwgqjhenw') primary key)",
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
	return BuildPlan(ctx, stmts[0], false)
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
// must not mistake it for a genuine Expr_List, otherwise GetList() returns nil
// and .List panics during plan building. This asserts BuildPlan does not panic.
func TestCountDistinctRowSubqueryNoPanic(t *testing.T) {
	mock := NewMockOptimizer(false)
	require.NotPanics(t, func() {
		_, _ = runOneStmt(mock, t,
			"select count(distinct (select n_nationkey, n_regionkey from nation)) from nation")
	})
}
