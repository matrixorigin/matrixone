// Copyright 2026 Matrix Origin
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

package plan

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

type issue24822Optimizer struct {
	ctx *fullTextJoinMockCompilerContext
}

func (o *issue24822Optimizer) CurrentContext() CompilerContext {
	return o.ctx
}

func (o *issue24822Optimizer) Optimize(stmt tree.Statement) (*Query, error) {
	logicPlan, err := BuildPlan(o.ctx, stmt, false)
	if err != nil {
		return nil, err
	}
	return logicPlan.GetQuery(), nil
}

func newIssue24822Optimizer() *issue24822Optimizer {
	ctx := newFullTextJoinMockCompilerContext()

	ftDef := makeFullTextJoinTestTableDef("ft", true)
	ftDef.TblId = 24822
	ctx.tables["ft"] = ftDef
	ctx.objects["ft"] = &planpb.ObjectRef{
		SchemaName: "tpch",
		ObjName:    "ft",
		Obj:        int64(ftDef.TblId),
	}

	idxName := ftDef.Indexes[0].IndexTableName
	ctx.tables[idxName] = &planpb.TableDef{
		Name: idxName,
		Cols: []*planpb.ColDef{
			{Name: "doc_id", Typ: planpb.Type{Id: int32(types.T_varchar), Width: 191}},
			{Name: "score", Typ: planpb.Type{Id: int32(types.T_float32)}},
		},
		Name2ColIndex: map[string]int32{"doc_id": 0, "score": 1},
	}
	ctx.objects[idxName] = &planpb.ObjectRef{SchemaName: "tpch", ObjName: idxName}

	dstDef := &planpb.TableDef{
		Name:  "t",
		TblId: 24823,
		Cols: []*planpb.ColDef{
			{Name: "id", Typ: planpb.Type{Id: int32(types.T_varchar), Width: 191}},
			{Name: "sc", Typ: planpb.Type{Id: int32(types.T_float32)}},
			{Name: catalog.Row_ID, Typ: planpb.Type{Id: int32(types.T_Rowid)}, Hidden: true},
		},
		Name2ColIndex: map[string]int32{"id": 0, "sc": 1, catalog.Row_ID: 2},
		Pkey: &planpb.PrimaryKeyDef{
			PkeyColName: "id",
			Names:       []string{"id"},
			Cols:        []uint64{0},
		},
	}
	ctx.tables["t"] = dstDef
	ctx.objects["t"] = &planpb.ObjectRef{
		SchemaName: "tpch",
		ObjName:    "t",
		Obj:        int64(dstDef.TblId),
	}

	return &issue24822Optimizer{ctx: ctx}
}

func countReachableFullTextScans(query *planpb.Query) int {
	seen := make(map[int32]bool)
	var visit func(int32) int
	visit = func(nodeID int32) int {
		if nodeID < 0 || int(nodeID) >= len(query.Nodes) || seen[nodeID] {
			return 0
		}
		seen[nodeID] = true
		node := query.Nodes[nodeID]
		count := 0
		if node.NodeType == planpb.Node_FUNCTION_SCAN && node.TableDef != nil &&
			node.TableDef.TblFunc != nil && node.TableDef.TblFunc.Name == fulltext_index_scan_func_name {
			count++
		}
		for _, childID := range node.Children {
			count += visit(childID)
		}
		return count
	}

	count := 0
	for _, step := range query.Steps {
		count += visit(step)
	}
	return count
}

func countFullTextMatchesInExpr(expr *planpb.Expr) int {
	if expr == nil {
		return 0
	}

	count := 0
	switch impl := expr.Expr.(type) {
	case *planpb.Expr_F:
		if impl.F.Func.ObjName == "fulltext_match" {
			count++
		}
		for _, arg := range impl.F.Args {
			count += countFullTextMatchesInExpr(arg)
		}
	case *planpb.Expr_List:
		for _, item := range impl.List.List {
			count += countFullTextMatchesInExpr(item)
		}
	case *planpb.Expr_W:
		count += countFullTextMatchesInExpr(impl.W.WindowFunc)
		for _, partitionBy := range impl.W.PartitionBy {
			count += countFullTextMatchesInExpr(partitionBy)
		}
		for _, orderBy := range impl.W.OrderBy {
			count += countFullTextMatchesInExpr(orderBy.Expr)
		}
	}
	return count
}

func countReachableFullTextMatches(query *planpb.Query) int {
	seen := make(map[int32]bool)
	var visit func(int32) int
	visit = func(nodeID int32) int {
		if nodeID < 0 || int(nodeID) >= len(query.Nodes) || seen[nodeID] {
			return 0
		}
		seen[nodeID] = true
		node := query.Nodes[nodeID]
		count := 0
		for _, exprList := range [][]*planpb.Expr{
			node.ProjectList,
			node.FilterList,
			node.OnList,
			node.AggList,
			node.GroupBy,
			node.WinSpecList,
		} {
			for _, expr := range exprList {
				count += countFullTextMatchesInExpr(expr)
			}
		}
		for _, orderBy := range node.OrderBy {
			count += countFullTextMatchesInExpr(orderBy.Expr)
		}
		for _, childID := range node.Children {
			count += visit(childID)
		}
		return count
	}

	count := 0
	for _, step := range query.Steps {
		count += visit(step)
	}
	return count
}

func TestIssue24822FullTextComposesWithNestedQueries(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{
			name: "multiple CTEs and left join",
			sql: `WITH a AS (
				SELECT id, MATCH(title, body) AGAINST('hello' IN BOOLEAN MODE) sc
				FROM ft
				WHERE MATCH(title, body) AGAINST('hello' IN BOOLEAN MODE)
			), r AS (
				SELECT id, ROW_NUMBER() OVER (ORDER BY sc DESC) rk FROM a
			)
			SELECT f.id, r.rk FROM ft f LEFT JOIN r ON f.id = r.id`,
		},
		{
			name: "insert select",
			sql: `INSERT INTO t(id, sc)
				SELECT id, MATCH(title, body) AGAINST('hello')
				FROM ft
				WHERE MATCH(title, body) AGAINST('hello')`,
		},
		{
			name: "score projection nested in derived tables",
			sql: `SELECT d.id, d.sc FROM (
				SELECT n.id, n.sc FROM (
					SELECT id, MATCH(title, body) AGAINST('hello') sc
					FROM ft
				) n
			) d`,
		},
		{
			name: "filter only CTE below left join",
			sql: `WITH a AS (
				SELECT id FROM ft
				WHERE MATCH(title, body) AGAINST('hello')
			), r AS (
				SELECT id, ROW_NUMBER() OVER (ORDER BY id) rk FROM a
			)
			SELECT f.id, r.rk FROM ft f LEFT JOIN r ON f.id = r.id`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			logicPlan, err := runOneStmt(newIssue24822Optimizer(), t, test.sql)
			require.NoError(t, err)
			query := logicPlan.GetQuery()
			require.GreaterOrEqual(t, countReachableFullTextScans(query), 1)
			require.Zero(t, countReachableFullTextMatches(query))
		})
	}
}
