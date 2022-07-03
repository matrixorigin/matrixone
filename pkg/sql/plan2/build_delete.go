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

package plan2

import (
	"strings"

	"github.com/matrixorigin/matrixone/pkg/errno"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/errors"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func buildDelete(stmt *tree.Delete, ctx CompilerContext) (*Plan, error) {
	// check database's name and table's name
	alsTbl, ok := stmt.Table.(*tree.AliasedTableExpr)
	if !ok {
		return nil, errors.New(errno.FeatureNotSupported, "cannot delete from multiple tables")
	}
	tbl, ok := alsTbl.Expr.(*tree.TableName)
	if !ok {
		return nil, errors.New(errno.FeatureNotSupported, "cannot delete from multiple tables")
	}
	var dbName string
	if tbl.SchemaName == "" {
		dbName = ctx.DefaultDatabase()
	}
	objRef, tableDef := ctx.Resolve(dbName, string(tbl.ObjectName))
	if tableDef == nil {
		return nil, errors.New(errno.FeatureNotSupported, "cannot find delete table")
	}

	if stmt.Where == nil && stmt.Limit == nil {
		return buildDelete2Truncate(objRef, tableDef)
	}
	// find out use key to delete
	var useKey *ColDef = nil
	var useProjectExprs tree.SelectExprs = nil
	priKeys := ctx.GetPrimaryKeyDef(objRef.SchemaName, tableDef.Name)
	for _, key := range priKeys {
		e, _ := tree.NewUnresolvedName(key.Name)
		if isContainNameInWhere(stmt.Where, key.Name) || isContainNameInOrderBy(stmt.OrderBy, key.Name) {
			useProjectExprs = append(useProjectExprs, tree.SelectExpr{Expr: e})
			useKey = key
			break
		}
	}
	if useKey == nil {
		hideKey := ctx.GetHideKeyDef(objRef.SchemaName, tableDef.Name)
		if hideKey == nil {
			return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, "cannot find hide key now")
		}
		useKey = hideKey
		e, _ := tree.NewUnresolvedName(hideKey.Name)
		useProjectExprs = append(useProjectExprs, tree.SelectExpr{Expr: e})
	}

	// build the stmt of select and append select node
	selectStmt := &tree.Select{
		Select: &tree.SelectClause{
			Exprs: useProjectExprs,
			From:  &tree.From{Tables: tree.TableExprs{stmt.Table}},
			Where: stmt.Where,
		},
		OrderBy: stmt.OrderBy,
		Limit:   stmt.Limit,
	}
	usePlan, err := runBuildSelectByBinder(plan.Query_SELECT, ctx, selectStmt)
	if err != nil {
		return nil, err
	}
	usePlan.Plan.(*plan.Plan_Query).Query.StmtType = plan.Query_DELETE
	qry := usePlan.Plan.(*plan.Plan_Query).Query

	// build delete node
	node := &Node{
		NodeType: plan.Node_DELETE,
		ObjRef:   objRef,
		TableDef: tableDef,
		Children: []int32{qry.Steps[len(qry.Steps)-1]},
		NodeId:   int32(len(qry.Nodes)),
		DeleteInfo: &plan.DeleteInfo{
			UseDeleteKey: useKey.Name,
			CanTruncate:  false,
		},
	}
	qry.Nodes = append(qry.Nodes, node)
	qry.Steps[len(qry.Steps)-1] = node.NodeId

	return usePlan, nil
}

func buildDelete2Truncate(objRef *ObjectRef, tblDef *TableDef) (*Plan, error) {
	node := &Node{
		NodeType: plan.Node_DELETE,
		ObjRef:   objRef,
		TableDef: tblDef,
		DeleteInfo: &plan.DeleteInfo{
			CanTruncate: true,
		},
	}
	return &Plan{
		Plan: &plan.Plan_Query{
			Query: &Query{
				StmtType: plan.Query_DELETE,
				Steps:    []int32{0},
				Nodes:    []*Node{node},
			},
		},
	}, nil
}

func isContainNameInWhere(expr *tree.Where, name string) bool {
	if expr == nil {
		return false
	}
	return strings.Contains(tree.String(expr, dialect.MYSQL), name)
}

func isContainNameInOrderBy(expr tree.OrderBy, name string) bool {
	if expr == nil {
		return false
	}
	for _, order := range expr {
		if strings.Contains(tree.String(order.Expr, dialect.MYSQL), name) {
			return true
		}
	}
	return false
}
