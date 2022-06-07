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
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/errno"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/errors"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func buildDelete(stmt *tree.Delete, ctx CompilerContext) (*Plan, error) {
	// check database's name and table's name
	tbl, ok := stmt.Table.(*tree.AliasedTableExpr).Expr.(*tree.TableName)
	if !ok {
		return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, "cannot delete from multiple tables")
	}
	var dbName string
	if tbl.SchemaName == "" {
		dbName = ctx.DefaultDatabase()
	}
	objRef, tableDef := ctx.Resolve(dbName, string(tbl.ObjectName))
	if tableDef == nil {
		return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, "cannot find delete table")
	}

	// build the projection of select
	var projectExprs tree.SelectExprs
	if stmt.Where != nil {
		if err := buildProjectionFromExpr(stmt.Where.Expr, &projectExprs); err != nil {
			return nil, err
		}
	}
	if stmt.OrderBy != nil {
		for _, order := range stmt.OrderBy {
			if err := buildProjectionFromExpr(order.Expr, &projectExprs); err != nil {
				return nil, err
			}
		}
	}

	// check col's def if exists in table's def.
	// this check can remove if buildSelect has checked.
	// cols, err := checkColumns(projectExprs, tableDef)
	// if err != nil {
	//	 return nil, err
	// }

	// find out use key to delete
	var useKey *ColDef = nil
	var useProjectExprs tree.SelectExprs = nil
	priKeys := ctx.GetPrimaryKeyDef(objRef.SchemaName, tableDef.Name)
	if priKeys != nil {
		for _, key := range priKeys {
			e, _ := tree.NewUnresolvedName(key.Name)
			if isDuplicated(e, &projectExprs) {
				useProjectExprs = append(useProjectExprs, tree.SelectExpr{Expr: e})
				useKey = key
				break
			}
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
	if len(stmt.OrderBy) > 0 && (stmt.Where == nil && stmt.Limit == nil) {
		stmt.OrderBy = nil
	}
	selectStmt := &tree.Select{
		Select: &tree.SelectClause{
			Exprs: useProjectExprs,
			From:  &tree.From{Tables: tree.TableExprs{stmt.Table}},
			Where: stmt.Where,
		},
		OrderBy: stmt.OrderBy,
		Limit:   stmt.Limit,
	}
	query, binderCtx := newQueryAndSelectCtx(plan.Query_DELETE)
	nodeId, err := buildSelect(selectStmt, ctx, query, binderCtx)
	if err != nil {
		return nil, err
	}
	query.Steps = append(query.Steps, nodeId)

	// build delete node
	node := &Node{
		NodeType:     plan.Node_DELETE,
		ObjRef:       objRef,
		TableDef:     tableDef,
		UseDeleteKey: useKey.Name,
		Children:     []int32{nodeId},
	}
	appendQueryNode(query, node)

	// reset root node
	preNode := query.Nodes[len(query.Nodes)-1]
	query.Steps[len(query.Steps)-1] = preNode.NodeId

	return &Plan{
		Plan: &plan.Plan_Query{
			Query: query,
		},
	}, nil
}

func checkColumns(projectExprs tree.SelectExprs, tableDef *TableDef) ([]string, error) {
	var cols []string = nil
	for _, e := range projectExprs {
		col, _ := e.Expr.(*tree.UnresolvedName)
		colName := col.Parts[0]
		cols = append(cols, colName)
		if !inTableDef(colName, tableDef) {
			return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("column '%v' not in table '%v'", colName, tableDef.Name))
		}
	}
	return cols, nil
}

func inTableDef(colName string, tableDef *TableDef) bool {
	for _, def := range tableDef.Cols {
		if colName == def.Name {
			return true
		}
	}
	return false
}

func buildStarProjection() tree.SelectExprs {
	expr := tree.SelectExpr{
		Expr: tree.UnqualifiedStar{},
	}
	return tree.SelectExprs{expr}
}

func buildProjectionFromExpr(expr tree.Expr, selectExprs *tree.SelectExprs) error {
	switch e := expr.(type) {
	case *tree.NumVal:
		return nil
	case *tree.ParenExpr:
		return buildProjectionFromExpr(e.Expr, selectExprs)
	case *tree.OrExpr:
		if err := buildProjectionFromExpr(e.Left, selectExprs); err != nil {
			return err
		}
		return buildProjectionFromExpr(e.Right, selectExprs)
	case *tree.NotExpr:
		return buildProjectionFromExpr(e.Expr, selectExprs)
	case *tree.AndExpr:
		if err := buildProjectionFromExpr(e.Left, selectExprs); err != nil {
			return err
		}
		return buildProjectionFromExpr(e.Right, selectExprs)
	case *tree.UnaryExpr:
		return buildProjectionFromExpr(e.Expr, selectExprs)
	case *tree.BinaryExpr:
		if err := buildProjectionFromExpr(e.Left, selectExprs); err != nil {
			return err
		}
		return buildProjectionFromExpr(e.Right, selectExprs)
	case *tree.ComparisonExpr:
		if err := buildProjectionFromExpr(e.Left, selectExprs); err != nil {
			return err
		}
		return buildProjectionFromExpr(e.Right, selectExprs)
	case *tree.FuncExpr:
		for _, ex := range e.Exprs {
			if err := buildProjectionFromExpr(ex, selectExprs); err != nil {
				return err
			}
		}
		return nil
	case *tree.CastExpr:
		return buildProjectionFromExpr(e.Expr, selectExprs)
	case *tree.RangeCond:
		return errors.New(errno.SQLStatementNotYetComplete, "range condition is not supported")
	case *tree.UnresolvedName:
		if !isDuplicated(e, selectExprs) {
			*selectExprs = append(*selectExprs, tree.SelectExpr{Expr: e})
		}
		return nil
	}
	return errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("'%v' is not support now", tree.String(expr, dialect.MYSQL)))
}

func isDuplicated(name *tree.UnresolvedName, selectExprs *tree.SelectExprs) bool {
	for _, expr := range *selectExprs {
		e := expr.Expr.(*tree.UnresolvedName)
		if name.Parts[0] == e.Parts[0] {
			return true
		}
	}
	return false
}
