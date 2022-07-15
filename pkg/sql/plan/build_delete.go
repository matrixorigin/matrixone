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
	"fmt"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/errno"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/errors"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func buildDelete(stmt *tree.Delete, ctx CompilerContext) (*Plan, error) {
	if len(stmt.Tables) == 1 {
		return buildDeleteSingleTable(stmt, ctx)
	}
	return buildDeleteMultipleTable(stmt, ctx)
}

// buildDeleteSingleTable, delete single table can optimize to truncate and different from syntax of multiple-tables,
// so it is build singly.
func buildDeleteSingleTable(stmt *tree.Delete, ctx CompilerContext) (*Plan, error) {
	// check database's name and table's name
	tbl := getTableNames(stmt.Tables)[0]
	dbName := string(tbl.SchemaName)
	if dbName == "" {
		dbName = ctx.DefaultDatabase()
	}
	objRef, tableDef := ctx.Resolve(dbName, string(tbl.ObjectName))
	if tableDef == nil {
		return nil, errors.New(errno.FeatureNotSupported, "cannot find delete table")
	}

	// optimize to truncate,
	if stmt.Where == nil && stmt.Limit == nil {
		return buildDelete2Truncate(objRef, tableDef)
	}

	// find out use keys to delete
	var useProjectExprs tree.SelectExprs = nil
	useProjectExprs, useKey, err := buildUseProjection(stmt, useProjectExprs, objRef, tableDef, ctx)
	if err != nil {
		return nil, err
	}

	// build the stmt of select and append select node
	selectStmt := &tree.Select{
		Select: &tree.SelectClause{
			Exprs: useProjectExprs,
			From:  &tree.From{Tables: stmt.Tables},
			Where: stmt.Where,
		},
		OrderBy: stmt.OrderBy,
		Limit:   stmt.Limit,
		With:    stmt.With,
	}
	usePlan, err := runBuildSelectByBinder(plan.Query_SELECT, ctx, selectStmt)
	if err != nil {
		return nil, err
	}
	usePlan.Plan.(*plan.Plan_Query).Query.StmtType = plan.Query_DELETE
	qry := usePlan.Plan.(*plan.Plan_Query).Query

	// build delete node
	d := &plan.DeleteTableCtx{
		DbName:       objRef.SchemaName,
		TblName:      tableDef.Name,
		UseDeleteKey: useKey.Name,
		CanTruncate:  false,
	}
	node := &Node{
		NodeType:        plan.Node_DELETE,
		ObjRef:          nil,
		TableDef:        nil,
		Children:        []int32{qry.Steps[len(qry.Steps)-1]},
		NodeId:          int32(len(qry.Nodes)),
		DeleteTablesCtx: []*plan.DeleteTableCtx{d},
	}
	qry.Nodes = append(qry.Nodes, node)
	qry.Steps[len(qry.Steps)-1] = node.NodeId

	return usePlan, nil
}

func buildDelete2Truncate(objRef *ObjectRef, tblDef *TableDef) (*Plan, error) {
	// build delete node
	d := &plan.DeleteTableCtx{
		DbName:      objRef.SchemaName,
		TblName:     tblDef.Name,
		CanTruncate: true,
	}
	node := &Node{
		NodeType:        plan.Node_DELETE,
		ObjRef:          objRef,
		TableDef:        tblDef,
		DeleteTablesCtx: []*plan.DeleteTableCtx{d},
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

func buildDeleteMultipleTable(stmt *tree.Delete, ctx CompilerContext) (*Plan, error) {
	// check database's name and table's name
	tbs := getTableNames(stmt.Tables)
	tableCount := len(tbs)
	objRefs := make([]*ObjectRef, tableCount)
	tblDefs := make([]*TableDef, tableCount)
	for i, t := range tbs {
		dbName := string(t.SchemaName)
		if dbName == "" {
			dbName = ctx.DefaultDatabase()
		}
		objRefs[i], tblDefs[i] = ctx.Resolve(dbName, string(t.ObjectName))
		if tblDefs[i] == nil {
			return nil, errors.New(errno.FeatureNotSupported, fmt.Sprintf("cannot find delete table: %s.%s", dbName, string(t.ObjectName)))
		}
	}

	// find out use keys to delete
	var err error
	useKeys := make([]*ColDef, tableCount)
	var useProjectExprs tree.SelectExprs = nil
	for i := 0; i < tableCount; i++ {
		useProjectExprs, useKeys[i], err = buildUseProjection(stmt, useProjectExprs, objRefs[i], tblDefs[i], ctx)
		if err != nil {
			return nil, err
		}
	}

	// build the stmt of select and append select node
	selectStmt := &tree.Select{
		Select: &tree.SelectClause{
			Exprs: useProjectExprs,
			From:  &tree.From{Tables: stmt.TableRefs},
			Where: stmt.Where,
		},
		OrderBy: stmt.OrderBy,
		Limit:   stmt.Limit,
		With:    stmt.With,
	}
	usePlan, err := runBuildSelectByBinder(plan.Query_SELECT, ctx, selectStmt)
	if err != nil {
		return nil, err
	}
	usePlan.Plan.(*plan.Plan_Query).Query.StmtType = plan.Query_DELETE
	qry := usePlan.Plan.(*plan.Plan_Query).Query

	ds := make([]*plan.DeleteTableCtx, tableCount)
	for i := 0; i < tableCount; i++ {
		ds[i] = &plan.DeleteTableCtx{
			DbName:       objRefs[i].SchemaName,
			TblName:      tblDefs[i].Name,
			UseDeleteKey: useKeys[i].Name,
			CanTruncate:  false,
		}
	}
	node := &Node{
		NodeType:        plan.Node_DELETE,
		ObjRef:          nil,
		TableDef:        nil,
		Children:        []int32{qry.Steps[len(qry.Steps)-1]},
		NodeId:          int32(len(qry.Nodes)),
		DeleteTablesCtx: ds,
	}
	qry.Nodes = append(qry.Nodes, node)
	qry.Steps[len(qry.Steps)-1] = node.NodeId

	return usePlan, nil
}

func getTableNames(tableExprs tree.TableExprs) []*tree.TableName {
	tbs := make([]*tree.TableName, 0, len(tableExprs))
	for _, tableExpr := range tableExprs {
		if t, ok := tableExpr.(*tree.AliasedTableExpr); ok {
			tbs = append(tbs, t.Expr.(*tree.TableName))
			continue
		}
		tbs = append(tbs, tableExpr.(*tree.TableName))
	}
	return tbs
}

func buildUseProjection(stmt *tree.Delete, ps tree.SelectExprs, objRef *ObjectRef, tableDef *TableDef, ctx CompilerContext) (tree.SelectExprs, *ColDef, error) {
	var useKey *ColDef = nil
	priKeys := ctx.GetPrimaryKeyDef(objRef.SchemaName, tableDef.Name)
	for _, key := range priKeys {
		e := tree.SetUnresolvedName(objRef.SchemaName, tableDef.Name, key.Name)
		if isContainNameInFilter(stmt, key.Name) {
			ps = append(ps, tree.SelectExpr{Expr: e})
			useKey = key
			break
		}
	}
	if useKey == nil {
		hideKey := ctx.GetHideKeyDef(objRef.SchemaName, tableDef.Name)
		if hideKey == nil {
			return nil, nil, errors.New(errno.SyntaxErrororAccessRuleViolation, "cannot find hide key now")
		}
		useKey = hideKey
		e := tree.SetUnresolvedName(objRef.SchemaName, tableDef.Name, hideKey.Name)
		ps = append(ps, tree.SelectExpr{Expr: e})
	}
	return ps, useKey, nil
}

// isContainNameInFilter is to find out if contain primary key in expr.
// it works any way. Chose other way to delete when it is not accurate judgment
func isContainNameInFilter(stmt *tree.Delete, name string) bool {
	if stmt.TableRefs != nil {
		for _, e := range stmt.TableRefs {
			if strings.Contains(tree.String(e, dialect.MYSQL), name) {
				return true
			}
		}
	}
	if stmt.OrderBy != nil {
		for _, e := range stmt.OrderBy {
			if strings.Contains(tree.String(e.Expr, dialect.MYSQL), name) {
				return true
			}
		}
	}
	if stmt.Where != nil {
		if strings.Contains(tree.String(stmt.Where, dialect.MYSQL), name) {
			return true
		}
	}
	return false
}
