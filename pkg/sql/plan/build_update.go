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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
)

type tableInfo struct {
	dbNames     []string
	tableNames  []string
	baseNameMap map[string]string // key: alias name, value: base name
}

type updateCol struct {
	dbName       string
	tblName      string
	aliasTblName string
	colDef       *ColDef
}

func buildUpdate(stmt *tree.Update, ctx CompilerContext) (*Plan, error) {
	// build map between base table and alias table
	tf := &tableInfo{baseNameMap: make(map[string]string)}
	extractWithTable(stmt.With, tf, ctx)
	for _, expr := range stmt.Tables {
		if err := extractExprTable(expr, tf, ctx); err != nil {
			return nil, err
		}
	}

	// build reference
	objRefs := make([]*ObjectRef, 0, len(tf.dbNames))
	tblRefs := make([]*TableDef, 0, len(tf.tableNames))
	for i := range tf.dbNames {
		objRef, tblRef := ctx.Resolve(tf.dbNames[i], tf.tableNames[i])
		if tblRef == nil {
			return nil, moerr.NewInternalError("cannot find update table: %s.%s", tf.dbNames[i], tf.tableNames[i])
		}
		if tblRef.TableType == catalog.SystemExternalRel {
			return nil, moerr.NewInternalError("the external table is not support update operation")
		} else if tblRef.TableType == catalog.SystemViewRel {
			return nil, moerr.NewInternalError("view is not support update operation")
		}
		objRefs = append(objRefs, objRef)
		tblRefs = append(tblRefs, tblRef)
	}

	// check and build update's columns
	updateCols, err := buildUpdateColumns(stmt.Exprs, objRefs, tblRefs, tf.baseNameMap)
	if err != nil {
		return nil, err
	}

	// group update columns
	updateColsArray, updateExprsArray := groupUpdateAttrs(updateCols, stmt.Exprs)

	// build update ctx and projection
	updateCtxs, useProjectExprs, err := buildCtxAndProjection(updateColsArray, updateExprsArray, tf.baseNameMap, tblRefs, ctx)
	if err != nil {
		return nil, err
	}

	// build the stmt of select and append select node
	if len(stmt.OrderBy) > 0 && (stmt.Where == nil && stmt.Limit == nil) {
		stmt.OrderBy = nil
	}
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
	usePlan.Plan.(*plan.Plan_Query).Query.StmtType = plan.Query_UPDATE
	qry := usePlan.Plan.(*plan.Plan_Query).Query

	// rebuild projection for update cols to get right type and default value
	lastNode := qry.Nodes[qry.Steps[len(qry.Steps)-1]]
	for _, ct := range updateCtxs {
		var idx int
		if ct.PriKeyIdx != -1 {
			idx = int(ct.PriKeyIdx) + 1
		} else {
			idx = int(ct.HideKeyIdx) + 1
		}

		for _, col := range ct.UpdateCols {
			if c := lastNode.ProjectList[idx].GetC(); c != nil {
				if c.GetDefaultval() {
					if lastNode.ProjectList[idx], err = getDefaultExpr(col); err != nil {
						return nil, err
					}
					idx++
					continue
				}
				if c.GetUpdateVal() {
					lastNode.ProjectList[idx] = col.OnUpdate
					idx++
					continue
				}
			}
			lastNode.ProjectList[idx], err = makePlan2CastExpr(lastNode.ProjectList[idx], col.Typ)
			if err != nil {
				return nil, err
			}
			idx++
		}
	}

	// build update node
	node := &Node{
		NodeType:    plan.Node_UPDATE,
		ObjRef:      nil,
		TableDef:    nil,
		TableDefVec: tblRefs,
		Children:    []int32{qry.Steps[len(qry.Steps)-1]},
		NodeId:      int32(len(qry.Nodes)),
		UpdateCtxs:  updateCtxs,
	}
	qry.Nodes = append(qry.Nodes, node)
	qry.Steps[len(qry.Steps)-1] = node.NodeId

	return usePlan, nil
}

func extractSelectTable(stmt tree.TableExpr, tblName, dbName *string, ctx CompilerContext) {
	switch s := stmt.(type) {
	case *tree.Select:
		if clause, ok := s.Select.(*tree.SelectClause); ok {
			if len(clause.From.Tables) == 1 {
				extractSelectTable(clause.From.Tables[0], tblName, dbName, ctx)
			}
		}
	case *tree.ParenSelect:
		if clause, ok := s.Select.Select.(*tree.SelectClause); ok {
			if len(clause.From.Tables) == 1 {
				extractSelectTable(clause.From.Tables[0], tblName, dbName, ctx)
			}
		}
	case *tree.ParenTableExpr:
		extractSelectTable(s.Expr, tblName, dbName, ctx)
	case *tree.AliasedTableExpr:
		if s.As.Cols != nil {
			return
		}
		extractSelectTable(s.Expr, tblName, dbName, ctx)
	case *tree.TableName:
		*dbName = string(s.SchemaName)
		*tblName = string(s.ObjectName)
		if *dbName == "" {
			*dbName = ctx.DefaultDatabase()
		}
	}
}

func extractWithTable(with *tree.With, tf *tableInfo, ctx CompilerContext) {
	if with == nil {
		return
	}
	for _, cte := range with.CTEs {
		if cte.Name.Cols != nil {
			continue
		}
		tblName := new(string)
		dbName := new(string)
		switch s := cte.Stmt.(type) {
		case *tree.Select:
			if clause, ok := s.Select.(*tree.SelectClause); ok {
				if len(clause.From.Tables) == 1 {
					extractSelectTable(clause.From.Tables[0], tblName, dbName, ctx)
				}
			}
		case *tree.ParenSelect:
			if clause, ok := s.Select.Select.(*tree.SelectClause); ok {
				if len(clause.From.Tables) == 1 {
					extractSelectTable(clause.From.Tables[0], tblName, dbName, ctx)
				}
			}
		}
		if *tblName != "" {
			tf.tableNames = append(tf.tableNames, *tblName)
			tf.dbNames = append(tf.dbNames, *dbName)
			tf.baseNameMap[string(cte.Name.Alias)] = *tblName
		}
	}
}

func buildCtxAndProjection(updateColsArray [][]updateCol, updateExprsArray []tree.Exprs, baseNameMap map[string]string, tblRefs []*TableDef, ctx CompilerContext) ([]*plan.UpdateCtx, tree.SelectExprs, error) {
	var useProjectExprs tree.SelectExprs
	updateCtxs := make([]*plan.UpdateCtx, 0, len(updateColsArray))
	var offset int32 = 0

	for i, updateCols := range updateColsArray {
		// figure out if primary key update
		var priKey string
		var priKeyIdx int32 = -1
		priKeys := ctx.GetPrimaryKeyDef(updateCols[0].dbName, updateCols[0].tblName)
		for _, key := range priKeys {
			if key.IsCPkey {
				break
			}
			for _, updateCol := range updateCols {
				if key.Name == updateCol.colDef.Name {
					e, _ := tree.NewUnresolvedName(updateCol.dbName, updateCol.aliasTblName, key.Name)
					useProjectExprs = append(useProjectExprs, tree.SelectExpr{Expr: e})
					priKey = key.Name
					priKeyIdx = offset
					break
				}
			}
		}
		// use hide key to update if primary key will not be updated
		var hideKeyIdx int32 = -1
		hideKey := ctx.GetHideKeyDef(updateCols[0].dbName, updateCols[0].tblName).GetName()
		if priKeyIdx == -1 {
			if hideKey == "" {
				return nil, nil, moerr.NewInternalError("internal error: cannot find hide key")
			}
			e, _ := tree.NewUnresolvedName(updateCols[0].dbName, updateCols[0].aliasTblName, hideKey)
			useProjectExprs = append(useProjectExprs, tree.SelectExpr{Expr: e})
			hideKeyIdx = offset
		}
		// construct projection for list of update expr
		for _, expr := range updateExprsArray[i] {
			useProjectExprs = append(useProjectExprs, tree.SelectExpr{Expr: expr})
		}

		// construct other cols and table offset
		var otherAttrs []string = nil
		var k int
		// get table reference index
		for k = 0; k < len(tblRefs); k++ {
			if updateCols[0].tblName == tblRefs[k].Name {
				break
			}
		}
		orderAttrs := make([]string, 0, len(tblRefs[k].Cols)-1)
		// figure out other cols that will not be updated
		var onUpdateCols []updateCol
		for _, col := range tblRefs[k].Cols {
			if col.Name == hideKey {
				continue
			}

			orderAttrs = append(orderAttrs, col.Name)

			isUpdateCol := false
			for _, updateCol := range updateCols {
				if updateCol.colDef.Name == col.Name {
					isUpdateCol = true
				}
			}
			if !isUpdateCol {
				if col.OnUpdate != nil {
					onUpdateCols = append(onUpdateCols, updateCol{colDef: col})
					useProjectExprs = append(useProjectExprs, tree.SelectExpr{Expr: &tree.UpdateVal{}})
				} else {
					otherAttrs = append(otherAttrs, col.Name)
					e, _ := tree.NewUnresolvedName(updateCols[0].aliasTblName, col.Name)
					useProjectExprs = append(useProjectExprs, tree.SelectExpr{Expr: e})
				}
			}
		}
		offset += int32(len(orderAttrs)) + 1

		ct := &plan.UpdateCtx{
			DbName:     updateCols[0].dbName,
			TblName:    updateCols[0].tblName,
			PriKey:     priKey,
			PriKeyIdx:  priKeyIdx,
			HideKey:    hideKey,
			HideKeyIdx: hideKeyIdx,
			OtherAttrs: otherAttrs,
			OrderAttrs: orderAttrs,
		}
		for _, u := range updateCols {
			ct.UpdateCols = append(ct.UpdateCols, u.colDef)
		}
		for _, u := range onUpdateCols {
			ct.UpdateCols = append(ct.UpdateCols, u.colDef)
		}
		if len(priKeys) > 0 && priKeys[0].IsCPkey {
			ct.CompositePkey = priKeys[0]
		}
		updateCtxs = append(updateCtxs, ct)

	}
	return updateCtxs, useProjectExprs, nil
}

func groupUpdateAttrs(updateCols []updateCol, updateExprs tree.UpdateExprs) ([][]updateCol, []tree.Exprs) {
	var updateColsArray [][]updateCol
	var updateExprsArray []tree.Exprs

	groupMap := make(map[string]int)
	for i := 0; i < len(updateCols); i++ {
		// check if it has dealt with same table name
		if _, ok := groupMap[updateCols[i].tblName]; ok {
			continue
		}
		groupMap[updateCols[i].tblName] = 1

		var cols []updateCol
		cols = append(cols, updateCols[i])

		var exprs tree.Exprs
		exprs = append(exprs, updateExprs[i].Expr)

		// group same table name
		for j := i + 1; j < len(updateCols); j++ {
			if updateCols[j].tblName == updateCols[i].tblName {
				cols = append(cols, updateCols[j])
				exprs = append(exprs, updateExprs[j].Expr)
			}
		}

		updateColsArray = append(updateColsArray, cols)
		updateExprsArray = append(updateExprsArray, exprs)
	}
	return updateColsArray, updateExprsArray
}

func extractExprTable(expr tree.TableExpr, tf *tableInfo, ctx CompilerContext) error {
	switch t := expr.(type) {
	case *tree.TableName:
		tblName := string(t.ObjectName)
		if _, ok := tf.baseNameMap[tblName]; ok {
			return nil
		}
		dbName := string(t.SchemaName)
		if dbName == "" {
			dbName = ctx.DefaultDatabase()
		}
		tf.tableNames = append(tf.tableNames, tblName)
		tf.dbNames = append(tf.dbNames, dbName)
		tf.baseNameMap[tblName] = tblName
		return nil
	case *tree.AliasedTableExpr:
		tn, ok := t.Expr.(*tree.TableName)
		if !ok {
			return nil
		}
		if t.As.Cols != nil {
			return moerr.NewInternalError("syntax error at %s", tree.String(t, dialect.MYSQL))
		}
		tblName := string(tn.ObjectName)
		if _, ok := tf.baseNameMap[tblName]; ok {
			return nil
		}
		dbName := string(tn.SchemaName)
		if dbName == "" {
			dbName = ctx.DefaultDatabase()
		}
		tf.tableNames = append(tf.tableNames, tblName)
		tf.dbNames = append(tf.dbNames, dbName)
		asName := string(t.As.Alias)
		if asName != "" {
			tf.baseNameMap[asName] = tblName
		} else {
			tf.baseNameMap[tblName] = tblName
		}
		return nil
	case *tree.JoinTableExpr:
		if err := extractExprTable(t.Left, tf, ctx); err != nil {
			return err
		}
		return extractExprTable(t.Right, tf, ctx)
	default:
		return nil
	}
}

func buildUpdateColumns(exprs tree.UpdateExprs, objRefs []*ObjectRef, tblRefs []*TableDef, baseNameMap map[string]string) ([]updateCol, error) {
	updateCols := make([]updateCol, 0, len(objRefs))
	colCountMap := make(map[string]int)
	for _, expr := range exprs {
		var ctx updateCol
		dbName, tableName, columnName := expr.Names[0].GetNames()
		// check dbName
		if dbName != "" {
			hasFindDbName := false
			for i := range objRefs {
				if objRefs[i].SchemaName == dbName {
					hasFindDbName = true
					if tableName != tblRefs[i].Name {
						return nil, moerr.NewInternalError("cannot find update table of %s in database of %s", tableName, dbName)
					}
				}
			}
			if !hasFindDbName {
				return nil, moerr.NewInternalError("cannot find update database of %s", dbName)
			}
			ctx.dbName = dbName
		}
		// check tableName
		if tableName != "" {
			realTableName, ok := baseNameMap[tableName]
			if !ok {
				return nil, moerr.NewInternalError("the target table %s of the UPDATE is not updatable", tableName)
			}
			hasFindTableName := false
			for i := range tblRefs {
				// table reference
				if tblRefs[i].Name == realTableName {
					hasFindColumnName := false

					for _, col := range tblRefs[i].Cols {
						if col.Name == columnName {
							colName := fmt.Sprintf("%s.%s", realTableName, columnName)
							if cnt, ok := colCountMap[colName]; ok && cnt > 0 {
								return nil, moerr.NewInternalError("the target column %s.%s of the UPDATE is duplicate", tableName, columnName)
							} else {
								colCountMap[colName]++
							}

							hasFindColumnName = true
							ctx.colDef = col
							break
						}
					}
					if !hasFindColumnName {
						return nil, moerr.NewInternalError("the target column %s.%s of the UPDATE is ambiguous", tableName, columnName)
					}

					hasFindTableName = true
					ctx.dbName = objRefs[i].SchemaName

					break
				}
			}
			if !hasFindTableName {
				return nil, moerr.NewInternalError("the target table %s of the UPDATE is not updatable", tableName)
			}
			ctx.tblName = realTableName
			ctx.aliasTblName = tableName
		} else {
			for i := range tblRefs {
				for _, col := range tblRefs[i].Cols {
					if col.Name == columnName {
						colName := fmt.Sprintf("%s.%s", tblRefs[i].Name, columnName)
						if cnt, ok := colCountMap[colName]; ok && cnt > 0 {
							return nil, moerr.NewInternalError("the target column %s of the UPDATE is ambiguous", columnName)
						} else {
							colCountMap[colName]++
						}

						ctx.dbName = objRefs[i].SchemaName
						ctx.tblName = tblRefs[i].Name
						ctx.aliasTblName = tblRefs[i].Name
						ctx.colDef = col

						break
					}
				}
			}
			if ctx.tblName == "" {
				return nil, moerr.NewInternalError("the target column %s is not exists", columnName)
			}
		}
		updateCols = append(updateCols, ctx)
	}
	return updateCols, nil
}

func isSameColumnType(t1 *Type, t2 *Type) bool {
	if t1.Id != t2.Id {
		return false
	}
	if t1.Width == t2.Width && t1.Precision == t2.Precision && t1.Size == t2.Size && t1.Scale == t2.Scale {
		return true
	}
	return true
}
