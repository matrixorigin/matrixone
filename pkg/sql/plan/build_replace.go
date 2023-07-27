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
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func buildReplace(stmt *tree.Replace, ctx CompilerContext, isPrepareStmt bool) (p *Plan, err error) {
	return &Plan{
		Plan: &plan.Plan_Query{
			Query: &plan.Query{
				StmtType: plan.Query_REPLACE,
				Nodes:    []*plan.Node{{NodeType: plan.Node_REPLACE, ReplaceCtx: &plan.ReplaceCtx{}}},
			},
		},
	}, nil
}

func initReplaceStmt(builder *QueryBuilder, bindCtx *BindContext, stmt *tree.Replace, info *dmlSelectInfo) (bool, map[int]int, error) {
	var insertColumns []string
	tableDef := info.tblInfo.tableDefs[0]
	syntaxHasColumnNames := false
	colToIdx := make(map[string]int)
	oldColPosMap := make(map[string]int)
	tableDef.Name2ColIndex = make(map[string]int32)
	for i, col := range tableDef.Cols {
		colToIdx[col.Name] = i
		oldColPosMap[col.Name] = i
		tableDef.Name2ColIndex[col.Name] = int32(i)
	}
	info.tblInfo.oldColPosMap = append(info.tblInfo.oldColPosMap, oldColPosMap)
	info.tblInfo.newColPosMap = append(info.tblInfo.newColPosMap, oldColPosMap)

	checkInsertPkDup := true
	pkPosInValues := make(map[int]int)

	if stmt.Columns == nil {
		for _, col := range tableDef.Cols {
			// Hide pk can not added, because this column is auto increment
			// column. This must be fill by auto increment
			if col.Name != catalog.FakePrimaryKeyColName {
				insertColumns = append(insertColumns, col.Name)
			}
		}
	} else {
		syntaxHasColumnNames = true
		for _, column := range stmt.Columns {
			colName := string(column)
			if _, ok := colToIdx[colName]; !ok {
				return false, nil, moerr.NewBadFieldError(builder.GetContext(), colName, tableDef.Name)
			}
			insertColumns = append(insertColumns, colName)
		}
	}

	var astSlt *tree.Select
	switch slt := stmt.Rows.Select.(type) {
	// rewrite 'insert into tbl values (1,1)' to 'insert into tbl select * from (values row(1,1))'
	case *tree.ValuesClause:
		isAllDefault := false
		if slt.Rows[0] == nil {
			isAllDefault = true
		}
		if isAllDefault {
			for j, row := range slt.Rows {
				if row != nil {
					return false, nil, moerr.NewWrongValueCountOnRow(builder.GetContext(), j+1)
				}
			}
		} else {
			colCount := len(insertColumns)
			for j, row := range slt.Rows {
				if len(row) != colCount {
					return false, nil, moerr.NewWrongValueCountOnRow(builder.GetContext(), j+1)
				}
			}
		}

		//example1:insert into a values ();
		//but it does not work at the case:
		//insert into a(a) values (); insert into a values (0),();
		if isAllDefault && syntaxHasColumnNames {
			return false, nil, moerr.NewInvalidInput(builder.GetContext(), "insert values does not match the number of columns")
		}
		checkInsertPkDup = len(slt.Rows) > 1
		if !checkInsertPkDup {
			if len(tableDef.Pkey.Names) == 1 {
				for idx, name := range insertColumns {
					if name == tableDef.Pkey.PkeyColName {
						pkPosInValues[idx] = 0
						break
					}
				}
			} else {
				pkNameMap := make(map[string]int)
				for pkIdx, pkName := range tableDef.Pkey.Names {
					pkNameMap[pkName] = pkIdx
				}
				for idx, name := range insertColumns {
					if pkIdx, ok := pkNameMap[name]; ok {
						pkPosInValues[idx] = pkIdx
					}
				}
			}
		}

		err := buildValueScan(isAllDefault, info, builder, bindCtx, tableDef, slt, insertColumns, colToIdx)
		if err != nil {
			return false, nil, err
		}
	case *tree.SelectClause:
		astSlt = stmt.Rows
		subCtx := NewBindContext(builder, bindCtx)
		var err error
		info.rootId, err = builder.buildSelect(astSlt, subCtx, false)
		if err != nil {
			return false, nil, err
		}
	case *tree.ParenSelect:
		astSlt = slt.Select

		subCtx := NewBindContext(builder, bindCtx)
		var err error
		info.rootId, err = builder.buildSelect(astSlt, subCtx, false)
		if err != nil {
			return false, nil, err
		}
	default:
		return false, nil, moerr.NewInvalidInput(builder.GetContext(), "insert has unknown select statement")
	}

	err := builder.addBinding(info.rootId, tree.AliasClause{
		Alias: derivedTableName,
	}, bindCtx)
	if err != nil {
		return false, nil, err
	}

	lastNode := builder.qry.Nodes[info.rootId]
	if len(insertColumns) != len(lastNode.ProjectList) {
		return false, nil, moerr.NewInvalidInput(builder.GetContext(), "insert values does not match the number of columns")
	}

	tag := builder.qry.Nodes[info.rootId].BindingTags[0]
	info.derivedTableId = info.rootId
	oldProject := append([]*Expr{}, lastNode.ProjectList...)

	insertColToExpr := make(map[string]*Expr)
	for i, column := range insertColumns {
		colIdx := colToIdx[column]
		projExpr := &plan.Expr{
			Typ: oldProject[i].Typ,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: tag,
					ColPos: int32(i),
				},
			},
		}
		projExpr, err = forceCastExpr(builder.GetContext(), projExpr, tableDef.Cols[colIdx].Typ)
		if err != nil {
			return false, nil, err
		}
		insertColToExpr[column] = projExpr
	}

	// have tables : t1(a default 0, b int, pk(a,b)) ,  t2(j int,k int)
	// rewrite 'insert into t1 select * from t2' to
	// select 'select _t.j, _t.k from (select * from t2) _t(j,k)
	// --------
	// rewrite 'insert into t1(b) values (1)' to
	// select 'select 0, _t.column_0 from (select * from values (1)) _t(column_0)
	projectList := make([]*Expr, 0, len(tableDef.Cols))
	for _, col := range tableDef.Cols {
		if oldExpr, exists := insertColToExpr[col.Name]; exists {
			projectList = append(projectList, oldExpr)
		} else {
			defExpr, err := getDefaultExpr(builder.GetContext(), col)
			if err != nil {
				return false, nil, err
			}
			projectList = append(projectList, defExpr)
		}
	}

	// append ProjectNode
	projectCtx := NewBindContext(builder, bindCtx)
	lastTag := builder.genNewTag()
	info.rootId = builder.appendNode(&plan.Node{
		NodeType:    plan.Node_PROJECT,
		ProjectList: projectList,
		Children:    []int32{info.rootId},
		BindingTags: []int32{lastTag},
	}, projectCtx)

	info.projectList = make([]*Expr, 0, len(projectList))
	info.derivedTableId = info.rootId
	for i, e := range projectList {
		info.projectList = append(info.projectList, &plan.Expr{
			Typ: e.Typ,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: lastTag,
					ColPos: int32(i),
				},
			},
		})
	}
	info.idx = int32(len(info.projectList))
	return checkInsertPkDup, pkPosInValues, nil
}
