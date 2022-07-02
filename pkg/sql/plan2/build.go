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

func runBuildSelectByBinder(stmtType plan.Query_StatementType, ctx CompilerContext, stmt *tree.Select) (*Plan, error) {
	// query, binderCtx := newQueryAndSelectCtx(stmtType)
	// nodeId, err := buildSelect(stmt, ctx, query, binderCtx)
	// query.Steps = append(query.Steps, nodeId)
	// return &Plan{
	// 	Plan: &plan.Plan_Query{
	// 		Query: query,
	// 	},
	// }, err

	builder := NewQueryBuilder(stmtType, ctx)
	bindCtx := NewBindContext(builder, nil)
	rootId, err := builder.buildSelect(stmt, bindCtx, true)
	builder.qry.Steps = append(builder.qry.Steps, rootId)
	if err != nil {
		return nil, err
	}
	query, err := builder.createQuery()
	if err != nil {
		return nil, err
	}
	return &Plan{
		Plan: &plan.Plan_Query{
			Query: query,
		},
	}, err
}

func BuildPlan(ctx CompilerContext, stmt tree.Statement) (*Plan, error) {
	switch stmt := stmt.(type) {
	case *tree.Select:
		return runBuildSelectByBinder(plan.Query_SELECT, ctx, stmt)
	case *tree.ParenSelect:
		return runBuildSelectByBinder(plan.Query_SELECT, ctx, stmt.Select)
	case *tree.Insert:
		return buildInsert(stmt, ctx)
	case *tree.Update:
		return buildUpdate(stmt, ctx)
	case *tree.Delete:
		return buildDelete(stmt, ctx)
	case *tree.BeginTransaction:
		return buildBeginTransaction(stmt, ctx)
	case *tree.CommitTransaction:
		return buildCommitTransaction(stmt, ctx)
	case *tree.RollbackTransaction:
		return buildRollbackTransaction(stmt, ctx)
	case *tree.CreateDatabase:
		return buildCreateDatabase(stmt, ctx)
	case *tree.DropDatabase:
		return buildDropDatabase(stmt, ctx)
	case *tree.CreateTable:
		return buildCreateTable(stmt, ctx)
	case *tree.DropTable:
		return buildDropTable(stmt, ctx)
	case *tree.CreateIndex:
		return buildCreateIndex(stmt, ctx)
	case *tree.DropIndex:
		return buildDropIndex(stmt, ctx)
	case *tree.ShowCreateDatabase:
		return buildShowCreateDatabase(stmt, ctx)
	case *tree.ShowCreateTable:
		return buildShowCreateTable(stmt, ctx)
	case *tree.ShowDatabases:
		return buildShowDatabases(stmt, ctx)
	case *tree.ShowTables:
		return buildShowTables(stmt, ctx)
	case *tree.ShowColumns:
		return buildShowColumns(stmt, ctx)
	case *tree.ShowIndex:
		return buildShowIndex(stmt, ctx)
	case *tree.ShowVariables:
		return buildShowVariables(stmt, ctx)
	case *tree.ShowWarnings:
		return buildShowWarnings(stmt, ctx)
	case *tree.ShowErrors:
		return buildShowErrors(stmt, ctx)
	case *tree.ShowStatus:
		return buildShowStatus(stmt, ctx)
	case *tree.ShowProcessList:
		return buildShowProcessList(stmt, ctx)
	case *tree.SetVar:
		return buildSetVariables(stmt, ctx)
	default:
		return nil, errors.New(errno.SQLStatementNotYetComplete, fmt.Sprintf("unexpected statement: '%v'", tree.String(stmt, dialect.MYSQL)))
	}
}

//GetResultColumnsFromPlan
func GetResultColumnsFromPlan(p *Plan) []*ColDef {
	getResultColumnsByProjectionlist := func(query *Query) []*ColDef {
		lastNode := query.Nodes[query.Steps[len(query.Steps)-1]]
		columns := make([]*ColDef, len(lastNode.ProjectList))
		if len(query.Headings) > 0 { // use refactor plan2
			for idx, expr := range lastNode.ProjectList {
				columns[idx] = &ColDef{
					Name: query.Headings[idx],
					Typ:  expr.Typ,
				}
			}
		} else {
			for idx, expr := range lastNode.ProjectList {
				columns[idx] = &ColDef{
					Name: expr.ColName,
					Typ:  expr.Typ,
				}
			}
		}
		return columns
	}

	switch logicPlan := p.Plan.(type) {
	case *plan.Plan_Query:
		switch logicPlan.Query.StmtType {
		case plan.Query_SELECT:
			return getResultColumnsByProjectionlist(logicPlan.Query)
		default:
			// insert/update/delete statement will return nil
			return nil
		}
	case *plan.Plan_Tcl:
		// begin/commmit/rollback statement will return nil
		return nil
	case *plan.Plan_Ddl:
		switch logicPlan.Ddl.DdlType {
		case plan.DataDefinition_SHOW_VARIABLES:
			typ := &plan.Type{
				Id:    plan.Type_VARCHAR,
				Width: 1024,
			}
			return []*ColDef{
				{Typ: typ, Name: "Variable_name"},
				{Typ: typ, Name: "Value"},
			}
		case plan.DataDefinition_SHOW_CREATEDATABASE:
			typ := &plan.Type{
				Id:    plan.Type_VARCHAR,
				Width: 1024,
			}
			return []*ColDef{
				{Typ: typ, Name: "Database"},
				{Typ: typ, Name: "Create Database"},
			}
		case plan.DataDefinition_SHOW_CREATETABLE:
			typ := &plan.Type{
				Id:    plan.Type_VARCHAR,
				Width: 1024,
			}
			return []*ColDef{
				{Typ: typ, Name: "Table"},
				{Typ: typ, Name: "Create Table"},
			}
		default:
			// show statement(except show variables) will return a query
			if logicPlan.Ddl.Query != nil {
				return getResultColumnsByProjectionlist(logicPlan.Ddl.Query)
			}
			return nil
		}
	}
	return nil
}
