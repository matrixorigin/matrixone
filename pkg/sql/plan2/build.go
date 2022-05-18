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

func BuildPlan(ctx CompilerContext, stmt tree.Statement) (*plan.Plan, error) {
	runBuildSelect := func(stmt *tree.Select) (*plan.Plan, error) {
		query, selectCtx := newQueryAndSelectCtx(plan.Query_SELECT)
		err := buildSelect(stmt, ctx, query, selectCtx)
		return &plan.Plan{
			Plan: &plan.Plan_Query{
				Query: query,
			},
		}, err
	}

	switch stmt := stmt.(type) {
	case *tree.Select:
		return runBuildSelect(stmt)
	case *tree.ParenSelect:
		return runBuildSelect(stmt.Select)
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
	default:
		return nil, errors.New(errno.SQLStatementNotYetComplete, fmt.Sprintf("unexpected statement: '%v'", tree.String(stmt, dialect.MYSQL)))
	}
}
