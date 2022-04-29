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

func buildPlan(ctx CompilerContext, stmt tree.Statement) (*Query, error) {
	query := &Query{
		Steps: []int32{0},
	}
	err := buildStatement(stmt, ctx, query)
	if err != nil {
		return nil, err
	}
	return query, nil
}

func buildStatement(stmt tree.Statement, ctx CompilerContext, query *Query) error {
	selectCtx := &SelectContext{
		tableAlias:  make(map[string]string),
		columnAlias: make(map[string]*plan.Expr),
	}
	switch stmt := stmt.(type) {
	case *tree.Select:
		query.StmtType = plan.Query_SELECT
		return buildSelect(stmt, ctx, query, selectCtx)
	case *tree.ParenSelect:
		query.StmtType = plan.Query_SELECT
		return buildSelect(stmt.Select, ctx, query, selectCtx)
	case *tree.Insert:
		query.StmtType = plan.Query_INSERT
		return buildInsert(stmt, ctx, query)
	case *tree.Update:
		query.StmtType = plan.Query_UPDATE
		return buildUpdate(stmt, ctx, query)
	case *tree.Delete:
		query.StmtType = plan.Query_DELETE
		return buildDelete(stmt, ctx, query)
	}
	return errors.New(errno.SQLStatementNotYetComplete, fmt.Sprintf("unexpected statement: '%v'", tree.String(stmt, dialect.MYSQL)))
}
