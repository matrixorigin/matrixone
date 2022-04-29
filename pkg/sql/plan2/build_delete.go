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
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func buildDelete(stmt *tree.Delete, ctx CompilerContext, query *Query) error {
	selectStmt := &tree.Select{
		Select: &tree.SelectClause{
			//todo confirm how to set row_id
			Exprs: tree.SelectExprs{
				tree.SelectExpr{
					Expr: tree.UnqualifiedStar{},
				},
			},
			From:  &tree.From{Tables: tree.TableExprs{stmt.Table}},
			Where: stmt.Where,
		},
		OrderBy: stmt.OrderBy,
		Limit:   stmt.Limit,
	}
	selectCtx := &SelectContext{
		tableAlias:  make(map[string]string),
		columnAlias: make(map[string]*plan.Expr),
	}
	return buildSelect(selectStmt, ctx, query, selectCtx)
}
