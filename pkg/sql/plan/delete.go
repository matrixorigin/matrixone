// Copyright 2021 Matrix Origin
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
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/rewrite"
)

func (b *build) buildDeletePlan(deleteStmt *tree.Delete, plan *Delete) error {
	selectStmt := buildSelectStmtFromDelete(deleteStmt)
	//selectStmt = rewrite.Rewrite(selectStmt)
	selectStmt = rewrite.AstRewrite(selectStmt)

	b.isModify = true
	queryPlan, err := b.BuildStatement(selectStmt)
	if err != nil {
		return err
	}
	plan.Qry, _ = queryPlan.(*Query)
	return nil
}

func buildStarProjection() tree.SelectExprs {
	expr := tree.SelectExpr{
		Expr: tree.UnqualifiedStar{},
	}
	return tree.SelectExprs{expr}
}

func buildSelectStmtFromDelete(d *tree.Delete) tree.Statement {
	if len(d.OrderBy) > 0 && (d.Where == nil && d.Limit == nil) {
		d.OrderBy = nil
	}
	s := &tree.SelectClause{
		Exprs: buildStarProjection(),
		From:  &tree.From{Tables: tree.TableExprs{d.Table}},
		Where: d.Where,
	}
	return &tree.Select{
		Select:  s,
		OrderBy: d.OrderBy,
		Limit:   d.Limit,
	}
}
