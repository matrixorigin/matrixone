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
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/errno"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/extend"
	"github.com/matrixorigin/matrixone/pkg/sql/errors"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/rewrite"
)

func (b *build) buildUpdatePlan(updateStmt *tree.Update, plan *Update) error {
	selectStmt, err := buildSelectStmtFromUpdate(updateStmt)
	if err != nil {
		return err
	}
	//selectStmt = rewrite.Rewrite(selectStmt)
	selectStmt = rewrite.AstRewrite(selectStmt)

	b.isModify = true
	queryPlan, err := b.BuildStatement(selectStmt)
	if err != nil {
		return err
	}
	plan.Qry, _ = queryPlan.(*Query)
	if err := b.buildUpdateList(updateStmt.Exprs, plan); err != nil {
		return nil
	}
	return nil
}

func (b *build) buildUpdateList(exprs tree.UpdateExprs, plan *Update) error {
	var err error
	attrs := make(map[string]struct{})
	qry := *plan.Qry
	for _, expr := range exprs {
		// get the update column name
		updateName := string(expr.Names[0].Parts[0])

		// check if it has duplicate column name
		if _, ok := attrs[updateName]; ok {
			return errors.New(errno.SyntaxErrororAccessRuleViolation, "duplicate column name")
		} else {
			attrs[updateName] = struct{}{}
			if err != nil {
				return err
			}
			plan.UpdateAttrs = append(plan.UpdateAttrs, updateName)
		}
		// build one of update extend
		etd, err := b.buildWhereExpr(expr.Expr, &qry)
		if err != nil {
			return err
		}
		// construct updateExtend
		updateExtend := extend.UpdateExtend{
			Attr: extend.Attribute{
				Name: updateName,
				Type: qry.Scope.Result.AttrsMap[updateName].Type.Oid,
			},
			UpdateExtend: etd,
		}
		plan.UpdateList = append(plan.UpdateList, updateExtend)
	}
	// build other attribute
	for _, name := range qry.Result {
		if _, ok := attrs[name]; !ok {
			plan.OtherAttrs = append(plan.OtherAttrs, name)
		}
	}
	return nil
}

func buildSelectStmtFromUpdate(p *tree.Update) (tree.Statement, error) {
	exprs, err := buildUpdateProjection(p)
	if err != nil {
		return nil, err
	}
	s := &tree.SelectClause{
		Exprs: exprs,
		From:  &tree.From{Tables: tree.TableExprs{p.Table}},
		Where: p.Where,
	}
	return &tree.Select{
		Select:  s,
		OrderBy: p.OrderBy,
		Limit:   p.Limit,
	}, nil
}

func buildUpdateProjection(p *tree.Update) (tree.SelectExprs, error) {
	var selectExprs tree.SelectExprs
	// check update list
	for _, expr := range p.Exprs {
		if len(expr.Names) != 1 {
			return nil, errors.New(errno.CaseNotFound, "the set list of update must be one")
		}
		if !isDuplicated(expr.Names[0], &selectExprs) {
			selectExprs = append(selectExprs, tree.SelectExpr{Expr: expr.Names[0]})
		}
	}
	return tree.SelectExprs{tree.SelectExpr{Expr: tree.UnqualifiedStar{}}}, nil
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
