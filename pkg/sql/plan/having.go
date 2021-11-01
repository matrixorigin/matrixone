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
	"matrixone/pkg/errno"
	"matrixone/pkg/sql/colexec/extend"
	"matrixone/pkg/sql/errors"
	"matrixone/pkg/sql/tree"
)

func (b *build) buildHaving(stmt *tree.Where, qry *Query) error {
	e, err := b.buildHavingExpr(stmt.Expr, qry)
	if err != nil {
		return err
	}
	if e, err = b.pruneExtend(e); err != nil {
		return err
	}
	b.flg = true
	qry.RestrictConds = append(qry.RestrictConds, e)
	return nil
}

func (b *build) buildHavingExpr(n tree.Expr, qry *Query) (extend.Extend, error) {
	switch e := n.(type) {
	case *tree.NumVal:
		return buildValue(e.Value)
	case *tree.ParenExpr:
		return b.buildHavingExpr(e.Expr, qry)
	case *tree.OrExpr:
		return b.buildOr(e, qry, b.buildHavingExpr)
	case *tree.NotExpr:
		return b.buildNot(e, qry, b.buildHavingExpr)
	case *tree.AndExpr:
		return b.buildAnd(e, qry, b.buildHavingExpr)
	case *tree.UnaryExpr:
		return b.buildUnary(e, qry, b.buildHavingExpr)
	case *tree.BinaryExpr:
		return b.buildBinary(e, qry, b.buildHavingExpr)
	case *tree.ComparisonExpr:
		return b.buildComparison(e, qry, b.buildHavingExpr)
	case *tree.FuncExpr:
		return b.buildHavingFunc(e, qry, b.buildHavingExpr)
	case *tree.CastExpr:
		return b.buildCast(e, qry, b.buildHavingExpr)
	case *tree.RangeCond:
		return b.buildBetween(e, qry, b.buildHavingExpr)
	case *tree.UnresolvedName:
		return b.buildAttribute2(b.flg, e, qry)
	}
	return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("'%v' is not support now", n))
}
