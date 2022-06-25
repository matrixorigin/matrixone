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
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func (b *build) buildWhere(stmt *tree.Where, qry *Query) (extend.Extend, []*JoinCondition, error) {
	var conds []*JoinCondition

	e, err := b.buildWhereExpr(stmt.Expr, qry)
	if err != nil {
		return nil, nil, err
	}
	if e, err = b.pruneExtend(e, false); err != nil {
		return nil, nil, err
	}
	/*
		if !e.IsLogical() {
			return nil, nil, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("argument of WHERE must be type boolean"))
		}
	*/
	if jp, ok := qry.Scope.Op.(*Join); ok && jp.Type == CROSS { // push down join condition
		if es := extend.AndExtends(e, nil); len(es) > 0 {
			ss := newScopeSet()
			ss.Scopes = qry.Scope.Children
			for i := 0; i < len(es); i++ { // extracting join information
				if left, right, ok := stripEqual(es[i]); ok {
					li, lname, err := b.getJoinAttribute(getUnresolvedName(left), ss)
					if err != nil {
						return nil, nil, err
					}
					ri, rname, err := b.getJoinAttribute(getUnresolvedName(right), ss)
					if err != nil {
						return nil, nil, err
					}
					ss.Conds = append(ss.Conds, &JoinCondition{
						R:     li,
						S:     ri,
						Rattr: lname,
						Sattr: rname,
					})
					es = append(es[:i], es[i+1:]...)
					i--
				}
			}
			if len(ss.Conds) > 0 {
				conds = ss.Conds
				s, err := b.buildQualifiedJoin(INNER, ss)
				if err != nil {
					return nil, nil, err
				}
				qry.Scope = s
				if len(es) == 0 {
					return nil, conds, nil
				}
				e = extendsToAndExtend(es)
			}
		}
	}
	return e, conds, nil
}

func (b *build) buildWhereExpr(n tree.Expr, qry *Query) (extend.Extend, error) {
	switch e := n.(type) {
	case *tree.NumVal:
		return buildValue(e.Value, e.String())
	case *tree.ParenExpr:
		return b.buildWhereExpr(e.Expr, qry)
	case *tree.OrExpr:
		return b.buildOr(e, qry, b.buildWhereExpr)
	case *tree.NotExpr:
		return b.buildNot(e, qry, b.buildWhereExpr)
	case *tree.AndExpr:
		return b.buildAnd(e, qry, b.buildWhereExpr)
	case *tree.UnaryExpr:
		return b.buildUnary(e, qry, b.buildWhereExpr)
	case *tree.BinaryExpr:
		return b.buildBinary(e, qry, b.buildWhereExpr)
	case *tree.ComparisonExpr:
		return b.buildComparison(e, qry, b.buildWhereExpr)
	case *tree.FuncExpr:
		return b.buildFunc(false, e, qry, b.buildWhereExpr)
	case *tree.CastExpr:
		return b.buildCast(e, qry, b.buildWhereExpr)
	case *tree.RangeCond:
		return b.buildBetween(e, qry, b.buildWhereExpr)
	case *tree.UnresolvedName:
		return b.buildAttribute(e, qry)
	}
	return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("'%v' is not support now", n))
}
