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

func (b *build) buildWhere(stmt *tree.Where, qry *Query) error {
	e, err := b.buildWhereExpr(stmt.Expr, qry)
	if err != nil {
		return err
	}
	if e, err = b.pruneExtend(e, false); err != nil {
		return err
	}
	if es := extend.AndExtends(e, nil); len(es) > 0 { // push down join condition
		for i := 0; i < len(es); i++ { // extracting join information
			if left, right, ok := stripEqual(es[i]); ok {
				r, rattr, err := qry.getJoinAttribute(false, qry.Rels, left)
				if err != nil {
					return err
				}
				s, sattr, err := qry.getJoinAttribute(false, qry.Rels, right)
				if err != nil {
					return err
				}
				if r != s {
					if qry.RelsMap[r].AttrsMap[rattr].Type.Oid != qry.RelsMap[s].AttrsMap[sattr].Type.Oid {
						return errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("unsupport join condition '%v'", es[i]))
					}
					qry.Conds = append(qry.Conds, &JoinCondition{
						R:     r,
						S:     s,
						Rattr: rattr,
						Sattr: sattr,
					})
					es = append(es[:i], es[i+1:]...)
					i--
				}
			}
		}
		if len(es) == 0 {
			return nil
		}
		e = extendsToAndExtend(es)
	}
	if es := andExtends(qry, e, nil); len(es) > 0 { // push down restrict
		for i := 0; i < len(es); i++ {
			if ok := b.pushDownRestrict(es[i], qry); ok {
				es = append(es[:i], es[i+1:]...)
				i--
			}
		}
		if len(es) > 0 {
			qry.RestrictConds = append(qry.RestrictConds, extendsToAndExtend(es))
		}
		return nil
	}
	qry.RestrictConds = append(qry.RestrictConds, e)
	return nil
}

func (b *build) buildWhereExpr(n tree.Expr, qry *Query) (extend.Extend, error) {
	switch e := n.(type) {
	case *tree.NumVal:
		return buildValue(e.Value)
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
		return b.buildAttribute0(true, e, qry)
	}
	return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("'%v' is not support now", n))
}

func (b *build) pushDownRestrict(e extend.Extend, qry *Query) bool {
	var name string

	attrs := e.Attributes()
	mp := make(map[string]int)
	for _, attr := range attrs {
		if names, _, err := qry.getAttribute0(false, attr); err != nil {
			return false
		} else {
			for i := 0; i < len(names); i++ {
				mp[names[i]]++
				if len(name) == 0 {
					name = names[i]
				}
			}
		}
	}
	if len(mp) == 1 {
		qry.RelsMap[name].AddRestrict(pruneExtend(e))
		return true
	}
	return false
}
