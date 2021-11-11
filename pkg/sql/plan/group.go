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
	"matrixone/pkg/sql/parsers/tree"
)

func (b *build) buildGroupBy(exprs tree.GroupBy, qry *Query) error {
	for _, expr := range exprs {
		e, err := b.buildGroupByExpr(expr, qry)
		if err != nil {
			return err
		}
		if e, err = b.pruneExtend(e); err != nil {
			return err
		}
		{
			var rel string

			attrs := e.Attributes()
			mp := make(map[string]int) // relations map
			for _, attr := range attrs {
				rels, _, err := qry.getAttribute1(false, attr)
				if err != nil {
					return err
				}
				for i := range rels {
					if len(rel) == 0 {
						rel = rels[i]
					}
					mp[rels[i]]++
				}
			}
			if len(mp) == 0 {
				return errors.New(errno.SQLStatementNotYetComplete, fmt.Sprintf("Illegal expression '%s' in group by", e))
			}
			if len(mp) > 1 {
				return errors.New(errno.SQLStatementNotYetComplete, fmt.Sprintf("attributes involved in the group by must belong to the same relation"))
			}
			if _, ok := e.(*extend.Attribute); !ok {
				if i := qry.RelsMap[rel].ExistProjection(e.String()); i == -1 {
					qry.RelsMap[rel].AddProjection(&ProjectionExtend{
						E:     e,
						Ref:   1,
						Alias: e.String(),
					})
					{
						for _, attr := range attrs {
							qry.getAttribute1(true, attr)
						}
					}
				} else {
					qry.RelsMap[rel].ProjectionExtends[i].Ref++
				}
			} else {
				qry.getAttribute1(true, e.(*extend.Attribute).Name)
			}
			qry.FreeAttrs = append(qry.FreeAttrs, e.String())
		}
	}
	return nil
}

func (b *build) buildGroupByExpr(n tree.Expr, qry *Query) (extend.Extend, error) {
	switch e := n.(type) {
	case *tree.NumVal:
		return buildValue(e.Value)
	case *tree.ParenExpr:
		return b.buildGroupByExpr(e.Expr, qry)
	case *tree.OrExpr:
		return b.buildOr(e, qry, b.buildGroupByExpr)
	case *tree.NotExpr:
		return b.buildNot(e, qry, b.buildGroupByExpr)
	case *tree.AndExpr:
		return b.buildAnd(e, qry, b.buildGroupByExpr)
	case *tree.UnaryExpr:
		return b.buildUnary(e, qry, b.buildGroupByExpr)
	case *tree.BinaryExpr:
		return b.buildBinary(e, qry, b.buildGroupByExpr)
	case *tree.ComparisonExpr:
		return b.buildComparison(e, qry, b.buildGroupByExpr)
	case *tree.FuncExpr:
		return b.buildFunc(false, e, qry, b.buildGroupByExpr)
	case *tree.CastExpr:
		return b.buildCast(e, qry, b.buildGroupByExpr)
	case *tree.RangeCond:
		return b.buildBetween(e, qry, b.buildGroupByExpr)
	case *tree.UnresolvedName:
		return b.buildAttribute1(false, e, qry)
	}
	return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("'%v' is not support now", n))
}
