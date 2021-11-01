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

func (b *build) buildOrderBy(orders tree.OrderBy, qry *Query) error {
	for _, order := range orders {
		e, err := b.buildOrderByExpr(order.Expr, qry)
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
				rels, _, err := qry.getAttribute2(false, attr)
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
			if _, ok := e.(*extend.Attribute); !ok {
				if len(mp) == 1 {
					b.stripOrderByRelation(rel, e, qry)
				} else {
					b.stripOrderByQuery(e, qry)
				}
			} else {
				qry.getAttribute2(true, e.(*extend.Attribute).Name)
			}
		}
		qry.Fields = append(qry.Fields, &Field{
			Attr: e.String(),
			Type: Direction(order.Direction),
		})
	}
	return nil
}

func (b *build) stripOrderByQuery(e extend.Extend, qry *Query) error {
	for i := range qry.ProjectionExtends {
		if qry.ProjectionExtends[i].Alias == e.String() {
			qry.ProjectionExtends[i].IncRef()
			return nil
		}
	}
	attrs := e.Attributes()
	for _, attr := range attrs {
		qry.getAttribute2(true, attr)
	}
	if _, ok := e.(*extend.Attribute); !ok {
		qry.ProjectionExtends = append(qry.ProjectionExtends, &ProjectionExtend{
			Ref:   1,
			E:     e,
			Alias: e.String(),
		})
	}
	return nil
}

func (b *build) stripOrderByRelation(name string, e extend.Extend, qry *Query) error {
	rel := qry.RelsMap[name]
	if i := rel.ExistProjection(e.String()); i >= 0 {
		rel.ProjectionExtends[i].IncRef()
		return nil
	}
	if i := rel.ExistAggregation(e.String()); i >= 0 {
		rel.Aggregations[i].IncRef()
		return nil
	}
	attrs := e.Attributes()
	for _, attr := range attrs {
		qry.getAttribute2(true, attr)
	}
	if _, ok := e.(*extend.Attribute); !ok {
		rel.ProjectionExtends = append(rel.ProjectionExtends, &ProjectionExtend{
			Ref:   1,
			E:     e,
			Alias: e.String(),
		})
	}
	return nil
}

func (b *build) buildOrderByExpr(n tree.Expr, qry *Query) (extend.Extend, error) {
	switch e := n.(type) {
	case *tree.NumVal:
		return buildValue(e.Value)
	case *tree.ParenExpr:
		return b.buildOrderByExpr(e.Expr, qry)
	case *tree.OrExpr:
		return b.buildOr(e, qry, b.buildOrderByExpr)
	case *tree.NotExpr:
		return b.buildNot(e, qry, b.buildOrderByExpr)
	case *tree.AndExpr:
		return b.buildAnd(e, qry, b.buildOrderByExpr)
	case *tree.UnaryExpr:
		return b.buildUnary(e, qry, b.buildOrderByExpr)
	case *tree.BinaryExpr:
		return b.buildBinary(e, qry, b.buildOrderByExpr)
	case *tree.ComparisonExpr:
		return b.buildComparison(e, qry, b.buildOrderByExpr)
	case *tree.FuncExpr:
		return b.buildFunc(false, e, qry, b.buildOrderByExpr)
	case *tree.CastExpr:
		return b.buildCast(e, qry, b.buildOrderByExpr)
	case *tree.RangeCond:
		return b.buildBetween(e, qry, b.buildOrderByExpr)
	case *tree.UnresolvedName:
		return b.buildAttribute2(false, e, qry)
	}
	return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("'%v' is not support now", n))
}
