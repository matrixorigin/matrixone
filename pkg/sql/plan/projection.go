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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/errno"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/extend"
	"github.com/matrixorigin/matrixone/pkg/sql/errors"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func (b *build) buildProjection(exprs tree.SelectExprs, qry *Query) error {
	es := make([]*ProjectionExtend, 0, len(exprs))
	for _, expr := range exprs {
		if _, ok := expr.Expr.(tree.UnqualifiedStar); ok {
			for _, rel := range qry.Rels {
				attrs := qry.RelsMap[rel].GetAttributes()
				for _, attr := range attrs {
					names, _, err := qry.getAttribute0(false, attr.Name)
					if err != nil {
						return err
					}
					attr.IncRef()
					if len(names) > 1 {
						qry.ResultAttributes = append(qry.ResultAttributes, &Attribute{
							Type: attr.Type,
							Name: rel + "." + attr.Name,
						})
					} else {
						qry.ResultAttributes = append(qry.ResultAttributes, &Attribute{
							Name: attr.Name,
							Type: attr.Type,
						})
					}
				}
			}
			continue
		}
		e, err := b.buildProjectionExpr(expr.Expr, qry)
		if err != nil {
			return err
		}
		if e, err = b.pruneExtend(e); err != nil {
			return err
		}
		{
			if len(expr.As) > 0 {
				qry.ResultAttributes = append(qry.ResultAttributes, &Attribute{
					Name: string(expr.As),
					Type: types.Type{Oid: e.ReturnType()},
				})
			} else {
				qry.ResultAttributes = append(qry.ResultAttributes, &Attribute{
					Name: e.String(),
					Type: types.Type{Oid: e.ReturnType()},
				})

			}
		}
		if len(expr.As) > 0 {
			es = append(es, &ProjectionExtend{
				Ref:   1,
				E:     e,
				Alias: string(expr.As),
			})
		} else if _, ok := e.(*extend.Attribute); !ok {
			es = append(es, &ProjectionExtend{
				Ref:   1,
				E:     e,
				Alias: e.String(),
			})
		}
	}
	{ // check duplicate column
		mp := make(map[string]uint8)
		for _, attr := range qry.ResultAttributes {
			mp[attr.Name]++
		}
		for k, v := range mp {
			if v > 1 {
				return errors.New(errno.DuplicateColumn, fmt.Sprintf("Duplicate column name '%s'", k))
			}
		}
	}
	for i := 0; i < len(es); i++ {
		{
			var rn string

			attrs := es[i].E.Attributes()
			mp := make(map[string]int) // relations map
			for _, attr := range attrs {
				rns, _, err := qry.getAttribute2(false, attr)
				if err != nil {
					return err
				}
				for i := range rns {
					if len(rn) == 0 {
						rn = rns[i]
					}
					mp[rns[i]]++
				}
			}
			if len(mp) == 1 {
				rel := qry.RelsMap[rn]
				if ok := rel.ExistAggregations(attrs); !ok {
					rel.AddProjection(es[i])
					es = append(es[:i], es[i+1:]...)
					i--
				}
			}
		}
	}
	qry.ProjectionExtends = es
	return nil
}

func (b *build) buildProjectionExpr(n tree.Expr, qry *Query) (extend.Extend, error) {
	switch e := n.(type) {
	case *tree.NumVal:
		return buildValue(e.Value)
	case *tree.ParenExpr:
		return b.buildProjectionExpr(e.Expr, qry)
	case *tree.OrExpr:
		return b.buildOr(e, qry, b.buildProjectionExpr)
	case *tree.NotExpr:
		return b.buildNot(e, qry, b.buildProjectionExpr)
	case *tree.AndExpr:
		return b.buildAnd(e, qry, b.buildProjectionExpr)
	case *tree.UnaryExpr:
		return b.buildUnary(e, qry, b.buildProjectionExpr)
	case *tree.BinaryExpr:
		return b.buildBinary(e, qry, b.buildProjectionExpr)
	case *tree.ComparisonExpr:
		return b.buildComparison(e, qry, b.buildProjectionExpr)
	case *tree.FuncExpr:
		return b.buildFunc(true, e, qry, b.buildProjectionExpr)
	case *tree.CastExpr:
		return b.buildCast(e, qry, b.buildProjectionExpr)
	case *tree.RangeCond:
		return b.buildBetween(e, qry, b.buildProjectionExpr)
	case *tree.UnresolvedName:
		return b.buildAttribute0(true, e, qry)
	}
	return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("'%v' is not support now", n))
}
