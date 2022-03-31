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
)

func (b *build) buildProjection(exprs tree.SelectExprs, proj *Projection, qry *Query) (*ResultProjection, error) {
	op := new(ResultProjection)
	for _, expr := range exprs {
		if _, ok := expr.Expr.(tree.UnqualifiedStar); ok {
			for _, attr := range qry.Scope.Result.Attrs {
				proj.Rs = append(proj.Rs, 0)
				proj.Es = append(proj.Es, &extend.Attribute{
					Name: attr,
					Type: qry.Scope.Result.AttrsMap[attr].Type.Oid,
				})
				proj.As = append(proj.As, attr)
				op.Rs = append(op.Rs, 0)
				op.Es = append(op.Es, &extend.Attribute{
					Name: attr,
					Type: qry.Scope.Result.AttrsMap[attr].Type.Oid,
				})
				op.As = append(op.As, attr)
			}
			continue
		}
		e, err := b.buildProjectionExpr(expr.Expr, qry)
		if err != nil {
			return nil, err
		}
		if e, err = b.pruneExtend(e, true); err != nil {
			return nil, err
		}
		if len(e.Attributes()) == 0 {
			return nil, errors.New(errno.FeatureNotSupported, "projection attributes is empty")
		}
		proj.Rs = append(proj.Rs, 0)
		proj.Es = append(proj.Es, e)
		if len(expr.As) > 0 {
			proj.As = append(proj.As, string(expr.As))
			op.Rs = append(op.Rs, 0)
			op.Es = append(op.Es, &extend.Attribute{
				Name: string(expr.As),
				Type: e.ReturnType(),
			})
			op.As = append(op.As, string(expr.As))
		} else {
			proj.As = append(proj.As, e.String())
			op.Rs = append(op.Rs, 0)
			op.Es = append(op.Es, &extend.Attribute{
				Name: e.String(),
				Type: e.ReturnType(),
			})
			op.As = append(op.As, tree.String(&expr, dialect.MYSQL))
		}
	}
	return op, nil
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
		return b.buildAttribute(e, qry)
	}
	return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("'%v' is not support now", n))
}

func pruneAggregation(aggs []*Aggregation) []*Aggregation {
	for i := 0; i < len(aggs); i++ {
		for j := i + 1; j < len(aggs); j++ {
			if aggs[i].Op == aggs[j].Op && aggs[i].Type == aggs[j].Type &&
				aggs[i].Name == aggs[j].Name && aggs[i].Alias == aggs[j].Alias {
				aggs = append(aggs[:i], aggs[i+1:]...)
				i--
				break
			}
		}
	}
	return aggs
}

func pruneProjection(proj *Projection) *Projection {
	for i := 0; i < len(proj.Es); i++ {
		for j := i + 1; j < len(proj.Es); j++ {
			if proj.As[i] == proj.As[j] {
				proj.As = append(proj.As[:i], proj.As[i+1:]...)
				proj.Es = append(proj.Es[:i], proj.Es[i+1:]...)
				proj.Rs = append(proj.Rs[:i], proj.Rs[i+1:]...)
				i--
				break
			}
		}
	}
	return proj
}
