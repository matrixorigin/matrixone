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

func (b *build) buildOrderBy(orders tree.OrderBy, proj0, proj *Projection, qry *Query) ([]*Field, error) {
	var err error
	var fs []*Field
	var e extend.Extend

	for _, order := range orders {
		ovar := tree.String(order.Expr, dialect.MYSQL)
		for i := range proj0.As {
			if ovar == proj0.As[i] {
				proj.Rs = append(proj.Rs, 0)
				proj.As = append(proj.As, ovar)
				proj.Es = append(proj.Es, proj0.Es[i])
				goto OUT
			}
		}
		if e, err = b.buildOrderByExpr(order.Expr, qry); err != nil {
			return nil, err
		}
		if e, err = b.pruneExtend(e, true); err != nil {
			return nil, err
		}
		ovar = e.String()
		proj.Rs = append(proj.Rs, 0)
		proj.Es = append(proj.Es, e)
		proj.As = append(proj.As, e.String())
	OUT:
		fs = append(fs, &Field{
			Attr: ovar,
			Type: Direction(order.Direction),
		})
	}
	return fs, nil
}

func (b *build) buildOrderByExpr(n tree.Expr, qry *Query) (extend.Extend, error) {
	switch e := n.(type) {
	case *tree.NumVal:
		return buildValue(e.Value, e.String())
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
		return b.buildAttribute(e, qry)
	}
	return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("'%v' is not support now", n))
}
