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

func (b *build) buildGroupBy(exprs tree.GroupBy, proj0, proj *Projection, qry *Query) ([]string, error) {
	var err error
	var fvars []string
	var e extend.Extend

	for _, expr := range exprs {
		fvar := tree.String(expr, dialect.MYSQL)
		for i := range proj0.As {
			if fvar == proj0.As[i] {
				proj.Rs = append(proj.Rs, 0)
				proj.As = append(proj.As, fvar)
				proj.Es = append(proj.Es, proj0.Es[i])
				goto OUT
			}
		}
		if e, err = b.buildGroupByExpr(expr, qry); err != nil {
			return nil, err
		}
		if e, err = b.pruneExtend(e, true); err != nil {
			return nil, err
		}
		fvar = e.String()
		proj.Rs = append(proj.Rs, 0)
		proj.Es = append(proj.Es, e)
		proj.As = append(proj.As, fvar)
	OUT:
		fvars = append(fvars, fvar)
	}
	return fvars, nil
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
		return b.buildAttribute(e, qry)
	}
	return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("'%v' is not support now", n))
}
