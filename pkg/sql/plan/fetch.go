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

func (b *build) buildFetch(fetch *tree.Limit, qry *Query) error {
	if fetch.Offset != nil {
		e, err := b.buildFetchExpr(fetch.Offset, qry)
		if err != nil {
			return err
		}
		if e, err = b.pruneExtend(e); err != nil {
			return err
		}
		v, ok := e.(*extend.ValueExtend)
		if !ok {
			return errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("Undeclared variable '%s'", e))
		}
		if v.V.Typ.Oid != types.T_int64 {
			return errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("Undeclared variable '%s'", e))
		}
		qry.Offset = v.V.Col.([]int64)[0]
	}
	if fetch.Count != nil {
		e, err := b.buildFetchExpr(fetch.Count, qry)
		if err != nil {
			return err
		}
		if e, err = b.pruneExtend(e); err != nil {
			return err
		}
		v, ok := e.(*extend.ValueExtend)
		if !ok {
			return errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("Undeclared variable '%s'", e))
		}
		if v.V.Typ.Oid != types.T_int64 {
			return errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("Undeclared variable '%s'", e))
		}
		qry.Limit = v.V.Col.([]int64)[0]
	}
	return nil
}

func (b *build) buildFetchExpr(n tree.Expr, qry *Query) (extend.Extend, error) {
	switch e := n.(type) {
	case *tree.NumVal:
		return buildValue(e.Value)
	case *tree.ParenExpr:
		return b.buildFetchExpr(e.Expr, qry)
	case *tree.NotExpr:
		return b.buildNot(e, qry, b.buildFetchExpr)
	case *tree.AndExpr:
		return b.buildAnd(e, qry, b.buildFetchExpr)
	case *tree.UnaryExpr:
		return b.buildUnary(e, qry, b.buildFetchExpr)
	case *tree.BinaryExpr:
		return b.buildBinary(e, qry, b.buildFetchExpr)
	case *tree.ComparisonExpr:
		return b.buildComparison(e, qry, b.buildFetchExpr)
	case *tree.FuncExpr:
		return b.buildFunc(false, e, qry, b.buildFetchExpr)
	case *tree.CastExpr:
		return b.buildCast(e, qry, b.buildFetchExpr)
	case *tree.RangeCond:
		return b.buildBetween(e, qry, b.buildFetchExpr)
	case *tree.UnresolvedName:
		return b.buildAttribute0(false, e, qry)
	}
	return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("'%v' is not support now", n))
}
