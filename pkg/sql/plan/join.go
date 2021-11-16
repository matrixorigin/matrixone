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
	"matrixone/pkg/sql/errors"
	"matrixone/pkg/sql/parsers/tree"
)

func (b *build) buildJoinCond(expr tree.Expr, rs, ss []string, qry *Query) error {
	switch e := expr.(type) {
	case *tree.AndExpr:
		if err := b.buildJoinCond(e.Left, rs, ss, qry); err != nil {
			return err
		}
		return b.buildJoinCond(e.Right, rs, ss, qry)
	case *tree.ComparisonExpr:
		if e.Op != tree.EQUAL {
			return errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("unsupport join condition '%v'", expr))
		}
		left, ok := getColumnName(e.Left)
		if !ok {
			return errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("unsupport join condition '%v'", expr))
		}
		right, ok := getColumnName(e.Right)
		if !ok {
			return errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("unsupport join condition '%v'", expr))
		}
		r, rattr, err := qry.getJoinAttribute(true, append(rs, ss...), left)
		if err != nil {
			return err
		}
		s, sattr, err := qry.getJoinAttribute(true, append(rs, ss...), right)
		if err != nil {
			return err
		}
		if qry.RelsMap[r].AttrsMap[rattr].Type.Oid != qry.RelsMap[s].AttrsMap[sattr].Type.Oid {
			return errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("unsupport join condition '%v'", expr))
		}
		qry.Conds = append(qry.Conds, &JoinCondition{
			R:     r,
			S:     s,
			Rattr: rattr,
			Sattr: sattr,
		})
		return nil
	}
	return errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("unsupport join condition '%v'", expr))
}

func (b *build) buildUsingJoinCond(cols tree.IdentifierList, rs, ss []string, qry *Query) error {
	for _, col := range cols {
		r, rattr, err := qry.getJoinAttribute(true, rs, string(col))
		if err != nil {
			return err
		}
		s, sattr, err := qry.getJoinAttribute(true, ss, string(col))
		if err != nil {
			return err
		}
		if qry.RelsMap[r].AttrsMap[rattr].Type.Oid != qry.RelsMap[s].AttrsMap[sattr].Type.Oid {
			return errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("unsupport join condition 'using(%v)'", col))
		}
		qry.Conds = append(qry.Conds, &JoinCondition{
			R:     r,
			S:     s,
			Rattr: rattr,
			Sattr: sattr,
		})
	}
	return nil
}
