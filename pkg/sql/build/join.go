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

package build

import (
	"fmt"
	"matrixone/pkg/errno"
	"matrixone/pkg/sql/op"
	"matrixone/pkg/sql/op/innerJoin"
	"matrixone/pkg/sql/op/naturalJoin"
	"matrixone/pkg/sql/op/product"
	"matrixone/pkg/sql/tree"
	"matrixone/pkg/sqlerror"
)

func (b *build) buildJoin(stmt *tree.JoinTableExpr) (op.OP, error) {
	r, err := b.buildFromTable(stmt.Left)
	if err != nil {
		return nil, err
	}
	s, err := b.buildFromTable(stmt.Right)
	if err != nil {
		return nil, err
	}
	{
		switch stmt.JoinType {
		case tree.JOIN_TYPE_FULL:
			return nil, sqlerror.New(errno.SQLStatementNotYetComplete, fmt.Sprintf("unsupport join type '%v'", stmt.JoinType))
		case tree.JOIN_TYPE_LEFT:
			return nil, sqlerror.New(errno.SQLStatementNotYetComplete, fmt.Sprintf("unsupport join type '%v'", stmt.JoinType))
		case tree.JOIN_TYPE_RIGHT:
			return nil, sqlerror.New(errno.SQLStatementNotYetComplete, fmt.Sprintf("unsupport join type '%v'", stmt.JoinType))
		}
	}
	if stmt.Cond == nil {
		if err := b.checkProduct(r, s); err != nil {
			return nil, err
		}
		return product.New(r, s), nil
	}
	switch cond := stmt.Cond.(type) {
	case *tree.NaturalJoinCond:
		if err := b.checkNaturalJoin(r, s); err != nil {
			return nil, err
		}
		return naturalJoin.New(r, s), nil
	case *tree.OnJoinCond:
		rattrs, sattrs, err := b.checkInnerJoin(r, s, nil, nil, cond.Expr)
		if err != nil {
			return nil, err
		}
		return innerJoin.New(r, s, rattrs, sattrs), nil
	default:
		return nil, sqlerror.New(errno.SQLStatementNotYetComplete, fmt.Sprintf("unsupport join condition '%v'", cond))
	}
}
