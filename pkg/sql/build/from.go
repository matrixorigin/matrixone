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
	"matrixone/pkg/sql/op/product"
	"matrixone/pkg/sql/tree"
	"matrixone/pkg/sqlerror"
)

func (b *build) buildFrom(stmts tree.TableExprs) (op.OP, error) {
	r, err := b.buildFromTable(stmts[0])
	if err != nil {
		return nil, err
	}
	if stmts = stmts[1:]; len(stmts) == 0 {
		return r, nil
	}
	s, err := b.buildFrom(stmts)
	if err != nil {
		return nil, err
	}
	if err := b.checkProduct(r, s); err != nil {
		return nil, err
	}
	return product.New(r, s), nil
}

func (b *build) buildFromTable(stmt tree.TableExpr) (op.OP, error) {
	switch stmt := stmt.(type) {
	case *tree.AliasedTableExpr:
		o, err := b.buildFromTable(stmt.Expr)
		if err != nil {
			return nil, err
		}
		o.Rename(string(stmt.As.Alias))
		return o, nil
	case *tree.JoinTableExpr:
		return b.buildJoin(stmt)
	case *tree.TableName:
		return b.buildTable(stmt)
	case *tree.ParenTableExpr:
		return b.buildFromTable(stmt.Expr)
	case *tree.Subquery:
		return b.buildSelectStatement(stmt.Select)
	}
	return nil, sqlerror.New(errno.SQLStatementNotYetComplete, fmt.Sprintf("unknown table expr: %T", stmt))
}
