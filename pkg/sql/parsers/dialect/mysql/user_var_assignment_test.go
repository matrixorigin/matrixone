// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mysql

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestUserVariableAssignmentIsLowPrecedenceAndRightAssociative(t *testing.T) {
	stmt, err := ParseOne(context.Background(), "select @a := @b := 1", 1)
	require.NoError(t, err)
	selectStmt, ok := stmt.(*tree.Select)
	require.True(t, ok)
	clause, ok := selectStmt.Select.(*tree.SelectClause)
	require.True(t, ok)
	outer, ok := clause.Exprs[0].Expr.(*tree.UserVarAssignmentExpr)
	require.True(t, ok)
	require.Equal(t, "a", outer.Name)
	inner, ok := outer.Value.(*tree.UserVarAssignmentExpr)
	require.True(t, ok)
	require.Equal(t, "b", inner.Name)
	require.Equal(t, "select @a := (@b := 1)", tree.String(stmt, dialect.MYSQL))
}
