// Copyright 2026 Matrix Origin
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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestSelectHasSQLCalcFoundRows(t *testing.T) {
	stmt := &tree.Select{Select: &tree.SelectClause{
		Option: tree.QuerySpecOptionSqlCalcFoundRows,
	}}
	require.True(t, selectHasSQLCalcFoundRows(stmt))

	stmt.Select.(*tree.SelectClause).Option = tree.QuerySpecOptionNone
	require.False(t, selectHasSQLCalcFoundRows(stmt))

	paren := &tree.Select{Select: &tree.ParenSelect{Select: stmt}}
	paren.Select.(*tree.ParenSelect).Select.Select = &tree.SelectClause{
		Option: tree.QuerySpecOptionSqlCalcFoundRows,
	}
	require.True(t, selectHasSQLCalcFoundRows(paren))

	union := &tree.Select{Select: &tree.UnionClause{
		Left: &tree.ParenSelect{Select: &tree.Select{Select: &tree.SelectClause{
			Option: tree.QuerySpecOptionSqlCalcFoundRows,
		}}},
		Right: &tree.ValuesClause{},
	}}
	require.True(t, selectHasSQLCalcFoundRows(union))

	union.Select.(*tree.UnionClause).Left = &tree.ValuesClause{}
	require.False(t, selectHasSQLCalcFoundRows(union))
}
