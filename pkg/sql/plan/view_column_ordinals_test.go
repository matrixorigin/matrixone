// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/util/sysview"
	"github.com/stretchr/testify/require"
)

func ordinalProjectionClauses(statement tree.SelectStatement) []*tree.SelectClause {
	switch node := statement.(type) {
	case *tree.Select:
		return ordinalProjectionClauses(node.Select)
	case *tree.ParenSelect:
		return ordinalProjectionClauses(node.Select)
	case *tree.UnionClause:
		return append(ordinalProjectionClauses(node.Left), ordinalProjectionClauses(node.Right)...)
	case *tree.SelectClause:
		return []*tree.SelectClause{node}
	default:
		return nil
	}
}

func TestInformationSchemaVisibleOrdinalProjection(t *testing.T) {
	for name, definition := range map[string]string{
		"current":     sysview.InformationSchemaColumnsDDL,
		"v41":         sysview.InformationSchemaColumnsV41DDL,
		"v46":         sysview.InformationSchemaColumnsV46DDL,
		"v46-upgrade": sysview.InformationSchemaColumnsV46UpgradeDDL,
		"v58":         sysview.InformationSchemaColumnsV58DDL(),
		"v100":        sysview.InformationSchemaColumnsV100DDL(),
		"v109":        sysview.InformationSchemaColumnsV109DDL(),
	} {
		t.Run(name, func(t *testing.T) {
			require.True(t, knownInformationSchemaColumns("information_schema", "COLUMNS", definition))
			metadata, ok := sysview.ViewMetadataColumnsDDL(definition)
			require.True(t, ok)
			for _, input := range []string{definition, metadata} {
				statements, err := mysql.Parse(t.Context(), input, 1)
				require.NoError(t, err)
				require.Len(t, statements, 1)
				func() {
					defer statements[0].Free()
					view := statements[0].(*tree.CreateView)
					before := cloneTreeSelect(view.AsSource)
					require.True(t, rewriteInformationSchemaColumnOrdinals(view.AsSource))
					old, updated := ordinalProjectionClauses(before), ordinalProjectionClauses(view.AsSource)
					require.Equal(t, len(old), len(updated))
					for i, clause := range updated {
						cast := clause.Exprs[4].Expr.(*tree.CastExpr)
						require.Equal(t, int32(32), cast.Type.(*tree.T).InternalType.Width)
						function := cast.Expr.(*tree.FuncExpr)
						require.Equal(t, "row_number", function.FuncName.Compare())
						require.Len(t, function.WindowSpec.PartitionBy, 3)
						account := function.WindowSpec.PartitionBy[0].(*tree.UnresolvedName)
						require.Equal(t, "account_id", account.ColName())
						require.Equal(t, ordinalPartitionColumn(clause.Exprs[1].Expr).TblName(), account.TblName())
						require.Equal(t, "attnum", function.WindowSpec.OrderBy[0].Expr.(*tree.UnresolvedName).ColName())
						// Restore only the owned projection. Authorization, hidden
						// filtering, CTEs and every versioned formatter are unchanged.
						clause.Exprs[4] = old[i].Exprs[4]
					}
					require.Equal(t, tree.String(before, dialect.MYSQL), tree.String(view.AsSource, dialect.MYSQL))
				}()
			}
		})
	}
}

func TestInformationSchemaOrdinalTrustBoundary(t *testing.T) {
	definition := sysview.InformationSchemaColumnsDDL
	require.False(t, knownInformationSchemaColumns("user_db", "columns", definition))
	require.False(t, knownInformationSchemaColumns("information_schema", "user_view", definition))
	require.False(t, knownInformationSchemaColumns("information_schema", "columns", definition+" where true"))
	require.False(t, knownInformationSchemaColumns("information_schema", "columns", "CREATE VIEW information_schema.COLUMNS AS SELECT 1"))
	require.False(t, rewriteInformationSchemaColumnOrdinals(&tree.SelectClause{}))
}
