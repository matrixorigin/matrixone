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

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/stretchr/testify/require"
)

func TestMoChangedTablesPublicPlan(t *testing.T) {
	for _, sql := range []string{
		"select * from mo_changed_tables(NULL)",
		"select * from mo_changed_tables(?)",
		"select * from mo_changed_tables(?, 'moi_ops_1') as m",
		"select kind, table_id, changed_count from mo_changed_tables(?)",
		"select mo_changed_tables.kind from (select 1) s cross join mo_changed_tables(NULL)",
		"select w.kind from mo_changed_tables(NULL) w cross join (select 1) s",
		"select mo_changed_tables.kind from (mo_changed_tables(NULL))",
	} {
		t.Run(sql, func(t *testing.T) {
			ctx := NewMockCompilerContext(false, newPlanTestProcess(t))
			stmt, err := mysql.ParseOne(t.Context(), sql, 1)
			require.NoError(t, err)
			defer stmt.Free()
			p, err := BuildPlan(ctx, stmt, true, nil)
			require.NoError(t, err)
			var scan *plan.Node
			for _, n := range p.GetQuery().GetNodes() {
				if n.NodeType == plan.Node_FUNCTION_SCAN {
					scan = n
					break
				}
			}
			require.NotNil(t, scan)
			require.Empty(t, scan.Children)
			require.True(t, scan.TableDef.TblFunc.IsSingle)
			require.Equal(t, moChangedTablesName, scan.TableDef.TblFunc.Name)
			columns := scan.TableDef.Cols
			require.Len(t, columns, 14)
			for i, name := range [...]string{
				"kind", "reason", "account_id", "account_name", "database_id", "database_name", "table_id",
				"table_name", "marker", "from_ts", "to_ts", "source_count", "baseline_count", "changed_count",
			} {
				require.Equal(t, name, columns[i].Name)
			}
			require.Equal(t, int32(types.T_text), columns[8].Typ.Id)
			require.Equal(t, int32(types.MaxMediumTextLen), columns[8].Typ.Width)
			require.Equal(t, columns[8].Typ.Id, scan.TblFuncExprList[0].Typ.Id)
			require.Equal(t, columns[8].Typ.Width, scan.TblFuncExprList[0].Typ.Width)
		})
	}
}

func TestMoChangedTablesAliasScope(t *testing.T) {
	for _, tc := range []struct{ sql, wantErr string }{
		{"select mo_changed_tables.kind from mo_changed_tables(NULL) w", "missing FROM-clause entry"},
		{"select * from (select 1) s cross join generate_series(1, 2)", "Every table function must have an alias"},
		{"select * from (select 1) s cross apply mo_changed_tables(NULL) w", "must run as an independent table function"},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			ctx := NewMockCompilerContext(false, newPlanTestProcess(t))
			stmt, err := mysql.ParseOne(t.Context(), tc.sql, 1)
			require.NoError(t, err)
			defer stmt.Free()
			_, err = BuildPlan(ctx, stmt, false, nil)
			require.ErrorContains(t, err, tc.wantErr)
		})
	}
}
