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

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/stretchr/testify/require"
)

// SHOW CREATE TABLE must keep backslashes in column and table comments, so
// that replaying its output stores the same comments (#31068).
func TestShowCreateTableCommentBackslashRoundTrip(t *testing.T) {
	for _, tc := range []struct {
		name      string
		mode      string
		sourceSQL string
	}{
		{
			name:      "default",
			mode:      "",
			sourceSQL: `create table comment_bs (a int comment 'C:\\dir\\new it''s', b int) comment 'x\\ty''z'`,
		},
		{
			name:      "no backslash escapes",
			mode:      "NO_BACKSLASH_ESCAPES",
			sourceSQL: `create table comment_bs (a int comment 'C:\dir\new it''s', b int) comment 'x\ty''z'`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := NewMockOptimizer(false, newPlanTestProcess(t))
			mock.ctxt.SetSqlModeOverride(tc.mode)
			build := func(sql string) *plan.TableDef {
				stmt, err := mysql.ParseOneWithSQLMode(t.Context(), sql, 1, tc.mode)
				require.NoError(t, err, sql)
				t.Cleanup(stmt.Free)
				p, err := BuildPlan(&mock.ctxt, stmt, false)
				require.NoError(t, err, sql)
				return p.GetDdl().GetCreateTable().GetTableDef()
			}
			sourceDef := build(tc.sourceSQL)
			showSQL, _, err := ConstructCreateTableSQL(&mock.ctxt, sourceDef, nil, false, nil)
			require.NoError(t, err)
			replayedDef := build(showSQL)

			require.Equal(t, sourceDef.Cols[0].Comment, replayedDef.Cols[0].Comment, showSQL)
			require.Equal(t, tableCommentForTest(sourceDef), tableCommentForTest(replayedDef), showSQL)
		})
	}
}

func tableCommentForTest(tableDef *plan.TableDef) string {
	for _, def := range tableDef.Defs {
		if properties := def.GetProperties(); properties != nil {
			for _, property := range properties.Properties {
				if property.Key == catalog.SystemRelAttr_Comment {
					return property.Value
				}
			}
		}
	}
	return ""
}
