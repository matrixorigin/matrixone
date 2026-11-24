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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestOneVisibleColumnKeepsWholeObjectPolicy(t *testing.T) {
	for _, tc := range []struct {
		name   string
		column string
	}{
		{name: "visible_column", column: "id"},
		{name: "only_hidden_row_id", column: catalog.Row_ID},
	} {
		t.Run(tc.name, func(t *testing.T) {
			opt := newCheckAlterOptimizer(t, "create table one_visible_column (id bigint primary key)")
			opt.ctxt.isDml = true // keep the row ID in the mock catalog as the engine does
			table := opt.ctxt.tables["one_visible_column"]
			if _, ok := table.Name2ColIndex[catalog.Row_ID]; !ok {
				if table.Name2ColIndex == nil {
					table.Name2ColIndex = map[string]int32{"id": 0}
				}
				table.Name2ColIndex[catalog.Row_ID] = int32(len(table.Cols))
				table.Cols = append(table.Cols, &pb.ColDef{
					Name: catalog.Row_ID, Hidden: true, Typ: pb.Type{Id: int32(types.T_Rowid)},
				})
			}
			p, err := runOneStmt(opt, t, "select "+tc.column+" from one_visible_column")
			require.NoError(t, err)
			scans := 0
			for _, node := range p.GetQuery().Nodes {
				if node.NodeType != pb.Node_TABLE_SCAN {
					continue
				}
				scans++
				require.Len(t, node.TableDef.Cols, 1)
				require.Equal(t, tc.column, node.TableDef.Cols[0].Name)
				require.False(t, fileservice.Policy(node.ReadPolicy).Any(fileservice.ColumnPrefetch, fileservice.SkipFullFilePreloads),
					"a table with one visible column should keep whole-object prefetch")
			}
			require.Equal(t, 1, scans)
		})
	}
}
