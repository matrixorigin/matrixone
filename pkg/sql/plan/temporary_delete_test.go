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

	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestTemporaryDeleteDoesNotRewriteToTruncate(t *testing.T) {
	for _, temporary := range []bool{false, true} {
		name := "permanent"
		if temporary {
			name = "temporary"
		}
		t.Run(name, func(t *testing.T) {
			mock := NewMockOptimizer(true, newPlanTestProcess(t))
			table := mock.ctxt.tables["nation"]
			table.IsTemporary = temporary
			ctx := &DMLContext{
				tableDefs:      []*TableDef{table},
				objRefs:        []*ObjectRef{{SchemaName: "tpch", ObjName: "nation"}},
				isClusterTable: []bool{false},
			}
			allowed, err := canDeleteRewriteToTruncate(&mock.ctxt, ctx)
			require.NoError(t, err)
			require.Equal(t, !temporary, allowed)

			// Exercise the legacy planner too: it has a separate truncate
			// eligibility check and must not reintroduce a physical replacement.
			stmt, err := mysql.ParseOne(t.Context(), "delete from tpch.nation", 1)
			require.NoError(t, err)
			defer stmt.Free()
			p, err := buildDelete(stmt.(*tree.Delete), &mock.ctxt, false)
			require.NoError(t, err)
			found := false
			for _, node := range p.GetQuery().Nodes {
				if node.NodeType == planpb.Node_DELETE && node.DeleteCtx.TableDef.TblId == table.TblId {
					found = true
					require.Equal(t, !temporary, node.DeleteCtx.CanTruncate)
				}
			}
			require.True(t, found, "expected an origin-table DELETE node")
		})
	}
}
