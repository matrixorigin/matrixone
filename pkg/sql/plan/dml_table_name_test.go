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

package plan

import (
	"context"
	"testing"

	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestDeleteSourceAliases(t *testing.T) {
	for _, tc := range []struct {
		sql  string
		want map[string][2]string
	}{
		{"delete nation from nation", map[string][2]string{}},
		{"delete x from ((nation as x))", map[string][2]string{"x": {"", "nation"}}},
		{"delete x,y from tpch.nation x join nation2 y on x.n_nationkey=y.n_nationkey", map[string][2]string{"x": {"tpch", "nation"}, "y": {"", "nation2"}}},
		{"delete x from (select * from nation) as x", map[string][2]string{}},
		// Collection preserves traversal order; duplicate validation belongs to binding.
		{"delete x from nation x join nation2 x on 1=1", map[string][2]string{"x": {"", "nation2"}}},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			stmt, err := mysql.ParseOne(context.Background(), tc.sql, 1)
			require.NoError(t, err)
			defer stmt.Free()
			before := tree.String(stmt, dialect.MYSQL)
			require.Equal(t, tc.want, CollectDeleteSourceAliases(stmt.(*tree.Delete).TableRefs))
			require.Equal(t, before, tree.String(stmt, dialect.MYSQL))
		})
	}
	// Non-DELETE consumers supply nil aliases and must retain an existing binding alias.
	db, table, alias := ResolveDMLTargetName("db", "t", "binding", nil)
	require.Equal(t, [3]string{"db", "t", "binding"}, [3]string{db, table, alias})
}

func TestDeleteTargetIdentity(t *testing.T) {
	for _, tc := range []struct {
		sql  string
		kind planpb.Node_NodeType
		want []string
	}{
		{"delete tpch.nation from tpch.nation where n_nationkey=1", planpb.Node_MULTI_UPDATE, []string{"nation"}},
		// Both nation and region exist: success alone would not detect the wrong victim.
		{"delete ignored.region from (nation as region) where region.n_nationkey=1", planpb.Node_MULTI_UPDATE, []string{"nation"}},
		{"delete region from tpch.nation as region where region.n_nationkey=1", planpb.Node_MULTI_UPDATE, []string{"nation"}},
		{"delete ignored.region,n from nation as region join nation2 n on region.n_nationkey=n.n_nationkey", planpb.Node_DELETE, []string{"nation", "nation2"}},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			logicPlan, err := runOneStmt(NewMockOptimizer(true, newPlanTestProcess(t)), t, tc.sql)
			require.NoError(t, err)
			var targets []string
			for _, node := range logicPlan.GetQuery().Nodes {
				if node.NodeType != tc.kind {
					continue
				}
				if tc.kind == planpb.Node_MULTI_UPDATE {
					for _, target := range node.UpdateCtxList {
						targets = append(targets, target.ObjRef.ObjName)
					}
				} else {
					targets = append(targets, node.DeleteCtx.Ref.ObjName)
				}
			}
			require.ElementsMatch(t, tc.want, targets)
		})
	}
}
