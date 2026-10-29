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
	lockpb "github.com/matrixorigin/matrixone/pkg/pb/lock"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestParenthesizedLockPoliciesReachPlan(t *testing.T) {
	for _, tc := range []struct {
		sql    string
		mode   lockpb.LockMode
		nowait bool
	}{
		{"((SELECT N_NATIONKEY FROM NATION)) FOR UPDATE NOWAIT", lockpb.LockMode_Exclusive, true},
		{"(SELECT N_NATIONKEY FROM NATION FOR SHARE)", lockpb.LockMode_Shared, false},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			mock := NewMockOptimizer(true, newPlanTestProcess(t))
			logicPlan, err := runOneStmt(mock, t, tc.sql)
			require.NoError(t, err)
			targets := 0
			for _, node := range logicPlan.GetQuery().Nodes {
				if node.NodeType != pb.Node_LOCK_OP {
					continue
				}
				for _, target := range node.LockTargets {
					require.Equal(t, tc.mode, target.Mode)
					require.Equal(t, tc.nowait, target.GetNoWait())
					targets++
				}
			}
			require.Positive(t, targets)
		})
	}
	for _, sql := range []string{
		"(SELECT N_NATIONKEY FROM NATION FOR SHARE) FOR UPDATE",
		"(SELECT N_NATIONKEY FROM NATION FOR SHARE) FOR UPDATE NOWAIT",
	} {
		statements, err := mysql.Parse(t.Context(), sql, 0)
		require.Error(t, err, "invalid duplicate policies must not reach BuildPlan")
		require.Empty(t, statements)
	}
}
