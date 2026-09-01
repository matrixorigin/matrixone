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
	"testing"

	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func requireOnDupUpdateColumns(t *testing.T, logicPlan *planpb.Plan, included, excluded []int32) {
	t.Helper()
	found := false
	for _, node := range logicPlan.GetQuery().Nodes {
		if node.NodeType != planpb.Node_JOIN || node.JoinType != planpb.Node_DEDUP ||
			node.OnDuplicateAction != planpb.Node_UPDATE {
			continue
		}
		found = true
		for _, col := range included {
			require.Contains(t, node.DedupJoinCtx.UpdateColIdxList, col)
		}
		for _, col := range excluded {
			require.NotContains(t, node.DedupJoinCtx.UpdateColIdxList, col)
		}
	}
	require.True(t, found, "expected an ON DUPLICATE KEY UPDATE dedup join")
}

func TestInsertOnDupIncomingPrimaryKeyNoop(t *testing.T) {
	mock := NewMockOptimizer(true)
	logicPlan, err := runOneStmt(mock, t,
		"insert into constraint_test.t1(a, b) values (1, 'x') "+
			"on duplicate key update a = values(a), b = values(b)")
	require.NoError(t, err)
	requireOnDupUpdateColumns(t, logicPlan, []int32{1}, []int32{0})
}

func TestInsertOnDupIncomingPrimaryKeyOnlyNoop(t *testing.T) {
	mock := NewMockOptimizer(true)
	logicPlan, err := runOneStmt(mock, t,
		"insert into constraint_test.t1(a, b) values (1, 'x') "+
			"on duplicate key update a = values(a)")
	require.NoError(t, err)
	requireOnDupUpdateColumns(t, logicPlan, nil, []int32{0})
}

func TestInsertOnDupIncomingCompositePrimaryKeyNoop(t *testing.T) {
	mock := NewMockOptimizer(true)
	logicPlan, err := runOneStmt(mock, t,
		"insert into tpch.partsupp values (1, 2, 3, 4.50, 'x') "+
			"on duplicate key update ps_partkey = values(ps_partkey), "+
			"ps_suppkey = values(ps_suppkey), ps_availqty = values(ps_availqty)")
	require.NoError(t, err)
	requireOnDupUpdateColumns(t, logicPlan, []int32{2}, []int32{0, 1})
}

func TestInsertOnDupPrimaryKeyMutationStillRejected(t *testing.T) {
	mock := NewMockOptimizer(true)
	_, err := runOneStmt(mock, t,
		"insert into constraint_test.t1(a, b) values (1, 'x') "+
			"on duplicate key update a = a + 1, b = values(b)")
	require.ErrorContains(t, err, "unsupported DML: update primary key on duplicate")
}

func TestInsertOnDupPrimaryKeyFromDifferentIncomingColumnStillRejected(t *testing.T) {
	mock := NewMockOptimizer(true)
	_, err := runOneStmt(mock, t,
		"insert into constraint_test.t1(a, b) values (1, '2') "+
			"on duplicate key update a = values(b)")
	require.ErrorContains(t, err, "unsupported DML: update primary key on duplicate")
}

func TestInsertOnDupIncomingPrimaryKeyWithSecondaryUniqueStillRejected(t *testing.T) {
	mock := NewMockOptimizer(true)
	_, err := runOneStmt(mock, t,
		"insert into constraint_test.dept(deptno, dname, loc) values (1, 'Sales', 'NY') "+
			"on duplicate key update deptno = values(deptno), loc = values(loc)")
	require.ErrorContains(t, err, "unsupported DML: update primary key on duplicate")
}

func TestIsOnDupIncomingColumn(t *testing.T) {
	expr := &planpb.Expr{Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 7, ColPos: 3}}}
	require.True(t, isOnDupIncomingColumn(expr, 7, 3))
	require.False(t, isOnDupIncomingColumn(expr, 8, 3))
	require.False(t, isOnDupIncomingColumn(expr, 7, 4))
	require.False(t, isOnDupIncomingColumn(&planpb.Expr{}, 7, 3))
}
