// Copyright 2026 MatrixOne
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

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_lock "github.com/matrixorigin/matrixone/pkg/frontend/test/mock_lock"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	lockpb "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	motoml "github.com/matrixorigin/matrixone/pkg/util/toml"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// Pre-PR review round 7, finding 1 (plan side): applyLockTableFallback
// flips ONLY the physical LockTable disposition of a sufficiently large
// Shared target — the bind-time marked LockingRead stays on the SAME
// target object, so plan-level presence survives the promotion a small
// MaxLockRowCount budget forces. compileLock must then preserve exactly
// this promoted marked shape (its members live in pkg/sql/compile).
func TestTableLockPromotionKeepsMarkedSharedTargetProvenance(t *testing.T) {
	budgets := []struct {
		name     string
		maxRow   int64
		promoted bool
	}{
		{name: "small lock budget promotes the shared target", maxRow: 3, promoted: true},
		{name: "large lock budget keeps row locks", maxRow: 10000, promoted: false},
	}
	for _, tc := range budgets {
		t.Run(tc.name, func(t *testing.T) {
			mock := NewMockOptimizer(true, nil)
			proc := testutil.NewProc(t)
			lockService := mock_lock.NewMockLockService(gomock.NewController(t))
			lockService.EXPECT().GetConfig().Return(lockservice.Config{
				MaxLockRowCount: motoml.ByteSize(tc.maxRow),
			}).AnyTimes()
			proc.Base.LockService = lockService
			mock.ctxt.GetProcessFunc = func() *process.Process { return proc }

			markedShared := &plan.LockTarget{
				Mode:        lockpb.LockMode_Shared,
				LockingRead: true,
			}
			builder := &QueryBuilder{
				compCtx: &mock.ctxt,
				qry: &plan.Query{Nodes: []*plan.Node{
					{NodeType: plan.Node_TABLE_SCAN, Stats: &plan.Stats{Outcnt: 4}},
					{
						NodeType:    plan.Node_LOCK_OP,
						Stats:       &plan.Stats{Outcnt: 4},
						LockTargets: []*plan.LockTarget{markedShared},
					},
				}},
			}

			applyLockTableFallback(builder)
			require.Equal(t, tc.promoted, markedShared.LockTable,
				"the configured MaxLockRowCount budget drives the physical row/table disposition")
			require.Same(t, markedShared, builder.qry.Nodes[1].LockTargets[0],
				"promotion mutates the same target object in place")
			require.True(t, markedShared.GetLockingRead(),
				"promotion preserves the bind-time marked provenance (D-8) on the promoted target")
			require.True(t, PlanHasLockingCurrentRead(builder.qry),
				"plan-level presence stays visible after promotion so the compile-side consumers can rely on it")
		})
	}
}

// The Shared covering-index current read: the same production constructor
// path that carried an Exclusive covering locking read (rev-18 finding 1)
// gives a LOCK IN SHARE MODE statement its owning base table's identity and
// the source occurrence's role, and keeps the statement's locking-read
// presence end to end. This is the plan-side half of the review's "Shared
// covering-index current reads" coverage; the compile-side promotion
// members prove the same statement survives compileLock's table promotion.
func TestSharedHintedCoveringLockingScanCarriesSourceProvenance(t *testing.T) {
	proc := newPlanTestProcess(t)
	setLockingCurrentReadGate(t, proc, defines.MORPCVersion135)

	const coveringSharedSQL = `
		select id, a, b from index_hint_t force index for order by(idx_ab)
		where a = 1 and b between 10 and 20 order by b, id lock in share mode`

	mock := NewMockOptimizer(true, proc)
	addIndexHintChoiceTableForTest(mock)
	baseTableID := mock.ctxt.tables["index_hint_t"].TblId
	require.NotZero(t, baseTableID, "the fixture registers index_hint_t with a nonzero id")

	queryPlan, err := runOneStmt(mock, t, coveringSharedSQL)
	require.NoError(t, err)
	cover := findFirstIndexScanNode(queryPlan)
	require.NotNil(t, cover,
		"the ORDER-BY force requirement routes the shared locking query through the covering-index constructor")
	require.True(t, cover.IndexScanInfo.IsIndexScan)
	require.Equal(t, "idx_ab", cover.IndexScanInfo.IndexName)
	require.Equal(t, int64(baseTableID), cover.IndexScanInfo.ParentTableId,
		"the fabricated shared covering scan names its owning locked base table (D-10.2 admission)")
	require.Equal(t, plan.ScanReadRole_ScanOuterLocking, cover.GetScanReadRole(),
		"the shared covering scan carries the locked occurrence's source role (R13-F3)")

	q := queryPlan.GetQuery()
	require.NotNil(t, q)
	require.True(t, PlanHasLockingCurrentRead(q),
		"a shared covering locking plan carries locking-current-read presence end to end")
	var lockTargets []*plan.LockTarget
	for _, n := range q.Nodes {
		if n.NodeType == plan.Node_LOCK_OP {
			lockTargets = append(lockTargets, n.LockTargets...)
		}
	}
	require.NotEmpty(t, lockTargets,
		"the LOCK_OP of the shared covering query keeps its base-table lock target")
	shared := false
	for _, target := range lockTargets {
		require.Equal(t, baseTableID, target.GetTableId())
		require.True(t, target.GetLockingRead(), "every shared lock target stays marked")
		if target.GetMode() == lockpb.LockMode_Shared {
			shared = true
		}
	}
	require.True(t, shared, "the shared statement's lock target carries the shared mode")
}
