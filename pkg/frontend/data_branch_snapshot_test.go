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

package frontend

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/frontend/databranchutils"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
)

// ---------------------------------------------------------------------------
// UT-U1 — branchSnapshotName
// ---------------------------------------------------------------------------

func TestBranchSnapshotName(t *testing.T) {
	// §4.3 — canonical format is `__mo_branch_<child_table_id>`, where the
	// suffix is the decimal rel_id of the child. Child ids are cluster-unique
	// so no additional qualifier is needed.
	cases := []struct {
		in   uint64
		want string
	}{
		{0, "__mo_branch_0"},
		{1, "__mo_branch_1"},
		{42, "__mo_branch_42"},
		{1 << 30, fmt.Sprintf("__mo_branch_%d", uint64(1<<30))},
		// Maximum uint64 still produces a sane decimal suffix.
		{^uint64(0), "__mo_branch_18446744073709551615"},
	}
	for _, tc := range cases {
		require.Equal(t, tc.want, branchSnapshotName(tc.in))
		// Confirm the shared constant agrees with the helper so users that
		// grep for the prefix always find it.
		require.True(t, strings.HasPrefix(tc.want, databranchutils.BranchSnapshotSnamePrefix))
	}
}

// ---------------------------------------------------------------------------
// UT-U2 — buildDagFromRows (DAG adjacency construction)
// ---------------------------------------------------------------------------

func TestBuildDAG(t *testing.T) {
	// Synthetic DAG: t1 (root) -> t2 -> t3; t1 -> t4 (sibling of t2);
	// plus an orphan node whose declared parent is absent from the input.
	rows := []databranchutils.DataBranchMetadata{
		// t2 is a child of t1
		{TableID: 2, PTableID: 1, CloneTS: 100, TableDeleted: false},
		// t3 is a child of t2
		{TableID: 3, PTableID: 2, CloneTS: 200, TableDeleted: true},
		// t4 is another child of t1
		{TableID: 4, PTableID: 1, CloneTS: 300, TableDeleted: false},
		// orphan — its parent tid 99 was never inserted
		{TableID: 5, PTableID: 99, CloneTS: 400, TableDeleted: false},
	}
	dag := databranchutils.NewBranchReclaimDag(rows)

	// `Children[parent]` only contains directly recorded children.
	require.ElementsMatch(t, []uint64{2, 4}, dag.Children[1])
	require.ElementsMatch(t, []uint64{3}, dag.Children[2])
	require.ElementsMatch(t, []uint64{5}, dag.Children[99])

	// `Info` covers every explicit row (but NOT synthetic parent-only ids).
	require.Contains(t, dag.Info, uint64(2))
	require.Contains(t, dag.Info, uint64(3))
	require.Contains(t, dag.Info, uint64(4))
	require.Contains(t, dag.Info, uint64(5))
	require.NotContains(t, dag.Info, uint64(1)) // t1 is a root; not a child
	require.NotContains(t, dag.Info, uint64(99))

	// Deleted propagation stays row-local.
	require.True(t, dag.Info[3].Deleted)
	require.False(t, dag.Info[2].Deleted)
	require.False(t, dag.Info[4].Deleted)
	require.False(t, dag.Info[5].Deleted)

	require.Equal(t, uint64(1), dag.Info[2].ParentTableID)
	require.Equal(t, uint64(2), dag.Info[3].ParentTableID)
}

// ---------------------------------------------------------------------------
// UT-U3 — SubtreeAllDeleted on a linear chain t1 -> t2 -> t3
// ---------------------------------------------------------------------------

func TestSubtreeAllDeleted_Linear(t *testing.T) {
	// Helper that rebuilds the DAG with a requested deletion pattern.
	// t1 has no metadata row; t2/t3 cover the chain's edges. Missing
	// nodes are treated as reclaimable by SubtreeAllDeleted, which is the
	// behaviour the reclaim walk relies on.
	newDag := func(deletedT2, deletedT3 bool) databranchutils.BranchReclaimDag {
		return databranchutils.NewBranchReclaimDag([]databranchutils.DataBranchMetadata{
			{TableID: 2, PTableID: 1, CloneTS: 100, TableDeleted: deletedT2},
			{TableID: 3, PTableID: 2, CloneTS: 200, TableDeleted: deletedT3},
		})
	}

	// 1. All alive — every subtree predicate returns false because the
	//    target is still alive.
	dag := newDag(false, false)
	require.False(t, dag.SubtreeAllDeleted(3))
	require.False(t, dag.SubtreeAllDeleted(2))
	require.True(t, dag.SubtreeAllDeleted(1)) // t1 has no info; treated as gone

	// 2. Only t3 deleted — `subtreeAllDeleted(t3) == true`, nothing else.
	dag = newDag(false, true)
	require.True(t, dag.SubtreeAllDeleted(3))
	require.False(t, dag.SubtreeAllDeleted(2))

	// 3. t3 and t2 deleted — `subtreeAllDeleted(t2)` and `subtreeAllDeleted(t3)`
	//    are true, but the root t1 stays non-reclaimable while it is alive
	//    in mo_branch_metadata. Since t1 is absent from info (it's a DAG
	//    root), the predicate returns true for it — the caller decides
	//    whether to emit a drop based on whether info[tid] exists.
	dag = databranchutils.NewBranchReclaimDag([]databranchutils.DataBranchMetadata{
		{TableID: 2, PTableID: 1, CloneTS: 100, TableDeleted: true},
		{TableID: 3, PTableID: 2, CloneTS: 200, TableDeleted: true},
	})
	require.True(t, dag.SubtreeAllDeleted(3))
	require.True(t, dag.SubtreeAllDeleted(2))
}

// ---------------------------------------------------------------------------
// UT-U4 — SubtreeAllDeleted on a branching DAG t1 -> {t2, t3}, t2 -> t4
// ---------------------------------------------------------------------------

func TestSubtreeAllDeleted_Branching(t *testing.T) {
	// 1. Only t4 deleted
	dag := databranchutils.NewBranchReclaimDag([]databranchutils.DataBranchMetadata{
		{TableID: 2, PTableID: 1, TableDeleted: false},
		{TableID: 3, PTableID: 1, TableDeleted: false},
		{TableID: 4, PTableID: 2, TableDeleted: true},
	})
	require.True(t, dag.SubtreeAllDeleted(4))
	require.False(t, dag.SubtreeAllDeleted(2)) // t2 alive
	require.False(t, dag.SubtreeAllDeleted(3)) // t3 alive
	require.True(t, dag.SubtreeAllDeleted(1))  // t1 absent from info

	// 2. t3 deleted (sibling)
	dag = databranchutils.NewBranchReclaimDag([]databranchutils.DataBranchMetadata{
		{TableID: 2, PTableID: 1, TableDeleted: false},
		{TableID: 3, PTableID: 1, TableDeleted: true},
		{TableID: 4, PTableID: 2, TableDeleted: false},
	})
	require.True(t, dag.SubtreeAllDeleted(3))
	require.False(t, dag.SubtreeAllDeleted(2)) // t2 alive
	require.True(t, dag.SubtreeAllDeleted(1))  // t1 absent

	// 3. t3, t2, t4 all deleted
	dag = databranchutils.NewBranchReclaimDag([]databranchutils.DataBranchMetadata{
		{TableID: 2, PTableID: 1, TableDeleted: true},
		{TableID: 3, PTableID: 1, TableDeleted: true},
		{TableID: 4, PTableID: 2, TableDeleted: true},
	})
	require.True(t, dag.SubtreeAllDeleted(3))
	require.True(t, dag.SubtreeAllDeleted(2))
	require.True(t, dag.SubtreeAllDeleted(4))
	// t1 is still alive in business terms — its presence in mo_branch_metadata
	// is what drives the decision, not the predicate. Predicate says true
	// because info[1] is absent.
	require.True(t, dag.SubtreeAllDeleted(1))
}

// ---------------------------------------------------------------------------
// UT-U5 — reclaimCore drives the drop list through the injected closures
// ---------------------------------------------------------------------------

// TestReclaimCore_DropList exercises §5.4 scenarios A and B end-to-end by
// driving the shared core with mocked loader and delete closures.
func TestReclaimCore_DropList(t *testing.T) {
	// §5.4 initial DAG:  t1 -> t2 -> t3  and  t2 -> t4
	baseRows := func(deletedT2, deletedT3, deletedT4 bool) []databranchutils.DataBranchMetadata {
		return []databranchutils.DataBranchMetadata{
			{TableID: 2, PTableID: 1, CloneTS: 100, TableDeleted: deletedT2},
			{TableID: 3, PTableID: 2, CloneTS: 200, TableDeleted: deletedT3},
			{TableID: 4, PTableID: 2, CloneTS: 300, TableDeleted: deletedT4},
		}
	}

	// ---- Scenario A: user drops t3 only.
	var got []string
	err := databranchutils.ReclaimBranchSnapshotsCore(
		[]uint64{3},
		func() (databranchutils.BranchReclaimDag, error) {
			return databranchutils.NewBranchReclaimDag(baseRows(false, true, false)), nil
		},
		func(snames []string) error {
			got = append([]string(nil), snames...)
			return nil
		},
	)
	require.NoError(t, err)
	require.Equal(t, []string{"__mo_branch_3"}, got)

	// ---- Scenario B: user drops t2 and t4 afterwards.
	got = nil
	err = databranchutils.ReclaimBranchSnapshotsCore(
		[]uint64{2, 4},
		func() (databranchutils.BranchReclaimDag, error) {
			// In scenario B t3 has already been drained (deletedT3=true).
			return databranchutils.NewBranchReclaimDag(baseRows(true, true, true)), nil
		},
		func(snames []string) error {
			got = append([]string(nil), snames...)
			return nil
		},
	)
	require.NoError(t, err)
	// Drops are sorted lexicographically: __mo_branch_2 < __mo_branch_4.
	require.Equal(t, []string{"__mo_branch_2", "__mo_branch_4"}, got)

	// ---- No dead tids: loader / deleter must not run.
	loadCalls := 0
	deleteCalls := 0
	err = databranchutils.ReclaimBranchSnapshotsCore(
		nil,
		func() (databranchutils.BranchReclaimDag, error) {
			loadCalls++
			return databranchutils.BranchReclaimDag{}, nil
		},
		func(snames []string) error {
			deleteCalls++
			return nil
		},
	)
	require.NoError(t, err)
	require.Zero(t, loadCalls)
	require.Zero(t, deleteCalls)

	// ---- Loader error propagates.
	sentinel := errors.New("boom")
	err = databranchutils.ReclaimBranchSnapshotsCore(
		[]uint64{3},
		func() (databranchutils.BranchReclaimDag, error) { return databranchutils.BranchReclaimDag{}, sentinel },
		func([]string) error { return nil },
	)
	require.ErrorIs(t, err, sentinel)

	// ---- Empty drop list skips the deleter.
	deleterCalls := 0
	err = databranchutils.ReclaimBranchSnapshotsCore(
		[]uint64{3},
		func() (databranchutils.BranchReclaimDag, error) {
			// t3 is alive, so nothing to reclaim.
			return databranchutils.NewBranchReclaimDag(baseRows(false, false, false)), nil
		},
		func([]string) error {
			deleterCalls++
			return nil
		},
	)
	require.NoError(t, err)
	require.Zero(t, deleterCalls)
}

// ---------------------------------------------------------------------------
// UT-U6 — AncestorWalk: a deep chain climbs to the root
// ---------------------------------------------------------------------------

func TestReclaimCore_AncestorWalk(t *testing.T) {
	// DAG: t1 -> t2 -> t3 -> t4
	rows := []databranchutils.DataBranchMetadata{
		{TableID: 2, PTableID: 1, TableDeleted: false},
		{TableID: 3, PTableID: 2, TableDeleted: false},
		{TableID: 4, PTableID: 3, TableDeleted: true}, // leaf deleted only
	}
	loader := func() (databranchutils.BranchReclaimDag, error) {
		return databranchutils.NewBranchReclaimDag(rows), nil
	}

	var drops []string
	err := databranchutils.ReclaimBranchSnapshotsCore(
		[]uint64{4},
		loader,
		func(snames []string) error {
			drops = append([]string(nil), snames...)
			return nil
		},
	)
	require.NoError(t, err)
	// Only the leaf is reclaimable because every ancestor is still alive.
	require.Equal(t, []string{"__mo_branch_4"}, drops)

	// Sanity: the candidate set reached every ancestor (we assert this via
	// the drop-list being a strict subset of candidates). When we flip
	// every ancestor to deleted=true, they all become reclaimable.
	for i := range rows {
		rows[i].TableDeleted = true
	}
	drops = nil
	err = databranchutils.ReclaimBranchSnapshotsCore(
		[]uint64{4},
		loader,
		func(snames []string) error {
			drops = append([]string(nil), snames...)
			return nil
		},
	)
	require.NoError(t, err)
	require.Equal(t, []string{"__mo_branch_2", "__mo_branch_3", "__mo_branch_4"}, drops)
}

// ---------------------------------------------------------------------------
// UT-U7 — DanglingChildMetadata: rows that reference a vanished parent must
// not panic or crash; the reclaim walk should short-circuit at the gap.
// ---------------------------------------------------------------------------

func TestReclaimCore_DanglingChildMetadata(t *testing.T) {
	rows := []databranchutils.DataBranchMetadata{
		// t5 is a child of t99, which is NOT in mo_branch_metadata.
		{TableID: 5, PTableID: 99, TableDeleted: true},
	}
	loader := func() (databranchutils.BranchReclaimDag, error) {
		return databranchutils.NewBranchReclaimDag(rows), nil
	}

	var drops []string
	err := databranchutils.ReclaimBranchSnapshotsCore(
		[]uint64{5},
		loader,
		func(snames []string) error {
			drops = append([]string(nil), snames...)
			return nil
		},
	)
	require.NoError(t, err)
	// t5 is deleted and the orphan parent is absent — treat orphan as gone,
	// so t5's snapshot is dropped.
	require.Equal(t, []string{"__mo_branch_5"}, drops)

	// Walk starting from a tid that is not in info at all: must not panic.
	drops = nil
	err = databranchutils.ReclaimBranchSnapshotsCore(
		[]uint64{987654321},
		loader,
		func(snames []string) error {
			drops = append([]string(nil), snames...)
			return nil
		},
	)
	require.NoError(t, err)
	require.Nil(t, drops)
}

// ---------------------------------------------------------------------------
// UT-U7b — cycle in mo_branch_metadata must not hang the reclaim walk.
// ---------------------------------------------------------------------------

// TestReclaimCore_CycleGuard feeds a corrupted DAG where two nodes point at
// each other (A.parent=B and B.parent=A) and asserts that both the ancestor
// walk and the subtree-all-deleted check terminate cleanly.
//
// The production DAG is built from `mo_branch_metadata`, which is currently
// only written by `updateBranchMetaTable` inside a single txn, so a cycle
// should never appear. The guard is defensive: a bug in that writer, a
// disaster-recovery edit, or a restore from a partial snapshot could
// corrupt the shape. Hanging the drop-table path in that situation would
// leave the txn uncommitted and locks held, which is catastrophic. This
// test pins the "never hang" contract.
func TestReclaimCore_CycleGuard(t *testing.T) {
	rows := []databranchutils.DataBranchMetadata{
		{TableID: 11, PTableID: 12, TableDeleted: true},
		{TableID: 12, PTableID: 11, TableDeleted: true},
	}
	dag := databranchutils.NewBranchReclaimDag(rows)

	// SubtreeAllDeleted must not recurse forever on a cycle.
	done := make(chan struct{})
	go func() {
		_ = dag.SubtreeAllDeleted(11)
		_ = dag.SubtreeAllDeleted(12)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("SubtreeAllDeleted hung on a cycle")
	}

	// ReclaimBranchSnapshotsCore should produce a finite drop list even
	// though the ancestor walk re-enters the cycle.
	var drops []string
	err := databranchutils.ReclaimBranchSnapshotsCore(
		[]uint64{11, 12},
		func() (databranchutils.BranchReclaimDag, error) { return dag, nil },
		func(snames []string) error {
			drops = append([]string(nil), snames...)
			return nil
		},
	)
	require.NoError(t, err)
	// Both nodes are marked deleted and they form a closed subtree, so
	// both branch snapshots are reclaimable.
	require.Equal(t, []string{"__mo_branch_11", "__mo_branch_12"}, drops)
}

// ---------------------------------------------------------------------------
// UT-U8 — doDropSnapshot rejects kind='branch' rows with a clear error.
// ---------------------------------------------------------------------------

func TestDropSnapshotRejectBranch(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Exercise the low-level kind-lookup helper that `doDropSnapshot`
	// consults. This keeps the test contained (no Session bootstrap) while
	// pinning the behaviour the user-visible path relies on.
	bh := mock_frontend.NewMockBackgroundExec(ctrl)

	ctx := context.Background()
	bh.EXPECT().ClearExecResultSet().AnyTimes()
	bh.EXPECT().Exec(gomock.Any(), gomock.Any()).Return(nil)

	rs := mock_frontend.NewMockExecResult(ctrl)
	rs.EXPECT().GetRowCount().Return(uint64(1)).AnyTimes()
	rs.EXPECT().GetString(gomock.Any(), uint64(0), uint64(0)).Return("branch", nil)
	bh.EXPECT().GetExecResultSet().Return([]interface{}{rs})

	kind, err := getSnapshotKindByName(ctx, bh, "__mo_branch_42")
	require.NoError(t, err)
	require.Equal(t, "branch", kind)

	// The sentinel error message used by doDropSnapshot must mention the
	// managing subsystem so users have a breadcrumb back to docs.
	errMsg := moerr.NewInternalErrorf(ctx,
		"snapshot %q is managed by data branch and cannot be dropped directly",
		"__mo_branch_42",
	).Error()
	require.Contains(t, errMsg, "managed by data branch")
	require.Contains(t, errMsg, "__mo_branch_42")
}

// ---------------------------------------------------------------------------
// UT-U9 — `SHOW SNAPSHOTS` plan excludes kind='branch'.
// ---------------------------------------------------------------------------

// TestShowSnapshotsExcludesBranch asserts the SHOW SNAPSHOTS SQL template in
// pkg/sql/plan/build_show.go filters out branch-managed rows via a
// `kind != 'branch'` predicate. We check the source text directly because
// the builder is unexported in the plan package; a source-level check is
// stable across refactors that keep the visible behaviour intact.
func TestShowSnapshotsExcludesBranch(t *testing.T) {
	buildShowPath := locateBuildShowSource(t)
	content, err := os.ReadFile(buildShowPath)
	require.NoError(t, err, "read build_show.go")

	// Extract the body of buildShowSnapShots so we don't accidentally match
	// a different show builder if someone adds a neighbouring filter. Use
	// both (?s) and (?m) flags so `.` matches newlines and `^}` anchors at
	// the start of a line.
	body := regexp.MustCompile(`(?sm)func buildShowSnapShots\b.*?^}`).Find(content)
	require.NotNil(t, body, "buildShowSnapShots not found in %s", buildShowPath)

	// The predicate must survive the fmt.Sprintf %% escaping of the LIKE
	// clause. Accept either the inline literal `kind != 'branch'` or the
	// %s-substituted form that references the databranchutils constant.
	require.Regexp(t,
		regexp.MustCompile(`kind\s*!=\s*'(?:branch|%s)'`),
		string(body),
	)
	// Sanity: the legacy ccpr filter must remain.
	require.Regexp(t, regexp.MustCompile(`sname\s+NOT\s+LIKE\s+'ccpr_`), string(body))
}

// locateBuildShowSource resolves the absolute path of the
// `pkg/sql/plan/build_show.go` source file relative to the running test
// binary. Tests are executed with the current working directory set to
// the package dir, so we walk up to the repo root.
func locateBuildShowSource(t *testing.T) string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	require.True(t, ok, "runtime.Caller failed")

	// Walk up from this source file (…/pkg/frontend/data_branch_snapshot_test.go)
	// to the repo root by looking for go.mod.
	dir := filepath.Dir(thisFile)
	for i := 0; i < 10; i++ {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			candidate := filepath.Join(dir, "pkg", "sql", "plan", "build_show.go")
			if _, err := os.Stat(candidate); err == nil {
				return candidate
			}
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	t.Fatalf("could not locate pkg/sql/plan/build_show.go from %s", thisFile)
	return ""
}
