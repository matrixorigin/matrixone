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

package client

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/stretchr/testify/require"
)

type catalogReadTestWorkspace struct {
	Workspace
	op          *txnOperator
	visibility  uint64
	stable      bool
	outsideLock bool
}

func (workspace *catalogReadTestWorkspace) CatalogVisibility() (uint64, bool) {
	workspace.outsideLock = workspace.op.mu.TryLock()
	if workspace.outsideLock {
		workspace.op.mu.Unlock()
	}
	return workspace.visibility, workspace.stable
}

type catalogReadCaller struct {
	TxnOperator
	sample func() (CatalogReadStamp, bool)
}

func (caller *catalogReadCaller) TryCatalogReadStamp() (CatalogReadStamp, bool) {
	return caller.sample()
}

func TestCatalogReadStampMatchesBoundDomains(t *testing.T) {
	op := &catalogReadCaller{}
	workspace := &struct{ Workspace }{}
	stamp := (CatalogReadStamp{
		TransactionID: "txn", Snapshot: timestamp.Timestamp{PhysicalTime: 10, LogicalTime: 2},
		Revision: 3, Workspace: workspace,
	}).BindRead(op, 4)
	require.True(t, stamp.MatchesRead(stamp, CatalogSnapshotSemantic))
	require.True(t, stamp.MatchesRead(stamp, CatalogSnapshotStructural))
	for name, change := range map[string]func(*CatalogReadStamp){
		"operator":    func(s *CatalogReadStamp) { s.Op = &catalogReadCaller{} },
		"transaction": func(s *CatalogReadStamp) { s.TransactionID = "other" },
		"workspace":   func(s *CatalogReadStamp) { s.Workspace = &struct{ Workspace }{} },
		"revision":    func(s *CatalogReadStamp) { s.Revision++ },
		"visibility":  func(s *CatalogReadStamp) { s.Visibility++ },
		"physical":    func(s *CatalogReadStamp) { s.Snapshot.PhysicalTime++ },
		"logical":     func(s *CatalogReadStamp) { s.Snapshot.LogicalTime++ },
	} {
		t.Run(name, func(t *testing.T) {
			current := stamp
			change(&current)
			require.False(t, stamp.MatchesRead(current, CatalogSnapshotSemantic))
			require.False(t, stamp.MatchesRead(current, CatalogSnapshotStructural))
		})
	}
	raw := stamp
	raw.Op = nil
	require.False(t, raw.MatchesRead(raw, CatalogSnapshotSemantic), "unbound samples have no caller identity")
	require.False(t, stamp.MatchesRead(stamp, CatalogSnapshotMatch(255)))
}

func TestCatalogReadStampPreservesSnapshotPolicies(t *testing.T) {
	stamp := (CatalogReadStamp{Snapshot: timestamp.Timestamp{PhysicalTime: 10, LogicalTime: 2}}).BindRead(&catalogReadCaller{}, 0)
	current := stamp
	current.Snapshot.NodeID = 7
	require.True(t, stamp.MatchesRead(current, CatalogSnapshotSemantic))
	require.False(t, stamp.MatchesRead(current, CatalogSnapshotStructural))
}

func TestTryCatalogReadStampBindsActualCallerOutsideLock(t *testing.T) {
	_, ok := TryCatalogReadStamp(nil)
	require.False(t, ok)
	runOperatorTests(t, func(_ context.Context, op *txnOperator, _ *testTxnSender) {
		before, err := op.CatalogReadStamp()
		require.NoError(t, err)
		defer op.AddWorkspace(before.Workspace)
		workspace := &catalogReadTestWorkspace{op: op, visibility: 9, stable: true}
		op.AddWorkspace(workspace)
		caller := &catalogReadCaller{TxnOperator: op, sample: op.TryCatalogReadStamp}
		stamp, accepted := TryCatalogReadStamp(caller)
		require.True(t, accepted)
		require.Same(t, caller, stamp.Op)
		require.Same(t, workspace, stamp.Workspace)
		require.Equal(t, uint64(9), stamp.Visibility)
		require.True(t, workspace.outsideLock)
		underlying, accepted := TryCatalogReadStamp(op)
		require.True(t, accepted)
		require.False(t, stamp.MatchesRead(underlying, CatalogSnapshotSemantic), "a wrapper is part of read identity")
		op.mu.Lock()
		_, accepted = TryCatalogReadStamp(caller)
		op.mu.Unlock()
		require.False(t, accepted, "optional capture cannot wait behind transaction work")
		workspace.stable = false
		_, accepted = TryCatalogReadStamp(caller)
		require.False(t, accepted)
	})
}

func TestCatalogReadStampRejectsClosedAndDetectsABA(t *testing.T) {
	runOperatorTests(t, func(ctx context.Context, op *txnOperator, _ *testTxnSender) {
		before, err := op.CatalogReadStamp()
		require.NoError(t, err)
		snapshot := before.Snapshot
		snapshot.PhysicalTime++
		op.SetSnapshotTS(snapshot)
		op.SetSnapshotTS(before.Snapshot)
		after, err := op.CatalogReadStamp()
		require.NoError(t, err)
		require.Equal(t, before.Snapshot, after.Snapshot)
		require.Greater(t, after.Revision, before.Revision)
		op.AddWorkspace(nil)
		changed, err := op.CatalogReadStamp()
		require.NoError(t, err)
		require.Greater(t, changed.Revision, after.Revision)
		op.mu.Lock()
		op.mu.closed = true
		op.mu.Unlock()
		_, err = op.CatalogReadStamp()
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnClosed))
	})
}

func TestTryCatalogReadStampIsNonblockingAndBounded(t *testing.T) {
	var missing *txnOperator
	_, ok := missing.TryCatalogReadStamp()
	require.False(t, ok)
	runOperatorTests(t, func(_ context.Context, op *txnOperator, _ *testTxnSender) {
		expected, err := op.CatalogReadStamp()
		require.NoError(t, err)
		actual, ok := op.TryCatalogReadStamp()
		require.True(t, ok)
		require.Equal(t, expected, actual)
		op.mu.Lock()
		_, ok = op.TryCatalogReadStamp()
		op.mu.Unlock()
		require.False(t, ok, "contention cannot inherit an unbounded transaction wait")
		originalID := op.mu.txn.ID
		defer func() {
			op.mu.Lock()
			op.mu.txn.ID = originalID
			op.mu.terminalAction = txnTerminalActionNone
			op.mu.Unlock()
		}()
		for _, size := range []int{0, 64, 65} {
			op.mu.Lock()
			op.mu.txn.ID = make([]byte, size)
			op.mu.Unlock()
			stamp, accepted := op.TryCatalogReadStamp()
			require.Equal(t, size == 64, accepted)
			if accepted {
				require.Len(t, stamp.TransactionID, size)
			}
		}
		op.mu.Lock()
		op.mu.txn.ID = originalID
		op.mu.terminalAction = txnTerminalActionRollbackAfterRunningSQL
		op.mu.Unlock()
		_, ok = op.TryCatalogReadStamp()
		require.False(t, ok, "terminal work cannot acquire a new planning observation")
		op.mu.Lock()
		op.mu.terminalAction = txnTerminalActionNone
		op.mu.closed = true
		op.mu.Unlock()
		_, ok = op.TryCatalogReadStamp()
		require.False(t, ok)
	})
}

func TestTryCatalogReadStampTracksSnapshotAndWorkspaceABA(t *testing.T) {
	runOperatorTests(t, func(_ context.Context, op *txnOperator, _ *testTxnSender) {
		before, ok := op.TryCatalogReadStamp()
		require.True(t, ok)
		next := before.Snapshot.Next()
		op.SetSnapshotTS(next)
		op.SetSnapshotTS(before.Snapshot)
		after, ok := op.TryCatalogReadStamp()
		require.True(t, ok)
		require.Equal(t, before.Snapshot, after.Snapshot)
		require.Greater(t, after.Revision, before.Revision)
		op.AddWorkspace(nil)
		changed, ok := op.TryCatalogReadStamp()
		require.True(t, ok)
		require.Greater(t, changed.Revision, after.Revision)
	})
}
