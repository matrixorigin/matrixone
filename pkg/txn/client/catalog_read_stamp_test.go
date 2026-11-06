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
	"github.com/stretchr/testify/require"
)

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
