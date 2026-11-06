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

package disttae

import (
	"context"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/stretchr/testify/require"
)

func TestWorkspaceDumpCancellationRetainsEarlierStatement(t *testing.T) {
	colexec.NewServer("")
	txn := newTransactionWithActivePKTableForTest(t, "pk")
	issue25590InitDumpableFixture(t, txn)
	defer func() {
		for i := range txn.writes {
			txn.releaseWorkspaceEntryBatchLocked(i)
		}
	}()
	ctx := context.Background()
	issue25590AdmitBoundary(t, txn, ctx)
	source := issue25590AppendInsertEntry(t, txn, 1, 2, 3)
	txn.EndStatement()

	fs := issue25590SwapProcSharedFS(t, txn)
	entered := make(chan struct{})
	fs.mu.Lock()
	fs.writeHook = func(ctx context.Context) error {
		close(entered)
		<-ctx.Done()
		return ctx.Err()
	}
	fs.mu.Unlock()
	txn.writeWorkspaceThreshold = 0
	txn.StartStatement()
	requestCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	joined := false
	defer func() {
		cancel()
		if !joined {
			<-done
		}
	}()
	go func() { done <- txn.IncrStatementID(requestCtx, false) }()
	select {
	case <-entered:
	case <-time.After(30 * time.Second):
		t.Fatal("workspace dump did not reach the file service")
	}
	cancel()
	select {
	case err := <-done:
		joined = true
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(30 * time.Second):
		t.Fatal("canceled dump did not return")
	}
	require.NoError(t, txn.proc.Ctx.Err(), "request cancellation must not stop the transaction context")
	require.Len(t, txn.writes, 1)
	require.Same(t, source, txn.writes[0].bat)
	require.Equal(t, 3, source.RowCount())
	require.NoError(t, txn.checkWorkspaceAccountingLocked())
	txn.UpdateSnapshotWriteOffset()
	require.Equal(t, []int64{1, 2, 3}, issue25590VisibleValuesLocked(t, txn))
	require.NoError(t, txn.RollbackLastStatement(ctx))
	txn.EndStatement()
	require.Equal(t, []int64{1, 2, 3}, issue25590VisibleValuesLocked(t, txn))
	require.NoError(t, txn.checkWorkspaceAccountingLocked())
}
