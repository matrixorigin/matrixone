// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package disttae

import (
	"context"
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/stretchr/testify/require"
)

// TestIssue25590FragmentSettlementRetiresOnlyOwnSegmentRegistrations pins
// the request-changes review finding on PR #30988 head 6352367bfe (issue
// #25590): an ordinary remote SELECT fragment's terminal — the
// settleRemoteFragmentWorkspaceAfterDrain non-transferred leg,
// FinalizeRemoteFragmentWorkspace(_, false), which runs the full
// workspace Rollback and delTransaction — must not couple to segment
// registrations it does not own. The pre-fix delTransaction removed the
// transaction's segment-map entries by sweeping the ENTIRE CN map by
// txn-ID prefix under the shared mutex, so:
//
//   - every read fragment's settlement paid a whole-CN scan that grew
//     with unrelated open transactions' registrations (the review's
//     diagnostic measured ~449µs at 50k and ~8.1ms at 200k foreign
//     segments), blocking segment registration and lookups CN-wide; and
//   - because the remote statement's mirror workspaces share the USER
//     transaction's ID, the sweep deleted the registration a concurrent
//     WRITE fragment of the same live transaction had made on this CN,
//     silently corrupting later in-transaction delete routing — the
//     registration below must survive the reader fragment's settlement.
//
// The writer registers through the REAL production publish path
// (WriteFile -> stageWorkspaceRecordLocked ->
// publishStagedWorkspaceRecordLocked -> PutCnSegment), so this test source
// also compiles against the pre-fix head and pins both defects: the
// survival assertion fails there, and the fix keeps it while the
// registering workspace's OWN terminal still retires exactly its own
// registration.
func TestIssue25590FragmentSettlementRetiresOnlyOwnSegmentRegistrations(t *testing.T) {
	writer, _, failFS, tbl := newR32MaterializingFixture(t)
	ctx := context.Background()
	mp := writer.proc.Mp()

	// remoterunServer binds one mirror workspace per remote fragment to
	// the SAME user txn operator, so the reader below shares one
	// transaction ID with the writer exactly like a distributed
	// statement's concurrent scopes on one CN.
	reader := newIssue25590SharedOpWorkspaceForTest(t, writer.op)
	txnID := writer.op.Txn().ID
	server := colexec.MustGetServer(writer.engine.service)

	// The write fragment records one published object — the exact class
	// publishStagedWorkspaceRecordLocked registers under the shared user
	// txn ID.
	probeStats := mockStatsList(t, 1)[0]
	probeName := probeStats.ObjectName().String()
	require.NoError(t, writeObjectToFS(ctx, failFS, probeName))
	recBat := colexec.AllocCNS3ResultBat(false)
	defer recBat.Clean(mp)
	require.NoError(t, colexec.ExpandObjectStatsToBatch(
		mp, false, recBat, false, probeStats))
	require.NoError(t, writer.WriteFile(
		INSERT, 1, tbl.db.databaseId, tbl.tableId, "db", tbl.tableName,
		probeName, recBat, writer.tnStores[0]))

	// Sanity: the publish registered the object's segment exactly the way
	// an in-transaction delete lookup (deleteBatch's GetCnSegmentType
	// routing) must find it.
	sid := probeStats.ObjectName().ObjectId().Segment()
	require.Equal(t, int32(colexec.TxnWorkspaceUnCommitType),
		server.GetCnSegmentType(sid, tbl.tableId, txnID),
		"the write fragment's publish registered its workspace segment")

	// The reader fragment's non-transferred settlement — the exact
	// terminal an ordinary remote SELECT runs.
	require.NoError(t, reader.FinalizeRemoteFragmentWorkspace(ctx, false))

	// The registration ABOVE must survive: the read fragment owns no
	// segment, and pre-fix delTransaction swept the whole map by the
	// shared txn ID here, deleting the live write fragment's entry.
	require.Equal(t, int32(colexec.TxnWorkspaceUnCommitType),
		server.GetCnSegmentType(sid, tbl.tableId, txnID),
		"a read-only fragment settlement must not unregister another fragment's segment registration")

	// The write fragment's OWN terminal removes exactly its own
	// registration — the fix may not overcorrect into never removing.
	require.NoError(t, writer.Rollback(ctx))
	require.Zero(t, server.GetCnSegmentType(sid, tbl.tableId, txnID),
		"the registering workspace's terminal retires exactly its own registration")

	// One more live actor keeps functioning after both terminals: a fresh
	// statement of the not-yet-distributed workspace sees the map exactly
	// as its own registrations left it.
	require.Empty(t, reader.pendingObjectCleanupNames(),
		"the reader fragment's settlement left no pending cleanup obligations")
}

// newIssue25590SharedOpWorkspaceForTest mirrors remoterunServer's
// NewTxnWorkSpace construction for a workspace bound to the given user
// txn operator: a minimal, read-only workspace like the per-fragment
// mirror, with the same field set NewTxnWorkSpace installs.
func newIssue25590SharedOpWorkspaceForTest(t *testing.T, op client.TxnOperator) *Transaction {
	t.Helper()
	txn := &Transaction{
		proc:            testutil.NewProc(t),
		op:              op,
		engine:          &Engine{},
		tablesInVain:    make(map[uint64]int),
		batchSelectList: make(map[*batch.Batch][]int64),
		deletedBlocks:   &deletedBlocks{offsets: make(map[types.Blockid][]int64)},
		tableCache:      new(sync.Map),
		tableOps:        newTableOps(),
		databaseOps:     newDbOps(),
	}
	txn.readOnly.Store(true)
	txn.currentRowId.SetSegment(colexec.TxnWorkspaceSegment)
	return txn
}
