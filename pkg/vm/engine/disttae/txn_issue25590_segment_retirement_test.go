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

// TestIssue25590TransferredFragmentSettlementTearsDownLocalWorkspace
// pins the round-55 request-changes finding (PR #30988, inline
// remoterunServer.go:331): a successful remote fragment's settlement —
// FinalizeRemoteFragmentWorkspace(_, true) — released the object-name
// accounting and returned, while the failed leg ran the full workspace
// Rollback teardown. A single-CPU source scope carrying both
// UpdateWriteS3 and UpdateFlushS3Info stages copied mpool batches in
// the fragment's mirror workspace and registers its CN segments, and
// NOTHING ever revisits a settled mirror workspace again — so an
// idle-but-live completed mirror keeps its staged batches and its
// segment registrations forever: the settlement's own local state.
//
// The successful transfer must therefore tear down the local workspace
// WITHOUT deleting the objects the coordinator adopted:
//
//   - the staged copied batches release, restoring the mpool baseline
//     while the sinkers' files stay on the filesystem for the recording
//     side's records (no storage I/O runs);
//   - the mirror's own segment-map claims release — a sole-claim
//     registration retires with the settlement (pre-fix this leaves
//     the map entry leaked), while an entry the recording workspace
//     re-claimed — what the coordinator's record choke does for an
//     adopted object on the same CN — keeps serving that workspace's
//     routing until its own terminal (round-49's visibility contract,
//     now per-claim), and the recording workspace's rollback retires
//     it as the last claim.
func TestIssue25590TransferredFragmentSettlementTearsDownLocalWorkspace(t *testing.T) {
	writer, _, failFS, tbl := newR32MaterializingFixture(t)
	ctx := context.Background()
	mp := writer.proc.Mp()
	server := colexec.MustGetServer(writer.engine.service)
	txnID := writer.op.Txn().ID

	// The workspace the coordinator re-records on: one mirror bound to
	// the SAME user txn operator, exactly the shape the coordinator's
	// recording workspace carries on this CN.
	recorder := newIssue25590SharedOpWorkspaceForTest(t, writer.op)

	stats := mockStatsList(t, 2)
	bats := make([]*batch.Batch, 0, 2)
	defer func() {
		for _, bat := range bats {
			bat.Clean(mp)
		}
	}()
	names := make([]string, 0, 2)
	for i := range stats {
		probeStats := stats[i]
		names = append(names, probeStats.ObjectName().String())
		recBat := colexec.AllocCNS3ResultBat(false)
		bats = append(bats, recBat)
		require.NoError(t, colexec.ExpandObjectStatsToBatch(
			mp, false, recBat, false, probeStats))
		require.NoError(t, writeObjectToFS(ctx, failFS, names[i]))
	}

	baseline := mp.OnHeapCurrNB()
	require.False(t, writer.removed, "the mirror workspace is live before its settlement")

	// Write 1 — the mirror is the ONLY workspace that claims this
	// segment: the fragment-CN shape where no re-record happens.
	require.NoError(t, writer.WriteFile(
		INSERT, 1, tbl.db.databaseId, tbl.tableId, "db", tbl.tableName,
		names[0], bats[0], writer.tnStores[0]))

	// Write 2 — the same publish plus the coordinator's re-record claim:
	// after the adoption barrier hands the name to the recording side,
	// the record choke's publishStagedWorkspaceRecordLocked claims the
	// same (txnID, table, segment) entry in the RECORDING workspace's
	// ledger on this CN.
	require.NoError(t, writer.WriteFile(
		INSERT, 1, tbl.db.databaseId, tbl.tableId, "db", tbl.tableName,
		names[1], bats[1], writer.tnStores[0]))
	sidSole := stats[0].ObjectName().ObjectId().Segment()
	sidClaimed := stats[1].ObjectName().ObjectId().Segment()
	server.PutCnSegment(&recorder.workspaceSegments, txnID, tbl.tableId,
		sidClaimed, colexec.TxnWorkspaceUnCommitType)

	require.False(t, writer.removed)
	require.Greater(t, mp.OnHeapCurrNB(), baseline,
		"the downstream source.Write's staged copies are live mirror-workspace memory before the settlement")
	require.Equal(t, int32(colexec.TxnWorkspaceUnCommitType),
		server.GetCnSegmentType(sidSole, tbl.tableId, txnID),
		"the mirror registered its persisted object's segment")
	require.Equal(t, int32(colexec.TxnWorkspaceUnCommitType),
		server.GetCnSegmentType(sidClaimed, tbl.tableId, txnID),
		"the recording workspace's re-record sees the segment it claimed")
	require.NotZero(t, writer.workspaceSize,
		"the mirror's workspace accounting carries its staged entries")

	// The successful fragment's terminal: transfer the objects, then
	// tear down the local workspace state.
	require.NoError(t, writer.FinalizeRemoteFragmentWorkspace(ctx, true))

	// Local teardown without deleting adopted objects: the local state
	// of a settled mirror retires exactly, and the stored objects stay
	// on the filesystem for the recording side's records.
	require.True(t, writer.removed,
		"a successful transfer terminates the mirror workspace — no other terminal ever reaches it")
	require.Equal(t, baseline, mp.OnHeapCurrNB(),
		"the transferred settlement released every staged copy — the successful-transfer mpool baseline (on-heap) is restored")
	require.Zero(t, writer.workspaceSize,
		"the workspace accounting is torn down with the recorded-name settlement")
	require.Empty(t, writer.pendingObjectCleanupNames(),
		"the settlement left no pending cleanup obligations behind")
	require.Contains(t, failFS.persistedSnapshot(), names[0],
		"an adopted object's file stays exactly where its record owns its fate")
	require.Contains(t, failFS.persistedSnapshot(), names[1],
		"the re-claimed object's file stays on the same transferred-name fate")

	// Segment claims: the mirror's sole-claim registration retired with
	// the settlement — the leak the review found — while the entry the
	// recording workspace re-claimed keeps serving that workspace's
	// routing until ITS terminal (round-49's same-transaction
	// visibility, released per claim now).
	require.Zero(t, server.GetCnSegmentType(sidSole, tbl.tableId, txnID),
		"the mirror's sole segment claim retired with its local workspace teardown")
	require.Equal(t, int32(colexec.TxnWorkspaceUnCommitType),
		server.GetCnSegmentType(sidClaimed, tbl.tableId, txnID),
		"the recording workspace's claim keeps the entry routing while that workspace lives")

	// The recording workspace's own terminal is the LAST claim on
	// its entry: the dual-claimed registration cannot outlive every
	// owner either, and its objects stay untouched by that terminal's
	// own storage fate — the transfer already handed the names' fate
	// to the records.
	require.NoError(t, recorder.FinalizeRemoteFragmentWorkspace(ctx, false))
	require.Zero(t, server.GetCnSegmentType(sidClaimed, tbl.tableId, txnID),
		"the recording workspace's terminal released the entry's last claim")
	require.Contains(t, failFS.persistedSnapshot(), names[1],
		"the mirror-side settlement never handed the adopted object's storage fate anywhere but its record")
}
