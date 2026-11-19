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

// Round-57 (issue #25590 review of PR #30988 head 9df36413c2, P2 at
// pkg/vm/engine/disttae/txn.go:1818): the reported same-CN re-recording
// case - two workspaces of ONE Engine recording the same transferred
// object, exactly the topology the r55 design contract documents
// (docs/design/20261021-issue-25590-workspace-scaling-and-cleanup-ownership.md,
// round-55 record) - collapsed the capacity ledger's per-name
// membership to one entry shared by both claims. The fragment's
// transferred settlement then deleted that single shared membership and
// returned its capacity while the RECORDING workspace still claimed the
// object and its physical file was still live, so the recording side's
// obligation became invisible to the admission bound: with the limit
// pinned at two, the same-engine topology admitted two further
// reservations on top of the recorder's still-live obligation - three
// obligations behind a two-object bound, the exact distinct-engine
// control shape the bound refuses.
//
// The per-claim retirement rule r55 established for segment entries -
// a settlement releases exactly ITS OWN claim, the capacity retires
// only when the name's FINAL claim does - therefore governs the ledger
// as well: a recording workspace's published-class release keeps the
// membership counted while another claimant still lives, a queued
// entry or a storage deletion still retires the whole membership (the
// queue and the deleted file ARE the obligation then), and every
// membership-creating path registers the CREATING workspace's claim,
// idempotent per (workspace, name), so a duplicate record still spends
// capacity exactly once.
//
// The production-path regression below compiles against the pre-fix
// head: every assertion is behavioral (the live counter, the
// reservation gate's verdicts, the physical file's fate), so the RED
// run fails exactly at the review's capacity contract and the fixed
// tree passes the same assertions.

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/stretchr/testify/require"
)

// TestIssue25590SharedEngineReRecordKeepsLedgerCapacityThroughTransfer
// drives the reviewer's topology through the REAL production surfaces
// only: the fragment mirror's WriteFile choke admits the transferred
// object, the recording workspace of the SAME Engine re-records it
// through its own choke (the adoption-before-ACK shape
// publishStagedWorkspaceRecordLocked performs), and the fragment's
// transferred settlement runs exactly as remoterunServer ends a clean
// fragment.
func TestIssue25590SharedEngineReRecordKeepsLedgerCapacityThroughTransfer(t *testing.T) {
	writer, e, failFS, tbl := newR32MaterializingFixture(t)
	ctx := context.Background()
	mp := writer.proc.Mp()

	// The bound the probe prices in: two retained objects. Every
	// reservation the gate lets through below is therefore observable
	// against the recorder's still-live obligation.
	knobSaved := unpublishedPendingAdmissionLimit
	t.Cleanup(func() { unpublishedPendingAdmissionLimit = knobSaved })
	unpublishedPendingAdmissionLimit = 2

	// Single-CN topology: the recording workspace shares the fragment's
	// engine, so both claims live on ONE capacity ledger.
	recorder := newIssue25590SharedOpWorkspaceForTest(t, writer.op)
	recorder.engine = e

	stats := mockStatsList(t, 1)
	name := stats[0].ObjectName().String()
	recBat := colexec.AllocCNS3ResultBat(false)
	defer recBat.Clean(mp)
	require.NoError(t, colexec.ExpandObjectStatsToBatch(
		mp, false, recBat, false, stats[0]))
	require.NoError(t, writeObjectToFS(ctx, failFS, name))

	liveOwned := func() int64 {
		e.unpublishedPending.mu.Lock()
		defer e.unpublishedPending.mu.Unlock()
		return e.unpublishedPending.live
	}

	// 1 - the fragment's own record claims the object's capacity once.
	require.NoError(t, writer.WriteFile(
		INSERT, 1, tbl.db.databaseId, tbl.tableId, "db", tbl.tableName,
		name, recBat, writer.tnStores[0]))
	require.Equal(t, int64(1), liveOwned(),
		"the fragment's record claims one capacity unit for the transferred object")

	// 2 - the coordinator's same-CN re-record of the transferred
	// object: the recording workspace's choke runs on the SAME engine
	// (production publishStagedWorkspaceRecordLocked after the
	// adoption barrier), so the name is already counted and the
	// record registers the recorder's claim without spending a second
	// capacity unit - one obligation, two workspace claims.
	require.NoError(t, recorder.WriteFile(
		INSERT, 1, tbl.db.databaseId, tbl.tableId, "db", tbl.tableName,
		name, recBat, writer.tnStores[0]))
	require.Equal(t, int64(1), liveOwned(),
		"a same-engine re-record of an already-counted object claims no second capacity unit")

	// 3 - the fragment's transferred settlement retires exactly the
	// fragment's claim; the recorder's claim keeps the object's
	// capacity counted - the review's defect released the shared
	// membership here and the recorder's live obligation escaped the
	// bound.
	require.NoError(t, writer.FinalizeRemoteFragmentWorkspace(ctx, true))
	require.Equal(t, int64(1), liveOwned(),
		"the fragment's settlement keeps the recording workspace's claim counted — the capacity of a shared object retires only with its final claim")
	require.Contains(t, failFS.persistedSnapshot(), name,
		"the transferred settlement owns no deletion — the recorded object's file keeps its record's fate")

	// 4 - the bound stays honest for the still-live recorder: exactly
	// ONE further reservation fits on this engine, the same verdict a
	// distinct-engine recorder's ledger would give; the second is
	// refused fail-closed.
	require.NoError(t, e.reserveUnpublishedPendingObject("issue25590-r57-further-1"),
		"one further reservation fits beside the recorder's counted obligation")
	require.Error(t, e.reserveUnpublishedPendingObject("issue25590-r57-further-2"),
		"the second further reservation exceeds the bound — the recorder's claim still occupies capacity")
	e.abandonUnpublishedPendingReservations([]string{"issue25590-r57-further-1"})

	// 5 - the recording workspace's own terminal is the FINAL claim on
	// the object: its published-class release retires the last claim
	// and the capacity with it, with the physical file untouched by
	// either settlement.
	require.NoError(t, recorder.FinalizeRemoteFragmentWorkspace(ctx, true))
	require.Equal(t, int64(0), liveOwned(),
		"the recording workspace's terminal retires the final claim and frees the capacity")
	require.Contains(t, failFS.persistedSnapshot(), name,
		"a published-class terminal deletes nothing — the object's fate stays with its record")

	// 6 - the retired capacity re-opens admission fully: both bound
	// units are available again and nothing of the settled pair leaks.
	require.NoError(t, e.reserveUnpublishedPendingObject("issue25590-r57-further-3"))
	require.NoError(t, e.reserveUnpublishedPendingObject("issue25590-r57-further-4"),
		"the bound's full capacity returned with the final claim's retirement")
	e.abandonUnpublishedPendingReservations([]string{
		"issue25590-r57-further-3", "issue25590-r57-further-4"})

	e.unpublishedPending.mu.Lock()
	live, inFlight := e.unpublishedPending.live, e.unpublishedPending.inFlight
	e.unpublishedPending.mu.Unlock()
	require.Zero(t, live,
		"no settled workspace left claimed capacity behind")
	require.Zero(t, inFlight,
		"every probe reservation settled back into the bound")
}
