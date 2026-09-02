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

package table_function

// TVF-side half of the named-snapshot MATCH fix (#27941) for classic fulltext.
//
// Unlike the vector algorithms, classic fulltext has no veccache entry to key: every index
// read goes through fulltextState.sqlProcess, so the whole fix is that ONE function handing
// the snapshot TS to the SqlProcess. sqlexec.txnForRun then clones the read txn at that TS
// (covered in pkg/vectorindex/sqlexec/snapshot_test.go), and the index SQL time-travels.
//
// These tests pin that hand-off directly rather than through a full start()/call() drive,
// because sqlProcess IS the whole surface -- and it must compose with the publisher
// identity override, which shares the same SqlProcess and predates this fix.

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

// A snapshotted MATCH must put the snapshot TS on the SqlProcess, so the index SQL reads
// the historical index. Before the fix the TS never reached here and every MATCH read the
// current index, whatever snapshot the query named.
func TestFulltextSQLProcessCarriesSnapshotTS(t *testing.T) {
	proc := testutil.NewProc(t)
	t.Cleanup(proc.Free)

	ts := timestamp.Timestamp{PhysicalTime: 1700000000, LogicalTime: 7}
	u := &fulltextState{scanSnapshot: &plan.Snapshot{TS: &ts}}

	sp := u.sqlProcess(proc)
	require.NotNil(t, sp.SnapshotTS, "a snapshotted MATCH must carry the read TS onto the SqlProcess")
	require.Equal(t, ts.PhysicalTime, sp.SnapshotTS.PhysicalTime)
	require.Equal(t, ts.LogicalTime, sp.SnapshotTS.LogicalTime)
}

// No snapshot => no TS, so the index SQL runs on the current txn exactly as before the fix.
func TestFulltextSQLProcessNoSnapshotLeavesTSNil(t *testing.T) {
	proc := testutil.NewProc(t)
	t.Cleanup(proc.Free)

	require.Nil(t, (&fulltextState{}).sqlProcess(proc).SnapshotTS,
		"an unsnapshotted MATCH must leave the read at the current txn")
}

// The snapshot TS and the publisher identity override live on the same SqlProcess and are
// set by the same function; a subscribed (PubInfo) table read on a snapshot needs BOTH, so
// neither may clobber the other.
func TestFulltextSQLProcessSnapshotComposesWithPublisherIdentity(t *testing.T) {
	proc := testutil.NewProc(t)
	t.Cleanup(proc.Free)

	ts := timestamp.Timestamp{PhysicalTime: 1700000000, LogicalTime: 7}
	acct := uint32(42)
	u := &fulltextState{
		scanSnapshot:     &plan.Snapshot{TS: &ts},
		publisherAccount: &acct,
		publisherDB:      "pub_db",
	}

	sp := u.sqlProcess(proc)
	require.NotNil(t, sp.SnapshotTS)
	require.Equal(t, ts.PhysicalTime, sp.SnapshotTS.PhysicalTime)
	require.NotNil(t, sp.AccountIDOverride)
	require.Equal(t, acct, *sp.AccountIDOverride)
	require.Equal(t, "pub_db", sp.DatabaseOverride)
}

// resetRowState runs per query row and must clear the snapshot, so a reused operator cannot
// carry one row's snapshot into the next row's index read.
func TestFulltextResetRowStateClearsSnapshot(t *testing.T) {
	proc := testutil.NewProc(t)
	t.Cleanup(proc.Free)

	ts := timestamp.Timestamp{PhysicalTime: 1700000000}
	u := &fulltextState{scanSnapshot: &plan.Snapshot{TS: &ts}}

	u.resetRowState(proc)
	require.Nil(t, u.scanSnapshot, "resetRowState must clear the per-row snapshot")
	require.Nil(t, u.sqlProcess(proc).SnapshotTS, "and the next SqlProcess must read current")
}
