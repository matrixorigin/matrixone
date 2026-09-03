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

// SqlProcess.SnapshotTS behaviour of the classic fulltext TVF under a named snapshot
// (#27941).

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

// A snapshotted MATCH puts the snapshot TS on the SqlProcess.
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

// Without a snapshot the SqlProcess carries no TS.
func TestFulltextSQLProcessNoSnapshotLeavesTSNil(t *testing.T) {
	proc := testutil.NewProc(t)
	t.Cleanup(proc.Free)

	require.Nil(t, (&fulltextState{}).sqlProcess(proc).SnapshotTS,
		"an unsnapshotted MATCH must leave the read at the current txn")
}

// The snapshot TS and the publisher identity override coexist on the same SqlProcess.
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

// resetRowState clears the snapshot.
func TestFulltextResetRowStateClearsSnapshot(t *testing.T) {
	proc := testutil.NewProc(t)
	t.Cleanup(proc.Free)

	ts := timestamp.Timestamp{PhysicalTime: 1700000000}
	u := &fulltextState{scanSnapshot: &plan.Snapshot{TS: &ts}}

	u.resetRowState(proc)
	require.Nil(t, u.scanSnapshot, "resetRowState must clear the per-row snapshot")
	require.Nil(t, u.sqlProcess(proc).SnapshotTS, "and the next SqlProcess must read current")
}
