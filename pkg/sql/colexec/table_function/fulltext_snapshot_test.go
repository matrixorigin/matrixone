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

	"github.com/golang/mock/gomock"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
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

// A subscribed table read at a named snapshot resolves under the PUBLISHER, not under the
// snapshot's tenant. The two identities disagree here and both used to be written into
// AccountIDOverride, so whichever setter ran last won -- a silent dependency on the order of
// two adjacent lines. The mocked txn is what makes this reachable: ApplyScanSnapshot binds a
// tenant only when EffectiveSnapshotTS reports a historical read, and testutil.NewProc has no
// TxnOperator, so a test without one exercises the early return and cannot tell the orders
// apart.
func TestFulltextSQLProcessPublisherOutranksSnapshotTenant(t *testing.T) {
	ctrl := gomock.NewController(t)
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	t.Cleanup(proc.Free)

	// The read txn is NEWER than the snapshot, so the snapshot TS is genuinely historical.
	txnOp := mock_frontend.NewMockTxnOperator(ctrl)
	txnOp.EXPECT().Txn().Return(txn.TxnMeta{
		SnapshotTS: timestamp.Timestamp{PhysicalTime: 1800000000},
	}).AnyTimes()
	proc.Base.TxnOperator = txnOp

	const publisher = uint32(42)
	const snapshotTenant = uint32(7)
	pub := publisher
	ts := timestamp.Timestamp{PhysicalTime: 1700000000, LogicalTime: 7}
	u := &fulltextState{
		scanSnapshot: &plan.Snapshot{
			TS:     &ts,
			Tenant: &plan.SnapshotTenant{TenantID: snapshotTenant},
		},
		publisherAccount: &pub,
		publisherDB:      "pub_db",
	}

	sp := u.sqlProcess(proc)

	require.NotNil(t, sp.SnapshotTS, "the read stays historical")
	require.Equal(t, ts.PhysicalTime, sp.SnapshotTS.PhysicalTime)
	require.NotNil(t, sp.EffectiveSnapshotTS(), "and the tenant branch was actually reached")

	// Both identities are recorded, neither clobbers the other.
	require.NotNil(t, sp.AccountIDOverride)
	require.Equal(t, publisher, *sp.AccountIDOverride)
	require.NotNil(t, sp.SnapshotAccountID)
	require.Equal(t, snapshotTenant, *sp.SnapshotAccountID)

	// And the account the index-table SQL actually runs as is the publisher, which is where
	// those tables live -- matching the base-table scan's own PubInfo-over-snapshot precedence.
	got, err := sp.EffectiveAccountID()
	require.NoError(t, err)
	require.Equal(t, publisher, got, "the publisher owns the index tables, not the snapshot tenant")
	require.Equal(t, "pub_db", sp.DatabaseOverride)
}

// With no publisher, the snapshot's tenant is what the read resolves under -- the cross-account
// case ApplyScanSnapshot exists for.
func TestFulltextSQLProcessSnapshotTenantAppliesWithoutAPublisher(t *testing.T) {
	ctrl := gomock.NewController(t)
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	t.Cleanup(proc.Free)

	txnOp := mock_frontend.NewMockTxnOperator(ctrl)
	txnOp.EXPECT().Txn().Return(txn.TxnMeta{
		SnapshotTS: timestamp.Timestamp{PhysicalTime: 1800000000},
	}).AnyTimes()
	proc.Base.TxnOperator = txnOp

	ts := timestamp.Timestamp{PhysicalTime: 1700000000, LogicalTime: 7}
	u := &fulltextState{scanSnapshot: &plan.Snapshot{
		TS:     &ts,
		Tenant: &plan.SnapshotTenant{TenantID: 7},
	}}

	sp := u.sqlProcess(proc)
	require.Nil(t, sp.AccountIDOverride, "nothing published, so no execution identity")
	got, err := sp.EffectiveAccountID()
	require.NoError(t, err)
	require.EqualValues(t, 7, got, "the snapshot's owning tenant still binds")
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
