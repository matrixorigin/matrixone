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

package sqlexec

import (
	"testing"

	"github.com/golang/mock/gomock"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

// txnForRun is the shared foundation for index-search snapshot reads (#27941): it
// clones the read txn at a historical SnapshotTS so nested index-table SQL time-travels,
// and otherwise returns the current txn unchanged. current txn SnapshotTS = 10 below.

func TestTxnForRunHistoricalSnapshotClones(t *testing.T) {
	ctrl := gomock.NewController(t)
	proc := testutil.NewProc(t)
	t.Cleanup(proc.Free)
	original := mock_frontend.NewMockTxnOperator(ctrl)
	clone := mock_frontend.NewMockTxnOperator(ctrl)
	proc.Base.TxnOperator = original

	snapshotTS := timestamp.Timestamp{PhysicalTime: 8} // < current (10) => historical
	original.EXPECT().Txn().Return(txn.TxnMeta{SnapshotTS: timestamp.Timestamp{PhysicalTime: 10}})
	original.EXPECT().CloneSnapshotOp(snapshotTS).Return(clone)

	sp := &SqlProcess{Proc: proc, SnapshotTS: &snapshotTS}
	require.Same(t, clone, sp.txnForRun(proc), "a historical SnapshotTS must clone the read txn at that TS")
}

func TestTxnForRunNoSnapshotUsesCurrent(t *testing.T) {
	ctrl := gomock.NewController(t)
	proc := testutil.NewProc(t)
	t.Cleanup(proc.Free)
	original := mock_frontend.NewMockTxnOperator(ctrl)
	proc.Base.TxnOperator = original

	// No CloneSnapshotOp / Txn expectations: with nil SnapshotTS the current txn is
	// returned without inspecting it. gomock fails the test if any unexpected call fires.
	sp := &SqlProcess{Proc: proc}
	require.Same(t, original, sp.txnForRun(proc), "no SnapshotTS must return the current txn, no clone")
}

func TestTxnForRunNonHistoricalSnapshotUsesCurrent(t *testing.T) {
	ctrl := gomock.NewController(t)
	proc := testutil.NewProc(t)
	t.Cleanup(proc.Free)
	original := mock_frontend.NewMockTxnOperator(ctrl)
	proc.Base.TxnOperator = original

	// SnapshotTS >= current txn SnapshotTS: nothing historical to read, so no clone.
	future := timestamp.Timestamp{PhysicalTime: 20}
	original.EXPECT().Txn().Return(txn.TxnMeta{SnapshotTS: timestamp.Timestamp{PhysicalTime: 10}})

	sp := &SqlProcess{Proc: proc, SnapshotTS: &future}
	require.Same(t, original, sp.txnForRun(proc), "a non-earlier SnapshotTS must not clone")
}

func TestTxnForRunEmptySnapshotUsesCurrent(t *testing.T) {
	ctrl := gomock.NewController(t)
	proc := testutil.NewProc(t)
	t.Cleanup(proc.Free)
	original := mock_frontend.NewMockTxnOperator(ctrl)
	proc.Base.TxnOperator = original

	// An empty (zero) SnapshotTS is not a real snapshot => current txn, no clone.
	// IsEmpty() short-circuits before the current txn's TS is even inspected, so no
	// Txn()/CloneSnapshotOp expectations (gomock fails on any unexpected call).
	empty := timestamp.Timestamp{}

	sp := &SqlProcess{Proc: proc, SnapshotTS: &empty}
	require.Same(t, original, sp.txnForRun(proc), "an empty SnapshotTS must not clone")
}

// EffectiveSnapshotTS guards: no txn operator, no process, and the historical case.

func TestEffectiveSnapshotTSNilTxnOperatorIsNotHistorical(t *testing.T) {
	proc := testutil.NewProc(t)
	t.Cleanup(proc.Free)
	proc.Base.TxnOperator = nil

	snapshotTS := timestamp.Timestamp{PhysicalTime: 8}
	sp := &SqlProcess{Proc: proc, SnapshotTS: &snapshotTS}
	require.Nil(t, sp.EffectiveSnapshotTS(), "no txn to compare against => not a historical read")
	require.Nil(t, sp.txnForRun(proc), "and nothing to clone")
}

func TestEffectiveSnapshotTSNilProcIsNotHistorical(t *testing.T) {
	snapshotTS := timestamp.Timestamp{PhysicalTime: 8}
	sp := &SqlProcess{SnapshotTS: &snapshotTS}
	require.Nil(t, sp.EffectiveSnapshotTS(), "no process => not a historical read")
}

// A TS earlier than the current txn's is returned, as the same pointer the caller passed.
func TestEffectiveSnapshotTSHistoricalReturnsTheTS(t *testing.T) {
	ctrl := gomock.NewController(t)
	proc := testutil.NewProc(t)
	t.Cleanup(proc.Free)
	txnOp := mock_frontend.NewMockTxnOperator(ctrl)
	txnOp.EXPECT().Txn().Return(txn.TxnMeta{SnapshotTS: timestamp.Timestamp{PhysicalTime: 10}}).AnyTimes()
	proc.Base.TxnOperator = txnOp

	snapshotTS := timestamp.Timestamp{PhysicalTime: 8, LogicalTime: 3}
	sp := &SqlProcess{Proc: proc, SnapshotTS: &snapshotTS}
	require.Same(t, &snapshotTS, sp.EffectiveSnapshotTS())
}

// ApplyScanSnapshot binds BOTH halves of a named snapshot's identity. Threading only the
// timestamp is a cross-account correctness bug: an account-level snapshot carries the owning
// account in Tenant, so without the override the index-table SQL would resolve under the
// CALLING account while the base scan reads the snapshot's account.
func TestApplyScanSnapshotBindsTenantAndTS(t *testing.T) {
	ctrl := gomock.NewController(t)
	proc := testutil.NewProc(t)
	t.Cleanup(proc.Free)
	original := mock_frontend.NewMockTxnOperator(ctrl)
	proc.Base.TxnOperator = original
	original.EXPECT().Txn().Return(txn.TxnMeta{SnapshotTS: timestamp.Timestamp{PhysicalTime: 10}}).AnyTimes()

	snapshotTS := timestamp.Timestamp{PhysicalTime: 8} // < current (10) => historical
	sp := &SqlProcess{Proc: proc}
	ets := sp.ApplyScanSnapshot(&plan.Snapshot{
		TS:     &snapshotTS,
		Tenant: &plan.SnapshotTenant{TenantName: "acc1", TenantID: 42},
	})

	require.NotNil(t, ets, "a TS older than the current txn is a historical read")
	require.Equal(t, snapshotTS, *ets)
	require.NotNil(t, sp.SnapshotAccountID, "the snapshot's owning tenant must be bound")
	require.EqualValues(t, 42, *sp.SnapshotAccountID,
		"the snapshot's account, not the calling session's")
	got, err := sp.EffectiveAccountID()
	require.NoError(t, err)
	require.EqualValues(t, 42, got, "and with no publisher it is what the read resolves under")
}

// A snapshot that is not historical relative to this txn binds nothing: the read is an ordinary
// current-state read as the calling account, matching the compile layer, which rebinds the
// tenant only inside the same condition that clones the txn.
func TestApplyScanSnapshotNonHistoricalBindsNothing(t *testing.T) {
	ctrl := gomock.NewController(t)
	proc := testutil.NewProc(t)
	t.Cleanup(proc.Free)
	original := mock_frontend.NewMockTxnOperator(ctrl)
	proc.Base.TxnOperator = original
	original.EXPECT().Txn().Return(txn.TxnMeta{SnapshotTS: timestamp.Timestamp{PhysicalTime: 10}}).AnyTimes()

	notHistorical := timestamp.Timestamp{PhysicalTime: 20} // >= current
	sp := &SqlProcess{Proc: proc}
	require.Nil(t, sp.ApplyScanSnapshot(&plan.Snapshot{
		TS:     &notHistorical,
		Tenant: &plan.SnapshotTenant{TenantID: 42},
	}))
	require.Nil(t, sp.SnapshotAccountID, "no time travel => no tenant rebinding")
}

// A snapshot with no Tenant (cluster level) time-travels without rebinding the account.
func TestApplyScanSnapshotWithoutTenantOnlyBindsTS(t *testing.T) {
	ctrl := gomock.NewController(t)
	proc := testutil.NewProc(t)
	t.Cleanup(proc.Free)
	original := mock_frontend.NewMockTxnOperator(ctrl)
	proc.Base.TxnOperator = original
	original.EXPECT().Txn().Return(txn.TxnMeta{SnapshotTS: timestamp.Timestamp{PhysicalTime: 10}}).AnyTimes()

	snapshotTS := timestamp.Timestamp{PhysicalTime: 8}
	sp := &SqlProcess{Proc: proc}
	require.NotNil(t, sp.ApplyScanSnapshot(&plan.Snapshot{TS: &snapshotTS}))
	require.Nil(t, sp.SnapshotAccountID)
}

func TestApplyScanSnapshotNilIsNoop(t *testing.T) {
	sp := &SqlProcess{}
	require.Nil(t, sp.ApplyScanSnapshot(nil))
	require.Nil(t, sp.SnapshotTS)
	require.Nil(t, sp.SnapshotAccountID)
	require.Nil(t, sp.AccountIDOverride)
}

// BuildSnapshotTS is the version an index generation built here reflects: the transaction's
// SnapshotTS, recorded as metadata.build_ts. It must be the TSO timestamp and not a wall clock,
// which is the whole reason build_ts exists beside the wall-clock "timestamp" column.
func TestBuildSnapshotTSFromProc(t *testing.T) {
	ctrl := gomock.NewController(t)
	proc := testutil.NewProc(t)
	t.Cleanup(proc.Free)
	op := mock_frontend.NewMockTxnOperator(ctrl)
	proc.Base.TxnOperator = op
	op.EXPECT().Txn().Return(txn.TxnMeta{SnapshotTS: timestamp.Timestamp{PhysicalTime: 4242, LogicalTime: 7}}).AnyTimes()

	sp := &SqlProcess{Proc: proc}
	require.EqualValues(t, 4242, sp.BuildSnapshotTS(),
		"the physical component of the txn SnapshotTS")
}

// The background / ISCP shape carries its operator on SqlCtx instead, and must work identically:
// a CDC sync reaches the metadata writer this way.
func TestBuildSnapshotTSFromSqlContext(t *testing.T) {
	ctrl := gomock.NewController(t)
	op := mock_frontend.NewMockTxnOperator(ctrl)
	op.EXPECT().Txn().Return(txn.TxnMeta{SnapshotTS: timestamp.Timestamp{PhysicalTime: 99}}).AnyTimes()

	sp := &SqlProcess{SqlCtx: &SqlContext{TxnOperator: op}}
	require.EqualValues(t, 99, sp.BuildSnapshotTS())
}

// No transaction to ask => unknown, which readers treat as "no provenance" rather than as a
// generation built at the epoch.
func TestBuildSnapshotTSUnknownWithoutTxn(t *testing.T) {
	require.EqualValues(t, 0, (*SqlProcess)(nil).BuildSnapshotTS())
	require.EqualValues(t, 0, (&SqlProcess{}).BuildSnapshotTS())
	require.EqualValues(t, 0, (&SqlProcess{SqlCtx: &SqlContext{}}).BuildSnapshotTS())

	proc := testutil.NewProc(t)
	t.Cleanup(proc.Free)
	proc.Base.TxnOperator = nil
	require.EqualValues(t, 0, (&SqlProcess{Proc: proc}).BuildSnapshotTS())
}
