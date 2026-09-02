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
