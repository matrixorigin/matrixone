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

package client

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	commonruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// D206 (r88) the expected-conflict availability contract: the volatile,
// txn-scoped, CN-local mark on the txn operator, its two consult sites
// (this package's commit watchdog and compile's fatalLog), and the
// deterministic mark mechanics. The unmarked + checker-ON disposition is a
// process Fatal by construction and is owned by the cell-owned live RED
// twins (D206(C.2)-(C.4), D208(C.1), D211(C.1), D212(C.2)); in-process these
// tests pin the mark API, the helper's no-op shapes, the defect flag, the
// pessimism gate, and the byte-identical checker-disabled disposition a
// marked transaction receives with the checker enabled.

func newTestExpectedConflictOperator(t *testing.T, mode txn.TxnMode) *txnOperator {
	commonruntime.SetupServiceBasedRuntime("", commonruntime.DefaultRuntime())
	op := newTxnOperator(
		"",
		commonruntime.DefaultRuntime().Logger(),
		clock.NewHLCClock(func() int64 { return 1 }, 0),
		newTestTxnSender(),
		txn.TxnMeta{
			ID:         []byte{0xEE, 0x06},
			SnapshotTS: timestamp.Timestamp{PhysicalTime: 1},
			Mode:       mode,
		},
	)
	require.NotNil(t, op)
	return op
}

func setCheckerEnabled(t *testing.T, on bool) {
	commonruntime.ServiceRuntime("").SetGlobalVariables(
		commonruntime.EnableCheckInvalidRCErrors, on)
}

// commitErrEnvelope rebuilds the CN-observed commit failure: the TN coarse
// class rides TxnErrCode (ErrTAECommit, present in commitTxnErrors) while the
// exact moerr payload (WW conflict or duplicate entry) is restored by
// checkTxnError's UnwrapError.
func commitErrEnvelope(t *testing.T, inner *moerr.Error) *txn.TxnError {
	raw, err := inner.MarshalBinary()
	require.NoError(t, err)
	return &txn.TxnError{
		TxnErrCode: uint32(moerr.ErrTAECommit),
		Error:      raw,
	}
}

func TestExpectedConflictMarkAPI(t *testing.T) {
	op := newTestExpectedConflictOperator(t, txn.TxnMode_Pessimistic)
	assert.False(t, op.ExpectedConflictMarked())
	op.MarkExpectedConflict()
	assert.True(t, op.ExpectedConflictMarked())
	op.MarkExpectedConflict()
	assert.True(t, op.ExpectedConflictMarked(), "the setter is idempotent")
}

func TestMarkTxnExpectedConflictIfSupportedShapes(t *testing.T) {
	// nil operator: no-op.
	MarkTxnExpectedConflictIfSupported(nil)

	// An operator that does not implement the optional interface: no-op.
	type unmarkable struct{ TxnOperator }
	MarkTxnExpectedConflictIfSupported(&unmarkable{})

	// A markable operator gets marked.
	op := newTestExpectedConflictOperator(t, txn.TxnMode_Pessimistic)
	MarkTxnExpectedConflictIfSupported(op)
	assert.True(t, op.ExpectedConflictMarked())
}

func TestExpectedConflictMarkDefectFlag(t *testing.T) {
	op := newTestExpectedConflictOperator(t, txn.TxnMode_Pessimistic)
	restore := DisableExpectedConflictMarkForTest(true)
	MarkTxnExpectedConflictIfSupported(op)
	assert.False(t, op.ExpectedConflictMarked(),
		"the mark-disabled defect flag suppresses every site together")
	restore()
	MarkTxnExpectedConflictIfSupported(op)
	assert.True(t, op.ExpectedConflictMarked(),
		"the restore functor re-enables the setter")
}

// The consult's core: with the checker ARMED, a marked pessimistic
// transaction's commit-path WW conflict RETURNS to the caller instead of
// hitting the verifier's Fatal arm (asserted by the test process staying
// alive through handleErrorResponse).
func TestExpectedConflictWatchdogMarkedSurvivesWW(t *testing.T) {
	setCheckerEnabled(t, true)
	op := newTestExpectedConflictOperator(t, txn.TxnMode_Pessimistic)
	op.MarkExpectedConflict()

	inner := moerr.NewTxnWWConflict(context.Background(), 42,
		"seal over the concurrently committed version")
	resp := txn.TxnResponse{
		Method:   txn.TxnMethod_Commit,
		TxnError: commitErrEnvelope(t, inner),
	}
	err := op.handleErrorResponse(context.Background(), resp)
	require.Error(t, err)
	assert.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnWWConflict),
		"the exact refusal class rides through byte-identically, got %v", err)
}

func TestExpectedConflictWatchdogMarkedSurvivesDuplicate(t *testing.T) {
	setCheckerEnabled(t, true)
	op := newTestExpectedConflictOperator(t, txn.TxnMode_Pessimistic)
	op.MarkExpectedConflict()

	inner := moerr.NewDuplicateEntry(context.Background(), "2", "PRIMARY")
	resp := txn.TxnResponse{
		Method:   txn.TxnMethod_Commit,
		TxnError: commitErrEnvelope(t, inner),
	}
	err := op.handleErrorResponse(context.Background(), resp)
	require.Error(t, err)
	assert.True(t, moerr.IsMoErrCode(err, moerr.ErrDuplicateEntry),
		"the duplicate class rides through byte-identically, got %v", err)
}

// The parity control: an UNMARKED pessimistic transaction with the checker
// DISABLED gets the same returned-error disposition the marked transaction
// gets with the checker armed.
func TestExpectedConflictWatchdogCheckerDisabledControl(t *testing.T) {
	setCheckerEnabled(t, false)
	op := newTestExpectedConflictOperator(t, txn.TxnMode_Pessimistic)

	inner := moerr.NewTxnWWConflict(context.Background(), 42,
		"seal over the concurrently committed version")
	resp := txn.TxnResponse{
		Method:   txn.TxnMethod_Commit,
		TxnError: commitErrEnvelope(t, inner),
	}
	err := op.handleErrorResponse(context.Background(), resp)
	require.Error(t, err)
	assert.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnWWConflict))
}

// The pessimism gate stands verbatim (D206(B.2)): a non-pessimistic
// transaction returns before the verifier arm is even consulted, marked or
// unmarked, checker armed.
func TestExpectedConflictWatchdogPessimismGate(t *testing.T) {
	setCheckerEnabled(t, true)
	op := newTestExpectedConflictOperator(t, txn.TxnMode_Optimistic)

	inner := moerr.NewTxnWWConflict(context.Background(), 42, "overlap")
	resp := txn.TxnResponse{
		Method:   txn.TxnMethod_Commit,
		TxnError: commitErrEnvelope(t, inner),
	}
	err := op.handleErrorResponse(context.Background(), resp)
	require.Error(t, err)
	assert.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnWWConflict))
}
