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

package databranchutils

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
)

// This file is the protocol half of the pk-move retention-ledger fencing
// witness subsystem (design D173-D178). One fencing ROUND runs before the
// retirement's first ledger grant (and before every re-fence grant) and
// produces one fencing record (C_f, C_f1, B_f):
//
//  1. An exclusive admission lock on the round's FRESH row_kind=1 scratch
//     key records the current lock-table binding B_f. Scratch keys live in a
//     PK namespace disjoint from every real (row_kind=0) caller key, so the
//     round can never wait on a caller-held lock.
//  2. T_f1: a leading kind=1 sweep whose candidates are only rows older
//     than the age hint (a scheduling hint, never load-bearing), each
//     reclaimed ONLY under its witnessed lock grant with a bounded
//     lock-wait SKIP on timeout, then exactly one scratch insert, then a
//     real commit assigning C_f1.
//  3. T_f2: an ownership-precondition point probe of the round's own
//     scratch key; absent means the round is INTERRUPTED and NO commit is
//     issued; present means one delete and a real commit assigning C_f.
//
// Call contract: the retiring statement (or its fencing helper session)
// drives ExecuteDataBranchFencingRound through a DataBranchFencingLaneRunner
// (the driver supplies lanes; tests may drive inline); the retiring side
// then applies AcceptDataBranchFencingRecord against its exclusive ledger
// grant (B_r, fence) and routes rejections through
// ClassifyDataBranchRetireRejection + DataBranchRefenceCounter. Every
// consumer of this file must honor: ErrTxnUnknown is CONSUMED by the
// classification layer (never re-thrown to the statement), and the only
// statement-visible fencing failure is the determinate internal-class
// error built by NewDataBranchFencingUnavailable (never a
// transaction-identity code).

// DataBranchFencingBinding is the (Table, ServiceID, Version) identity of
// the lock-table binding generation recorded by an admission grant: B_f
// from the round's scratch grant, B_r from the retirement's ledger grant.
type DataBranchFencingBinding struct {
	Table     uint64
	ServiceID string
	Version   uint64
}

func (b DataBranchFencingBinding) IsZero() bool {
	return b.Table == 0 || b.ServiceID == "" || b.Version == 0
}

func (b DataBranchFencingBinding) Equal(rhs DataBranchFencingBinding) bool {
	return b.Table == rhs.Table && b.ServiceID == rhs.ServiceID && b.Version == rhs.Version
}

// DataBranchFencingBindingFromLockTable projects a lock-table binding the
// lock client recorded (lock_op.go AddLockTable) into the fencing view.
func DataBranchFencingBindingFromLockTable(t lock.LockTable) DataBranchFencingBinding {
	return DataBranchFencingBinding{Table: t.Table, ServiceID: t.ServiceID, Version: t.Version}
}

// DataBranchFencingRoundInput is one round's identity: the fresh scratch
// table_id, the round issue time written into created_ts, the last writer
// commit the fencing pair must out-order, and the sweep scheduling knobs.
type DataBranchFencingRoundInput struct {
	// ScratchTableID must be a fresh per-round draw (see
	// DataBranchFencingScratchDraw); reusing one breaks ownership.
	ScratchTableID uint64
	// IssueTimeNS is the round's issue time stored in created_ts
	// (physical ns); the sweep age hint derives from it.
	IssueTimeNS uint64
	// LastWriterCommitTS is C: the last link-writer commit observed before
	// T_f1. Empty when no writer committed on this ledger yet.
	LastWriterCommitTS timestamp.Timestamp
	// SweepAgeCutoffNS selects kind=1 candidates with created_ts older
	// than the cutoff. It is a scheduling hint only and never proves
	// abandonment; a candidate dies only under its witnessed lock grant.
	SweepAgeCutoffNS uint64
	// SweepLockWait bounds one candidate's witnessed lock-wait; a timeout
	// SKIPs the candidate for a later sweep.
	SweepLockWait time.Duration
}

// DataBranchScratchGrant is what the exclusive admission lock on the
// round's fresh scratch key delivers: the recorded binding B_f and the
// grant watermark. The implementation must record the binding for the
// (table, group) of the ledger table (lock_op.go: AddLockTable).
type DataBranchScratchGrant struct {
	Binding   DataBranchFencingBinding
	WitnessTS timestamp.Timestamp
}

// DataBranchFencingStageShape summarizes the write content one committed
// stage carried — the micro-shape gate's input (D182(B.1)(i)): T_f1 may
// hold only the bounded kind=1 sweep tombstones plus exactly one scratch
// insert; T_f2 exactly one kind=1 point tombstone; neither may commit
// through the no-write fast path, which would leave C_f1/C_f unassigned.
type DataBranchFencingStageShape struct {
	// TargetLedgerTable: every write touched the retention ledger table.
	TargetLedgerTable bool
	InsertBatches     int
	PointTombstones   int
	SweepTombstones   int
	// ExtraWrites counts any write outside the ledger kind=1 namespace.
	ExtraWrites int
	// NoWriteFastPath marks a commit resolved locally with no TN write.
	NoWriteFastPath bool
}

// DataBranchFencingF1Commit is T_f1's committed outcome.
type DataBranchFencingF1Commit struct {
	CommitTS timestamp.Timestamp
	// Swept lists candidate ids reclaimed under witnessed grants;
	// Skipped lists candidates left alone after bounded lock-wait
	// timeouts (ordinary residue for a later sweep).
	Swept   []uint64
	Skipped []uint64
	Shape   DataBranchFencingStageShape
	// Cert is the certification value published BEFORE this stage's
	// commit was invoked (D184(B.1)(iv), carried by D185(B.1)(iv)): the
	// stage observed CHECK A (lane admission) and CHECK B (pre-commit)
	// envelope loads with the driver-exact staged shape and floors. A
	// zero Cert is tolerated by record assembly for legacy fabric
	// producers; the production stage executor always fills it, and the
	// retirement path never accepts a record off a certification bypass.
	Cert DataBranchFencingCertificate
}

// DataBranchFencingF2Commit is T_f2's outcome; ProbeFound=false means the
// ownership precondition failed and NO commit was issued (CommitTS empty).
type DataBranchFencingF2Commit struct {
	ProbeFound bool
	CommitTS   timestamp.Timestamp
	Shape      DataBranchFencingStageShape
	// Cert: same publication contract as DataBranchFencingF1Commit.Cert;
	// when ProbeFound=false no commit happened and Cert stays zero.
	Cert DataBranchFencingCertificate
}

// DataBranchScratchLocks acquires the round's witness locks. The
// production implementation composes LockRowsForAdmissionWithContext;
// fabric tests implement it in memory.
type DataBranchScratchLocks interface {
	// AcquireScratchGrant takes the exclusive admission lock on the
	// round's fresh kind=1 scratch key and records the current binding
	// generation B_f. Lock lifetime rides the acquiring transaction.
	AcquireScratchGrant(ctx context.Context, in DataBranchFencingRoundInput) (DataBranchScratchGrant, error)
}

// DataBranchFencingStageExecutor runs the two short pessimistic RC
// transactions of one round through the account-scoped internal executor.
// Both commits must be real TN requests: indeterminate outcomes surface as
// the ordinary commit error (ErrTxnUnknown) and are classified upstream.
type DataBranchFencingStageExecutor interface {
	// CommitRoundF1 executes the leading witness sweep (skip-on-timeout)
	// then the single scratch insert and commits, returning C_f1.
	CommitRoundF1(ctx context.Context, in DataBranchFencingRoundInput) (DataBranchFencingF1Commit, error)
	// CommitRoundF2 probes the round's own scratch key; absent => no
	// commit and ProbeFound=false; present => delete + commit, C_f.
	CommitRoundF2(ctx context.Context, in DataBranchFencingRoundInput) (DataBranchFencingF2Commit, error)
}

// DataBranchFencingLaneRunner dispatches one round step onto an execution
// lane and awaits it. The fencing driver implements it with lanes, timers
// and classifications; tests can drive steps inline.
type DataBranchFencingLaneRunner interface {
	RunStep(ctx context.Context, step DataBranchFencingStepKind, fn DataBranchFencingLaneFunc) (any, error)
}

// dataBranchFencingOwnedRunner is the owned-round contract the real
// fencing driver implements (D186(B.2), D189(B.1)): the round mints its
// stage owners through the runner (ONE charged owner for the T_f1 pair,
// ONE for T_f2) and dispatches every stage step under its owner with the
// lane publication record (D186(B.1)). Fabric runners do not implement
// it and keep the ownerless legacy geometry of the fabric cells.
type dataBranchFencingOwnedRunner interface {
	DataBranchFencingLaneRunner
	mintStageOwner(scratch uint64, label string) (*DataBranchStageOwner, *DataBranchFencingError)
	runStepOwned(
		ctx context.Context, step DataBranchFencingStepKind, fn DataBranchFencingLaneFunc,
		owner *DataBranchStageOwner, pub *DataBranchFencingLanePublication,
	) (any, error)
}

// DataBranchFencingLaneFunc is one round step: the only thing that crosses
// the lane boundary is its typed (value, error) pair.
type DataBranchFencingLaneFunc func(ctx context.Context) (any, error)

// DataBranchFencingStepKind enumerates the round's lane steps.
type DataBranchFencingStepKind int

const (
	DataBranchFencingStepGrant DataBranchFencingStepKind = iota
	DataBranchFencingStepRoundF1
	DataBranchFencingStepRoundF2
)

func (k DataBranchFencingStepKind) String() string {
	switch k {
	case DataBranchFencingStepGrant:
		return "scratch-grant"
	case DataBranchFencingStepRoundF1:
		return "round-f1"
	case DataBranchFencingStepRoundF2:
		return "round-f2"
	}
	return "unknown"
}

// DataBranchFencingRoundClass is the protocol-level classification of one
// round. Only Committed hands a record over; the rest feed the bounded
// re-fence policy and, at exhaustion, the determinate fencing-unavailable
// statement error.
type DataBranchFencingRoundClass int

const (
	DataBranchFencingRoundCommitted DataBranchFencingRoundClass = iota
	// DataBranchFencingRoundGrantFailed: the scratch admission lock
	// failed before any write.
	DataBranchFencingRoundGrantFailed
	// DataBranchFencingRoundStageFailed: a determinate T_f1/T_f2 failure
	// (known-aborted); the round carries no proven record.
	DataBranchFencingRoundStageFailed
	// DataBranchFencingRoundCommitUnknown: a stage commit came back
	// ErrTxnUnknown (indeterminate). ALWAYS fails the round; the driver
	// consumes it and never re-throws it as the statement's own error.
	DataBranchFencingRoundCommitUnknown
	// DataBranchFencingRoundInterrupted: T_f2's ownership precondition
	// found the round's own key absent; NO empty delete commits.
	DataBranchFencingRoundInterrupted
	// DataBranchFencingRoundGateRejected: the micro-shape gate or record
	// assembly rejected the round (empty binding/commit timestamps,
	// unordered pair, content outside the kind=1 stage shape).
	DataBranchFencingRoundGateRejected
	// DataBranchFencingRoundExpired: T_round fired; the lane lost
	// acceptance authority irrevocably and its late results are refused.
	DataBranchFencingRoundExpired
	// DataBranchFencingRoundSaturated: the lane pool had no charge;
	// fail-loud, no lane id or session was allocated.
	DataBranchFencingRoundSaturated
	// DataBranchFencingRoundPoolSealed: pool admission is sealed by the
	// CN drain; no new round starts during close.
	DataBranchFencingRoundPoolSealed
)

func (c DataBranchFencingRoundClass) String() string {
	switch c {
	case DataBranchFencingRoundCommitted:
		return "committed"
	case DataBranchFencingRoundGrantFailed:
		return "grant-failed"
	case DataBranchFencingRoundStageFailed:
		return "stage-failed"
	case DataBranchFencingRoundCommitUnknown:
		return "commit-unknown-consumed"
	case DataBranchFencingRoundInterrupted:
		return "round-interrupted"
	case DataBranchFencingRoundGateRejected:
		return "gate-rejected"
	case DataBranchFencingRoundExpired:
		return "expired-abandoned"
	case DataBranchFencingRoundSaturated:
		return "saturated"
	case DataBranchFencingRoundPoolSealed:
		return "pool-sealed"
	}
	return "unknown"
}

// DataBranchFencingError is the typed protocol/driver failure the
// retirement call site classifies; it converts to the statement-visible
// determinate internal error only through StatementError /
// NewDataBranchFencingUnavailable.
type DataBranchFencingError struct {
	Class DataBranchFencingRoundClass
	Step  DataBranchFencingStepKind
	Cause error
}

func (e *DataBranchFencingError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("data branch fencing round %s at %s: %v", e.Class, e.Step, e.Cause)
	}
	return fmt.Sprintf("data branch fencing round %s at %s", e.Class, e.Step)
}

func (e *DataBranchFencingError) Unwrap() error { return e.Cause }

// StatementError converts the round failure into the retiring statement's
// ONLY fencing-visible error class: a determinate moerr internal error.
// It is deliberately NOT a transaction-identity code: the frontend's
// txn.go isTxnCommitResultUnknown / wholeTxnRollbackCodes paths can never
// observe a fencing failure through it.
func (e *DataBranchFencingError) StatementError(ctx context.Context) error {
	return NewDataBranchFencingUnavailable(ctx, e.Class, e.Cause)
}

// AsDataBranchFencingError extracts the typed fencing failure.
func AsDataBranchFencingError(err error) (*DataBranchFencingError, bool) {
	var fe *DataBranchFencingError
	if errors.As(err, &fe) {
		return fe, true
	}
	return nil, false
}

// NewDataBranchFencingUnavailable builds the DETERMINATE statement-level
// fencing-unavailable error raised at round-budget exhaustion (D177(B.4)):
// existing internal-error class, never a transaction-identity code.
func NewDataBranchFencingUnavailable(ctx context.Context, class DataBranchFencingRoundClass, cause error) error {
	if cause != nil {
		return moerr.NewInternalErrorf(ctx,
			"data branch fencing unavailable (%s): %v", class, cause)
	}
	return moerr.NewInternalErrorf(ctx, "data branch fencing unavailable (%s)", class)
}

// ExecuteDataBranchFencingRound runs ONE full fencing round: scratch grant,
// then the T_f1/T_f2 pair, then gated record assembly. It does not retry:
// retries are the caller's/driver's policy on the returned classification.
// The returned record is valid only on a nil error; every error is a typed
// *DataBranchFencingError with ErrTxnUnknown already classified, never
// re-thrown raw.
func ExecuteDataBranchFencingRound(
	ctx context.Context,
	runner DataBranchFencingLaneRunner,
	locks DataBranchScratchLocks,
	exec DataBranchFencingStageExecutor,
	in DataBranchFencingRoundInput,
) (DataBranchFencingRecord, error) {
	if in.ScratchTableID == 0 || in.IssueTimeNS == 0 {
		return DataBranchFencingRecord{}, &DataBranchFencingError{
			Class: DataBranchFencingRoundGateRejected,
			Step:  DataBranchFencingStepGrant,
			Cause: moerr.NewInternalErrorf(ctx,
				"malformed round input: scratch id %d, issue time %d", in.ScratchTableID, in.IssueTimeNS),
		}
	}

	// D186(B.2)/D189(B.1): the real fencing driver dispatches the owned
	// geometry; fabric runners (which do not implement the owned contract)
	// keep the ownerless legacy body below verbatim. The amalgamation
	// defect flag forces the legacy path for the driver too, so the
	// RED/oracle pairs can ablate the owned geometry itself.
	if or, ok := runner.(dataBranchFencingOwnedRunner); ok && !dataBranchFencingAmalgamationDisabled.Load() {
		return executeDataBranchFencingRoundOwned(ctx, or, locks, exec, in)
	}

	grantAny, err := runner.RunStep(ctx, DataBranchFencingStepGrant, func(lctx context.Context) (any, error) {
		return locks.AcquireScratchGrant(lctx, in)
	})
	if err != nil {
		return DataBranchFencingRecord{}, classifyFencingStepError(DataBranchFencingStepGrant, err)
	}
	grant, ok := grantAny.(DataBranchScratchGrant)
	if !ok {
		return DataBranchFencingRecord{}, &DataBranchFencingError{
			Class: DataBranchFencingRoundGateRejected,
			Step:  DataBranchFencingStepGrant,
			Cause: moerr.NewInternalErrorf(ctx, "scratch grant step returned %T", grantAny),
		}
	}

	f1Any, err := runner.RunStep(ctx, DataBranchFencingStepRoundF1, func(lctx context.Context) (any, error) {
		return exec.CommitRoundF1(lctx, in)
	})
	if err != nil {
		return DataBranchFencingRecord{}, classifyFencingStepError(DataBranchFencingStepRoundF1, err)
	}
	f1, ok := f1Any.(DataBranchFencingF1Commit)
	if !ok {
		return DataBranchFencingRecord{}, &DataBranchFencingError{
			Class: DataBranchFencingRoundGateRejected,
			Step:  DataBranchFencingStepRoundF1,
			Cause: moerr.NewInternalErrorf(ctx, "round f1 step returned %T", f1Any),
		}
	}
	if err = CheckDataBranchFencingStageShape(DataBranchFencingStepRoundF1, f1.Shape); err != nil {
		return DataBranchFencingRecord{}, &DataBranchFencingError{
			Class: DataBranchFencingRoundGateRejected, Step: DataBranchFencingStepRoundF1, Cause: err,
		}
	}

	f2Any, err := runner.RunStep(ctx, DataBranchFencingStepRoundF2, func(lctx context.Context) (any, error) {
		return exec.CommitRoundF2(lctx, in)
	})
	if err != nil {
		return DataBranchFencingRecord{}, classifyFencingStepError(DataBranchFencingStepRoundF2, err)
	}
	f2, ok := f2Any.(DataBranchFencingF2Commit)
	if !ok {
		return DataBranchFencingRecord{}, &DataBranchFencingError{
			Class: DataBranchFencingRoundGateRejected,
			Step:  DataBranchFencingStepRoundF2,
			Cause: moerr.NewInternalErrorf(ctx, "round f2 step returned %T", f2Any),
		}
	}
	if !f2.ProbeFound {
		// Ownership precondition failed: the round is interrupted and by
		// contract NO commit was issued (CommitTS stays empty).
		return DataBranchFencingRecord{}, &DataBranchFencingError{
			Class: DataBranchFencingRoundInterrupted,
			Step:  DataBranchFencingStepRoundF2,
			Cause: moerr.NewInternalErrorf(ctx, "scratch key %d absent at ownership probe", in.ScratchTableID),
		}
	}
	if err = CheckDataBranchFencingStageShape(DataBranchFencingStepRoundF2, f2.Shape); err != nil {
		return DataBranchFencingRecord{}, &DataBranchFencingError{
			Class: DataBranchFencingRoundGateRejected, Step: DataBranchFencingStepRoundF2, Cause: err,
		}
	}

	rec, err := AssembleDataBranchFencingRecord(grant, f1, f2, in)
	if err != nil {
		return DataBranchFencingRecord{}, &DataBranchFencingError{
			Class: DataBranchFencingRoundGateRejected, Step: DataBranchFencingStepRoundF2, Cause: err,
		}
	}
	return rec, nil
}

// executeDataBranchFencingRoundOwned is the owned round geometry of
// D186(B.2)/D189(B.1): ONE charged stage owner covers the scratch grant
// and T_f1, ONE covers T_f2, and every step runs under its owner with a
// FRESH lane publication (D186(B.1)(i): no observation crosses a lane or
// stage boundary). Every bail route abandons the owner asynchronously
// (the lane already exited; the operator, when bound, gets exactly one
// scheduled rollback) and marks it lane-complete so the retained
// lifecycle of D188(B.1) cannot leak the charge; close observation
// finishes the owner exactly once.
func executeDataBranchFencingRoundOwned(
	ctx context.Context,
	runner dataBranchFencingOwnedRunner,
	locks DataBranchScratchLocks,
	exec DataBranchFencingStageExecutor,
	in DataBranchFencingRoundInput,
) (DataBranchFencingRecord, error) {
	bail := func(owner *DataBranchStageOwner, step DataBranchFencingStepKind, err error) error {
		owner.AbandonAsync()
		owner.NoteNoMoreLanes()
		return classifyFencingStepError(step, err)
	}

	o1, merr := runner.mintStageOwner(in.ScratchTableID, "scratch-grant+round-f1")
	if merr != nil {
		return DataBranchFencingRecord{}, merr
	}

	grantAny, err := runner.runStepOwned(ctx, DataBranchFencingStepGrant, func(lctx context.Context) (any, error) {
		return locks.AcquireScratchGrant(lctx, in)
	}, o1, NewDataBranchFencingLanePublication())
	if err != nil {
		return DataBranchFencingRecord{}, bail(o1, DataBranchFencingStepGrant, err)
	}
	grant, ok := grantAny.(DataBranchScratchGrant)
	if !ok {
		return DataBranchFencingRecord{}, bail(o1, DataBranchFencingStepGrant, &DataBranchFencingError{
			Class: DataBranchFencingRoundGateRejected,
			Step:  DataBranchFencingStepGrant,
			Cause: moerr.NewInternalErrorf(ctx, "scratch grant step returned %T", grantAny),
		})
	}

	f1Any, err := runner.runStepOwned(ctx, DataBranchFencingStepRoundF1, func(lctx context.Context) (any, error) {
		return exec.CommitRoundF1(lctx, in)
	}, o1, NewDataBranchFencingLanePublication())
	if err != nil {
		return DataBranchFencingRecord{}, bail(o1, DataBranchFencingStepRoundF1, err)
	}
	f1, ok := f1Any.(DataBranchFencingF1Commit)
	if !ok {
		return DataBranchFencingRecord{}, bail(o1, DataBranchFencingStepRoundF1, &DataBranchFencingError{
			Class: DataBranchFencingRoundGateRejected,
			Step:  DataBranchFencingStepRoundF1,
			Cause: moerr.NewInternalErrorf(ctx, "round f1 step returned %T", f1Any),
		})
	}
	if err = CheckDataBranchFencingStageShape(DataBranchFencingStepRoundF1, f1.Shape); err != nil {
		return DataBranchFencingRecord{}, bail(o1, DataBranchFencingStepRoundF1, &DataBranchFencingError{
			Class: DataBranchFencingRoundGateRejected, Step: DataBranchFencingStepRoundF1, Cause: err,
		})
	}
	// T_f1 is durable: no further lanes ride this owner. Its commit
	// already schedules the platform ClosedEvent, so close observation
	// completes the owner without further round input.
	o1.NoteNoMoreLanes()

	o2, merr := runner.mintStageOwner(in.ScratchTableID, "round-f2")
	if merr != nil {
		// o1's residue stands: its F1 commit is durable and close
		// observation releases the charge independently of this error.
		return DataBranchFencingRecord{}, merr
	}

	f2Any, err := runner.runStepOwned(ctx, DataBranchFencingStepRoundF2, func(lctx context.Context) (any, error) {
		return exec.CommitRoundF2(lctx, in)
	}, o2, NewDataBranchFencingLanePublication())
	if err != nil {
		return DataBranchFencingRecord{}, bail(o2, DataBranchFencingStepRoundF2, err)
	}
	f2, ok := f2Any.(DataBranchFencingF2Commit)
	if !ok {
		return DataBranchFencingRecord{}, bail(o2, DataBranchFencingStepRoundF2, &DataBranchFencingError{
			Class: DataBranchFencingRoundGateRejected,
			Step:  DataBranchFencingStepRoundF2,
			Cause: moerr.NewInternalErrorf(ctx, "round f2 step returned %T", f2Any),
		})
	}
	o2.NoteNoMoreLanes()
	if !f2.ProbeFound {
		// Ownership precondition failed: the lane itself abandoned the
		// owner before returning ProbeFound=false, and by contract NO
		// commit was issued (CommitTS stays empty).
		return DataBranchFencingRecord{}, &DataBranchFencingError{
			Class: DataBranchFencingRoundInterrupted,
			Step:  DataBranchFencingStepRoundF2,
			Cause: moerr.NewInternalErrorf(ctx, "scratch key %d absent at ownership probe", in.ScratchTableID),
		}
	}
	if err = CheckDataBranchFencingStageShape(DataBranchFencingStepRoundF2, f2.Shape); err != nil {
		return DataBranchFencingRecord{}, &DataBranchFencingError{
			Class: DataBranchFencingRoundGateRejected, Step: DataBranchFencingStepRoundF2, Cause: err,
		}
	}

	rec, err := AssembleDataBranchFencingRecord(grant, f1, f2, in)
	if err != nil {
		return DataBranchFencingRecord{}, &DataBranchFencingError{
			Class: DataBranchFencingRoundGateRejected, Step: DataBranchFencingStepRoundF2, Cause: err,
		}
	}
	return rec, nil
}

// classifyFencingStepError consumes ErrTxnUnknown into the
// commit-unknown class; driver-typed errors pass through unchanged.
func classifyFencingStepError(step DataBranchFencingStepKind, err error) error {
	if fe, ok := AsDataBranchFencingError(err); ok {
		return fe
	}
	class := DataBranchFencingRoundStageFailed
	if step == DataBranchFencingStepGrant {
		class = DataBranchFencingRoundGrantFailed
	}
	if moerr.IsMoErrCode(err, moerr.ErrTxnUnknown) {
		class = DataBranchFencingRoundCommitUnknown
	}
	return &DataBranchFencingError{Class: class, Step: step, Cause: err}
}

// CheckDataBranchFencingStageShape is the D182(B.1) micro-shape gate: it
// discriminates CONTENT, not emptiness — a stage whose writes leave the
// kind=1 ledger stage shape is rejected before its timestamps can ride a
// record, and T_f1/T_f2 through the no-write fast path are likewise
// rejected because their commit timestamps would be empty.
func CheckDataBranchFencingStageShape(step DataBranchFencingStepKind, s DataBranchFencingStageShape) error {
	if !s.TargetLedgerTable || s.ExtraWrites != 0 {
		return moerr.NewInternalErrorNoCtxf("fencing stage %s carries writes outside the ledger kind=1 namespace", step)
	}
	switch step {
	case DataBranchFencingStepRoundF1:
		if s.InsertBatches != 1 || s.PointTombstones != 0 {
			return moerr.NewInternalErrorNoCtxf("fencing stage %s must carry exactly one scratch insert and its bounded sweep tombstones, got %d inserts, %d point tombstones",
				step, s.InsertBatches, s.PointTombstones)
		}
		if s.NoWriteFastPath {
			return moerr.NewInternalErrorNoCtxf("fencing stage %s committed through the no-write fast path", step)
		}
	case DataBranchFencingStepRoundF2:
		if s.InsertBatches != 0 || s.SweepTombstones != 0 || s.PointTombstones != 1 {
			return moerr.NewInternalErrorNoCtxf("fencing stage %s must carry exactly one scratch point tombstone, got %d inserts, %d sweep tombstones, %d point tombstones",
				step, s.InsertBatches, s.SweepTombstones, s.PointTombstones)
		}
		if s.NoWriteFastPath {
			return moerr.NewInternalErrorNoCtxf("fencing stage %s committed through the no-write fast path", step)
		}
	default:
		return moerr.NewInternalErrorNoCtxf("unknown fencing stage %s", step)
	}
	return nil
}

// DataBranchFencingRecord is the proven fencing record handed to the
// retiring session: (C_f, C_f1, B_f) plus the round identity and the
// sweep's accounting.
type DataBranchFencingRecord struct {
	ScratchTableID uint64
	IssueTimeNS    uint64
	// CommitTS1 = C_f1, CommitTS = C_f, with the assembly-proven
	// invariant C_f > C_f1 > C (C = LastWriterCommitTS) whenever C is
	// non-empty.
	CommitTS1 timestamp.Timestamp
	CommitTS  timestamp.Timestamp
	Binding   DataBranchFencingBinding
	// Certificate merges the per-stage certification values (CHECK A and
	// CHECK B observations OR'd across T_f1/T_f2; the decisive stage's
	// staged shape and floors preferred). It stays transient per D(D).
	Certificate DataBranchFencingCertificate
	Swept       []uint64
	Skipped     []uint64
}

// AssembleDataBranchFencingRecord builds the round record with the
// assembly-time rejections of D177(B.2)(ii): empty/zero binding or commit
// timestamps are REFUSED before handover, and the pair must order
// strictly.
func AssembleDataBranchFencingRecord(
	grant DataBranchScratchGrant,
	f1 DataBranchFencingF1Commit,
	f2 DataBranchFencingF2Commit,
	in DataBranchFencingRoundInput,
) (DataBranchFencingRecord, error) {
	if grant.Binding.IsZero() {
		return DataBranchFencingRecord{}, moerr.NewInternalErrorNoCtx("fencing record rejected: empty recorded binding B_f")
	}
	if f1.CommitTS.IsEmpty() {
		return DataBranchFencingRecord{}, moerr.NewInternalErrorNoCtx("fencing record rejected: empty fencing commit timestamp C_f1")
	}
	if f2.CommitTS.IsEmpty() {
		return DataBranchFencingRecord{}, moerr.NewInternalErrorNoCtx("fencing record rejected: empty fencing commit timestamp C_f")
	}
	if !f1.CommitTS.Less(f2.CommitTS) {
		return DataBranchFencingRecord{}, moerr.NewInternalErrorNoCtx(
			fmt.Sprintf("fencing record rejected: non-ordered pair C_f1 %s, C_f %s",
				f1.CommitTS.DebugString(), f2.CommitTS.DebugString()))
	}
	if !in.LastWriterCommitTS.IsEmpty() && !in.LastWriterCommitTS.Less(f1.CommitTS) {
		return DataBranchFencingRecord{}, moerr.NewInternalErrorNoCtx(
			fmt.Sprintf("fencing record rejected: C_f1 %s does not out-order last writer commit %s",
				f1.CommitTS.DebugString(), in.LastWriterCommitTS.DebugString()))
	}
	return DataBranchFencingRecord{
		ScratchTableID: in.ScratchTableID,
		IssueTimeNS:    in.IssueTimeNS,
		CommitTS1:      f1.CommitTS,
		CommitTS:       f2.CommitTS,
		Binding:        grant.Binding,
		Certificate:    mergeDataBranchFencingCertificates(f1.Cert, f2.Cert),
		Swept:          f1.Swept,
		Skipped:        f1.Skipped,
	}, nil
}

// mergeDataBranchFencingCertificates merges the two stage certificates into
// the record's certificate: both check observations OR'd across stages, the
// decisive (T_f2) stage's staged shape, counts and floors preferred when
// they are filled. A pair of zero certificates merges to the zero value,
// which legacy fabric producers rely on.
func mergeDataBranchFencingCertificates(f1, f2 DataBranchFencingCertificate) DataBranchFencingCertificate {
	merged := DataBranchFencingCertificate{
		CheckAEnvelopeDataObserved: f1.CheckAEnvelopeDataObserved || f2.CheckAEnvelopeDataObserved,
		CheckBEnvelopeDataObserved: f1.CheckBEnvelopeDataObserved || f2.CheckBEnvelopeDataObserved,
	}
	for _, src := range [...]DataBranchFencingCertificate{f2, f1} {
		if merged.Floors == (DataBranchFencingWorkspaceFloors{}) {
			merged.Floors = src.Floors
		}
		if merged.Staged == (DataBranchFencingStageShape{}) {
			merged.Staged = src.Staged
		}
		if merged.StagedRows == 0 {
			merged.StagedRows = src.StagedRows
		}
		if merged.StagedBytes == 0 {
			merged.StagedBytes = src.StagedBytes
		}
	}
	return merged
}

// AcceptDataBranchFencingRecord is the retirement-side acceptance gate
// (D173/D174, verbatim): accept only when the ledger grant's recorded
// binding B_r equals B_f on (Table, ServiceID, Version) AND the grant
// watermark fence >= C_f. Rejection is fully side-effect-free; it happens
// BEFORE any WaitLogTailAppliedAt / UpdateSnapshot / re-probe / delete
// staging.
func AcceptDataBranchFencingRecord(
	rec DataBranchFencingRecord,
	grantBinding DataBranchFencingBinding,
	fence timestamp.Timestamp,
) bool {
	return !rec.Binding.IsZero() &&
		!rec.CommitTS.IsEmpty() &&
		grantBinding.Equal(rec.Binding) &&
		!fence.Less(rec.CommitTS)
}

// DataBranchRetireRejectionClass partitions an acceptance rejection by
// whether the RETIRING transaction had already recorded the ledger
// binding (D176(B.2)).
type DataBranchRetireRejectionClass int

const (
	// DataBranchRetireRejectionFoldLag — class (ii): same-generation
	// (B_r == B_f) asynchronous fold lag; bounded in-place re-fence.
	DataBranchRetireRejectionFoldLag DataBranchRetireRejectionClass = iota
	// DataBranchRetireRejectionPreGrantFlip — class (iii): the retiring
	// transaction was UNBOUND; its first grant appended the fresh
	// generation; one in-place round re-fences on the current generation.
	DataBranchRetireRejectionPreGrantFlip
	// DataBranchRetireRejectionBindingChanged — class (i): the retiring
	// transaction's RECORDED binding changed; terminal on first
	// occurrence (the platform's AddLockTable Changed branch already
	// raised ErrLockTableBindChanged inside the lock call).
	DataBranchRetireRejectionBindingChanged
)

func (c DataBranchRetireRejectionClass) String() string {
	switch c {
	case DataBranchRetireRejectionFoldLag:
		return "same-generation-fold-lag"
	case DataBranchRetireRejectionPreGrantFlip:
		return "unbound-pre-grant-flip"
	case DataBranchRetireRejectionBindingChanged:
		return "recorded-binding-changed"
	}
	return "unknown"
}

// Terminal reports whether the class allows any in-place re-fence round:
// only classes (ii)/(iii) ever do; class (i) is whole-transaction fatal.
func (c DataBranchRetireRejectionClass) Terminal() bool {
	return c == DataBranchRetireRejectionBindingChanged
}

// DataBranchRetireGrantView is the retirement grant's observation at the
// point an acceptance gate rejected it.
type DataBranchRetireGrantView struct {
	// CallerBindingRecorded is true when the retiring transaction had
	// already recorded the ledger lock-table binding (any shared probe or
	// insert lock on the ledger table records it).
	CallerBindingRecorded bool
	// GrantErr is the lock acquisition error, if any; the platform's
	// AddLockTable Changed branch surfaces ErrLockTableBindChanged here.
	GrantErr error
	// GrantBinding is B_r, the binding the ledger grant rode.
	GrantBinding DataBranchFencingBinding
	// Fence is the grant watermark (result.Timestamp).
	Fence timestamp.Timestamp
}

// ClassifyDataBranchRetireRejection maps one rejected acceptance gate to
// its partition class. Precedence: a caller-recorded binding change is
// terminal (platform-fatal), then the recorded-binding comparison splits
// same-generation fold lag from the unbound pre-grant flip.
func ClassifyDataBranchRetireRejection(
	view DataBranchRetireGrantView, rec DataBranchFencingRecord,
) DataBranchRetireRejectionClass {
	if view.CallerBindingRecorded {
		if moerr.IsMoErrCode(view.GrantErr, moerr.ErrLockTableBindChanged) ||
			!view.GrantBinding.Equal(rec.Binding) {
			return DataBranchRetireRejectionBindingChanged
		}
		return DataBranchRetireRejectionFoldLag
	}
	if view.GrantBinding.Equal(rec.Binding) {
		return DataBranchRetireRejectionFoldLag
	}
	return DataBranchRetireRejectionPreGrantFlip
}

// DataBranchRefenceCounter bounds in-place re-fence rounds to K (K+1
// consecutive reachable rejections raise the existing ErrLockTableBindChanged
// onto the whole-transaction rollback boundary). The counter consumes ONLY
// acceptance rejections — a round failing before any grant rejection must
// never advance it — and class (i) is terminal on first occurrence.
type DataBranchRefenceCounter struct {
	max     int
	current int
}

// NewDataBranchRefenceCounter builds the per-retirement rejection counter
// with the caller's bound K.
func NewDataBranchRefenceCounter(maxRounds int) *DataBranchRefenceCounter {
	if maxRounds < 1 {
		maxRounds = 1
	}
	return &DataBranchRefenceCounter{max: maxRounds}
}

// Rounds reports the consumed rejection rounds.
func (c *DataBranchRefenceCounter) Rounds() int { return c.current }

// Consume records ONE acceptance rejection: terminal classes and
// exhaustion return the existing ErrLockTableBindChanged; within budget,
// classes (ii)/(iii) return nil so the caller re-fences in place.
func (c *DataBranchRefenceCounter) Consume(ctx context.Context, class DataBranchRetireRejectionClass) error {
	if class.Terminal() {
		return moerr.NewLockTableBindChanged(ctx)
	}
	c.current++
	if c.current > c.max {
		return moerr.NewLockTableBindChanged(ctx)
	}
	return nil
}

// DataBranchFencingScratchDraw returns the round's fresh scratch table_id
// (kind=1 namespace): a fresh per-round UUID draw, reduced to its low 64
// bits. Zero is rejected because PK value 0 is the ledger's empty key.
func DataBranchFencingScratchDraw() (uint64, error) {
	u := uuid.New()
	id := binary.BigEndian.Uint64(u[8:16])
	if id == 0 {
		return 0, moerr.NewInternalErrorNoCtx("fencing scratch draw produced the empty key")
	}
	return id, nil
}

// DataBranchFencingSweepCandidatesSQL selects the kind=1 sweep candidates:
// rows whose created_ts is older than the caller's age-hint cutoff. Each
// returned id is then locked with the bounded witnessed lock-wait:
// granted => deleted inside the same sweep transaction; timeout => SKIP.
func DataBranchFencingSweepCandidatesSQL(ageCutoffNS uint64) string {
	return fmt.Sprintf(
		"select %s, %s from mo_catalog.%s where %s = %d and %s < %d order by %s",
		catalog.DataBranchLinkRetentionColTableID,
		catalog.DataBranchLinkRetentionColCreatedTS,
		catalog.MO_DATA_BRANCH_LINK_RETENTION,
		catalog.DataBranchLinkRetentionColRowKind, catalog.DataBranchLinkRetentionRowKindFencingScratch,
		catalog.DataBranchLinkRetentionColCreatedTS, ageCutoffNS,
		catalog.DataBranchLinkRetentionColTableID,
	)
}

// DataBranchFencingSweepDeleteSQL removes one candidate under its
// witnessed lock grant, inside the sweep transaction. The row_kind prefix
// pins the kind=1 namespace so no kind=0 row can ever match.
func DataBranchFencingSweepDeleteSQL(scratchTableID uint64) string {
	return fmt.Sprintf(
		"delete from mo_catalog.%s where %s = %d and %s = %d",
		catalog.MO_DATA_BRANCH_LINK_RETENTION,
		catalog.DataBranchLinkRetentionColRowKind, catalog.DataBranchLinkRetentionRowKindFencingScratch,
		catalog.DataBranchLinkRetentionColTableID, scratchTableID,
	)
}

// DataBranchFencingScratchInsertSQL inserts the round's one scratch row:
// every NOT NULL column is byte-fixed synthetic content and created_ts
// carries the round's issue time as its creation marker.
func DataBranchFencingScratchInsertSQL(scratchTableID, issueTimeNS uint64) string {
	return fmt.Sprintf(
		"insert into mo_catalog.%s(%s, %s, %s, %s, %s, %s, %s) values (%d, %d, '', '', 0, %d, 0)",
		catalog.MO_DATA_BRANCH_LINK_RETENTION,
		catalog.DataBranchLinkRetentionColRowKind,
		catalog.DataBranchLinkRetentionColTableID,
		catalog.DataBranchLinkRetentionColDatabaseName,
		catalog.DataBranchLinkRetentionColTableName,
		catalog.DataBranchLinkRetentionColObjID,
		catalog.DataBranchLinkRetentionColCreatedTS,
		catalog.DataBranchLinkRetentionColLogicalID,
		catalog.DataBranchLinkRetentionRowKindFencingScratch, scratchTableID, issueTimeNS,
	)
}

// DataBranchFencingScratchProbeSQL is T_f2's ownership-precondition point
// probe of the round's own scratch key.
func DataBranchFencingScratchProbeSQL(scratchTableID uint64) string {
	return fmt.Sprintf(
		"select count(%s) from mo_catalog.%s where %s = %d and %s = %d",
		catalog.DataBranchLinkRetentionColTableID,
		catalog.MO_DATA_BRANCH_LINK_RETENTION,
		catalog.DataBranchLinkRetentionColRowKind, catalog.DataBranchLinkRetentionRowKindFencingScratch,
		catalog.DataBranchLinkRetentionColTableID, scratchTableID,
	)
}

// DataBranchFencingScratchDeleteSQL is T_f2's point delete of the round's
// own scratch row (runs only after the probe found it present).
func DataBranchFencingScratchDeleteSQL(scratchTableID uint64) string {
	return DataBranchFencingSweepDeleteSQL(scratchTableID)
}

// DataBranchFencingSweepAgeCutoff computes the sweep's age-hint cutoff:
// candidates strictly older than issue time minus the hint. The hint must
// cover T_round plus the platform clock-offset tolerance; it is never
// load-bearing.
func DataBranchFencingSweepAgeCutoff(issueTimeNS uint64, hint time.Duration) uint64 {
	if hint < 0 {
		hint = 0
	}
	if issueTimeNS > uint64(hint.Nanoseconds()) {
		return issueTimeNS - uint64(hint.Nanoseconds())
	}
	return 0
}
