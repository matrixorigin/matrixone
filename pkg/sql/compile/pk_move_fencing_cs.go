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

package compile

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/frontend/databranchutils"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	pbplan "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	qclient "github.com/matrixorigin/matrixone/pkg/queryservice/client"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/lockop"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/udf"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// This file is the production substrate and teardown-free adapter of the
// pk-move retention-ledger fencing retirement critical section (design
// D168(B), D170-D177 lifecycle, D179-D182 lane mechanics, D183-D185
// certification). The protocol core and its contracts live in
// pkg/frontend/databranchutils (pk_move_fencing*.go); this file supplies
// the production closures databranchutils.DataBranchRetireCSDeps requires:
//
//   - the round's witness locks and stage transactions, executed as short
//     pessimistic RC transactions through the CN's internal SQL executor;
//   - the retiring transaction's EXCLUSIVE ledger-row grant (group 0)
//     plus its recorded-binding observation;
//   - the D173(B) frontier advance (WaitLogTailAppliedAt then
//     UpdateSnapshot, in that order);
//   - certification CHECK A (lane admission) and CHECK B (immediately
//     before each stage commit is invoked) with the driver-exact staged
//     shape and the pinned workspace floors.
//
// A certification failure routes the lane IN-BODY to the rollback shape
// with NO commit RPC sent, fail-loud, per D185(B.1)(v). Contexts without a
// CN txn substrate (mock procs, non-CN engines, pre-upgrade legacy ledger
// shapes, non-pessimistic-RC retiring statements) DEGRADE: the critical
// section is skipped and the caller's paired deletes run as before.

// DataBranchFencingWorkspaceFloorsPinned returns the compiled dump-workspace
// floor set the certificate asserts staged shapes against (D184(B.1)(ii),
// carried by D185). The values mirror the disttae package constants
// CommitWorkspaceThreshold 1MB, WriteWorkspaceThreshold 5MB,
// ExtraWorkspaceThreshold 500MB and InsertEntryThreshold 5000
// (pkg/vm/engine/disttae/types.go:154-157); the same-package tripwire UT in
// disttae reads those constants and asserts exact equality with this pin.
func DataBranchFencingWorkspaceFloorsPinned() databranchutils.DataBranchFencingWorkspaceFloors {
	return databranchutils.DataBranchFencingWorkspaceFloors{
		Commit:      1 << 20,
		Write:       5 << 20,
		Extra:       500 << 20,
		InsertEntry: 5000,
	}
}

// DataBranchFencingRetireSubstrate carries the CN services one retirement
// critical section composes. Construction refuses a nil engine, txn
// client, or lock service: without them no fencing grant is meaningful.
type DataBranchFencingRetireSubstrate struct {
	eng       engine.Engine
	mp        *mpool.MPool
	txnClient client.TxnClient
	fs        fileservice.FileService
	ls        lockservice.LockService
	qc        qclient.QueryClient
	hakeeper  logservice.CNHAKeeperClient
	us        udf.Service
	ts        taskservice.TaskService
	exec      executor.SQLExecutor
	// genKey is the fencing pool's GENERATION key (D189(B.2)): the CN
	// service UUID this substrate's rounds and drain closures scope to
	// (lockservice ServiceID == proc.GetService()). "" selects the legacy
	// global pool (unit-test contexts with no service identity).
	genKey string
	// alloc, when non-nil, owns the substrate's fresh allocator through
	// every outstanding lane-scoped fencing closure (D186(B.4)); Enter
	// and Exit are nil-receiver safe, so process-owned pools (the
	// compile path) need no allocator.
	alloc *databranchutils.DataBranchStageAllocator
}

// WithGenerationKey sets the fencing pool generation key (chainable).
func (s *DataBranchFencingRetireSubstrate) WithGenerationKey(
	key string,
) *DataBranchFencingRetireSubstrate {
	s.genKey = key
	return s
}

// WithStageAllocator installs the substrate's allocator-ownership record
// (chainable, D186(B.4)); the substrate does not construct it.
func (s *DataBranchFencingRetireSubstrate) WithStageAllocator(
	a *databranchutils.DataBranchStageAllocator,
) *DataBranchFencingRetireSubstrate {
	s.alloc = a
	return s
}

// NewDataBranchFencingRetireSubstrate assembles the substrate from a CN
// process's own parts. A nil engine, txnClient, or lock service returns
// nil (the degrade signal); a nil executor degrades only the stage SQL,
// which construction here also treats as unavailable because the fencing
// stages cannot issue their writes.
func NewDataBranchFencingRetireSubstrate(
	eng engine.Engine,
	mp *mpool.MPool,
	txnClient client.TxnClient,
	fs fileservice.FileService,
	ls lockservice.LockService,
	qc qclient.QueryClient,
	hakeeper logservice.CNHAKeeperClient,
	us udf.Service,
	ts taskservice.TaskService,
	exec executor.SQLExecutor,
) *DataBranchFencingRetireSubstrate {
	if eng == nil || mp == nil || txnClient == nil || ls == nil || exec == nil {
		return nil
	}
	return &DataBranchFencingRetireSubstrate{
		eng: eng, mp: mp, txnClient: txnClient, fs: fs, ls: ls,
		qc: qc, hakeeper: hakeeper, us: us, ts: ts, exec: exec,
	}
}

// DataBranchFencingRetireSubstrateFromRuntime resolves the substrate from a
// running process's own base process (its txn client, services, engine) and
// the runtime-registered internal SQL executor. Any missing part degrades
// to nil per the constructor.
func DataBranchFencingRetireSubstrateFromRuntime(
	proc *process.Process, eng engine.Engine,
) *DataBranchFencingRetireSubstrate {
	if proc == nil || proc.Base == nil {
		return nil
	}
	base := proc.Base
	var exec executor.SQLExecutor
	if rt := moruntime.ServiceRuntime(proc.GetService()); rt != nil {
		if val, ok := rt.GetGlobalVariables(moruntime.InternalSQLExecutor); ok {
			exec, _ = val.(executor.SQLExecutor)
		}
	}
	sub := NewDataBranchFencingRetireSubstrate(
		eng, proc.Mp(), base.TxnClient, base.FileService, base.LockService,
		base.QueryClient, base.Hakeeper, base.UdfService, base.TaskService, exec,
	)
	if sub == nil {
		return nil
	}
	return sub.WithGenerationKey(proc.GetService())
}

// resolveRetentionLedger resolves the shared retention ledger relation and
// its live TableDef under the retiring address's own account. The ledger
// catalog is a PER-ACCOUNT physical relation: every account holds its own
// mo_data_branch_link_retention copy, and stage DML resolves the name under
// l.accountID. The fencing locks, grants and watermark probes must resolve
// against that SAME physical table, or the stage commits fold one physical
// lock table while the grant samples another. ok=false reports the degrade
// classes: a pre-upgrade tenant with no ledger catalog, or the legacy
// dev-window single-column primary-key shape which has no fencing scratch
// namespace (disjoint row kinds do not exist there).
func (s *DataBranchFencingRetireSubstrate) resolveRetentionLedger(
	ctx context.Context, txnOp client.TxnOperator, accountID uint32,
) (ledgerID uint64, def *pbplan.TableDef, ok bool, err error) {
	accCtx := defines.AttachAccountId(ctx, accountID)
	db, err := s.eng.Database(accCtx, catalog.MO_CATALOG, txnOp)
	if err != nil {
		if moerr.IsMoErrCode(err, moerr.ErrNoSuchTable) {
			return 0, nil, false, nil
		}
		return 0, nil, false, err
	}
	rel, err := db.Relation(accCtx, catalog.MO_DATA_BRANCH_LINK_RETENTION, nil)
	if err != nil {
		if moerr.IsMoErrCode(err, moerr.ErrNoSuchTable) {
			return 0, nil, false, nil
		}
		return 0, nil, false, err
	}
	def = rel.GetTableDef(accCtx)
	if def == nil || def.Pkey == nil || def.Pkey.PkeyColName != catalog.CPrimaryKeyColName {
		// Legacy dev-window shape: no disjoint scratch namespace; the
		// fencing critical section cannot run loudly here, so the caller
		// runs its ordinary paired deletes as before.
		return 0, nil, false, nil
	}
	return rel.GetTableID(accCtx), def, true, nil
}

// newBoundProc builds a short-lived top process bound to txnOp for the
// lock pipeline; callers Free it after the grant call.
func (s *DataBranchFencingRetireSubstrate) newBoundProc(
	ctx context.Context, txnOp client.TxnOperator,
) (*process.Process, func()) {
	proc := process.NewTopProcess(
		ctx, s.mp, s.txnClient, txnOp,
		s.fs, s.ls, s.qc, s.hakeeper, s.us, nil, s.ts,
	)
	return proc, func() { proc.Free() }
}

// dataBranchFencingProductionLanes is the one production round adapter: it
// implements both databranchutils.DataBranchScratchLocks and
// databranchutils.DataBranchFencingStageExecutor over the CN substrate. The
// grant and the T_f1 stage share T_f1's transaction; T_f2 is a fresh
// transaction per round. Under the owned round geometry (D186(B.2)) no
// operator rides this struct: each stage's transaction is bound to the
// round's stage owner carried by the lane context (owner.BindOperator at
// the grant/F2 open, owner.BoundOperator at F1), so retention, terminal
// classification, and exactly-once close are the owner's protocol (D187,
// D188), never an adapter slot.
type dataBranchFencingProductionLanes struct {
	sub       *DataBranchFencingRetireSubstrate
	accountID uint32
	ledgerID  uint64
	ledgerDef *pbplan.TableDef
	floors    databranchutils.DataBranchFencingWorkspaceFloors
}

// newStageTxn opens one short pessimistic RC transaction on the substrate
// and registers it with the engine, mirroring the internal executor's own
// transaction construction.
func (s *DataBranchFencingRetireSubstrate) newStageTxn(
	ctx context.Context, accountID uint32,
) (client.TxnOperator, error) {
	op, err := s.txnClient.New(ctx, timestamp.Timestamp{},
		client.WithTxnIsolation(txn.TxnIsolation_RC),
		client.WithTxnMode(txn.TxnMode_Pessimistic),
		client.WithTxnCreateBy(accountID, "", "databranch-fencing", 0),
	)
	if err != nil {
		return nil, err
	}
	// D206(B.1) SETTER SITE (3): every 'databranch-fencing' stage
	// transaction stages the protocol's scratch/witness artifacts, so its
	// expected internal conflicts are protocol-expected; one idempotent
	// mark at creation covers all three call sites.
	client.MarkTxnExpectedConflictIfSupported(op)
	if err = s.eng.New(ctx, op); err != nil {
		_ = op.Rollback(ctx)
		return nil, err
	}
	return op, nil
}

// execInStage executes one statement inside an ALREADY OPEN stage
// transaction without committing or rolling it back (the Exec fast path
// for an existing keep-alive transaction).
func (s *DataBranchFencingRetireSubstrate) execInStage(
	ctx context.Context,
	op client.TxnOperator,
	sql string,
	stmtOpt executor.StatementOption,
) (executor.Result, error) {
	return s.exec.Exec(ctx, sql,
		executor.Options{}.WithTxn(op).WithKeepTxnAlive().WithStatementOption(stmtOpt))
}

// AcquireScratchGrant takes the exclusive group-0 admission lock on the
// round's fresh scratch identity row inside T_f1 and reports the recorded
// binding generation B_f and the grant watermark. T_f1's transaction is
// bound to the round's stage owner (D186(B.2)(ii)); every failure below
// the bind abandons the owner, which schedules the operator's rollback
// exactly once (D187(B.1)).
func (l *dataBranchFencingProductionLanes) AcquireScratchGrant(
	ctx context.Context, in databranchutils.DataBranchFencingRoundInput,
) (databranchutils.DataBranchScratchGrant, error) {
	l.sub.alloc.Enter()
	defer l.sub.alloc.Exit()
	fail := func(err error) (databranchutils.DataBranchScratchGrant, error) {
		return databranchutils.DataBranchScratchGrant{}, err
	}
	owner := databranchutils.DataBranchRoundOwnerFromCtx(ctx)
	if owner == nil {
		return fail(moerr.NewInternalErrorNoCtx(
			"data branch fencing scratch grant: round owner missing"))
	}
	op, err := l.sub.newStageTxn(ctx, l.accountID)
	if err != nil {
		return fail(err)
	}
	if err = owner.BindOperator(op, in.ScratchTableID); err != nil {
		_ = op.Rollback(ctx)
		return fail(err)
	}
	discard := func(derr error) (databranchutils.DataBranchScratchGrant, error) {
		owner.Abandon(ctx)
		return fail(derr)
	}
	keys, keyType, err := databranchutils.PkMoveRetentionScratchLockKey(
		l.sub.mp, l.ledgerDef, in.ScratchTableID)
	if err != nil {
		return discard(err)
	}
	defer keys.Clean(l.sub.mp)
	// The grant must pin the lane-account's own physical ledger copy: the
	// stage DML writes that copy, so its commit folds only that copy's
	// watermark.
	accCtx := defines.AttachAccountId(ctx, l.accountID)
	proc, free := l.sub.newBoundProc(accCtx, op)
	defer free()
	result, err := lockop.LockRowsForAdmissionResult(
		accCtx, l.sub.eng, proc, l.ledgerID, keys, 0, keyType,
		lock.LockMode_Exclusive, 0)
	if err != nil {
		return discard(err)
	}
	return databranchutils.DataBranchScratchGrant{
		Binding:   databranchutils.DataBranchFencingBindingFromLockTable(result.LockedOn),
		WitnessTS: result.Timestamp,
	}, nil
}

// CommitRoundF1 executes the bounded kind=1 sweep (skip-on-conflict) then
// exactly one scratch insert inside the owner-bound T_f1, certifies CHECK
// A (lane admission) and CHECK B (immediately before the commit is
// invoked), and commits. A certification failure rolls the stage back
// IN-BODY through the owner's exactly-once abandonment with NO commit RPC
// sent (D185(B.1)(v), D187(B.1)). The commit itself rides the terminal
// protocol of D186(B.1)(ii)/D187(B.2): publish the certified shape, enter
// the lane's terminal segment, take the terminal decision, then invoke
// Commit — so a strike between certification and commit classifies the
// lane terminal-in-flight, never running.
func (l *dataBranchFencingProductionLanes) CommitRoundF1(
	ctx context.Context, in databranchutils.DataBranchFencingRoundInput,
) (databranchutils.DataBranchFencingF1Commit, error) {
	l.sub.alloc.Enter()
	defer l.sub.alloc.Exit()
	owner := databranchutils.DataBranchRoundOwnerFromCtx(ctx)
	if owner == nil {
		return databranchutils.DataBranchFencingF1Commit{}, moerr.NewInternalErrorNoCtx(
			"data branch fencing round f1: round owner missing")
	}
	op := owner.BoundOperator()
	if op == nil {
		return databranchutils.DataBranchFencingF1Commit{}, moerr.NewInternalErrorNoCtx(
			"data branch fencing round f1: scratch grant missing")
	}
	pub := databranchutils.DataBranchLanePublicationFromCtx(ctx)
	fail := func(err error) (databranchutils.DataBranchFencingF1Commit, error) {
		owner.Abandon(ctx)
		return databranchutils.DataBranchFencingF1Commit{}, err
	}
	// CHECK A (lane admission): envelope only, no stage content yet.
	if err := databranchutils.DataBranchFencingEnvelopeCertify(
		ctx, databranchutils.DataBranchFencingCertCheckA, 0, 0,
		databranchutils.DataBranchFencingStageShape{}, l.floors); err != nil {
		return fail(err)
	}
	stmtOpt := executor.StatementOption{}.WithAccountID(l.accountID)
	// Bounded age-eligible sweep candidates (the round's own fresh row is
	// younger than the cutoff by construction). The sweep never waits
	// behind a contender past its fast-fail lock attempt: on any lock
	// conflict the whole batch is SKIPPED for a later round.
	sweepSQL := fmt.Sprintf(
		"select %s from %s.%s where %s = %d and %s < %d order by %s asc, %s asc limit 16",
		catalog.DataBranchLinkRetentionColTableID,
		catalog.MO_CATALOG, catalog.MO_DATA_BRANCH_LINK_RETENTION,
		catalog.DataBranchLinkRetentionColRowKind, catalog.DataBranchLinkRetentionRowKindFencingScratch,
		catalog.DataBranchLinkRetentionColCreatedTS, in.SweepAgeCutoffNS,
		catalog.DataBranchLinkRetentionColCreatedTS, catalog.DataBranchLinkRetentionColTableID,
	)
	res, err := l.sub.execInStage(ctx, op, sweepSQL, stmtOpt)
	if err != nil {
		return fail(err)
	}
	candidates, err := readUint64Column(res)
	res.Close()
	if err != nil {
		return fail(err)
	}
	var swept, skipped []uint64
	if len(candidates) > 0 {
		ids := make([]string, 0, len(candidates))
		for _, id := range candidates {
			ids = append(ids, fmt.Sprintf("%d", id))
		}
		deleteSQL := fmt.Sprintf(
			"delete from %s.%s where %s = %d and %s in (%s)",
			catalog.MO_CATALOG, catalog.MO_DATA_BRANCH_LINK_RETENTION,
			catalog.DataBranchLinkRetentionColRowKind, catalog.DataBranchLinkRetentionRowKindFencingScratch,
			catalog.DataBranchLinkRetentionColTableID, strings.Join(ids, ","),
		)
		if _, derr := l.sub.execInStage(ctx, op, deleteSQL,
			stmtOpt.WithWaitPolicy(lock.WaitPolicy_FastFail)); derr != nil {
			if moerr.IsMoErrCode(derr, moerr.ErrLockConflict) ||
				moerr.IsMoErrCode(derr, moerr.ErrLockTableNotFound) ||
				moerr.IsMoErrCode(derr, moerr.ErrDeadLockDetected) ||
				moerr.IsMoErrCode(derr, moerr.ErrLockWaitTimeout) {
				// Witnessed contention: skip this round's candidates.
				skipped = candidates
			} else {
				return fail(derr)
			}
		} else {
			swept = candidates
		}
	}
	// Exactly one scratch insert: the round's ownership row.
	insertSQL := fmt.Sprintf(
		"insert into %s.%s(%s, %s, %s, %s, %s, %s, %s) values(%d, %d, '', '', %d, %d, %d)",
		catalog.MO_CATALOG, catalog.MO_DATA_BRANCH_LINK_RETENTION,
		catalog.DataBranchLinkRetentionColRowKind,
		catalog.DataBranchLinkRetentionColTableID,
		catalog.DataBranchLinkRetentionColDatabaseName,
		catalog.DataBranchLinkRetentionColTableName,
		catalog.DataBranchLinkRetentionColObjID,
		catalog.DataBranchLinkRetentionColCreatedTS,
		catalog.DataBranchLinkRetentionColLogicalID,
		catalog.DataBranchLinkRetentionRowKindFencingScratch,
		in.ScratchTableID, in.ScratchTableID, in.IssueTimeNS, 0,
	)
	if _, ierr := l.sub.execInStage(ctx, op, insertSQL, stmtOpt); ierr != nil {
		return fail(ierr)
	}
	shape := databranchutils.DataBranchFencingStageShape{
		TargetLedgerTable: true,
		InsertBatches:     1,
		SweepTombstones:   len(swept),
	}
	stagedRows := int64(len(swept) + 1)
	stagedBytes := uint64(stagedRows) * 128
	// CHECK B (immediately before the commit is invoked): envelope plus
	// the driver-exact staged shape under the pinned floors.
	if err := databranchutils.DataBranchFencingEnvelopeCertify(
		ctx, databranchutils.DataBranchFencingCertCheckB, stagedRows, stagedBytes,
		shape, l.floors); err != nil {
		return fail(err)
	}
	cert := databranchutils.DataBranchFencingCertificate{
		CheckAEnvelopeDataObserved: true,
		CheckBEnvelopeDataObserved: true,
		Staged:                     shape,
		StagedRows:                 stagedRows,
		StagedBytes:                stagedBytes,
		Floors:                     l.floors,
	}
	// D186(B.1)(ii): certificate BEFORE terminal entry BEFORE the
	// decision gate BEFORE the commit call. A strike racing this segment
	// classifies the lane terminal-in-flight, and the decision gate
	// refuses the commit once the round's acceptance was revoked.
	if pub != nil {
		pub.PublishCertificate(cert)
		pub.PublishTerminalEntry()
	}
	if !owner.SelectCommitting() {
		return fail(moerr.NewInternalErrorNoCtx(
			"data branch fencing round f1: terminal decision revoked"))
	}
	if err := op.Commit(ctx); err != nil {
		// NO in-body abandonment on a commit error (D187(B.3)): the
		// commit may have landed; the platform's deferred cleanup and the
		// owner's observed-close finish own the tail.
		return databranchutils.DataBranchFencingF1Commit{}, err
	}
	return databranchutils.DataBranchFencingF1Commit{
		CommitTS: op.Txn().CommitTS,
		Swept:    swept,
		Skipped:  skipped,
		Shape:    shape,
		Cert:     cert,
	}, nil
}

// CommitRoundF2 opens T_f2 on the round's second stage owner, probes the
// round's own scratch key, and on presence CHECK B-certifies the single
// point tombstone shape, deletes the row, and commits through the same
// publish/decide/commit terminal protocol as F1 (D186(B.1)(ii),
// D187(B.2)). Absence means the round's ownership precondition failed:
// the owner is abandoned (rollback, NO commit, D176(B.1)) and
// ProbeFound=false.
func (l *dataBranchFencingProductionLanes) CommitRoundF2(
	ctx context.Context, in databranchutils.DataBranchFencingRoundInput,
) (databranchutils.DataBranchFencingF2Commit, error) {
	l.sub.alloc.Enter()
	defer l.sub.alloc.Exit()
	owner := databranchutils.DataBranchRoundOwnerFromCtx(ctx)
	if owner == nil {
		return databranchutils.DataBranchFencingF2Commit{}, moerr.NewInternalErrorNoCtx(
			"data branch fencing round f2: round owner missing")
	}
	op, err := l.sub.newStageTxn(ctx, l.accountID)
	if err != nil {
		return databranchutils.DataBranchFencingF2Commit{}, err
	}
	if err = owner.BindOperator(op, in.ScratchTableID); err != nil {
		_ = op.Rollback(ctx)
		return databranchutils.DataBranchFencingF2Commit{}, err
	}
	pub := databranchutils.DataBranchLanePublicationFromCtx(ctx)
	fail := func(err error) (databranchutils.DataBranchFencingF2Commit, error) {
		owner.Abandon(ctx)
		return databranchutils.DataBranchFencingF2Commit{}, err
	}
	if err := databranchutils.DataBranchFencingEnvelopeCertify(
		ctx, databranchutils.DataBranchFencingCertCheckA, 0, 0,
		databranchutils.DataBranchFencingStageShape{}, l.floors); err != nil {
		return fail(err)
	}
	stmtOpt := executor.StatementOption{}.WithAccountID(l.accountID)
	probeSQL := fmt.Sprintf(
		"select %s from %s.%s where %s = %d and %s = %d limit 1",
		catalog.DataBranchLinkRetentionColTableID,
		catalog.MO_CATALOG, catalog.MO_DATA_BRANCH_LINK_RETENTION,
		catalog.DataBranchLinkRetentionColRowKind, catalog.DataBranchLinkRetentionRowKindFencingScratch,
		catalog.DataBranchLinkRetentionColTableID, in.ScratchTableID,
	)
	res, err := l.sub.execInStage(ctx, op, probeSQL, stmtOpt)
	if err != nil {
		return fail(err)
	}
	found, err := resultHasAnyRow(res)
	res.Close()
	if err != nil {
		return fail(err)
	}
	if !found {
		// Ownership precondition failed: exactly-once abandonment through
		// the owner, NO commit per D176(B.1).
		owner.Abandon(ctx)
		return databranchutils.DataBranchFencingF2Commit{}, nil
	}
	deleteSQL := fmt.Sprintf(
		"delete from %s.%s where %s = %d and %s = %d",
		catalog.MO_CATALOG, catalog.MO_DATA_BRANCH_LINK_RETENTION,
		catalog.DataBranchLinkRetentionColRowKind, catalog.DataBranchLinkRetentionRowKindFencingScratch,
		catalog.DataBranchLinkRetentionColTableID, in.ScratchTableID,
	)
	if _, derr := l.sub.execInStage(ctx, op, deleteSQL, stmtOpt); derr != nil {
		return fail(derr)
	}
	shape := databranchutils.DataBranchFencingStageShape{
		TargetLedgerTable: true,
		PointTombstones:   1,
	}
	if err := databranchutils.DataBranchFencingEnvelopeCertify(
		ctx, databranchutils.DataBranchFencingCertCheckB, 1, 128,
		shape, l.floors); err != nil {
		return fail(err)
	}
	cert := databranchutils.DataBranchFencingCertificate{
		CheckAEnvelopeDataObserved: true,
		CheckBEnvelopeDataObserved: true,
		Staged:                     shape,
		StagedRows:                 1,
		StagedBytes:                128,
		Floors:                     l.floors,
	}
	if pub != nil {
		pub.PublishCertificate(cert)
		pub.PublishTerminalEntry()
	}
	if !owner.SelectCommitting() {
		return fail(moerr.NewInternalErrorNoCtx(
			"data branch fencing round f2: terminal decision revoked"))
	}
	if err := op.Commit(ctx); err != nil {
		// NO in-body abandonment on a commit error (D187(B.3)); the
		// observed-close finish owns the tail exactly as for F1.
		return databranchutils.DataBranchFencingF2Commit{}, err
	}
	return databranchutils.DataBranchFencingF2Commit{
		ProbeFound: true,
		CommitTS:   op.Txn().CommitTS,
		Shape:      shape,
		Cert:       cert,
	}, nil
}

// AcquireLedgerGrant takes the retiring transaction's EXCLUSIVE group-0
// admission lock on the address's real retention identity row and reports
// B_r, the grant watermark, the pre-grant recorded-binding fact, and any
// lock-layer error verbatim (ErrLockTableBindChanged for class (i)).
func (s *DataBranchFencingRetireSubstrate) AcquireLedgerGrant(
	ctx context.Context,
	txnOp client.TxnOperator,
	accountID uint32,
	ledgerID uint64,
	def *pbplan.TableDef,
	tableID uint64,
) databranchutils.DataBranchRetireLedgerGrant {
	recordedBefore := false
	if snap, err := txnOp.Snapshot(); err == nil {
		for _, lt := range snap.LockTables {
			if lt.Table == ledgerID {
				recordedBefore = true
				break
			}
		}
	}
	keys, keyType, err := databranchutils.PkMoveRetentionRowLockKey(s.mp, def, tableID)
	if err != nil {
		return databranchutils.DataBranchRetireLedgerGrant{Err: err}
	}
	defer keys.Clean(s.mp)
	accCtx := defines.AttachAccountId(ctx, accountID)
	proc, free := s.newBoundProc(accCtx, txnOp)
	defer free()
	result, err := lockop.LockRowsForAdmissionResult(
		accCtx, s.eng, proc, ledgerID, keys, 0, keyType,
		lock.LockMode_Exclusive, 0)
	if err != nil {
		return databranchutils.DataBranchRetireLedgerGrant{Err: err}
	}
	return databranchutils.DataBranchRetireLedgerGrant{
		Binding:               databranchutils.DataBranchFencingBindingFromLockTable(result.LockedOn),
		Fence:                 result.Timestamp,
		BindingRecordedBefore: recordedBefore,
	}
}

// AdvanceToFence installs the D173(B) causal frontier inside the retiring
// transaction: WaitLogTailAppliedAt(fence) then UpdateSnapshot(fence), in
// that order.
func (s *DataBranchFencingRetireSubstrate) AdvanceToFence(
	ctx context.Context, txnOp client.TxnOperator, fence timestamp.Timestamp,
) error {
	if _, err := s.txnClient.WaitLogTailAppliedAt(ctx, fence); err != nil {
		return err
	}
	return txnOp.UpdateSnapshot(ctx, fence)
}

// dataBranchFencingWatermarkFoldPoll is the bounded re-probe cadence and
// overall cap of awaitLedgerWatermark's tail: the logtail wait discharges
// the stage commits' own deferred-unlock gate, and the poll retires only
// the remaining unlock-RPC ordering remnant (one iteration in the common
// case). A cap turn raises the closure's error, which the critical
// section consumes through the whole-pair round-failure budget.
const (
	dataBranchFencingWatermarkFoldPoll    = 2 * time.Millisecond
	dataBranchFencingWatermarkFoldTimeout = 10 * time.Second
)

// errDataBranchWatermarkFoldTimeout is the named fold-timeout cause
// carried by awaitLedgerWatermark's foldCtx (D195(B.1)/(B.3)): it
// distinguishes fold-deadline returns from genuine caller cancellation so
// the former map to the one named fold-timeout class while the latter keep
// the ctx.Err() class verbatim.
var errDataBranchWatermarkFoldTimeout = moerr.NewInternalErrorNoCtx(
	"data branch fencing: watermark fold timeout")

// awaitLedgerWatermark holds the round until the ledger lock-table grant
// watermark can observe a fence >= floor (the round's own C_f2). The
// production stage commits' unlocks fold the lock-table watermark
// ASYNCHRONOUSLY after the commit response — witnessed live: the grant
// repeatedly samples a fence 16-25ms BELOW the round-minted C_f2 while
// every re-fence round mints a yet newer C_f2, so the class-(ii) re-fence
// budget cannot converge by itself. The wait is the round's OWN fold
// condition: probe the watermark once (the common case is already
// folded), otherwise wait on the exact logtail condition the deferred
// unlock itself gates on, then bounded-poll the watermark until the fold
// is observable. The watermark is sampled by a SHARED lock on the round's
// scratch identity key (which also serializes any still-held stage lock),
// taken in a throwaway transaction that rolls back having written
// nothing.
func (s *DataBranchFencingRetireSubstrate) awaitLedgerWatermark(
	ctx context.Context,
	accountID uint32,
	ledgerID uint64,
	def *pbplan.TableDef,
	scratchTableID uint64,
	floor timestamp.Timestamp,
) error {
	if floor.IsEmpty() {
		return nil
	}
	// D195(B.1): ONE absolute fold deadline minted at ENTRY, before the
	// first probe and the logtail pre-wait; EVERY fold wait below consumes
	// foldCtx, so the first probe's creation/snapshot wait, the pre-wait's
	// timestamp-waiter entry and every loop probe are bounded by the SAME
	// deadline. context.WithDeadlineCause names the fold-timeout cause: a
	// return caused by foldCtx's deadline maps to the SAME named
	// fold-timeout class the loop already raises (D195(B.3)), while genuine
	// CALLER cancellation keeps the ctx.Err() class verbatim.
	deadline := time.Now().Add(dataBranchFencingWatermarkFoldTimeout)
	foldCtx, foldCancel := context.WithDeadlineCause(ctx, deadline, errDataBranchWatermarkFoldTimeout)
	defer foldCancel()
	mapFoldErr := func(err error) error {
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return err
		}
		// Two fold-wait shapes carry an unusable inner class and map to the
		// named fold-timeout class (D195(B.3)): (i) any error observed at
		// or after the ONE entry-minted deadline — foldCtx's deadline flag
		// is not the sole authority because a stall-delayed inner wait can
		// surface its error before the derived flag becomes visible; (ii)
		// an inner lock WAIT timeout — every lock wait this round mints
		// lives inside the fold budget (the probe's grant sample and the
		// scratch/ledger grant rounds), so a lock-wait timeout is exactly
		// the condition "the fold consumed its budget waiting", and the
		// safe re-fence budget already decides how to proceed from there.
		// Errors strictly inside the budget with their own information
		// (abort, bind loss, capacity) keep their provenance verbatim.
		if foldCtx.Err() != nil || time.Now().After(deadline) ||
			moerr.IsMoErrCode(err, moerr.ErrLockWaitTimeout) {
			return moerr.NewInternalError(ctx,
				"data branch fencing: watermark fold timeout")
		}
		return err
	}
	watermark, err := s.probeScratchWatermark(foldCtx, accountID, ledgerID, def, scratchTableID)
	if err != nil {
		return mapFoldErr(err)
	}
	if !watermark.Less(floor) {
		return nil
	}
	_, waitErr := s.txnClient.WaitLogTailAppliedAt(foldCtx, floor)
	if waitErr != nil {
		return mapFoldErr(waitErr)
	}
	for {
		watermark, err = s.probeScratchWatermark(foldCtx, accountID, ledgerID, def, scratchTableID)
		if err != nil {
			return mapFoldErr(err)
		}
		if !watermark.Less(floor) {
			return nil
		}
		if time.Now().After(deadline) {
			return moerr.NewInternalError(ctx,
				"data branch fencing: watermark fold timeout")
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(dataBranchFencingWatermarkFoldPoll):
		}
	}
}

// probeScratchWatermark samples the ledger lock-table watermark via a
// SHARED admission lock on the round's scratch identity key inside a
// throwaway stage transaction; the shared request waits behind any
// still-held exclusive stage lock, and the returned timestamp is the
// watermark at grant time. The probe's transaction rides a LANELESS stage
// owner of the fencing pool (D186(B.2)(iii)): it charges the pool beneath
// the same saturation circuit breaker as the round lanes, and its
// exactly-once abandonment finishes the owner before this call returns.
func (s *DataBranchFencingRetireSubstrate) probeScratchWatermark(
	ctx context.Context,
	accountID uint32,
	ledgerID uint64,
	def *pbplan.TableDef,
	scratchTableID uint64,
) (timestamp.Timestamp, error) {
	s.alloc.Enter()
	defer s.alloc.Exit()
	owner, class := databranchutils.DataBranchFencingPoolFor(s.genKey).
		MintLanelessStageOwner(scratchTableID, "watermark-probe")
	if class != databranchutils.DataBranchFencingRoundCommitted {
		return timestamp.Timestamp{}, &databranchutils.DataBranchFencingError{
			Class: class,
			Cause: moerr.NewInternalErrorf(ctx,
				"data branch fencing watermark probe reserve for scratch %d: %s",
				scratchTableID, class),
		}
	}
	// D192(B.2): the probe operator's DISPOSAL is detached accounted work,
	// never statement-thread work: the carried defer's AbandonAsync records
	// the owner's terminal selection synchronously on this thread (the
	// D187(B.2) chosen-before-invocation authority) and runs the single
	// public Rollback DETACHED under the service-lifetime context; the
	// owner releases its charge only at the operator's observed ClosedEvent
	// (D188(B.1)(i)). Early returns ride the same defer as the recorded
	// duplicate (identity-checked no-op per D187(B.2)(iv)).
	defer owner.AbandonAsync()
	op, err := s.newStageTxn(ctx, accountID)
	if err != nil {
		return timestamp.Timestamp{}, err
	}
	if err = owner.BindOperator(op, scratchTableID); err != nil {
		_ = op.Rollback(ctx)
		return timestamp.Timestamp{}, err
	}
	keys, keyType, err := databranchutils.PkMoveRetentionScratchLockKey(
		s.mp, def, scratchTableID)
	if err != nil {
		return timestamp.Timestamp{}, err
	}
	defer keys.Clean(s.mp)
	// Same-account lock identity as the stage commits: only the lane
	// account's physical ledger copy folds its watermark on stage unlocks.
	accCtx := defines.AttachAccountId(ctx, accountID)
	proc, free := s.newBoundProc(accCtx, op)
	defer free()
	result, err := lockop.LockRowsForAdmissionResult(
		accCtx, s.eng, proc, ledgerID, keys, 0, keyType,
		lock.LockMode_Shared, 0)
	if err != nil {
		return timestamp.Timestamp{}, err
	}
	// D192(B.2): the sample is fully captured in the admission result;
	// disposal happens through the carried defer's detached AbandonAsync.
	return result.Timestamp, nil
}

// dataBranchRetireCSDegradeForced is the D155-C-family skip-path fixture
// (D212(C.2)/(C.3) guard/degrade cells): it forces this critical section's
// done=false degrade class so the caller's paired deletes run plainly in a
// LIVE multi-service schedule. Harness-only: never enabled on any
// production path, never referenced except the single guard site below.
var dataBranchRetireCSDegradeForced atomic.Bool

// ForceDataBranchRetireCSDegradeForTest flips the skip-path fixture and
// returns a restore func; harness-only.
func ForceDataBranchRetireCSDegradeForTest(forced bool) (restore func()) {
	prev := dataBranchRetireCSDegradeForced.Swap(forced)
	return func() { dataBranchRetireCSDegradeForced.Store(prev) }
}

// RunDataBranchFencingRetirementCS drives one address's retirement
// critical section on the retiring statement's transaction. It returns
// (done=false, false, nil) in every degrade class — missing CN substrate,
// non-pessimistic-RC retiring statement, pre-upgrade or legacy-shape
// ledger — so the caller runs its paired deletes as before; every fencing
// error propagates as the statement's determinate error surface.
//
// Before any witness round is minted, the D168(B) pre-round gate runs:
// the retiring transaction takes its EXCLUSIVE ledger-row-key lock FIRST,
// then the address's committed link presence AND ledger identity row are
// probed at that locked view. When the locked view shows neither, no
// writer-staged state can exist for the address (every stage transaction
// must take this same ledger-key lock at stage time and hold it to
// commit, so an in-flight first touch cannot be invisible behind the
// gate's grant), the caller's paired deletes are in-txn no-ops, and the
// witness round and watermark fold are skipped entirely — nothing needs
// a fence and the round's stage commits, fold budget and envelope
// certification never engage on a plain unlink drop path. When either is
// present the full critical section below runs unchanged.
func RunDataBranchFencingRetirementCS(
	ctx context.Context,
	sub *DataBranchFencingRetireSubstrate,
	txnOp client.TxnOperator,
	accountID uint32,
	tableID uint64,
	reprobe databranchutilsReprobeFunc,
	ledgerProbe databranchutilsReprobeFunc,
) (done bool, linkPresence bool, err error) {
	if dataBranchRetireCSDegradeForced.Load() || sub == nil || txnOp == nil ||
		!txnOp.Txn().IsPessimistic() || !txnOp.Txn().IsRCIsolation() {
		// The harness skip-path fixture stands FIRST with the production
		// degrade classes untouched.
		return false, false, nil
	}
	// Carry the statement's stage allocator (nil on the compile
	// producer's process-pool path) so the fencing driver reserves every
	// lane's coverage in the dispatching thread, before the lane
	// goroutine exists (D186(B.4), review F3).
	ctx = databranchutils.AttachDataBranchStageAllocator(ctx, sub.alloc)
	ledgerID, def, ok, err := sub.resolveRetentionLedger(ctx, txnOp, accountID)
	if err != nil || !ok {
		return false, false, err
	}
	lanes := &dataBranchFencingProductionLanes{
		sub:       sub,
		accountID: accountID,
		ledgerID:  ledgerID,
		ledgerDef: def,
		floors:    DataBranchFencingWorkspaceFloorsPinned(),
	}
	// roundIn mirrors the loop's latest drawn round input so the
	// watermark-fold closure can probe THIS round's scratch key. Every
	// closure runs on the retiring statement's own thread and the loop
	// sequences NewRoundInput -> RunRound -> AwaitWatermarkFold, so the
	// captured value is race-free.
	var roundIn databranchutils.DataBranchFencingRoundInput
	deps := databranchutils.DataBranchRetireCSDeps{
		// The round driver rides THIS CN generation's fencing pool
		// (D189(B.2)): shutdown closure and the drain barrier scope to
		// the service UUID, never a process-global pool.
		Rounds: databranchutils.NewDataBranchFencingDriver(
			databranchutils.DataBranchFencingPoolFor(sub.genKey)),
		Locks: lanes,
		Exec:  lanes,
		AwaitWatermarkFold: func(gctx context.Context, floor timestamp.Timestamp) error {
			return sub.awaitLedgerWatermark(
				gctx, accountID, ledgerID, def, roundIn.ScratchTableID, floor)
		},
		AcquireLedgerGrant: func(gctx context.Context) databranchutils.DataBranchRetireLedgerGrant {
			return sub.AcquireLedgerGrant(gctx, txnOp, accountID, ledgerID, def, tableID)
		},
		AdvanceToFence: func(gctx context.Context, fence timestamp.Timestamp) error {
			return sub.AdvanceToFence(gctx, txnOp, fence)
		},
		ReprobeLinkPresence: reprobe,
		NewRoundInput: func() (databranchutils.DataBranchFencingRoundInput, error) {
			in, inErr := databranchutils.DataBranchFencingNewRoundInput()
			if inErr == nil {
				roundIn = in
			}
			return in, inErr
		},
	}
	// D168(B) pre-round gate. The gate grant's own lock-layer classes
	// propagate verbatim exactly as the loop's acceptance grant would
	// surface them (recorded-binding change, lock-wait timeout, deadlock
	// victimization). The gate records the ledger binding inside this
	// transaction BEFORE the round, so when the locked view proves the
	// section is needed, a binding flip surfacing at the acceptance grant
	// classifies as a recorded-binding change (terminal) rather than an
	// unbound pre-grant flip (in-place re-fence): the flip window the
	// re-fence class covered shrinks to the gate-to-acceptance gap on an
	// already-held lock, and the fail-safe whole-transaction rollback
	// covers it (D176(B.2) class (i)).
	gateGrant := deps.AcquireLedgerGrant(ctx)
	if gateGrant.Err != nil {
		return true, false, gateGrant.Err
	}
	gateLinks, gateErr := reprobe(ctx)
	if gateErr != nil {
		return true, false, gateErr
	}
	if !gateLinks {
		var gateLedger bool
		if gateLedger, gateErr = ledgerProbe(ctx); gateErr != nil {
			return true, false, gateErr
		}
		if !gateLedger {
			// Locked view shows no committed links and no identity row,
			// and no unseen concurrent stage can commit either behind
			// this transaction's held grant: the paired deletes are
			// no-ops at this same view, so the witness round and fold
			// never engage.
			return true, false, nil
		}
	}
	linkPresence, err = databranchutils.RunDataBranchFencingRetireCS(ctx, deps)
	return true, linkPresence, err
}

// databranchutilsReprobeFunc is the caller's D170(B)(3) locked-view
// re-probe closure: a fresh internal statement inside the retiring
// transaction reporting the address's committed link presence.
type databranchutilsReprobeFunc = func(ctx context.Context) (bool, error)

// runDataBranchPkMoveFencingRetire is the compile drop-lifecycle entry:
// resolve this Compile's CN substrate, and when present and usable retire
// the address through the fencing critical section before the caller's
// paired deletes. Degrade classes (done=false): no CN substrate (mock
// procs, non-CN engines), a non-pessimistic-RC retiring statement, or a
// pre-upgrade/legacy-shape ledger.
func runDataBranchPkMoveFencingRetire(
	c *Compile, accountID uint32, tableID uint64,
) (done bool, linkPresence bool, err error) {
	if c == nil || c.proc == nil || c.e == nil {
		return false, false, nil
	}
	txnOp := c.proc.GetTxnOperator()
	if txnOp == nil {
		return false, false, nil
	}
	sub := DataBranchFencingRetireSubstrateFromRuntime(c.proc, c.e)
	if sub == nil {
		return false, false, nil
	}
	return RunDataBranchFencingRetirementCS(
		c.proc.Ctx, sub, txnOp, accountID, tableID,
		func(rctx context.Context) (bool, error) {
			return pkMoveLinkPresenceProbeRC(c, rctx, accountID, tableID)
		},
		func(rctx context.Context) (bool, error) {
			return pkMoveLedgerRowPresenceProbeRC(c, rctx, accountID, tableID)
		},
	)
}

// pkMoveLedgerRowPresenceProbeRC is the D168(B) pre-round gate's
// locked-view ledger probe for one retired address: a fresh statement
// inside the retiring transaction reporting whether the wide retention
// ledger still holds the address's row_kind = 0 identity row. A missing
// ledger catalog (pre-upgrade tenant) reads as absent.
func pkMoveLedgerRowPresenceProbeRC(
	c *Compile, rctx context.Context, accountID uint32, tableID uint64,
) (bool, error) {
	res, err := c.runSqlWithResult(fmt.Sprintf(
		"select 1 from mo_catalog.%s where %s = %d and %s = %d limit 1",
		catalog.MO_DATA_BRANCH_LINK_RETENTION,
		catalog.DataBranchLinkRetentionColRowKind, catalog.DataBranchLinkRetentionRowKindReal,
		catalog.DataBranchLinkRetentionColTableID, tableID,
	), int32(accountID))
	if err != nil {
		if moerr.IsMoErrCode(err, moerr.ErrNoSuchTable) {
			return false, nil
		}
		return false, err
	}
	found, ferr := resultHasAnyRow(res)
	res.Close()
	return found, ferr
}

// pkMoveLinkPresenceProbeRC is the D170(B)(3) locked-view re-probe for one
// retired address: a fresh statement inside the retiring transaction
// reporting whether any committed update link row survives under the
// address's link-id prefix. A pre-upgrade tenant (no link catalog) reads
// as absent. The presence bit is informational; the caller's delete set is
// computed at the same locked view.
func pkMoveLinkPresenceProbeRC(
	c *Compile, rctx context.Context, accountID uint32, tableID uint64,
) (bool, error) {
	if tableID+1 == 0 {
		// MaxUint64 overflows the half-open upper bound (matched with the
		// delete's guard); no probe can name it, so presence is reported
		// absent and the delete's own guard decides.
		return false, nil
	}
	res, err := c.runSqlWithResult(fmt.Sprintf(
		"select %s from mo_catalog.%s where %s >= x'%016x' and %s < x'%016x' limit 1",
		catalog.DataBranchUpdateLinkColLinkID, catalog.MO_DATA_BRANCH_UPDATE_LINK,
		catalog.DataBranchUpdateLinkColLinkID, tableID,
		catalog.DataBranchUpdateLinkColLinkID, tableID+1,
	), int32(accountID))
	if err != nil {
		if moerr.IsMoErrCode(err, moerr.ErrNoSuchTable) {
			return false, nil
		}
		return false, err
	}
	found := false
	res.ReadRows(func(n int, _ []*vector.Vector) bool {
		found = found || n > 0
		return !found
	})
	res.Close()
	return found, nil
}

// readUint64Column drains one single-uint64-column executor result.
func readUint64Column(res executor.Result) ([]uint64, error) {
	var out []uint64
	for _, bat := range res.Batches {
		if bat == nil || len(bat.Vecs) == 0 {
			continue
		}
		col := vector.MustFixedColNoTypeCheck[uint64](bat.Vecs[0])
		out = append(out, col...)
	}
	return out, nil
}

// resultHasAnyRow reports whether a single-row probe result carries a row.
func resultHasAnyRow(res executor.Result) (bool, error) {
	for _, bat := range res.Batches {
		if bat != nil && bat.RowCount() > 0 {
			return true, nil
		}
	}
	return false, nil
}
