// Copyright 2021 Matrix Origin
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
	"cmp"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/parquet-go/parquet-go"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/common/system"
	commonutil "github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	icebergio "github.com/matrixorigin/matrixone/pkg/iceberg/io"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/apply"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/connector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/deletion"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/dispatch"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/external"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/fill"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/filter"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/group"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashbuild"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/intersect"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/intersectall"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/lockop"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/merge"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergeblock"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergecte"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergedelete"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergerecursive"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/minus"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mongoscan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/multi_update"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/output"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/partition"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/sample"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/table_scan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/unionall"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/value_scan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/vectorscan"
	"github.com/matrixorigin/matrixone/pkg/sql/crt"
	sqldatastream "github.com/matrixorigin/matrixone/pkg/sql/datastream"
	"github.com/matrixorigin/matrixone/pkg/sql/foreignext"
	"github.com/matrixorigin/matrixone/pkg/sql/internal/materialized"
	sqlmongodb "github.com/matrixorigin/matrixone/pkg/sql/mongodb"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/rule"
	"github.com/matrixorigin/matrixone/pkg/sql/schedule"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	txnTrace "github.com/matrixorigin/matrixone/pkg/txn/trace"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace/statistic"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/panjf2000/ants/v2"
	"go.uber.org/zap"
)

// Note: Now the cost going from stat is actually the number of rows, so we can only estimate a number for the size of each row.
// The current insertion of around 200,000 rows triggers cn to write s3 directly
const (
	DistributedThreshold         uint64 = 10 * mpool.MB
	SingleLineSizeEstimate       uint64 = 300 * mpool.B
	shuffleChannelBufferSize            = 32
	loadWriteS3ParallelSizeLimit        = 4

	NoAccountId = -1
)

var (
	cantCompileForPrepareErr = moerr.NewCantCompileForPrepareNoCtx()
)

// NewCompile is used to new an object of compile
func NewCompile(
	addr, db, sql, tenant, uid string,
	e engine.Engine,
	proc *process.Process,
	stmt tree.Statement,
	isInternal bool,
	cnLabel map[string]string,
	startAt time.Time,
) *Compile {
	c := allocateNewCompile(proc)

	c.e = e
	c.db = db
	c.tenant = tenant
	c.uid = uid
	c.sql = sqlmongodb.RedactSQLForDiagnostics(sql)
	c.proc.SetMessageBoard(c.MessageBoard)
	c.stmt = stmt
	c.addr = addr
	c.isInternal = isInternal
	c.cnLabel = cnLabel
	c.startAt = startAt
	c.disableRetry = false
	c.ncpu = system.GoMaxProcs()
	c.lockMeta = NewLockMeta()
	// TODO: The action of updating the WriteOffset logic should be executed in the `func (c *Compile) Run(_ uint64)` method.
	// However, considering that the delay ranges are not completed yet, the UpdateSnapshotWriteOffset() and
	// the assignment of `Compile.TxnOffset` should be moved into the `func (c *Compile) Run(_ uint64)` method in the later stage.
	c.TxnOffset = txnOffsetOfCompile(c.proc)
	return c
}

// txnOffsetOfCompile returns the workspace write offset a new compile reads
// with. Compiling a user statement advances the statement boundary of the
// workspace and reads with it. An internal sub-sql of the current statement
// (DisableIncrStatement, marked on the process) instead captures the current
// end of the workspace — it reads everything its caller has written so far,
// but it opens no statement, so it must not advance the shared boundary:
// moving the boundary mid-statement breaks the positional visibility of the
// caller's workspace entries (issue #25557).
func txnOffsetOfCompile(proc *process.Process) int {
	op := proc.GetTxnOperator()
	if op == nil {
		return 0
	}
	ws := op.GetWorkspace()
	if proc.IncrStatementDisabled() {
		return int(ws.WriteOffset())
	}
	ws.UpdateSnapshotWriteOffset()
	return ws.GetSnapshotWriteOffset()
}

func (c *Compile) Release() {
	if c == nil {
		return
	}
	if c.siriusRead != nil {
		if err := c.siriusRead.finish(context.Background(), false); err != nil && c.proc != nil {
			c.proc.Error(context.Background(), "failed to quiesce Sirius read during compile release", zap.Error(err))
		}
		c.siriusRead = nil
	}
	c.SetSchedulingTraceRecorder(nil)
	if c.proc != nil {
		c.proc.ResetQueryContext()
		c.proc.ResetCloneTxnOperator()
		// Compile owns the immutable binding. Process only carries it while this
		// execution is active; leaving it behind can contaminate a later lock
		// caller that has no compiled plan.
		c.proc.ClearPlanSnapshotTS()
	}
	doCompileRelease(c)
}

func (c Compile) TypeName() string {
	return "compile.Compile"
}

func (c *Compile) GetMessageCenter() *message.MessageCenter {
	if c == nil || c.e == nil {
		return nil
	}
	m := c.e.GetMessageCenter()
	if m != nil {
		mc, ok := m.(*message.MessageCenter)
		if ok {
			return mc
		}
	}
	return nil
}

// GetPlan returns the current plan of the Compile.
// This is useful for getting the latest plan after a retry.
func (c *Compile) GetPlan() *plan.Plan {
	if c == nil {
		return nil
	}
	return c.pn
}

// PlanGenerationRebuilt reports whether a retry rebuilt this Compile's logical
// plan. A frontend prepared statement must then discard both the old logical
// plan and any physical topology derived from it.
func (c *Compile) PlanGenerationRebuilt() bool {
	return c != nil && c.planGenerationRebuilt
}

// PlanSnapshotTS returns the snapshot bound to the current logical-plan
// generation. The binding is immutable until a definition-change retry
// publishes a replacement plan generation.
func (c *Compile) PlanSnapshotTS() (timestamp.Timestamp, bool) {
	if c == nil || !c.hasPlanSnapshotTS {
		return timestamp.Timestamp{}, false
	}
	return c.planSnapshotTS, true
}

// SetPlanSnapshotTS binds Compile to the snapshot of an already-built logical
// plan. It must be called before Compile when planning and physical compilation
// are separated, as they are for prepared statements without a cached pipeline.
func (c *Compile) SetPlanSnapshotTS(ts timestamp.Timestamp) {
	c.planSnapshotTS = ts
	c.hasPlanSnapshotTS = true
	c.planGenerationReused = false
	c.applyPlanSnapshot()
}

// SetPlanGenerationReused marks whether the current execution admitted this
// logical-plan generation from a session or prepared cache.
func (c *Compile) SetPlanGenerationReused(reused bool) {
	c.planGenerationReused = reused
	c.applyPlanSnapshot()
}

// FreezeResultMetadata prevents a definition retry from executing a rebuilt
// plan whose result schema differs from metadata already materialized by the
// frontend or another streaming consumer.
func (c *Compile) FreezeResultMetadata() {
	if c != nil {
		c.resultMetadataFrozen = true
	}
}

func (c *Compile) Reset(proc *process.Process, startAt time.Time, fill func(*batch.Batch, *perfcounter.CounterSet) error, sql string) error {
	if c.siriusRead != nil {
		if err := c.siriusRead.finish(context.Background(), false); err != nil {
			return err
		}
		c.siriusRead = nil
	}
	// clean up the process for a new query.
	proc.ResetQueryContext()
	proc.ResetCloneTxnOperator()
	c.proc = proc
	c.proc.BeginFoundRowsStatement(statementHasSQLCalcFoundRows(c.stmt))
	c.applyPlanSnapshot()
	c.captureStringShuffleHashAlgorithm()

	c.fill = fill
	c.sql = sqlmongodb.RedactSQLForDiagnostics(sql)
	c.affectRows.Store(0)
	// Reset reuses an existing logical/physical generation. Reused generations
	// are deliberately ineligible for LOAD unique-index promotion.
	c.clearLoadUniqueIndexPromotion()
	c.executionGeneration = 0
	c.resultMetadataFrozen = false
	c.anal.Reset(c.isPrepare, c.IsTpQuery())

	if c.lockMeta != nil {
		c.lockMeta.reset(c.proc)
	}
	if err := refreshGroupConcatMaxLen(c.scopes, proc); err != nil {
		return err
	}
	rejectZeroTemporal, err := util.RejectZeroTemporalWritePolicy(proc)
	if err != nil {
		return err
	}
	for _, s := range c.scopes {
		if err = s.reset(c, rejectZeroTemporal); err != nil {
			return err
		}
	}

	for _, e := range c.filterExprExes {
		e.ResetForNextQuery()
	}

	c.MessageBoard = c.MessageBoard.Reset()
	proc.SetMessageBoard(c.MessageBoard)
	c.remoteFragmentCounts = nil
	c.remoteExecutionID = uuid.Nil
	c.counterSet.Reset()

	for _, f := range c.fuzzys {
		f.reset()
	}
	c.startAt = startAt
	c.TxnOffset = txnOffsetOfCompile(c.proc)
	if c.proc.GetTxnOperator() != nil {
		// all scopes should update the txn offset, or the reader will receive a 0 txnOffset,
		// that cause a dml statement can not see the previous statements' operations.
		if len(c.scopes) > 0 {
			for i := range c.scopes {
				UpdateScopeTxnOffset(c.scopes[i], c.TxnOffset)
			}
		}
	}

	// A reused prepared pipeline runs directly after Reset. Only retries compile
	// again, so the cached placement is this execution's first real attempt.
	c.beginSchedulingTraceAttempt()
	if c.queryPlacement.Reason != "" {
		c.recordQuerySchedulingTrace(c.queryPlacement)
	}
	return nil
}

// capturePlanSnapshot starts a new compiled-plan generation. Compile owns the
// binding; Process only transports it to local and remote pipeline operators.
func (c *Compile) capturePlanSnapshot() {
	c.planGenerationReused = false
	txnOp := c.proc.GetTxnOperator()
	if txnOp == nil {
		c.hasPlanSnapshotTS = false
		c.planSnapshotTS = timestamp.Timestamp{}
		c.applyPlanSnapshot()
		return
	}
	c.planSnapshotTS = txnOp.Txn().SnapshotTS
	c.hasPlanSnapshotTS = true
	c.applyPlanSnapshot()
}

// applyPlanSnapshot binds the Process transport to this Compile's immutable
// plan generation. In particular, Reset must apply rather than recapture it.
func (c *Compile) applyPlanSnapshot() {
	if !c.hasPlanSnapshotTS {
		c.proc.ClearPlanSnapshotTS()
		return
	}
	c.proc.SetPlanSnapshotTS(c.planSnapshotTS)
	c.proc.SetPlanGenerationReused(c.planGenerationReused)
}

func (c *Compile) inheritPlanSnapshot(from *Compile) {
	c.planSnapshotTS = from.planSnapshotTS
	c.hasPlanSnapshotTS = from.hasPlanSnapshotTS
	c.planGenerationReused = from.planGenerationReused
	c.applyPlanSnapshot()
}

func (c *Compile) bindPlanSnapshotForCompile() {
	if !c.hasPlanSnapshotTS {
		c.capturePlanSnapshot()
		return
	}
	c.applyPlanSnapshot()
}

// captureStringShuffleHashAlgorithm starts a new execution owner-mapping
// generation. MOProtocolVersion is consulted exactly once; operators consume
// only the frozen Process value afterwards.
func (c *Compile) captureStringShuffleHashAlgorithm() {
	c.stringShuffleHashAlgorithm = process.StringShuffleHashLegacy
	if supportsStableStringShuffleHash(c.proc.GetService()) {
		c.stringShuffleHashAlgorithm = process.StringShuffleHashComplete
	}
	c.stringShuffleHashAlgorithmFrozen = true
	c.applyStringShuffleHashAlgorithm()
}

func (c *Compile) applyStringShuffleHashAlgorithm() {
	c.proc.SetStringShuffleHashAlgorithm(c.stringShuffleHashAlgorithm)
}

func (c *Compile) inheritStringShuffleHashAlgorithm(from *Compile) {
	c.stringShuffleHashAlgorithm = from.stringShuffleHashAlgorithm
	c.stringShuffleHashAlgorithmFrozen = from.stringShuffleHashAlgorithmFrozen
	c.applyStringShuffleHashAlgorithm()
}

func (c *Compile) bindStringShuffleHashAlgorithmForCompile() {
	if !c.stringShuffleHashAlgorithmFrozen {
		c.captureStringShuffleHashAlgorithm()
		return
	}
	c.applyStringShuffleHashAlgorithm()
}

func supportsStableStringShuffleHash(service string) bool {
	rt := moruntime.ServiceRuntime(service)
	if rt == nil {
		return false
	}
	version, ok := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	protocolVersion, valid := version.(int64)
	return ok && valid && protocolVersion >= defines.MORPCVersion33
}

func UpdateScopeTxnOffset(scope *Scope, txnOffset int) {
	scope.TxnOffset = txnOffset
	_ = vm.HandleAllOp(scope.RootOp, func(_ vm.Operator, op vm.Operator) error {
		if applyOp, ok := op.(*apply.Apply); ok {
			applyOp.TxnOffset = txnOffset
		}
		return nil
	})
	for i := range scope.PreScopes {
		UpdateScopeTxnOffset(scope.PreScopes[i], txnOffset)
	}
}

func (c *Compile) clear() {
	if c.anal != nil {
		c.anal.release()
	}
	// Materialized sources own allocation-account-backed retained and decoded
	// batches but are not VM operators. Close their compile-owned safety nets
	// before sealing the execution account, especially on partial-run failures
	// where producer/reader Reset did not release every source owner.
	for k, source := range c.materializedSources {
		source.Close()
		delete(c.materializedSources, k)
	}
	// The attempt owns references to allocation-aware operators. Finalize it
	// before Scope.release returns those operators to reuse pools; otherwise a
	// defensive cleanup path could clear an already-reset or reused owner.
	if err := c.finishAllocationAccountAttempt(); err != nil {
		logutil.Errorf("allocation account terminal cleanup failed: %v", err)
	}
	for i := range c.scopes {
		c.scopes[i].release()
	}
	for i := range c.fuzzys {
		c.fuzzys[i].release()
	}

	c.MessageBoard = c.MessageBoard.Reset()
	c.fuzzys = c.fuzzys[:0]
	c.scopes = c.scopes[:0]
	c.pn = nil
	c.fill = nil
	c.resultSink = nil
	c.executionGeneration = 0
	c.affectRows.Store(0)
	c.addr = ""
	c.db = ""
	c.tenant = ""
	c.uid = ""
	c.sql = ""
	c.originSQL = ""
	c.anal = nil
	c.e = nil
	c.clearLoadUniqueIndexPromotion()

	if c.lockMeta != nil {
		c.lockMeta.clear(c.proc)
		c.lockMeta = nil
	}

	c.proc.Free()
	c.proc = nil
	c.planSnapshotTS = timestamp.Timestamp{}
	c.hasPlanSnapshotTS = false
	c.planGenerationReused = false
	c.stringShuffleHashAlgorithm = process.StringShuffleHashLegacy
	c.stringShuffleHashAlgorithmFrozen = false
	c.resultMetadataFrozen = false
	c.planGenerationRebuilt = false

	c.cnList = c.cnList[:0]
	c.queryPlacement = schedule.QueryDecision{}
	c.querySchedulingIntent = schedule.SchedulingIntent{}
	c.schedulingTrace = nil
	c.schedulingAttempt = 0
	c.stmt = nil
	c.startAt = time.Time{}
	c.needLockMeta = false
	c.isInternal = false
	c.resourceAttemptOwnerEligible = false
	c.allocationAccountRegistry = nil
	c.allocationAccountLimit = 0
	c.allocationControllerProvider = nil
	c.allocationTerminalExporter = nil
	c.allocationAccountOwners = nil
	c.allocationAttempt = nil
	c.remoteFragmentCounts = nil
	c.remoteExecutionID = uuid.Nil
	c.isPrepare = false
	c.hasMergeOp = false
	c.needBlock = false
	c.ignorePublish = false
	c.adjustTableExtraFunc = nil
	c.disableDropAutoIncrement = false
	c.skipDataBranchReclaim = false
	c.keepAutoIncrement = 0
	c.disableLock = false
	c.icebergScanPlanner = nil

	for _, exe := range c.filterExprExes {
		exe.Free()
	}
	c.filterExprExes = nil

	for k := range c.lockTables {
		delete(c.lockTables, k)
	}
	for k := range c.nodeRegs {
		delete(c.nodeRegs, k)
	}
	for k := range c.stepRegs {
		delete(c.stepRegs, k)
	}
	for k := range c.materializedSinkScanNodes {
		delete(c.materializedSinkScanNodes, k)
	}
	for k := range c.materializedReaderIDs {
		delete(c.materializedReaderIDs, k)
	}
	for k := range c.cnLabel {
		delete(c.cnLabel, k)
	}
}

func (c *Compile) addAllAffectedRows(s *Scope) {
	for _, ps := range s.PreScopes {
		c.addAllAffectedRows(ps)
	}
	c.addAffectedRows(s.affectedRows())
}

func (c *Compile) addAffectedRows(n uint64) {
	c.affectRows.Add(n)
}

func (c *Compile) setAffectedRows(n uint64) {
	c.affectRows.Store(n)
}

func (c *Compile) getAffectedRows() uint64 {
	affectRows := c.affectRows.Load()
	return affectRows
}

func (c *Compile) run(s *Scope) error {
	if s == nil {
		return nil
	}

	switch s.Magic {
	case Normal:
		err := s.Run(c)
		if err != nil {
			return err
		}

		c.addAffectedRows(s.affectedRows())
		return nil
	case Merge, MergeInsert:
		err := s.MergeRun(c)
		if err != nil {
			return err
		}

		c.addAffectedRows(s.affectedRows())
		return nil
	case MergeDelete:
		err := s.MergeRun(c)
		if err != nil {
			return err
		}
		mergeArg := s.RootOp.(*mergedelete.MergeDelete)
		if mergeArg.AddAffectedRows {
			c.addAffectedRows(mergeArg.GetAffectedRows())
		}
		return nil
	case Remote:
		err := s.RemoteRun(c)
		//@FIXME not a good choice after all DML refactor finish
		if _, ok := s.RootOp.(*multi_update.MultiUpdate); ok {
			for _, ps := range s.PreScopes {
				c.addAllAffectedRows(ps)
			}
		}
		c.addAffectedRows(s.affectedRows())
		return err
	case CreateDatabase:
		return s.CreateDatabase(c)
	case DropDatabase:
		err := s.DropDatabase(c)
		if err != nil {
			return err
		}
		return nil
	case CreateTable:
		return s.CreateTable(c)
	case CreatePitr:
		return s.CreatePitr(c)
	case CreateCDC:
		return s.CreateCDC(c)
	case CreateView:
		return s.CreateView(c)
	case AlterView:
		return s.AlterView(c)
	case AlterTable:
		return s.AlterTable(c)
	case RenameTable:
		return s.RenameTable(c)
	case DropTable:
		return s.DropTable(c)
	case DropPitr:
		return s.DropPitr(c)
	case DropCDC:
		return s.DropCDC(c)
	case DropSequence:
		return s.DropSequence(c)
	case CreateSequence:
		return s.CreateSequence(c)
	case AlterSequence:
		return s.AlterSequence(c)
	case CreateIndex:
		return s.CreateIndex(c)
	case DropIndex:
		return s.DropIndex(c)
	case TruncateTable:
		return s.TruncateTable(c)
	case TableClone:
		return s.TableClone(c)
	}
	return nil
}

// isRetryErr if the error is ErrTxnNeedRetry and the transaction is RC isolation, we need to retry t
// he statement
func (c *Compile) isRetryErr(err error) bool {
	return (moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetry) ||
		moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetryWithDefChanged)) &&
		c.proc.GetTxnOperator().Txn().IsRCIsolation()
}

type scopeRunResult struct {
	err      error
	ctx      context.Context
	queryCtx context.Context
}

func newScopeRunResult(err error, scope *Scope) scopeRunResult {
	if scope == nil {
		return scopeRunResult{err: err}
	}
	return newScopeRunResultForProcess(err, scope.Proc)
}

func newScopeRunResultForProcess(err error, proc *process.Process) scopeRunResult {
	result := scopeRunResult{err: err}
	if proc == nil {
		return result
	}
	result.ctx = proc.Ctx
	result.queryCtx = scopeRunQueryContext(proc)
	return result
}

func scopeRunQueryContext(proc *process.Process) context.Context {
	if proc == nil || proc.Base == nil {
		return nil
	}
	queryCtx, _ := process.GetQueryCtxFromProc(proc)
	if queryCtx != nil {
		return queryCtx
	}
	return proc.GetTopContext()
}

func isScopeCancellationError(err error) bool {
	if err == nil {
		return false
	}
	// errors.Join must not turn a substantive execution failure into
	// cancellation fallout merely because one of its siblings is a context
	// error. Every leaf has to be cancellation-shaped before it is safe to
	// suppress or replace the result.
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		children := joined.Unwrap()
		if len(children) == 0 {
			return false
		}
		for _, child := range children {
			if !isScopeCancellationError(child) {
				return false
			}
		}
		return true
	}
	if wrapped, ok := err.(interface{ Unwrap() error }); ok {
		if child := wrapped.Unwrap(); child != nil {
			return isScopeCancellationError(child)
		}
	}
	return errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded) ||
		moerr.IsMoErrCode(err, moerr.ErrQueryInterrupted)
}

// isScopeCancellationFrom reports whether every leaf in err can be attributed
// to the same canceled context. A joined error is attributable only as a whole;
// one matching cancellation leaf must not hide an independent deadline leaf.
func isScopeCancellationFrom(err error, contextErr error) bool {
	if err == nil || contextErr == nil {
		return false
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		children := joined.Unwrap()
		if len(children) == 0 {
			return false
		}
		for _, child := range children {
			if !isScopeCancellationFrom(child, contextErr) {
				return false
			}
		}
		return true
	}
	if wrapped, ok := err.(interface{ Unwrap() error }); ok {
		if child := wrapped.Unwrap(); child != nil {
			return isScopeCancellationFrom(child, contextErr)
		}
	}
	return errors.Is(err, contextErr) ||
		moerr.IsMoErrCode(err, moerr.ErrQueryInterrupted)
}

// normalizeScopeRunError distinguishes a substantive execution failure from
// cancellation fallout.  A child pipeline canceled by a successfully finished
// consumer is secondary and may be ignored, while a real error carried as the
// pipeline cancel cause must survive.  Query-level cancellation remains owned
// by the query context and is reported by Compile.Run.
func normalizeScopeRunError(
	err error,
	pipelineCtx context.Context,
	queryCtx context.Context,
) (error, bool) {
	if err == nil || !isScopeCancellationError(err) ||
		pipelineCtx == nil || pipelineCtx.Err() == nil {
		return err, false
	}
	// Query cancellation owns the terminal classification. In particular,
	// context.WithTimeoutCause reports DeadlineExceeded through Err while Cause
	// carries diagnostic detail. Replacing the former with the latter would make
	// callers misclassify a timeout as an ordinary execution failure; they can
	// attach the cause after observing DeadlineExceeded.
	if queryCtx != nil {
		if queryErr := queryCtx.Err(); queryErr != nil {
			if errors.Is(queryErr, context.DeadlineExceeded) {
				return queryErr, true
			}
			if !isScopeCancellationFrom(err, queryErr) {
				return err, false
			}
			if cause := context.Cause(queryCtx); cause != nil {
				return cause, true
			}
			return queryErr, true
		}
	}

	// A context-shaped error is secondary only when every leaf was derived from
	// this pipeline's cancellation. Preserve an independent operator timeout
	// that merely raced a different pipeline cancellation, including when the
	// two errors were joined.
	if !isScopeCancellationFrom(err, pipelineCtx.Err()) {
		return err, false
	}

	if cause := context.Cause(pipelineCtx); cause != nil {
		err = cause
	}
	if isScopeCancellationError(err) && queryCtx != nil && queryCtx.Err() == nil {
		return nil, true
	}
	return err, true
}

func (r scopeRunResult) resolveCancelCause() (scopeRunResult, bool) {
	var normalized bool
	r.err, normalized = normalizeScopeRunError(r.err, r.ctx, r.queryCtx)
	return r, normalized
}

// preferPrimaryScopeResult keeps cleanup fallout from masking the execution
// error that caused another scope to stop consuming its pipeline input. A
// cancellation result is first resolved through that scope's CancelCauseFunc:
// internally canceled siblings therefore report the triggering execution
// error, while an externally canceled query keeps its external cause.
func preferPrimaryScopeResult(current, candidate scopeRunResult) scopeRunResult {
	current, _ = current.resolveCancelCause()
	candidate, candidateNormalized := candidate.resolveCancelCause()

	if current.err == nil {
		return candidate
	}
	if candidate.err == nil ||
		!errors.Is(current.err, process.ErrPipelineEndSignalDeliveryFailed) ||
		errors.Is(candidate.err, process.ErrPipelineEndSignalDeliveryFailed) {
		return current
	}
	// An unresolved pure cancellation does not prove that the cleanup fallback
	// was secondary. A mixed error tree is substantive, however, and must not be
	// rejected merely because one leaf is context.Canceled.
	if !candidateNormalized &&
		isScopeCancellationFrom(candidate.err, context.Canceled) {
		return current
	}
	return candidate
}

func (c *Compile) canRetry(err error) bool {
	if c.disableRetry {
		return false
	}
	if moerr.IsMoErrCode(err, moerr.ErrVectorNeedRetryWithPreMode) {
		return true
	}
	return c.isRetryErr(err)
}

func (c *Compile) IsTpQuery() bool {
	return c.execType == plan2.ExecTypeTP
}

func (c *Compile) IsSingleScope(ss []*Scope) bool {
	if c.IsTpQuery() {
		return true
	}
	return len(ss) == 1 && ss[0].NodeInfo.Mcpu == 1
}

func (c *Compile) SetIsPrepare(isPrepare bool) {
	c.isPrepare = isPrepare
}

func (c *Compile) FreeOperator() {
	for _, s := range c.scopes {
		s.FreeOperator(c)
	}
}

/*
func (c *Compile) printPipeline() {
	if c.IsTpQuery() {
		fmt.Println("pipeline for tp query!", "sql: ", c.originSQL)
	} else {
		fmt.Println("pipeline for ap query! current cn", c.addr, "sql: ", c.originSQL)
	}
	fmt.Println(DebugShowScopes(c.scopes, OldLevel))
}
*/

// prePipelineInitializer is responsible for handling some tasks that need to be done before truly launching the pipeline.
//
// for example
// 1. lock table.
// 2. init data source.
func (c *Compile) prePipelineInitializer() (startedSources []*materialized.Source, err error) {
	// do table lock.
	if err = c.lockMeta.doLock(c.e, c.proc); err != nil {
		return nil, err
	}
	if err = c.lockTable(); err != nil {
		return nil, err
	}
	if err = c.maybePromoteLoadUniqueIndexes(); err != nil {
		return nil, err
	}

	// init data source.
	for _, s := range c.scopes {
		if err = s.InitAllDataSource(c); err != nil {
			return nil, err
		}
	}
	var spillBudget materialized.SpillBudget
	if len(c.materializedSources) > 0 {
		spillBudget = newMaterializedSpillBudget(c.proc)
	}
	startedSources = make([]*materialized.Source, 0, len(c.materializedSources))
	for _, source := range c.materializedSources {
		if c.allocationAttempt == nil || c.allocationAttempt.account == nil {
			return startedSources, mpool.ErrAllocationAccountInvariant
		}
		if err = source.Begin(c.proc.Mp(), materialized.SpillConfig{FileFactory: func(name string) (*os.File, error) {
			spillFS, spillErr := c.proc.GetSpillFileService()
			if spillErr != nil {
				return nil, spillErr
			}
			return spillFS.CreateAndRemoveFile(c.proc.Ctx, name)
		}, Budget: spillBudget, AllocationAccount: c.allocationAttempt.account}); err != nil {
			return startedSources, err
		}
		startedSources = append(startedSources, source)
	}
	return startedSources, nil
}

func closeMaterializedSourceGenerations(sources []*materialized.Source) {
	for _, source := range sources {
		source.Close()
	}
}

// runPipelineAttempt owns every materialized-source generation opened by its
// initializer. The callback may start no scopes, return an error, or panic;
// after it returns, all submitted scope goroutines have quiesced and the
// attempt closes both executed and statically planned-but-unstarted owners.
func (c *Compile) runPipelineAttempt(run func() error) (err error) {
	startedSources, err := c.prePipelineInitializer()
	defer closeMaterializedSourceGenerations(startedSources)
	if err != nil {
		return err
	}
	return run()
}

func newMaterializedSpillBudget(proc *process.Process) materialized.SpillBudget {
	return materialized.SpillBudget{
		ReserveMemory: func(size uint64) (materialized.Reservation, error) {
			budget, err := proc.GetExecutionResourceBudget()
			if err != nil {
				return nil, hashbuild.TerminalBudgetError(proc.Ctx, err)
			}
			reservation, err := budget.ReserveTransientMemory(size)
			if err != nil {
				return nil, hashbuild.TerminalBudgetError(proc.Ctx, err)
			}
			return reservation, nil
		},
		ReserveDisk: func(size uint64) (materialized.GrowingReservation, error) {
			budget, err := proc.GetExecutionResourceBudget()
			if err != nil {
				return nil, hashbuild.TerminalBudgetError(proc.Ctx, err)
			}
			reservation, err := budget.ReserveSpillDisk(size)
			if err != nil {
				return nil, hashbuild.TerminalBudgetError(proc.Ctx, err)
			}
			return &materializedSpillDiskReservation{
				GrowingReservation: reservation,
				ctx:                proc.Ctx,
			}, nil
		},
		ReserveFD: func(size uint64) (materialized.Reservation, error) {
			budget, err := proc.GetExecutionResourceBudget()
			if err != nil {
				return nil, hashbuild.TerminalBudgetError(proc.Ctx, err)
			}
			reservation, err := budget.ReserveSpillFD(size)
			if err != nil {
				return nil, hashbuild.TerminalBudgetError(proc.Ctx, err)
			}
			return reservation, nil
		},
	}
}

// materializedSpillDiskReservation converts a terminal capacity rejection on
// every growth attempt before the materialized source publishes it to readers.
// The source cannot recover from a rejected spill write, so the raw admission
// error must not escape through either the producer or a dependent consumer.
type materializedSpillDiskReservation struct {
	materialized.GrowingReservation
	ctx context.Context
}

func (r *materializedSpillDiskReservation) Grow(size uint64) error {
	if err := r.GrowingReservation.Grow(size); err != nil {
		return hashbuild.TerminalBudgetError(r.ctx, err)
	}
	return nil
}

// run once
func (c *Compile) runOnce() (err error) {
	//c.printPipeline()

	// defer cleanup at the end of runOnce()
	defer func() {
		// cleanup post dml sql and stage cache
		c.proc.Base.PostDmlSqlList.Clear()
		c.proc.Base.StageCache.Clear()
	}()

	// REPLACE parent checks and actions run before the main pipeline.
	query := c.pn.GetQuery()
	if query != nil && len(query.GetDetectSqls()) != 0 {
		if err = validateForeignKeyParentTxnMode(
			c.proc.Ctx, query, c.proc.GetTxnOperator().Txn().IsPessimistic()); err != nil {
			return err
		}
	}
	if query != nil && query.StmtType == plan.Query_INSERT && len(query.GetDetectSqls()) != 0 {
		for _, sql := range query.DetectSqls {
			if strings.HasPrefix(sql, "REPLACE_PARENT_PLAN:") {
				continue
			} else if strings.HasPrefix(sql, "REPLACE_PARENT_LOCK:") {
				if err = c.runSql(strings.TrimPrefix(sql, "REPLACE_PARENT_LOCK:")); err != nil {
					return err
				}
			} else if strings.HasPrefix(sql, "REPLACE_PARENT_CHK:") {
				if err = runDetectSql(c, strings.TrimPrefix(sql, "REPLACE_PARENT_CHK:")); err != nil {
					// Only translate the "check returned false" signal into the
					// parent-row-referenced error; pass through real execution
					// errors (syntax, permissions, network, txn conflicts) so
					// they are not masked.
					if moerr.IsMoErrCode(err, moerr.ErrFKNoReferencedRow2) {
						return moerr.NewErrFKRowIsReferenced(c.proc.Ctx)
					}
					return err
				}
			} else if strings.HasPrefix(sql, "REPLACE_PARENT_ACTION:") {
				if err = c.runSql(strings.TrimPrefix(sql, "REPLACE_PARENT_ACTION:")); err != nil {
					return err
				}
			}
		}
	}

	// Publish every dispatch receiver that will execute on this CN before any
	// scope goroutine starts. Remote consumers can otherwise notify while a
	// local source is still blocked before vm.Prepare reaches its Dispatch.
	registrations, err := registerLocalDispatchReceivers(c.scopes, c.addr)
	if err != nil {
		return err
	}
	defer registrations.cleanup()

	if c.IsTpQuery() && len(c.scopes) == 1 {
		if err = c.run(c.scopes[0]); err != nil {
			return err
		}
	} else {
		errC := make(chan scopeRunResult, len(c.scopes))
		for i := range c.scopes {
			scope := c.scopes[i]
			errSubmit := ants.Submit(func() {
				defer func() {
					if e := recover(); e != nil {
						err := moerr.ConvertPanicError(c.proc.Ctx, e)
						c.proc.Error(c.proc.Ctx, "panic in run",
							zap.String("sql", commonutil.Abbreviate(c.sql, 500)),
							zap.String("error", err.Error()))
						errC <- newScopeRunResult(err, scope)
					}
				}()
				errC <- newScopeRunResult(c.run(scope), scope)
			})
			if errSubmit != nil {
				errC <- newScopeRunResult(errSubmit, scope)
			}
		}

		var resultToThrowOut scopeRunResult
		for i := 0; i < cap(errC); i++ {
			result := <-errC
			result, _ = result.resolveCancelCause()
			e := result.err

			// cancel this query if the first error occurs.
			if e != nil && resultToThrowOut.err == nil {

				// cancel all scope tree.
				for j := range c.scopes {
					if c.scopes[j].Proc != nil {
						c.scopes[j].Proc.Cancel(e)
					}
				}
			}
			resultToThrowOut = preferPrimaryScopeResult(resultToThrowOut, result)

			// if any error already return is retryable, we should throw this one
			// to make sure query will retry.
			if e != nil && c.isRetryErr(e) {
				resultToThrowOut = result
			}
		}
		close(errC)

		resultToThrowOut, _ = resultToThrowOut.resolveCancelCause()
		if resultToThrowOut.err != nil {
			return resultToThrowOut.err
		}
	}

	for _, sql := range c.proc.Base.PostDmlSqlList.Values() {
		err = c.runSql(sql)
		if err != nil {
			c.debugLogFor19288(err, sql)
			return err
		}
	}

	// fuzzy filter not sure whether this insert / load obey duplicate constraints, need double check
	for _, f := range c.fuzzys {
		if f != nil && f.cnt > 0 {
			if f.cnt > 10 {
				c.proc.Debugf(c.proc.Ctx, "double check dup for `%s`.`%s`:collision cnt is %d, may be too high", f.db, f.tbl, f.cnt)
			}
			err = f.backgroundSQLCheck(c)
			if err != nil {
				return err
			}
		}
	}

	//detect fk self refer
	//update, insert
	query = c.pn.GetQuery()
	if query != nil && (query.StmtType == plan.Query_INSERT ||
		query.StmtType == plan.Query_UPDATE) && len(query.GetDetectSqls()) != 0 {
		// Filter out REPLACE parent-side checks and actions already executed before
		// the main operation.
		var postCheckSqls []string
		for _, sql := range query.DetectSqls {
			if strings.HasPrefix(sql, "REPLACE_PARENT_LOCK:") ||
				strings.HasPrefix(sql, "REPLACE_PARENT_PLAN:") ||
				strings.HasPrefix(sql, "REPLACE_PARENT_CHK:") ||
				strings.HasPrefix(sql, "REPLACE_PARENT_ACTION:") ||
				strings.HasPrefix(sql, "UPDATE_PARENT_PLAN:") {
				continue
			}
			postCheckSqls = append(postCheckSqls, sql)
		}
		err = detectFkSelfRefer(c, postCheckSqls)
	}
	//alter table ... add/drop foreign key
	if err == nil && c.pn.GetDdl() != nil {
		alterTable := c.pn.GetDdl().GetAlterTable()
		if alterTable != nil && len(alterTable.GetDetectSqls()) != 0 {
			err = detectFkSelfRefer(c, alterTable.GetDetectSqls())
		}
	}
	return err
}

func validateForeignKeyParentTxnMode(ctx context.Context, query *plan.Query, pessimistic bool) error {
	if pessimistic || query == nil {
		return nil
	}
	for _, sql := range query.DetectSqls {
		if strings.HasPrefix(sql, "REPLACE_PARENT_LOCK:") ||
			strings.HasPrefix(sql, "REPLACE_PARENT_PLAN:") {
			return moerr.NewNotSupported(ctx,
				"REPLACE on a referenced parent table in optimistic transaction mode")
		}
		if strings.HasPrefix(sql, "UPDATE_PARENT_PLAN:") {
			return moerr.NewNotSupported(ctx,
				"UPDATE on a referenced parent table in optimistic transaction mode")
		}
	}
	return nil
}

// add log to check if background sql return NeedRetry error when origin sql execute successfully
func (c *Compile) debugLogFor19288(err error, bsql string) {
	if c.isRetryErr(err) {
		logutil.Debugf("Origin SQL: %s\nBackground SQL: %s\nTransaction Meta: %v", c.originSQL, bsql, c.proc.GetTxnOperator().Txn())
	}
}

func (c *Compile) compileScope(pn *plan.Plan) ([]*Scope, error) {
	start := time.Now()
	defer func() {
		v2.TxnStatementCompileScopeHistogram.Observe(time.Since(start).Seconds())
	}()
	switch qry := pn.Plan.(type) {
	case *plan.Plan_Query:
		scopes, err := c.compileQuery(qry.Query)
		if err != nil {
			return nil, err
		}
		for _, s := range scopes {
			if s.Plan == nil {
				s.Plan = pn
			}
		}
		return scopes, nil
	case *plan.Plan_Ddl:
		switch qry.Ddl.DdlType {
		case plan.DataDefinition_CREATE_DATABASE:
			return []*Scope{
				newScope(CreateDatabase).
					withPlan(pn),
			}, nil
		case plan.DataDefinition_DROP_DATABASE:
			return []*Scope{
				newScope(DropDatabase).
					withPlan(pn),
			}, nil
		case plan.DataDefinition_CREATE_PITR:
			return []*Scope{
				newScope(CreatePitr).
					withPlan(pn),
			}, nil
		case plan.DataDefinition_CREATE_CDC:
			return []*Scope{
				newScope(CreateCDC).
					withPlan(pn),
			}, nil
		case plan.DataDefinition_CREATE_TABLE:
			return []*Scope{
				newScope(CreateTable).
					withPlan(pn),
			}, nil
		case plan.DataDefinition_CREATE_VIEW:
			return []*Scope{
				newScope(CreateView).
					withPlan(pn),
			}, nil
		case plan.DataDefinition_ALTER_VIEW:
			return []*Scope{
				newScope(AlterView).
					withPlan(pn),
			}, nil
		case plan.DataDefinition_ALTER_TABLE:
			return []*Scope{
				newScope(AlterTable).
					withPlan(pn),
			}, nil
		case plan.DataDefinition_RENAME_TABLE:
			return []*Scope{
				newScope(RenameTable).
					withPlan(pn),
			}, nil
		case plan.DataDefinition_DROP_TABLE:
			return []*Scope{
				newScope(DropTable).
					withPlan(pn),
			}, nil
		case plan.DataDefinition_DROP_PITR:
			return []*Scope{
				newScope(DropPitr).
					withPlan(pn),
			}, nil
		case plan.DataDefinition_DROP_CDC:
			return []*Scope{
				newScope(DropCDC).
					withPlan(pn),
			}, nil
		case plan.DataDefinition_DROP_SEQUENCE:
			return []*Scope{
				newScope(DropSequence).
					withPlan(pn),
			}, nil
		case plan.DataDefinition_ALTER_SEQUENCE:
			return []*Scope{
				newScope(AlterSequence).
					withPlan(pn),
			}, nil
		case plan.DataDefinition_TRUNCATE_TABLE:
			return []*Scope{
				newScope(TruncateTable).
					withPlan(pn),
			}, nil
		case plan.DataDefinition_CREATE_SEQUENCE:
			return []*Scope{
				newScope(CreateSequence).
					withPlan(pn),
			}, nil
		case plan.DataDefinition_CREATE_INDEX:
			return []*Scope{
				newScope(CreateIndex).
					withPlan(pn),
			}, nil
		case plan.DataDefinition_DROP_INDEX:
			return []*Scope{
				newScope(DropIndex).
					withPlan(pn),
			}, nil
		case plan.DataDefinition_SHOW_DATABASES,
			plan.DataDefinition_SHOW_TABLES,
			plan.DataDefinition_SHOW_COLUMNS,
			plan.DataDefinition_SHOW_CREATETABLE:
			return c.compileQuery(pn.GetDdl().GetQuery())
		// 1、not supported: show arnings/errors/status/processlist
		// 2、show variables will not return query
		// 3、show create database/table need rewrite to create sql

		case plan.DataDefinition_CREATE_TABLE_WITH_CLONE:
			return c.compileTableClone(pn)
		}
	}
	return nil, moerr.NewNYI(c.proc.Ctx, fmt.Sprintf("query '%s'", pn))
}

func (c *Compile) appendMetaTables(objRes *plan.ObjectRef) {
	if objRes.NotLockMeta {
		return
	}
	if !c.needLockMeta {
		return
	}
	c.lockMeta.appendMetaTables(objRes)
}

func (c *Compile) lockTable() error {
	tableIDs := make([]uint64, 0, len(c.lockTables))
	for tableID := range c.lockTables {
		tableIDs = append(tableIDs, tableID)
	}
	slices.Sort(tableIDs)
	for _, tableID := range tableIDs {
		tbl := c.lockTables[tableID]
		typ := plan2.MakeTypeByPlan2Type(tbl.PrimaryColTyp)
		if err := lockop.LockTableWithMode(
			c.e,
			c.proc,
			tbl.TableId,
			typ,
			tbl.Mode,
			false); err != nil {
			return err
		}
	}
	return nil
}

func (c *Compile) shouldPrePipelineLockTable(target *plan.LockTarget) bool {
	target.LockTableAtTheEnd = false
	if !target.LockTable {
		return false
	}
	qry := c.pn.GetQuery()
	if qry == nil {
		return true
	}
	// For INSERT statements, pre-run table locking can stretch the target-table
	// lock hold window. Keep the same table-lock semantics by letting LockOp
	// acquire it when the first batch reaches the target pipeline and by
	// falling back to EOF-time table locking if the child produces no rows.
	if qry.StmtType == plan.Query_INSERT {
		// LOAD DATA always plans a table-locking LockOp. Keeping the pre-pipeline
		// lock avoids retrying the same whole-table lock on every non-empty batch.
		if qry.LoadTag {
			return true
		}
		target.LockTableAtTheEnd = true
		return false
	}
	return true
}

// func (c *Compile) compileAttachedScope(attachedPlan *plan.Plan) ([]*Scope, error) {
// 	query := attachedPlan.Plan.(*plan.Plan_Query)
// 	attachedScope, err := c.compileQuery(ctx, query.Query)
// 	if err != nil {
// 		return nil, err
// 	}
// 	for _, s := range attachedScope {
// 		s.Plan = attachedPlan
// 	}
// 	return attachedScope, nil
// }

func (c *Compile) compileQuery(qry *plan.Query) ([]*Scope, error) {
	var err error
	c.foundRowsOwnerNode = c.selectFoundRowsOwnerNode(qry)
	c.compiledLocalRuntimeFilterNodes = nil
	defer func() {
		c.compiledLocalRuntimeFilterNodes = nil
	}()

	start := time.Now()
	defer func() {
		v2.TxnStatementCompileQueryHistogram.Observe(time.Since(start).Seconds())
	}()

	c.execType = plan2.GetExecType(c.pn.GetQuery(), c.getHaveDDL(), c.isPrepare)

	c.cnList, err = c.scheduleQueryWorkers()
	if err != nil {
		return nil, err
	}

	if c.isPrepare && !c.IsTpQuery() {
		return nil, cantCompileForPrepareErr
	}

	ncpu := int32(c.ncpu)
	if qry.MaxDop > 0 {
		ncpu = min(ncpu, int32(qry.MaxDop))
	}

	plan2.CalcQueryDOP(c.pn, ncpu, len(c.cnList), c.execType)

	c.initAnalyzeModule(qry)
	firstStep := c.firstStepToCompile(qry)
	// Deal with sink scans first. A final literal LIMIT 0 has no demand for
	// producer steps; compiling recursive CTE producers would otherwise leave
	// their pipelines waiting for consumers that the LIMIT fast path never builds.
	for i := len(qry.Steps) - 1; i >= firstStep; i-- {
		err := c.compileSinkScan(qry, qry.Steps[i])
		if err != nil {
			return nil, err
		}
	}

	steps := make([]*Scope, 0, len(qry.Steps))
	defer func() {
		if err != nil {
			ReleaseScopes(steps)
		}
	}()
	for i := len(qry.Steps) - 1; i >= firstStep; i-- {
		var scopes []*Scope
		scopes, err = c.compilePlanScope(int32(i), qry.Steps[i], qry.Nodes)
		if err != nil {
			return nil, err
		}
		scopes, err = c.compileSteps(qry, scopes, qry.Steps[i])
		if err != nil {
			return nil, err
		}
		steps = append(steps, scopes...)
	}
	if err = validateLocalRuntimeFilterTopology(qry, c.compiledLocalRuntimeFilterNodes, steps); err != nil {
		return nil, err
	}

	return steps, err
}

func (c *Compile) compileSinkScan(qry *plan.Query, nodeId int32) error {
	n := qry.Nodes[nodeId]
	for _, childId := range n.Children {
		err := c.compileSinkScan(qry, childId)
		if err != nil {
			return err
		}
	}

	if n.NodeType == plan.Node_SINK_SCAN || n.NodeType == plan.Node_RECURSIVE_SCAN || n.NodeType == plan.Node_RECURSIVE_CTE {
		for _, s := range n.SourceStep {
			var edge *process.PipelineEdge
			if c.anal.qry.LoadTag {
				edge = process.NewPipelineEdge(int(c.ncpu), 0)
			} else {
				edge = process.NewPipelineEdge(1, 0)
			}
			c.appendStepRegs(s, nodeId, edge)
			if n.NodeType == plan.Node_SINK_SCAN && len(n.SourceStep) == 1 &&
				c.isMaterializedCTEStep(qry, s) {
				if c.materializedSinkScanNodes == nil {
					c.materializedSinkScanNodes = make(map[int32][]int32)
				}
				if c.materializedReaderIDs == nil {
					c.materializedReaderIDs = make(map[[2]int32]int)
				}
				readerID := len(c.materializedSinkScanNodes[s])
				c.materializedSinkScanNodes[s] = append(c.materializedSinkScanNodes[s], nodeId)
				c.materializedReaderIDs[[2]int32{s, nodeId}] = readerID
			}
		}
	}
	return nil
}

func (c *Compile) isMaterializedCTEStep(qry *plan.Query, step int32) bool {
	if qry == nil || step < 0 || int(step) >= len(qry.Steps) {
		return false
	}
	nodeID := qry.Steps[step]
	if nodeID < 0 || int(nodeID) >= len(qry.Nodes) {
		return false
	}
	sink := qry.Nodes[nodeID]
	return sink.NodeType == plan.Node_SINK && !sink.RecursiveSink && !sink.RecursiveCte &&
		sink.ExtraOptions == materialized.CTESinkOption
}

func (c *Compile) isAdaptiveVectorSearch(qry *plan.Query) bool {
	if qry == nil {
		return false
	}

	for _, node := range qry.Nodes {
		// Check for vector search in auto mode
		if node.RankOption != nil && node.RankOption.Mode == "auto" {
			return true
		}
	}

	return false
}

func (c *Compile) compileSteps(qry *plan.Query, ss []*Scope, step int32) ([]*Scope, error) {
	if qry.Nodes[step].NodeType == plan.Node_SINK {
		return ss, nil
	}

	switch qry.StmtType {
	case plan.Query_DELETE, plan.Query_INSERT, plan.Query_UPDATE, plan.Query_MERGE:
		if !qry.HasReturning || qry.ReturningStep < 0 || int(qry.ReturningStep) >= len(qry.Steps) || qry.Steps[qry.ReturningStep] != step {
			updateScopesLastFlag(ss)
			return ss, nil
		}
		fallthrough
	default:
		var rs *Scope
		if c.IsSingleScope(ss) {
			// Output owns a callback created by this Compile and cannot be
			// serialized for execution on another CN. Keep the result sink on
			// its owner and return the remote child through the existing
			// connector/merge path.
			if ss[0].Magic == Remote && !ss[0].ipAddrMatch(c.addr) {
				rs = c.newMergeScope(ss)
			} else {
				rs = ss[0]
			}
		} else {
			ss = c.mergeShuffleScopesIfNeeded(ss, false)
			rs = c.newMergeScope(ss)
		}
		updateScopesLastFlag([]*Scope{rs})
		c.setAnalyzeCurrent([]*Scope{rs}, c.anal.curNodeIdx)
		// sql_select_limit belongs to a client session. Background and internal
		// SQL use the default (unlimited) behavior even if they happen to carry a
		// variable resolver from the calling session.
		if qry.ApplySqlSelectLimit &&
			c.proc.Base.SessionInfo.ApplySQLSelectLimit &&
			c.proc.GetResolveVariableFunc() != nil {
			limitExpr, err := c.makeSQLSelectLimitExpr()
			if err != nil {
				// compileSteps owns rs at this point. A merge scope owns all of
				// its input scopes through PreScopes, so releasing this root also
				// releases the complete compiled tree exactly once.
				ReleaseScopes([]*Scope{rs})
				return nil, err
			}
			if limitExpr != nil {
				limitNode := &plan.Node{Limit: limitExpr}
				drainForFoundRows := false
				if statementHasSQLCalcFoundRows(c.stmt) {
					if c.foundRowsOwnerNode == nil {
						c.foundRowsOwnerNode = limitNode
					} else {
						// An explicit top-level OFFSET remains the owner of the
						// pre-offset count. The dynamic prepared session limit is
						// above it, so it must drain without publishing; otherwise
						// it stops before the OFFSET observes EOF.
						drainForFoundRows = true
					}
				}
				rs = c.compileLimitWithFoundRowsDrain(limitNode, []*Scope{rs}, drainForFoundRows)[0]
			}
		}

		isAdaptive := c.isAdaptiveVectorSearch(qry)

		rs.setRootOperator(
			output.NewArgument().
				WithFunc(c.resultWriter()).
				WithBlock(c.needBlock).
				WithAdaptive(isAdaptive),
		)
		return []*Scope{rs}, nil
	}
}

type sqlSelectLimitMaterialization struct {
	query         *plan.Query
	root          *plan.Node
	originalLimit *plan.Expr
}

func (m sqlSelectLimitMaterialization) restore() {
	if m.query == nil {
		return
	}
	m.query.ApplySqlSelectLimit = true
	if m.root != nil {
		m.root.Limit = m.originalLimit
	}
}

// materializeSQLSelectLimit resolves an ordinary statement's session limit at
// the post-optimizer compile boundary. A finite limit temporarily becomes part
// of the final logical root so Sirius export and native physical compilation
// share it. The returned token restores the dynamic cached-plan marker after
// those consumers have finished reading the plan.
func (c *Compile) materializeSQLSelectLimit(queryPlan *plan.Plan) (sqlSelectLimitMaterialization, error) {
	if c == nil || c.isPrepare || queryPlan == nil {
		return sqlSelectLimitMaterialization{}, nil
	}
	qry := queryPlan.GetQuery()
	if qry == nil || !qry.ApplySqlSelectLimit ||
		!c.proc.Base.SessionInfo.ApplySQLSelectLimit ||
		c.proc.GetResolveVariableFunc() == nil {
		return sqlSelectLimitMaterialization{}, nil
	}

	limitExpr, err := c.makeSQLSelectLimitExpr()
	if err != nil {
		return sqlSelectLimitMaterialization{}, err
	}
	// Resolution succeeded, so compileSteps must not add a second limit.
	materialization := sqlSelectLimitMaterialization{query: qry}
	qry.ApplySqlSelectLimit = false
	if limitExpr == nil || len(qry.Steps) == 0 {
		return materialization, nil
	}
	finalStep := qry.Steps[len(qry.Steps)-1]
	if finalStep < 0 || int(finalStep) >= len(qry.Nodes) || qry.Nodes[finalStep] == nil {
		// Leave malformed-plan reporting to the existing compile/export
		// validation rather than panicking at this optional materialization.
		return materialization, nil
	}
	// A false marker is the normal representation of an explicit LIMIT. Keep
	// the defensive check so an inconsistent plan still preserves SQL syntax.
	if qry.Nodes[finalStep].Limit == nil {
		materialization.root = qry.Nodes[finalStep]
		materialization.originalLimit = materialization.root.Limit
		materialization.root.Limit = limitExpr
	}
	return materialization, nil
}

// makeSQLSelectLimitExpr keeps prepared pipelines dynamic because they are
// reused across EXECUTEs. Ordinary statements are compiled for one execution,
// so resolve their value once and omit the default unlimited no-op entirely.
func (c *Compile) makeSQLSelectLimitExpr() (*plan.Expr, error) {
	if c.isPrepare {
		return plan2.MakeSQLSelectLimitExpr(c.proc.Ctx)
	}

	value, err := c.proc.GetResolveVariableFunc()(plan2.SQLSelectLimitVariable, true, false)
	if err != nil {
		return nil, err
	}
	limit, ok := value.(uint64)
	if !ok {
		return nil, moerr.NewInternalErrorf(c.proc.Ctx,
			"unexpected %s type %T", plan2.SQLSelectLimitVariable, value)
	}
	if limit == ^uint64(0) {
		return nil, nil
	}
	return plan2.MakePlan2Uint64ConstExprWithType(limit), nil
}

func streamingUnionAllDemand(node *plan.Node, outerDemand bool) bool {
	if node == nil || len(node.OrderBy) > 0 || node.FilterIsBarrier ||
		nodeHasUserLevelLockFunction(node) {
		return false
	}
	switch node.NodeType {
	case plan.Node_PROJECT, plan.Node_FILTER, plan.Node_SORT, plan.Node_UNION_ALL:
	default:
		return false
	}
	return outerDemand || node.Limit != nil
}

// orderedScalarUnionAll reports whether node is a UNION ALL tree whose leaves
// are the single-row, tableless PROJECT -> VALUE_SCAN shape produced for
// statements such as "SELECT 3 UNION ALL SELECT 1". Connector/ODBC uses this
// shape to execute parameter arrays and maps returned rows back to parameter
// sets by branch position, matching MySQL's left-to-right result order.
//
// Keep the recognition deliberately narrow. General UNION ALL inputs retain
// their concurrent topology; only cheap scalar branches use sequential branch
// activation to make that compatibility order deterministic.
func orderedScalarUnionAll(nodeIdx int32, nodes []*plan.Node) bool {
	if nodeIdx < 0 || int(nodeIdx) >= len(nodes) || nodes[nodeIdx] == nil {
		return false
	}

	node := nodes[nodeIdx]
	switch node.NodeType {
	case plan.Node_UNION_ALL:
		return len(node.Children) == 2 &&
			orderedScalarUnionAll(node.Children[0], nodes) &&
			orderedScalarUnionAll(node.Children[1], nodes)
	case plan.Node_PROJECT:
		if len(node.Children) != 1 {
			return false
		}
		childIdx := node.Children[0]
		if childIdx < 0 || int(childIdx) >= len(nodes) || nodes[childIdx] == nil {
			return false
		}
		child := nodes[childIdx]
		return child.NodeType == plan.Node_VALUE_SCAN &&
			len(child.Children) == 0 && child.RowsetData == nil && child.TableDef == nil
	default:
		return false
	}
}

// orderedScalarUnionAllResult keeps the compatibility behavior at the result
// boundary. A scalar UNION ALL used as an input to a join or another blocking
// operator must retain the normal concurrent topology: making that input lazy
// can leave its consumer waiting for a branch that has not been started yet.
func orderedScalarUnionAllResult(step, nodeIdx int32, qry *plan.Query) bool {
	if qry == nil || step < 0 || int(step) >= len(qry.Steps) ||
		nodeIdx < 0 || int(nodeIdx) >= len(qry.Nodes) {
		return false
	}

	rootIdx := qry.Steps[step]
	if rootIdx < 0 || int(rootIdx) >= len(qry.Nodes) || qry.Nodes[rootIdx] == nil {
		return false
	}
	root := qry.Nodes[rootIdx]
	return root.NodeType == plan.Node_PROJECT && len(root.Children) == 1 &&
		root.Children[0] == nodeIdx && orderedScalarUnionAll(nodeIdx, qry.Nodes)
}

func (c *Compile) compilePlanScope(step int32, curNodeIdx int32, nodes []*plan.Node) ([]*Scope, error) {
	return c.compilePlanScopeWithUnionAllDemand(step, curNodeIdx, nodes, false)
}

// compilePlanScopeWithUnionAllDemand carries an outer streaming LIMIT demand
// down to UNION ALL before its physical scopes are built. This lets eligible
// unions choose their lazy topology during construction while leaving ordinary
// UNION ALL plans on their original concurrent topology.
func (c *Compile) compilePlanScopeWithUnionAllDemand(
	step int32,
	curNodeIdx int32,
	nodes []*plan.Node,
	outerUnionAllDemand bool,
) ([]*Scope, error) {
	start := time.Now()
	defer func() {
		v2.TxnStatementCompilePlanScopeHistogram.Observe(time.Since(start).Seconds())
	}()
	var ss []*Scope
	var left []*Scope
	var right []*Scope
	var err error
	defer func() {
		if err != nil {
			ReleaseScopes(ss)
			ReleaseScopes(left)
			ReleaseScopes(right)
		}
	}()
	node := nodes[curNodeIdx]

	if c.canUseLiteralLimitZeroFastPath(node) {
		// optimize for limit 0
		rs := c.newEmptyMergeScope()
		rs.Proc = c.proc.NewNoContextChildProc(0)
		return c.compileLimit(node, []*Scope{rs}), nil
	}

	if nodeHasLocalRuntimeFilter(node) {
		// This is deliberately after the literal LIMIT 0 shortcut. The flat
		// logical plan retains pruned descendants, while topology validation must
		// cover only local-filter nodes whose physical subtree was constructed.
		c.compiledLocalRuntimeFilterNodes = append(c.compiledLocalRuntimeFilterNodes, curNodeIdx)
	}

	switch node.NodeType {
	case plan.Node_VALUE_SCAN:
		c.setAnalyzeCurrent(nil, int(curNodeIdx))
		ss, err = c.compileValueScan(node)
		if err != nil {
			return nil, err
		}
		ss = c.ensureCoordinatorOnlyFunctions(node, ss)
		ss = c.compileSort(node, c.compileProjection(node, ss))
		return ss, nil
	case plan.Node_EXTERNAL_SCAN:
		if node.ObjRef != nil {
			c.appendMetaTables(node.ObjRef)
		}
		nodeCopy := plan2.DeepCopyNode(node)

		c.setAnalyzeCurrent(nil, int(curNodeIdx))
		ss, err = c.compileExternScanWithPlanNodeID(nodeCopy, curNodeIdx)
		if err != nil {
			return nil, err
		}
		ss = c.ensureCoordinatorOnlyFunctions(nodeCopy, ss)
		ss = c.compileSort(node, c.compileProjection(node, c.compileRestrict(nodeCopy, ss)))
		return ss, nil
	case plan.Node_TABLE_SCAN:
		c.appendMetaTables(node.ObjRef)

		c.setAnalyzeCurrent(nil, int(curNodeIdx))
		ss, err = c.compileTableScan(node)
		if err != nil {
			return nil, err
		}
		ss = c.compileTableScanFiltersAndProjection(node, ss)

		if node.Offset != nil {
			ss = c.compileOffset(node, ss)
		}
		if node.Limit != nil {
			ss = c.compileLimit(node, ss)
		}
		return ss, nil
	case plan.Node_VECTOR_INDEX_SCAN:
		c.appendMetaTables(node.ObjRef)

		c.setAnalyzeCurrent(nil, int(curNodeIdx))
		ss, err = c.compileVectorIndexScan(node)
		if err != nil {
			return nil, err
		}
		ss = c.compileTableScanFiltersAndProjection(node, ss)
		return ss, nil
	case plan.Node_FILTER, plan.Node_ASSERT, plan.Node_PROJECT:
		childDemand := streamingUnionAllDemand(node, outerUnionAllDemand)
		ss, err = c.compilePlanScopeWithUnionAllDemand(step, node.Children[0], nodes, childDemand)
		if err != nil {
			return nil, err
		}
		ss = c.ensureCoordinatorOnlyFunctions(node, ss)

		c.setAnalyzeCurrent(ss, int(curNodeIdx))
		ss = c.compileRestrict(node, ss)
		semanticBoundary := node.NodeType == plan.Node_ASSERT || node.FilterIsBarrier
		if !semanticBoundary ||
			!isIdentityProjectionOfChild(node.ProjectList, nodes[node.Children[0]].ProjectList) {
			ss = c.compileProjection(node, ss)
		}
		ss = c.compileSort(node, ss)
		return ss, nil
	case plan.Node_AGG:
		childNodeID := node.Children[0]
		childNode := nodes[childNodeID]
		if isLocalPreAggregationGroup(node, childNode) {
			ss, err = c.compileLocalPreAggregationScope(step, childNodeID, nodes)
		} else {
			ss, err = c.compilePlanScope(step, childNodeID, nodes)
		}
		if err != nil {
			return nil, err
		}
		groupInfo := constructGroup(c.proc.Ctx, node, childNode, false, 0, c.proc)
		defer groupInfo.Release()
		distinctRequiresSingleStage := plan2.RequiresSingleStageDistinctAgg(node)

		c.setAnalyzeCurrent(ss, int(curNodeIdx))
		ss = c.ensureCoordinatorOnlyFunctions(node, ss)
		if c.canCompileShuffleGroup(node) {
			ss = c.compileSort(node, c.compileProjection(node, c.compileRestrict(node, c.compileShuffleGroup(node, ss, nodes))))
			return ss, nil
		}
		ss = c.compileGroupWithoutShuffle(
			node, ss, nodes, distinctRequiresSingleStage)
		ss = c.compileSort(node, c.compileProjection(node, c.compileRestrict(node, ss)))
		return ss, nil
	case plan.Node_SAMPLE:
		ss, err = c.compilePlanScope(step, node.Children[0], nodes)
		if err != nil {
			return nil, err
		}

		c.setAnalyzeCurrent(ss, int(curNodeIdx))
		ss = c.ensureCoordinatorOnlyFunctions(node, ss)
		ss = c.compileSort(node, c.compileProjection(node, c.compileRestrict(node, c.compileSample(node, ss))))
		return ss, nil
	case plan.Node_WINDOW:
		ss, err = c.compilePlanScope(step, node.Children[0], nodes)
		if err != nil {
			return nil, err
		}

		c.setAnalyzeCurrent(ss, int(curNodeIdx))
		ss = c.ensureCoordinatorOnlyFunctions(node, ss)
		ss = c.compileSort(node, c.compileProjection(node, c.compileRestrict(node, c.compileWin(node, ss))))
		return ss, nil
	case plan.Node_TIME_WINDOW:
		ss, err = c.compilePlanScope(step, node.Children[0], nodes)
		if err != nil {
			return nil, err
		}

		c.setAnalyzeCurrent(ss, int(curNodeIdx))
		ss = c.ensureCoordinatorOnlyFunctions(node, ss)
		ss = c.compileProjection(node, c.compileRestrict(node, c.compileTimeWin(node, c.compileSort(node, ss))))
		return ss, nil
	case plan.Node_FILL:
		ss, err = c.compilePlanScope(step, node.Children[0], nodes)
		if err != nil {
			return nil, err
		}

		c.setAnalyzeCurrent(ss, int(curNodeIdx))
		ss = c.ensureCoordinatorOnlyFunctions(node, ss)
		ss = c.compileProjection(node, c.compileRestrict(node, c.compileFill(node, ss)))
		return ss, nil
	case plan.Node_JOIN:
		left, err = c.compilePlanScope(step, node.Children[0], nodes)
		if err != nil {
			return nil, err
		}
		right, err = c.compilePlanScope(step, node.Children[1], nodes)
		if err != nil {
			return nil, err
		}

		c.setAnalyzeCurrent(left, int(curNodeIdx))
		c.setAnalyzeCurrent(right, int(curNodeIdx))
		left = c.ensureCoordinatorOnlyFunctions(node, left)
		right = c.ensureCoordinatorOnlyFunctions(node, right)
		ss = c.compileSort(node, c.compileJoin(node, nodes[node.Children[0]], nodes[node.Children[1]], left, right))
		return ss, nil
	case plan.Node_SORT:
		ss, err = c.compilePlanScopeWithUnionAllDemand(
			step,
			node.Children[0],
			nodes,
			streamingUnionAllDemand(node, outerUnionAllDemand),
		)
		if err != nil {
			return nil, err
		}

		c.setAnalyzeCurrent(ss, int(curNodeIdx))
		ss = c.ensureCoordinatorOnlyFunctions(node, ss)
		ss = c.compileProjection(node, c.compileRestrict(node, c.compileSort(node, ss)))
		return ss, nil
	case plan.Node_PARTITION:
		ss, err = c.compilePlanScope(step, node.Children[0], nodes)
		if err != nil {
			return nil, err
		}

		c.setAnalyzeCurrent(ss, int(curNodeIdx))
		ss = c.ensureCoordinatorOnlyFunctions(node, ss)
		ss = c.compileProjection(node, c.compileRestrict(node, c.compilePartition(node, ss)))
		return ss, nil
	case plan.Node_UNION:
		left, err = c.compilePlanScope(step, node.Children[0], nodes)
		if err != nil {
			return nil, err
		}
		right, err = c.compilePlanScope(step, node.Children[1], nodes)
		if err != nil {
			return nil, err
		}

		c.setAnalyzeCurrent(left, int(curNodeIdx))
		c.setAnalyzeCurrent(right, int(curNodeIdx))
		ss = c.compileUnion(node, left, right)
		ss = c.ensureCoordinatorOnlyFunctions(node, ss)
		ss = c.compileSort(node, ss)
		return ss, nil
	case plan.Node_MINUS, plan.Node_INTERSECT, plan.Node_INTERSECT_ALL:
		left, err = c.compilePlanScope(step, node.Children[0], nodes)
		if err != nil {
			return nil, err
		}
		right, err = c.compilePlanScope(step, node.Children[1], nodes)
		if err != nil {
			return nil, err
		}

		c.setAnalyzeCurrent(left, int(curNodeIdx))
		c.setAnalyzeCurrent(right, int(curNodeIdx))
		ss = c.compileMinusAndIntersect(node, left, right, node.NodeType)
		ss = c.ensureCoordinatorOnlyFunctions(node, ss)
		ss = c.compileSort(node, ss)
		return ss, nil
	case plan.Node_UNION_ALL:
		lazy := streamingUnionAllDemand(node, outerUnionAllDemand)
		if !lazy && c.pn != nil {
			lazy = orderedScalarUnionAllResult(step, curNodeIdx, c.pn.GetQuery())
		}
		left, err = c.compilePlanScopeWithUnionAllDemand(step, node.Children[0], nodes, lazy)
		if err != nil {
			return nil, err
		}
		right, err = c.compilePlanScopeWithUnionAllDemand(step, node.Children[1], nodes, lazy)
		if err != nil {
			return nil, err
		}

		c.setAnalyzeCurrent(left, int(curNodeIdx))
		c.setAnalyzeCurrent(right, int(curNodeIdx))
		ss = c.compileUnionAll(node, left, right, lazy)
		ss = c.ensureCoordinatorOnlyFunctions(node, ss)
		ss = c.compileSort(node, ss)
		return ss, nil
	case plan.Node_DELETE:
		// Check if target table is a CCPR shared table (from publication)
		if node.DeleteCtx != nil && c.shouldBlockCCPRReadOnly(node.DeleteCtx.TableDef) {
			return nil, moerr.NewCCPRReadOnly(c.proc.Ctx)
		}
		if node.DeleteCtx.CanTruncate {
			s := newScope(TruncateTable)
			s.Plan = &plan.Plan{
				Plan: &plan.Plan_Ddl{
					Ddl: &plan.DataDefinition{
						DdlType: plan.DataDefinition_TRUNCATE_TABLE,
						Definition: &plan.DataDefinition_TruncateTable{
							TruncateTable: node.DeleteCtx.TruncateTable,
						},
					},
				},
			}
			ss = []*Scope{s}
			return ss, nil
		}
		c.appendMetaTables(node.DeleteCtx.Ref)
		ss, err = c.compilePlanScope(step, node.Children[0], nodes)
		if err != nil {
			return nil, err
		}

		node.NotCacheable = true
		c.setAnalyzeCurrent(ss, int(curNodeIdx))
		return c.compileDelete(node, ss)
	case plan.Node_FUZZY_FILTER:
		left, err = c.compilePlanScope(step, node.Children[0], nodes)
		if err != nil {
			return nil, err
		}
		right, err = c.compilePlanScope(step, node.Children[1], nodes)
		if err != nil {
			return nil, err
		}

		c.setAnalyzeCurrent(left, int(curNodeIdx))
		left = c.ensureCoordinatorOnlyFunctions(node, left)
		right = c.ensureCoordinatorOnlyFunctions(node, right)
		c.setAnalyzeCurrent(right, int(curNodeIdx))
		return c.compileFuzzyFilter(node, nodes, left, right)
	case plan.Node_PRE_INSERT_UK:
		ss, err = c.compilePlanScope(step, node.Children[0], nodes)
		if err != nil {
			return nil, err
		}

		c.setAnalyzeCurrent(ss, int(curNodeIdx))
		ss = c.compilePreInsertUk(node, ss)
		return ss, nil
	case plan.Node_PRE_INSERT_SK:
		ss, err = c.compilePlanScope(step, node.Children[0], nodes)
		if err != nil {
			return nil, err
		}
		c.setAnalyzeCurrent(ss, int(curNodeIdx))
		ss = c.compilePreInsertSK(node, ss)
		return ss, nil
	case plan.Node_PRE_INSERT:
		ss, err = c.compilePlanScope(step, node.Children[0], nodes)
		if err != nil {
			return nil, err
		}

		c.setAnalyzeCurrent(ss, int(curNodeIdx))
		return c.compilePreInsert(nodes, node, ss)
	case plan.Node_INSERT:
		// Check if target table is a CCPR shared table (from publication)
		if node.InsertCtx != nil && c.shouldBlockCCPRReadOnly(node.InsertCtx.TableDef) {
			return nil, moerr.NewCCPRReadOnly(c.proc.Ctx)
		}
		c.appendMetaTables(node.ObjRef)
		ss, err = c.compilePlanScope(step, node.Children[0], nodes)
		if err != nil {
			return nil, err
		}

		node.NotCacheable = true
		c.setAnalyzeCurrent(ss, int(curNodeIdx))
		return c.compileInsert(nodes, node, ss)
	case plan.Node_MULTI_UPDATE:
		// Check if any target table is a CCPR shared table (from publication)
		for _, updateCtx := range node.UpdateCtxList {
			if c.shouldBlockCCPRReadOnly(updateCtx.TableDef) {
				return nil, moerr.NewCCPRReadOnly(c.proc.Ctx)
			}
			c.appendMetaTables(updateCtx.ObjRef)
		}
		ss, err = c.compilePlanScope(step, node.Children[0], nodes)
		if err != nil {
			return nil, err
		}

		node.NotCacheable = true
		c.setAnalyzeCurrent(ss, int(curNodeIdx))
		return c.compileMultiUpdate(node, ss)
	case plan.Node_LOCK_OP:
		ss, err = c.compilePlanScope(step, node.Children[0], nodes)
		if err != nil {
			return nil, err
		}

		c.setAnalyzeCurrent(ss, int(curNodeIdx))
		ss = c.ensureCoordinatorOnlyFunctions(node, ss)
		ss, err = c.compileLock(node, ss)
		if err != nil {
			return nil, err
		}
		ss = c.compileProjection(node, ss)
		return ss, nil
	case plan.Node_FUNCTION_SCAN:
		if len(node.Children) != 0 {
			ss, err = c.compilePlanScope(step, node.Children[0], nodes)
			if err != nil {
				return nil, err
			}
		}

		c.setAnalyzeCurrent(ss, int(curNodeIdx))
		ss, err = c.compileTableFunction(node, ss)
		if err != nil {
			return nil, err
		}
		ss = c.ensureCoordinatorOnlyFunctions(node, ss)
		ss = c.compileSort(node, c.compileProjection(node, c.compileRestrict(node, ss)))
		return ss, nil
	case plan.Node_SINK_SCAN:
		c.setAnalyzeCurrent(nil, int(curNodeIdx))
		ss, err = c.compileSinkScanNode(node, curNodeIdx)
		if err != nil {
			return nil, err
		}
		ss = c.ensureCoordinatorOnlyFunctions(node, ss)
		ss = c.compileProjection(node, ss)
		return ss, nil
	case plan.Node_RECURSIVE_SCAN:
		c.setAnalyzeCurrent(ss, int(curNodeIdx))
		return c.compileRecursiveScan(node, curNodeIdx)
	case plan.Node_RECURSIVE_CTE:
		c.setAnalyzeCurrent(ss, int(curNodeIdx))
		ss, err = c.compileRecursiveCte(node, curNodeIdx)
		if err != nil {
			return nil, err
		}
		ss = c.compileSort(node, ss)
		return ss, nil
	case plan.Node_SINK:
		ss, err = c.compilePlanScope(step, node.Children[0], nodes)
		if err != nil {
			return nil, err
		}

		c.setAnalyzeCurrent(ss, int(curNodeIdx))
		return c.compileSinkNode(node, ss, step)
	case plan.Node_APPLY:
		left, err = c.compilePlanScope(step, node.Children[0], nodes)
		if err != nil {
			return nil, err
		}

		c.setAnalyzeCurrent(left, int(curNodeIdx))
		left = c.ensureCoordinatorOnlyFunctions(node, left)
		ss = c.compileSort(node, c.compileApply(node, nodes[node.Children[1]], left))
		return ss, nil
	case plan.Node_POSTDML:
		ss, err = c.compilePlanScope(step, node.Children[0], nodes)
		if err != nil {
			return nil, err
		}

		c.setAnalyzeCurrent(ss, int(curNodeIdx))
		ss = c.ensureCoordinatorOnlyFunctions(node, ss)
		ss = c.compilePostDml(node, ss)
		return ss, nil

	default:
		return nil, moerr.NewNYI(c.proc.Ctx, fmt.Sprintf("query '%s'", node))
	}
}

func (c *Compile) firstStepToCompile(qry *plan.Query) int {
	if qry == nil || len(qry.Steps) == 0 {
		return 0
	}
	finalStep := qry.Steps[len(qry.Steps)-1]
	if finalStep >= 0 && int(finalStep) < len(qry.Nodes) && c.canUseLiteralLimitZeroFastPath(qry.Nodes[finalStep]) {
		return len(qry.Steps) - 1
	}
	return 0
}

func (c *Compile) canUseLiteralLimitZeroFastPath(node *plan.Node) bool {
	if node == nil || node.Limit == nil || c.ownsFoundRows(node) {
		return false
	}
	cExpr, ok := node.Limit.Expr.(*plan.Expr_Lit)
	if !ok || cExpr.Lit == nil {
		return false
	}
	cval, ok := cExpr.Lit.Value.(*plan.Literal_U64Val)
	return ok && cval.U64Val == 0
}

func (c *Compile) ownsFoundRows(node *plan.Node) bool {
	return statementHasSQLCalcFoundRows(c.stmt) && node != nil && node == c.foundRowsOwnerNode
}

// selectFoundRowsOwnerNode designates only pagination that belongs to the
// statement's final result. An explicit top-level LIMIT/OFFSET is identified
// from the AST before looking through projection wrappers. A finite ordinary
// sql_select_limit is identified by the exact node materialized by Compile.
// Prepared sql_select_limit remains dynamic and is assigned later by
// compileSteps. Nested semantic pagination must never become the owner.
func (c *Compile) selectFoundRowsOwnerNode(qry *plan.Query) *plan.Node {
	if c == nil || qry == nil || !statementHasSQLCalcFoundRows(c.stmt) || len(qry.Steps) == 0 {
		return nil
	}

	rootID := qry.Steps[len(qry.Steps)-1]
	if rootID < 0 || int(rootID) >= len(qry.Nodes) {
		return nil
	}
	if statementHasSQLCalcFoundRowsPagination(c.stmt) {
		return findFoundRowsOwnerNode(qry, rootID)
	}

	root := qry.Nodes[rootID]
	if root != nil && root == c.materializedSQLSelectLimitOwner {
		return root
	}
	return nil
}

func findFoundRowsOwnerNode(qry *plan.Query, rootID int32) *plan.Node {
	if qry == nil || rootID < 0 || int(rootID) >= len(qry.Nodes) {
		return nil
	}

	queue := []int32{rootID}
	visited := make(map[int32]struct{})
	for len(queue) > 0 {
		nodeID := queue[0]
		queue = queue[1:]
		if _, ok := visited[nodeID]; ok || nodeID < 0 || int(nodeID) >= len(qry.Nodes) {
			continue
		}
		visited[nodeID] = struct{}{}

		node := qry.Nodes[nodeID]
		if node == nil {
			continue
		}
		if node.Limit != nil || node.Offset != nil {
			return node
		}
		queue = append(queue, node.Children...)
	}
	return nil
}

func isIdentityProjectionOfChild(projectList, childProjectList []*plan.Expr) bool {
	if len(projectList) == 0 || len(projectList) != len(childProjectList) {
		return false
	}
	for i, expr := range projectList {
		col := expr.GetCol()
		if col == nil || col.RelPos != 0 || col.ColPos != int32(i) {
			return false
		}
	}
	return true
}

func (c *Compile) appendStepRegs(step, nodeId int32, reg *process.WaitRegister) {
	c.nodeRegs[[2]int32{step, nodeId}] = reg
	c.stepRegs[step] = append(c.stepRegs[step], [2]int32{step, nodeId})
}

func (c *Compile) getNodeReg(step, nodeId int32) *process.WaitRegister {
	return c.nodeRegs[[2]int32{step, nodeId}]
}

func (c *Compile) getStepRegs(step int32) []*process.WaitRegister {
	wrs := make([]*process.WaitRegister, len(c.stepRegs[step]))
	for i, sn := range c.stepRegs[step] {
		wrs[i] = c.nodeRegs[sn]
	}
	return wrs
}

func (c *Compile) getMaterializedSource(step int32) *materialized.Source {
	if c.materializedSinkScanNodes == nil {
		return nil
	}
	readers := c.materializedSinkScanNodes[step]
	if len(readers) < 2 {
		return nil
	}
	if source := c.materializedSources[step]; source != nil {
		return source
	}
	source := materialized.NewSource(len(readers))
	if c.materializedSources == nil {
		c.materializedSources = make(map[int32]*materialized.Source)
	}
	c.materializedSources[step] = source
	return source
}

func (c *Compile) constructScopeForExternal(addr string, parallel bool) *Scope {
	ds := newScope(Merge)
	ds.NodeInfo = getEngineNode(c)
	if parallel {
		ds.Magic = Remote
	} else {
		ds.NodeInfo.Mcpu = 1
	}
	ds.NodeInfo.Addr = addr
	ds.Proc = c.proc.NewNoContextChildProc(0)
	c.proc.Base.LoadTag = c.anal.qry.LoadTag
	ds.Proc.Base.LoadTag = true
	ds.DataSource = &Source{isConst: true}
	return ds
}

func (c *Compile) constructScopeForExternalNode(node engine.Node, parallel bool) *Scope {
	scope := c.constructScopeForExternal(node.Addr, parallel)
	scope.NodeInfo.Id = node.Id
	scope.NodeInfo.WorkState = node.WorkState
	return scope
}

func (c *Compile) constructLoadMergeScope() *Scope {
	ds := c.newEmptyMergeScope()
	ds.Proc = c.proc.NewNoContextChildProc(1)
	ds.Proc.Base.LoadTag = true
	arg := merge.NewArgument()
	c.hasMergeOp = true
	arg.SetAnalyzeControl(c.anal.curNodeIdx, false)

	ds.setRootOperator(arg)
	return ds
}

func StrictSqlMode(proc *process.Process) (error, bool) {
	mode, err := resolveVariableOrDefault(proc, "sql_mode", true, false)
	if err != nil {
		return err, false
	}
	return nil, process.IsStrictMode(mode)
}

func effectiveExternalStrictMode(proc *process.Process, param *tree.ExternParam, strict bool) bool {
	if !strict || proc == nil || param == nil || param.ExternType != int32(plan.ExternType_LOAD) {
		return strict
	}
	return !proc.GetStmtProfile().GetStatementIgnore()
}

func (c *Compile) getExternParam(proc *process.Process, externScan *plan.ExternScan, createsql string) (*tree.ExternParam, error) {
	param := &tree.ExternParam{}
	if externScan.LoadType == tree.INLINE {
		param.ScanType = int(externScan.LoadType)
		param.Data = externScan.Data
		param.Format = externScan.Format
		param.Tail = new(tree.TailParameter)
		param.Tail.IgnoredLines = externScan.IgnoredLines
		param.Tail.Fields = &tree.Fields{
			Terminated: &tree.Terminated{
				Value: externScan.Terminated,
			},
			EnclosedBy: &tree.EnclosedBy{
				Value: externScan.EnclosedBy[0],
			},
			EscapedBy: &tree.EscapedBy{
				Value: externScan.EscapedBy[0],
			},
		}
		param.JsonData = externScan.JsonType
	} else {
		if err := json.Unmarshal([]byte(createsql), param); err != nil {
			return nil, err
		}
	}
	param.ExternType = externScan.Type
	param.FileService = c.proc.Base.FileService
	param.Ctx = c.proc.Ctx

	if externScan.Type == int32(plan.ExternType_EXTERNAL_TB) || externScan.Type == int32(plan.ExternType_RESULT_SCAN) {
		switch param.ScanType {
		case tree.INFILE:
			if err := plan2.InitInfileOrStageParam(param, proc); err != nil {
				return nil, err
			}
		case tree.S3:
			if err := plan2.InitS3Param(param); err != nil {
				return nil, err
			}
		}
	}
	return param, nil
}

func (c *Compile) getReadWriteParallelFlag(param *tree.ExternParam, fileList []string) (readParallel bool, writeParallel bool) {
	if !param.Parallel {
		return false, false
	}
	if param.Format == tree.PARQUET {
		return false, true
	}
	if param.Local || crt.GetCompressType(param.CompressType, fileList[0]) != tree.NOCOMPRESS {
		return false, true
	}
	return true, true
}

func (c *Compile) getExternalFileListAndSize(node *plan.Node, param *tree.ExternParam) (fileList []string, fileSize []int64, err error) {
	// Hive partition tables use recursive list-and-filter discovery, not ReadDir.
	// ReadDir requires glob patterns in filepath; Hive base paths are opaque directories.
	if param.HivePartitioning {
		return c.getHivePartitionFileList(node, param)
	}
	switch node.ExternScan.Type {
	case int32(plan.ExternType_EXTERNAL_TB):
		t := time.Now()
		_, spanReadDir := trace.Start(c.proc.Ctx, "compileExternScan.ReadDir")
		fileList, fileSize, err = plan2.ReadDir(param)
		if err != nil {
			spanReadDir.End()
			return nil, nil, err
		}
		spanReadDir.End()
		fileList, fileSize, node.FilterList, err = external.FilterFileList(c.proc.Ctx, node, c.proc, fileList, fileSize)
		if err != nil {
			return nil, nil, err
		}
		if time.Since(t) > time.Second {
			c.proc.Infof(c.proc.Ctx, "read dir cost %v", time.Since(t))
		}
	case int32(plan.ExternType_RESULT_SCAN):
		fileList = strings.Split(param.Filepath, ",")
		for i := range fileList {
			fileList[i] = strings.TrimSpace(fileList[i])
		}
		fileList, fileSize, node.FilterList, err = external.FilterFileList(c.proc.Ctx, node, c.proc, fileList, fileSize)
		if err != nil {
			return nil, nil, err
		}
	case int32(plan.ExternType_LOAD):
		if param.Format == tree.PARQUET && strings.ContainsAny(strings.TrimSpace(param.Filepath), "*?[") {
			fileList, fileSize, err = plan2.ReadDir(param)
			if err != nil {
				return nil, nil, err
			}
		} else {
			fileList = []string{param.Filepath}
			fileSize = []int64{param.FileSize}
		}
	}
	return fileList, fileSize, nil
}

func (c *Compile) getHivePartitionFileList(node *plan.Node, param *tree.ExternParam) ([]string, []int64, error) {
	partColSet := toLowerSet(param.HivePartitionCols)
	partFilters, fpFilters, rowFilters := external.ClassifyFilters(
		node.TableDef, node.FilterList, partColSet)

	pruneExpr := external.ExtractPartitionPruneExprFromExprs(node.TableDef, partFilters, partColSet)

	listDir := external.NewListDirFunc(param)
	options, err := c.getHivePartitionDiscoverOptions(param)
	if err != nil {
		return nil, nil, err
	}
	result, err := external.DiscoverHivePartitionsWithPruneExpr(
		c.proc.Ctx, listDir, param.Filepath,
		param.HivePartitionCols, param.HivePartitionColTypes, pruneExpr, options)
	if err != nil {
		return nil, nil, err
	}

	fileList := make([]string, len(result.Files))
	fileSize := make([]int64, len(result.Files))
	for i, f := range result.Files {
		fileList[i] = f.FilePath
		fileSize[i] = f.FileSize
	}

	if len(fpFilters) > 0 {
		var leftover []*plan.Expr
		fileList, fileSize, leftover, err = runFilePathFilters(c.proc.Ctx, c.proc, node, fpFilters, fileList, fileSize)
		if err != nil {
			return nil, nil, err
		}
		rowFilters = append(rowFilters, leftover...)
	}

	updateHivePartitionScanStats(node, param.Filepath, result, fileSize)
	node.FilterList = rowFilters
	return fileList, fileSize, nil
}

const (
	hivePartitionCacheTTLVar        = "hive_partition_cache_ttl"
	hivePartitionCacheMaxEntriesVar = "hive_partition_cache_max_entries"
	hivePartitionCacheMaxBytesVar   = "hive_partition_cache_max_bytes"
	hivePartitionListConcurrencyVar = "hive_partition_list_concurrency"
)

func (c *Compile) getHivePartitionDiscoverOptions(param *tree.ExternParam) (*external.DiscoverOptions, error) {
	resolve := c.proc.GetResolveVariableFunc()
	if resolve == nil {
		return nil, nil
	}
	cacheTTLSeconds, err := resolveHivePartitionIntVar(resolve, hivePartitionCacheTTLVar)
	if err != nil {
		return nil, err
	}
	cacheMaxEntries, err := resolveHivePartitionIntVar(resolve, hivePartitionCacheMaxEntriesVar)
	if err != nil {
		return nil, err
	}
	cacheMaxBytes, err := resolveHivePartitionIntVar(resolve, hivePartitionCacheMaxBytesVar)
	if err != nil {
		return nil, err
	}
	listConcurrency, err := resolveHivePartitionIntVar(resolve, hivePartitionListConcurrencyVar)
	if err != nil {
		return nil, err
	}

	if cacheTTLSeconds <= 0 && cacheMaxEntries <= 0 && cacheMaxBytes <= 0 && listConcurrency <= 0 {
		return nil, nil
	}
	opts := &external.DiscoverOptions{}
	if listConcurrency > 0 {
		opts.ListConcurrency = int(listConcurrency)
	}
	if cacheTTLSeconds > 0 {
		accountID := uint32(0)
		if id, err := defines.GetAccountId(c.proc.Ctx); err == nil {
			accountID = id
		}
		opts.CacheTTL = time.Duration(cacheTTLSeconds) * time.Second
		opts.CacheKeyPrefix = external.BuildHivePartitionListCacheKeyPrefix(param, accountID, param.Filepath)
	}
	if cacheMaxEntries > 0 {
		opts.CacheMaxEntries = int(cacheMaxEntries)
	}
	if cacheMaxBytes > 0 {
		opts.CacheMaxBytes = cacheMaxBytes
	}
	return opts, nil
}

func resolveHivePartitionIntVar(
	resolve func(varName string, isSystemVar, isGlobalVar bool) (interface{}, error),
	name string,
) (int64, error) {
	v, err := resolve(name, true, false)
	if err != nil {
		return 0, err
	}
	switch x := v.(type) {
	case nil:
		return 0, nil
	case int:
		return int64(x), nil
	case int8:
		return int64(x), nil
	case int16:
		return int64(x), nil
	case int32:
		return int64(x), nil
	case int64:
		return x, nil
	case uint:
		return int64(x), nil
	case uint8:
		return int64(x), nil
	case uint16:
		return int64(x), nil
	case uint32:
		return int64(x), nil
	case uint64:
		if x > uint64(^uint64(0)>>1) {
			return 0, moerr.NewInvalidInputNoCtxf("%s is too large: %d", name, x)
		}
		return int64(x), nil
	case string:
		if strings.TrimSpace(x) == "" {
			return 0, nil
		}
		n, err := strconv.ParseInt(strings.TrimSpace(x), 10, 64)
		if err != nil {
			return 0, moerr.NewInvalidInputNoCtxf("invalid %s value %q: %v", name, x, err)
		}
		return n, nil
	default:
		return 0, moerr.NewInvalidInputNoCtxf("invalid %s type %T", name, v)
	}
}

func updateHivePartitionScanStats(node *plan.Node, basePath string, result *external.PartitionDiscoveryResult, fileSize []int64) {
	if node.Stats == nil {
		node.Stats = &plan.Stats{}
	}
	var prunedBytes int64
	for _, size := range fileSize {
		prunedBytes += size
	}
	node.Stats.BlockNum = int32(len(fileSize))
	node.Stats.Cost = float64(prunedBytes)
	if node.Stats.TableCnt == 0 {
		node.Stats.TableCnt = float64(len(fileSize))
	}
	if node.Stats.Outcnt == 0 {
		node.Stats.Outcnt = float64(len(fileSize))
	}
	logutil.Debugf("hive partition discovery summary: base=%s files=%d bytes=%d pruned_files=%d pruned_bytes=%d partitions=%d pruned=%d list_calls=%d cache_hits=%d cache_misses=%d direct_prefix_hits=%d direct_prefix_misses=%d duration=%s",
		basePath, len(fileSize), prunedBytes, result.PrunedFiles, result.PrunedBytes,
		result.PartitionCount, result.PrunedCount, result.ListCalls, result.CacheHits, result.CacheMisses,
		result.DirectPrefixHits, result.DirectPrefixMisses, result.DiscoveryDuration)
}

func runFilePathFilters(
	ctx context.Context,
	proc *process.Process,
	node *plan.Node,
	fpFilters []*plan.Expr,
	fileList []string,
	fileSize []int64,
) ([]string, []int64, []*plan.Expr, error) {
	filterNode := plan2.DeepCopyNode(node)
	filterNode.FilterList = fpFilters
	outFileList, outFileSize, leftover, err := external.FilterFileList(ctx, filterNode, proc, fileList, fileSize)
	if err != nil {
		return nil, nil, nil, err
	}
	return outFileList, outFileSize, leftover, nil
}

func toLowerSet(cols []string) map[string]bool {
	m := make(map[string]bool, len(cols))
	for _, col := range cols {
		m[strings.ToLower(col)] = true
	}
	return m
}

func (c *Compile) compileExternScan(node *plan.Node) ([]*Scope, error) {
	return c.compileExternScanWithPlanNodeID(node, -1)
}

func (c *Compile) compileExternScanWithPlanNodeID(node *plan.Node, planNodeID int32) ([]*Scope, error) {
	if c.isPrepare {
		return nil, cantCompileForPrepareErr
	}
	ctx, span := trace.Start(c.proc.Ctx, "compileExternScan")
	defer span.End()
	start := time.Now()
	defer func() {
		if t := time.Since(start); t > time.Second {
			c.proc.Infof(ctx, "compileExternScan cost %v", t)
		}
	}()

	if node.ExternScan != nil && node.ExternScan.Type == int32(plan.ExternType_MONGODB_TB) {
		// Hydration resolves execution-time catalog state and prunes the source
		// mapping to the physical projection. Keep that mutation isolated even
		// when this helper is called outside compilePlanScope, because a prepared
		// execution may otherwise hand us its cached logical plan directly.
		executionNode := plan2.DeepCopyNode(node)
		if err := c.configureMongoUserQuery(executionNode); err != nil {
			return nil, err
		}
		if err := c.hydrateMongoScan(executionNode); err != nil {
			return nil, err
		}
		scope := c.constructMongoScanScope()
		currentFirstFlag := c.anal.isFirst
		op := mongoscan.NewArgument().WithScan(executionNode.ExternScan.MongodbScan)
		op.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
		scope.setRootOperator(op)
		c.anal.isFirst = false
		return []*Scope{scope}, nil
	}

	err, strictSqlMode := StrictSqlMode(c.proc)
	if err != nil {
		return nil, err
	}

	if node.ExternScan != nil && node.ExternScan.Type == int32(plan.ExternType_DATASTREAM_TB) {
		return c.compileDatastreamScan(node, strictSqlMode)
	}

	if node.ExternScan != nil && node.ExternScan.Type == int32(plan.ExternType_FOREIGN_TB) {
		return c.compileForeignScan(node, strictSqlMode)
	}
	if node.ExternScan != nil && node.ExternScan.Type == int32(plan.ExternType_KAFKA_TB) {
		return c.compileKafkaScan(node, strictSqlMode)
	}

	if node.ExternScan != nil && node.ExternScan.Type == int32(plan.ExternType_ICEBERG_TB) {
		access, err := c.checkIcebergScanAccess(node)
		if err != nil {
			return nil, err
		}
		return c.compileIcebergScanWithAccessForPlanNode(planNodeID, node, strictSqlMode, access)
	}

	param, err := c.getExternParam(c.proc, node.ExternScan, node.TableDef.Createsql)
	if err != nil {
		return nil, err
	}

	strictSqlMode = effectiveExternalStrictMode(c.proc, param, strictSqlMode)
	if param.ScanType == tree.INLINE {
		return c.compileExternValueScan(node, param, strictSqlMode)
	}

	fileList, fileSize, err := c.getExternalFileListAndSize(node, param)
	if err != nil {
		return nil, err
	}

	if len(fileList) == 0 {
		ret := newScope(Merge)
		ret.NodeInfo = getEngineNode(c)
		ret.NodeInfo.Mcpu = 1
		ret.DataSource = &Source{isConst: true, node: node}

		currentFirstFlag := c.anal.isFirst

		op, err := constructValueScan(c.proc, nil)
		if err != nil {
			return nil, err
		}
		op.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
		ret.setRootOperator(op)
		c.anal.isFirst = false

		ret.Proc = c.proc.NewNoContextChildProc(0)
		return []*Scope{ret}, nil
	}

	if param.HivePartitioning {
		return c.compileExternScanHiveFileFanout(node, param, fileList, fileSize, strictSqlMode)
	}
	if param.ExternType == int32(plan.ExternType_LOAD) &&
		param.Format == tree.PARQUET &&
		param.Parallel {
		// A file is already the smallest independently executable unit when
		// the matched files fill every available load scope.  Do not pay one
		// serial footer round trip per file merely to discover that row-group
		// fanout cannot add useful execution parallelism.
		if c.parquetLoadFileFanoutSaturates(param, len(fileList)) {
			return c.compileExternScanParquetLoadFileFanout(node, param, fileList, fileSize, strictSqlMode)
		}
		rowGroups, footerStats, err := c.readLoadParquetRowGroupMetadata(node, param, fileList, fileSize)
		if err != nil {
			return nil, err
		}
		logutil.Debugf("parquet load footer metadata: files=%d row_groups=%d rows=%d read_calls=%d read_bytes=%d duration=%s",
			footerStats.Files, footerStats.RowGroups, footerStats.Rows,
			footerStats.ReadCalls, footerStats.ReadBytes, footerStats.Duration)
		if footerStats.RowGroups > parquetRowGroupFileCount(rowGroups) {
			return c.compileExternScanParquetRowGroupFanout(node, param, fileList, fileSize, rowGroups, strictSqlMode)
		}
		if len(fileList) > 1 {
			return c.compileExternScanParquetLoadFileFanout(node, param, fileList, fileSize, strictSqlMode)
		}
	}

	readParallel, writeParallel := c.getReadWriteParallelFlag(param, fileList)

	if readParallel && writeParallel {
		return c.compileExternScanParallelReadWrite(node, param, fileList, fileSize, strictSqlMode)
	} else if writeParallel {
		return c.compileExternScanParallelWrite(node, param, fileList, fileSize, strictSqlMode)
	} else {
		return c.compileExternScanSerialReadWrite(node, param, fileList, fileSize, strictSqlMode)
	}
}

// compileDatastreamScan builds the single-scope pipeline for a datastream
// external table: deparse the pushable filter conjuncts into the pushdown
// hint, optionally drop them from local rechecking (recheck=false), and run
// the external operator with a DataStreamReader.  node is the compile-owned
// deep copy, so trimming its FilterList only affects this pipeline's
// downstream restrict.
func (c *Compile) compileDatastreamScan(node *plan.Node, strictSqlMode bool) ([]*Scope, error) {
	ds := node.ExternScan.GetDatastreamScan()
	if ds == nil {
		return nil, moerr.NewInvalidInput(c.proc.Ctx, "datastream external table is missing scan metadata")
	}
	// The pushed filter can only be sent to the server when the user has
	// opted into trusting the server's predicate semantics (recheck=false).
	//
	// A pushed predicate is NOT provably superset-preserving across engines:
	// the server may drop a row MO would keep under a different collation,
	// time zone, or coercion (e.g. a case-insensitive source evaluating
	// `s <> 'a'` drops both 'a' and 'A', but MO distinguishes them and wants
	// the 'A' row). Local recheck can only *remove* over-returned rows, never
	// restore ones the source already filtered out — so pushing in the
	// recheck=true default would under-return. Therefore recheck=true (the
	// safe default) sends no narrowing filter: the server returns the full
	// datasource and MO applies every predicate locally, which is correct
	// under any server semantics. recheck=false trusts the server for exactly
	// the conjuncts that were pushed; conjuncts the deparser could not express
	// always stay local.
	if !ds.Recheck {
		pushedText, pushed := sqldatastream.DeparseFilters(node.FilterList, node.TableDef.Cols, c.proc.GetSessionInfo().TimeZone)
		ds.PushedFilter = pushedText
		if pushedText != "" {
			kept := make([]*plan.Expr, 0, len(node.FilterList))
			for i, expr := range node.FilterList {
				if !pushed[i] {
					kept = append(kept, expr)
				}
			}
			node.FilterList = kept
		}
	}

	param := external.DatastreamExternParam()
	param.Ctx = c.proc.Ctx

	scope := c.constructScopeForExternal(c.addr, false)
	currentFirstFlag := c.anal.isFirst
	op := constructExternal(node, param, c.proc.Ctx, nil, nil, nil, strictSqlMode)
	op.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
	scope.setRootOperator(op)
	c.anal.isFirst = false
	return []*Scope{scope}, nil
}

// compileForeignScan compiles a scan of an ESQL/SQL foreign external table.
// The query texts to run are derived from __mo_query predicates (falling back
// to the table's 'query' option) and become the scan's FileList: one query is
// one virtual file. The scope is pinned to the session's CN with Mcpu=1 --
// the session-local connection cache must be reachable, and foreign queries
// within one scan run sequentially. Separate scans in one MO query are
// separate scopes and run concurrently.
func (c *Compile) compileForeignScan(node *plan.Node, strictSqlMode bool) ([]*Scope, error) {
	fs := node.ExternScan.GetForeignScan()
	if fs == nil {
		return nil, moerr.NewInvalidInput(c.proc.Ctx, "foreign external table is missing scan metadata")
	}

	queryList, err := external.DeriveForeignQueryList(c.proc.Ctx, node, c.proc)
	if err != nil {
		return nil, err
	}
	if len(queryList) == 0 {
		if fs.DefaultQuery == "" {
			return nil, moerr.NewInvalidInputf(c.proc.Ctx,
				"%s external table requires a __mo_query = '<text>' predicate or a 'query' table option", fs.Kind)
		}
		queryList = []string{fs.DefaultQuery}
	}
	// Apply ALL query-level conjuncts to the candidate list (the generating
	// =/IN ones trivially pass; non-generating ones like LIKE may prune) and
	// keep only row-level conjuncts in the compile-owned node's FilterList.
	fileSize := make([]int64, len(queryList))
	for i := range fileSize {
		fileSize[i] = -1
	}
	queryList, fileSize, residual, err := external.FilterFileList(c.proc.Ctx, node, c.proc, queryList, fileSize)
	if err != nil {
		return nil, err
	}
	// Predicate pushdown, SQL only and opt-in ('pushdown' = 'true'): wrap each
	// query text as a derived table carrying the conjuncts MO can render, and
	// stop evaluating those locally -- the source has them now.
	//
	// Opting in is a statement about the source: that its result columns are
	// the DECLARED ones, by name, because a WHERE clause has to name what it
	// filters and MO writes the names it knows. The reader checks that claim
	// against the columns the source actually answers with, and errors when it
	// does not hold. Predicates MO cannot render stay local, since there is
	// nothing to send.
	if fs.Pushdown && fs.Kind == foreignext.KindSQL && len(residual) > 0 {
		// Bare identifiers: the quoting character is dialect-specific, and
		// sql_tvf speaks to PostgreSQL as well as MySQL.
		filter, pushed := sqldatastream.DeparseFiltersBareIdents(
			residual, pushdownCols(node.TableDef.Cols), c.proc.GetSessionInfo().TimeZone)
		if filter != "" {
			for i := range queryList {
				queryList[i] = foreignext.WrapPushdownQuery(queryList[i], filter)
			}
			kept := make([]*plan.Expr, 0, len(residual))
			for i, expr := range residual {
				if !pushed[i] {
					kept = append(kept, expr)
				}
			}
			residual = kept
		}
	}
	node.FilterList = residual

	param := external.ForeignExternParam(fs.Kind)
	param.Ctx = c.proc.Ctx

	scope := c.constructScopeForExternal(c.addr, false)
	currentFirstFlag := c.anal.isFirst
	op := constructExternal(node, param, c.proc.Ctx, queryList, fileSize, nil, strictSqlMode)
	op.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
	scope.setRootOperator(op)
	c.anal.isFirst = false
	return []*Scope{scope}, nil
}

// pushdownCols masks the synthetic external-scan columns out of a scan's
// column list.  They exist only inside MO -- __mo_query selects which query
// runs, the error-mode columns are synthesized by the reader -- so a conjunct
// mentioning one must never be rendered into the source's WHERE clause.  The
// deparser resolves column refs by position and already refuses a nil entry,
// so blanking the slot is enough.
//
// The mask is by name, which is blunter than the (name, ColId) scoping used
// elsewhere: a pre-existing schema may hold a REAL column called __mo_query,
// and masking it merely costs that one conjunct its pushdown.
func pushdownCols(cols []*plan.ColDef) []*plan.ColDef {
	masked := make([]*plan.ColDef, len(cols))
	for i, col := range cols {
		if col == nil || catalog.IsReservedExternalColName(col.Name) {
			continue
		}
		masked[i] = col
	}
	return masked
}

// compileKafkaScan compiles a scan of a Kafka external table. The read
// position/limits come from the __mo_read_* control predicates (consumed
// here); the scope is pinned to the session's CN with Mcpu=1 — the read is a
// single ordered partition consume, and LAST_KAFKA_MESSAGE_ID() lives in the
// session.
func (c *Compile) compileKafkaScan(node *plan.Node, strictSqlMode bool) ([]*Scope, error) {
	ks := node.ExternScan.GetKafkaScan()
	if ks == nil {
		return nil, moerr.NewInvalidInput(c.proc.Ctx, "kafka external table is missing scan metadata")
	}
	if err := external.DeriveKafkaReadControl(c.proc.Ctx, node, c.proc); err != nil {
		return nil, err
	}

	param := external.KafkaExternParam(ks)
	param.Ctx = c.proc.Ctx

	scope := c.constructScopeForExternal(c.addr, false)
	currentFirstFlag := c.anal.isFirst
	op := constructExternal(node, param, c.proc.Ctx, nil, nil, nil, strictSqlMode)
	op.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
	scope.setRootOperator(op)
	c.anal.isFirst = false
	return []*Scope{scope}, nil
}

// constructMongoScanScope keeps the single MongoDB reader in the scheduled
// query-worker set. This matters when a downstream DEDUP join builds a
// distributed shuffle: a coordinator-local source outside that set has no
// receiver tree to start it, leaving every shuffle receiver waiting forever.
func (c *Compile) constructMongoScanScope() *Scope {
	current := c.materializeScheduledWorker(c.currentCNWorker())
	node := current
	stageNodes := c.queryWorkerStageNodes()
	if len(stageNodes) > 0 {
		node = stageNodes[0]
		for _, candidate := range stageNodes {
			if sameExecutionNode(candidate, current) {
				node = candidate
				break
			}
		}
	}

	scope := c.constructScopeForExternalNode(node, !sameExecutionNode(node, current))
	scope.NodeInfo.Mcpu = 1
	return scope
}

// configureMongoUserQuery extracts at most one explicit __mo_query value from
// the compile-owned node. The text is parsed and reduced to validated BSON
// before it enters the execution plan. Query-level predicates are evaluated
// against that one candidate and removed; every ordinary predicate remains an
// MO residual.
func (c *Compile) configureMongoUserQuery(node *plan.Node) error {
	if node == nil || node.ExternScan == nil || node.ExternScan.MongodbScan == nil {
		return moerr.NewInvalidInput(c.proc.Ctx, "MongoDB external table is missing scan metadata")
	}
	scan := node.ExternScan.MongodbScan
	queryList, err := external.DeriveForeignQueryList(c.proc.Ctx, node, c.proc)
	if err != nil {
		return err
	}
	if len(queryList) > 1 {
		return moerr.NewNotSupported(c.proc.Ctx, "MongoDB MVP accepts exactly one __mo_query value")
	}
	if len(queryList) == 0 {
		if mongoQueryColumnUsed(node, node.FilterList) {
			return moerr.NewNotSupported(c.proc.Ctx, "MongoDB MVP requires __mo_query = <constant>")
		}
		scan.IncludeQueryColumn = mongoQueryColumnUsed(node, node.ProjectList)
		if mongoScanUsesV44Payload(scan) && !supportsRemoteMongoUserQuery(c.proc.GetService()) {
			return moerr.NewNotSupported(
				c.proc.Ctx,
				"MongoDB query semantics require MORPC protocol version 44",
			)
		}
		return nil
	}

	queryList, _, residual, err := external.FilterFileList(
		c.proc.Ctx, node, c.proc, queryList, []int64{-1})
	if err != nil {
		return err
	}
	node.FilterList = residual
	scan.IncludeQueryColumn = mongoQueryColumnUsed(node, node.FilterList) ||
		mongoQueryColumnUsed(node, node.ProjectList)
	if len(queryList) == 0 {
		scan.EmptyResult = true
		if !supportsRemoteMongoUserQuery(c.proc.GetService()) {
			return moerr.NewNotSupported(
				c.proc.Ctx,
				"MongoDB query semantics require MORPC protocol version 44",
			)
		}
		return nil
	}
	if len(queryList) != 1 {
		return moerr.NewNotSupported(c.proc.Ctx, "MongoDB MVP accepts exactly one __mo_query value")
	}
	query, err := sqlmongodb.ParseUserQuery(c.proc.Ctx, queryList[0])
	if err != nil {
		return err
	}
	if !supportsRemoteMongoUserQuery(c.proc.GetService()) {
		return moerr.NewNotSupported(
			c.proc.Ctx,
			"MongoDB explicit queries require MORPC protocol version 44",
		)
	}
	// The planner retains the selector as a local filter around an opaque
	// aggregation pipeline. Keep the hidden carrier available even when it is
	// not selected so that filter evaluates against the same canonical source
	// value the scan used; otherwise a three-column pipeline batch can reach a
	// four-column selector and panic.
	if query.Kind == sqlmongodb.UserQueryPipeline {
		scan.IncludeQueryColumn = true
	}
	return sqlmongodb.ApplyUserQueryToPlan(c.proc.Ctx, query, scan)
}

func mongoQueryColumnUsed(node *plan.Node, expressions []*plan.Expr) bool {
	if node == nil || node.TableDef == nil {
		return false
	}
	usesQueryColumn := func(expr *plan.Expr) bool {
		var visit func(*plan.Expr) bool
		visit = func(current *plan.Expr) bool {
			if current == nil {
				return false
			}
			if col := current.GetCol(); col != nil {
				position := int(col.ColPos)
				return position >= 0 && position < len(node.TableDef.Cols) &&
					catalog.IsForeignQueryCol(node.TableDef.Cols[position].Name, node.TableDef.Cols[position].ColId)
			}
			if functionExpr := current.GetF(); functionExpr != nil {
				for _, arg := range functionExpr.Args {
					if visit(arg) {
						return true
					}
				}
			}
			if list := current.GetList(); list != nil {
				for _, item := range list.List {
					if visit(item) {
						return true
					}
				}
			}
			return false
		}
		return visit(expr)
	}
	for _, expr := range expressions {
		if usesQueryColumn(expr) {
			return true
		}
	}
	return false
}

func (c *Compile) hydrateMongoScan(node *plan.Node) error {
	if node == nil || node.ExternScan == nil || node.ExternScan.MongodbScan == nil {
		return moerr.NewInvalidInput(c.proc.Ctx, "MongoDB external table is missing scan metadata")
	}
	scan := node.ExternScan.MongodbScan
	accountID, err := defines.GetAccountId(c.proc.Ctx)
	if err != nil {
		return err
	}
	res, err := c.runSqlWithResultAndOptions(
		sqlmongodb.GetMappingByTableIDSQL(accountID, scan.TableId),
		NoAccountId,
		executor.StatementOption{}.WithDisableLog(),
	)
	if err != nil {
		return err
	}
	defer res.Close()
	found := false
	var parseErr error
	res.ReadRows(func(rows int, cols []*vector.Vector) bool {
		if rows == 0 || len(cols) < 9 {
			return true
		}
		mappingIDs := executor.GetFixedRows[uint64](cols[0])
		connectionIDs := executor.GetFixedRows[uint64](cols[1])
		versions := executor.GetFixedRows[uint64](cols[2])
		parallelism := executor.GetFixedRows[int32](cols[6])
		disabled := executor.GetFixedRows[uint64](cols[7])
		mappingVersions := executor.GetFixedRows[uint64](cols[8])
		if len(mappingIDs) == 0 || len(connectionIDs) == 0 || len(versions) == 0 || len(parallelism) == 0 || len(disabled) == 0 || len(mappingVersions) == 0 {
			parseErr = moerr.NewInternalError(c.proc.Ctx, "MongoDB catalog mapping row has invalid types")
			return false
		}
		if disabled[0] != 0 {
			parseErr = moerr.NewInvalidInput(c.proc.Ctx, "MongoDB connection is disabled")
			return false
		}
		var columns []sqlmongodb.ColumnMapping
		if err := json.Unmarshal(cols[5].GetBytesAt(0), &columns); err != nil {
			parseErr = moerr.NewInternalError(c.proc.Ctx, "MongoDB catalog column mapping is invalid")
			return false
		}
		catalogMapping := sqlmongodb.TableMapping{
			TableID: scan.TableId, MappingID: mappingIDs[0], ConnectionID: connectionIDs[0],
			Database: string(cols[3].GetBytesAt(0)), Collection: string(cols[4].GetBytesAt(0)),
			Columns: columns, MaxParallelism: parallelism[0], Version: mappingVersions[0],
		}
		if !sqlmongodb.MappingDefinitionMatchesPlan(catalogMapping, scan) {
			parseErr = moerr.NewInvalidInput(c.proc.Ctx, "MongoDB table mapping changed during planning; retry the statement")
			return false
		}
		columns, parseErr = projectedMongoColumns(
			c.proc.Ctx,
			columns,
			node.TableDef,
			scan.IncludeQueryColumn || scan.EmptyResult || scan.UserQueryKind != int32(sqlmongodb.UserQueryInvalid),
		)
		if parseErr != nil {
			return false
		}
		scan.MappingId = mappingIDs[0]
		scan.MappingVersion = mappingVersions[0]
		scan.ConnectionId = connectionIDs[0]
		scan.ConnectionVersion = versions[0]
		scan.Database = string(cols[3].GetBytesAt(0))
		scan.Collection = string(cols[4].GetBytesAt(0))
		scan.Columns = sqlmongodb.ColumnsToPlan(columns)
		scan.ProjectedPaths = projectedMongoPlanPaths(columns)
		scan.MaxParallelism = parallelism[0]
		found = true
		return false
	})
	if parseErr != nil {
		return parseErr
	}
	if !found {
		return moerr.NewInvalidInput(c.proc.Ctx, "MongoDB table mapping does not exist for this account")
	}
	if scan.MaxParallelism != 1 {
		return moerr.NewNotSupported(c.proc.Ctx, "MongoDB MVP requires max_parallelism=1")
	}
	pushed, residualDigest := sqlmongodb.PushdownPlanFilters(c.proc.Ctx, node.FilterList, scan.Columns)
	if scan.UserQueryKind == int32(sqlmongodb.UserQueryPipeline) {
		// Ordinary MO predicates refer to the pipeline output. Moving them ahead
		// of an opaque user pipeline can change its meaning, so they remain local.
		pushed = nil
	}
	scan.PushedPredicate, scan.ResidualFilterDigest = pushed, residualDigest
	return nil
}

func projectedMongoColumns(
	ctx context.Context,
	columns []sqlmongodb.ColumnMapping,
	tableDef *plan.TableDef,
	allowEmpty bool,
) ([]sqlmongodb.ColumnMapping, error) {
	if tableDef == nil {
		return nil, moerr.NewInternalError(ctx, "MongoDB external scan is missing its table definition")
	}
	names := make([]string, 0, len(tableDef.Cols))
	for _, column := range tableDef.Cols {
		if column != nil && !column.Hidden && !catalog.IsForeignQueryCol(column.Name, column.ColId) {
			names = append(names, column.Name)
		}
	}
	if len(names) == 0 {
		if allowEmpty {
			return nil, nil
		}
		return nil, moerr.NewInternalError(ctx, "MongoDB external scan has no retained mapped columns")
	}
	return sqlmongodb.ProjectColumnsByName(ctx, columns, names)
}

func projectedMongoPlanPaths(columns []sqlmongodb.ColumnMapping) []string {
	result := make([]string, 0, len(columns))
	seen := make(map[string]struct{}, len(columns))
	for _, column := range columns {
		if _, ok := seen[column.Path]; ok {
			continue
		}
		seen[column.Path] = struct{}{}
		result = append(result, column.Path)
	}
	return result
}

func (c *Compile) getParallelSizeForExternalScan(node *plan.Node, cpuNum int) int {
	if node.Stats == nil {
		return cpuNum
	}
	totalSize := node.Stats.Cost * node.Stats.Rowsize
	parallelSize := int(totalSize / float64(colexec.WriteS3Threshold))
	if parallelSize < 1 {
		return 1
	} else if parallelSize < cpuNum {
		return parallelSize
	}
	return cpuNum
}

func (c *Compile) getLoadWriteS3ParallelSize(node *plan.Node, cpuNum int) int {
	parallelSize := c.getParallelSizeForExternalScan(node, cpuNum)
	if c.anal != nil && c.anal.qry != nil && c.anal.qry.LoadTag {
		parallelSize = min(parallelSize, loadWriteS3ParallelSizeLimit)
	}
	return parallelSize
}

// load data inline goes here, should always be single parallel
func (c *Compile) compileExternValueScan(node *plan.Node, param *tree.ExternParam, strictSqlMode bool) ([]*Scope, error) {
	s := c.constructScopeForExternal(c.addr, false)
	currentFirstFlag := c.anal.isFirst
	op := constructExternal(node, param, c.proc.Ctx, nil, nil, nil, strictSqlMode)
	op.SetIdx(c.anal.curNodeIdx)
	op.SetIsFirst(currentFirstFlag)
	s.setRootOperator(op)
	c.anal.isFirst = false
	return []*Scope{s}, nil
}

// construct one thread to read the file data, then dispatch to mcpu thread to get the filedata for insert
func (c *Compile) compileExternScanParallelWrite(node *plan.Node, param *tree.ExternParam, fileList []string, fileSize []int64, strictSqlMode bool) ([]*Scope, error) {
	loadEmptyNumericAsZero := param.ExternType == int32(plan.ExternType_LOAD) &&
		(param.Parallel || param.ParallelLoadRequested)
	param.Parallel = false
	fileOffsetTmp := make([]*pipeline.FileOffset, len(fileList))
	for i := 0; i < len(fileList); i++ {
		fileOffsetTmp[i] = &pipeline.FileOffset{}
		fileOffsetTmp[i].Offset = make([]int64, 0)
		fileOffsetTmp[i].Offset = append(fileOffsetTmp[i].Offset, []int64{0, -1}...)
	}
	scope := c.constructScopeForExternal(c.addr, false)
	currentFirstFlag := c.anal.isFirst
	extern := constructExternal(node, param, c.proc.Ctx, fileList, fileSize, fileOffsetTmp, strictSqlMode)
	parallelLoad := true
	if len(fileList) > 0 && crt.GetCompressType(param.CompressType, fileList[0]) != tree.NOCOMPRESS {
		parallelLoad = false
	}
	extern.Es.ParallelLoad = parallelLoad
	extern.Es.LoadEmptyNumericAsZero = loadEmptyNumericAsZero
	extern.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
	scope.setRootOperator(extern)
	c.anal.isFirst = false

	mcpu := c.getLoadWriteS3ParallelSize(node, c.ncpu) // dop of insert scopes
	if mcpu == 1 {
		return []*Scope{scope}, nil
	}

	ss := make([]*Scope, mcpu)
	for i := 0; i < mcpu; i++ {
		ss[i] = c.constructLoadMergeScope()
	}
	dispatchOp, err := constructLocalDispatchFromScopes(0, ss, scope)
	if err != nil {
		return nil, err
	}
	dispatchOp.FuncId = dispatch.SendToAnyLocalFunc
	dispatchOp.SetAnalyzeControl(c.anal.curNodeIdx, false)
	scope.setRootOperator(dispatchOp)

	ss[0].PreScopes = append(ss[0].PreScopes, scope)
	c.anal.isFirst = false
	return ss, nil
}

func GetExternParallelSize(totalSize int64, cpuNum int) int {
	parallelSize := int(totalSize / int64(colexec.WriteS3Threshold))
	if parallelSize < 1 {
		return 1
	} else if parallelSize < cpuNum {
		return parallelSize
	}
	return cpuNum
}

type hiveFileShard struct {
	node     engine.Node
	fileList []string
	fileSize []int64
}

type parquetRowGroupMeta struct {
	fileIndex     int32
	rowGroupIndex int32
	numRows       int64
	bytes         int64
}

type parquetFooterStats struct {
	Files     int
	RowGroups int
	Rows      int64
	Bytes     int64
	ReadCalls int64
	ReadBytes int64
	Duration  time.Duration
}

type parquetRowGroupScopeShard struct {
	node            engine.Node
	fileList        []string
	fileSize        []int64
	rowGroupShards  []*pipeline.ParquetRowGroupShard
	originalToLocal map[int32]int32
}

type parquetRowGroupSegment struct {
	fileIndex int32
	rowGroups []parquetRowGroupMeta
	load      int64
}

type icebergDataFileScopeShard struct {
	node      engine.Node
	fileList  []string
	fileSize  []int64
	dataTasks []*pipeline.IcebergDataFileTask
}

type icebergExternalScanRuntime struct {
	dataTasks            []*pipeline.IcebergDataFileTask
	deleteTasks          []*pipeline.IcebergDeleteFileTask
	columns              []*pipeline.IcebergColumnMapping
	snapshot             *pipeline.IcebergSnapshotRuntime
	objectIORef          string
	hiddenReadCols       []int32
	planningStats        process.ParquetProfileStats
	needRowOrdinal       bool
	deleteMaxMemoryBytes int64
	deleteSpillEnabled   bool
}

func (c *Compile) compileExternScanHiveFileFanout(node *plan.Node, param *tree.ExternParam, fileList []string, fileSize []int64, strictSqlMode bool) ([]*Scope, error) {
	return c.compileExternScanWholeFileFanout(node, param, fileList, fileSize, strictSqlMode, false)
}

func (c *Compile) compileExternScanParquetLoadFileFanout(node *plan.Node, param *tree.ExternParam, fileList []string, fileSize []int64, strictSqlMode bool) ([]*Scope, error) {
	return c.compileExternScanWholeFileFanout(node, param, fileList, fileSize, strictSqlMode, true)
}

func (c *Compile) compileExternScanWholeFileFanout(node *plan.Node, param *tree.ExternParam, fileList []string, fileSize []int64, strictSqlMode bool, parquetWholeFileFanout bool) ([]*Scope, error) {
	nodes := c.getHiveFileFanoutNodes(param, len(fileList))
	shards := splitHiveFileShards(fileList, fileSize, nodes)
	if len(shards) <= 1 {
		serialParam := *param
		serialParam.Parallel = false
		return c.compileExternScanSerialReadWrite(node, &serialParam, fileList, fileSize, strictSqlMode)
	}

	ss := make([]*Scope, 0, len(shards))
	stageNodes := c.queryWorkerStageNodes()
	currentFirstFlag := c.anal.isFirst
	for i := range shards {
		shard := shards[i]
		shardParam := new(tree.ExternParam)
		*shardParam = *param
		// Each fanout scope scans whole parquet files. Keeping Parallel=false
		// avoids the generic parallel path's per-file offset splitting while
		// preserving Extern.Filepath as the Hive base path for partition fills.
		shardParam.Parallel = false

		remote := param.ScanType == tree.S3 && len(stageNodes) > 0
		scope := c.constructScopeForExternalNode(shard.node, remote)
		scope.NodeInfo.Mcpu = 1
		scope.IsLoad = true
		op := constructExternal(
			node, shardParam, c.proc.Ctx,
			shard.fileList, shard.fileSize,
			makeWholeFileOffsets(len(shard.fileList)),
			strictSqlMode,
		)
		op.Es.ParquetWholeFileFanout = parquetWholeFileFanout
		op.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
		scope.setRootOperator(op)
		ss = append(ss, scope)
	}
	c.anal.isFirst = false
	return ss, nil
}

func (c *Compile) compileExternScanIcebergFileFanout(
	node *plan.Node,
	param *tree.ExternParam,
	runtime icebergExternalScanRuntime,
	strictSqlMode bool,
) ([]*Scope, error) {
	runtime.dataTasks = compactIcebergDataTasks(runtime.dataTasks)
	fileList, fileSize := icebergDataTaskFiles(runtime.dataTasks)
	shardParam := new(tree.ExternParam)
	*shardParam = *param
	shardParam.Parallel = false
	return c.compileExternScanIcebergShard(node, shardParam, runtime, icebergDataFileScopeShard{
		node:      engine.Node{Addr: c.addr, Mcpu: 1},
		fileList:  fileList,
		fileSize:  fileSize,
		dataTasks: runtime.dataTasks,
	}, strictSqlMode)
}

func (c *Compile) compileExternScanIcebergShard(
	node *plan.Node,
	param *tree.ExternParam,
	runtime icebergExternalScanRuntime,
	shard icebergDataFileScopeShard,
	strictSqlMode bool,
) ([]*Scope, error) {
	ss := make([]*Scope, 1)
	ss[0] = c.constructScopeForExternal(shard.node.Addr, param.Parallel)
	ss[0].NodeInfo.Mcpu = 1
	ss[0].IsLoad = true

	currentFirstFlag := c.anal.isFirst
	op := constructExternal(
		node, param, c.proc.Ctx,
		shard.fileList, shard.fileSize,
		makeWholeFileOffsets(len(shard.fileList)),
		strictSqlMode,
	)
	if err := attachIcebergRuntimeToExternal(c.proc.Ctx, op, runtime, shard.dataTasks); err != nil {
		return nil, err
	}
	op.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
	ss[0].setRootOperator(op)
	c.anal.isFirst = false
	return ss, nil
}

func (c *Compile) compileExternScanParquetRowGroupFanout(
	node *plan.Node,
	param *tree.ExternParam,
	fileList []string,
	fileSize []int64,
	rowGroups []parquetRowGroupMeta,
	strictSqlMode bool,
) ([]*Scope, error) {
	nodes := c.getHiveFileFanoutNodes(param, len(rowGroups))
	shards, err := splitParquetRowGroupShards(fileList, fileSize, rowGroups, nodes)
	if err != nil {
		return nil, err
	}
	if len(shards) <= 1 {
		serialParam := *param
		serialParam.Parallel = false
		return c.compileExternScanSerialReadWrite(node, &serialParam, fileList, fileSize, strictSqlMode)
	}

	ss := make([]*Scope, 0, len(shards))
	stageNodes := c.queryWorkerStageNodes()
	currentFirstFlag := c.anal.isFirst
	for i := range shards {
		shard := shards[i]
		shardParam := new(tree.ExternParam)
		*shardParam = *param
		shardParam.Parallel = false

		remote := param.ScanType == tree.S3 && len(stageNodes) > 0
		scope := c.constructScopeForExternalNode(shard.node, remote)
		scope.NodeInfo.Mcpu = 1
		scope.IsLoad = true
		op := constructExternal(
			node, shardParam, c.proc.Ctx,
			shard.fileList, shard.fileSize,
			makeWholeFileOffsets(len(shard.fileList)),
			strictSqlMode,
		)
		op.Es.ParquetRowGroupShards = shard.rowGroupShards
		op.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
		scope.setRootOperator(op)
		ss = append(ss, scope)
	}
	c.anal.isFirst = false
	return ss, nil
}

func (c *Compile) readLoadParquetRowGroupMetadata(
	node *plan.Node,
	param *tree.ExternParam,
	fileList []string,
	fileSize []int64,
) ([]parquetRowGroupMeta, parquetFooterStats, error) {
	ctx := param.Ctx
	if ctx == nil && c.proc != nil {
		ctx = c.proc.Ctx
	}
	if ctx == nil {
		ctx = context.Background()
	}

	start := time.Now()
	stats := parquetFooterStats{Files: len(fileList)}
	metas := make([]parquetRowGroupMeta, 0)
	for fileIdx, filePath := range fileList {
		size := hiveFileSizeAt(fileSize, fileIdx)
		var reader io.ReaderAt
		var footerReader *compileParquetFooterReaderAt
		if param.ScanType == tree.INLINE {
			reader = strings.NewReader(param.Data)
			size = int64(len(param.Data))
		} else {
			fs, readPath, err := plan2.GetForETLWithType(param, filePath)
			if err != nil {
				return nil, stats, err
			}
			if size <= 0 {
				st, err := fs.StatFile(ctx, readPath)
				if err != nil {
					return nil, stats, err
				}
				size = st.Size
			}
			footerReader = &compileParquetFooterReaderAt{
				fs:       fs,
				readPath: readPath,
				ctx:      ctx,
			}
			reader = footerReader
		}
		stats.Bytes += size

		// Planning only needs the schema, row count, and row-group boundaries.
		// Loading page indexes and bloom-filter headers here is unused work, and
		// row-group fanout would repeat it in every execution scope.
		f, err := openParquetLoadMetadataFile(reader, size)
		if footerReader != nil {
			stats.ReadCalls += footerReader.readCalls
			stats.ReadBytes += footerReader.readBytes
		}
		if err != nil {
			return nil, stats, moerr.ConvertGoError(ctx, err)
		}
		if f.NumRows() == 0 {
			if err := validateEmptyParquetLoadFile(ctx, node, param, f); err != nil {
				return nil, stats, err
			}
			continue
		}
		rowGroups := f.RowGroups()
		stats.RowGroups += len(rowGroups)
		totalRows := f.NumRows()
		for rowGroupIdx, rowGroup := range rowGroups {
			rows := rowGroup.NumRows()
			stats.Rows += rows
			metas = append(metas, parquetRowGroupMeta{
				fileIndex:     int32(fileIdx),
				rowGroupIndex: int32(rowGroupIdx),
				numRows:       rows,
				bytes:         estimateParquetRowGroupBytes(size, totalRows, rows, len(rowGroups)),
			})
		}
	}
	stats.Duration = time.Since(start)
	return metas, stats, nil
}

func openParquetLoadMetadataFile(reader io.ReaderAt, size int64) (*parquet.File, error) {
	return parquet.OpenFile(reader, size,
		parquet.SkipPageIndex(true),
		parquet.SkipBloomFilters(true),
	)
}

func validateEmptyParquetLoadFile(ctx context.Context, node *plan.Node, param *tree.ExternParam, f *parquet.File) error {
	if param == nil || f == nil {
		return nil
	}
	attrs := buildExternalAttrs(node)
	if param.ExternType == int32(plan.ExternType_LOAD) &&
		externalColumnListLen(node) > int32(len(attrs)) {
		return moerr.NewNYI(ctx, "parquet load with @variables in column list")
	}
	if param.HivePartitioning {
		return nil
	}
	parquetColCnt := len(f.Root().Columns())
	tableColCnt := getCompileParquetExpectedColCnt(param, attrs)
	if parquetColCnt != tableColCnt {
		return moerr.NewInvalidInputf(ctx,
			"column count mismatch: parquet file has %d columns, but table has %d columns",
			parquetColCnt, tableColCnt)
	}
	return nil
}

func getCompileParquetExpectedColCnt(param *tree.ExternParam, attrs []plan.ExternAttr) int {
	cnt := 0
	for _, attr := range attrs {
		if catalog.ContainExternalHidenCol(attr.ColName) {
			continue
		}
		if compileParquetIsHivePartitionCol(param, attr.ColName) {
			continue
		}
		cnt++
	}
	return cnt
}

func compileParquetIsHivePartitionCol(param *tree.ExternParam, colName string) bool {
	if param == nil || !param.HivePartitioning {
		return false
	}
	lower := strings.ToLower(colName)
	for _, pc := range param.HivePartitionCols {
		if pc == lower {
			return true
		}
	}
	return false
}

type compileParquetFooterReaderAt struct {
	fs        fileservice.ETLFileService
	readPath  string
	ctx       context.Context
	readCalls int64
	readBytes int64
}

func (r *compileParquetFooterReaderAt) ReadAt(p []byte, off int64) (n int, err error) {
	vec := fileservice.IOVector{
		FilePath: r.readPath,
		Policy:   fileservice.SkipFullFilePreloads,
		Entries: []fileservice.IOEntry{
			{
				Offset: off,
				Size:   int64(len(p)),
				Data:   p,
			},
		},
	}
	if err := r.fs.Read(r.ctx, &vec); err != nil {
		return 0, err
	}
	size := vec.Entries[0].Size
	r.readCalls++
	r.readBytes += size
	return int(size), nil
}

func estimateParquetRowGroupBytes(fileSize, totalRows, rowGroupRows int64, rowGroupCount int) int64 {
	if fileSize <= 0 {
		return 1
	}
	if totalRows > 0 && rowGroupRows > 0 {
		size := int64(float64(fileSize) * float64(rowGroupRows) / float64(totalRows))
		if size > 0 {
			return size
		}
		return 1
	}
	if rowGroupCount > 0 {
		size := fileSize / int64(rowGroupCount)
		if size > 0 {
			return size
		}
	}
	return 1
}

func parquetRowGroupFileCount(rowGroups []parquetRowGroupMeta) int {
	files := make(map[int32]struct{})
	for _, meta := range rowGroups {
		files[meta.fileIndex] = struct{}{}
	}
	return len(files)
}

// parquetLoadFileFanoutSaturates reports whether whole-file fanout can fill
// every bounded execution scope.  It deliberately uses the uncapped execution
// DOP rather than getHiveFileFanoutNodes(fileCount): the latter is capped by
// fileCount and therefore cannot tell whether additional row-group scopes
// would be useful.
func (c *Compile) parquetLoadFileFanoutSaturates(param *tree.ExternParam, fileCount int) bool {
	return fileCount > 1 && fileCount >= c.parquetLoadFileFanoutDOP(param)
}

func (c *Compile) parquetLoadFileFanoutDOP(param *tree.ExternParam) int {
	stageNodes := c.queryWorkerStageNodes()
	if param != nil && param.ScanType == tree.S3 && len(stageNodes) > 0 {
		dop := 0
		for _, node := range stageNodes {
			mcpu := node.Mcpu
			if mcpu <= 0 {
				mcpu = 1
			}
			dop += min(mcpu, external.S3ParallelMaxnum)
		}
		if dop > 0 {
			return dop
		}
	}
	if c.ncpu > 0 {
		return c.ncpu
	}
	return 1
}

func (c *Compile) getHiveFileFanoutNodes(param *tree.ExternParam, fileCount int) []engine.Node {
	if fileCount <= 0 {
		return nil
	}
	stageNodes := c.queryWorkerStageNodes()
	if param.ScanType == tree.S3 && len(stageNodes) > 0 {
		nodes := make([]engine.Node, 0, fileCount)
		for _, node := range stageNodes {
			mcpu := node.Mcpu
			if mcpu <= 0 {
				mcpu = 1
			}
			if mcpu > external.S3ParallelMaxnum {
				mcpu = external.S3ParallelMaxnum
			}
			for i := 0; i < mcpu && len(nodes) < fileCount; i++ {
				n := node
				n.Mcpu = 1
				nodes = append(nodes, n)
			}
			if len(nodes) >= fileCount {
				break
			}
		}
		if len(nodes) > 0 {
			return nodes
		}
	}

	mcpu := c.ncpu
	if mcpu <= 0 {
		mcpu = 1
	}
	if mcpu > fileCount {
		mcpu = fileCount
	}
	nodes := make([]engine.Node, mcpu)
	for i := range nodes {
		nodes[i] = engine.Node{Addr: c.addr, Mcpu: 1}
	}
	return nodes
}

func splitHiveFileShards(fileList []string, fileSize []int64, nodes []engine.Node) []hiveFileShard {
	if len(fileList) == 0 || len(nodes) == 0 {
		return nil
	}
	shardCount := len(nodes)
	if shardCount > len(fileList) {
		shardCount = len(fileList)
	}
	shards := make([]hiveFileShard, shardCount)
	loads := make([]int64, shardCount)
	for i := range shards {
		shards[i].node = nodes[i]
	}

	indices := make([]int, len(fileList))
	for i := range indices {
		indices[i] = i
	}
	slices.SortStableFunc(indices, func(a, b int) int {
		return cmp.Compare(hiveFileSizeAt(fileSize, b), hiveFileSizeAt(fileSize, a))
	})

	for _, fileIdx := range indices {
		shardIdx := 0
		for i := 1; i < shardCount; i++ {
			if loads[i] < loads[shardIdx] ||
				(loads[i] == loads[shardIdx] && len(shards[i].fileList) < len(shards[shardIdx].fileList)) {
				shardIdx = i
			}
		}
		shards[shardIdx].fileList = append(shards[shardIdx].fileList, fileList[fileIdx])
		size := hiveFileSizeAt(fileSize, fileIdx)
		shards[shardIdx].fileSize = append(shards[shardIdx].fileSize, size)
		loads[shardIdx] += size
	}

	nonEmpty := shards[:0]
	for _, shard := range shards {
		if len(shard.fileList) > 0 {
			nonEmpty = append(nonEmpty, shard)
		}
	}
	return nonEmpty
}

func splitIcebergDataFileShards(tasks []*pipeline.IcebergDataFileTask, nodes []engine.Node) []icebergDataFileScopeShard {
	if len(tasks) == 0 || len(nodes) == 0 {
		return nil
	}
	shardCount := len(nodes)
	if shardCount > len(tasks) {
		shardCount = len(tasks)
	}
	shards := make([]icebergDataFileScopeShard, shardCount)
	loads := make([]int64, shardCount)
	for i := range shards {
		shards[i].node = nodes[i]
	}

	indices := make([]int, len(tasks))
	for i := range indices {
		indices[i] = i
	}
	slices.SortStableFunc(indices, func(leftIdx, rightIdx int) int {
		left := icebergDataTaskLoad(tasks[leftIdx])
		right := icebergDataTaskLoad(tasks[rightIdx])
		if left != right {
			return cmp.Compare(right, left)
		}
		return cmp.Compare(tasks[leftIdx].FilePath, tasks[rightIdx].FilePath)
	})

	for _, taskIdx := range indices {
		shardIdx := 0
		for i := 1; i < shardCount; i++ {
			if loads[i] < loads[shardIdx] ||
				(loads[i] == loads[shardIdx] && len(shards[i].dataTasks) < len(shards[shardIdx].dataTasks)) {
				shardIdx = i
			}
		}
		task := tasks[taskIdx]
		shards[shardIdx].dataTasks = append(shards[shardIdx].dataTasks, task)
		shards[shardIdx].fileList = append(shards[shardIdx].fileList, task.FilePath)
		size := task.FileSize
		shards[shardIdx].fileSize = append(shards[shardIdx].fileSize, size)
		loads[shardIdx] += icebergDataTaskLoad(task)
	}

	nonEmpty := shards[:0]
	for _, shard := range shards {
		if len(shard.dataTasks) > 0 {
			nonEmpty = append(nonEmpty, shard)
		}
	}
	return nonEmpty
}

func compactIcebergDataTasks(tasks []*pipeline.IcebergDataFileTask) []*pipeline.IcebergDataFileTask {
	if len(tasks) == 0 {
		return nil
	}
	out := tasks[:0]
	for _, task := range tasks {
		if task != nil {
			out = append(out, task)
		}
	}
	return out
}

func icebergDataTaskLoad(task *pipeline.IcebergDataFileTask) int64 {
	if task == nil {
		return 1
	}
	if task.FileSize > 0 {
		return task.FileSize
	}
	if task.RecordCount > 0 {
		return task.RecordCount
	}
	return 1
}

func icebergDataTaskFiles(tasks []*pipeline.IcebergDataFileTask) ([]string, []int64) {
	fileList := make([]string, 0, len(tasks))
	fileSize := make([]int64, 0, len(tasks))
	for _, task := range tasks {
		if task == nil {
			continue
		}
		fileList = append(fileList, task.FilePath)
		fileSize = append(fileSize, task.FileSize)
	}
	return fileList, fileSize
}

func attachIcebergRuntimeToExternal(
	ctx context.Context,
	op *external.External,
	runtime icebergExternalScanRuntime,
	dataTasks []*pipeline.IcebergDataFileTask,
) error {
	if ref := strings.TrimSpace(runtime.objectIORef); ref != "" && !icebergio.RetainObjectIORef(ref) {
		return moerr.NewInternalError(ctx, "Iceberg object IO ref is not registered or expired")
	}
	ensureIcebergHiddenReadColumns(op.Es, runtime.columns)
	op.Es.Attrs = icebergProjectedAttrs(op.Es.Attrs, runtime.columns, runtime.hiddenReadCols)
	op.Es.IcebergDataTasks = dataTasks
	op.Es.IcebergDeleteTasks = filterIcebergDeleteTasksForDataFiles(runtime.deleteTasks, dataTasks)
	op.Es.IcebergColumns = runtime.columns
	op.Es.IcebergSnapshot = runtime.snapshot
	op.Es.IcebergObjectIORef = runtime.objectIORef
	op.Es.IcebergHiddenReadCols = runtime.hiddenReadCols
	op.Es.IcebergPlanningStats = runtime.planningStats
	op.Es.NeedRowOrdinal = runtime.needRowOrdinal
	op.Es.IcebergDeleteMaxMemoryBytes = runtime.deleteMaxMemoryBytes
	op.Es.IcebergDeleteSpillEnabled = runtime.deleteSpillEnabled
	op.Es.ParquetRowGroupShards = icebergDataTaskRowGroupShards(dataTasks)
	return nil
}

func ensureIcebergHiddenReadColumns(
	param *external.ExternalParam,
	columns []*pipeline.IcebergColumnMapping,
) {
	if param == nil || len(columns) == 0 {
		return
	}
	attrByIndex := make(map[int32]struct{}, len(param.Attrs))
	for _, attr := range param.Attrs {
		attrByIndex[attr.ColIndex] = struct{}{}
	}
	for _, column := range columns {
		if column == nil || !column.IsHidden || column.MoColIndex < 0 {
			continue
		}
		idx := int(column.MoColIndex)
		for len(param.Cols) <= idx {
			param.Cols = append(param.Cols, nil)
		}
		if param.Cols[idx] == nil {
			moType := plan.Type{}
			if column.MoType != nil {
				moType = *column.MoType
			}
			param.Cols[idx] = &plan.ColDef{
				Name: icebergFirstNonEmpty(column.CurrentFieldName, column.SnapshotFieldName),
				Typ:  moType,
			}
		}
		if _, ok := attrByIndex[column.MoColIndex]; ok {
			continue
		}
		param.Attrs = append(param.Attrs, plan.ExternAttr{
			ColName:  icebergFirstNonEmpty(column.CurrentFieldName, column.SnapshotFieldName),
			ColIndex: column.MoColIndex,
		})
		attrByIndex[column.MoColIndex] = struct{}{}
	}
}

func icebergFirstNonEmpty(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func icebergDataTaskRowGroupShards(tasks []*pipeline.IcebergDataFileTask) []*pipeline.ParquetRowGroupShard {
	if len(tasks) == 0 {
		return nil
	}
	out := make([]*pipeline.ParquetRowGroupShard, 0, len(tasks))
	for idx, task := range tasks {
		if task == nil || task.RowGroupEnd <= task.RowGroupStart {
			continue
		}
		out = append(out, &pipeline.ParquetRowGroupShard{
			FileIndex:     int32(idx),
			RowGroupStart: task.RowGroupStart,
			RowGroupEnd:   task.RowGroupEnd,
			NumRows:       task.RecordCount,
			Bytes:         task.FileSize,
		})
	}
	return out
}

func icebergProjectedAttrs(
	attrs []plan.ExternAttr,
	columns []*pipeline.IcebergColumnMapping,
	hiddenReadCols []int32,
) []plan.ExternAttr {
	if len(attrs) == 0 || (len(columns) == 0 && len(hiddenReadCols) == 0) {
		return attrs
	}
	needed := make(map[int32]struct{}, len(columns)+len(hiddenReadCols))
	for _, mapping := range columns {
		if mapping == nil {
			continue
		}
		needed[mapping.MoColIndex] = struct{}{}
	}
	for _, colIdx := range hiddenReadCols {
		needed[colIdx] = struct{}{}
	}
	if len(needed) == 0 {
		return attrs
	}
	out := attrs[:0]
	for _, attr := range attrs {
		if _, ok := needed[attr.ColIndex]; ok || isIcebergDMLMetadataColumnName(attr.ColName) {
			out = append(out, attr)
		}
	}
	return out
}

func filterIcebergDeleteTasksForDataFiles(
	deleteTasks []*pipeline.IcebergDeleteFileTask,
	dataTasks []*pipeline.IcebergDataFileTask,
) []*pipeline.IcebergDeleteFileTask {
	if len(deleteTasks) == 0 || len(dataTasks) == 0 {
		return nil
	}
	dataFiles := make(map[string]bool, len(dataTasks))
	for _, task := range dataTasks {
		if task != nil && task.FilePath != "" {
			dataFiles[task.FilePath] = true
		}
	}
	filtered := make([]*pipeline.IcebergDeleteFileTask, 0, len(deleteTasks))
	for _, task := range deleteTasks {
		if task == nil {
			continue
		}
		if task.ReferencedDataFile == "" || dataFiles[task.ReferencedDataFile] {
			filtered = append(filtered, task)
		}
	}
	return filtered
}

func splitParquetRowGroupShards(
	fileList []string,
	fileSize []int64,
	rowGroups []parquetRowGroupMeta,
	nodes []engine.Node,
) ([]parquetRowGroupScopeShard, error) {
	if len(rowGroups) == 0 || len(nodes) == 0 {
		return nil, nil
	}
	shardCount := len(nodes)
	if shardCount > len(rowGroups) {
		shardCount = len(rowGroups)
	}
	shards := make([]parquetRowGroupScopeShard, shardCount)
	loads := make([]int64, shardCount)
	for i := range shards {
		shards[i].node = nodes[i]
		shards[i].originalToLocal = make(map[int32]int32)
	}

	rowGroupsByFile := make(map[int32][]parquetRowGroupMeta)
	for _, meta := range rowGroups {
		if meta.fileIndex < 0 || int(meta.fileIndex) >= len(fileList) {
			return nil, moerr.NewInternalErrorNoCtxf(
				"invalid parquet row group file index %d for %d files",
				meta.fileIndex, len(fileList),
			)
		}
		if meta.rowGroupIndex < 0 {
			return nil, moerr.NewInternalErrorNoCtxf(
				"invalid parquet row group index %d for file index %d",
				meta.rowGroupIndex, meta.fileIndex,
			)
		}
		rowGroupsByFile[meta.fileIndex] = append(rowGroupsByFile[meta.fileIndex], meta)
	}

	fileIndexes := make([]int32, 0, len(rowGroupsByFile))
	for fileIndex := range rowGroupsByFile {
		fileIndexes = append(fileIndexes, fileIndex)
	}
	slices.Sort(fileIndexes)
	segments := make([]parquetRowGroupSegment, 0, shardCount)
	for _, fileIndex := range fileIndexes {
		fileRowGroups := rowGroupsByFile[fileIndex]
		slices.SortStableFunc(fileRowGroups, func(left, right parquetRowGroupMeta) int {
			return cmp.Compare(left.rowGroupIndex, right.rowGroupIndex)
		})
		segments = append(segments, splitContiguousParquetRowGroups(
			fileIndex, fileRowGroups, min(shardCount, len(fileRowGroups)))...)
	}
	slices.SortStableFunc(segments, func(left, right parquetRowGroupSegment) int {
		if c := cmp.Compare(right.load, left.load); c != 0 {
			return c
		}
		if c := cmp.Compare(left.fileIndex, right.fileIndex); c != 0 {
			return c
		}
		return cmp.Compare(left.rowGroups[0].rowGroupIndex, right.rowGroups[0].rowGroupIndex)
	})

	for _, segment := range segments {

		shardIdx := 0
		for i := 1; i < shardCount; i++ {
			if loads[i] < loads[shardIdx] ||
				(loads[i] == loads[shardIdx] && len(shards[i].rowGroupShards) < len(shards[shardIdx].rowGroupShards)) {
				shardIdx = i
			}
		}

		localFileIndex := appendParquetShardFile(&shards[shardIdx], fileList, fileSize, segment.fileIndex)
		for _, meta := range segment.rowGroups {
			shards[shardIdx].rowGroupShards = append(shards[shardIdx].rowGroupShards, &pipeline.ParquetRowGroupShard{
				FileIndex:     localFileIndex,
				RowGroupStart: meta.rowGroupIndex,
				RowGroupEnd:   meta.rowGroupIndex + 1,
				NumRows:       meta.numRows,
				Bytes:         meta.bytes,
			})
		}
		loads[shardIdx] = addParquetLoad(loads[shardIdx], segment.load)
	}

	nonEmpty := shards[:0]
	for _, shard := range shards {
		if len(shard.rowGroupShards) == 0 {
			continue
		}
		slices.SortStableFunc(shard.rowGroupShards, func(left, right *pipeline.ParquetRowGroupShard) int {
			if c := cmp.Compare(left.FileIndex, right.FileIndex); c != 0 {
				return c
			}
			return cmp.Compare(left.RowGroupStart, right.RowGroupStart)
		})
		shard.rowGroupShards = mergeAdjacentParquetRowGroupShards(shard.rowGroupShards)
		shard.originalToLocal = nil
		nonEmpty = append(nonEmpty, shard)
	}
	return nonEmpty, nil
}

func splitContiguousParquetRowGroups(
	fileIndex int32,
	rowGroups []parquetRowGroupMeta,
	partCount int,
) []parquetRowGroupSegment {
	if len(rowGroups) == 0 || partCount <= 0 {
		return nil
	}
	partCount = min(partCount, len(rowGroups))
	remainingLoad := int64(0)
	for _, meta := range rowGroups {
		remainingLoad = addParquetLoad(remainingLoad, parquetRowGroupLoad(meta))
	}

	segments := make([]parquetRowGroupSegment, 0, partCount)
	start := 0
	for part := 0; part < partCount; part++ {
		partsLeft := partCount - part
		if partsLeft == 1 {
			segments = append(segments, makeParquetRowGroupSegment(fileIndex, rowGroups[start:]))
			break
		}

		target := remainingLoad / int64(partsLeft)
		if remainingLoad%int64(partsLeft) != 0 {
			target++
		}
		endLimit := len(rowGroups) - (partsLeft - 1)
		end := start
		load := int64(0)
		for end < endLimit {
			nextLoad := addParquetLoad(load, parquetRowGroupLoad(rowGroups[end]))
			if end > start && parquetLoadDistance(load, target) <= parquetLoadDistance(nextLoad, target) {
				break
			}
			load = nextLoad
			end++
		}
		if end == start {
			end++
		}
		segment := makeParquetRowGroupSegment(fileIndex, rowGroups[start:end])
		segments = append(segments, segment)
		remainingLoad -= min(remainingLoad, segment.load)
		start = end
	}
	return segments
}

func makeParquetRowGroupSegment(fileIndex int32, rowGroups []parquetRowGroupMeta) parquetRowGroupSegment {
	segment := parquetRowGroupSegment{fileIndex: fileIndex, rowGroups: rowGroups}
	for _, meta := range rowGroups {
		segment.load = addParquetLoad(segment.load, parquetRowGroupLoad(meta))
	}
	return segment
}

func addParquetLoad(left, right int64) int64 {
	if right > math.MaxInt64-left {
		return math.MaxInt64
	}
	return left + right
}

func parquetLoadDistance(left, right int64) int64 {
	if left >= right {
		return left - right
	}
	return right - left
}

func appendParquetShardFile(shard *parquetRowGroupScopeShard, fileList []string, fileSize []int64, originalFileIndex int32) int32 {
	if localFileIndex, ok := shard.originalToLocal[originalFileIndex]; ok {
		return localFileIndex
	}
	localFileIndex := int32(len(shard.fileList))
	shard.originalToLocal[originalFileIndex] = localFileIndex
	shard.fileList = append(shard.fileList, fileList[originalFileIndex])
	shard.fileSize = append(shard.fileSize, hiveFileSizeAt(fileSize, int(originalFileIndex)))
	return localFileIndex
}

func mergeAdjacentParquetRowGroupShards(shards []*pipeline.ParquetRowGroupShard) []*pipeline.ParquetRowGroupShard {
	merged := shards[:0]
	for _, shard := range shards {
		if len(merged) > 0 {
			last := merged[len(merged)-1]
			if last.FileIndex == shard.FileIndex && last.RowGroupEnd == shard.RowGroupStart {
				last.RowGroupEnd = shard.RowGroupEnd
				last.NumRows += shard.NumRows
				last.Bytes += shard.Bytes
				continue
			}
		}
		merged = append(merged, shard)
	}
	return merged
}

func parquetRowGroupLoad(meta parquetRowGroupMeta) int64 {
	if meta.bytes > 0 {
		return meta.bytes
	}
	if meta.numRows > 0 {
		return meta.numRows
	}
	return 1
}

func hiveFileSizeAt(fileSize []int64, idx int) int64 {
	if idx >= 0 && idx < len(fileSize) {
		return fileSize[idx]
	}
	return 0
}

func makeWholeFileOffsets(count int) []*pipeline.FileOffset {
	offsets := make([]*pipeline.FileOffset, count)
	for i := range offsets {
		offsets[i] = &pipeline.FileOffset{Offset: []int64{0, -1}}
	}
	return offsets
}

func (c *Compile) compileExternScanParallelReadWrite(node *plan.Node, param *tree.ExternParam, fileList []string, fileSize []int64, strictSqlMode bool) ([]*Scope, error) {
	if param.Format == tree.PARQUET {
		return nil, moerr.NewInternalError(c.proc.Ctx, "parquet load cannot use byte-offset parallel read")
	}
	visibleCols := make([]*plan.ColDef, 0)
	if param.Strict {
		for _, col := range node.TableDef.Cols {
			if !col.Hidden {
				visibleCols = append(visibleCols, col)
			}
		}
	}

	var mcpu int
	var ID2Addr map[int]int = make(map[int]int, 0)
	stageNodes := c.queryWorkerStageNodes()
	if len(stageNodes) == 0 {
		return nil, moerr.NewInternalErrorNoCtx("external scan stage has no query workers")
	}

	if param.ScanType == tree.S3 {
		for i := 0; i < len(stageNodes); i++ {
			tmp := mcpu
			if stageNodes[i].Mcpu > external.S3ParallelMaxnum {
				mcpu += external.S3ParallelMaxnum
			} else {
				mcpu += stageNodes[i].Mcpu
			}
			ID2Addr[i] = mcpu - tmp
		}
	} else {
		for i := 0; i < len(stageNodes); i++ {
			tmp := mcpu
			mcpu += stageNodes[i].Mcpu
			ID2Addr[i] = mcpu - tmp
		}
	}

	parallelSize := GetExternParallelSize(fileSize[0], mcpu)

	var fileOffset [][]int64
	for i := 0; i < len(fileList); i++ {
		param.Filepath = fileList[i]
		arr, err := external.ReadFileOffset(param, parallelSize, fileSize[i], visibleCols)
		fileOffset = append(fileOffset, arr)
		if err != nil {
			return nil, err
		}
	}

	var ss []*Scope
	pre := 0
	currentFirstFlag := c.anal.isFirst
	for i := 0; i < len(stageNodes); i++ {
		scope := c.constructScopeForExternalNode(stageNodes[i], param.Parallel)
		ss = append(ss, scope)
		scope.IsLoad = true
		count := min(parallelSize, ID2Addr[i])
		scope.NodeInfo.Mcpu = count
		fileOffsetTmp := make([]*pipeline.FileOffset, len(fileList))
		for j := range fileOffsetTmp {
			preIndex := pre
			fileOffsetTmp[j] = &pipeline.FileOffset{}
			fileOffsetTmp[j].Offset = make([]int64, 0)
			if param.Strict {
				if 2*preIndex+2*count < len(fileOffset[j]) {
					fileOffsetTmp[j].Offset = append(fileOffsetTmp[j].Offset, fileOffset[j][2*preIndex:2*preIndex+2*count]...)
				} else if 2*preIndex < len(fileOffset[j]) {
					fileOffsetTmp[j].Offset = append(fileOffsetTmp[j].Offset, fileOffset[j][2*preIndex:]...)
				} else {
					continue
				}
			} else {
				fileOffsetTmp[j].Offset = append(fileOffsetTmp[j].Offset, fileOffset[j][2*preIndex:2*preIndex+2*count]...)
			}
		}
		logutil.Infof("compileExternScanParallelReadWrite, len of cnList is %d, cn addr is %s, mcpu is %d, filepath is %s, file size is %d", len(stageNodes), stageNodes[i].Addr, scope.NodeInfo.Mcpu, param.ExParamConst.Filepath, param.ExParamConst.FileSize)
		logutil.Infof("compileExternScanParallelReadWrite, %v\n", fileOffsetTmp)
		op := constructExternal(node, param, c.proc.Ctx, fileList, fileSize, fileOffsetTmp, strictSqlMode)
		op.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
		scope.setRootOperator(op)
		pre += count
		if parallelSize <= count {
			break
		}
		parallelSize -= count
	}
	c.anal.isFirst = false
	return ss, nil
}

func (c *Compile) compileExternScanSerialReadWrite(node *plan.Node, param *tree.ExternParam, fileList []string, fileSize []int64, strictSqlMode bool) ([]*Scope, error) {
	ss := make([]*Scope, 1)
	ss[0] = c.constructScopeForExternal(c.addr, param.Parallel)

	currentFirstFlag := c.anal.isFirst
	ss[0].IsLoad = true
	fileOffsetTmp := make([]*pipeline.FileOffset, len(fileList))
	for j := range fileOffsetTmp {
		fileOffsetTmp[j] = &pipeline.FileOffset{}
		fileOffsetTmp[j].Offset = make([]int64, 0)
		fileOffsetTmp[j].Offset = append(fileOffsetTmp[j].Offset, []int64{param.FileStartOff, -1}...)
	}
	op := constructExternal(node, param, c.proc.Ctx, fileList, fileSize, fileOffsetTmp, strictSqlMode)
	op.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
	ss[0].setRootOperator(op)
	c.anal.isFirst = false
	return ss, nil
}

func (c *Compile) getTableFunctionParallelSize(node *plan.Node, mcpu int) int {
	if node.TableDef.TblFunc.Name != "generate_series" {
		return 1
	}

	temp := max(int(node.Stats.Cost)/80000, 1)
	return min(temp, mcpu)
}

func (c *Compile) generateSeriesParallel(proc *process.Process, node *plan.Node, parallelSize int) (canOpt bool, step int64, offset [][2]int64, err error) {
	for _, expr := range node.TblFuncExprList {
		_, ok := expr.Expr.(*plan.Expr_Lit)
		if !ok {
			return false, 0, nil, nil
		}
	}

	var start, end int64
	switch val := node.TblFuncExprList[0].Expr.(*plan.Expr_Lit).Lit.GetValue().(type) {
	case *plan.Literal_I32Val:
		if len(node.TblFuncExprList) == 1 {
			start = 1
			end = int64(val.I32Val)
		} else {
			start = int64(val.I32Val)
			if val, ok := node.TblFuncExprList[1].Expr.(*plan.Expr_Lit).Lit.GetValue().(*plan.Literal_I32Val); ok {
				end = int64(val.I32Val)
			} else if val, ok := node.TblFuncExprList[1].Expr.(*plan.Expr_Lit).Lit.GetValue().(*plan.Literal_I64Val); ok {
				end = val.I64Val
			} else {
				return false, 0, nil, moerr.NewInvalidInput(proc.Ctx, "generate_series end must be int32 or int64")
			}
		}

	case *plan.Literal_I64Val:
		if len(node.TblFuncExprList) == 1 {
			start = 1
			end = int64(val.I64Val)
		} else {
			start = int64(val.I64Val)
			if val, ok := node.TblFuncExprList[1].Expr.(*plan.Expr_Lit).Lit.GetValue().(*plan.Literal_I32Val); ok {
				end = int64(val.I32Val)
			} else if val, ok := node.TblFuncExprList[1].Expr.(*plan.Expr_Lit).Lit.GetValue().(*plan.Literal_I64Val); ok {
				end = val.I64Val
			} else {
				return false, 0, nil, moerr.NewInvalidInput(proc.Ctx, "generate_series end must be int32 or int64")
			}
		}
	default:
		return false, 0, nil, nil
	}

	if len(node.TblFuncExprList) == 3 {
		if val, ok := node.TblFuncExprList[2].Expr.(*plan.Expr_Lit).Lit.GetValue().(*plan.Literal_I32Val); ok {
			step = int64(val.I32Val)
		} else if val, ok := node.TblFuncExprList[2].Expr.(*plan.Expr_Lit).Lit.GetValue().(*plan.Literal_I64Val); ok {
			step = val.I64Val
		} else {
			return false, 0, nil, moerr.NewInvalidInput(proc.Ctx, "generate_series step must be int32 or int64")
		}
	} else {
		if start < end {
			step = 1
		} else {
			step = -1
		}
	}
	if step == 0 {
		return false, 0, nil, moerr.NewInvalidInput(proc.Ctx, "generate_series step cannot be zero")
	}

	if parallelSize == 1 {
		return true, 0, nil, nil
	}

	temp := (end - start + 1) / int64(parallelSize)
	for i := 0; i < parallelSize; i++ {
		tempEnd := start + temp - 1
		if i == parallelSize-1 {
			tempEnd = end
		}

		arr := [2]int64{start, tempEnd}
		offset = append(offset, arr)
		start = tempEnd + 1
	}

	return true, step, offset, nil
}

func (c *Compile) compileSingleTableFunction(node *plan.Node) ([]*Scope, error) {
	currentFirstFlag := c.anal.isFirst
	ds := newScope(Merge)
	ds.NodeInfo = getEngineNode(c)
	ds.DataSource = &Source{isConst: true, node: node}
	ds.NodeInfo = engine.Node{Addr: c.addr, Mcpu: 1}
	ds.Proc = c.proc.NewNoContextChildProc(0)
	op := constructTableFunction(node, c.pn.GetQuery())
	op.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
	ds.setRootOperator(op)
	ss := []*Scope{ds}
	c.anal.isFirst = false
	return ss, nil
}

func (c *Compile) compileGenerateSeriesParallel(node *plan.Node, ss []*Scope, parallelSize int, canOpt bool, offset [][2]int64, step int64) ([]*Scope, error) {
	currentFirstFlag := c.anal.isFirst
	startOffset := 0
	for i := 0; i < len(c.cnList); i++ {
		ds := newScope(Merge)
		currMcpu := min(c.cnList[i].Mcpu, parallelSize)
		op := constructTableFunction(node, c.pn.GetQuery())
		op.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)

		if currMcpu > 1 {
			ds.Magic = Remote
		}

		op.CanOpt = canOpt
		op.GenerateSeriesCtrNumState(offset[0][0], offset[len(offset)-1][1], step, offset[0][0])
		op.OffsetTotal = append(op.OffsetTotal, offset[startOffset:startOffset+currMcpu]...)
		startOffset += currMcpu

		ds.NodeInfo = getEngineNode(c)
		ds.DataSource = &Source{isConst: true, node: node}

		ds.NodeInfo = engine.Node{Addr: c.addr, Mcpu: currMcpu}
		ds.Proc = c.proc.NewNoContextChildProc(0)
		parallelSize -= currMcpu
		ds.IsTbFunc = true
		ss = append(ss, ds)

		ds.setRootOperator(op)
		if parallelSize == 0 {
			break
		}
	}
	c.anal.isFirst = false
	return ss, nil
}

func (c *Compile) compileTableFunction(node *plan.Node, ss []*Scope) ([]*Scope, error) {
	currentFirstFlag := c.anal.isFirst

	if len(node.Children) == 0 {
		switch node.TableDef.TblFunc.Name {
		case "generate_series":
			var mcpuTotal int
			for i := 0; i < len(c.cnList); i++ {
				mcpuTotal += c.cnList[i].Mcpu
			}
			parallelSize := c.getTableFunctionParallelSize(node, mcpuTotal)
			canOpt, step, offset, err := c.generateSeriesParallel(c.proc, node, parallelSize)
			if err != nil {
				return nil, err
			}
			if parallelSize == 1 || !canOpt {
				return c.compileSingleTableFunction(node)
			}
			return c.compileGenerateSeriesParallel(node, ss, parallelSize, canOpt, offset, step)
		default:
			return c.compileSingleTableFunction(node)
		}
	}
	for i := range ss {
		op := constructTableFunction(node, c.pn.GetQuery())
		op.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
		ss[i].setRootOperator(op)
	}
	c.anal.isFirst = false

	return ss, nil
}

func (c *Compile) compileValueScan(node *plan.Node) ([]*Scope, error) {
	ds := newScope(Merge)
	ds.NodeInfo = getEngineNode(c)
	ds.DataSource = &Source{isConst: true, node: node}
	ds.NodeInfo = engine.Node{Addr: c.addr, Mcpu: 1}
	ds.Proc = c.proc.NewNoContextChildProc(0)

	currentFirstFlag := c.anal.isFirst
	op, err := constructValueScan(c.proc, node)
	if err != nil {
		return nil, err
	}
	op.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
	ds.setRootOperator(op)
	c.anal.isFirst = false

	return []*Scope{ds}, nil
}

func (c *Compile) compileTableScan(node *plan.Node) ([]*Scope, error) {
	stats := statistic.StatsInfoFromContext(c.proc.GetTopContext())
	compileStart := time.Now()
	defer func() {
		stats.AddCompileTableScanConsumption(time.Since(compileStart))
	}()

	nodes, err := c.generateNodes(node)
	if err != nil {
		return nil, err
	}
	ss := make([]*Scope, 0, len(nodes))

	currentFirstFlag := c.anal.isFirst
	for i := range nodes {
		s, err := c.compileTableScanWithNode(node, nodes[i], currentFirstFlag)
		if err != nil {
			return nil, err
		}
		ss = append(ss, s)
	}
	c.anal.isFirst = false

	if len(node.AggList) > 0 {
		partialResults, _, _ := checkAggOptimize(node)
		if partialResults != nil {
			ss[0].HasPartialResults = true
		}
	}

	return ss, nil
}

func (c *Compile) compileTableScanWithNode(node *plan.Node, engNode engine.Node, firstFlag bool) (*Scope, error) {
	s := newScope(Remote)
	s.NodeInfo = engNode
	s.TxnOffset = c.TxnOffset
	s.DataSource = &Source{
		node: node,
	}

	op := constructTableScan(node)
	op.SetAnalyzeControl(c.anal.curNodeIdx, firstFlag)
	s.setRootOperator(op)
	s.Proc = c.proc.NewNoContextChildProc(0)
	return s, nil
}

func (c *Compile) compileVectorIndexScan(node *plan.Node) ([]*Scope, error) {
	var nodes engine.Nodes
	var workspace client.Workspace
	if txnOp := c.proc.GetTxnOperator(); txnOp != nil {
		workspace = txnOp.GetWorkspace()
	}
	if c.execType == plan2.ExecTypeAP_MULTICN && len(c.cnList) > 1 &&
		(workspace == nil || workspace.Readonly()) &&
		(node.Stats == nil || !node.Stats.ForceOneCN) {
		nodes = make(engine.Nodes, len(c.cnList))
		for i := range c.cnList {
			nodes[i] = engine.Node{
				Id:    c.cnList[i].Id,
				Addr:  c.cnList[i].Addr,
				Mcpu:  1,
				CNCNT: int32(len(c.cnList)),
				CNIDX: int32(i),
			}
		}
	} else {
		local := getEngineNode(c)
		local.Mcpu = 1
		local.CNCNT = 1
		local.CNIDX = 0
		nodes = engine.Nodes{local}
	}
	currentFirstFlag := c.anal.isFirst
	ss := make([]*Scope, 0, len(nodes))
	for i := range nodes {
		// One adaptive reader owns one centroid cursor and one bounded top-k.
		// Parallelism is expressed by independent CN partitions, not duplicate
		// readers over the same partition.
		nodes[i].Mcpu = 1
		nodeCopy := plan2.DeepCopyNode(node)
		s := newScope(Remote)
		s.NodeInfo = nodes[i]
		s.TxnOffset = c.TxnOffset
		s.DataSource = &Source{
			node:                    nodeCopy,
			vectorIndexScanTemplate: plan2.DeepCopyVectorIndexScan(nodeCopy.VectorIndexScan),
		}
		op := constructTableScan(nodeCopy)
		op.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
		s.setRootOperator(op)
		s.Proc = c.proc.NewNoContextChildProc(0)
		ss = append(ss, s)
	}
	c.anal.isFirst = false
	return ss, nil
}

func (c *Compile) getCompileTableScanDataSourceTxn(s *Scope) (client.TxnOperator, context.Context, error) {
	var err error
	var txnOp client.TxnOperator

	node := s.DataSource.node
	ctx := c.proc.GetTopContext()
	txnOp = c.proc.GetTxnOperator()
	err = disttae.CheckTxnIsValid(txnOp)
	if err != nil {
		return nil, nil, err
	}
	if node.ScanSnapshot != nil && node.ScanSnapshot.TS != nil {
		if !node.ScanSnapshot.TS.Equal(timestamp.Timestamp{LogicalTime: 0, PhysicalTime: 0}) &&
			node.ScanSnapshot.TS.Less(c.proc.GetTxnOperator().Txn().SnapshotTS) {
			if c.proc.GetCloneTxnOperator() != nil {
				txnOp = c.proc.GetCloneTxnOperator()
			} else {
				txnOp = c.proc.GetTxnOperator().CloneSnapshotOp(*node.ScanSnapshot.TS)
				c.proc.SetCloneTxnOperator(txnOp)
			}

			if node.ScanSnapshot.Tenant != nil {
				ctx = context.WithValue(ctx, defines.TenantIDKey{}, node.ScanSnapshot.Tenant.TenantID)
			}
		}
	}

	err = disttae.CheckTxnIsValid(txnOp)
	if err != nil {
		return nil, nil, err
	}
	if util.TableIsClusterTable(node.TableDef.GetTableType()) {
		ctx = defines.AttachAccountId(ctx, catalog.System_Account)
	}
	if node.ObjRef.PubInfo != nil {
		ctx = defines.AttachAccountId(ctx, uint32(node.ObjRef.PubInfo.TenantId))
	}
	if util.TableIsLoggingTable(node.ObjRef.SchemaName, node.ObjRef.ObjName) {
		ctx = defines.AttachAccountId(ctx, catalog.System_Account)
	}
	logCatalogSnapshotScan("compile.table-scan.txn", node, ctx, txnOp)
	return txnOp, ctx, nil
}

// normalizeVectorIndexScanSnapshot closes the two representations of a
// vector scan's snapshot before the generic datasource transaction selection
// runs.  The node-level snapshot is the compiler contract; the nested copy is
// the self-contained identity consumed by the index reader.  Keep a fallback
// from the nested field for plans produced before the node-level field was
// populated, then make independent copies so later expression folding or
// remote-scope cloning cannot mutate either representation.
func normalizeVectorIndexScanSnapshot(node *plan.Node) {
	if node == nil || node.VectorIndexScan == nil {
		return
	}
	snapshot := node.ScanSnapshot
	if snapshot == nil {
		snapshot = node.VectorIndexScan.ScanSnapshot
	}
	if snapshot == nil {
		return
	}
	node.ScanSnapshot = plan2.DeepCopySnapshot(snapshot)
	node.VectorIndexScan.ScanSnapshot = plan2.DeepCopySnapshot(node.ScanSnapshot)
}

func prepareVectorIndexScanForExecution(source *Source, proc *process.Process) (*plan.VectorIndexScan, error) {
	if source == nil || source.node == nil || source.node.VectorIndexScan == nil {
		return nil, moerr.NewInvalidInputNoCtx("vector index scan is missing its specification")
	}
	if source.vectorIndexScanTemplate == nil {
		source.vectorIndexScanTemplate = plan2.DeepCopyVectorIndexScan(source.node.VectorIndexScan)
	}
	spec, err := vectorscan.PrepareScalar(source.vectorIndexScanTemplate, proc)
	if err != nil {
		return nil, err
	}
	source.node.VectorIndexScan = spec
	return spec, nil
}

func (c *Compile) compileTableScanDataSource(s *Scope) error {
	var err error
	var tblDef *plan.TableDef
	var ts timestamp.Timestamp
	var db engine.Database

	node := s.DataSource.node
	attrs := make([]string, len(node.TableDef.Cols))
	for j, col := range node.TableDef.Cols {
		attrs[j] = col.GetOriginCaseName()
	}

	//-----------------------------------------------------------------------------------------------------

	txnOp, ctx, err := c.getCompileTableScanDataSourceTxn(s)
	if err != nil {
		return err
	}

	//-----------------------------------------------------------------------------------------------------

	if c.proc != nil && c.proc.GetTxnOperator() != nil {
		ts = txnOp.Txn().SnapshotTS
	}

	if s.DataSource.Rel == nil {
		var rel engine.Relation
		db, err = c.e.Database(ctx, node.ObjRef.SchemaName, txnOp)
		if err != nil {
			panic(err)
		}
		rel, err = db.Relation(ctx, node.TableDef.Name, c.proc)
		if err != nil {
			return err
		}
		s.DataSource.Rel = engine.NewRelationHandle(rel)
	} else if err = s.DataSource.Rel.Reset(txnOp); err != nil {
		return err
	}
	tblDef = s.DataSource.Rel.GetTableDef(ctx)

	storageFilters := filterScanStorageExprs(node.FilterList)
	if len(storageFilters) != len(s.DataSource.FilterList) {
		s.DataSource.FilterList = plan2.DeepCopyExprList(storageFilters)
		for _, e := range s.DataSource.FilterList {
			_, err := plan2.ReplaceFoldExpr(c.proc, e, &c.filterExprExes)
			if err != nil {
				return err
			}
		}
	}
	for _, e := range s.DataSource.FilterList {
		err = plan2.EvalFoldExpr(c.proc, e, &c.filterExprExes)
		if err != nil {
			return err
		}
	}
	s.DataSource.FilterExpr = colexec.RewriteFilterExprList(s.DataSource.FilterList)

	if len(node.BlockFilterList) != len(s.DataSource.BlockFilterList) {
		s.DataSource.BlockFilterList = plan2.DeepCopyExprList(node.BlockFilterList)
		for _, e := range s.DataSource.BlockFilterList {
			_, err := plan2.ReplaceFoldExpr(c.proc, e, &c.filterExprExes)
			if err != nil {
				return err
			}
		}
	}

	s.DataSource.Timestamp = ts
	s.DataSource.Attributes = attrs
	s.DataSource.TableDef = tblDef
	s.DataSource.RelationName = node.TableDef.Name
	s.DataSource.SchemaName = node.ObjRef.SchemaName
	s.DataSource.AccountId = node.ObjRef.GetPubInfo()
	s.DataSource.RuntimeFilterSpecs = node.RuntimeFilterProbeList
	s.DataSource.OrderBy = node.OrderBy
	s.DataSource.IndexReaderParam = node.IndexReaderParam
	s.DataSource.RecvMsgList = node.RecvMsgList

	return nil
}

// filterScanStorageExprs excludes row-dependent predicates from the engine
// reader. The complete node.FilterList remains owned by TableScan or Restrict,
// so these predicates are still evaluated once at the row-level boundary.
func filterScanStorageExprs(exprs []*plan.Expr) []*plan.Expr {
	for i, expr := range exprs {
		if plan2.ContainsVolatileFunction(expr) {
			filtered := make([]*plan.Expr, 0, len(exprs)-1)
			filtered = append(filtered, exprs[:i]...)
			for _, remaining := range exprs[i+1:] {
				if !plan2.ContainsVolatileFunction(remaining) {
					filtered = append(filtered, remaining)
				}
			}
			return filtered
		}
	}
	return exprs
}

func (c *Compile) compileVectorIndexScanDataSource(s *Scope) error {
	node := s.DataSource.node
	if node == nil || node.VectorIndexScan == nil {
		return moerr.NewInvalidInputNoCtx("vector index scan is missing its specification")
	}

	_, err := prepareVectorIndexScanForExecution(s.DataSource, c.proc)
	if err != nil {
		return err
	}
	normalizeVectorIndexScanSnapshot(node)
	txnOp, _, err := c.getCompileTableScanDataSourceTxn(s)
	if err != nil {
		return err
	}

	attrs := make([]string, len(node.TableDef.Cols))
	for i, col := range node.TableDef.Cols {
		attrs[i] = col.GetOriginCaseName()
	}
	s.DataSource.Attributes = attrs
	s.DataSource.TableDef = node.TableDef
	s.DataSource.RelationName = node.TableDef.Name
	s.DataSource.SchemaName = node.ObjRef.SchemaName
	s.DataSource.AccountId = node.ObjRef.GetPubInfo()
	s.DataSource.RuntimeFilterSpecs = node.RuntimeFilterProbeList
	s.DataSource.Timestamp = txnOp.Txn().SnapshotTS
	return nil
}

func (c *Compile) compileTableScanFiltersAndProjection(node *plan.Node, ss []*Scope) []*Scope {
	ss = c.ensureCoordinatorOnlyFunctions(node, ss)

	hasUserLevelLockFilter := hasUserLevelLockFunction(node.FilterList)

	// Embed ordinary static filters directly into TableScan.
	// handleRuntimeFilters will set TableScan.RuntimeFilterExprs at execution time (before Prepare).
	// This keeps TableScan as RootOp so compileProjection can push ProjectList into it.
	embeddedStaticFilters := false
	if len(node.FilterList) > 0 && !hasUserLevelLockFilter {
		embeddedStaticFilters = true
		for i := range ss {
			if _, ok := ss[i].RootOp.(*table_scan.TableScan); !ok {
				embeddedStaticFilters = false
				break
			}
		}
		if embeddedStaticFilters {
			for i := range ss {
				ss[i].RootOp.(*table_scan.TableScan).FilterExprs = plan2.DeepCopyExprList(node.FilterList)
			}
		}
	}
	if hasUserLevelLockFilter ||
		runtimeFilterSpecsHaveUserLevelLockFunction(node.RuntimeFilterProbeList) ||
		(len(node.FilterList) > 0 && !embeddedStaticFilters) {
		ss = c.compileRestrict(node, ss)
	}
	return c.compileProjection(node, ss)
}

func (c *Compile) compileRestrict(node *plan.Node, ss []*Scope) []*Scope {
	if len(node.FilterList) == 0 && len(node.RuntimeFilterProbeList) == 0 {
		return ss
	}
	ss = c.ensureCoordinatorOnlyFunctions(node, ss)
	currentFirstFlag := c.anal.isFirst
	var op *filter.Filter
	for i := range ss {
		op = constructRestrict(node, plan2.DeepCopyExprList(node.FilterList))
		op.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
		ss[i].setRootOperator(op)
	}
	c.anal.isFirst = false
	return ss
}

func (c *Compile) compileProjection(node *plan.Node, ss []*Scope) []*Scope {
	if len(node.ProjectList) == 0 {
		return ss
	}

	ss = c.ensureCoordinatorOnlyFunctions(node, ss)
	if _, groupingSetExpand := plan2.DecodeGroupingSetExpandOption(node.ExtraOptions); groupingSetExpand {
		for i := range ss {
			c.setProjection(node, ss[i])
		}
		c.anal.isFirst = false
		return ss
	}
	for i := range ss {
		rootOp := ss[i].RootOp
		if rootOp == nil {
			c.setProjection(node, ss[i])
			continue
		}

		switch op := rootOp.(type) {
		case *table_scan.TableScan:
			if op.ProjectList == nil {
				op.ProjectList = node.ProjectList
			} else {
				c.setProjection(node, ss[i])
			}
		case *value_scan.ValueScan:
			if op.ProjectList == nil {
				op.ProjectList = node.ProjectList
			} else {
				c.setProjection(node, ss[i])
			}
		case *fill.Fill:
			if op.ProjectList == nil {
				op.ProjectList = node.ProjectList
			} else {
				c.setProjection(node, ss[i])
			}
		case *external.External:
			if op.ProjectList == nil {
				op.ProjectList = node.ProjectList
			} else {
				c.setProjection(node, ss[i])
			}
		case *mongoscan.MongoScan:
			if op.ProjectList == nil {
				op.ProjectList = node.ProjectList
			} else {
				c.setProjection(node, ss[i])
			}
		case *group.Group:
			if op.ProjectList == nil {
				op.ProjectList = node.ProjectList
			} else {
				c.setProjection(node, ss[i])
			}
		case *group.MergeGroup:
			if op.ProjectList == nil {
				op.ProjectList = node.ProjectList
			} else {
				c.setProjection(node, ss[i])
			}
		default:
			c.setProjection(node, ss[i])
		}
	}

	c.anal.isFirst = false
	return ss
}

func (c *Compile) ensureCoordinatorOnlyFunctions(node *plan.Node, ss []*Scope) []*Scope {
	if (!nodeHasUserLevelLockFunction(node) && !nodeHasFoundRowsFunction(node)) || c.scopesRunOnCoordinator(ss) {
		return ss
	}
	return []*Scope{c.newMergeScope(ss)}
}

func (c *Compile) scopesRunOnCoordinator(ss []*Scope) bool {
	if len(ss) != 1 {
		return false
	}
	return ss[0].NodeInfo.Mcpu == 1 && sameExecutionAddr(ss[0].NodeInfo.Addr, c.addr)
}

func nodeHasUserLevelLockFunction(node *plan.Node) bool {
	if node == nil {
		return false
	}
	if hasUserLevelLockFunction(node.ProjectList) ||
		hasUserLevelLockFunction(node.OnList) ||
		hasUserLevelLockFunction(node.FilterList) ||
		hasUserLevelLockFunction(node.GroupBy) ||
		hasUserLevelLockFunction(node.AggList) ||
		hasUserLevelLockFunction(node.WinSpecList) ||
		orderBySpecsHaveUserLevelLockFunction(node.OrderBy) ||
		exprHasUserLevelLockFunction(node.Limit) ||
		exprHasUserLevelLockFunction(node.Offset) ||
		hasUserLevelLockFunction(node.TblFuncExprList) ||
		hasUserLevelLockFunction(node.BlockFilterList) ||
		runtimeFilterSpecsHaveUserLevelLockFunction(node.RuntimeFilterProbeList) ||
		runtimeFilterSpecsHaveUserLevelLockFunction(node.RuntimeFilterBuildList) ||
		exprHasUserLevelLockFunction(node.Interval) ||
		exprHasUserLevelLockFunction(node.Sliding) ||
		exprHasUserLevelLockFunction(node.Timestamp) ||
		exprHasUserLevelLockFunction(node.WEnd) ||
		hasUserLevelLockFunction(node.FillVal) ||
		hasUserLevelLockFunction(node.OnUpdateExprs) {
		return true
	}
	if node.IndexReaderParam != nil {
		return orderBySpecsHaveUserLevelLockFunction(node.IndexReaderParam.OrderBy) ||
			exprHasUserLevelLockFunction(node.IndexReaderParam.Limit)
	}
	return false
}

func nodeHasFoundRowsFunction(node *plan.Node) bool {
	if node == nil {
		return false
	}
	if hasFoundRowsFunction(node.ProjectList) ||
		hasFoundRowsFunction(node.OnList) ||
		hasFoundRowsFunction(node.FilterList) ||
		hasFoundRowsFunction(node.GroupBy) ||
		hasFoundRowsFunction(node.AggList) ||
		hasFoundRowsFunction(node.WinSpecList) ||
		orderBySpecsHaveFoundRowsFunction(node.OrderBy) ||
		exprHasFoundRowsFunction(node.Limit) ||
		exprHasFoundRowsFunction(node.Offset) ||
		hasFoundRowsFunction(node.TblFuncExprList) ||
		hasFoundRowsFunction(node.BlockFilterList) ||
		exprHasFoundRowsFunction(node.Interval) ||
		exprHasFoundRowsFunction(node.Sliding) ||
		exprHasFoundRowsFunction(node.Timestamp) ||
		exprHasFoundRowsFunction(node.WEnd) ||
		hasFoundRowsFunction(node.FillVal) ||
		hasFoundRowsFunction(node.OnUpdateExprs) {
		return true
	}
	if node.IndexReaderParam != nil {
		return orderBySpecsHaveFoundRowsFunction(node.IndexReaderParam.OrderBy) ||
			exprHasFoundRowsFunction(node.IndexReaderParam.Limit)
	}
	return false
}

func hasUserLevelLockFunction(exprs []*plan.Expr) bool {
	for _, expr := range exprs {
		if exprHasUserLevelLockFunction(expr) {
			return true
		}
	}
	return false
}

func hasFoundRowsFunction(exprs []*plan.Expr) bool {
	for _, expr := range exprs {
		if exprHasFoundRowsFunction(expr) {
			return true
		}
	}
	return false
}

func exprHasFoundRowsFunction(expr *plan.Expr) bool {
	if expr == nil {
		return false
	}
	switch e := expr.Expr.(type) {
	case *plan.Expr_F:
		if e.F == nil {
			return false
		}
		if e.F.Func != nil {
			fid, _ := function.DecodeOverloadID(e.F.Func.Obj)
			if fid == function.FOUND_ROWS {
				return true
			}
		}
		return hasFoundRowsFunction(e.F.Args)
	case *plan.Expr_List:
		return e.List != nil && hasFoundRowsFunction(e.List.List)
	case *plan.Expr_W:
		if e.W == nil {
			return false
		}
		if exprHasFoundRowsFunction(e.W.WindowFunc) || hasFoundRowsFunction(e.W.PartitionBy) {
			return true
		}
		for _, orderBy := range e.W.OrderBy {
			if orderBy != nil && exprHasFoundRowsFunction(orderBy.Expr) {
				return true
			}
		}
	}
	return false
}

func orderBySpecsHaveFoundRowsFunction(specs []*plan.OrderBySpec) bool {
	for _, spec := range specs {
		if spec != nil && exprHasFoundRowsFunction(spec.Expr) {
			return true
		}
	}
	return false
}

func exprHasUserLevelLockFunction(expr *plan.Expr) bool {
	if expr == nil {
		return false
	}
	switch e := expr.Expr.(type) {
	case *plan.Expr_F:
		if e.F == nil {
			return false
		}
		if e.F.Func != nil {
			fid, _ := function.DecodeOverloadID(e.F.Func.Obj)
			if function.IsUserLevelLockFunctionID(fid) {
				return true
			}
		}
		return hasUserLevelLockFunction(e.F.Args)
	case *plan.Expr_List:
		if e.List == nil {
			return false
		}
		return hasUserLevelLockFunction(e.List.List)
	case *plan.Expr_W:
		return windowSpecHasUserLevelLockFunction(e.W)
	}
	return false
}

func windowSpecHasUserLevelLockFunction(spec *plan.WindowSpec) bool {
	if spec == nil {
		return false
	}
	if exprHasUserLevelLockFunction(spec.WindowFunc) ||
		hasUserLevelLockFunction(spec.PartitionBy) ||
		orderBySpecsHaveUserLevelLockFunction(spec.OrderBy) {
		return true
	}
	frame := spec.Frame
	return frame != nil &&
		(frameBoundHasUserLevelLockFunction(frame.Start) ||
			frameBoundHasUserLevelLockFunction(frame.End))
}

func frameBoundHasUserLevelLockFunction(bound *plan.FrameBound) bool {
	return bound != nil && exprHasUserLevelLockFunction(bound.Val)
}

func orderBySpecsHaveUserLevelLockFunction(specs []*plan.OrderBySpec) bool {
	for _, spec := range specs {
		if spec != nil && exprHasUserLevelLockFunction(spec.Expr) {
			return true
		}
	}
	return false
}

func runtimeFilterSpecsHaveUserLevelLockFunction(specs []*plan.RuntimeFilterSpec) bool {
	for _, spec := range specs {
		if spec != nil &&
			(exprHasUserLevelLockFunction(spec.Expr) ||
				exprHasUserLevelLockFunction(spec.BuildExpr)) {
			return true
		}
	}
	return false
}

func (c *Compile) setProjection(node *plan.Node, s *Scope) {
	op := constructProjection(node)
	op.SetAnalyzeControl(c.anal.curNodeIdx, c.anal.isFirst)
	s.setRootOperator(op)
}

func (c *Compile) compileUnion(node *plan.Node, left []*Scope, right []*Scope) []*Scope {
	left = c.mergeShuffleScopesIfNeeded(left, false)
	right = c.mergeShuffleScopesIfNeeded(right, false)
	return c.mergeDistinctSetScopes(node, append(left, right...), c.anal.isFirst)
}

// mergeDistinctSetScopes applies the global distinct phase required after a
// parallel distinct set operation.  INTERSECT and MINUS may each produce one
// copy of a set key per worker, so their local operator-level deduplication is
// insufficient once those worker outputs are merged.
func (c *Compile) mergeDistinctSetScopes(node *plan.Node, scopes []*Scope, isFirst bool) []*Scope {
	rs := c.newMergeScope(scopes)
	gn := new(plan.Node)
	gn.GroupBy = make([]*plan.Expr, len(node.ProjectList))
	for i := range gn.GroupBy {
		gn.GroupBy[i] = plan2.DeepCopyExpr(node.ProjectList[i])
		gn.GroupBy[i].Typ.NotNullable = false
	}
	if len(node.PhysicalEqualityKeyList) > 0 {
		visibleCount := len(gn.GroupBy)
		for i, expr := range node.PhysicalEqualityKeyList {
			key := plan2.DeepCopyExpr(expr)
			key.Typ.NotNullable = false
			gn.GroupBy = append(gn.GroupBy, key)
			gn.GroupByHashKey = append(gn.GroupByHashKey, int32(visibleCount+i))
		}
	}
	op := constructGroup(c.proc.Ctx, gn, node, true, 0, c.proc)
	if len(node.PhysicalEqualityKeyList) > 0 {
		op.ProjectList = make([]*plan.Expr, len(node.ProjectList))
		for i, expr := range node.ProjectList {
			op.ProjectList[i] = &plan.Expr{
				Typ:  expr.Typ,
				Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: int32(i)}},
			}
		}
	}
	op.SetAnalyzeControl(c.anal.curNodeIdx, isFirst)
	rs.setRootOperator(op)
	c.anal.isFirst = false
	return []*Scope{rs}
}

func (c *Compile) compileTpMinusAndIntersect(node *plan.Node, left []*Scope, right []*Scope, nodeType plan.Node_NodeType) []*Scope {
	rs := c.newScopeListOnSingleWorkerStage(2, 1)
	rs[0].PreScopes = append(rs[0].PreScopes, left[0], right[0])

	connectLeftArg := connector.NewArgument().WithReg(rs[0].Proc.Reg.MergeReceivers[0])
	connectLeftArg.SetAnalyzeControl(c.anal.curNodeIdx, false)
	left[0].setRootOperator(connectLeftArg)

	connectRightArg := connector.NewArgument().WithReg(rs[0].Proc.Reg.MergeReceivers[1])
	connectRightArg.SetAnalyzeControl(c.anal.curNodeIdx, false)
	right[0].setRootOperator(connectRightArg)

	merge0 := rs[0].RootOp.(*merge.Merge)
	merge0.WithPartial(0, 1)
	merge1 := merge.NewArgument().WithPartial(1, 2)
	c.hasMergeOp = true

	currentFirstFlag := c.anal.isFirst
	switch nodeType {
	case plan.Node_MINUS:
		arg := minus.NewArgument()
		arg.KeyExprs = node.PhysicalEqualityKeyList
		arg.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
		rs[0].setRootOperator(arg)
		arg.AppendChild(merge1)
	case plan.Node_INTERSECT:
		arg := intersect.NewArgument()
		arg.KeyExprs = node.PhysicalEqualityKeyList
		arg.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
		rs[0].setRootOperator(arg)
		arg.AppendChild(merge1)
	case plan.Node_INTERSECT_ALL:
		arg := intersectall.NewArgument()
		arg.KeyExprs = node.PhysicalEqualityKeyList
		arg.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
		rs[0].setRootOperator(arg)
		arg.AppendChild(merge1)
	}
	c.anal.isFirst = false
	return rs
}

func (c *Compile) compileMinusAndIntersect(node *plan.Node, left []*Scope, right []*Scope, nodeType plan.Node_NodeType) []*Scope {
	if c.IsSingleScope(left) && c.IsSingleScope(right) {
		return c.compileTpMinusAndIntersect(node, left, right, nodeType)
	}
	rs := c.newScopeListOnSingleWorkerStage(2, int(node.Stats.Dop))
	rs = c.newScopeListForMinusAndIntersect(rs, left, right, node)

	c.hasMergeOp = true
	currentFirstFlag := c.anal.isFirst
	switch nodeType {
	case plan.Node_MINUS:
		for i := range rs {
			merge0 := rs[i].RootOp.(*merge.Merge)
			merge0.WithPartial(0, 1)
			merge1 := merge.NewArgument().WithPartial(1, 2)
			arg := minus.NewArgument()
			arg.KeyExprs = node.PhysicalEqualityKeyList
			arg.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
			rs[i].setRootOperator(arg)
			arg.AppendChild(merge1)
		}
	case plan.Node_INTERSECT:
		for i := range rs {
			merge0 := rs[i].RootOp.(*merge.Merge)
			merge0.WithPartial(0, 1)
			merge1 := merge.NewArgument().WithPartial(1, 2)
			arg := intersect.NewArgument()
			arg.KeyExprs = node.PhysicalEqualityKeyList
			arg.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
			rs[i].setRootOperator(arg)
			arg.AppendChild(merge1)
		}
	case plan.Node_INTERSECT_ALL:
		for i := range rs {
			merge0 := rs[i].RootOp.(*merge.Merge)
			merge0.WithPartial(0, 1)
			merge1 := merge.NewArgument().WithPartial(1, 2)
			arg := intersectall.NewArgument()
			arg.KeyExprs = node.PhysicalEqualityKeyList
			arg.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
			rs[i].setRootOperator(arg)
			arg.AppendChild(merge1)
		}
	}
	if nodeType != plan.Node_INTERSECT_ALL {
		return c.mergeDistinctSetScopes(node, rs, currentFirstFlag)
	}
	c.anal.isFirst = false
	return rs
}

func (c *Compile) compileUnionAll(
	node *plan.Node,
	leftScopes []*Scope,
	rightScopes []*Scope,
	lazy bool,
) []*Scope {
	var rs *Scope
	if lazy {
		// Preserve each logical branch's internal parallelism behind one local
		// receiver so MergeRun can admit the branches in SQL order. Absorb a
		// directly nested lazy UNION ALL into the same scheduler; leaving it
		// behind a connector would let its producer advance before an outer LIMIT
		// can stop consumption.
		branches := c.lazyUnionAllBranches(leftScopes)
		branches = append(branches, c.lazyUnionAllBranches(rightScopes)...)
		rs = c.newMergeScope(branches)
		rs.LazyPreScopes = true
	} else {
		// Keep the original concurrent UNION ALL topology when no streaming
		// LIMIT can stop consumption. This avoids adding another connector,
		// spool, channel, merge, and goroutine hop to every batch.
		inputs := make([]*Scope, 0, len(leftScopes)+len(rightScopes))
		inputs = append(inputs, leftScopes...)
		inputs = append(inputs, rightScopes...)
		rs = c.newMergeScope(inputs)
	}

	currentFirstFlag := c.anal.isFirst
	op := constructUnionAll(node)
	if lazy {
		mergeOp, ok := rs.RootOp.(*merge.Merge)
		if !ok {
			panic("lazy UNION ALL scope has no merge input")
		}
		mergeOp.WithPartial(0, 1)
		op.WithSequentialBranches(len(rs.PreScopes))
	}
	op.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
	rs.setRootOperator(op)
	c.anal.isFirst = false

	return []*Scope{rs}
}

// lazyUnionAllBranches transfers a directly nested lazy UNION ALL's branch
// scopes to its parent scheduler. Each transferred branch receives a
// pass-through marker for the absorbed logical union node so ANALYZE still
// attributes that node's rows only to its own descendants.
func (c *Compile) lazyUnionAllBranches(scopes []*Scope) []*Scope {
	if len(scopes) != 1 || scopes[0] == nil || !scopes[0].LazyPreScopes {
		return []*Scope{c.newMergeScope(scopes)}
	}

	nested := scopes[0]
	nestedUnion, ok := nested.RootOp.(*unionall.UnionAll)
	if !ok || nestedUnion.GetOperatorBase().NumChildren() != 1 {
		return []*Scope{c.newMergeScope(scopes)}
	}
	nestedMerge, ok := nestedUnion.GetOperatorBase().GetChildren(0).(*merge.Merge)
	if !ok || len(nested.PreScopes) < 2 {
		return []*Scope{c.newMergeScope(scopes)}
	}

	connectors := make([]*connector.Connector, len(nested.PreScopes))
	for i, branch := range nested.PreScopes {
		if branch == nil || branch.RootOp == nil {
			return []*Scope{c.newMergeScope(scopes)}
		}
		branchConnector, ok := branch.RootOp.(*connector.Connector)
		if !ok || branchConnector.GetOperatorBase().NumChildren() != 1 {
			return []*Scope{c.newMergeScope(scopes)}
		}
		connectors[i] = branchConnector
	}

	branches := nested.PreScopes
	unionInfo := nestedUnion.GetOperatorBase().OperatorInfo
	for i, branch := range branches {
		branchConnector := connectors[i]
		branch.RootOp = branchConnector.GetOperatorBase().GetChildren(0)
		branchConnector.GetOperatorBase().SetChild(nil, 0)
		branchConnector.GetOperatorBase().ResetChildren()
		branchConnector.Release()

		marker := unionall.NewArgument()
		marker.SetInfo(&unionInfo)
		branch.setRootOperator(marker)
	}

	nestedUnion.GetOperatorBase().SetChild(nil, 0)
	nestedUnion.GetOperatorBase().ResetChildren()
	nestedUnion.Release()
	nestedMerge.Release()
	nested.RootOp = nil
	nested.PreScopes = nil
	nested.release()
	return branches
}

// shouldBuildLeftForAsof compares the conservative retained payload of the two
// physical ASOF strategies. Build-left keeps the logical left and uses direct
// candidate slots for small actual groups. A larger actual group switches to a
// range-update tree with at most two full-right candidate slots per left
// row, so stale cardinality estimates cannot reintroduce O(actual-L*R) work.
// Build-right retains the right input for predecessor lookup. Unknown or
// invalid estimates stay on the established build-right path.
func shouldBuildLeftForAsof(node, left, right *plan.Node) bool {
	if node == nil || left == nil || right == nil ||
		(node.JoinType != plan.Node_ASOF && node.JoinType != plan.Node_ASOF_LEFT) ||
		left.Stats == nil {
		return false
	}
	leftRows := left.Stats.Outcnt
	leftRowSize := left.Stats.Rowsize
	if leftRows <= 0 || leftRowSize <= 0 ||
		math.IsNaN(leftRows) || math.IsNaN(leftRowSize) ||
		math.IsInf(leftRows, 0) || math.IsInf(leftRowSize, 0) {
		return false
	}
	if right.Stats == nil {
		return false
	}
	rightRows := right.Stats.Outcnt
	rightRowSize := right.Stats.Rowsize
	if rightRows <= 0 || rightRowSize <= 0 ||
		math.IsNaN(rightRows) || math.IsNaN(rightRowSize) ||
		math.IsInf(rightRows, 0) || math.IsInf(rightRowSize, 0) {
		return false
	}
	buildLeftBytes := leftRows * (leftRowSize + 2*rightRowSize)
	buildRightBytes := rightRows * rightRowSize
	return !math.IsInf(buildLeftBytes, 0) && !math.IsInf(buildRightBytes, 0) &&
		buildLeftBytes < buildRightBytes
}

func scopesContainOperator(scopes []*Scope, target vm.OpType) bool {
	visited := make(map[*Scope]struct{}, len(scopes))
	stack := append([]*Scope(nil), scopes...)
	for len(stack) > 0 {
		last := len(stack) - 1
		scope := stack[last]
		stack = stack[:last]
		if scope == nil {
			continue
		}
		if _, ok := visited[scope]; ok {
			continue
		}
		visited[scope] = struct{}{}
		found := false
		if scope.RootOp != nil {
			_ = vm.HandleAllOp(scope.RootOp, func(_ vm.Operator, op vm.Operator) error {
				if op.OpType() == target {
					found = true
				}
				return nil
			})
		}
		if found {
			return true
		}
		stack = append(stack, scope.PreScopes...)
	}
	return false
}

func (c *Compile) compileJoin(node, left, right *plan.Node, probeScopes, buildScopes []*Scope) []*Scope {
	if shouldBuildLeftForAsof(node, left, right) &&
		!scopesContainOperator(probeScopes, vm.MergeRecursive) &&
		!scopesContainOperator(buildScopes, vm.MergeRecursive) {
		if node.Stats.HashmapStats.Shuffle {
			return c.compileShuffleJoinWithBuildLeft(
				node, left, right, probeScopes, buildScopes, true)
		}
		return c.compileBroadcastAsofBuildLeft(node, left, right, probeScopes, buildScopes)
	}
	if node.Stats.HashmapStats.Shuffle {
		if node.JoinType == plan.Node_MARK && !canUseShuffleHashMarkJoinWithInputs(node, left, right) {
			node.Stats.HashmapStats.Shuffle = false
			node.Stats.HashmapStats.ShuffleColIdx = -1
		} else {
			return c.compileShuffleJoin(node, left, right, probeScopes, buildScopes)
		}
	}

	rs := c.compileProbeSideForBroadcastJoin(node, left, right, probeScopes)
	return c.compileBuildSideForBroadcastJoin(node, rs, buildScopes)
}

func (c *Compile) compileBroadcastAsofBuildLeft(
	node, left, right *plan.Node,
	leftScopes, rightScopes []*Scope,
) []*Scope {
	// Independent right scan scopes would each produce one local predecessor
	// for every broadcast left row. Merge them into one stream so finalization
	// happens exactly once. Shuffle ASOF keeps parallelism by key instead.
	rightScopes = c.mergeShuffleScopesIfNeeded(rightScopes, false)
	if len(rightScopes) != 1 || rightScopes[0].NodeInfo.Mcpu != 1 {
		rightScopes = []*Scope{c.newMergeScope(rightScopes)}
	}

	leftTypes := make([]types.Type, len(left.ProjectList))
	for i, expr := range left.ProjectList {
		leftTypes[i] = dupType(&expr.Typ)
	}
	rightTypes := make([]types.Type, len(right.ProjectList))
	for i, expr := range right.ProjectList {
		rightTypes[i] = dupType(&expr.Typ)
	}
	op := constructHashJoin(node, left, leftTypes, rightTypes, c.proc)
	op.AsofBuildLeft = true
	op.RuntimeFilterSpecs = nil
	op.SetAnalyzeControl(c.anal.curNodeIdx, c.anal.isFirst)
	rightScopes[0].setRootOperator(op)
	c.anal.isFirst = false
	return c.compileBuildSideForBroadcastJoin(node, rightScopes, leftScopes)
}

func (c *Compile) compileShuffleJoin(
	node, left, right *plan.Node,
	leftscopes, rightscopes []*Scope,
) []*Scope {
	return c.compileShuffleJoinWithBuildLeft(
		node, left, right, leftscopes, rightscopes, false)
}

func (c *Compile) compileShuffleJoinWithBuildLeft(
	node, left, right *plan.Node,
	leftscopes, rightscopes []*Scope,
	asofBuildLeft bool,
) []*Scope {
	stageNodes, hasLocalDependency := c.shuffleJoinStageNodes(leftscopes, rightscopes)
	if !hasLocalDependency &&
		len(stageNodes) == 1 && len(leftscopes) == 1 && len(rightscopes) == 1 &&
		sameExecutionNode(leftscopes[0].NodeInfo, rightscopes[0].NodeInfo) &&
		leftscopes[0].NodeInfo.Mcpu == int(left.Stats.Dop) &&
		rightscopes[0].NodeInfo.Mcpu == int(right.Stats.Dop) {
		return c.compileLocalShuffleJoinWithBuildLeft(
			node, left, right, leftscopes, rightscopes, asofBuildLeft)
	}
	return c.compileDistributedShuffleJoin(
		node, left, right, leftscopes, rightscopes, stageNodes, hasLocalDependency, asofBuildLeft)
}

// canReuseDistributedShuffleJoin reports whether probeScopes already use the
// physical layout required by distributed shuffle: one Mcpu=1 scope per
// global bucket, ordered by stage node and then by that node's bucket index.
// A packed scope (one scope with Mcpu=dop) is reusable only by the local
// shared-pool implementation and must be reshuffled before distributed use.
func canReuseDistributedShuffleJoin(probeScopes []*Scope, stageNodes engine.Nodes, dop int) bool {
	if dop <= 0 || len(stageNodes) == 0 || len(probeScopes) != len(stageNodes)*dop {
		return false
	}
	for i, scope := range probeScopes {
		if scope == nil || scope.NodeInfo.Mcpu != 1 ||
			!sameExecutionNode(scope.NodeInfo, stageNodes[i/dop]) {
			return false
		}
	}
	return true
}

func (c *Compile) shuffleStageNodes(scopes []*Scope) engine.Nodes {
	stageNodes := c.queryWorkerStageNodes()
	if len(stageNodes) > 0 {
		return stageNodes
	}
	for _, scope := range scopes {
		found := false
		for _, node := range stageNodes {
			if sameExecutionNode(node, scope.NodeInfo) {
				found = true
				break
			}
		}
		if !found {
			stageNodes = append(stageNodes, scope.NodeInfo)
		}
	}
	return stageNodes
}

func (c *Compile) compileLocalShuffleJoin(
	node, left, right *plan.Node,
	leftscopes, rightscopes []*Scope,
) []*Scope {
	return c.compileLocalShuffleJoinWithBuildLeft(
		node, left, right, leftscopes, rightscopes, false)
}

func (c *Compile) compileLocalShuffleJoinWithBuildLeft(
	node, left, right *plan.Node,
	leftscopes, rightscopes []*Scope,
	asofBuildLeft bool,
) []*Scope {
	if node.Stats.Dop != left.Stats.Dop || node.Stats.Dop != right.Stats.Dop {
		panic("wrong dop for shuffle join!")
	}
	if len(leftscopes) != len(rightscopes) {
		panic("wrong scopes for shuffle join!")
	}

	probeScopes, buildScopes := leftscopes, rightscopes
	probeIsLogicalLeft := true
	if asofBuildLeft {
		probeScopes, buildScopes = rightscopes, leftscopes
		probeIsLogicalLeft = false
	}
	reuse := !asofBuildLeft &&
		node.Stats.HashmapStats.ShuffleMethod == plan.ShuffleMethod_Reuse
	bucketNum := len(c.shuffleStageNodes(leftscopes)) * int(node.Stats.Dop)
	for i := range probeScopes {
		probeScopes[i].PreScopes = append(probeScopes[i].PreScopes, buildScopes[i])
		if !reuse {
			shuffleOpForProbe := constructShuffleOperatorForJoin(
				int32(bucketNum), node, probeIsLogicalLeft)
			if asofBuildLeft && len(node.RuntimeFilterProbeList) > 0 {
				// Shuffle HashBuild publishes PASS as its build-completion signal.
				// After swapping the physical sides, move that wait to the logical
				// right (physical probe); waiting on the logical-left build stream
				// would create a self-dependency.
				shuffleOpForProbe.RuntimeFilterSpec =
					plan2.DeepCopyRuntimeFilterSpec(node.RuntimeFilterProbeList[0])
			}
			shuffleOpForProbe.SetAnalyzeControl(c.anal.curNodeIdx, false)
			probeScopes[i].setRootOperator(shuffleOpForProbe)
		}

		shuffleOpForBuild := constructShuffleOperatorForJoin(
			int32(bucketNum), node, !probeIsLogicalLeft)
		if asofBuildLeft {
			shuffleOpForBuild.RuntimeFilterSpec = nil
		}
		shuffleOpForBuild.SetAnalyzeControl(c.anal.curNodeIdx, false)
		buildScopes[i].setRootOperator(shuffleOpForBuild)
	}

	constructShuffleJoinOPWithBuildLeft(
		c, probeScopes, node, left, right, true, asofBuildLeft)

	for i := range probeScopes {
		buildOp := constructShuffleHashBuild(node, probeScopes[i].RootOp, c.proc)
		buildOp.SetAnalyzeControl(c.anal.curNodeIdx, false)
		buildScopes[i].setRootOperator(buildOp)
	}

	return probeScopes
}

func constructShuffleJoinOP(
	c *Compile,
	shuffleJoins []*Scope,
	node, left, right *plan.Node,
	sharedPool bool,
) {
	constructShuffleJoinOPWithBuildLeft(
		c, shuffleJoins, node, left, right, sharedPool, false)
}

func constructShuffleJoinOPWithBuildLeft(
	c *Compile,
	shuffleJoins []*Scope,
	node, left, right *plan.Node,
	sharedPool bool,
	asofBuildLeft bool,
) {
	rightTypes := make([]types.Type, len(right.ProjectList))
	for i, expr := range right.ProjectList {
		rightTypes[i] = dupType(&expr.Typ)
	}

	leftTypes := make([]types.Type, len(left.ProjectList))
	for i, expr := range left.ProjectList {
		leftTypes[i] = dupType(&expr.Typ)
	}

	currentFirstFlag := c.anal.isFirst
	switch node.JoinType {
	case plan.Node_INNER, plan.Node_LEFT, plan.Node_RIGHT, plan.Node_SEMI, plan.Node_ANTI, plan.Node_OUTER, plan.Node_MARK,
		plan.Node_ASOF, plan.Node_ASOF_LEFT:
		for i := range shuffleJoins {
			op := constructHashJoin(node, left, leftTypes, rightTypes, c.proc)
			op.AsofBuildLeft = asofBuildLeft
			op.ShuffleIdx = int32(i)
			if sharedPool {
				op.ShuffleIdx = -1
			}
			op.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
			shuffleJoins[i].setRootOperator(op)
		}

	case plan.Node_DEDUP:
		if node.IsRightJoin {
			for i := range shuffleJoins {
				op := constructRightDedupJoin(node, leftTypes, rightTypes, c.proc)
				op.ShuffleIdx = int32(i)
				if sharedPool {
					op.ShuffleIdx = -1
				}
				op.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
				shuffleJoins[i].setRootOperator(op)
			}
		} else {
			if node.DedupJoinCtx != nil && len(node.DedupJoinCtx.OldColCaptureList) > 0 {
				panic(moerr.NewNYI(c.proc.Ctx, "shuffle DedupJoin with OldColCapture is not supported"))
			}
			for i := range shuffleJoins {
				op := constructDedupJoin(node, leftTypes, rightTypes, c.proc)
				op.ShuffleIdx = int32(i)
				if sharedPool {
					op.ShuffleIdx = -1
				}
				op.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
				shuffleJoins[i].setRootOperator(op)
			}
		}

	default:
		panic(moerr.NewNYI(c.proc.Ctx, fmt.Sprintf("shuffle join do not support join type '%v'", node.JoinType)))
	}
	c.anal.isFirst = false
}

func (c *Compile) compileDistributedShuffleJoin(
	node, left, right *plan.Node,
	lefts, rights []*Scope,
	stageNodes engine.Nodes,
	attachRemoteSources bool,
	asofBuildLeft bool,
) []*Scope {
	probeScopes, buildScopes := lefts, rights
	probeIsLogicalLeft := true
	if asofBuildLeft {
		probeScopes, buildScopes = rights, lefts
		probeIsLogicalLeft = false
	}
	shuffleJoins := c.newShuffleJoinScopeListAtSides(
		probeScopes, buildScopes, node, stageNodes, attachRemoteSources, probeIsLogicalLeft)
	constructShuffleJoinOPWithBuildLeft(
		c, shuffleJoins, node, left, right, false, asofBuildLeft)

	//construct shuffle build
	currentFirstFlag := c.anal.isFirst
	for i := range shuffleJoins {
		buildScope := shuffleJoins[i].PreScopes[0]
		mergeOp := merge.NewArgument()
		mergeOp.SetAnalyzeControl(c.anal.curNodeIdx, false)
		buildScope.setRootOperator(mergeOp)

		buildOp := constructShuffleHashBuild(node, shuffleJoins[i].RootOp, c.proc)
		buildOp.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
		buildScope.setRootOperator(buildOp)
	}
	c.anal.isFirst = false

	return shuffleJoins
}
func (c *Compile) newProbeScopeListForBroadcastJoin(probeScopes []*Scope, forceOneCN bool) []*Scope {
	if forceOneCN { // for right join, we have to merge these input for now
		probeScopes = c.mergeShuffleScopesIfNeeded(probeScopes, false)
		if len(probeScopes) > 1 {
			probeScopes = []*Scope{c.newMergeScope(probeScopes)}
		}
	}
	// don't need to break pipelines for probe side of broadcast join
	return probeScopes
}

// canUseHashMarkJoin returns true when equality hashing is sufficient to
// preserve MARK's three-valued result.
//
// A single equality key only needs two hash-side facts: exact membership and
// whether the build contains NULL. Every predicate must be an actual hash key
// with one relation per operand; residual or mixed-side predicates require
// row-aware evaluation by LoopJoin. For composite keys, a partially-NULL row
// can be FALSE or UNKNOWN depending on the other components, so use hash MARK
// only when every key is statically NOT NULL.
func canUseHashMarkJoin(node *plan.Node) bool {
	return canUseHashMarkJoinWithInputs(node, nil, nil)
}

func canUseHashMarkJoinWithInputs(node, left, right *plan.Node) bool {
	if node == nil || node.JoinType != plan.Node_MARK {
		return false
	}

	conditions := colexec.SplitAndExprs(node.OnList)
	if len(conditions) == 0 {
		return false
	}
	nonEqCond, hashConditions := extraJoinConditions(conditions)
	if nonEqCond != nil || len(hashConditions) != len(conditions) {
		return false
	}

	allNotNull := true
	for _, condition := range hashConditions {
		fn := condition.GetF()
		if fn == nil || !plan2.IsEqualFunc(fn.Func.GetObj()) || len(fn.Args) != 2 {
			return false
		}
		leftRel, leftSingleRel := hashMarkOperandRel(fn.Args[0])
		rightRel, rightSingleRel := hashMarkOperandRel(fn.Args[1])
		if !leftSingleRel || !rightSingleRel ||
			!((leftRel == 0 && rightRel == 1) || (leftRel == 1 && rightRel == 0)) {
			return false
		}
		if left != nil && right != nil {
			allNotNull = allNotNull &&
				plan2.IsJoinExprEffectivelyNotNullable(fn.Args[0], left, right) &&
				plan2.IsJoinExprEffectivelyNotNullable(fn.Args[1], left, right)
		} else {
			allNotNull = allNotNull && fn.Args[0].Typ.NotNullable && fn.Args[1].Typ.NotNullable
		}
	}
	return len(hashConditions) == 1 || allNotNull
}

// canUseShuffleHashMarkJoin is stricter than canUseHashMarkJoin because each
// shuffle bucket builds an independent hash table. Nullable MARK keys require
// global build facts (whether the entire build is empty and whether any key is
// NULL) to preserve SQL three-valued semantics. Those facts are available to
// broadcast hash MARK joins, but are not replicated across shuffle buckets.
//
// With effectively non-null keys on both materialized inputs, exact matches
// are co-located by the shuffle and every non-match is FALSE, so bucket-local
// state is sufficient.
func canUseShuffleHashMarkJoin(node *plan.Node) bool {
	return canUseShuffleHashMarkJoinWithInputs(node, nil, nil)
}

func canUseShuffleHashMarkJoinWithInputs(node, left, right *plan.Node) bool {
	if !canUseHashMarkJoinWithInputs(node, left, right) {
		return false
	}
	for _, condition := range colexec.SplitAndExprs(node.OnList) {
		fn := condition.GetF()
		if fn == nil || len(fn.Args) != 2 {
			return false
		}
		if left != nil && right != nil {
			if !plan2.IsJoinExprProvenNotNullable(fn.Args[0], left, right) ||
				!plan2.IsJoinExprProvenNotNullable(fn.Args[1], left, right) {
				return false
			}
		} else if !fn.Args[0].Typ.NotNullable || !fn.Args[1].Typ.NotNullable {
			return false
		}
	}
	return true
}

// hashMarkOperandRel returns the single relation referenced by an equality
// operand. MARK cannot hash an operand that mixes probe and build columns: the
// resulting residual condition needs row-aware evaluation by LoopJoin.
func hashMarkOperandRel(expr *plan.Expr) (int32, bool) {
	relPos := int32(-1)
	singleRel := true

	var visit func(*plan.Expr)
	visit = func(current *plan.Expr) {
		if current == nil || !singleRel {
			return
		}
		switch impl := current.Expr.(type) {
		case *plan.Expr_Col:
			if relPos == -1 {
				relPos = impl.Col.RelPos
			} else if relPos != impl.Col.RelPos {
				singleRel = false
			}
		case *plan.Expr_F:
			for _, arg := range impl.F.Args {
				visit(arg)
			}
		case *plan.Expr_List:
			for _, item := range impl.List.List {
				visit(item)
			}
		}
	}

	visit(expr)
	return relPos, singleRel && relPos >= 0
}

func (c *Compile) compileProbeSideForBroadcastJoin(node, left, right *plan.Node, probeScopes []*Scope) []*Scope {
	var rs []*Scope
	isEq := plan2.IsEquiJoin2(node.OnList)

	rightTypes := make([]types.Type, len(right.ProjectList))
	for i, expr := range right.ProjectList {
		rightTypes[i] = dupType(&expr.Typ)
	}

	leftTypes := make([]types.Type, len(left.ProjectList))
	for i, expr := range left.ProjectList {
		leftTypes[i] = dupType(&expr.Typ)
	}

	switch node.JoinType {
	case plan.Node_INNER:
		rs = c.newProbeScopeListForBroadcastJoin(probeScopes, false)
		currentFirstFlag := c.anal.isFirst
		if len(node.OnList) == 0 {
			for i := range rs {
				op := constructProduct(node, rightTypes, c.proc)
				op.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
				rs[i].setRootOperator(op)
			}
		} else {
			for i := range rs {
				if isEq {
					op := constructHashJoin(node, left, leftTypes, rightTypes, c.proc)
					op.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
					rs[i].setRootOperator(op)
				} else {
					op := constructLoopJoin(node, leftTypes, rightTypes, c.proc)
					op.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
					rs[i].setRootOperator(op)
				}
			}
		}
		c.anal.isFirst = false
	case plan.Node_L2:
		rs = c.newProbeScopeListForBroadcastJoin(probeScopes, false)
		currentFirstFlag := c.anal.isFirst
		for i := range rs {
			op := constructProductL2(node, c.proc)
			op.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
			rs[i].setRootOperator(op)
			if rs[i].NodeInfo.Mcpu != 1 {
				//product_l2 join is very time_consuming, increase the parallelism
				rs[i].NodeInfo.Mcpu *= 8
			}
			if rs[i].NodeInfo.Mcpu > c.ncpu {
				rs[i].NodeInfo.Mcpu = c.ncpu
			}
		}
		c.anal.isFirst = false
	case plan.Node_INDEX:
		rs = c.newProbeScopeListForBroadcastJoin(probeScopes, false)
		currentFirstFlag := c.anal.isFirst
		for i := range rs {
			op := constructIndexJoin(node, c.proc)
			op.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
			rs[i].setRootOperator(op)
		}
		c.anal.isFirst = false
	case plan.Node_LEFT, plan.Node_RIGHT, plan.Node_SEMI, plan.Node_ANTI, plan.Node_SINGLE:
		rs = c.newProbeScopeListForBroadcastJoin(probeScopes, isEq && node.IsRightJoin)
		currentFirstFlag := c.anal.isFirst
		if isEq {
			for i := range rs {
				op := constructHashJoin(node, left, leftTypes, rightTypes, c.proc)
				op.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
				rs[i].setRootOperator(op)
			}
		} else {
			for i := range rs {
				op := constructLoopJoin(node, leftTypes, rightTypes, c.proc)
				op.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
				rs[i].setRootOperator(op)
			}
		}
		c.anal.isFirst = false
	case plan.Node_ASOF, plan.Node_ASOF_LEFT:
		rs = c.newProbeScopeListForBroadcastJoin(probeScopes, false)
		currentFirstFlag := c.anal.isFirst
		for i := range rs {
			op := constructHashJoin(node, left, leftTypes, rightTypes, c.proc)
			op.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
			rs[i].setRootOperator(op)
		}
		c.anal.isFirst = false
	case plan.Node_OUTER:
		// FULL OUTER JOIN: equi → hashjoin (Phase 1); non-equi → loopjoin
		// (Phase 4). IsRightJoin=true (set in stats.go for Node_OUTER) routes
		// the probe scope through forceOneCN, avoiding distributed
		// double-emission of unmatched-build rows.
		rs = c.newProbeScopeListForBroadcastJoin(probeScopes, true)
		currentFirstFlag := c.anal.isFirst
		if isEq {
			for i := range rs {
				op := constructHashJoin(node, left, leftTypes, rightTypes, c.proc)
				op.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
				rs[i].setRootOperator(op)
			}
		} else {
			for i := range rs {
				op := constructLoopJoin(node, leftTypes, rightTypes, c.proc)
				op.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
				rs[i].setRootOperator(op)
			}
		}
		c.anal.isFirst = false
	case plan.Node_DEDUP:
		if node.IsRightJoin {
			rs = c.newProbeScopeListForBroadcastJoin(probeScopes, true)
			currentFirstFlag := c.anal.isFirst
			for i := range rs {
				op := constructRightDedupJoin(node, leftTypes, rightTypes, c.proc)
				op.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
				rs[i].setRootOperator(op)
				rs[i].NodeInfo.Mcpu = 1
			}
			c.anal.isFirst = false
		} else {
			rs = c.newProbeScopeListForBroadcastJoin(probeScopes, true)
			currentFirstFlag := c.anal.isFirst
			for i := range rs {
				op := constructDedupJoin(node, leftTypes, rightTypes, c.proc)
				op.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
				rs[i].setRootOperator(op)
			}
			c.anal.isFirst = false
		}
	case plan.Node_MARK:
		rs = c.newProbeScopeListForBroadcastJoin(probeScopes, false)
		currentFirstFlag := c.anal.isFirst
		for i := range rs {
			var op vm.Operator
			if canUseHashMarkJoinWithInputs(node, left, right) {
				op = constructBroadcastHashMarkJoin(node, left, leftTypes, rightTypes, c.proc)
			} else {
				op = constructLoopJoin(node, leftTypes, rightTypes, c.proc)
			}
			op.GetOperatorBase().SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
			rs[i].setRootOperator(op)
		}
		c.anal.isFirst = false
	default:
		panic(moerr.NewNYI(c.proc.Ctx, fmt.Sprintf("join typ '%v'", node.JoinType)))
	}
	return rs
}

func (c *Compile) compileBuildSideForBroadcastJoin(node *plan.Node, rs, buildScopes []*Scope) []*Scope {
	if !c.IsSingleScope(buildScopes) { // first merge scopes of build side, will optimize this in the future
		buildScopes = c.mergeShuffleScopesIfNeeded(buildScopes, false)
		buildScopes = []*Scope{c.newMergeScope(buildScopes)}
	}

	if len(rs) == 1 { // broadcast join on single cn
		buildScopes[0].setRootOperator(constructJoinBuildOperator(
			c, rs[0].RootOp, int32(rs[0].NodeInfo.Mcpu), node.RuntimeFilterBuildList))
		rs[0].PreScopes = append(rs[0].PreScopes, buildScopes[0])
		return rs
	}

	buildScopeAttached := false
	for i := range rs {
		if sameExecutionNode(rs[i].NodeInfo, buildScopes[0].NodeInfo) {
			rs[i].PreScopes = append(rs[i].PreScopes, buildScopes[0])
			buildScopeAttached = true
			break
		}
	}
	if !buildScopeAttached {
		rs[0].PreScopes = append(rs[0].PreScopes, buildScopes[0])
	}

	stageNodes := c.queryWorkerStageNodes()
	buildOpScopes := make([]*Scope, 0, len(stageNodes))
	probeScopeGroups := c.groupBroadcastProbeScopesByCN(rs, stageNodes)

	if len(rs) > len(stageNodes) || hasMultiScopeGroup(probeScopeGroups) { // probe side is shuffle scopes
		for _, tmp := range probeScopeGroups {
			bs := newScope(Remote)
			bs.NodeInfo = scopeNodeWithMcpu(tmp[0].NodeInfo, 1)
			bs.Proc = c.proc.NewNoContextChildProc(0)
			edge := process.NewPipelineEdge(10, 0)
			bs.Proc.Reg.MergeReceivers = append(bs.Proc.Reg.MergeReceivers, edge)

			mergeOp := merge.NewArgument()
			c.hasMergeOp = true
			mergeOp.SetAnalyzeControl(c.anal.curNodeIdx, false)
			bs.setRootOperator(mergeOp)
			bs.setRootOperator(constructJoinBuildOperator(
				c, tmp[0].RootOp, int32(len(tmp)), node.RuntimeFilterBuildList))
			tmp[0].PreScopes = append(tmp[0].PreScopes, bs)
			buildOpScopes = append(buildOpScopes, bs)
		}
		dispatchArg := constructDispatch(0, buildOpScopes, buildScopes[0], node, false)
		dispatchArg.SetAnalyzeControl(c.anal.curNodeIdx, false)
		buildScopes[0].setRootOperator(dispatchArg)
		return rs
	}

	//broadcast join on multi CN

	for i := range rs {
		bs := newScope(Remote)
		bs.NodeInfo = scopeNodeWithMcpu(rs[i].NodeInfo, 1)
		bs.Proc = c.proc.NewNoContextChildProc(0)
		edge := process.NewPipelineEdge(10, 0)
		bs.Proc.Reg.MergeReceivers = append(bs.Proc.Reg.MergeReceivers, edge)

		mergeOp := merge.NewArgument()
		c.hasMergeOp = true
		mergeOp.SetAnalyzeControl(c.anal.curNodeIdx, false)
		bs.setRootOperator(mergeOp)
		bs.setRootOperator(constructJoinBuildOperator(
			c, rs[i].RootOp, int32(rs[i].NodeInfo.Mcpu), node.RuntimeFilterBuildList))
		rs[i].PreScopes = append(rs[i].PreScopes, bs)
		buildOpScopes = append(buildOpScopes, bs)
	}

	dispatchArg := constructDispatch(0, buildOpScopes, buildScopes[0], node, false)
	dispatchArg.SetAnalyzeControl(c.anal.curNodeIdx, false)
	buildScopes[0].setRootOperator(dispatchArg)
	return rs
}

func (c *Compile) groupBroadcastProbeScopesByCN(rs []*Scope, stageNodes engine.Nodes) [][]*Scope {
	groups := make([][]*Scope, 0, len(rs))
	used := make([]bool, len(rs))

	for _, cn := range stageNodes {
		var group []*Scope
		for i := range rs {
			if !used[i] && sameExecutionNode(cn, rs[i].NodeInfo) {
				group = append(group, rs[i])
				used[i] = true
			}
		}
		if len(group) > 0 {
			groups = append(groups, group)
		}
	}

	for i := range rs {
		if used[i] {
			continue
		}
		group := []*Scope{rs[i]}
		used[i] = true
		for j := i + 1; j < len(rs); j++ {
			if !used[j] && sameExecutionNode(rs[i].NodeInfo, rs[j].NodeInfo) {
				group = append(group, rs[j])
				used[j] = true
			}
		}
		groups = append(groups, group)
	}

	return groups
}

func hasMultiScopeGroup(groups [][]*Scope) bool {
	for _, group := range groups {
		if len(group) > 1 {
			return true
		}
	}
	return false
}

func (c *Compile) compileApply(node, right *plan.Node, rs []*Scope) []*Scope {

	switch node.ApplyType {
	case plan.Node_CROSSAPPLY:
		for i := range rs {
			op := constructApply(node, right, apply.CROSS, c.proc)
			op.TxnOffset = c.TxnOffset
			if op.TableFunction != nil && op.TableFunction.IsSingle {
				rs[i].NodeInfo.Mcpu = 1
			}
			op.SetIdx(c.anal.curNodeIdx)
			rs[i].setRootOperator(op)
		}
	case plan.Node_OUTERAPPLY:
		for i := range rs {
			op := constructApply(node, right, apply.OUTER, c.proc)
			op.TxnOffset = c.TxnOffset
			op.SetIdx(c.anal.curNodeIdx)
			rs[i].setRootOperator(op)
		}
	default:
		panic("unknown apply")
	}

	return rs
}

func (c *Compile) compilePostDml(node *plan.Node, ss []*Scope) []*Scope {
	currentFirstFlag := c.anal.isFirst
	for i := range ss {
		arg := constructPostDml(node, c.e)
		arg.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
		ss[i].setRootOperator(arg)
	}
	c.anal.isFirst = false
	return ss
}

func (c *Compile) compilePartition(node *plan.Node, ss []*Scope) []*Scope {
	if node.Limit != nil && c.supportsRemotePartitionTopN() {
		currentFirstFlag := c.anal.isFirst
		for i := range ss {
			op := constructPartition(node)
			op.PreReduce = true
			op.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
			ss[i].setRootOperator(op)
		}
		c.anal.isFirst = false

		rs := c.newMergeScope(ss)
		arg := constructPartition(node)
		arg.PreReduce = true
		arg.SetAnalyzeControl(c.anal.curNodeIdx, c.anal.isFirst)
		rs.setRootOperator(arg)
		c.anal.isFirst = false
		return []*Scope{rs}
	}
	if node.PartitionAlgorithm == plan.Node_PARTITION_ALGORITHM_HASH && c.supportsRemoteHashPartition() {
		rs := c.newMergeScope(ss)
		arg := constructPartition(node)
		arg.SetAnalyzeControl(c.anal.curNodeIdx, c.anal.isFirst)
		rs.setRootOperator(arg)
		c.anal.isFirst = false
		return []*Scope{rs}
	}

	currentFirstFlag := c.anal.isFirst
	for i := range ss {
		//c.anal.isFirst = currentFirstFlag
		op := constructOrder(node)
		if node.PartitionByCount > 0 {
			op.OrderBySpec = node.OrderBy[:node.PartitionByCount]
		}
		op.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
		ss[i].setRootOperator(op)
	}
	c.anal.isFirst = false

	rs := c.newMergeScope(ss)

	currentFirstFlag = c.anal.isFirst
	arg := constructPartition(node)
	if node.PartitionAlgorithm == plan.Node_PARTITION_ALGORITHM_HASH {
		// A mixed-version cluster cannot understand the HASH pipeline field.
		// Keep both its prerequisite local orders and its coordinator algorithm
		// on the legacy path.
		arg.Algorithm = plan.Node_PARTITION_ALGORITHM_SORT
	}
	if node.PartitionByCount > 0 {
		arg.OrderBySpecs = node.OrderBy[:node.PartitionByCount]
		arg.Limit = nil
		arg.PartitionByCount = 0
	}
	arg.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
	rs.setRootOperator(arg)
	c.anal.isFirst = false

	return []*Scope{rs}
}

func (c *Compile) compileSort(node *plan.Node, ss []*Scope) []*Scope {
	if c.ownsFoundRows(node) {
		// SQL_CALC_FOUND_ROWS must not use the Top-N shortcut: Top-N is
		// intentionally allowed to stop upstream work. Build the complete
		// ordered stream first, then apply OFFSET and LIMIT on the final scope.
		if len(node.OrderBy) > 0 {
			ss = c.compileOrder(node, ss)
		}
		if node.Offset != nil {
			ss = c.compileOffset(node, ss)
		}
		if node.Limit != nil {
			ss = c.compileLimit(node, ss)
		}
		return ss
	}
	switch {
	case node.Limit != nil && node.Offset == nil && len(node.OrderBy) > 0: // top
		return c.compileTop(node, node.Limit, ss)

	case node.Limit == nil && node.Offset == nil && len(node.OrderBy) > 0: // top
		return c.compileOrder(node, ss)

	case node.Limit != nil && node.Offset != nil && len(node.OrderBy) > 0:
		if rule.IsConstant(node.Limit, false) && rule.IsConstant(node.Offset, false) {
			// get limit
			vec1, free1, err := colexec.GetReadonlyResultFromNoColumnExpression(c.proc, node.Limit)
			if err != nil {
				panic(err)
			}
			defer free1()

			// get offset
			vec2, free2, err := colexec.GetReadonlyResultFromNoColumnExpression(c.proc, node.Offset)
			if err != nil {
				panic(err)
			}
			defer free2()

			limit, offset := vector.MustFixedColWithTypeCheck[uint64](vec1)[0], vector.MustFixedColWithTypeCheck[uint64](vec2)[0]
			topN := limit + offset
			overflow := false
			if topN < limit || topN < offset {
				overflow = true
			}
			if !overflow && topN <= mergeTopResidentPlanThreshold {
				// if n is small, convert `order by col limit m offset n` to `top m+n offset n`
				return c.compileOffset(node, c.compileTop(node, plan2.MakePlan2Uint64ConstExprWithType(topN), ss))
			}
		}
		return c.compileLimit(node, c.compileOffset(node, c.compileOrder(node, ss)))

	case node.Limit == nil && node.Offset != nil && len(node.OrderBy) > 0: // order and offset
		return c.compileOffset(node, c.compileOrder(node, ss))

	case node.Limit != nil && node.Offset == nil && len(node.OrderBy) == 0: // limit
		return c.compileLimit(node, ss)

	case node.Limit == nil && node.Offset != nil && len(node.OrderBy) == 0: // offset
		return c.compileOffset(node, ss)

	case node.Limit != nil && node.Offset != nil && len(node.OrderBy) == 0: // limit and offset
		return c.compileLimit(node, c.compileOffset(node, ss))

	default:
		return ss
	}
}

const mergeTopResidentPlanThreshold uint64 = 8192 * 2

// canUseResidentMergeTop limits the resident-only global MergeTop to small,
// statically bounded plans. Large or runtime limits use the existing spill-capable
// Top and MergeOrder operators instead.
func canUseResidentMergeTop(topN *plan.Expr) bool {
	if topN == nil {
		return false
	}
	literal, ok := topN.Expr.(*plan.Expr_Lit)
	if !ok || literal.Lit == nil {
		return false
	}
	value, ok := literal.Lit.Value.(*plan.Literal_U64Val)
	return ok && value.U64Val <= mergeTopResidentPlanThreshold
}

func (c *Compile) compileTop(node *plan.Node, topN *plan.Expr, ss []*Scope) []*Scope {
	// use topN TO make scope.
	if c.IsSingleScope(ss) {
		currentFirstFlag := c.anal.isFirst
		op := constructTop(node, topN)
		op.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
		ss[0].setRootOperator(op)
		c.anal.isFirst = false
		return ss
	}

	currentFirstFlag := c.anal.isFirst
	for i := range ss {
		//c.anal.isFirst = currentFirstFlag
		op := constructTop(node, topN)
		op.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
		ss[i].setRootOperator(op)
	}
	c.anal.isFirst = false
	ss = c.mergeShuffleScopesIfNeeded(ss, false)
	rs := c.newMergeScope(ss)

	currentFirstFlag = c.anal.isFirst
	if canUseResidentMergeTop(topN) {
		arg := constructMergeTop(node, topN)
		arg.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
		rs.setRootOperator(arg)
		c.anal.isFirst = false
		return []*Scope{rs}
	}

	mergeOrder := constructMergeOrder(node)
	mergeOrder.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
	rs.setRootOperator(mergeOrder)
	c.anal.isFirst = false

	globalLimit := constructLimit(&plan.Node{Limit: topN})
	globalLimit.SetAnalyzeControl(c.anal.curNodeIdx, c.anal.isFirst)
	rs.setRootOperator(globalLimit)
	c.anal.isFirst = false
	return []*Scope{rs}
}

func (c *Compile) compileOrder(node *plan.Node, ss []*Scope) []*Scope {
	if c.IsSingleScope(ss) {
		currentFirstFlag := c.anal.isFirst
		order := constructOrder(node)
		order.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
		ss[0].setRootOperator(order)
		c.anal.isFirst = false

		currentFirstFlag = c.anal.isFirst
		mergeOrder := constructMergeOrder(node)
		mergeOrder.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
		ss[0].setRootOperator(mergeOrder)
		c.anal.isFirst = false
		return ss
	}

	currentFirstFlag := c.anal.isFirst
	for i := range ss {
		//c.anal.isFirst = currentFirstFlag
		order := constructOrder(node)
		order.SetIdx(c.anal.curNodeIdx)
		order.SetIsFirst(currentFirstFlag)
		ss[i].setRootOperator(order)
	}
	c.anal.isFirst = false

	ss = c.mergeShuffleScopesIfNeeded(ss, false)
	rs := c.newMergeScope(ss)

	currentFirstFlag = c.anal.isFirst
	mergeOrder := constructMergeOrder(node)
	mergeOrder.SetIdx(c.anal.curNodeIdx)
	mergeOrder.SetIsFirst(currentFirstFlag)
	rs.setRootOperator(mergeOrder)
	c.anal.isFirst = false

	return []*Scope{rs}
}

func (c *Compile) compileWin(node *plan.Node, ss []*Scope) []*Scope {
	partitionTopN := len(ss) == 1
	if partitionTopN {
		upstreamOp := ss[0].RootOp
		for upstreamOp.OpType() == vm.Projection && upstreamOp.GetOperatorBase().NumChildren() == 1 {
			upstreamOp = upstreamOp.GetOperatorBase().GetChildren(0)
		}
		upstream, ok := upstreamOp.(*partition.Partition)
		partitionTopN = ok && upstream.Limit != nil && upstream.PreReduce
	}
	rs := c.newMergeScope(ss)

	currentFirstFlag := c.anal.isFirst
	arg := constructWindow(c.proc.Ctx, node, c.proc)
	arg.PartitionTopN = partitionTopN
	arg.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
	rs.setRootOperator(arg)
	c.anal.isFirst = false

	return []*Scope{rs}
}

func (c *Compile) compileTimeWin(node *plan.Node, ss []*Scope) []*Scope {
	rs := c.newMergeScope(ss)

	currentFirstFlag := c.anal.isFirst
	arg := constructTimeWindow(c.proc.Ctx, node, c.proc)
	arg.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
	rs.setRootOperator(arg)
	c.anal.isFirst = false

	return []*Scope{rs}
}

func (c *Compile) compileFill(node *plan.Node, ss []*Scope) []*Scope {
	rs := c.newMergeScope(ss)

	currentFirstFlag := c.anal.isFirst
	arg := constructFill(node)
	arg.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
	rs.setRootOperator(arg)
	c.anal.isFirst = false

	return []*Scope{rs}
}

func (c *Compile) compileOffset(node *plan.Node, ss []*Scope) []*Scope {
	if c.ownsFoundRows(node) {
		// OFFSET owns the pre-offset count for SQL_CALC_FOUND_ROWS, so it must
		// run on the coordinator as well. This keeps remote workers stateless and
		// lets the following coordinator Limit preserve this complete count.
		rs := c.newMergeScope(ss)
		arg := constructOffset(node)
		arg.WithFoundRows(true)
		arg.SetAnalyzeControl(c.anal.curNodeIdx, c.anal.isFirst)
		rs.setRootOperator(arg)
		c.anal.isFirst = false
		return []*Scope{rs}
	}

	if c.IsSingleScope(ss) {
		currentFirstFlag := c.anal.isFirst
		op := constructOffset(node)
		op.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
		ss[0].setRootOperator(op)
		c.anal.isFirst = false
		return ss
	}

	rs := c.newMergeScope(ss)

	currentFirstFlag := c.anal.isFirst
	arg := constructOffset(node)
	arg.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
	rs.setRootOperator(arg)
	c.anal.isFirst = false

	return []*Scope{rs}
}

func (c *Compile) compileLimit(node *plan.Node, ss []*Scope) []*Scope {
	return c.compileLimitWithFoundRowsDrain(node, ss, false)
}

func (c *Compile) compileLimitWithFoundRowsDrain(node *plan.Node, ss []*Scope, drainOnly bool) []*Scope {
	if c.ownsFoundRows(node) || drainOnly {
		// Keep FOUND_ROWS-aware Limits on the coordinator. Installing them on
		// producer scopes would either publish partial counts from local workers
		// or stop a producer before the downstream owner observes EOF. Merging the
		// complete producer streams first gives draining and publication one
		// deterministic coordinator path.
		ss = c.mergeShuffleScopesIfNeeded(ss, false)
		rs := c.newMergeScope(ss)
		arg := constructLimit(node)
		if c.ownsFoundRows(node) {
			arg.WithFoundRows(true)
		} else {
			arg.WithFoundRowsDrain(true)
		}
		arg.SetAnalyzeControl(c.anal.curNodeIdx, c.anal.isFirst)
		rs.setRootOperator(arg)
		c.anal.isFirst = false
		return []*Scope{rs}
	}

	if c.IsSingleScope(ss) {
		currentFirstFlag := c.anal.isFirst
		op := constructLimit(node)
		op.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
		ss[0].setRootOperator(op)
		c.anal.isFirst = false
		return ss
	}

	currentFirstFlag := c.anal.isFirst
	for i := range ss {
		//c.anal.isFirst = currentFirstFlag
		op := constructLimit(node)
		op.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
		ss[i].setRootOperator(op)
	}
	c.anal.isFirst = false

	ss = c.mergeShuffleScopesIfNeeded(ss, false)
	rs := c.newMergeScope(ss)

	currentFirstFlag = c.anal.isFirst
	arg := constructLimit(node)
	arg.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
	rs.setRootOperator(arg)
	c.anal.isFirst = false

	return []*Scope{rs}
}

func (c *Compile) compileFuzzyFilter(node *plan.Node, ns []*plan.Node, left []*Scope, right []*Scope) ([]*Scope, error) {
	var l, r *Scope
	if c.IsSingleScope(left) {
		l = left[0]
	} else {
		l = c.newMergeScope(left)
	}
	if c.IsSingleScope(right) {
		r = right[0]
	} else {
		r = c.newMergeScope(right)
	}
	all := []*Scope{l, r}
	rs := c.newMergeScope(all)

	merge1 := rs.RootOp.(*merge.Merge)
	merge1.WithPartial(0, 1)
	merge2 := merge.NewArgument().WithPartial(1, 2)
	c.hasMergeOp = true

	currentFirstFlag := c.anal.isFirst
	op := constructFuzzyFilter(node, ns[node.Children[0]], ns[node.Children[1]])
	op.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
	rs.setRootOperator(op)
	op.AppendChild(merge2)
	c.anal.isFirst = false

	fuzzyCheck, err := newFuzzyCheck(node)
	if err != nil {
		return nil, err
	}
	c.fuzzys = append(c.fuzzys, fuzzyCheck)

	// wrap the collision key into c.fuzzy, for more information,
	// please refer fuzzyCheck.go
	op.Callback = func(bat *batch.Batch) error {
		if bat == nil || bat.IsEmpty() {
			return nil
		}
		// the batch will contain the key that fuzzyCheck
		if err := fuzzyCheck.fill(c.proc.Ctx, bat); err != nil {
			return err
		}
		return nil
	}
	return []*Scope{rs}, nil
}

func (c *Compile) compileSample(node *plan.Node, ss []*Scope) []*Scope {
	currentFirstFlag := c.anal.isFirst
	isSingle := c.IsSingleScope(ss)
	for i := range ss {
		op := constructSample(node, !isSingle)
		op.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
		ss[i].setRootOperator(op)
	}
	c.anal.isFirst = false
	if isSingle {
		return ss
	}

	rs := c.newMergeScope(ss)
	// should sample again if sample by rows.
	if node.SampleFunc.Rows != plan2.NotSampleByRows {
		currentFirstFlag = c.anal.isFirst
		op := sample.NewMergeSample(constructSample(node, true), false)
		op.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
		rs.setRootOperator(op)
		c.anal.isFirst = false
	}
	return []*Scope{rs}
}

func (c *Compile) compileTPGroup(node *plan.Node, ss []*Scope, ns []*plan.Node) []*Scope {
	currentFirstFlag := c.anal.isFirst
	if ss[0].HasPartialResults {
		op := constructGroup(c.proc.Ctx, node, ns[node.Children[0]], false, 0, c.proc)
		op.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
		ss[0].setRootOperator(op)
		arg := constructMergeGroup(node, ns[node.Children[0]], op.Aggs, op.UsesGroupingAwareHash())
		arg.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
		ss[0].setRootOperator(arg)
	} else {
		op := constructGroup(c.proc.Ctx, node, ns[node.Children[0]], true, 0, c.proc)
		op.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
		ss[0].setRootOperator(op)
	}
	ss[0].HasPartialResults = false
	c.anal.isFirst = false
	return ss
}

func (c *Compile) compileGroupWithoutShuffle(
	node *plan.Node,
	ss []*Scope,
	ns []*plan.Node,
	distinctRequiresSingleStage bool,
) []*Scope {
	if hasOrderedGroupConcat(node) || hasOrderedSetPercentile(node) ||
		(hasVarianceAggregate(node) && !c.supportsRemoteVarianceAggregates()) {
		return c.compileOrderedAggregateSingleStage(node, ss, ns)
	}
	if c.IsSingleScope(ss) {
		return c.compileTPGroup(node, ss, ns)
	}
	return c.compileMergeGroup(
		node, ss, ns, distinctRequiresSingleStage)
}

func isLocalPreAggregationGroup(parent, child *plan.Node) bool {
	if parent == nil || child == nil ||
		parent.NodeType != plan.Node_AGG || child.NodeType != plan.Node_AGG ||
		parent.Stats == nil || parent.Stats.HashmapStats == nil ||
		!parent.Stats.HashmapStats.Shuffle ||
		len(parent.AggList) != 0 || len(child.AggList) != 0 ||
		len(parent.GroupBy) == 0 || len(parent.GroupBy) != len(child.GroupBy) ||
		len(parent.GroupingFlag) != 0 || len(child.GroupingFlag) != 0 ||
		len(child.Children) != 1 ||
		child.Limit != nil || child.Offset != nil ||
		len(child.FilterList) != 0 || !isIdentityGroupByProjection(child) {
		return false
	}
	for i, expr := range parent.GroupBy {
		col := expr.GetCol()
		if col == nil {
			return false
		}
		if len(child.BindingTags) > 0 &&
			col.RelPos == child.BindingTags[0] && col.ColPos == int32(i) {
			continue
		}
		if col.RelPos != 0 || col.ColPos < 0 || int(col.ColPos) >= len(child.ProjectList) {
			return false
		}
		projectCol := child.ProjectList[col.ColPos].GetCol()
		if projectCol == nil || projectCol.RelPos != -1 || projectCol.ColPos != int32(i) {
			return false
		}
	}
	return true
}

func isIdentityGroupByProjection(node *plan.Node) bool {
	if len(node.ProjectList) == 0 {
		return true
	}
	if len(node.ProjectList) != len(node.GroupBy) {
		return false
	}
	for i, expr := range node.ProjectList {
		col := expr.GetCol()
		if col == nil || col.RelPos != -1 || col.ColPos != int32(i) {
			return false
		}
	}
	return true
}

func (c *Compile) compileLocalPreAggregationScope(
	step int32,
	nodeID int32,
	nodes []*plan.Node,
) ([]*Scope, error) {
	node := nodes[nodeID]
	ss, err := c.compilePlanScope(step, node.Children[0], nodes)
	if err != nil {
		return nil, err
	}
	groupInfo := constructGroup(c.proc.Ctx, node, nodes[node.Children[0]], false, 0, c.proc)
	defer groupInfo.Release()

	c.setAnalyzeCurrent(ss, int(nodeID))
	ss = c.ensureCoordinatorOnlyFunctions(node, ss)
	ss = c.compileLocalGroupBy(node, ss, nodes)
	ss = c.compileSort(node, c.compileProjection(node, c.compileRestrict(node, ss)))
	return ss, nil
}

// compileLocalGroupBy performs only the duplicate-reduction half of a logical
// GROUP BY. The planner emits this contract only when a downstream GROUP BY
// completes the same key after exchange, so merging here would defeat the
// pre-exchange reduction without adding correctness.
func (c *Compile) compileLocalGroupBy(
	node *plan.Node,
	ss []*Scope,
	ns []*plan.Node,
) []*Scope {
	currentFirstFlag := c.anal.isFirst
	for i := range ss {
		op := constructGroup(c.proc.Ctx, node, ns[node.Children[0]], false, 0, c.proc)
		op.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
		ss[i].setRootOperator(op)
		ss[i].HasPartialResults = false
	}
	c.anal.isFirst = false
	return ss
}

func (c *Compile) compileMergeGroup(
	node *plan.Node,
	ss []*Scope,
	ns []*plan.Node,
	distinctRequiresSingleStage bool,
) []*Scope {
	// DISTINCT aggregates without an exact state-merge contract run one Group
	// operator before MergeGroup. Parallel-mergeable DISTINCT aggregates use the
	// ordinary local Group + MergeGroup pipeline below.
	if distinctRequiresSingleStage {
		ss = c.mergeShuffleScopesIfNeeded(ss, false)
		mergeToGroup := c.newMergeScope(ss)

		currentFirstFlag := c.anal.isFirst
		op := constructGroup(c.proc.Ctx, node, ns[node.Children[0]], false, 0, c.proc)
		op.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
		mergeToGroup.setRootOperator(op)
		c.anal.isFirst = false

		rs := c.newMergeScope([]*Scope{mergeToGroup})

		currentFirstFlag = c.anal.isFirst
		arg := constructMergeGroup(node, ns[node.Children[0]], op.Aggs, op.UsesGroupingAwareHash())
		arg.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
		rs.setRootOperator(arg)
		c.anal.isFirst = false

		return []*Scope{rs}
	} else {
		var aggs []aggexec.AggFuncExecExpression
		groupingAware := false

		currentFirstFlag := c.anal.isFirst
		for i := range ss {
			op := constructGroup(c.proc.Ctx, node, ns[node.Children[0]], false, 0, c.proc)
			op.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
			ss[i].setRootOperator(op)

			if i == 0 {
				aggs = op.Aggs
				groupingAware = op.UsesGroupingAwareHash()
			}
		}
		c.anal.isFirst = false

		ss = c.mergeShuffleScopesIfNeeded(ss, false)
		rs := c.newMergeScope(ss)

		currentFirstFlag = c.anal.isFirst
		arg := constructMergeGroup(node, ns[node.Children[0]], aggs, groupingAware)
		arg.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
		rs.setRootOperator(arg)
		c.anal.isFirst = false

		return []*Scope{rs}
	}
}

func (c *Compile) compileOrderedAggregateSingleStage(
	node *plan.Node,
	ss []*Scope,
	ns []*plan.Node,
) []*Scope {
	ss = c.mergeShuffleScopesIfNeeded(ss, false)
	rs := c.newMergeScope(ss)
	currentFirstFlag := c.anal.isFirst
	op := constructGroup(c.proc.Ctx, node, ns[node.Children[0]], true, 0, c.proc)
	op.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
	rs.setRootOperator(op)
	c.anal.isFirst = false
	return []*Scope{rs}
}

func hasOrderedGroupConcat(node *plan.Node) bool {
	for _, agg := range node.AggList {
		if fn := agg.GetF(); fn != nil &&
			fn.Func.ObjName == plan2.NameGroupConcat &&
			fn.AggConfigType == plan.AggregateConfigType_AGG_CONFIG_GROUP_CONCAT_ORDER {
			return true
		}
	}
	return false
}

func hasOrderedSetPercentile(node *plan.Node) bool {
	for _, agg := range node.AggList {
		if fn := agg.GetF(); fn != nil {
			switch fn.Func.ObjName {
			case plan2.NamePercentileCont, plan2.NamePercentileDisc:
				return true
			}
		}
	}
	return false
}

func hasVarianceAggregate(node *plan.Node) bool {
	for _, agg := range node.AggList {
		if fn := agg.GetF(); fn != nil {
			switch int64(uint64(fn.Func.Obj) & function.DistinctMask) {
			case aggexec.AggIdOfVarPop, aggexec.AggIdOfVarSample,
				aggexec.AggIdOfStdDevPop, aggexec.AggIdOfStdDevSample:
				return true
			}
		}
	}
	return false
}

func (c *Compile) supportsRemoteOrderedAggregates() bool {
	return supportsRemoteOrderedAggregates(c.proc.GetService())
}

func (c *Compile) supportsRemoteOrderedSetAggregates() bool {
	return supportsRemoteOrderedSetAggregates(c.proc.GetService())
}

func supportsRemoteOrderedAggregates(service string) bool {
	// MOProtocolVersion is the service-local deployment rollout gate.
	// Deployment orchestration raises it after participating receivers
	// understand Aggregate.config_type and lowers it before rollback.
	version, ok := moruntime.ServiceRuntime(service).
		GetGlobalVariables(moruntime.MOProtocolVersion)
	if !ok {
		return false
	}
	protocolVersion, ok := version.(int64)
	return ok && protocolVersion >= defines.MORPCVersion6
}

func supportsRemoteOrderedSetAggregates(service string) bool {
	version, ok := moruntime.ServiceRuntime(service).
		GetGlobalVariables(moruntime.MOProtocolVersion)
	if !ok {
		return false
	}
	protocolVersion, ok := version.(int64)
	return ok && protocolVersion >= defines.MORPCVersion17
}

func (c *Compile) supportsRemoteVarianceAggregates() bool {
	version, ok := moruntime.ServiceRuntime(c.proc.GetService()).
		GetGlobalVariables(moruntime.MOProtocolVersion)
	if !ok {
		return false
	}
	protocolVersion, ok := version.(int64)
	return ok && protocolVersion >= defines.MORPCVersion35
}

func (c *Compile) supportsRemotePartitionTopN() bool {
	version, ok := moruntime.ServiceRuntime(c.proc.GetService()).
		GetGlobalVariables(moruntime.MOProtocolVersion)
	if !ok {
		return false
	}
	protocolVersion, ok := version.(int64)
	return ok && protocolVersion >= defines.MORPCVersion19
}

func (c *Compile) supportsRemoteHashPartition() bool {
	version, ok := moruntime.ServiceRuntime(c.proc.GetService()).
		GetGlobalVariables(moruntime.MOProtocolVersion)
	if !ok {
		return false
	}
	protocolVersion, ok := version.(int64)
	return ok && protocolVersion >= defines.MORPCVersion47
}

func supportsRemoteTextCollationAggregates(service string) bool {
	version, ok := moruntime.ServiceRuntime(service).
		GetGlobalVariables(moruntime.MOProtocolVersion)
	if !ok {
		return false
	}
	protocolVersion, ok := version.(int64)
	return ok && protocolVersion >= defines.MORPCVersion14
}

func supportsRemoteAsofJoin(service string) bool {
	version, ok := moruntime.ServiceRuntime(service).
		GetGlobalVariables(moruntime.MOProtocolVersion)
	if !ok {
		return false
	}
	protocolVersion, ok := version.(int64)
	return ok && protocolVersion >= defines.MORPCVersion27
}

// supportsRemoteMongoUserQuery guards the MongoScan payload that carries
// validated BSON. An older CN would ignore these protobuf fields and silently
// execute the legacy unfiltered Find path, so explicit queries must wait until
// deployment has raised the cluster's oldest-live protocol version.
func supportsRemoteMongoUserQuery(service string) bool {
	rt := moruntime.ServiceRuntime(service)
	if rt == nil {
		return false
	}
	version, ok := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	if !ok {
		return false
	}
	protocolVersion, ok := version.(int64)
	return ok && protocolVersion >= defines.MORPCVersion44
}

// mongoScanUsesV44Payload reports whether a scan uses any field introduced by
// the MongoDB query protocol. Older receivers ignore all three fields, so each
// one must be rejected during a mixed-version rollout.
func mongoScanUsesV44Payload(scan *plan.MongoScan) bool {
	return scan != nil && (scan.UserQueryKind != 0 || scan.IncludeQueryColumn || scan.EmptyResult)
}

func supportsRemoteTargetAwareUpdate(service string) bool {
	rt := moruntime.ServiceRuntime(service)
	if rt == nil {
		return false
	}
	version, ok := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	if !ok {
		return false
	}
	protocolVersion, ok := version.(int64)
	return ok && protocolVersion >= defines.MORPCVersion20
}

func supportsRemoteRightDedupInputKeysUnique(service string) bool {
	rt := moruntime.ServiceRuntime(service)
	if rt == nil {
		return false
	}
	version, ok := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	if !ok {
		return false
	}
	protocolVersion, ok := version.(int64)
	return ok && protocolVersion >= defines.MORPCVersion21
}

func supportsRemoteAffectedRowsSelectors(service string) bool {
	rt := moruntime.ServiceRuntime(service)
	if rt == nil {
		return false
	}
	version, ok := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	if !ok {
		return false
	}
	protocolVersion, ok := version.(int64)
	return ok && protocolVersion >= defines.MORPCVersion24
}

func supportsRemoteCrossDomainStringLiterals(service string) bool {
	rt := moruntime.ServiceRuntime(service)
	if rt == nil {
		return false
	}
	version, ok := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	if !ok {
		return false
	}
	protocolVersion, ok := version.(int64)
	return ok && protocolVersion >= defines.MORPCVersion23
}

func remoteMORPCProtocolVersion(service string) (int64, bool) {
	rt := moruntime.ServiceRuntime(service)
	if rt == nil {
		return 0, false
	}
	version, ok := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	if !ok {
		return 0, false
	}
	protocolVersion, ok := version.(int64)
	return protocolVersion, ok
}

func supportsRemoteStatementLastInsertID(service string) bool {
	rt := moruntime.ServiceRuntime(service)
	if rt == nil {
		return false
	}
	version, ok := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	if !ok {
		return false
	}
	protocolVersion, ok := version.(int64)
	return ok && protocolVersion >= defines.MORPCVersion26
}

func supportsRemoteUpdateChangedRows(service string) bool {
	rt := moruntime.ServiceRuntime(service)
	if rt == nil {
		return false
	}
	version, ok := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	if !ok {
		return false
	}
	protocolVersion, ok := version.(int64)
	return ok && protocolVersion >= defines.MORPCVersion25
}

func supportsRemotePadSpaceSemantics(service string) bool {
	rt := moruntime.ServiceRuntime(service)
	if rt == nil {
		return false
	}
	version, ok := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	if !ok {
		return false
	}
	protocolVersion, ok := version.(int64)
	return ok && protocolVersion >= defines.MORPCVersion40
}

func supportsRemoteParquetWholeFileFanout(service string) bool {
	rt := moruntime.ServiceRuntime(service)
	if rt == nil {
		return false
	}
	version, ok := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	if !ok {
		return false
	}
	protocolVersion, ok := version.(int64)
	return ok && protocolVersion >= defines.MORPCVersion45
}

func supportsRemoteGroupingSetExpansion(service string) bool {
	rt := moruntime.ServiceRuntime(service)
	if rt == nil {
		return false
	}
	version, ok := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	if !ok {
		return false
	}
	protocolVersion, ok := version.(int64)
	return ok && protocolVersion >= defines.MORPCVersion49
}

func (c *Compile) canCompileShuffleGroup(node *plan.Node) bool {
	return node.Stats.HashmapStats != nil &&
		node.Stats.HashmapStats.Shuffle &&
		(!hasOrderedGroupConcat(node) || c.supportsRemoteOrderedAggregates()) &&
		(!hasOrderedSetPercentile(node) || c.supportsRemoteOrderedSetAggregates()) &&
		(!hasVarianceAggregate(node) || c.supportsRemoteVarianceAggregates())
}

func (c *Compile) compileLocalShuffleGroup(node *plan.Node, inputSS []*Scope, nodes []*plan.Node) []*Scope {
	if node.Stats.Dop != nodes[node.Children[0]].Stats.Dop {
		panic("wrong shuffle dop for shuffle group!")
	}
	shuffleArg := constructShuffleArgForGroup(node.Stats.Dop, node)
	shuffleArg.SetAnalyzeControl(c.anal.curNodeIdx, false)
	inputSS[0].setRootOperator(shuffleArg)

	groupOp := constructGroup(c.proc.Ctx, node, nodes[node.Children[0]], true, inputSS[0].NodeInfo.Mcpu, c.proc)
	groupOp.SetAnalyzeControl(c.anal.curNodeIdx, false)
	inputSS[0].setRootOperator(groupOp)

	return inputSS
}

func (c *Compile) compileShuffleGroup(node *plan.Node, inputSS []*Scope, nodes []*plan.Node) []*Scope {
	stageNodes := c.shuffleStageNodes(inputSS)
	if node.Stats.HashmapStats.ShuffleMethod == plan.ShuffleMethod_Reuse {
		currentFirstFlag := c.anal.isFirst
		for i := range inputSS {
			op := constructGroup(c.proc.Ctx, node, nodes[node.Children[0]], true, len(inputSS), c.proc)
			op.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
			inputSS[i].setRootOperator(op)
		}
		c.anal.isFirst = false
		return inputSS
	}
	// A normal shuffle is useful only when it creates more than one physical
	// aggregate owner. In particular, max_dop=1 on one CN would otherwise hash
	// every row into one bucket and add a dispatch/receiver pipeline with no
	// reduction in retained aggregate state.
	if node.Stats.Dop <= 1 && len(stageNodes) <= 1 {
		return c.compileGroupWithoutShuffle(
			node,
			inputSS,
			nodes,
			plan2.RequiresSingleStageDistinctAgg(node),
		)
	}
	if len(stageNodes) == 1 && len(inputSS) == 1 && inputSS[0].NodeInfo.Mcpu > 1 && inputSS[0].NodeInfo.Mcpu == int(node.Stats.Dop) {
		return c.compileLocalShuffleGroup(node, inputSS, nodes)
	}

	inputSS = c.mergeShuffleScopesIfNeeded(inputSS, true)
	if len(stageNodes) > 1 {
		// merge here to avoid bugs, delete this in the future
		for i := range inputSS {
			if inputSS[i].NodeInfo.Mcpu > 1 {
				inputSS[i] = c.newMergeScopeByCN([]*Scope{inputSS[i]}, inputSS[i].NodeInfo)
			}
		}
	}

	shuffleGroups := make([]*Scope, 0, len(stageNodes))
	dop := int(node.Stats.Dop)
	for _, cn := range stageNodes {
		scopes := c.newScopeListWithNode(dop, len(inputSS), cn)
		for _, s := range scopes {
			for _, rr := range s.Proc.Reg.MergeReceivers {
				rr.ResetForReuse(shuffleChannelBufferSize, rr.NilBatchCnt)
			}
		}
		shuffleGroups = append(shuffleGroups, scopes...)
	}

	j := 0
	for i := range inputSS {
		shuffleArg := constructShuffleArgForGroup(int32(len(shuffleGroups)), node)
		shuffleArg.DrainAllBuckets = true
		shuffleArg.SetAnalyzeControl(c.anal.curNodeIdx, false)
		inputSS[i].setRootOperator(shuffleArg)
		if len(stageNodes) > 1 && inputSS[i].NodeInfo.Mcpu > 1 { // merge here to avoid bugs, delete this in the future
			inputSS[i] = c.newMergeScopeByCN([]*Scope{inputSS[i]}, inputSS[i].NodeInfo)
		}
		dispatchArg := constructDispatch(j, shuffleGroups, inputSS[i], node, false)
		dispatchArg.SetAnalyzeControl(c.anal.curNodeIdx, false)
		inputSS[i].setRootOperator(dispatchArg)
		j++
		inputSS[i].IsEnd = true
	}

	currentIsFirst := c.anal.isFirst
	for i := range shuffleGroups {
		groupOp := constructGroup(c.proc.Ctx, node, nodes[node.Children[0]], true, len(shuffleGroups), c.proc)
		groupOp.SetAnalyzeControl(c.anal.curNodeIdx, currentIsFirst)
		shuffleGroups[i].setRootOperator(groupOp)
	}
	c.anal.isFirst = false

	//append prescopes
	c.appendPrescopes(shuffleGroups, inputSS, stageNodes)
	return shuffleGroups

}

func (c *Compile) appendPrescopes(parents, children []*Scope, stageNodes engine.Nodes) {
	for _, cn := range stageNodes {
		index := 0
		for i := range parents {
			if sameExecutionNode(cn, parents[i].NodeInfo) {
				index = i
				break
			}
		}
		for i := range children {
			if sameExecutionNode(cn, children[i].NodeInfo) {
				parents[index].PreScopes = append(parents[index].PreScopes, children[i])
			}
		}
	}
}

// compilePreInsert Compile PreInsert Node and set it as the root operator for each Scope.
func (c *Compile) compilePreInsert(nodes []*plan.Node, node *plan.Node, ss []*Scope) ([]*Scope, error) {
	currentFirstFlag := c.anal.isFirst
	for i := range ss {
		preInsertArg, err := constructPreInsert(nodes, node, c.e, c.proc)
		if err != nil {
			return nil, err
		}
		preInsertArg.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
		ss[i].setRootOperator(preInsertArg)
	}
	c.anal.isFirst = false
	return ss, nil
}

func (c *Compile) compileInsert(nodes []*plan.Node, node *plan.Node, ss []*Scope) ([]*Scope, error) {
	if ok, err := isIcebergAppendInsert(c.proc.Ctx, node); err != nil {
		return nil, err
	} else if ok {
		currentFirstFlag := c.anal.isFirst
		// A single Iceberg writer is a correctness boundary, not only a layout
		// optimization. The coordinator owns one commit generation and publishes
		// only after its input reaches terminal state; splitting it across remote
		// or parallel scopes would require explicit scope registration and a
		// failure-aware barrier before any scope may commit.
		if icebergInsertNeedsSingleWriterMerge(ss, toEngineNode(c.currentCNWorker())) {
			ss = []*Scope{c.newMergeScope(ss)}
		}
		for i := range ss {
			insertArg, err := c.constructIcebergInsert(nodes, node)
			if err != nil {
				return nil, err
			}
			insertArg.GetOperatorBase().SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
			ss[i].setRootOperator(insertArg)
			ss[i].NodeInfo.Mcpu = 1
		}
		c.anal.isFirst = false
		return ss, nil
	}

	// Writable external table: each parallel pipeline owns one writer/file.
	// Reuse the simple (non-S3, no merge-block) layout: one insert operator per
	// source scope, with no shuffle.
	if isExternalWriteInsert(node) {
		currentFirstFlag := c.anal.isFirst
		// One timestamp per statement: all scopes must expand WRITE_FILE_PATTERN
		// time directives against the same instant.
		stmtAt := externalInsertStmtTime(c.proc, c.startAt)
		localFileStage, err := externalInsertTargetIsLocalFile(c.proc, node, stmtAt)
		if err != nil {
			return nil, err
		}
		if localFileStage {
			// Only merge scopes on remote CNs onto the current CN.
			// Same-CN parallel writers share the same filesystem and
			// use unique filename directives (%U / %<n>N), so they are safe
			// to keep unmerged.
			var localSS, remoteSS []*Scope
			currentNode := toEngineNode(c.currentCNWorker())
			for _, s := range ss {
				if sameExecutionNode(s.NodeInfo, currentNode) {
					localSS = append(localSS, s)
				} else {
					remoteSS = append(remoteSS, s)
				}
			}
			if len(remoteSS) > 0 {
				mergedRemote := c.newMergeScope(remoteSS)
				localSS = append(localSS, mergedRemote)
			}
			ss = localSS
		}
		for i := range ss {
			insertArg, err := constructExternalInsert(c.proc, node, c.e, stmtAt)
			if err != nil {
				return nil, err
			}
			insertArg.GetOperatorBase().SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
			ss[i].setRootOperator(insertArg)
		}
		c.anal.isFirst = false
		return ss, nil
	}

	// Determine whether to Write S3
	toWriteS3 := node.Stats.GetOutcnt()*float64(SingleLineSizeEstimate) >
		float64(DistributedThreshold) || c.anal.qry.LoadWriteS3

	if !toWriteS3 {
		// A non-S3 INSERT can still drive a cross-CN shuffle join: toWriteS3 is decided by the
		// INSERT *output* row count, while shuffle is decided by the JOIN *input* table size and
		// CN count -- independent decisions. A large shuffle join with a highly selective filter
		// can produce few output rows (non-S3) yet still shuffle across CNs. So group the same-CN
		// shuffle buckets (with their nested cross-CN dispatch) into one per-CN send unit *before*
		// attaching Insert. This (a) keeps the dispatch in the same tree as all its local buckets
		// so it remains standalone-executable on its own CN instead of failing before remote start
		// (historically this was silently converted to local and hung; issue #24919), and (b) puts
		// Insert on the per-CN container's RootOp chain
		// (Insert -> Merge), so affectedRows() -- which walks the RootOp chain -- still counts it.
		// Noop when ss carries no cross-CN shuffle dispatch.
		ss = c.groupShuffleBucketsByCNIfNeeded(ss)
		currentFirstFlag := c.anal.isFirst
		// Not write S3
		for i := range ss {
			insertArg, err := constructInsert(c.proc, node, c.e, false)
			if err != nil {
				return nil, err
			}

			insertArg.GetOperatorBase().SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
			ss[i].setRootOperator(insertArg)
		}
		c.anal.isFirst = false
		return ss, nil
	}

	// to write S3
	if haveSinkScanInPlan(nodes, node.Children[0]) {
		// todo : pipelines with sink scan ,must refactor this in the future
		currentFirstFlag := c.anal.isFirst
		c.anal.isFirst = false
		// dataScope merges the buckets, but dataScope.MergeRun still sends each bucket as an
		// individual RemoteRun unit, so a cross-CN shuffle dispatch here would hit the same
		// non-standalone remote-start failure. Group same-CN buckets into one per-CN send unit first
		// (historically this was a convert-to-local hang; issue #24919).
		ss = c.groupShuffleBucketsByCNIfNeeded(ss)
		dataScope := c.newMergeScope(ss)
		if c.anal.qry.LoadTag {
			// reset the channel buffer of sink for load
			dataScope.Proc.Reg.MergeReceivers[0].ResetForReuse(
				loadMergeReceiverChannelBufferSize,
				dataScope.Proc.Reg.MergeReceivers[0].NilBatchCnt)
		}
		parallelSize := c.getLoadWriteS3ParallelSize(node, c.ncpu)
		scopes := make([]*Scope, 0, parallelSize)
		c.hasMergeOp = true
		for i := 0; i < parallelSize; i++ {
			s := c.newEmptyMergeScope()
			mergeArg := merge.NewArgument()
			mergeArg.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
			s.setRootOperator(mergeArg)
			scopes = append(scopes, s)
			scopes[i].Proc = c.proc.NewNoContextChildProc(1)
			if c.anal.qry.LoadTag {
				for _, rr := range scopes[i].Proc.Reg.MergeReceivers {
					rr.ResetForReuse(shuffleChannelBufferSize, rr.NilBatchCnt)
				}
			}
		}
		if c.anal.qry.LoadTag && node.Stats.HashmapStats != nil && node.Stats.HashmapStats.Shuffle && dataScope.NodeInfo.Mcpu == parallelSize && parallelSize > 1 {
			arg, err := constructLocalDispatchFromScopes(0, scopes, dataScope)
			if err != nil {
				return nil, err
			}
			arg.FuncId = dispatch.ShuffleToAllFunc
			arg.ShuffleType = plan2.ShuffleToLocalMatchedReg
			arg.SetAnalyzeControl(c.anal.curNodeIdx, false)
			dataScope.setRootOperator(arg)
		} else {
			dispatchArg, err := constructLocalDispatchFromScopes(0, scopes, dataScope)
			if err != nil {
				return nil, err
			}
			dispatchArg.FuncId = dispatch.SendToAnyLocalFunc
			dispatchArg.SetAnalyzeControl(c.anal.curNodeIdx, false)
			dataScope.setRootOperator(dispatchArg)
		}
		dataScope.IsEnd = true
		for i := range scopes {
			insertArg, err := constructInsert(c.proc, node, c.e, true)
			if err != nil {
				return nil, err
			}

			insertArg.GetOperatorBase().SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
			scopes[i].setRootOperator(insertArg)
		}
		currentFirstFlag = false
		rs := c.newMergeScope(scopes)
		rs.PreScopes = append(rs.PreScopes, dataScope)
		rs.Magic = MergeInsert
		mergeInsertArg := constructMergeblock(c.e, node)
		mergeInsertArg.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
		rs.setRootOperator(mergeInsertArg)
		ss = []*Scope{rs}
		return ss, nil
	}

	c.proc.Debugf(c.proc.Ctx, "insert of '%s' write s3\n", c.sql)
	currentFirstFlag := c.anal.isFirst
	c.anal.isFirst = false
	for i := range ss {
		insertArg, err := constructInsert(c.proc, node, c.e, true)
		if err != nil {
			return nil, err
		}

		insertArg.GetOperatorBase().SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
		ss[i].setRootOperator(insertArg)
	}
	currentFirstFlag = false
	// Group a CN's dop shuffle buckets (and the shuffle dispatch nested under them) into one
	// per-CN send unit before the coordinator merge, so the cross-CN shuffle dispatch is sent
	// to and executed at its own CN. Without grouping the tree is rejected before remote start;
	// historically it was moved to the coordinator, mispaired the receiver, and hung (#24919).
	ss = c.groupShuffleBucketsByCNIfNeeded(ss)
	rs := c.newMergeScope(ss)
	rs.Magic = MergeInsert
	mergeInsertArg := constructMergeblock(c.e, node)
	mergeInsertArg.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
	rs.setRootOperator(mergeInsertArg)
	ss = []*Scope{rs}
	return ss, nil
}

func icebergInsertNeedsSingleWriterMerge(ss []*Scope, currentCN engine.Node) bool {
	if len(ss) != 1 {
		return len(ss) > 1
	}
	if ss[0] == nil {
		return false
	}
	return ss[0].NodeInfo.Mcpu > 1 || !sameExecutionNode(ss[0].NodeInfo, currentCN)
}

func (c *Compile) compileMultiUpdate(node *plan.Node, ss []*Scope) ([]*Scope, error) {
	// Determine whether to Write S3
	toWriteS3 := node.Stats.GetOutcnt()*float64(SingleLineSizeEstimate) >
		float64(DistributedThreshold) || c.anal.qry.LoadWriteS3

	currentFirstFlag := c.anal.isFirst
	if toWriteS3 {
		if len(ss) == 1 && ss[0].NodeInfo.Mcpu == 1 {
			mcpu := c.getParallelSizeForExternalScan(node, c.ncpu)
			if mcpu > 1 {
				oldScope := ss[0]

				ss = make([]*Scope, mcpu)
				for i := 0; i < mcpu; i++ {
					ss[i] = c.newEmptyMergeScope()
					mergeArg := merge.NewArgument()
					mergeArg.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
					ss[i].setRootOperator(mergeArg)
					ss[i].Proc = c.proc.NewNoContextChildProc(1)
					ss[i].NodeInfo = scopeNodeWithMcpu(oldScope.NodeInfo, 1)
				}
				dispatchOp, err := constructLocalDispatchFromScopes(0, ss, oldScope)
				if err != nil {
					return nil, err
				}
				dispatchOp.FuncId = dispatch.SendToAnyLocalFunc
				dispatchOp.SetAnalyzeControl(c.anal.curNodeIdx, false)
				oldScope.setRootOperator(dispatchOp)

				ss[0].PreScopes = append(ss[0].PreScopes, oldScope)
			}
		}

		for i := range ss {
			multiUpdateArg, err := constructMultiUpdate(node, c.e, c.proc, multi_update.UpdateWriteS3, ss[i].IsRemote)
			if err != nil {
				return nil, err
			}

			multiUpdateArg.GetOperatorBase().SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
			ss[i].setRootOperator(multiUpdateArg)
		}

		// Group a CN's dop shuffle buckets (and the shuffle dispatch nested under them) into one
		// per-CN send unit before the coordinator merge, so the cross-CN shuffle dispatch is sent
		// to and executed at its own CN. Without grouping the tree is rejected before remote start;
		// historically it was moved to the coordinator, mispaired the receiver, and hung (#24919).
		ss = c.groupShuffleBucketsByCNIfNeeded(ss)
		rs := ss[0]
		if len(ss) > 1 || ss[0].NodeInfo.Mcpu > 1 {
			rs = c.newMergeScope(ss)
		}

		multiUpdateArg, err := constructMultiUpdate(node, c.e, c.proc, multi_update.UpdateFlushS3Info, rs.IsRemote)
		if err != nil {
			return nil, err
		}

		rs.setRootOperator(multiUpdateArg)
		ss = []*Scope{rs}
	} else {
		if !c.IsTpQuery() {
			// keep a cross-CN shuffle dispatch in the same send unit as all its local buckets (issue #24919).
			ss = c.groupShuffleBucketsByCNIfNeeded(ss)
			rs := c.newMergeScope(ss)
			ss = []*Scope{rs}
		}
		multiUpdateArg, err := constructMultiUpdate(node, c.e, c.proc, multi_update.UpdateWriteTable, ss[0].IsRemote)
		if err != nil {
			return nil, err
		}
		multiUpdateArg.GetOperatorBase().SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
		ss[0].setRootOperator(multiUpdateArg)
	}
	c.anal.isFirst = false

	return ss, nil
}

func (c *Compile) compilePreInsertUk(node *plan.Node, ss []*Scope) []*Scope {
	currentFirstFlag := c.anal.isFirst
	if node.PreInsertUkCtx.GetInsertIgnoreMultiDedup() &&
		(len(ss) > 1 || ss[0].NodeInfo.Mcpu > 1) {
		// Multi-key INSERT IGNORE arbitration is row-global: partitioning by one
		// key cannot observe conflicts on the other keys.  Merge candidate streams
		// before the stateful arbiter; ordinary index PRE_INSERT_UK stays parallel.
		ss = []*Scope{c.newMergeScope(ss)}
	}
	for i := range ss {
		preInsertUkArg := constructPreInsertUk(node)
		preInsertUkArg.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
		ss[i].setRootOperator(preInsertUkArg)
	}
	c.anal.isFirst = false
	return ss
}

func (c *Compile) compilePreInsertSK(node *plan.Node, ss []*Scope) []*Scope {
	currentFirstFlag := c.anal.isFirst
	for i := range ss {
		preInsertSkArg := constructPreInsertSk(node)
		preInsertSkArg.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
		ss[i].setRootOperator(preInsertSkArg)
	}
	c.anal.isFirst = false
	return ss
}

func (c *Compile) compileDelete(node *plan.Node, ss []*Scope) ([]*Scope, error) {
	currentFirstFlag := c.anal.isFirst
	op, err := constructDeletion(c.proc, node, c.e)
	if err != nil {
		return nil, err
	}

	op.GetOperatorBase().SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
	c.anal.isFirst = false

	var arg *deletion.Deletion
	if _, ok := op.(*deletion.Deletion); ok {
		arg = op.(*deletion.Deletion)
	} else {
		arg = op.(*deletion.PartitionDelete).GetDelete()
	}

	if node.Stats.GetOutcnt()*float64(SingleLineSizeEstimate) > float64(DistributedThreshold) && !arg.DeleteCtx.CanTruncate {
		rs := c.newDeleteMergeScope(arg, ss, node)
		rs.Magic = MergeDelete

		mergeDeleteArg := mergedelete.NewArgument().
			WithObjectRef(arg.DeleteCtx.Ref).
			WithEngine(c.e).
			WithAddAffectedRows(arg.DeleteCtx.AddAffectedRows)

		currentFirstFlag = c.anal.isFirst
		mergeDeleteArg.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
		rs.setRootOperator(mergeDeleteArg)
		c.anal.isFirst = false

		ss = []*Scope{rs}
		arg.Release()
		return ss, nil
	} else {
		var rs *Scope
		if c.IsSingleScope(ss) {
			rs = ss[0]
		} else {
			rs = c.newMergeScope(ss)
		}

		rs.setRootOperator(op)
		ss = []*Scope{rs}
		return ss, nil
	}
}

func (c *Compile) compileLock(node *plan.Node, ss []*Scope) ([]*Scope, error) {
	lockRows := make([]*plan.LockTarget, 0, len(node.LockTargets))
	localizeLoadPlan := c.loadUniqueIndexPromotion != nil
	filterPromotedRows := false
	if state := c.loadUniqueIndexPromotion; state != nil &&
		state.phase == loadUniqueIndexPromotionFenced {
		if err := state.validateRetryProof(c); err != nil {
			return nil, err
		}
		filterPromotedRows = true
	}
	for _, canonicalTarget := range node.LockTargets {
		if filterPromotedRows && c.loadUniqueIndexPromotion.coversRowTarget(canonicalTarget) {
			continue
		}
		tbl := canonicalTarget
		if localizeLoadPlan && (canonicalTarget.LockTable || canonicalTarget.LockTableAtTheEnd) {
			// Only table-lock disposition is annotated during physical compile. A
			// shallow value copy keeps the canonical generation immutable without
			// changing allocation or mutation behavior for non-candidate statements.
			localTarget := *canonicalTarget
			tbl = &localTarget
		}
		if c.shouldPrePipelineLockTable(tbl) {
			c.lockTables[tbl.TableId] = tbl
		} else {
			if _, ok := c.lockTables[tbl.TableId]; !ok {
				lockRows = append(lockRows, tbl)
			}
		}
	}
	if !localizeLoadPlan {
		// Preserve exact-main compile behavior outside the positively admitted
		// LOAD path, including its existing canonical-node reuse contract.
		node.LockTargets = lockRows
	}
	if len(lockRows) == 0 {
		return ss, nil
	}

	block := false
	// only pessimistic txn needs to block downstream operators.
	if c.proc.GetTxnOperator().Txn().IsPessimistic() {
		block = lockRows[0].Block
		if block {
			c.needBlock = true
		}
	}

	currentFirstFlag := c.anal.isFirst
	if !c.IsTpQuery() || len(c.pn.GetQuery().Steps) > 1 { // todo: don't support dml with multi steps for now
		rs := c.newMergeScope(ss)
		ss = []*Scope{rs}
	}
	var err error
	var lockOpArg *lockop.LockOp
	lockNode := node
	if localizeLoadPlan {
		localNode := *node
		localNode.LockTargets = lockRows
		lockNode = &localNode
	}
	lockOpArg, err = constructLockOp(lockNode, c.e)
	if err != nil {
		return nil, err
	}
	lockOpArg.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
	ss[0].doSetRootOperator(lockOpArg)
	c.anal.isFirst = false
	return ss, nil
}

func (c *Compile) compileRecursiveCte(node *plan.Node, curNodeIdx int32) ([]*Scope, error) {
	receivers := make([]*process.WaitRegister, len(node.SourceStep))
	for i, step := range node.SourceStep {
		receivers[i] = c.getNodeReg(step, curNodeIdx)
		if receivers[i] == nil {
			return nil, moerr.NewInternalError(c.proc.Ctx, "no data sender for sinkScan node")
		}
	}
	rs := c.newEmptyMergeScope()
	rs.Proc = c.proc.NewNoContextChildProc(len(receivers))
	rs.Proc.Reg.MergeReceivers = receivers

	//for mergecte, children[0] receive from the first channel, and children[1] receive from the rest channels
	mergeOp1 := merge.NewArgument()
	mergeOp1.SetAnalyzeControl(c.anal.curNodeIdx, false)
	mergeOp1.WithPartial(0, 1)
	rs.setRootOperator(mergeOp1)

	currentFirstFlag := c.anal.isFirst
	mergecteArg := mergecte.NewArgument().
		WithNodeCnt(len(node.SourceStep) - 1).
		WithDistinct(node.RecursiveUnionDistinct)
	mergecteArg.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
	rs.setRootOperator(mergecteArg)
	c.anal.isFirst = false

	mergeOp2 := merge.NewArgument()
	mergeOp2.WithPartial(1, int32(len(receivers)))
	mergecteArg.SetAnalyzeControl(c.anal.curNodeIdx, false)
	mergecteArg.AppendChild(mergeOp2)
	c.anal.isFirst = false
	c.hasMergeOp = true

	return []*Scope{rs}, nil
}

func (c *Compile) compileRecursiveScan(node *plan.Node, curNodeIdx int32) ([]*Scope, error) {
	receivers := make([]*process.WaitRegister, len(node.SourceStep))
	for i, step := range node.SourceStep {
		receivers[i] = c.getNodeReg(step, curNodeIdx)
		if receivers[i] == nil {
			return nil, moerr.NewInternalError(c.proc.Ctx, "no data sender for sinkScan node")
		}
	}
	rs := c.newEmptyMergeScope()
	rs.Proc = c.proc.NewNoContextChildProc(len(receivers))
	rs.Proc.Reg.MergeReceivers = receivers

	mergeOp := merge.NewArgument()
	c.hasMergeOp = true
	rs.setRootOperator(mergeOp)
	currentFirstFlag := c.anal.isFirst
	mergeRecursiveArg := mergerecursive.NewArgument()
	mergeRecursiveArg.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
	rs.setRootOperator(mergeRecursiveArg)
	c.anal.isFirst = false
	return []*Scope{rs}, nil
}

func (c *Compile) compileSinkScanNode(node *plan.Node, curNodeIdx int32) ([]*Scope, error) {
	receivers := make([]*process.WaitRegister, len(node.SourceStep))
	for i, step := range node.SourceStep {
		receivers[i] = c.getNodeReg(step, curNodeIdx)
		if receivers[i] == nil {
			return nil, moerr.NewInternalError(c.proc.Ctx, "no data sender for sinkScan node")
		}
	}
	rs := c.newEmptyMergeScope()
	rs.Proc = c.proc.NewNoContextChildProc(1)

	currentFirstFlag := c.anal.isFirst
	mergeArg := merge.NewArgument().WithSinkScan(true)
	if len(node.SourceStep) == 1 {
		step := node.SourceStep[0]
		if source := c.getMaterializedSource(step); source != nil {
			mergeArg.MaterializedSource = source
			mergeArg.MaterializedReaderID = c.materializedReaderIDs[[2]int32{step, curNodeIdx}]
		}
	}
	c.hasMergeOp = true
	mergeArg.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
	rs.setRootOperator(mergeArg)
	c.anal.isFirst = false

	rs.Proc.Reg.MergeReceivers = receivers
	return []*Scope{rs}, nil
}

func (c *Compile) compileSinkNode(node *plan.Node, ss []*Scope, step int32) ([]*Scope, error) {
	receivers := c.getStepRegs(step)
	if len(receivers) == 0 {
		return nil, moerr.NewInternalError(c.proc.Ctx, "no data receiver for sink node")
	}

	materializedSource := c.getMaterializedSource(step)
	var rs *Scope
	if materializedSource != nil {
		// The materialized source is process-local state. Always terminate the
		// producer at a local merge scope so it is never serialized as part of a
		// remote scope without its consumers. Group same-CN shuffle buckets first
		// so every remote producer scope is independently executable.
		ss = c.groupShuffleBucketsByCNIfNeeded(ss)
		rs = c.newMergeScope(ss)
	} else if c.IsSingleScope(ss) {
		rs = ss[0]
	} else {
		rs = c.newMergeScope(ss)
	}

	currentFirstFlag := c.anal.isFirst
	dispatchLocal := constructDispatchLocal(true, true, node.RecursiveSink, node.RecursiveCte, receivers)
	dispatchLocal.MaterializedSource = materializedSource
	dispatchLocal.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
	rs.setRootOperator(dispatchLocal)
	c.anal.isFirst = false

	ss = []*Scope{rs}
	return ss, nil
}

// DeleteMergeScope need to assure this:
// one block can be only deleted by one and the same
// CN, so we need to transfer the rows from the
// the same block to one and the same CN to perform
// the deletion operators.
func (c *Compile) newDeleteMergeScope(arg *deletion.Deletion, ss []*Scope, node *plan.Node) *Scope {
	for i := 0; i < len(ss); i++ {
		if ss[i].NodeInfo.Mcpu > 1 { // merge here to avoid bugs, delete this in the future
			ss[i] = c.newMergeScope([]*Scope{ss[i]})
		}
	}

	rs := make([]*Scope, len(ss))
	for i := 0; i < len(ss); i++ {
		rs[i] = newScope(Remote)
		rs[i].NodeInfo = scopeNodeWithMcpu(ss[i].NodeInfo, 1)
		rs[i].PreScopes = append(rs[i].PreScopes, ss[i])
		rs[i].Proc = c.proc.NewNoContextChildProc(len(ss))
		mergeOp := merge.NewArgument()
		mergeOp.SetAnalyzeControl(c.anal.curNodeIdx, false)
		rs[i].setRootOperator(mergeOp)
	}
	c.hasMergeOp = true

	for i := 0; i < len(ss); i++ {
		dispatchArg := constructDispatch(i, rs, ss[i], node, false)
		dispatchArg.SetAnalyzeControl(c.anal.curNodeIdx, false)
		ss[i].setRootOperator(dispatchArg)
		ss[i].IsEnd = true
	}

	for i := range rs {
		// use distributed delete
		arg.RemoteDelete = true
		// maybe just copy only once?
		arg.SegmentMap = colexec.MustGetServer(c.proc.GetService()).GetCnSegmentMap()
		arg.IBucket = uint32(i)
		arg.Nbucket = uint32(len(rs))
		rs[i].setRootOperator(dupOperator(arg, 0, len(rs)))
	}
	return c.newMergeScope(rs)
}

func (c *Compile) newEmptyMergeScope() *Scope {
	rs := newScope(Merge)
	rs.NodeInfo = scopeNodeWithMcpu(toEngineNode(c.currentCNWorker()), 1) //merge scope is single parallel by default
	return rs
}

func (c *Compile) newMergeScope(ss []*Scope) *Scope {
	rs := c.newEmptyMergeScope()
	ss = c.groupRemoteRunDependenciesByCNIfNeeded(ss, rs.NodeInfo)
	rs.PreScopes = ss

	rs.Proc = c.proc.NewNoContextChildProc(len(ss))
	if len(ss) > 0 {
		rs.Proc.Base.LoadTag = ss[0].Proc.Base.LoadTag
	}

	// waring: `Merge` operator` is not used as an input/output analyze,
	// and `Merge` operator cannot play the role of IsFirst/IsLast
	mergeOp := merge.NewArgument()
	c.hasMergeOp = true
	mergeOp.SetAnalyzeControl(c.anal.curNodeIdx, false)
	rs.setRootOperator(mergeOp)

	j := 0
	for i := range ss {
		nilBatchCnt := 1
		if sameExecutionNode(rs.NodeInfo, ss[i].NodeInfo) {
			nilBatchCnt = ss[i].NodeInfo.Mcpu
		}
		rs.Proc.Reg.MergeReceivers[j].ResetForReuse(mergeReceiverChannelBufferSize(ss[i]), nilBatchCnt)
		// waring: `connector` operator is not used as an input/output analyze,
		// and `connector` operator cannot play the role of IsFirst/IsLast
		connArg := connector.NewArgument().WithReg(rs.Proc.Reg.MergeReceivers[j])
		connArg.SetAnalyzeControl(c.anal.curNodeIdx, false)
		ss[i].setRootOperator(connArg)
		j++
	}
	return rs
}

// newScopeListOnSingleWorkerStage builds a single-worker stage. If query
// placement already collapsed to one worker, inherit that worker; otherwise
// keep the legacy current-CN coordinator for multi-CN stages.
//
// waing: newScopeListOnSingleWorkerStage result is only used to build Scope and add one merge operator.
// If other operators are added, please let @qingxinhome know
func (c *Compile) newScopeListOnSingleWorkerStage(childrenCount int, mcpu int) []*Scope {
	node := c.singleWorkerStageNode()
	ss := c.newScopeListWithNode(mcpu, childrenCount, node)
	return ss
}

func (c *Compile) singleWorkerStageNode() engine.Node {
	queryWorkers := c.scheduledQueryWorkers()
	decision := schedule.DecideSingleWorkerStagePlacement(schedule.StageRequest{
		QueryWorkers: queryWorkers,
		CurrentCN:    c.currentCNWorker(),
	})
	c.schedulingTrace.RecordSingleWorkerStage(c.schedulingAttempt, queryWorkers, decision)
	return c.materializeScheduledWorker(decision.Worker)
}

func (c *Compile) queryWorkerStageNodes() engine.Nodes {
	queryWorkers := c.scheduledQueryWorkers()
	decision := schedule.DecideQueryWorkerStagePlacement(schedule.StageRequest{
		QueryWorkers: queryWorkers,
		CurrentCN:    c.currentCNWorker(),
	})
	c.schedulingTrace.RecordStage(
		c.schedulingAttempt,
		schedule.StageKindQueryWorkerSet,
		queryWorkers,
		decision,
	)
	return c.materializeScheduledWorkers(decision.Workers)
}

// shuffleJoinStageNodes keeps the SINK_SCAN producer on its owning CN while
// allowing the shuffle receivers to use every query worker. SINK_SCAN consumes
// an in-process PipelineEdge created for another query step, so its scope cannot
// be serialized to a remote CN. Including its owning CN in the receiver set lets
// attachShuffleDispatchSource keep that scope local; the following dispatch can
// still send buckets to hashbuild receivers on the other query workers.
func (c *Compile) shuffleJoinStageNodes(probeScopes, buildScopes []*Scope) (engine.Nodes, bool) {
	stageNode, hasSinkScan := sinkScanDependencyNode(probeScopes, buildScopes)
	if !hasSinkScan {
		return c.queryWorkerStageNodes(), false
	}
	if stageNode.Addr == "" {
		stageNode = c.materializeScheduledWorker(c.currentCNWorker())
	}
	stageNodes := c.queryWorkerStageNodes()
	for _, node := range stageNodes {
		if sameExecutionNode(node, stageNode) {
			return stageNodes, true
		}
	}
	return append(stageNodes, scopeNodeWithMcpu(stageNode, 1)), true
}

func sinkScanDependencyNode(scopeLists ...[]*Scope) (engine.Node, bool) {
	visitedScopes := make(map[*Scope]bool)
	visitedOps := make(map[vm.Operator]bool)
	for _, scopes := range scopeLists {
		for _, s := range scopes {
			if node, ok := scopeTreeSinkScanNode(s, visitedScopes, visitedOps); ok {
				return node, true
			}
		}
	}
	return engine.Node{}, false
}

func scopeTreeSinkScanNode(s *Scope, visitedScopes map[*Scope]bool, visitedOps map[vm.Operator]bool) (engine.Node, bool) {
	if s == nil || visitedScopes[s] {
		return engine.Node{}, false
	}
	visitedScopes[s] = true
	if operatorTreeContainsSinkScan(s.RootOp, visitedOps) {
		return s.NodeInfo, true
	}
	for _, pre := range s.PreScopes {
		if node, ok := scopeTreeSinkScanNode(pre, visitedScopes, visitedOps); ok {
			return node, true
		}
	}
	return engine.Node{}, false
}

// operatorTreeContainsSinkScan reports whether the operator tree contains a
// CN-pinned local source: a SINK_SCAN merge (consumes an in-process
// PipelineEdge) or an ESQL/SQL foreign external scan (its connection cache
// lives only on the interactive session's CN). Either one must keep its
// owning CN inside the shuffle receiver stage set, or no receiver tree would
// ever start the scope and every shuffle receiver would wait forever.
func operatorTreeContainsSinkScan(op vm.Operator, visited map[vm.Operator]bool) bool {
	if op == nil || visited[op] {
		return false
	}
	visited[op] = true
	if mergeOp, ok := op.(*merge.Merge); ok && mergeOp.SinkScan {
		return true
	}
	if ext, ok := op.(*external.External); ok && ext.Es != nil &&
		(ext.Es.ForeignScan != nil || ext.Es.KafkaScan != nil) {
		return true
	}
	base := op.GetOperatorBase()
	for i := 0; i < base.NumChildren(); i++ {
		if operatorTreeContainsSinkScan(base.GetChildren(i), visited) {
			return true
		}
	}
	return false
}

func scopeNodeWithMcpu(node engine.Node, mcpu int) engine.Node {
	return engine.Node{
		Id:        node.Id,
		Addr:      node.Addr,
		Mcpu:      normalizeMcpu(mcpu),
		WorkState: node.WorkState,
	}
}

// all scopes in ss are on the same CN
func (c *Compile) newMergeScopeByCN(ss []*Scope, nodeinfo engine.Node) *Scope {
	rs := newScope(Remote)
	rs.NodeInfo = scopeNodeWithMcpu(nodeinfo, 1) // merge scope is single parallel by default
	rs.PreScopes = ss
	rs.Proc = c.proc.NewNoContextChildProc(1)
	nilBatchCnt := 0
	for i := range ss {
		nilBatchCnt += ss[i].NodeInfo.Mcpu
	}
	rs.Proc.Reg.MergeReceivers[0].ResetForReuse(len(ss), nilBatchCnt)

	// waring: `Merge` operator` is not used as an input/output analyze,
	// and `Merge` operator cannot play the role of IsFirst/IsLast
	mergeOp := merge.NewArgument()
	c.hasMergeOp = true
	mergeOp.SetAnalyzeControl(c.anal.curNodeIdx, false)
	rs.setRootOperator(mergeOp)
	for i := range ss {
		// waring: `connector` operator is not used as an input/output analyze,
		// and `connector` operator cannot play the role of IsFirst/IsLast
		connArg := connector.NewArgument().WithReg(rs.Proc.Reg.MergeReceivers[0])
		connArg.SetAnalyzeControl(c.anal.curNodeIdx, false)
		ss[i].setRootOperator(connArg)
		ss[i].IsEnd = true
	}
	return rs
}

// waing: newScopeListWithNode only used to build Scope with cpuNum and add one merge operator.
// If other operators are added, please let @qingxinhome know
func (c *Compile) newScopeListWithNode(mcpu, childrenCount int, node engine.Node) []*Scope {
	ss := make([]*Scope, mcpu)
	for i := range ss {
		ss[i] = newScope(Remote)
		ss[i].Magic = Remote
		ss[i].NodeInfo = scopeNodeWithMcpu(node, 1) // ss is already the mcpu length so we don't need to parallel it
		ss[i].Proc = c.proc.NewNoContextChildProc(childrenCount)

		// The merge operator does not act as First/Last, It needs to handle its analyze status
		mergeOp := merge.NewArgument()
		mergeOp.SetAnalyzeControl(c.anal.curNodeIdx, false)
		ss[i].setRootOperator(mergeOp)
	}
	c.hasMergeOp = true
	//c.anal.isFirst = false
	return ss
}

func (c *Compile) newScopeListForMinusAndIntersect(rs, left, right []*Scope, node *plan.Node) []*Scope {
	// construct left
	left = c.mergeShuffleScopesIfNeeded(left, false)
	leftMerge := c.newMergeScope(left)
	leftDispatch := constructDispatch(0, rs, leftMerge, node, false)
	leftDispatch.SetAnalyzeControl(c.anal.curNodeIdx, false)
	leftMerge.setRootOperator(leftDispatch)
	leftMerge.IsEnd = true

	// construct right
	right = c.mergeShuffleScopesIfNeeded(right, false)
	rightMerge := c.newMergeScope(right)
	rightDispatch := constructDispatch(1, rs, rightMerge, node, false)
	leftDispatch.SetAnalyzeControl(c.anal.curNodeIdx, false)
	rightMerge.setRootOperator(rightDispatch)
	rightMerge.IsEnd = true

	rs[0].PreScopes = append(rs[0].PreScopes, leftMerge, rightMerge)
	return rs
}

func (c *Compile) mergeShuffleScopesIfNeeded(ss []*Scope, force bool) []*Scope {
	stageNodes := c.shuffleStageNodes(ss)
	if len(stageNodes) == 1 && !force {
		return ss
	}
	if len(ss) <= len(stageNodes) {
		return ss
	}
	for i := range ss {
		if ss[i].NodeInfo.Mcpu != 1 {
			return ss
		}
	}
	rs := c.mergeScopesByStageNodes(ss, stageNodes)
	for i := range rs {
		for _, rr := range rs[i].Proc.Reg.MergeReceivers {
			rr.ResetForReuse(shuffleChannelBufferSize, rr.NilBatchCnt)
		}
	}
	return rs
}

// groupRemoteRunDependenciesByCNIfNeeded preserves the ownership boundary of
// in-process dispatch and connector receivers when a local merge makes its
// inputs separate RemoteRun units. A scope that targets a receiver owned by a
// sibling scope cannot execute remotely on its own; wrapping all inputs from
// the same CN in one merge scope keeps those dependencies in one serialized
// tree. Only a non-local invalid input triggers regrouping; once triggered, the
// whole input stage is grouped consistently by CN. Independent input stages
// retain the direct fast path.
func (c *Compile) groupRemoteRunDependenciesByCNIfNeeded(
	ss []*Scope,
	mergeNode engine.Node,
) []*Scope {
	stageNodes := shuffleBucketStageNodes(ss)
	if len(ss) <= len(stageNodes) {
		return ss
	}

	for _, scope := range ss {
		if !sameExecutionNode(scope.NodeInfo, mergeNode) &&
			findPipelineExternalLocalReceiver(scope) != nil {
			return c.mergeScopesByStageNodes(ss, stageNodes)
		}
	}
	return ss
}

// shuffleBucketsNeedPerCNGrouping reports whether a dispatch in one top-level
// bucket tree targets a local receiver owned only by a sibling bucket tree on
// the same CN. Such a tree is not independently executable by RemoteRun and
// must travel together with the sibling that owns the receiver.
//
// Do not use RemoteRegs as a proxy for this dependency. A shuffle source on a
// remote CN can have only LocalRegs and still depend on sibling dop buckets.
func shuffleBucketsNeedPerCNGrouping(ss []*Scope) bool {
	receiverOwners := make(map[*process.WaitRegister]map[int]struct{})
	for ownerIdx, root := range ss {
		walkScopeTree(root, func(scope *Scope) bool {
			if scope.Proc == nil {
				return false
			}
			for _, reg := range scope.Proc.Reg.MergeReceivers {
				if reg == nil {
					continue
				}
				owners := receiverOwners[reg]
				if owners == nil {
					owners = make(map[int]struct{})
					receiverOwners[reg] = owners
				}
				owners[ownerIdx] = struct{}{}
			}
			return false
		})
	}

	for sourceIdx, root := range ss {
		if walkScopeTree(root, func(scope *Scope) bool {
			d, ok := scope.RootOp.(*dispatch.Dispatch)
			if !ok {
				return false
			}
			for _, reg := range d.LocalRegs {
				if reg == nil {
					continue
				}
				owners := receiverOwners[reg]
				if _, ownedBySourceTree := owners[sourceIdx]; ownedBySourceTree {
					continue
				}
				for ownerIdx := range owners {
					if sameExecutionNode(ss[sourceIdx].NodeInfo, ss[ownerIdx].NodeInfo) {
						return true
					}
				}
			}
			return false
		}) {
			return true
		}
	}
	return false
}

func walkScopeTree(root *Scope, visit func(*Scope) bool) bool {
	toVisit := []*Scope{root}
	visited := make(map[*Scope]struct{})
	for len(toVisit) > 0 {
		last := len(toVisit) - 1
		scope := toVisit[last]
		toVisit = toVisit[:last]
		if scope == nil {
			continue
		}
		if _, ok := visited[scope]; ok {
			continue
		}
		visited[scope] = struct{}{}
		if visit(scope) {
			return true
		}
		toVisit = append(toVisit, scope.PreScopes...)
	}
	return false
}

// groupShuffleBucketsByCNIfNeeded groups the same-CN shuffle buckets (together with the
// shuffle dispatch nested under them) into one per-CN send unit, so a cross-CN shuffle
// dispatch always travels in the same pipeline tree as all of its dop local buckets.
//
// Background (issue #24919): newShuffleJoinScopeList leaves a CN's dop join buckets in
// separate RemoteRun trees while the shuffle dispatch only attaches to the first bucket.
// When a consumer sends each bucket individually, RemoteRun ->
// checkPipelineStandaloneExecutableAtRemote sees the dispatch.LocalRegs pointing to the
// sibling out-of-tree buckets, so the tree is not independently executable and must fail
// before remote start. Historically RemoteRun silently moved that tree to the coordinator;
// the dispatch then ran on the wrong CN, was mispaired with the cross-CN receiver's FromAddr,
// and the remote receiver/merge could wait forever. Regrouping by CN keeps all of a CN's
// buckets in one tree, so the whole group executes at the intended remote CN.
//
// It is a no-op unless a dispatch has an out-of-tree local receiver dependency
// that grouping by CN can close, so unrelated insert scopes are unaffected.
//
// Operator-chain note: callers attach their own root operator to each bucket first (e.g.
// the insert / multiUpdate operator). mergeScopesByCN (via newMergeScopeByCN ->
// doSetRootOperator) appends a connector *on top of* that existing root using AppendChild
// semantics, so the caller's operator is preserved as the connector's child, not replaced.
func (c *Compile) groupShuffleBucketsByCNIfNeeded(ss []*Scope) []*Scope {
	stageNodes := shuffleBucketStageNodes(ss)
	if len(stageNodes) <= 1 || len(ss) <= len(stageNodes) {
		return ss
	}
	if !shuffleBucketsNeedPerCNGrouping(ss) {
		return ss
	}
	return c.mergeScopesByStageNodes(ss, stageNodes)
}

// shuffleBucketStageNodes derives the grouping boundary from the receiver
// scopes themselves. The receiver set may include a local SINK_SCAN owner that
// is intentionally absent from the scheduled query-worker set.
func shuffleBucketStageNodes(ss []*Scope) engine.Nodes {
	stageNodes := make(engine.Nodes, 0, len(ss))
	for _, scope := range ss {
		found := false
		for _, node := range stageNodes {
			if sameExecutionNode(node, scope.NodeInfo) {
				found = true
				break
			}
		}
		if !found {
			stageNodes = append(stageNodes, scope.NodeInfo)
		}
	}
	return stageNodes
}

func (c *Compile) mergeScopesByStageNodes(ss []*Scope, stageNodes engine.Nodes) []*Scope {
	rs := make([]*Scope, 0, len(stageNodes))
	for i := range stageNodes {
		cn := stageNodes[i]
		currentSS := make([]*Scope, 0, cn.Mcpu)
		for j := range ss {
			if sameExecutionNode(ss[j].NodeInfo, cn) {
				currentSS = append(currentSS, ss[j])
			}
		}
		if len(currentSS) > 0 {
			mergeScope := c.newMergeScopeByCN(currentSS, cn)
			rs = append(rs, mergeScope)
		}
	}

	return rs
}

func (c *Compile) newShuffleJoinScopeList(
	probeScopes, buildScopes []*Scope,
	node *plan.Node,
) []*Scope {
	return c.newShuffleJoinScopeListAt(
		probeScopes, buildScopes, node, c.shuffleStageNodes(probeScopes), false)
}

func (c *Compile) newShuffleJoinScopeListAt(
	probeScopes, buildScopes []*Scope,
	node *plan.Node,
	cnlist engine.Nodes,
	attachRemoteSources bool,
) []*Scope {
	return c.newShuffleJoinScopeListAtSides(
		probeScopes, buildScopes, node, cnlist, attachRemoteSources, true)
}

func (c *Compile) newShuffleJoinScopeListAtSides(
	probeScopes, buildScopes []*Scope,
	node *plan.Node,
	cnlist engine.Nodes,
	attachRemoteSources bool,
	probeIsLogicalLeft bool,
) []*Scope {
	if len(cnlist) == 0 {
		cnlist = c.shuffleStageNodes(probeScopes)
	}
	if len(cnlist) <= 1 {
		node.Stats.HashmapStats.ShuffleTypeForMultiCN = plan.ShuffleTypeForMultiCN_Simple
	}

	dop := int(node.Stats.Dop)
	bucketNum := len(cnlist) * dop
	reuse := probeIsLogicalLeft &&
		node.Stats.HashmapStats.ShuffleMethod == plan.ShuffleMethod_Reuse &&
		canReuseDistributedShuffleJoin(probeScopes, cnlist, dop)
	// Multi-CN DEDUP normalizes the probe scopes below by merging them, so the
	// per-bucket layout cannot be reused by the distributed join.
	if node.JoinType == plan.Node_DEDUP && len(cnlist) > 1 {
		reuse = false
	}
	if !reuse {
		probeScopes = c.mergeShuffleScopesIfNeeded(probeScopes, true)
	}
	buildScopes = c.mergeShuffleScopesIfNeeded(buildScopes, true)
	if node.JoinType == plan.Node_DEDUP && len(cnlist) > 1 {
		//merge build side to avoid bugs
		if !c.IsSingleScope(probeScopes) {
			probeScopes = []*Scope{c.newMergeScope(probeScopes)}
		}
		if !c.IsSingleScope(buildScopes) {
			buildScopes = []*Scope{c.newMergeScope(buildScopes)}
		}
	}

	shuffleProbes := make([]*Scope, 0, bucketNum)
	shuffleBuilds := make([]*Scope, 0, bucketNum)

	lenLeft := len(probeScopes)
	lenRight := len(buildScopes)

	if !reuse {
		for _, cn := range cnlist {
			probes := make([]*Scope, dop)
			builds := make([]*Scope, dop)
			for i := range probes {
				probes[i] = newScope(Remote)
				probes[i].NodeInfo = scopeNodeWithMcpu(cn, 1)
				probes[i].Proc = c.proc.NewNoContextChildProc(lenLeft)

				builds[i] = newScope(Remote)
				builds[i].NodeInfo = probes[i].NodeInfo
				builds[i].Proc = c.proc.NewNoContextChildProc(lenRight)

				probes[i].PreScopes = []*Scope{builds[i]}
				for _, rr := range probes[i].Proc.Reg.MergeReceivers {
					rr.ResetForReuse(shuffleChannelBufferSize, rr.NilBatchCnt)
				}
				for _, rr := range builds[i].Proc.Reg.MergeReceivers {
					rr.ResetForReuse(shuffleChannelBufferSize, rr.NilBatchCnt)
				}
			}
			shuffleProbes = append(shuffleProbes, probes...)
			shuffleBuilds = append(shuffleBuilds, builds...)
		}
	} else {
		shuffleProbes = probeScopes
		for i := range shuffleProbes {
			buildscope := newScope(Remote)
			buildscope.NodeInfo = shuffleProbes[i].NodeInfo
			buildscope.Proc = c.proc.NewNoContextChildProc(lenRight)
			for _, rr := range buildscope.Proc.Reg.MergeReceivers {
				rr.ResetForReuse(shuffleChannelBufferSize, rr.NilBatchCnt)
			}
			shuffleBuilds = append(shuffleBuilds, buildscope)
			prescopes := shuffleProbes[i].PreScopes
			shuffleProbes[i].PreScopes = []*Scope{buildscope}
			shuffleProbes[i].PreScopes = append(shuffleProbes[i].PreScopes, prescopes...) //make sure build scope is in prescope[0]
		}
	}

	currentFirstFlag := c.anal.isFirst
	if !reuse {
		for i := range probeScopes {
			shuffleProbeOp := constructShuffleOperatorForJoin(
				int32(bucketNum), node, probeIsLogicalLeft)
			if !probeIsLogicalLeft && len(node.RuntimeFilterProbeList) > 0 {
				shuffleProbeOp.RuntimeFilterSpec =
					plan2.DeepCopyRuntimeFilterSpec(node.RuntimeFilterProbeList[0])
			}
			shuffleProbeOp.DrainAllBuckets = true
			//shuffleProbeOp.SetIdx(c.anal.curNodeIdx)
			shuffleProbeOp.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
			probeScopes[i].setRootOperator(shuffleProbeOp)

			if len(cnlist) > 1 && probeScopes[i].NodeInfo.Mcpu > 1 { // merge here to avoid bugs, delete this in the future
				probeScopes[i] = c.newMergeScopeByCN([]*Scope{probeScopes[i]}, probeScopes[i].NodeInfo)
			}

			dispatchArg := constructDispatch(i, shuffleProbes, probeScopes[i], node, true)
			dispatchArg.SetAnalyzeControl(c.anal.curNodeIdx, false)
			probeScopes[i].setRootOperator(dispatchArg)
			probeScopes[i].IsEnd = true

			attachShuffleDispatchSource(shuffleProbes, probeScopes[i], attachRemoteSources)
		}
	}

	c.anal.isFirst = currentFirstFlag
	for i := range buildScopes {
		shuffleBuildOp := constructShuffleOperatorForJoin(
			int32(bucketNum), node, !probeIsLogicalLeft)
		if !probeIsLogicalLeft {
			shuffleBuildOp.RuntimeFilterSpec = nil
		}
		shuffleBuildOp.DrainAllBuckets = true
		//shuffleBuildOp.SetIdx(c.anal.curNodeIdx)
		shuffleBuildOp.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
		buildScopes[i].setRootOperator(shuffleBuildOp)

		if len(cnlist) > 1 && buildScopes[i].NodeInfo.Mcpu > 1 { // merge here to avoid bugs, delete this in the future
			buildScopes[i] = c.newMergeScopeByCN([]*Scope{buildScopes[i]}, buildScopes[i].NodeInfo)
		}

		dispatchArg := constructDispatch(i, shuffleBuilds, buildScopes[i], node, false)
		dispatchArg.SetAnalyzeControl(c.anal.curNodeIdx, false)
		buildScopes[i].setRootOperator(dispatchArg)
		buildScopes[i].IsEnd = true

		attachShuffleDispatchSource(shuffleBuilds, buildScopes[i], attachRemoteSources)
	}
	c.anal.isFirst = false
	c.hasMergeOp = true
	if !reuse {
		for i := range shuffleProbes {
			mergeOp := merge.NewArgument()
			mergeOp.SetAnalyzeControl(c.anal.curNodeIdx, false)
			shuffleProbes[i].setRootOperator(mergeOp)
		}
	}

	return shuffleProbes
}

// attachShuffleDispatchSource prefers a receiver bucket on the source CN. A
// coordinator-local shuffle (used for SINK_SCAN dependencies) has no receiver
// bucket on remote scan CNs, so attach those remote dispatch sources to the
// first receiver tree to ensure RemoteRun still starts them.
func attachShuffleDispatchSource(receivers []*Scope, source *Scope, allowFallback bool) {
	if len(receivers) == 0 || source == nil {
		return
	}
	for _, receiver := range receivers {
		if sameExecutionNode(receiver.NodeInfo, source.NodeInfo) {
			receiver.PreScopes = append(receiver.PreScopes, source)
			return
		}
	}
	if allowFallback {
		receivers[0].PreScopes = append(receivers[0].PreScopes, source)
	}
}

func collectTombstones(
	c *Compile,
	node *plan.Node,
	rel engine.Relation,
	policy engine.TombstoneCollectPolicy,
) (engine.Tombstoner, error) {
	var err error
	//var relData engine.RelData
	var tombstone engine.Tombstoner

	//-----------------------------------------------------------------------------------------------------
	ctx := c.proc.GetTopContext()
	if node.ScanSnapshot != nil && node.ScanSnapshot.TS != nil {
		zeroTS := timestamp.Timestamp{LogicalTime: 0, PhysicalTime: 0}
		snapTS := c.proc.GetTxnOperator().Txn().SnapshotTS
		if !node.ScanSnapshot.TS.Equal(zeroTS) && node.ScanSnapshot.TS.Less(snapTS) {
			if c.proc.GetCloneTxnOperator() == nil {
				txnOp := c.proc.GetTxnOperator().CloneSnapshotOp(*node.ScanSnapshot.TS)
				c.proc.SetCloneTxnOperator(txnOp)
			}

			if node.ScanSnapshot.Tenant != nil {
				ctx = context.WithValue(ctx, defines.TenantIDKey{}, node.ScanSnapshot.Tenant.TenantID)
			}
		}
	}
	//-----------------------------------------------------------------------------------------------------

	if util.TableIsClusterTable(node.TableDef.GetTableType()) {
		ctx = defines.AttachAccountId(ctx, catalog.System_Account)
	}
	if node.ObjRef.PubInfo != nil {
		ctx = defines.AttachAccountId(ctx, uint32(node.ObjRef.PubInfo.GetTenantId()))
	}
	if util.TableIsLoggingTable(node.ObjRef.SchemaName, node.ObjRef.ObjName) {
		ctx = defines.AttachAccountId(ctx, catalog.System_Account)
	}
	logCatalogSnapshotScan("compile.collect-tombstones", node, ctx, c.proc.GetCloneTxnOperator())

	tombstone, err = rel.CollectTombstones(ctx, c.TxnOffset, policy)
	if err != nil {
		return nil, err
	}

	return tombstone, nil
}

func logCatalogSnapshotScan(tag string, node *plan.Node, ctx context.Context, txnOp client.TxnOperator) {
	if node == nil || node.ObjRef == nil {
		return
	}
	if !strings.EqualFold(node.ObjRef.SchemaName, catalog.MO_CATALOG) ||
		!strings.EqualFold(node.ObjRef.ObjName, catalog.MO_DATABASE) {
		return
	}

	fields := []zap.Field{
		zap.String("schema", node.ObjRef.SchemaName),
		zap.String("table", node.ObjRef.ObjName),
	}
	if txnOp != nil {
		fields = append(fields,
			zap.String("txn-snapshot-ts", types.TimestampToTS(txnOp.Txn().SnapshotTS).ToString()),
			zap.String("txn", txnOp.Txn().DebugString()),
		)
	}
	if node.ScanSnapshot != nil && node.ScanSnapshot.TS != nil {
		fields = append(fields, zap.String("scan-snapshot-ts", types.TimestampToTS(*node.ScanSnapshot.TS).ToString()))
		if node.ScanSnapshot.Tenant != nil {
			fields = append(fields, zap.Uint32("scan-tenant-id", node.ScanSnapshot.Tenant.TenantID))
		}
	}
	if accountID, err := defines.GetAccountId(ctx); err == nil {
		fields = append(fields, zap.Uint32("ctx-account-id", accountID))
	} else {
		fields = append(fields, zap.String("ctx-account-id", "missing"))
	}
	logutil.Info(tag, fields...)
}

func (c *Compile) expandRanges(
	node *plan.Node, rel engine.Relation, db engine.Database, ctx context.Context,
	blockFilterList []*plan.Expr, policy engine.DataCollectPolicy, rsp *engine.RangesShuffleParam) (engine.RelData, error) {

	preAllocBlocks := 2
	if policy&engine.Policy_CollectCommittedPersistedData != 0 {
		if !c.IsTpQuery() {
			if len(blockFilterList) > 0 {
				preAllocBlocks = 64
			} else {
				preAllocBlocks = int(node.Stats.BlockNum)
				if rsp != nil {
					preAllocBlocks = preAllocBlocks / int(rsp.CNCNT)
				}
			}
		}
	}

	counterSet := new(perfcounter.CounterSet)
	newCtx := perfcounter.AttachS3RequestKey(ctx, counterSet)
	rangesParam := engine.RangesParam{
		BlockFilters:       blockFilterList,
		PreAllocBlocks:     preAllocBlocks,
		TxnOffset:          c.TxnOffset,
		Policy:             policy,
		Rsp:                rsp,
		DontSupportRelData: false,
	}
	relData, err := rel.Ranges(newCtx, rangesParam)
	if err != nil {
		return nil, err
	}

	stats := statistic.StatsInfoFromContext(ctx)
	stats.AddScopePrepareS3Request(statistic.S3Request{
		List:      counterSet.FileService.S3.List.Load(),
		Head:      counterSet.FileService.S3.Head.Load(),
		Put:       counterSet.FileService.S3.Put.Load(),
		Get:       counterSet.FileService.S3.Get.Load(),
		Delete:    counterSet.FileService.S3.Delete.Load(),
		DeleteMul: counterSet.FileService.S3.DeleteMulti.Load(),
	})

	return relData, nil
}

func (c *Compile) handleDbRelContext(node *plan.Node, onRemoteCN bool) (engine.Relation, engine.Database, context.Context, error) {
	var err error
	var db engine.Database
	var rel engine.Relation
	var txnOp client.TxnOperator

	if onRemoteCN {
		// Workspace may have been created earlier in remote run scenario (e.g., in remoterunServer.go).
		// Only create if it doesn't exist to avoid duplicate creation.
		if c.proc.GetTxnOperator().GetWorkspace() == nil {
			ws := disttae.NewTxnWorkSpace(c.e.(*disttae.Engine), c.proc)
			c.proc.GetTxnOperator().AddWorkspace(ws)
			ws.BindTxnOp(c.proc.GetTxnOperator())
		}
	}

	//------------------------------------------------------------------------------------------------------------------
	ctx := c.proc.GetTopContext()
	txnOp = c.proc.GetTxnOperator()
	if node.ScanSnapshot != nil && node.ScanSnapshot.TS != nil {
		if !node.ScanSnapshot.TS.Equal(timestamp.Timestamp{LogicalTime: 0, PhysicalTime: 0}) &&
			node.ScanSnapshot.TS.Less(c.proc.GetTxnOperator().Txn().SnapshotTS) {

			if c.proc.GetCloneTxnOperator() != nil {
				txnOp = c.proc.GetCloneTxnOperator()
			} else {
				txnOp = c.proc.GetTxnOperator().CloneSnapshotOp(*node.ScanSnapshot.TS)
				c.proc.SetCloneTxnOperator(txnOp)
			}

			if node.ScanSnapshot.Tenant != nil {
				ctx = context.WithValue(ctx, defines.TenantIDKey{}, node.ScanSnapshot.Tenant.TenantID)
			}
		}
	}
	//-------------------------------------------------------------------------------------------------------------
	if util.TableIsClusterTable(node.TableDef.GetTableType()) {
		ctx = defines.AttachAccountId(ctx, catalog.System_Account)
	}
	if node.ObjRef.PubInfo != nil {
		ctx = defines.AttachAccountId(ctx, uint32(node.ObjRef.PubInfo.GetTenantId()))
	}
	if util.TableIsLoggingTable(node.ObjRef.SchemaName, node.ObjRef.ObjName) {
		ctx = defines.AttachAccountId(ctx, catalog.System_Account)
	}

	db, err = c.e.Database(ctx, node.ObjRef.SchemaName, txnOp)
	if err != nil {
		return nil, nil, nil, err
	}
	rel, err = db.Relation(ctx, node.TableDef.Name, c.proc)
	if err != nil {
		return nil, nil, nil, err
	}

	return rel, db, ctx, nil
}

func checkAggOptimize(node *plan.Node) ([]any, []types.T, map[int]int) {
	partialResults := make([]any, len(node.AggList))
	partialResultTypes := make([]types.T, len(node.AggList))
	columnMap := make(map[int]int)
	for i := range node.AggList {
		agg := node.AggList[i].Expr.(*plan.Expr_F)
		name := agg.F.Func.ObjName
		args := agg.F.Args[0]
		switch name {
		case "starcount":
			partialResults[i] = int64(0)
			partialResultTypes[i] = types.T_int64
		case "count":
			if (uint64(agg.F.Func.Obj) & function.Distinct) != 0 {
				return nil, nil, nil
			} else {
				partialResults[i] = int64(0)
				partialResultTypes[i] = types.T_int64
			}
			col, ok := args.Expr.(*plan.Expr_Col)
			if !ok {
				if lit, ok := args.Expr.(*plan.Expr_Lit); ok {
					// COUNT(NULL) must count zero values, not all input rows.
					if lit.Lit == nil || lit.Lit.Isnull {
						return nil, nil, nil
					}
					// COUNT(lit) e.g. count(1) from count(*): set ObjName+Obj so runtime uses countStarExec
					agg.F.Func.ObjName = "starcount"
					agg.F.Func.Obj = function.EncodeOverloadID(int32(function.STARCOUNT), 0)
					return partialResults, partialResultTypes, columnMap
				}
				return nil, nil, nil
			} else {
				if node.TableDef == nil {
					return nil, nil, nil
				}
				colPos := int(col.Col.ColPos)
				if colPos < 0 || colPos >= len(node.TableDef.Cols) {
					return nil, nil, nil
				}
				// Check if column is NOT NULL
				if node.TableDef.Cols[colPos].Typ.NotNullable {
					// Rewrite COUNT(not_null_col) to STARCOUNT so runtime uses countStarExec
					agg.F.Func.ObjName = "starcount"
					agg.F.Func.Obj = function.EncodeOverloadID(int32(function.STARCOUNT), 0)
				} else {
					columnMap[colPos] = int(node.TableDef.Cols[colPos].Seqnum)
				}
			}
		case "min", "max":
			partialResults[i] = nil
			col, ok := args.Expr.(*plan.Expr_Col)
			if !ok {
				return nil, nil, nil
			}
			if node.TableDef == nil {
				return nil, nil, nil
			}
			columnMap[int(col.Col.ColPos)] = int(node.TableDef.Cols[int(col.Col.ColPos)].Seqnum)
		default:
			return nil, nil, nil
		}
	}
	return partialResults, partialResultTypes, columnMap
}

func (c *Compile) evalAggOptimize(node *plan.Node, blk *objectio.BlockInfo, partialResults []any, partialResultTypes []types.T, columnMap map[int]int) error {
	if len(node.AggList) == 1 && node.AggList[0].Expr.(*plan.Expr_F).F.Func.ObjName == "starcount" {
		partialResults[0] = partialResults[0].(int64) + int64(blk.MetaLocation().Rows())
		return nil
	}
	location := blk.MetaLocation()
	fs, err := fileservice.Get[fileservice.FileService](c.proc.Base.FileService, defines.SharedFileServiceName)
	if err != nil {
		return err
	}
	objMeta, err := objectio.FastLoadObjectMeta(c.proc.Ctx, &location, false, fs)
	if err != nil {
		return err
	}
	blkMeta := objMeta.MustDataMeta().GetBlockMeta(uint32(location.ID()))
	for i := range node.AggList {
		agg := node.AggList[i].Expr.(*plan.Expr_F)
		name := agg.F.Func.ObjName
		switch name {
		case "starcount":
			partialResults[i] = partialResults[i].(int64) + int64(blkMeta.GetRows())
		case "count":
			partialResults[i] = partialResults[i].(int64) + int64(blkMeta.GetRows())
			col := agg.F.Args[0].Expr.(*plan.Expr_Col)
			nullCnt := blkMeta.ColumnMeta(uint16(columnMap[int(col.Col.ColPos)])).NullCnt()
			partialResults[i] = partialResults[i].(int64) - int64(nullCnt)
		case "min":
			col := agg.F.Args[0].Expr.(*plan.Expr_Col)
			zm := blkMeta.ColumnMeta(uint16(columnMap[int(col.Col.ColPos)])).ZoneMap()
			if zm.GetType().FixedLength() < 0 {
				return &moerr.Error{}
			} else {
				if partialResults[i] == nil {
					partialResults[i] = zm.GetMin()
					partialResultTypes[i] = zm.GetType()
				} else {
					switch zm.GetType() {
					case types.T_bool:
						partialResults[i] = partialResults[i].(bool) && types.DecodeFixed[bool](zm.GetMinBuf())
					case types.T_bit:
						min := types.DecodeFixed[uint64](zm.GetMinBuf())
						if min < partialResults[i].(uint64) {
							partialResults[i] = min
						}
					case types.T_int8:
						min := types.DecodeFixed[int8](zm.GetMinBuf())
						if min < partialResults[i].(int8) {
							partialResults[i] = min
						}
					case types.T_int16:
						min := types.DecodeFixed[int16](zm.GetMinBuf())
						if min < partialResults[i].(int16) {
							partialResults[i] = min
						}
					case types.T_int32:
						min := types.DecodeFixed[int32](zm.GetMinBuf())
						if min < partialResults[i].(int32) {
							partialResults[i] = min
						}
					case types.T_int64:
						min := types.DecodeFixed[int64](zm.GetMinBuf())
						if min < partialResults[i].(int64) {
							partialResults[i] = min
						}
					case types.T_uint8:
						min := types.DecodeFixed[uint8](zm.GetMinBuf())
						if min < partialResults[i].(uint8) {
							partialResults[i] = min
						}
					case types.T_uint16:
						min := types.DecodeFixed[uint16](zm.GetMinBuf())
						if min < partialResults[i].(uint16) {
							partialResults[i] = min
						}
					case types.T_uint32:
						min := types.DecodeFixed[uint32](zm.GetMinBuf())
						if min < partialResults[i].(uint32) {
							partialResults[i] = min
						}
					case types.T_uint64:
						min := types.DecodeFixed[uint64](zm.GetMinBuf())
						if min < partialResults[i].(uint64) {
							partialResults[i] = min
						}
					case types.T_float32:
						min := types.DecodeFixed[float32](zm.GetMinBuf())
						if min < partialResults[i].(float32) {
							partialResults[i] = min
						}
					case types.T_float64:
						min := types.DecodeFixed[float64](zm.GetMinBuf())
						if min < partialResults[i].(float64) {
							partialResults[i] = min
						}
					case types.T_date:
						min := types.DecodeFixed[types.Date](zm.GetMinBuf())
						if min < partialResults[i].(types.Date) {
							partialResults[i] = min
						}
					case types.T_time:
						min := types.DecodeFixed[types.Time](zm.GetMinBuf())
						if min < partialResults[i].(types.Time) {
							partialResults[i] = min
						}
					case types.T_datetime:
						min := types.DecodeFixed[types.Datetime](zm.GetMinBuf())
						if min < partialResults[i].(types.Datetime) {
							partialResults[i] = min
						}
					case types.T_timestamp:
						min := types.DecodeFixed[types.Timestamp](zm.GetMinBuf())
						if min < partialResults[i].(types.Timestamp) {
							partialResults[i] = min
						}
					case types.T_enum:
						min := types.DecodeFixed[types.Enum](zm.GetMinBuf())
						if min < partialResults[i].(types.Enum) {
							partialResults[i] = min
						}
					case types.T_decimal64:
						min := types.DecodeFixed[types.Decimal64](zm.GetMinBuf())
						if min < partialResults[i].(types.Decimal64) {
							partialResults[i] = min
						}
					case types.T_decimal128:
						min := types.DecodeFixed[types.Decimal128](zm.GetMinBuf())
						if min.Compare(partialResults[i].(types.Decimal128)) < 0 {
							partialResults[i] = min
						}
					case types.T_uuid:
						min := types.DecodeFixed[types.Uuid](zm.GetMinBuf())
						if min.Lt(partialResults[i].(types.Uuid)) {
							partialResults[i] = min
						}
					case types.T_TS:
						min := types.DecodeFixed[types.TS](zm.GetMinBuf())
						ts := partialResults[i].(types.TS)
						if min.LT(&ts) {
							partialResults[i] = min
						}
					case types.T_Rowid:
						min := types.DecodeFixed[types.Rowid](zm.GetMinBuf())
						v := partialResults[i].(types.Rowid)
						if min.LT(&v) {
							partialResults[i] = min
						}
					case types.T_Blockid:
						min := types.DecodeFixed[types.Blockid](zm.GetMinBuf())
						v := partialResults[i].(types.Blockid)
						if min.LT(&v) {
							partialResults[i] = min
						}
					}
				}
			}
		case "max":
			col := agg.F.Args[0].Expr.(*plan.Expr_Col)
			zm := blkMeta.ColumnMeta(uint16(columnMap[int(col.Col.ColPos)])).ZoneMap()
			if zm.GetType().FixedLength() < 0 {
				return &moerr.Error{}
			} else {
				if partialResults[i] == nil {
					partialResults[i] = zm.GetMax()
					partialResultTypes[i] = zm.GetType()
				} else {
					switch zm.GetType() {
					case types.T_bool:
						partialResults[i] = partialResults[i].(bool) || types.DecodeFixed[bool](zm.GetMaxBuf())
					case types.T_bit:
						max := types.DecodeFixed[uint64](zm.GetMaxBuf())
						if max > partialResults[i].(uint64) {
							partialResults[i] = max
						}
					case types.T_int8:
						max := types.DecodeFixed[int8](zm.GetMaxBuf())
						if max > partialResults[i].(int8) {
							partialResults[i] = max
						}
					case types.T_int16:
						max := types.DecodeFixed[int16](zm.GetMaxBuf())
						if max > partialResults[i].(int16) {
							partialResults[i] = max
						}
					case types.T_int32:
						max := types.DecodeFixed[int32](zm.GetMaxBuf())
						if max > partialResults[i].(int32) {
							partialResults[i] = max
						}
					case types.T_int64:
						max := types.DecodeFixed[int64](zm.GetMaxBuf())
						if max > partialResults[i].(int64) {
							partialResults[i] = max
						}
					case types.T_uint8:
						max := types.DecodeFixed[uint8](zm.GetMaxBuf())
						if max > partialResults[i].(uint8) {
							partialResults[i] = max
						}
					case types.T_uint16:
						max := types.DecodeFixed[uint16](zm.GetMaxBuf())
						if max > partialResults[i].(uint16) {
							partialResults[i] = max
						}
					case types.T_uint32:
						max := types.DecodeFixed[uint32](zm.GetMaxBuf())
						if max > partialResults[i].(uint32) {
							partialResults[i] = max
						}
					case types.T_uint64:
						max := types.DecodeFixed[uint64](zm.GetMaxBuf())
						if max > partialResults[i].(uint64) {
							partialResults[i] = max
						}
					case types.T_float32:
						max := types.DecodeFixed[float32](zm.GetMaxBuf())
						if max > partialResults[i].(float32) {
							partialResults[i] = max
						}
					case types.T_float64:
						max := types.DecodeFixed[float64](zm.GetMaxBuf())
						if max > partialResults[i].(float64) {
							partialResults[i] = max
						}
					case types.T_date:
						max := types.DecodeFixed[types.Date](zm.GetMaxBuf())
						if max > partialResults[i].(types.Date) {
							partialResults[i] = max
						}
					case types.T_time:
						max := types.DecodeFixed[types.Time](zm.GetMaxBuf())
						if max > partialResults[i].(types.Time) {
							partialResults[i] = max
						}
					case types.T_datetime:
						max := types.DecodeFixed[types.Datetime](zm.GetMaxBuf())
						if max > partialResults[i].(types.Datetime) {
							partialResults[i] = max
						}
					case types.T_timestamp:
						max := types.DecodeFixed[types.Timestamp](zm.GetMaxBuf())
						if max > partialResults[i].(types.Timestamp) {
							partialResults[i] = max
						}
					case types.T_enum:
						max := types.DecodeFixed[types.Enum](zm.GetMaxBuf())
						if max > partialResults[i].(types.Enum) {
							partialResults[i] = max
						}
					case types.T_decimal64:
						max := types.DecodeFixed[types.Decimal64](zm.GetMaxBuf())
						if max > partialResults[i].(types.Decimal64) {
							partialResults[i] = max
						}
					case types.T_decimal128:
						max := types.DecodeFixed[types.Decimal128](zm.GetMaxBuf())
						if max.Compare(partialResults[i].(types.Decimal128)) > 0 {
							partialResults[i] = max
						}
					case types.T_uuid:
						max := types.DecodeFixed[types.Uuid](zm.GetMaxBuf())
						if max.Gt(partialResults[i].(types.Uuid)) {
							partialResults[i] = max
						}
					case types.T_TS:
						max := types.DecodeFixed[types.TS](zm.GetMaxBuf())
						ts := partialResults[i].(types.TS)
						if max.GT(&ts) {
							partialResults[i] = max
						}
					case types.T_Rowid:
						max := types.DecodeFixed[types.Rowid](zm.GetMaxBuf())
						v := partialResults[i].(types.Rowid)
						if max.GT(&v) {
							partialResults[i] = max
						}
					case types.T_Blockid:
						max := types.DecodeFixed[types.Blockid](zm.GetMaxBuf())
						v := partialResults[i].(types.Blockid)
						if max.GT(&v) {
							partialResults[i] = max
						}
					}
				}
			}
		}
	}
	return nil
}

func dupType(typ *plan.Type) types.Type {
	return types.NewWithCharset(types.T(typ.Id), typ.Width, typ.Scale, uint8(typ.Charset))
}

func sameExecutionNode(left, right engine.Node) bool {
	if left.Id != "" && right.Id != "" && left.Id == right.Id {
		return true
	}
	if left.Addr == "" || right.Addr == "" {
		if left.Addr != "" || right.Addr != "" {
			return false
		}
		if left.Id != "" && right.Id != "" {
			return left.Id == right.Id
		}
		return true
	}
	return sameExecutionAddr(left.Addr, right.Addr)
}

func sameExecutionAddr(addr string, currentCNAddr string) bool {
	if addr == "" || currentCNAddr == "" {
		return addr == currentCNAddr
	}
	parts1 := strings.Split(addr, ":")
	parts2 := strings.Split(currentCNAddr, ":")
	if len(parts1) != 2 || len(parts2) != 2 {
		return addr == currentCNAddr
	}
	return parts1[0] == parts2[0] && parts1[1] == parts2[1]
}

func (s *Scope) affectedRows() uint64 {
	op := s.RootOp
	affectedRows := uint64(0)

	for op != nil {
		if arg, ok := op.(vm.ModificationArgument); ok {
			if marg, ok := arg.(*mergeblock.MergeBlock); ok {
				return marg.GetAffectedRows()
			}
			affectedRows += arg.GetAffectedRows()
		}
		if op.GetOperatorBase().NumChildren() == 0 {
			op = nil
		} else {
			op = op.GetOperatorBase().GetChildren(0)
		}
	}
	return affectedRows
}

func (c *Compile) runSql(sql string) error {
	return c.runSqlWithAccountId(sql, NoAccountId)
}

func (c *Compile) runSqlWithOptions(
	sql string,
	options executor.StatementOption,
) error {
	return c.runSqlWithAccountIdAndOptions(sql, NoAccountId, options)
}

func (c *Compile) runSqlWithAccountId(sql string, accountId int32) error {
	return c.runSqlWithAccountIdAndOptions(sql, accountId, executor.StatementOption{})
}

func (c *Compile) runSqlWithAccountIdAndOptions(
	sql string,
	accountId int32,
	options executor.StatementOption,
) error {
	if sql == "" {
		return nil
	}
	res, err := c.runSqlWithResultAndOptions(sql, accountId, options)
	if err != nil {
		return err
	}
	res.Close()
	return nil
}

func (c *Compile) runSqlWithResult(sql string, accountId int32) (executor.Result, error) {
	return c.runSqlWithResultAndOptions(sql, accountId, executor.StatementOption{})
}

func (c *Compile) runSqlWithResultAndOptions(
	sql string,
	accountId int32,
	options executor.StatementOption,
) (executor.Result, error) {
	v, ok := moruntime.ServiceRuntime(c.proc.GetService()).GetGlobalVariables(moruntime.InternalSQLExecutor)
	if !ok {
		panic("missing lock service")
	}

	lower := c.getLower()

	if c.pn != nil {
		if qry, ok := c.pn.Plan.(*plan.Plan_Ddl); ok {
			if qry.Ddl.DdlType == plan.DataDefinition_DROP_DATABASE {
				options = options.WithIgnoreForeignKey()
			}
		}
	}

	exec := v.(executor.SQLExecutor)
	// Propagate the IsFrontend signal from the outer Compile's proc
	// to the sub-execution. Without this, sub-Compiles spawned for
	// internal sub-SQL (e.g. ALTER TABLE COPY's CreateTmpTableSql)
	// default to IsFrontend=false even when the outer caller is
	// user-driven, and any downstream code that gates on
	// ctx.IsFrontend() / proc.Base.IsFrontend silently misfires —
	// most notably CreateAllIndexUpdateTasks, which would otherwise
	// see metadata=nil from BuildIdxcronMetadata and write '' into
	// mo_index_update's JSON column. Mirrors the propagation already
	// in pkg/vectorindex/sqlexec/sqlexec.go.
	opts := executor.Options{}.
		// All runSql and runSqlWithResult is a part of input sql, can not incr statement.
		// All these sub-sql's need to be rolled back and retried en masse when they conflict in pessimistic mode
		WithDisableIncrStatement().
		WithTxn(c.proc.GetTxnOperator()).
		WithDatabase(c.db).
		WithTimeZone(c.proc.GetSessionInfo().TimeZone).
		WithLowerCaseTableNames(&lower).
		WithStatementOption(options).
		WithResolveVariableFunc(c.proc.GetResolveVariableFunc()).
		WithFrontend(c.proc.Base.IsFrontend)

	ctx := c.proc.Ctx
	if ctx == nil {
		ctx = c.proc.GetTopContext()
	}
	if ctx == nil {
		ctx = context.Background()
	}
	// Ensure ParameterUnit is available in ctx for downstream helpers which call config.GetParameterUnit(ctx)
	if ctx.Value(config.ParameterUnitKey) == nil {
		if v, ok := moruntime.ServiceRuntime(c.proc.GetService()).GetGlobalVariables("parameter-unit"); ok {
			if pu, ok2 := v.(*config.ParameterUnit); ok2 && pu != nil {
				ctx = context.WithValue(ctx, config.ParameterUnitKey, pu)
			}
		}
	}
	if accountId >= 0 {
		opts = opts.WithAccountID(uint32(accountId))
	}
	return exec.Exec(ctx, sql, opts)
}

func (c *Compile) fatalLog(retry int, err error) {
	if err == nil {
		return
	}
	fatal := moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetry) ||
		moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetryWithDefChanged) ||
		moerr.IsMoErrCode(err, moerr.ErrTxnWWConflict) ||
		moerr.IsMoErrCode(err, moerr.ErrDuplicateEntry) ||
		moerr.IsMoErrCode(err, moerr.ER_DUP_ENTRY) ||
		moerr.IsMoErrCode(err, moerr.ER_DUP_ENTRY_WITH_KEY_NAME)
	if !fatal {
		return
	}

	if retry == 0 &&
		(moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetry) ||
			moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetryWithDefChanged)) {
		return
	}

	txnTrace.GetService(c.proc.GetService()).TxnError(c.proc.GetTxnOperator(), err)

	v, ok := moruntime.ServiceRuntime(c.proc.GetService()).
		GetGlobalVariables(moruntime.EnableCheckInvalidRCErrors)
	if !ok || !v.(bool) {
		return
	}

	c.proc.Fatalf(c.proc.Ctx, "BUG(RC): txn %s retry %d, error %+v\n",
		hex.EncodeToString(c.proc.GetTxnOperator().Txn().ID),
		retry,
		err.Error())
}

func (c *Compile) SetOriginSQL(sql string) {
	c.originSQL = sqlmongodb.RedactSQLForDiagnostics(sql)
}

// SetResourceAttemptOwnerEligible marks this Compile as the top-level
// statement candidate allowed to publish retry generations. The statement
// resource root enforces that at most one eligible Compile becomes the owner.
func (c *Compile) SetResourceAttemptOwnerEligible() {
	c.resourceAttemptOwnerEligible = true
}

func (c *Compile) SetBuildPlanFunc(buildPlanFunc func(ctx context.Context) (*plan2.Plan, error)) {
	c.buildPlanFunc = buildPlanFunc
}

// detectFkSelfRefer checks if foreign key self refer confirmed
func detectFkSelfRefer(c *Compile, detectSqls []string) error {
	if len(detectSqls) == 0 {
		return nil
	}
	for _, sql := range detectSqls {
		err := runDetectSql(c, sql)
		if err != nil {
			c.debugLogFor19288(err, sql)
			return err
		}
	}

	return nil
}

// runDetectSql runs the fk detecting sql
func runDetectSql(c *Compile, sql string) error {
	res, err := c.runSqlWithResultAndOptions(sql, NoAccountId, executor.StatementOption{}.WithDisableLog())
	if err != nil {
		c.proc.Errorf(c.proc.Ctx, "The sql that caused the fk self refer check failed is %s, and generated background sql is %s", c.sql, sql)
		return err
	}
	defer res.Close()

	if res.Batches != nil {
		vs := res.Batches[0].Vecs
		if vs != nil && vs[0].Length() > 0 {
			yes := vector.GetFixedAtWithTypeCheck[bool](vs[0], 0)
			if !yes {
				return moerr.NewErrFKNoReferencedRow2(c.proc.Ctx)
			}
		}
	}
	return nil
}

// runDetectFkReferToDBSql runs the fk detecting sql
func runDetectFkReferToDBSql(c *Compile, sql string) error {
	res, err := c.runSqlWithResultAndOptions(sql, NoAccountId, executor.StatementOption{}.WithDisableLog())
	if err != nil {
		c.proc.Errorf(c.proc.Ctx, "The sql that caused the fk self refer check failed is %s, and generated background sql is %s", c.sql, sql)
		return err
	}
	defer res.Close()

	if res.Batches != nil {
		vs := res.Batches[0].Vecs
		if vs != nil && vs[0].Length() > 0 {
			yes := vector.GetFixedAtWithTypeCheck[bool](vs[0], 0)
			if yes {
				return moerr.NewInternalError(c.proc.Ctx,
					"can not drop database. It has been referenced by foreign keys")
			}
		}
	}
	return nil
}

func getEngineNode(c *Compile) engine.Node {
	// getEngineNode only describes the local execution identity and capacity.
	// Runtime work state is intentionally left Unknown here; callers that make
	// AP multi-CN placement decisions must enrich it from cluster metadata.
	if c.IsTpQuery() {
		return engine.Node{Addr: c.addr, Mcpu: 1}
	} else {
		return engine.Node{Addr: c.addr, Mcpu: c.ncpu}
	}
}

func (c *Compile) setHaveDDL(haveDDL bool) {
	txn := c.proc.GetTxnOperator()
	if txn != nil && txn.GetWorkspace() != nil {
		txn.GetWorkspace().SetHaveDDL(haveDDL)
	}
}

func (c *Compile) getHaveDDL() bool {
	txn := c.proc.GetTxnOperator()
	if txn != nil && txn.GetWorkspace() != nil {
		return txn.GetWorkspace().GetHaveDDL()
	}
	return false
}

func (c *Compile) getLower() int64 {
	// default 1
	var lower int64 = 1
	if resolveVariableFunc := c.proc.GetResolveVariableFunc(); resolveVariableFunc != nil {
		lowerVar, err := resolveVariableFunc("lower_case_table_names", true, false)
		if err != nil {
			return 1
		}
		lower = lowerVar.(int64)
	}
	return lower
}

func (c *Compile) compileTableClone(
	pn *plan.Plan,
) ([]*Scope, error) {

	var (
		err error
		s1  *Scope

		node      engine.Node
		clonePlan = pn.GetDdl().GetCloneTable()
	)

	node = getEngineNode(c)

	copyOp, err := constructTableClone(c, clonePlan)
	if err != nil {
		return nil, err
	}

	s1 = newScope(TableClone)
	s1.NodeInfo = node
	s1.TxnOffset = c.TxnOffset
	s1.Plan = pn

	s1.Proc = c.proc.NewNoContextChildProc(0)
	s1.setRootOperator(copyOp)

	return []*Scope{s1}, nil
}

// isTableFromPublication checks if a table is a CCPR shared table (from publication)
func isTableFromPublication(tableDef *plan.TableDef) bool {
	if tableDef == nil {
		return false
	}
	for _, def := range tableDef.Defs {
		if propDef, ok := def.Def.(*plan.TableDef_DefType_Properties); ok {
			for _, prop := range propDef.Properties.Properties {
				if prop.Key == catalog.PropFromPublication && prop.Value == "true" {
					return true
				}
			}
		}
	}
	return false
}

// shouldBlockCCPRReadOnly checks if the CCPR read-only check should block the operation.
// Returns true if the operation should be blocked (table is from publication AND this is NOT a CCPR task transaction).
func (c *Compile) shouldBlockCCPRReadOnly(tableDef *plan.TableDef) bool {
	if !isTableFromPublication(tableDef) {
		return false
	}
	// If this is a CCPR task transaction with a valid task ID, allow the operation
	if c.isCCPRTaskTransaction() {
		return false
	}
	return true
}

// isCCPRTaskTransaction checks if the current transaction is a CCPR task transaction.
// Returns true if the transaction has a valid CCPR task ID.
func (c *Compile) isCCPRTaskTransaction() bool {
	if txnOp := c.proc.GetTxnOperator(); txnOp != nil {
		if ws := txnOp.GetWorkspace(); ws != nil {
			if ws.GetCCPRTaskID() != "" {
				return true
			}
		}
	}
	return false
}
