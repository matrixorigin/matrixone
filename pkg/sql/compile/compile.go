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
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"net"
	"runtime"
	gotrace "runtime/trace"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/sql/colexec/offset"

	"github.com/matrixorigin/matrixone/pkg/sql/colexec/intersect"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/intersectall"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/minus"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"

	"github.com/google/uuid"
	"github.com/panjf2000/ants/v2"
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/cnservice/cnclient"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/connector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/deletion"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/dispatch"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/external"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/insert"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/lockop"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/merge"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergeblock"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergecte"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergedelete"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergerecursive"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/output"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/preinsert"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/preinsertsecondaryindex"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/preinsertunique"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/sample"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/rule"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
	mokafka "github.com/matrixorigin/matrixone/pkg/stream/adapter/kafka"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/txn/storage/memorystorage"
	txnTrace "github.com/matrixorigin/matrixone/pkg/txn/trace"
	util2 "github.com/matrixorigin/matrixone/pkg/util"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace/statistic"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// Note: Now the cost going from stat is actually the number of rows, so we can only estimate a number for the size of each row.
// The current insertion of around 200,000 rows triggers cn to write s3 directly
const (
	DistributedThreshold     uint64 = 10 * mpool.MB
	SingleLineSizeEstimate   uint64 = 300 * mpool.B
	shuffleChannelBufferSize        = 16
)

var (
	ncpu           = runtime.GOMAXPROCS(0)
	ctxCancelError = context.Canceled.Error()
)

// NewCompile is used to new an object of compile
func NewCompile(
	addr, db, sql, tenant, uid string,
	ctx context.Context,
	e engine.Engine,
	proc *process.Process,
	stmt tree.Statement,
	isInternal bool,
	cnLabel map[string]string,
	startAt time.Time,
) *Compile {
	c := reuse.Alloc[Compile](nil)
	c.e = e
	c.db = db
	c.ctx = ctx
	c.tenant = tenant
	c.uid = uid
	c.sql = sql
	c.proc = proc
	c.proc.MessageBoard = c.MessageBoard
	c.stmt = stmt
	c.addr = addr
	c.isInternal = isInternal
	c.cnLabel = cnLabel
	c.startAt = startAt
	c.disableRetry = false
	if c.proc.TxnOperator != nil {
		// TODO: The action of updating the WriteOffset logic should be executed in the `func (c *Compile) Run(_ uint64)` method.
		// However, considering that the delay ranges are not completed yet, the UpdateSnapshotWriteOffset() and
		// the assignment of `Compile.TxnOffset` should be moved into the `func (c *Compile) Run(_ uint64)` method in the later stage.
		c.proc.TxnOperator.GetWorkspace().UpdateSnapshotWriteOffset()
		c.TxnOffset = c.proc.TxnOperator.GetWorkspace().GetSnapshotWriteOffset()
	} else {
		c.TxnOffset = 0
	}
	return c
}

func (c *Compile) Release() {
	if c == nil {
		return
	}
	reuse.Free[Compile](c, nil)
}

func (c Compile) TypeName() string {
	return "compile.Compile"
}

func (c *Compile) GetMessageCenter() *process.MessageCenter {
	if c == nil || c.e == nil {
		return nil
	}
	m := c.e.GetMessageCenter()
	if m != nil {
		mc, ok := m.(*process.MessageCenter)
		if ok {
			return mc
		}
	}
	return nil
}

func (c *Compile) Reset(startAt time.Time) {
	c.affectRows.Store(0)

	for _, info := range c.anal.analInfos {
		info.Reset()
	}

	c.MessageBoard = c.MessageBoard.Reset()
	c.counterSet.Reset()

	for _, f := range c.fuzzys {
		f.reset()
	}
	c.startAt = startAt
}

func (c *Compile) clear() {
	if c.anal != nil {
		c.anal.release()
	}
	for i := range c.scope {
		c.scope[i].release()
	}
	for i := range c.fuzzys {
		c.fuzzys[i].release()
	}

	c.MessageBoard = c.MessageBoard.Reset()
	c.fuzzys = c.fuzzys[:0]
	c.scope = c.scope[:0]
	c.pn = nil
	c.fill = nil
	c.affectRows.Store(0)
	c.addr = ""
	c.db = ""
	c.tenant = ""
	c.uid = ""
	c.sql = ""
	c.originSQL = ""
	c.anal = nil
	c.e = nil
	c.ctx = nil
	c.proc = nil
	c.cnList = c.cnList[:0]
	c.stmt = nil
	c.startAt = time.Time{}
	c.needLockMeta = false
	c.isInternal = false
	c.lastAllocID = 0

	for k := range c.metaTables {
		delete(c.metaTables, k)
	}
	for k := range c.lockTables {
		delete(c.lockTables, k)
	}
	for k := range c.nodeRegs {
		delete(c.nodeRegs, k)
	}
	for k := range c.stepRegs {
		delete(c.stepRegs, k)
	}
	for k := range c.cnLabel {
		delete(c.cnLabel, k)
	}
}

// helper function to judge if init temporary engine is needed
func (c *Compile) NeedInitTempEngine() bool {
	for _, s := range c.scope {
		ddl := s.Plan.GetDdl()
		if ddl == nil {
			continue
		}
		if qry := ddl.GetCreateTable(); qry != nil && qry.Temporary {
			if c.e.(*engine.EntireEngine).TempEngine == nil {
				return true
			}
		}
	}
	return false
}

func (c *Compile) SetTempEngine(tempEngine engine.Engine, tempStorage *memorystorage.Storage) {
	e := c.e.(*engine.EntireEngine)
	e.TempEngine = tempEngine
	if c.ctx != nil && c.ctx.Value(defines.TemporaryTN{}) == nil {
		c.ctx = context.WithValue(c.ctx, defines.TemporaryTN{}, tempStorage)
	}
}

// Compile is the entrance of the compute-execute-layer.
// It generates a scope (logic pipeline) for a query plan.
func (c *Compile) Compile(ctx context.Context, pn *plan.Plan, fill func(*batch.Batch) error) (err error) {
	start := time.Now()
	defer func() {
		v2.TxnStatementCompileDurationHistogram.Observe(time.Since(start).Seconds())
	}()

	_, task := gotrace.NewTask(context.TODO(), "pipeline.Compile")
	defer task.End()
	defer func() {
		if e := recover(); e != nil {
			err = moerr.ConvertPanicError(ctx, e)
			c.proc.Error(ctx, "panic in compile",
				zap.String("sql", c.sql),
				zap.String("error", err.Error()))
		}
	}()

	if c.proc.TxnOperator != nil && c.proc.TxnOperator.Txn().IsPessimistic() {
		txnOp := c.proc.TxnOperator
		seq := txnOp.NextSequence()
		txnTrace.GetService().AddTxnDurationAction(
			txnOp,
			client.CompileEvent,
			seq,
			0,
			0,
			err)
		defer func() {
			txnTrace.GetService().AddTxnDurationAction(
				txnOp,
				client.CompileEvent,
				seq,
				0,
				time.Since(start),
				err)
		}()

		if qry, ok := pn.Plan.(*plan.Plan_Query); ok {
			if qry.Query.StmtType == plan.Query_SELECT {
				for _, n := range qry.Query.Nodes {
					if n.NodeType == plan.Node_LOCK_OP {
						c.needLockMeta = true
						break
					}
				}
			} else {
				c.needLockMeta = true
			}
		}
	}

	// with values
	c.proc.Ctx = perfcounter.WithCounterSet(c.proc.Ctx, c.counterSet)
	c.ctx = c.proc.Ctx

	// session info and callback function to write back query result.
	// XXX u is really a bad name, I'm not sure if `session` or `user` will be more suitable.
	c.fill = fill

	c.pn = pn

	// Compile may exec some function that need engine.Engine.
	c.proc.Ctx = context.WithValue(c.proc.Ctx, defines.EngineKey{}, c.e)
	// generate logic pipeline for query.
	c.scope, err = c.compileScope(ctx, pn)

	if err != nil {
		return err
	}
	for _, s := range c.scope {
		if len(s.NodeInfo.Addr) == 0 {
			s.NodeInfo.Addr = c.addr
		}
	}
	if c.shouldReturnCtxErr() {
		return c.proc.Ctx.Err()
	}
	return nil
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
	//fmt.Println(DebugShowScopes([]*Scope{s}))
	switch s.Magic {
	case Normal:
		defer c.fillAnalyzeInfo()
		err := s.Run(c)
		if err != nil {
			return err
		}

		c.addAffectedRows(s.affectedRows())
		return nil
	case Merge, MergeInsert:
		defer c.fillAnalyzeInfo()
		err := s.MergeRun(c)
		if err != nil {
			return err
		}

		c.addAffectedRows(s.affectedRows())
		return nil
	case MergeDelete:
		defer c.fillAnalyzeInfo()
		err := s.MergeRun(c)
		if err != nil {
			return err
		}
		mergeArg := s.Instructions[len(s.Instructions)-1].Arg.(*mergedelete.Argument)
		if mergeArg.AddAffectedRows {
			c.addAffectedRows(mergeArg.AffectedRows)
		}
		return nil
	case Remote:
		defer c.fillAnalyzeInfo()
		err := s.RemoteRun(c)
		c.addAffectedRows(s.affectedRows())
		return err
	case CreateDatabase:
		err := s.CreateDatabase(c)
		if err != nil {
			return err
		}
		c.setAffectedRows(1)
		return nil
	case DropDatabase:
		err := s.DropDatabase(c)
		if err != nil {
			return err
		}
		c.setAffectedRows(1)
		return nil
	case CreateTable:
		qry := s.Plan.GetDdl().GetCreateTable()
		if qry.Temporary {
			return s.CreateTempTable(c)
		} else {
			return s.CreateTable(c)
		}
	case CreateView:
		return s.CreateView(c)
	case AlterView:
		return s.AlterView(c)
	case AlterTable:
		return s.AlterTable(c)
	case DropTable:
		return s.DropTable(c)
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
	case Replace:
		return s.replace(c)
	}
	return nil
}

func (c *Compile) allocOperatorID() int32 {
	c.lock.Lock()
	defer func() {
		c.lastAllocID++
		c.lock.Unlock()
	}()

	return c.lastAllocID
}

// Run is an important function of the compute-layer, it executes a single sql according to its scope
// Need call Release() after call this function.
func (c *Compile) Run(_ uint64) (result *util2.RunResult, err error) {
	sql := c.originSQL
	if sql == "" {
		sql = c.sql
	}

	txnOp := c.proc.TxnOperator
	seq := uint64(0)
	if txnOp != nil {
		seq = txnOp.NextSequence()
		txnOp.EnterRunSql()
	}

	defer func() {
		if txnOp != nil {
			txnOp.ExitRunSql()
		}
		c.proc.CleanValueScanBatchs()
		c.proc.SetPrepareBatch(nil)
		c.proc.SetPrepareExprList(nil)
	}()

	var writeOffset uint64

	start := time.Now()
	v2.TxnStatementExecuteLatencyDurationHistogram.Observe(start.Sub(c.startAt).Seconds())

	stats := statistic.StatsInfoFromContext(c.proc.Ctx)
	stats.ExecutionStart()

	txnTrace.GetService().TxnStatementStart(txnOp, sql, seq)
	defer func() {
		stats.ExecutionEnd()

		cost := time.Since(start)
		row := 0
		if result != nil {
			row = int(result.AffectRows)
		}
		txnTrace.GetService().TxnStatementCompleted(
			txnOp,
			sql,
			cost,
			seq,
			row,
			err,
		)
		v2.TxnStatementExecuteDurationHistogram.Observe(cost.Seconds())
	}()

	for _, s := range c.scope {
		s.SetOperatorInfoRecursively(c.allocOperatorID)
	}

	if c.proc.TxnOperator != nil {
		writeOffset = uint64(c.proc.TxnOperator.GetWorkspace().GetSnapshotWriteOffset())
	}
	result = &util2.RunResult{}
	var span trace.Span
	var runC *Compile // compile structure for rerun.
	// var result = &util2.RunResult{}
	// var err error
	var retryTimes int
	releaseRunC := func() {
		if runC != c {
			runC.Release()
		}
	}

	sp := c.proc.GetStmtProfile()
	c.ctx, span = trace.Start(c.ctx, "Compile.Run", trace.WithKind(trace.SpanKindStatement))
	_, task := gotrace.NewTask(context.TODO(), "pipeline.Run")
	defer func() {
		releaseRunC()

		task.End()
		span.End(trace.WithStatementExtra(sp.GetTxnId(), sp.GetStmtId(), sp.GetSqlOfStmt()))
	}()

	if c.proc.TxnOperator != nil {
		c.proc.TxnOperator.GetWorkspace().IncrSQLCount()
		c.proc.TxnOperator.ResetRetry(false)
	}

	v2.TxnStatementTotalCounter.Inc()
	runC = c
	for {
		if err = runC.runOnce(); err == nil {
			break
		}

		c.fatalLog(retryTimes, err)
		if !c.canRetry(err) {
			if c.proc.TxnOperator.Txn().IsRCIsolation() &&
				moerr.IsMoErrCode(err, moerr.ErrDuplicateEntry) {
				orphan, e := c.proc.LockService.IsOrphanTxn(
					c.proc.Ctx,
					c.proc.TxnOperator.Txn().ID,
				)
				if e != nil {
					getLogger().Error("failed to convert dup to orphan txn error",
						zap.String("txn", hex.EncodeToString(c.proc.TxnOperator.Txn().ID)),
						zap.Error(err),
					)
				}
				if e == nil && orphan {
					getLogger().Warn("convert dup to orphan txn error",
						zap.String("txn", hex.EncodeToString(c.proc.TxnOperator.Txn().ID)),
					)
					err = moerr.NewCannotCommitOrphan(c.proc.Ctx)
				}
			}
			return nil, err
		}

		retryTimes++
		releaseRunC()
		defChanged := moerr.IsMoErrCode(
			err,
			moerr.ErrTxnNeedRetryWithDefChanged)
		if runC, err = c.prepareRetry(defChanged); err != nil {
			return nil, err
		}
	}

	if c.shouldReturnCtxErr() {
		return nil, c.proc.Ctx.Err()
	}
	result.AffectRows = runC.getAffectedRows()

	if c.proc.TxnOperator != nil {
		return result, c.proc.TxnOperator.GetWorkspace().Adjust(writeOffset)
	}
	return result, nil
}

func (c *Compile) prepareRetry(defChanged bool) (*Compile, error) {
	v2.TxnStatementRetryCounter.Inc()
	c.proc.TxnOperator.ResetRetry(true)
	c.proc.TxnOperator.GetWorkspace().IncrSQLCount()

	// clear the workspace of the failed statement
	if e := c.proc.TxnOperator.GetWorkspace().RollbackLastStatement(c.ctx); e != nil {
		return nil, e
	}

	// increase the statement id
	if e := c.proc.TxnOperator.GetWorkspace().IncrStatementID(c.ctx, false); e != nil {
		return nil, e
	}

	// FIXME: the current retry method is quite bad, the overhead is relatively large, and needs to be
	// improved to refresh expression in the future.

	var e error
	runC := NewCompile(c.addr, c.db, c.sql, c.tenant, c.uid, c.proc.Ctx, c.e, c.proc, c.stmt, c.isInternal, c.cnLabel, c.startAt)
	defer func() {
		if e != nil {
			runC.Release()
		}
	}()
	if defChanged {
		var pn *plan2.Plan
		pn, e = c.buildPlanFunc()
		if e != nil {
			return nil, e
		}
		c.pn = pn
	}
	if e = runC.Compile(c.proc.Ctx, c.pn, c.fill); e != nil {
		return nil, e
	}

	return runC, nil
}

// isRetryErr if the error is ErrTxnNeedRetry and the transaction is RC isolation, we need to retry t
// he statement
func (c *Compile) isRetryErr(err error) bool {
	return (moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetry) ||
		moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetryWithDefChanged)) &&
		c.proc.TxnOperator.Txn().IsRCIsolation()
}

func (c *Compile) canRetry(err error) bool {
	return !c.disableRetry && c.isRetryErr(err)
}

// run once
func (c *Compile) runOnce() error {
	var wg sync.WaitGroup
	err := c.lockMetaTables()
	if err != nil {
		return err
	}
	err = c.lockTable()
	if err != nil {
		return err
	}
	errC := make(chan error, len(c.scope))
	for _, s := range c.scope {
		s.SetContextRecursively(c.proc.Ctx)
		err = s.InitAllDataSource(c)
		if err != nil {
			return err
		}
	}

	for i := range c.scope {
		wg.Add(1)
		scope := c.scope[i]
		errSubmit := ants.Submit(func() {
			defer func() {
				if e := recover(); e != nil {
					err := moerr.ConvertPanicError(c.ctx, e)
					c.proc.Error(c.ctx, "panic in run",
						zap.String("sql", c.sql),
						zap.String("error", err.Error()))
					errC <- err
				}
				wg.Done()
			}()
			errC <- c.run(scope)
		})
		if errSubmit != nil {
			errC <- errSubmit
			wg.Done()
		}
	}
	wg.Wait()
	close(errC)

	errList := make([]error, 0, len(c.scope))
	for e := range errC {
		if e != nil {
			errList = append(errList, e)
			if c.isRetryErr(e) {
				return e
			}
		}
	}

	if len(errList) > 0 {
		err = errList[0]
	}
	if err != nil {
		return err
	}

	// fuzzy filter not sure whether this insert / load obey duplicate constraints, need double check
	for _, f := range c.fuzzys {
		if f != nil && f.cnt > 0 {
			if f.cnt > 10 {
				c.proc.Warnf(c.ctx, "fuzzy filter cnt is %d, may be too high", f.cnt)
			}
			err = f.backgroundSQLCheck(c)
			if err != nil {
				return err
			}
		}
	}

	//detect fk self refer
	//update, insert
	query := c.pn.GetQuery()
	if query != nil && (query.StmtType == plan.Query_INSERT ||
		query.StmtType == plan.Query_UPDATE) && len(query.GetDetectSqls()) != 0 {
		err = detectFkSelfRefer(c, query.DetectSqls)
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

// shouldReturnCtxErr return true only if the ctx has error and the error is not canceled.
// maybe deadlined or other error.
func (c *Compile) shouldReturnCtxErr() bool {
	if e := c.proc.Ctx.Err(); e != nil && e.Error() != ctxCancelError {
		return true
	}
	return false
}

func (c *Compile) compileScope(ctx context.Context, pn *plan.Plan) ([]*Scope, error) {
	start := time.Now()
	defer func() {
		v2.TxnStatementCompileScopeHistogram.Observe(time.Since(start).Seconds())
	}()
	switch qry := pn.Plan.(type) {
	case *plan.Plan_Query:
		switch qry.Query.StmtType {
		case plan.Query_REPLACE:
			return []*Scope{
				newScope(Replace).
					withPlan(pn),
			}, nil
		}
		scopes, err := c.compileQuery(ctx, qry.Query)
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
		case plan.DataDefinition_DROP_TABLE:
			return []*Scope{
				newScope(DropTable).
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
			return c.compileQuery(ctx, pn.GetDdl().GetQuery())
			// 1、not supported: show arnings/errors/status/processlist
			// 2、show variables will not return query
			// 3、show create database/table need rewrite to create sql
		}
	}
	return nil, moerr.NewNYI(ctx, fmt.Sprintf("query '%s'", pn))
}

func (c *Compile) appendMetaTables(objRes *plan.ObjectRef) {
	if !c.needLockMeta {
		return
	}

	if objRes.SchemaName == catalog.MO_CATALOG && (objRes.ObjName == catalog.MO_DATABASE || objRes.ObjName == catalog.MO_TABLES || objRes.ObjName == catalog.MO_COLUMNS) {
		// do not lock meta table for meta table
	} else {
		key := fmt.Sprintf("%s %s", objRes.SchemaName, objRes.ObjName)
		c.metaTables[key] = struct{}{}
	}
}

func (c *Compile) lockMetaTables() error {
	lockLen := len(c.metaTables)
	if lockLen == 0 {
		return nil
	}

	tables := make([]string, 0, lockLen)
	for table := range c.metaTables {
		tables = append(tables, table)
	}
	sort.Strings(tables)

	for _, table := range tables {
		names := strings.SplitN(table, " ", 2)

		err := lockMoTable(c, names[0], names[1], lock.LockMode_Shared)
		if err != nil {
			// if get error in locking mocatalog.mo_tables by it's dbName & tblName
			// that means the origin table's schema was changed. then return NeedRetryWithDefChanged err
			if moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetry) ||
				moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetryWithDefChanged) {
				return moerr.NewTxnNeedRetryWithDefChangedNoCtx()
			}

			// other errors, just throw  out
			return err
		}
	}
	return nil
}

func (c *Compile) lockTable() error {
	for _, tbl := range c.lockTables {
		typ := plan2.MakeTypeByPlan2Type(tbl.PrimaryColTyp)
		if len(tbl.PartitionTableIds) == 0 {
			return lockop.LockTable(
				c.e,
				c.proc,
				tbl.TableId,
				typ,
				false)
		}

		for _, tblId := range tbl.PartitionTableIds {
			err := lockop.LockTable(
				c.e,
				c.proc,
				tblId,
				typ,
				false)
			if err != nil {
				return err
			}
		}

	}
	return nil
}

// func (c *Compile) compileAttachedScope(ctx context.Context, attachedPlan *plan.Plan) ([]*Scope, error) {
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

func isAvailable(client morpc.RPCClient, addr string) bool {
	_, _, err := net.SplitHostPort(addr)
	if err != nil {
		logutil.Warnf("compileScope received a malformed cn address '%s', expected 'ip:port'", addr)
		return false
	}
	logutil.Debugf("ping %s start", addr)
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	err = client.Ping(ctx, addr)
	if err != nil {
		// ping failed
		logutil.Debugf("ping %s err %+v\n", addr, err)
		return false
	}
	return true
}

func (c *Compile) removeUnavailableCN() {
	client := cnclient.GetRPCClient()
	if client == nil {
		return
	}
	i := 0
	for _, cn := range c.cnList {
		if isSameCN(c.addr, cn.Addr) || isAvailable(client, cn.Addr) {
			c.cnList[i] = cn
			i++
		}
	}
	c.cnList = c.cnList[:i]
}

// getCNList gets the CN list from engine.Nodes() method. It will
// ensure the current CN is included in the result.
func (c *Compile) getCNList() (engine.Nodes, error) {
	cnList, err := c.e.Nodes(c.isInternal, c.tenant, c.uid, c.cnLabel)
	if err != nil {
		return nil, err
	}

	// We should always make sure the current CN is contained in the cn list.
	if c.proc == nil || c.proc.QueryClient == nil {
		return cnList, nil
	}
	cnID := c.proc.QueryClient.ServiceID()
	for _, node := range cnList {
		if node.Id == cnID {
			return cnList, nil
		}
	}
	n := getEngineNode(c)
	n.Id = cnID
	cnList = append(cnList, n)
	return cnList, nil
}

func (c *Compile) compileQuery(ctx context.Context, qry *plan.Query) ([]*Scope, error) {
	var err error

	start := time.Now()
	defer func() {
		v2.TxnStatementCompileQueryHistogram.Observe(time.Since(start).Seconds())
	}()

	c.execType = plan2.GetExecType(c.pn.GetQuery())
	n := getEngineNode(c)
	if c.execType == plan2.ExecTypeTP || c.execType == plan2.ExecTypeAP_ONECN {
		c.cnList = engine.Nodes{n}
	} else {
		c.cnList, err = c.getCNList()
		if err != nil {
			return nil, err
		}
		c.removeUnavailableCN()
		// sort by addr to get fixed order of CN list
		sort.Slice(c.cnList, func(i, j int) bool { return c.cnList[i].Addr < c.cnList[j].Addr })
	}

	c.initAnalyze(qry)

	// deal with sink scan first.
	for i := len(qry.Steps) - 1; i >= 0; i-- {
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
	for i := len(qry.Steps) - 1; i >= 0; i-- {
		var scopes []*Scope
		var scope *Scope
		scopes, err = c.compilePlanScope(ctx, int32(i), qry.Steps[i], qry.Nodes)
		if err != nil {
			return nil, err
		}
		scope, err = c.compileSteps(qry, scopes, qry.Steps[i])
		if err != nil {
			return nil, err
		}
		steps = append(steps, scope)
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
			var wr *process.WaitRegister
			if c.anal.qry.LoadTag {
				wr = &process.WaitRegister{
					Ctx: c.ctx,
					Ch:  make(chan *process.RegisterMessage, ncpu),
				}
			} else {
				wr = &process.WaitRegister{
					Ctx: c.ctx,
					Ch:  make(chan *process.RegisterMessage, 1),
				}
			}
			c.appendStepRegs(s, nodeId, wr)
		}
	}
	return nil
}

func (c *Compile) compileSteps(qry *plan.Query, ss []*Scope, step int32) (*Scope, error) {
	if qry.Nodes[step].NodeType == plan.Node_SINK {
		return ss[0], nil
	}
	var rs *Scope
	switch qry.StmtType {
	case plan.Query_DELETE:
		return ss[0], nil
	case plan.Query_INSERT:
		return ss[0], nil
	case plan.Query_UPDATE:
		return ss[0], nil
	default:
		if c.execType == plan2.ExecTypeTP {
			rs = ss[len(ss)-1]
		} else {
			rs = c.newMergeScope(ss)
		}
		updateScopesLastFlag([]*Scope{rs})
		c.setAnalyzeCurrent([]*Scope{rs}, c.anal.curr)
		rs.Instructions = append(rs.Instructions, vm.Instruction{
			Op: vm.Output,
			Arg: output.NewArgument().
				WithFunc(c.fill),
		})
	}
	return rs, nil
}

func constructValueScanBatch(ctx context.Context, proc *process.Process, node *plan.Node) (*batch.Batch, error) {
	var nodeId uuid.UUID
	var exprList []colexec.ExpressionExecutor

	if node == nil || node.TableDef == nil { // like : select 1, 2
		bat := batch.NewWithSize(1)
		bat.Vecs[0] = vector.NewConstNull(types.T_int64.ToType(), 1, proc.Mp())
		bat.SetRowCount(1)
		return bat, nil
	}
	// select * from (values row(1,1), row(2,2), row(3,3)) a;
	tableDef := node.TableDef
	colCount := len(tableDef.Cols)
	colsData := node.RowsetData.Cols
	copy(nodeId[:], node.Uuid)
	bat := proc.GetPrepareBatch()
	if bat == nil {
		bat = proc.GetValueScanBatch(nodeId)
		if bat == nil {
			return nil, moerr.NewInfo(ctx, fmt.Sprintf("constructValueScanBatch failed, node id: %s", nodeId.String()))
		}
	}
	params := proc.GetPrepareParams()
	if len(colsData) > 0 {
		exprs := proc.GetPrepareExprList()
		for i := 0; i < colCount; i++ {
			if exprs != nil {
				exprList = exprs.([][]colexec.ExpressionExecutor)[i]
			}
			if params != nil {
				vs := vector.MustFixedCol[types.Varlena](params)
				for _, row := range colsData[i].Data {
					if row.Pos >= 0 {
						isNull := params.GetNulls().Contains(uint64(row.Pos - 1))
						str := vs[row.Pos-1].UnsafeGetString(params.GetArea())
						if err := util.SetBytesToAnyVector(ctx, str, int(row.RowPos), isNull, bat.Vecs[i],
							proc); err != nil {
							return nil, err
						}
					}
				}
			}
			if err := evalRowsetData(proc, colsData[i].Data, bat.Vecs[i], exprList); err != nil {
				bat.Clean(proc.Mp())
				return nil, err
			}
		}
	}
	return bat, nil
}

func (c *Compile) compilePlanScope(ctx context.Context, step int32, curNodeIdx int32, ns []*plan.Node) ([]*Scope, error) {
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
	n := ns[curNodeIdx]
	switch n.NodeType {
	case plan.Node_VALUE_SCAN:
		ds := newScope(Normal)
		ds.DataSource = &Source{isConst: true, node: n}
		ds.NodeInfo = engine.Node{Addr: c.addr, Mcpu: 1}
		ds.Proc = process.NewWithAnalyze(c.proc, c.ctx, 0, c.anal.Nodes())
		ss = c.compileSort(n, c.compileProjection(n, []*Scope{ds}))
		return ss, nil
	case plan.Node_EXTERNAL_SCAN:
		if n.ObjRef != nil {
			c.appendMetaTables(n.ObjRef)
		}
		node := plan2.DeepCopyNode(n)
		ss, err = c.compileExternScan(ctx, node)
		if err != nil {
			return nil, err
		}
		ss = c.compileSort(n, c.compileProjection(n, c.compileRestrict(node, ss)))
		return ss, nil
	case plan.Node_TABLE_SCAN:
		c.appendMetaTables(n.ObjRef)
		ss, err = c.compileTableScan(n)
		if err != nil {
			return nil, err
		}
		ss = c.compileProjection(n, c.compileRestrict(n, ss))
		if n.Offset != nil {
			ss = c.compileOffset(n, ss)
		}
		if n.Limit != nil {
			ss = c.compileLimit(n, ss)
		}
		return ss, nil
	case plan.Node_SOURCE_SCAN:
		ss, err = c.compileSourceScan(ctx, n)
		if err != nil {
			return nil, err
		}
		ss = c.compileSort(n, c.compileProjection(n, c.compileRestrict(n, ss)))
		return ss, nil
	case plan.Node_FILTER, plan.Node_PROJECT, plan.Node_PRE_DELETE:
		curr := c.anal.curr
		c.setAnalyzeCurrent(nil, int(n.Children[0]))
		ss, err = c.compilePlanScope(ctx, step, n.Children[0], ns)
		if err != nil {
			return nil, err
		}
		c.setAnalyzeCurrent(ss, curr)
		ss = c.compileSort(n, c.compileProjection(n, c.compileRestrict(n, ss)))
		return ss, nil
	case plan.Node_AGG:
		curr := c.anal.curr
		c.setAnalyzeCurrent(nil, int(n.Children[0]))
		ss, err = c.compilePlanScope(ctx, step, n.Children[0], ns)
		if err != nil {
			return nil, err
		}
		c.setAnalyzeCurrent(ss, curr)

		groupInfo := constructGroup(c.ctx, n, ns[n.Children[0]], false, 0, c.proc)
		defer groupInfo.Release()
		anyDistinctAgg := groupInfo.AnyDistinctAgg()

		if c.execType == plan2.ExecTypeTP && ss[0].PartialResults == nil {
			ss = c.compileSort(n, c.compileProjection(n, c.compileRestrict(n, c.compileTPGroup(n, ss, ns))))
			return ss, nil
		} else if !anyDistinctAgg && n.Stats.HashmapStats != nil && n.Stats.HashmapStats.Shuffle {
			ss = c.compileSort(n, c.compileShuffleGroup(n, ss, ns))
			return ss, nil
		} else {
			ss = c.compileSort(n, c.compileProjection(n, c.compileRestrict(n, c.compileMergeGroup(n, ss, ns, anyDistinctAgg))))
			return ss, nil
		}
	case plan.Node_SAMPLE:
		curr := c.anal.curr
		c.setAnalyzeCurrent(nil, int(n.Children[0]))
		ss, err = c.compilePlanScope(ctx, step, n.Children[0], ns)
		if err != nil {
			return nil, err
		}
		c.setAnalyzeCurrent(ss, curr)

		ss = c.compileSort(n, c.compileProjection(n, c.compileRestrict(n, c.compileSample(n, ss))))
		return ss, nil
	case plan.Node_WINDOW:
		curr := c.anal.curr
		c.setAnalyzeCurrent(nil, int(n.Children[0]))
		ss, err = c.compilePlanScope(ctx, step, n.Children[0], ns)
		if err != nil {
			return nil, err
		}
		c.setAnalyzeCurrent(ss, curr)
		ss = c.compileSort(n, c.compileProjection(n, c.compileRestrict(n, c.compileWin(n, ss))))
		return ss, nil
	case plan.Node_TIME_WINDOW:
		curr := c.anal.curr
		c.setAnalyzeCurrent(nil, int(n.Children[0]))
		ss, err = c.compilePlanScope(ctx, step, n.Children[0], ns)
		if err != nil {
			return nil, err
		}
		c.setAnalyzeCurrent(ss, curr)
		ss = c.compileProjection(n, c.compileRestrict(n, c.compileTimeWin(n, c.compileSort(n, ss))))
		return ss, nil
	case plan.Node_FILL:
		curr := c.anal.curr
		c.setAnalyzeCurrent(nil, int(n.Children[0]))
		ss, err = c.compilePlanScope(ctx, step, n.Children[0], ns)
		if err != nil {
			return nil, err
		}
		c.setAnalyzeCurrent(ss, curr)
		ss = c.compileProjection(n, c.compileRestrict(n, c.compileFill(n, ss)))
		return ss, nil
	case plan.Node_JOIN:
		curr := c.anal.curr
		c.setAnalyzeCurrent(nil, int(n.Children[0]))
		left, err = c.compilePlanScope(ctx, step, n.Children[0], ns)
		if err != nil {
			return nil, err
		}
		c.setAnalyzeCurrent(left, int(n.Children[1]))
		right, err = c.compilePlanScope(ctx, step, n.Children[1], ns)
		if err != nil {
			return nil, err
		}
		c.setAnalyzeCurrent(right, curr)
		ss = c.compileSort(n, c.compileJoin(ctx, n, ns[n.Children[0]], ns[n.Children[1]], left, right))
		return ss, nil
	case plan.Node_SORT:
		curr := c.anal.curr
		c.setAnalyzeCurrent(nil, int(n.Children[0]))
		ss, err = c.compilePlanScope(ctx, step, n.Children[0], ns)
		if err != nil {
			return nil, err
		}
		c.setAnalyzeCurrent(ss, curr)
		ss = c.compileProjection(n, c.compileRestrict(n, c.compileSort(n, ss)))
		return ss, nil
	case plan.Node_PARTITION:
		curr := c.anal.curr
		c.setAnalyzeCurrent(nil, int(n.Children[0]))
		ss, err = c.compilePlanScope(ctx, step, n.Children[0], ns)
		if err != nil {
			return nil, err
		}
		c.setAnalyzeCurrent(ss, curr)
		ss = c.compileProjection(n, c.compileRestrict(n, c.compilePartition(n, ss)))
		return ss, nil
	case plan.Node_UNION:
		curr := c.anal.curr
		c.setAnalyzeCurrent(nil, int(n.Children[0]))
		left, err = c.compilePlanScope(ctx, step, n.Children[0], ns)
		if err != nil {
			return nil, err
		}
		c.setAnalyzeCurrent(left, int(n.Children[1]))
		right, err = c.compilePlanScope(ctx, step, n.Children[1], ns)
		if err != nil {
			return nil, err
		}
		c.setAnalyzeCurrent(right, curr)
		ss = c.compileSort(n, c.compileUnion(n, left, right))
		return ss, nil
	case plan.Node_MINUS, plan.Node_INTERSECT, plan.Node_INTERSECT_ALL:
		curr := c.anal.curr
		c.setAnalyzeCurrent(nil, int(n.Children[0]))
		left, err = c.compilePlanScope(ctx, step, n.Children[0], ns)
		if err != nil {
			return nil, err
		}
		c.setAnalyzeCurrent(left, int(n.Children[1]))
		right, err = c.compilePlanScope(ctx, step, n.Children[1], ns)
		if err != nil {
			return nil, err
		}
		c.setAnalyzeCurrent(right, curr)
		ss = c.compileSort(n, c.compileMinusAndIntersect(n, left, right, n.NodeType))
		return ss, nil
	case plan.Node_UNION_ALL:
		curr := c.anal.curr
		c.setAnalyzeCurrent(nil, int(n.Children[0]))

		left, err = c.compilePlanScope(ctx, step, n.Children[0], ns)
		if err != nil {
			return nil, err
		}
		c.setAnalyzeCurrent(left, int(n.Children[1]))
		right, err = c.compilePlanScope(ctx, step, n.Children[1], ns)
		if err != nil {
			return nil, err
		}
		c.setAnalyzeCurrent(right, curr)
		ss = c.compileSort(n, c.compileUnionAll(left, right))
		return ss, nil
	case plan.Node_DELETE:
		if n.DeleteCtx.CanTruncate {
			s := newScope(TruncateTable)
			s.Plan = &plan.Plan{
				Plan: &plan.Plan_Ddl{
					Ddl: &plan.DataDefinition{
						DdlType: plan.DataDefinition_TRUNCATE_TABLE,
						Definition: &plan.DataDefinition_TruncateTable{
							TruncateTable: n.DeleteCtx.TruncateTable,
						},
					},
				},
			}
			ss = []*Scope{s}
			return ss, nil
		}
		c.appendMetaTables(n.DeleteCtx.Ref)
		curr := c.anal.curr
		c.setAnalyzeCurrent(nil, int(n.Children[0]))

		ss, err = c.compilePlanScope(ctx, step, n.Children[0], ns)
		if err != nil {
			return nil, err
		}

		n.NotCacheable = true
		var arg *deletion.Argument
		arg, err = constructDeletion(n, c.e)
		if err != nil {
			return nil, err
		}

		if n.Stats.Cost*float64(SingleLineSizeEstimate) >
			float64(DistributedThreshold) &&
			!arg.DeleteCtx.CanTruncate {
			c.proc.Infof(c.ctx, "delete of '%s' write s3\n", c.sql)
			rs := c.newDeleteMergeScope(arg, ss)
			rs.Instructions = append(rs.Instructions, vm.Instruction{
				Op: vm.MergeDelete,
				Arg: mergedelete.NewArgument().
					WithObjectRef(arg.DeleteCtx.Ref).
					WithParitionNames(arg.DeleteCtx.PartitionTableNames).
					WithEngine(c.e).
					WithAddAffectedRows(arg.DeleteCtx.AddAffectedRows),
			})
			rs.Magic = MergeDelete
			ss = []*Scope{rs}
			arg.Release()
			return ss, nil
		}
		rs := c.newMergeScope(ss)
		// updateScopesLastFlag([]*Scope{rs})
		rs.Magic = Merge
		c.setAnalyzeCurrent([]*Scope{rs}, c.anal.curr)

		rs.Instructions = append(rs.Instructions, vm.Instruction{
			Op:  vm.Deletion,
			Arg: arg,
		})
		ss = []*Scope{rs}
		c.setAnalyzeCurrent(ss, curr)
		return ss, nil
	case plan.Node_ON_DUPLICATE_KEY:
		curr := c.anal.curr
		c.setAnalyzeCurrent(nil, int(n.Children[0]))
		ss, err = c.compilePlanScope(ctx, step, n.Children[0], ns)
		if err != nil {
			return nil, err
		}
		c.setAnalyzeCurrent(ss, curr)

		rs := c.newMergeScope(ss)
		rs.Instructions[0].Arg.Release()
		rs.Instructions[0] = vm.Instruction{
			Op:  vm.OnDuplicateKey,
			Idx: c.anal.curr,
			Arg: constructOnduplicateKey(n, c.e),
		}
		ss = []*Scope{rs}
		return ss, nil
	case plan.Node_FUZZY_FILTER:
		curr := c.anal.curr
		c.setAnalyzeCurrent(nil, int(n.Children[0]))
		left, err := c.compilePlanScope(ctx, step, n.Children[0], ns)
		if err != nil {
			return nil, err
		}
		c.setAnalyzeCurrent(left, int(n.Children[1]))
		right, err := c.compilePlanScope(ctx, step, n.Children[1], ns)
		if err != nil {
			return nil, err
		}
		c.setAnalyzeCurrent(right, curr)
		return c.compileFuzzyFilter(n, ns, left, right)
	case plan.Node_PRE_INSERT_UK, plan.Node_PRE_INSERT_SK:
		curr := c.anal.curr
		ss, err = c.compilePlanScope(ctx, step, n.Children[0], ns)
		if err != nil {
			return nil, err
		}
		currentFirstFlag := c.anal.isFirst
		for i := range ss {
			if n.NodeType == plan.Node_PRE_INSERT_UK {
				var preInsertUkArg *preinsertunique.Argument
				preInsertUkArg, err = constructPreInsertUk(n, c.proc)
				if err != nil {
					return nil, err
				}
				ss[i].appendInstruction(vm.Instruction{
					Op:      vm.PreInsertUnique,
					Idx:     c.anal.curr,
					IsFirst: currentFirstFlag,
					Arg:     preInsertUkArg,
				})
			} else {
				var preInsertSkArg *preinsertsecondaryindex.Argument
				preInsertSkArg, err = constructPreInsertSk(n, c.proc)
				if err != nil {
					return nil, err
				}
				ss[i].appendInstruction(vm.Instruction{
					Op:      vm.PreInsertSecondaryIndex,
					Idx:     c.anal.curr,
					IsFirst: currentFirstFlag,
					Arg:     preInsertSkArg,
				})
			}
		}
		c.setAnalyzeCurrent(ss, curr)
		return ss, nil
	case plan.Node_PRE_INSERT:
		curr := c.anal.curr
		ss, err = c.compilePlanScope(ctx, step, n.Children[0], ns)
		if err != nil {
			return nil, err
		}
		currentFirstFlag := c.anal.isFirst
		for i := range ss {
			var preInsertArg *preinsert.Argument
			preInsertArg, err = constructPreInsert(ns, n, c.e, c.proc)
			if err != nil {
				return nil, err
			}
			ss[i].appendInstruction(vm.Instruction{
				Op:      vm.PreInsert,
				Idx:     c.anal.curr,
				IsFirst: currentFirstFlag,
				Arg:     preInsertArg,
			})
		}
		c.setAnalyzeCurrent(ss, curr)
		return ss, nil
	case plan.Node_INSERT:
		c.appendMetaTables(n.ObjRef)
		curr := c.anal.curr
		n.NotCacheable = true
		ss, err = c.compilePlanScope(ctx, step, n.Children[0], ns)
		if err != nil {
			return nil, err
		}

		currentFirstFlag := c.anal.isFirst
		toWriteS3 := n.Stats.GetCost()*float64(SingleLineSizeEstimate) >
			float64(DistributedThreshold) || c.anal.qry.LoadTag

		if toWriteS3 {
			c.proc.Debugf(c.ctx, "insert of '%s' write s3\n", c.sql)
			if !haveSinkScanInPlan(ns, n.Children[0]) && len(ss) != 1 {
				var insertArg *insert.Argument
				insertArg, err = constructInsert(n, c.e)
				if err != nil {
					return nil, err
				}
				insertArg.ToWriteS3 = true
				rs := c.newInsertMergeScope(insertArg, ss)
				rs.Magic = MergeInsert
				rs.Instructions = append(rs.Instructions, vm.Instruction{
					Op: vm.MergeBlock,
					Arg: mergeblock.NewArgument().
						WithEngine(c.e).
						WithObjectRef(insertArg.InsertCtx.Ref).
						WithParitionNames(insertArg.InsertCtx.PartitionTableNames).
						WithAddAffectedRows(insertArg.InsertCtx.AddAffectedRows),
				})
				ss = []*Scope{rs}
				insertArg.Release()
			} else {
				dataScope := c.newMergeScope(ss)
				dataScope.IsEnd = true
				if c.anal.qry.LoadTag {
					dataScope.Proc.Reg.MergeReceivers[0].Ch = make(chan *process.RegisterMessage, dataScope.NodeInfo.Mcpu) // reset the channel buffer of sink for load
				}
				parallelSize := c.getParallelSizeForExternalScan(n, dataScope.NodeInfo.Mcpu)
				scopes := make([]*Scope, 0, parallelSize)
				regs := make([]*process.WaitRegister, 0, parallelSize)
				for i := 0; i < parallelSize; i++ {
					s := newScope(Merge)
					s.Instructions = []vm.Instruction{{Op: vm.Merge, Arg: merge.NewArgument()}}
					scopes = append(scopes, s)
					scopes[i].Proc = process.NewFromProc(c.proc, c.ctx, 1)
					if c.anal.qry.LoadTag {
						for _, rr := range scopes[i].Proc.Reg.MergeReceivers {
							rr.Ch = make(chan *process.RegisterMessage, shuffleChannelBufferSize)
						}
					}
					regs = append(regs, scopes[i].Proc.Reg.MergeReceivers...)
				}

				if c.anal.qry.LoadTag && n.Stats.HashmapStats != nil && n.Stats.HashmapStats.Shuffle && dataScope.NodeInfo.Mcpu == parallelSize {
					_, arg := constructDispatchLocalAndRemote(0, scopes, c.addr)
					arg.FuncId = dispatch.ShuffleToAllFunc
					arg.ShuffleType = plan2.ShuffleToLocalMatchedReg
					dataScope.Instructions = append(dataScope.Instructions, vm.Instruction{
						Op:  vm.Dispatch,
						Arg: arg,
					})
				} else {
					dataScope.Instructions = append(dataScope.Instructions, vm.Instruction{
						Op:  vm.Dispatch,
						Arg: constructDispatchLocal(false, false, false, regs),
					})
				}
				for i := range scopes {
					var insertArg *insert.Argument
					insertArg, err = constructInsert(n, c.e)
					if err != nil {
						return nil, err
					}
					insertArg.ToWriteS3 = true
					scopes[i].appendInstruction(vm.Instruction{
						Op:      vm.Insert,
						Idx:     c.anal.curr,
						IsFirst: currentFirstFlag,
						Arg:     insertArg,
					})
				}

				var insertArg *insert.Argument
				insertArg, err = constructInsert(n, c.e)
				if err != nil {
					return nil, err
				}
				insertArg.ToWriteS3 = true
				rs := c.newMergeScope(scopes)
				rs.PreScopes = append(rs.PreScopes, dataScope)
				rs.Magic = MergeInsert
				rs.Instructions = append(rs.Instructions, vm.Instruction{
					Op: vm.MergeBlock,
					Arg: mergeblock.NewArgument().
						WithEngine(c.e).
						WithObjectRef(insertArg.InsertCtx.Ref).
						WithParitionNames(insertArg.InsertCtx.PartitionTableNames).
						WithAddAffectedRows(insertArg.InsertCtx.AddAffectedRows),
				})
				ss = []*Scope{rs}
				insertArg.Release()
			}
		} else {
			for i := range ss {
				var insertArg *insert.Argument
				insertArg, err = constructInsert(n, c.e)
				if err != nil {
					return nil, err
				}
				ss[i].appendInstruction(vm.Instruction{
					Op:      vm.Insert,
					Idx:     c.anal.curr,
					IsFirst: currentFirstFlag,
					Arg:     insertArg,
				})
			}
		}
		c.setAnalyzeCurrent(ss, curr)
		return ss, nil
	case plan.Node_LOCK_OP:
		curr := c.anal.curr
		ss, err = c.compilePlanScope(ctx, step, n.Children[0], ns)
		if err != nil {
			return nil, err
		}

		lockRows := make([]*plan.LockTarget, 0, len(n.LockTargets))
		for _, tbl := range n.LockTargets {
			if tbl.LockTable {
				c.lockTables[tbl.TableId] = tbl
			} else {
				if _, ok := c.lockTables[tbl.TableId]; !ok {
					lockRows = append(lockRows, tbl)
				}
			}
		}
		n.LockTargets = lockRows
		if len(n.LockTargets) == 0 {
			return ss, nil
		}

		block := false
		// only pessimistic txn needs to block downstream operators.
		if c.proc.TxnOperator.Txn().IsPessimistic() {
			block = n.LockTargets[0].Block
			if block {
				ss = []*Scope{c.newMergeScope(ss)}
			}
		}
		currentFirstFlag := c.anal.isFirst
		for i := range ss {
			var lockOpArg *lockop.Argument
			lockOpArg, err = constructLockOp(n, c.e)
			if err != nil {
				return nil, err
			}
			lockOpArg.SetBlock(block)
			if block {
				ss[i].Instructions[len(ss[i].Instructions)-1].Arg.Release()
				ss[i].Instructions[len(ss[i].Instructions)-1] = vm.Instruction{
					Op:      vm.LockOp,
					Idx:     c.anal.curr,
					IsFirst: currentFirstFlag,
					Arg:     lockOpArg,
				}
			} else {
				ss[i].appendInstruction(vm.Instruction{
					Op:      vm.LockOp,
					Idx:     c.anal.curr,
					IsFirst: currentFirstFlag,
					Arg:     lockOpArg,
				})
			}
		}
		ss = c.compileProjection(n, ss)
		c.setAnalyzeCurrent(ss, curr)
		return ss, nil
	case plan.Node_FUNCTION_SCAN:
		curr := c.anal.curr
		c.setAnalyzeCurrent(nil, int(n.Children[0]))
		ss, err = c.compilePlanScope(ctx, step, n.Children[0], ns)
		if err != nil {
			return nil, err
		}
		c.setAnalyzeCurrent(ss, curr)
		ss = c.compileSort(n, c.compileProjection(n, c.compileRestrict(n, c.compileTableFunction(n, ss))))
		return ss, nil
	case plan.Node_SINK_SCAN:
		receivers := make([]*process.WaitRegister, len(n.SourceStep))
		for i, step := range n.SourceStep {
			receivers[i] = c.getNodeReg(step, curNodeIdx)
			if receivers[i] == nil {
				return nil, moerr.NewInternalError(c.ctx, "no data sender for sinkScan node")
			}
		}
		rs := newScope(Merge)
		rs.NodeInfo = getEngineNode(c)
		rs.Proc = process.NewWithAnalyze(c.proc, c.ctx, 1, c.anal.Nodes())
		rs.Instructions = []vm.Instruction{{Op: vm.Merge, Arg: merge.NewArgument().WithSinkScan(true)}}
		for _, r := range receivers {
			r.Ctx = rs.Proc.Ctx
		}
		rs.Proc.Reg.MergeReceivers = receivers
		ss = c.compileProjection(n, []*Scope{rs})
		return ss, nil
	case plan.Node_RECURSIVE_SCAN:
		receivers := make([]*process.WaitRegister, len(n.SourceStep))
		for i, step := range n.SourceStep {
			receivers[i] = c.getNodeReg(step, curNodeIdx)
			if receivers[i] == nil {
				return nil, moerr.NewInternalError(c.ctx, "no data sender for sinkScan node")
			}
		}
		rs := newScope(Merge)
		rs.NodeInfo = engine.Node{Addr: c.addr, Mcpu: 1}
		rs.Proc = process.NewWithAnalyze(c.proc, c.ctx, len(receivers), c.anal.Nodes())
		rs.Instructions = []vm.Instruction{{Op: vm.MergeRecursive, Arg: mergerecursive.NewArgument()}}

		for _, r := range receivers {
			r.Ctx = rs.Proc.Ctx
		}
		rs.Proc.Reg.MergeReceivers = receivers
		ss = []*Scope{rs}
		return ss, nil
	case plan.Node_RECURSIVE_CTE:
		receivers := make([]*process.WaitRegister, len(n.SourceStep))
		for i, step := range n.SourceStep {
			receivers[i] = c.getNodeReg(step, curNodeIdx)
			if receivers[i] == nil {
				return nil, moerr.NewInternalError(c.ctx, "no data sender for sinkScan node")
			}
		}
		rs := newScope(Merge)
		rs.NodeInfo = getEngineNode(c)
		rs.Proc = process.NewWithAnalyze(c.proc, c.ctx, len(receivers), c.anal.Nodes())
		rs.Instructions = []vm.Instruction{{Op: vm.MergeCTE, Arg: mergecte.NewArgument()}}

		for _, r := range receivers {
			r.Ctx = rs.Proc.Ctx
		}
		rs.Proc.Reg.MergeReceivers = receivers
		ss = c.compileSort(n, []*Scope{rs})
		return ss, nil
	case plan.Node_SINK:
		receivers := c.getStepRegs(step)
		if len(receivers) == 0 {
			return nil, moerr.NewInternalError(c.ctx, "no data receiver for sink node")
		}
		ss, err = c.compilePlanScope(ctx, step, n.Children[0], ns)
		if err != nil {
			return nil, err
		}
		rs := c.newMergeScope(ss)
		rs.appendInstruction(vm.Instruction{
			Op:  vm.Dispatch,
			Arg: constructDispatchLocal(true, true, n.RecursiveSink, receivers),
		})
		ss = []*Scope{rs}
		return ss, nil
	default:
		return nil, moerr.NewNYI(ctx, fmt.Sprintf("query '%s'", n))
	}
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

func (c *Compile) constructScopeForExternal(addr string, parallel bool) *Scope {
	ds := newScope(Normal)
	if parallel {
		ds.Magic = Remote
	}
	ds.NodeInfo = getEngineNode(c)
	ds.NodeInfo.Addr = addr
	ds.Proc = process.NewWithAnalyze(c.proc, c.ctx, 0, c.anal.Nodes())
	c.proc.LoadTag = c.anal.qry.LoadTag
	ds.Proc.LoadTag = true
	ds.DataSource = &Source{isConst: true}
	return ds
}

func (c *Compile) constructLoadMergeScope() *Scope {
	ds := newScope(Merge)
	ds.Proc = process.NewWithAnalyze(c.proc, c.ctx, 1, c.anal.Nodes())
	ds.Proc.LoadTag = true
	ds.appendInstruction(vm.Instruction{
		Op:      vm.Merge,
		Idx:     c.anal.curr,
		IsFirst: c.anal.isFirst,
		Arg:     merge.NewArgument(),
	})
	return ds
}

func (c *Compile) compileSourceScan(ctx context.Context, n *plan.Node) ([]*Scope, error) {
	_, span := trace.Start(ctx, "compileSourceScan")
	defer span.End()
	configs := make(map[string]interface{})
	for _, def := range n.TableDef.Defs {
		switch v := def.Def.(type) {
		case *plan.TableDef_DefType_Properties:
			for _, p := range v.Properties.Properties {
				configs[p.Key] = p.Value
			}
		}
	}

	end, err := mokafka.GetStreamCurrentSize(ctx, configs, mokafka.NewKafkaAdapter)
	if err != nil {
		return nil, err
	}
	ps := calculatePartitions(0, end, int64(ncpu))

	ss := make([]*Scope, len(ps))
	for i := range ss {
		ss[i] = newScope(Normal)
		ss[i].NodeInfo = getEngineNode(c)
		ss[i].Proc = process.NewWithAnalyze(c.proc, c.ctx, 0, c.anal.Nodes())
		ss[i].appendInstruction(vm.Instruction{
			Op:      vm.Source,
			Idx:     c.anal.curr,
			IsFirst: c.anal.isFirst,
			Arg:     constructStream(n, ps[i]),
		})
	}
	return ss, nil
}

const StreamMaxInterval = 8192

func calculatePartitions(start, end, n int64) [][2]int64 {
	var ps [][2]int64
	interval := (end - start) / n
	if interval < StreamMaxInterval {
		interval = StreamMaxInterval
	}
	var r int64
	l := start
	for i := int64(0); i < n; i++ {
		r = l + interval
		if r >= end {
			ps = append(ps, [2]int64{l, end})
			break
		}
		ps = append(ps, [2]int64{l, r})
		l = r
	}
	return ps
}

func (c *Compile) compileExternScan(ctx context.Context, n *plan.Node) ([]*Scope, error) {
	ctx, span := trace.Start(ctx, "compileExternScan")
	defer span.End()
	start := time.Now()
	defer func() {
		if t := time.Since(start); t > time.Second {
			c.proc.Infof(ctx, "compileExternScan cost %v", t)
		}
	}()

	t := time.Now()

	if time.Since(t) > time.Second {
		c.proc.Infof(ctx, "lock table %s.%s cost %v", n.ObjRef.SchemaName, n.ObjRef.ObjName, time.Since(t))
	}
	ID2Addr := make(map[int]int, 0)
	mcpu := 0
	for i := 0; i < len(c.cnList); i++ {
		tmp := mcpu
		mcpu += c.cnList[i].Mcpu
		ID2Addr[i] = mcpu - tmp
	}
	param := &tree.ExternParam{}
	if n.ExternScan == nil || n.ExternScan.Type != tree.INLINE {
		err := json.Unmarshal([]byte(n.TableDef.Createsql), param)
		if err != nil {
			return nil, err
		}
	} else {
		param.ScanType = int(n.ExternScan.Type)
		param.Data = n.ExternScan.Data
		param.Format = n.ExternScan.Format
		param.Tail = new(tree.TailParameter)
		param.Tail.IgnoredLines = n.ExternScan.IgnoredLines
		param.Tail.Fields = &tree.Fields{
			Terminated: &tree.Terminated{
				Value: n.ExternScan.Terminated,
			},
			EnclosedBy: &tree.EnclosedBy{
				Value: n.ExternScan.EnclosedBy[0],
			},
			EscapedBy: &tree.EscapedBy{
				Value: n.ExternScan.EscapedBy[0],
			},
		}
		param.JsonData = n.ExternScan.JsonType
	}
	if param.ScanType == tree.S3 {
		if !param.Init {
			if err := plan2.InitS3Param(param); err != nil {
				return nil, err
			}
		}
		if param.Parallel {
			mcpu = 0
			ID2Addr = make(map[int]int, 0)
			for i := 0; i < len(c.cnList); i++ {
				tmp := mcpu
				if c.cnList[i].Mcpu > external.S3ParallelMaxnum {
					mcpu += external.S3ParallelMaxnum
				} else {
					mcpu += c.cnList[i].Mcpu
				}
				ID2Addr[i] = mcpu - tmp
			}
		}
	} else if param.ScanType == tree.INLINE {
		return c.compileExternValueScan(n, param)
	} else {
		if err := plan2.InitInfileParam(param); err != nil {
			return nil, err
		}
	}

	t = time.Now()
	param.FileService = c.proc.FileService
	param.Ctx = c.ctx
	var err error
	var fileList []string
	var fileSize []int64
	if !param.Local && !param.Init {
		if param.QueryResult {
			fileList = strings.Split(param.Filepath, ",")
			for i := range fileList {
				fileList[i] = strings.TrimSpace(fileList[i])
			}
		} else {
			_, spanReadDir := trace.Start(ctx, "compileExternScan.ReadDir")
			fileList, fileSize, err = plan2.ReadDir(param)
			if err != nil {
				spanReadDir.End()
				return nil, err
			}
			spanReadDir.End()
		}
		fileList, fileSize, err = external.FilterFileList(ctx, n, c.proc, fileList, fileSize)
		if err != nil {
			return nil, err
		}
		if param.LoadFile && len(fileList) == 0 {
			return nil, moerr.NewInvalidInput(ctx, "the file does not exist in load flow")
		}
	} else {
		fileList = []string{param.Filepath}
		fileSize = []int64{param.FileSize}
	}
	if time.Since(t) > time.Second {
		c.proc.Infof(ctx, "read dir cost %v", time.Since(t))
	}

	if len(fileList) == 0 {
		ret := newScope(Normal)
		ret.DataSource = nil
		ret.Proc = process.NewWithAnalyze(c.proc, c.ctx, 0, c.anal.Nodes())

		return []*Scope{ret}, nil
	}
	if param.Parallel && (external.GetCompressType(param, fileList[0]) != tree.NOCOMPRESS || param.Local) {
		return c.compileExternScanParallel(n, param, fileList, fileSize)
	}

	t = time.Now()
	var fileOffset [][]int64
	if param.Parallel {
		if param.Strict {
			visibleCols := make([]*plan.ColDef, 0)
			for _, col := range n.TableDef.Cols {
				if !col.Hidden {
					visibleCols = append(visibleCols, col)
				}
			}
			for i := 0; i < len(fileList); i++ {
				param.Filepath = fileList[i]
				arr, err := external.ReadFileOffsetStrict(param, mcpu, fileSize[i], visibleCols)
				fileOffset = append(fileOffset, arr)
				if err != nil {
					return nil, err
				}
			}
		} else {
			for i := 0; i < len(fileList); i++ {
				param.Filepath = fileList[i]
				arr, err := external.ReadFileOffsetNoStrict(param, mcpu, fileSize[i])
				fileOffset = append(fileOffset, arr)
				if err != nil {
					return nil, err
				}
			}
		}

	} else {
		for i := 0; i < len(fileList); i++ {
			param.Filepath = fileList[i]
		}
	}

	if time.Since(t) > time.Second {
		c.proc.Infof(ctx, "read file offset cost %v", time.Since(t))
	}
	ss := make([]*Scope, 1)
	if param.Parallel {
		ss = make([]*Scope, len(c.cnList))
	}
	pre := 0
	for i := range ss {
		ss[i] = c.constructScopeForExternal(c.cnList[i].Addr, param.Parallel)
		ss[i].IsLoad = true
		count := ID2Addr[i]
		fileOffsetTmp := make([]*pipeline.FileOffset, len(fileList))
		for j := range fileOffsetTmp {
			preIndex := pre
			fileOffsetTmp[j] = &pipeline.FileOffset{}
			fileOffsetTmp[j].Offset = make([]int64, 0)
			if param.Parallel {
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
			} else {
				fileOffsetTmp[j].Offset = append(fileOffsetTmp[j].Offset, []int64{0, -1}...)
			}
		}
		ss[i].appendInstruction(vm.Instruction{
			Op:      vm.External,
			Idx:     c.anal.curr,
			IsFirst: c.anal.isFirst,
			Arg:     constructExternal(n, param, c.ctx, fileList, fileSize, fileOffsetTmp),
		})
		pre += count
	}

	return ss, nil
}

func (c *Compile) getParallelSizeForExternalScan(n *plan.Node, cpuNum int) int {
	if n.Stats == nil {
		return cpuNum
	}
	totalSize := n.Stats.Cost * n.Stats.Rowsize
	parallelSize := int(totalSize / float64(colexec.WriteS3Threshold))
	if parallelSize < 1 {
		return 1
	} else if parallelSize < cpuNum {
		return parallelSize
	}
	return cpuNum
}

func (c *Compile) compileExternValueScan(n *plan.Node, param *tree.ExternParam) ([]*Scope, error) {
	parallelSize := c.getParallelSizeForExternalScan(n, ncpu)
	ss := make([]*Scope, parallelSize)
	for i := 0; i < parallelSize; i++ {
		ss[i] = c.constructLoadMergeScope()
	}
	s := c.constructScopeForExternal(c.addr, false)
	s.appendInstruction(vm.Instruction{
		Op:      vm.External,
		Idx:     c.anal.curr,
		IsFirst: c.anal.isFirst,
		Arg:     constructExternal(n, param, c.ctx, nil, nil, nil),
	})
	_, arg := constructDispatchLocalAndRemote(0, ss, c.addr)
	arg.FuncId = dispatch.SendToAnyLocalFunc
	s.appendInstruction(vm.Instruction{
		Op:  vm.Dispatch,
		Arg: arg,
	})
	ss[0].PreScopes = append(ss[0].PreScopes, s)
	c.anal.isFirst = false
	return ss, nil
}

// construct one thread to read the file data, then dispatch to mcpu thread to get the filedata for insert
func (c *Compile) compileExternScanParallel(n *plan.Node, param *tree.ExternParam, fileList []string, fileSize []int64) ([]*Scope, error) {
	param.Parallel = false
	mcpu := c.cnList[0].Mcpu
	ss := make([]*Scope, mcpu)
	for i := 0; i < mcpu; i++ {
		ss[i] = c.constructLoadMergeScope()
	}
	fileOffsetTmp := make([]*pipeline.FileOffset, len(fileList))
	for i := 0; i < len(fileList); i++ {
		fileOffsetTmp[i] = &pipeline.FileOffset{}
		fileOffsetTmp[i].Offset = make([]int64, 0)
		fileOffsetTmp[i].Offset = append(fileOffsetTmp[i].Offset, []int64{0, -1}...)
	}
	extern := constructExternal(n, param, c.ctx, fileList, fileSize, fileOffsetTmp)
	extern.Es.ParallelLoad = true
	scope := c.constructScopeForExternal("", false)
	scope.appendInstruction(vm.Instruction{
		Op:      vm.External,
		Idx:     c.anal.curr,
		IsFirst: c.anal.isFirst,
		Arg:     extern,
	})
	_, arg := constructDispatchLocalAndRemote(0, ss, c.addr)
	arg.FuncId = dispatch.SendToAnyLocalFunc
	scope.appendInstruction(vm.Instruction{
		Op:  vm.Dispatch,
		Arg: arg,
	})
	ss[0].PreScopes = append(ss[0].PreScopes, scope)
	c.anal.isFirst = false
	return ss, nil
}

func (c *Compile) compileTableFunction(n *plan.Node, ss []*Scope) []*Scope {
	currentFirstFlag := c.anal.isFirst
	for i := range ss {
		ss[i].appendInstruction(vm.Instruction{
			Op:      vm.TableFunction,
			Idx:     c.anal.curr,
			IsFirst: currentFirstFlag,
			Arg:     constructTableFunction(n),
		})
	}
	c.anal.isFirst = false

	return ss
}

func (c *Compile) compileTableScan(n *plan.Node) ([]*Scope, error) {
	nodes, partialResults, partialResultTypes, err := c.generateNodes(n)
	if err != nil {
		return nil, err
	}
	ss := make([]*Scope, 0, len(nodes))

	for i := range nodes {
		s, err := c.compileTableScanWithNode(n, nodes[i])
		if err != nil {
			return nil, err
		}
		ss = append(ss, s)
	}
	ss[0].PartialResults = partialResults
	ss[0].PartialResultTypes = partialResultTypes
	return ss, nil
}

func (c *Compile) compileTableScanWithNode(n *plan.Node, node engine.Node) (*Scope, error) {
	s := newScope(Remote)
	s.NodeInfo = node
	s.TxnOffset = c.TxnOffset
	s.DataSource = &Source{
		node: n,
	}
	s.Proc = process.NewWithAnalyze(c.proc, c.ctx, 0, c.anal.Nodes())
	return s, nil
}

func (c *Compile) compileTableScanDataSource(s *Scope) error {
	var err error
	var tblDef *plan.TableDef
	var ts timestamp.Timestamp
	var db engine.Database
	var rel engine.Relation
	var txnOp client.TxnOperator

	n := s.DataSource.node
	attrs := make([]string, len(n.TableDef.Cols))
	for j, col := range n.TableDef.Cols {
		attrs[j] = col.Name
	}

	//-----------------------------------------------------------------------------------------------------
	ctx := c.ctx
	txnOp = c.proc.TxnOperator
	err = disttae.CheckTxnIsValid(txnOp)
	if err != nil {
		return err
	}
	if n.ScanSnapshot != nil && n.ScanSnapshot.TS != nil {
		if !n.ScanSnapshot.TS.Equal(timestamp.Timestamp{LogicalTime: 0, PhysicalTime: 0}) &&
			n.ScanSnapshot.TS.Less(c.proc.TxnOperator.Txn().SnapshotTS) {
			txnOp = c.proc.TxnOperator.CloneSnapshotOp(*n.ScanSnapshot.TS)

			if n.ScanSnapshot.Tenant != nil {
				ctx = context.WithValue(ctx, defines.TenantIDKey{}, n.ScanSnapshot.Tenant.TenantID)
			}
		}
	}
	//-----------------------------------------------------------------------------------------------------

	if c.proc != nil && c.proc.TxnOperator != nil {
		ts = txnOp.Txn().SnapshotTS
	}
	{
		err = disttae.CheckTxnIsValid(txnOp)
		if err != nil {
			return err
		}
		if util.TableIsClusterTable(n.TableDef.GetTableType()) {
			ctx = defines.AttachAccountId(ctx, catalog.System_Account)
		}
		if n.ObjRef.PubInfo != nil {
			ctx = defines.AttachAccountId(ctx, uint32(n.ObjRef.PubInfo.TenantId))
		}
		db, err = c.e.Database(ctx, n.ObjRef.SchemaName, txnOp)
		if err != nil {
			panic(err)
		}
		rel, err = db.Relation(ctx, n.TableDef.Name, c.proc)
		if err != nil {
			if txnOp.IsSnapOp() {
				return err
			}
			var e error // avoid contamination of error messages
			db, e = c.e.Database(c.ctx, defines.TEMPORARY_DBNAME, txnOp)
			if e != nil {
				panic(e)
			}
			rel, e = db.Relation(c.ctx, engine.GetTempTableName(n.ObjRef.SchemaName, n.TableDef.Name), c.proc)
			if e != nil {
				panic(e)
			}
		}
		tblDef = rel.GetTableDef(ctx)
	}

	// prcoess partitioned table
	var partitionRelNames []string
	if n.TableDef.Partition != nil {
		if n.PartitionPrune != nil && n.PartitionPrune.IsPruned {
			for _, partition := range n.PartitionPrune.SelectedPartitions {
				partitionRelNames = append(partitionRelNames, partition.PartitionTableName)
			}
		} else {
			partitionRelNames = append(partitionRelNames, n.TableDef.Partition.PartitionTableNames...)
		}
	}

	var filterExpr *plan.Expr
	if len(n.FilterList) > 0 {
		filterExpr = colexec.RewriteFilterExprList(n.FilterList)
		filterExpr, err = plan2.ConstantFold(batch.EmptyForConstFoldBatch, plan2.DeepCopyExpr(filterExpr), c.proc, true)
		if err != nil {
			return err
		}
	}

	s.DataSource.Timestamp = ts
	s.DataSource.Attributes = attrs
	s.DataSource.TableDef = tblDef
	s.DataSource.Rel = rel
	s.DataSource.RelationName = n.TableDef.Name
	s.DataSource.PartitionRelationNames = partitionRelNames
	s.DataSource.SchemaName = n.ObjRef.SchemaName
	s.DataSource.AccountId = n.ObjRef.GetPubInfo()
	s.DataSource.FilterExpr = filterExpr
	s.DataSource.RuntimeFilterSpecs = n.RuntimeFilterProbeList
	s.DataSource.OrderBy = n.OrderBy

	return nil
}

func (c *Compile) compileRestrict(n *plan.Node, ss []*Scope) []*Scope {
	if len(n.FilterList) == 0 && len(n.RuntimeFilterProbeList) == 0 {
		return ss
	}
	currentFirstFlag := c.anal.isFirst
	// for dynamic parameter, substitute param ref and const fold cast expression here to improve performance
	newFilters, err := plan2.ConstandFoldList(n.FilterList, c.proc, true)
	if err != nil {
		newFilters = n.FilterList
	}
	filterExpr := colexec.RewriteFilterExprList(newFilters)
	for i := range ss {
		ss[i].appendInstruction(vm.Instruction{
			Op:      vm.Filter,
			Idx:     c.anal.curr,
			IsFirst: currentFirstFlag,
			Arg:     constructRestrict(n, filterExpr),
		})
	}
	c.anal.isFirst = false
	return ss
}

func (c *Compile) compileProjection(n *plan.Node, ss []*Scope) []*Scope {
	if len(n.ProjectList) == 0 {
		return ss
	}
	currentFirstFlag := c.anal.isFirst
	for i := range ss {
		ss[i].appendInstruction(vm.Instruction{
			Op:      vm.Projection,
			Idx:     c.anal.curr,
			IsFirst: currentFirstFlag,
			Arg:     constructProjection(n),
		})
	}
	c.anal.isFirst = false
	return ss
}

func (c *Compile) compileUnion(n *plan.Node, ss []*Scope, children []*Scope) []*Scope {
	ss = append(ss, children...)
	rs := c.newScopeListOnCurrentCN(1, int(n.Stats.BlockNum))
	gn := new(plan.Node)
	gn.GroupBy = make([]*plan.Expr, len(n.ProjectList))
	for i := range gn.GroupBy {
		gn.GroupBy[i] = plan2.DeepCopyExpr(n.ProjectList[i])
		gn.GroupBy[i].Typ.NotNullable = false
	}
	idx := 0
	for i := range rs {
		rs[i].Instructions = append(rs[i].Instructions, vm.Instruction{
			Op:  vm.Group,
			Idx: c.anal.curr,
			Arg: constructGroup(c.ctx, gn, n, true, 0, c.proc),
		})
		if isSameCN(rs[i].NodeInfo.Addr, c.addr) {
			idx = i
		}
	}
	mergeChildren := c.newMergeScope(ss)
	mergeChildren.appendInstruction(vm.Instruction{
		Op:  vm.Dispatch,
		Arg: constructDispatch(0, rs, c.addr, n, false),
	})
	rs[idx].PreScopes = append(rs[idx].PreScopes, mergeChildren)
	return rs
}

func (c *Compile) compileMinusAndIntersect(n *plan.Node, ss []*Scope, children []*Scope, nodeType plan.Node_NodeType) []*Scope {
	rs := c.newJoinScopeListWithBucket(c.newScopeListOnCurrentCN(2, int(n.Stats.BlockNum)), ss, children, n)
	switch nodeType {
	case plan.Node_MINUS:
		for i := range rs {
			rs[i].Instructions[0].Arg.Release()
			rs[i].Instructions[0] = vm.Instruction{
				Op:  vm.Minus,
				Idx: c.anal.curr,
				Arg: minus.NewArgument(),
			}
		}
	case plan.Node_INTERSECT:
		for i := range rs {
			rs[i].Instructions[0].Arg.Release()
			rs[i].Instructions[0] = vm.Instruction{
				Op:  vm.Intersect,
				Idx: c.anal.curr,
				Arg: intersect.NewArgument(),
			}
		}
	case plan.Node_INTERSECT_ALL:
		for i := range rs {
			rs[i].Instructions[0].Arg.Release()
			rs[i].Instructions[0] = vm.Instruction{
				Op:  vm.IntersectAll,
				Idx: c.anal.curr,
				Arg: intersectall.NewArgument(),
			}
		}
	}
	return rs
}

func (c *Compile) compileUnionAll(ss []*Scope, children []*Scope) []*Scope {
	rs := c.newMergeScope(append(ss, children...))
	rs.Instructions[0].Idx = c.anal.curr
	return []*Scope{rs}
}

func (c *Compile) compileJoin(ctx context.Context, node, left, right *plan.Node, probeScopes []*Scope, buildScopes []*Scope) []*Scope {
	if node.Stats.HashmapStats.Shuffle {
		return c.compileShuffleJoin(ctx, node, left, right, probeScopes, buildScopes)
	}
	rs := c.compileBroadcastJoin(ctx, node, left, right, probeScopes, buildScopes)
	if c.execType == plan2.ExecTypeTP {
		//construct join build operator for tp join
		buildScopes[0].appendInstruction(constructJoinBuildInstruction(c, rs[0].Instructions[0], false, false))
		rs[0].Proc.Reg.MergeReceivers[1] = &process.WaitRegister{
			Ctx: rs[0].Proc.Ctx,
			Ch:  make(chan *process.RegisterMessage, 1),
		}
		buildScopes[0].appendInstruction(vm.Instruction{
			Op: vm.Connector,
			Arg: connector.NewArgument().
				WithReg(rs[0].Proc.Reg.MergeReceivers[1]),
		})
		rs[0].Proc.Reg.MergeReceivers = rs[0].Proc.Reg.MergeReceivers[:2]
		buildScopes[0].IsEnd = true
	}
	return rs
}

func (c *Compile) compileShuffleJoin(ctx context.Context, node, left, right *plan.Node, lefts []*Scope, rights []*Scope) []*Scope {
	isEq := plan2.IsEquiJoin2(node.OnList)
	if !isEq {
		panic("shuffle join only support equal join for now!")
	}

	rightTyps := make([]types.Type, len(right.ProjectList))
	for i, expr := range right.ProjectList {
		rightTyps[i] = dupType(&expr.Typ)
	}

	leftTyps := make([]types.Type, len(left.ProjectList))
	for i, expr := range left.ProjectList {
		leftTyps[i] = dupType(&expr.Typ)
	}

	parent, children := c.newShuffleJoinScopeList(lefts, rights, node)
	if parent != nil {
		lastOperator := make([]vm.Instruction, 0, len(children))
		for i := range children {
			ilen := len(children[i].Instructions) - 1
			lastOperator = append(lastOperator, children[i].Instructions[ilen])
			children[i].Instructions = children[i].Instructions[:ilen]
		}

		defer func() {
			// recovery the children's last operator
			for i := range children {
				children[i].appendInstruction(lastOperator[i])
			}
		}()
	}

	switch node.JoinType {
	case plan.Node_INNER:
		for i := range children {
			children[i].appendInstruction(vm.Instruction{
				Op:  vm.Join,
				Idx: c.anal.curr,
				Arg: constructJoin(node, rightTyps, c.proc),
			})
		}

	case plan.Node_ANTI:
		if node.BuildOnLeft {
			for i := range children {
				children[i].appendInstruction(vm.Instruction{
					Op:  vm.RightAnti,
					Idx: c.anal.curr,
					Arg: constructRightAnti(node, rightTyps, c.proc),
				})
			}
		} else {
			for i := range children {
				children[i].appendInstruction(vm.Instruction{
					Op:  vm.Anti,
					Idx: c.anal.curr,
					Arg: constructAnti(node, rightTyps, c.proc),
				})
			}
		}

	case plan.Node_SEMI:
		if node.BuildOnLeft {
			for i := range children {
				children[i].appendInstruction(vm.Instruction{
					Op:  vm.RightSemi,
					Idx: c.anal.curr,
					Arg: constructRightSemi(node, rightTyps, c.proc),
				})
			}
		} else {
			for i := range children {
				children[i].appendInstruction(vm.Instruction{
					Op:  vm.Semi,
					Idx: c.anal.curr,
					Arg: constructSemi(node, rightTyps, c.proc),
				})
			}
		}

	case plan.Node_LEFT:
		for i := range children {
			children[i].appendInstruction(vm.Instruction{
				Op:  vm.Left,
				Idx: c.anal.curr,
				Arg: constructLeft(node, rightTyps, c.proc),
			})
		}

	case plan.Node_RIGHT:
		for i := range children {
			children[i].appendInstruction(vm.Instruction{
				Op:  vm.Right,
				Idx: c.anal.curr,
				Arg: constructRight(node, leftTyps, rightTyps, c.proc),
			})
		}
	default:
		panic(moerr.NewNYI(ctx, fmt.Sprintf("shuffle join do not support join type '%v'", node.JoinType)))
	}

	if parent != nil {
		return parent
	}
	return children
}

func (c *Compile) compileBroadcastJoin(ctx context.Context, node, left, right *plan.Node, probeScopes []*Scope, buildScopes []*Scope) []*Scope {
	var rs []*Scope
	isEq := plan2.IsEquiJoin2(node.OnList)

	rightTyps := make([]types.Type, len(right.ProjectList))
	for i, expr := range right.ProjectList {
		rightTyps[i] = dupType(&expr.Typ)
	}

	leftTyps := make([]types.Type, len(left.ProjectList))
	for i, expr := range left.ProjectList {
		leftTyps[i] = dupType(&expr.Typ)
	}

	switch node.JoinType {
	case plan.Node_INNER:
		rs = c.newBroadcastJoinScopeList(probeScopes, buildScopes, node)
		if len(node.OnList) == 0 {
			for i := range rs {
				rs[i].appendInstruction(vm.Instruction{
					Op:  vm.Product,
					Idx: c.anal.curr,
					Arg: constructProduct(node, rightTyps, c.proc),
				})
			}
		} else {
			for i := range rs {
				if isEq {
					rs[i].appendInstruction(vm.Instruction{
						Op:  vm.Join,
						Idx: c.anal.curr,
						Arg: constructJoin(node, rightTyps, c.proc),
					})
				} else {
					rs[i].appendInstruction(vm.Instruction{
						Op:  vm.LoopJoin,
						Idx: c.anal.curr,
						Arg: constructLoopJoin(node, rightTyps, c.proc),
					})
				}
			}
		}
	case plan.Node_L2:
		rs = c.newBroadcastJoinScopeList(probeScopes, buildScopes, node)
		for i := range rs {
			rs[i].appendInstruction(vm.Instruction{
				Op:  vm.ProductL2,
				Idx: c.anal.curr,
				Arg: constructProductL2(node, rightTyps, c.proc),
			})
		}

	case plan.Node_INDEX:
		rs = c.newBroadcastJoinScopeList(probeScopes, buildScopes, node)
		for i := range rs {
			rs[i].appendInstruction(vm.Instruction{
				Op:  vm.IndexJoin,
				Idx: c.anal.curr,
				Arg: constructIndexJoin(node, rightTyps, c.proc),
			})
		}

	case plan.Node_SEMI:
		if isEq {
			if node.BuildOnLeft {
				if c.execType == plan2.ExecTypeTP {
					rs = c.newBroadcastJoinScopeList(probeScopes, buildScopes, node)
				} else {
					rs = c.newJoinScopeListWithBucket(c.newScopeListForRightJoin(2, 1, probeScopes), probeScopes, buildScopes, node)
				}
				for i := range rs {
					rs[i].appendInstruction(vm.Instruction{
						Op:  vm.RightSemi,
						Idx: c.anal.curr,
						Arg: constructRightSemi(node, rightTyps, c.proc),
					})
				}
			} else {
				rs = c.newBroadcastJoinScopeList(probeScopes, buildScopes, node)
				for i := range rs {
					rs[i].appendInstruction(vm.Instruction{
						Op:  vm.Semi,
						Idx: c.anal.curr,
						Arg: constructSemi(node, rightTyps, c.proc),
					})
				}
			}
		} else {
			rs = c.newBroadcastJoinScopeList(probeScopes, buildScopes, node)
			for i := range rs {
				rs[i].appendInstruction(vm.Instruction{
					Op:  vm.LoopSemi,
					Idx: c.anal.curr,
					Arg: constructLoopSemi(node, rightTyps, c.proc),
				})
			}
		}
	case plan.Node_LEFT:
		rs = c.newBroadcastJoinScopeList(probeScopes, buildScopes, node)
		for i := range rs {
			if isEq {
				rs[i].appendInstruction(vm.Instruction{
					Op:  vm.Left,
					Idx: c.anal.curr,
					Arg: constructLeft(node, rightTyps, c.proc),
				})
			} else {
				rs[i].appendInstruction(vm.Instruction{
					Op:  vm.LoopLeft,
					Idx: c.anal.curr,
					Arg: constructLoopLeft(node, rightTyps, c.proc),
				})
			}
		}
	case plan.Node_RIGHT:
		if isEq {
			if c.execType == plan2.ExecTypeTP {
				rs = c.newBroadcastJoinScopeList(probeScopes, buildScopes, node)
			} else {
				rs = c.newJoinScopeListWithBucket(c.newScopeListForRightJoin(2, 1, probeScopes), probeScopes, buildScopes, node)
			}
			for i := range rs {
				rs[i].appendInstruction(vm.Instruction{
					Op:  vm.Right,
					Idx: c.anal.curr,
					Arg: constructRight(node, leftTyps, rightTyps, c.proc),
				})
			}
		} else {
			panic("dont pass any no-equal right join plan to this function,it should be changed to left join by the planner")
		}
	case plan.Node_SINGLE:
		rs = c.newBroadcastJoinScopeList(probeScopes, buildScopes, node)
		for i := range rs {
			if isEq {
				rs[i].appendInstruction(vm.Instruction{
					Op:  vm.Single,
					Idx: c.anal.curr,
					Arg: constructSingle(node, rightTyps, c.proc),
				})
			} else {
				rs[i].appendInstruction(vm.Instruction{
					Op:  vm.LoopSingle,
					Idx: c.anal.curr,
					Arg: constructLoopSingle(node, rightTyps, c.proc),
				})
			}
		}
	case plan.Node_ANTI:
		if isEq {
			if node.BuildOnLeft {
				if c.execType == plan2.ExecTypeTP {
					rs = c.newBroadcastJoinScopeList(probeScopes, buildScopes, node)
				} else {
					rs = c.newJoinScopeListWithBucket(c.newScopeListForRightJoin(2, 1, probeScopes), probeScopes, buildScopes, node)
				}
				for i := range rs {
					rs[i].appendInstruction(vm.Instruction{
						Op:  vm.RightAnti,
						Idx: c.anal.curr,
						Arg: constructRightAnti(node, rightTyps, c.proc),
					})
				}
			} else {
				rs = c.newBroadcastJoinScopeList(probeScopes, buildScopes, node)
				for i := range rs {
					rs[i].appendInstruction(vm.Instruction{
						Op:  vm.Anti,
						Idx: c.anal.curr,
						Arg: constructAnti(node, rightTyps, c.proc),
					})
				}
			}
		} else {
			rs = c.newBroadcastJoinScopeList(probeScopes, buildScopes, node)
			for i := range rs {
				rs[i].appendInstruction(vm.Instruction{
					Op:  vm.LoopAnti,
					Idx: c.anal.curr,
					Arg: constructLoopAnti(node, rightTyps, c.proc),
				})
			}
		}
	case plan.Node_MARK:
		rs = c.newBroadcastJoinScopeList(probeScopes, buildScopes, node)
		for i := range rs {
			//if isEq {
			//	rs[i].appendInstruction(vm.Instruction{
			//		Op:  vm.Mark,
			//		Idx: c.anal.curr,
			//		Arg: constructMark(n, typs, c.proc),
			//	})
			//} else {
			rs[i].appendInstruction(vm.Instruction{
				Op:  vm.LoopMark,
				Idx: c.anal.curr,
				Arg: constructLoopMark(node, rightTyps, c.proc),
			})
			//}
		}
	default:
		panic(moerr.NewNYI(ctx, fmt.Sprintf("join typ '%v'", node.JoinType)))
	}
	return rs
}

func (c *Compile) compilePartition(n *plan.Node, ss []*Scope) []*Scope {
	currentFirstFlag := c.anal.isFirst
	for i := range ss {
		c.anal.isFirst = currentFirstFlag
		if containBrokenNode(ss[i]) {
			ss[i] = c.newMergeScope([]*Scope{ss[i]})
		}
		ss[i].appendInstruction(vm.Instruction{
			Op:      vm.Order,
			Idx:     c.anal.curr,
			IsFirst: c.anal.isFirst,
			Arg:     constructOrder(n),
		})
	}
	c.anal.isFirst = false

	rs := c.newMergeScope(ss)
	rs.Instructions[0].Arg.Release()
	rs.Instructions[0] = vm.Instruction{
		Op:  vm.Partition,
		Idx: c.anal.curr,
		Arg: constructPartition(n),
	}
	return []*Scope{rs}
}

func (c *Compile) compileSort(n *plan.Node, ss []*Scope) []*Scope {
	switch {
	case n.Limit != nil && n.Offset == nil && len(n.OrderBy) > 0: // top
		return c.compileTop(n, n.Limit, ss)

	case n.Limit == nil && n.Offset == nil && len(n.OrderBy) > 0: // top
		return c.compileOrder(n, ss)

	case n.Limit != nil && n.Offset != nil && len(n.OrderBy) > 0:
		if rule.IsConstant(n.Limit, false) && rule.IsConstant(n.Offset, false) {
			// get limit
			vec1, err := colexec.EvalExpressionOnce(c.proc, n.Limit, []*batch.Batch{constBat})
			if err != nil {
				panic(err)
			}
			defer vec1.Free(c.proc.Mp())

			// get offset
			vec2, err := colexec.EvalExpressionOnce(c.proc, n.Offset, []*batch.Batch{constBat})
			if err != nil {
				panic(err)
			}
			defer vec2.Free(c.proc.Mp())

			limit, offset := vector.MustFixedCol[uint64](vec1)[0], vector.MustFixedCol[uint64](vec2)[0]
			topN := limit + offset
			overflow := false
			if topN < limit || topN < offset {
				overflow = true
			}
			if !overflow && topN <= 8192*2 {
				// if n is small, convert `order by col limit m offset n` to `top m+n offset n`
				return c.compileOffset(n, c.compileTop(n, plan2.MakePlan2Uint64ConstExprWithType(topN), ss))
			}
		}
		return c.compileLimit(n, c.compileOffset(n, c.compileOrder(n, ss)))

	case n.Limit == nil && n.Offset != nil && len(n.OrderBy) > 0: // order and offset
		return c.compileOffset(n, c.compileOrder(n, ss))

	case n.Limit != nil && n.Offset == nil && len(n.OrderBy) == 0: // limit
		return c.compileLimit(n, ss)

	case n.Limit == nil && n.Offset != nil && len(n.OrderBy) == 0: // offset
		return c.compileOffset(n, ss)

	case n.Limit != nil && n.Offset != nil && len(n.OrderBy) == 0: // limit and offset
		return c.compileLimit(n, c.compileOffset(n, ss))

	default:
		return ss
	}
}

func containBrokenNode(s *Scope) bool {
	for i := range s.Instructions {
		if s.Instructions[i].IsBrokenNode() {
			return true
		}
	}
	return false
}

func (c *Compile) compileTop(n *plan.Node, topN *plan.Expr, ss []*Scope) []*Scope {
	// use topN TO make scope.
	if c.execType == plan2.ExecTypeTP {
		ss[0].appendInstruction(vm.Instruction{
			Op:      vm.Top,
			Idx:     c.anal.curr,
			IsFirst: c.anal.isFirst,
			Arg:     constructTop(n, topN),
		})
		return ss
	}

	currentFirstFlag := c.anal.isFirst
	for i := range ss {
		c.anal.isFirst = currentFirstFlag
		if containBrokenNode(ss[i]) {
			ss[i] = c.newMergeScope([]*Scope{ss[i]})
		}
		ss[i].appendInstruction(vm.Instruction{
			Op:      vm.Top,
			Idx:     c.anal.curr,
			IsFirst: c.anal.isFirst,
			Arg:     constructTop(n, topN),
		})
	}
	c.anal.isFirst = false

	rs := c.newMergeScope(ss)
	rs.Instructions[0].Arg.Release()
	rs.Instructions[0] = vm.Instruction{
		Op:  vm.MergeTop,
		Idx: c.anal.curr,
		Arg: constructMergeTop(n, topN),
	}
	return []*Scope{rs}
}

func (c *Compile) compileOrder(n *plan.Node, ss []*Scope) []*Scope {
	if c.execType == plan2.ExecTypeTP {
		ss[0].appendInstruction(vm.Instruction{
			Op:      vm.Order,
			Idx:     c.anal.curr,
			IsFirst: c.anal.isFirst,
			Arg:     constructOrder(n),
		})
		ss[0].appendInstruction(vm.Instruction{
			Op:  vm.MergeOrder,
			Idx: c.anal.curr,
			Arg: constructMergeOrder(n),
		})
		return ss
	}

	currentFirstFlag := c.anal.isFirst
	for i := range ss {
		c.anal.isFirst = currentFirstFlag
		if containBrokenNode(ss[i]) {
			ss[i] = c.newMergeScope([]*Scope{ss[i]})
		}
		ss[i].appendInstruction(vm.Instruction{
			Op:      vm.Order,
			Idx:     c.anal.curr,
			IsFirst: c.anal.isFirst,
			Arg:     constructOrder(n),
		})
	}
	c.anal.isFirst = false
	rs := c.newMergeScope(ss)
	rs.appendInstruction(vm.Instruction{
		Op:  vm.MergeOrder,
		Idx: c.anal.curr,
		Arg: constructMergeOrder(n),
	})
	return []*Scope{rs}
}

func (c *Compile) compileWin(n *plan.Node, ss []*Scope) []*Scope {
	rs := c.newMergeScope(ss)
	rs.Instructions[0].Arg.Release()
	rs.Instructions[0] = vm.Instruction{
		Op:  vm.Window,
		Idx: c.anal.curr,
		Arg: constructWindow(c.ctx, n, c.proc),
	}
	return []*Scope{rs}
}

func (c *Compile) compileTimeWin(n *plan.Node, ss []*Scope) []*Scope {
	rs := c.newMergeScope(ss)
	rs.Instructions[0].Arg.Release()
	rs.Instructions[0] = vm.Instruction{
		Op:  vm.TimeWin,
		Idx: c.anal.curr,
		Arg: constructTimeWindow(c.ctx, n),
	}
	return []*Scope{rs}
}

func (c *Compile) compileFill(n *plan.Node, ss []*Scope) []*Scope {
	rs := c.newMergeScope(ss)
	rs.Instructions[0].Arg.Release()
	rs.Instructions[0] = vm.Instruction{
		Op:  vm.Fill,
		Idx: c.anal.curr,
		Arg: constructFill(n),
	}
	return []*Scope{rs}
}

func (c *Compile) compileOffset(n *plan.Node, ss []*Scope) []*Scope {
	if c.execType == plan2.ExecTypeTP {
		ss[0].appendInstruction(vm.Instruction{
			Op:      vm.Offset,
			Idx:     c.anal.curr,
			IsFirst: c.anal.isFirst,
			Arg:     offset.NewArgument().WithOffset(n.Offset),
		})
		return ss
	}

	currentFirstFlag := c.anal.isFirst
	for i := range ss {
		if containBrokenNode(ss[i]) {
			c.anal.isFirst = currentFirstFlag
			ss[i] = c.newMergeScope([]*Scope{ss[i]})
		}
	}

	rs := c.newMergeScope(ss)
	rs.Instructions[0].Arg.Release()
	rs.Instructions[0] = vm.Instruction{
		Op:  vm.MergeOffset,
		Idx: c.anal.curr,
		Arg: constructMergeOffset(n),
	}
	return []*Scope{rs}
}

func (c *Compile) compileLimit(n *plan.Node, ss []*Scope) []*Scope {
	if c.execType == plan2.ExecTypeTP {
		ss[0].appendInstruction(vm.Instruction{
			Op:      vm.Limit,
			Idx:     c.anal.curr,
			IsFirst: c.anal.isFirst,
			Arg:     constructLimit(n),
		})
		return ss
	}
	currentFirstFlag := c.anal.isFirst

	for i := range ss {
		c.anal.isFirst = currentFirstFlag
		if containBrokenNode(ss[i]) {
			ss[i] = c.newMergeScope([]*Scope{ss[i]})
		}
		ss[i].appendInstruction(vm.Instruction{
			Op:      vm.Limit,
			Idx:     c.anal.curr,
			IsFirst: c.anal.isFirst,
			Arg:     constructLimit(n),
		})
	}
	c.anal.isFirst = false

	rs := c.newMergeScope(ss)
	rs.Instructions[0].Arg.Release()
	rs.Instructions[0] = vm.Instruction{
		Op:  vm.MergeLimit,
		Idx: c.anal.curr,
		Arg: constructMergeLimit(n),
	}
	return []*Scope{rs}
}

func (c *Compile) compileFuzzyFilter(n *plan.Node, ns []*plan.Node, left []*Scope, right []*Scope) ([]*Scope, error) {
	l := c.newMergeScope(left)
	r := c.newMergeScope(right)
	all := []*Scope{l, r}
	rs := c.newMergeScope(all)

	rs.Instructions[0].Idx = c.anal.curr

	arg := constructFuzzyFilter(n, ns[n.Children[0]], ns[n.Children[1]])

	rs.appendInstruction(vm.Instruction{
		Op:  vm.FuzzyFilter,
		Idx: c.anal.curr,
		Arg: arg,
	})

	outData, err := newFuzzyCheck(n)
	if err != nil {
		return nil, err
	}
	c.fuzzys = append(c.fuzzys, outData)
	// wrap the collision key into c.fuzzy, for more information,
	// please refer fuzzyCheck.go
	rs.appendInstruction(vm.Instruction{
		Op: vm.Output,
		Arg: output.NewArgument().
			WithFunc(
				func(bat *batch.Batch) error {
					if bat == nil || bat.IsEmpty() {
						return nil
					}
					// the batch will contain the key that fuzzyCheck
					if err := outData.fill(c.ctx, bat); err != nil {
						return err
					}

					return nil
				}),
	})

	return []*Scope{rs}, nil
}

func (c *Compile) compileSample(n *plan.Node, ss []*Scope) []*Scope {
	for i := range ss {
		if containBrokenNode(ss[i]) {
			ss[i] = c.newMergeScope([]*Scope{ss[i]})
		}
		ss[i].appendInstruction(vm.Instruction{
			Op:      vm.Sample,
			Idx:     c.anal.curr,
			IsFirst: c.anal.isFirst,
			Arg:     constructSample(n, len(ss) != 1),
		})
	}
	c.anal.isFirst = false

	rs := c.newMergeScope(ss)
	if len(ss) == 1 {
		return []*Scope{rs}
	}

	// should sample again if sample by rows.
	if n.SampleFunc.Rows != plan2.NotSampleByRows {
		rs.appendInstruction(vm.Instruction{
			Op:      vm.Sample,
			Idx:     c.anal.curr,
			IsFirst: c.anal.isFirst,
			Arg:     sample.NewMergeSample(constructSample(n, true), false),
		})
	}
	return []*Scope{rs}
}

func (c *Compile) compileTPGroup(n *plan.Node, ss []*Scope, ns []*plan.Node) []*Scope {
	ss[0].appendInstruction(vm.Instruction{
		Op:      vm.Group,
		Idx:     c.anal.curr,
		IsFirst: c.anal.isFirst,
		Arg:     constructGroup(c.ctx, n, ns[n.Children[0]], true, 0, c.proc),
	})
	c.anal.isFirst = false
	return ss
}

func (c *Compile) compileMergeGroup(n *plan.Node, ss []*Scope, ns []*plan.Node, hasDistinct bool) []*Scope {
	currentFirstFlag := c.anal.isFirst

	// for less memory usage while merge group,
	// we do not run the group-operator in parallel once this has a distinct aggregation.
	// because the parallel need to store all the source data in the memory for merging.
	// we construct a pipeline like the following description for this case:
	//
	// all the operators from ss[0] to ss[last] send the data to only one group-operator.
	// this group-operator sends its result to the merge-group-operator.
	// todo: I cannot remove the merge-group action directly, because the merge-group action is used to fill the partial result.
	if hasDistinct {
		for i := range ss {
			c.anal.isFirst = currentFirstFlag
			if containBrokenNode(ss[i]) {
				ss[i] = c.newMergeScope([]*Scope{ss[i]})
			}
		}
		c.anal.isFirst = false

		mergeToGroup := c.newMergeScope(ss)
		mergeToGroup.appendInstruction(
			vm.Instruction{
				Op:      vm.Group,
				Idx:     c.anal.curr,
				IsFirst: c.anal.isFirst,
				Arg:     constructGroup(c.ctx, n, ns[n.Children[0]], false, 0, c.proc),
			})

		rs := c.newMergeScope([]*Scope{mergeToGroup})
		arg := constructMergeGroup(true)
		if ss[0].PartialResults != nil {
			arg.PartialResults = ss[0].PartialResults
			arg.PartialResultTypes = ss[0].PartialResultTypes
			ss[0].PartialResults = nil
			ss[0].PartialResultTypes = nil
		}
		rs.Instructions[0].Arg.Release()
		rs.Instructions[0] = vm.Instruction{
			Op:  vm.MergeGroup,
			Idx: c.anal.curr,
			Arg: arg,
		}
		return []*Scope{rs}
	}

	for i := range ss {
		c.anal.isFirst = currentFirstFlag
		if containBrokenNode(ss[i]) {
			ss[i] = c.newMergeScope([]*Scope{ss[i]})
		}
		ss[i].appendInstruction(vm.Instruction{
			Op:      vm.Group,
			Idx:     c.anal.curr,
			IsFirst: c.anal.isFirst,
			Arg:     constructGroup(c.ctx, n, ns[n.Children[0]], false, 0, c.proc),
		})
	}
	c.anal.isFirst = false

	rs := c.newMergeScope(ss)
	arg := constructMergeGroup(true)
	if ss[0].PartialResults != nil {
		arg.PartialResults = ss[0].PartialResults
		arg.PartialResultTypes = ss[0].PartialResultTypes
		ss[0].PartialResults = nil
		ss[0].PartialResultTypes = nil
	}
	rs.Instructions[0].Arg.Release()
	rs.Instructions[0] = vm.Instruction{
		Op:  vm.MergeGroup,
		Idx: c.anal.curr,
		Arg: arg,
	}
	return []*Scope{rs}
}

// shuffle and dispatch must stick together
func (c *Compile) constructShuffleAndDispatch(ss, children []*Scope, n *plan.Node) {
	j := 0
	for i := range ss {
		if containBrokenNode(ss[i]) {
			isEnd := ss[i].IsEnd
			ss[i] = c.newMergeScope([]*Scope{ss[i]})
			ss[i].IsEnd = isEnd
		}
		if !ss[i].IsEnd {
			ss[i].appendInstruction(vm.Instruction{
				Op:  vm.Shuffle,
				Arg: constructShuffleGroupArg(children, n),
			})

			ss[i].appendInstruction(vm.Instruction{
				Op:  vm.Dispatch,
				Arg: constructDispatch(j, children, ss[i].NodeInfo.Addr, n, false),
			})
			j++
			ss[i].IsEnd = true
		}
	}
}

func (c *Compile) compileShuffleGroup(n *plan.Node, ss []*Scope, ns []*plan.Node) []*Scope {
	currentIsFirst := c.anal.isFirst
	c.anal.isFirst = false

	if len(c.cnList) > 1 {
		n.Stats.HashmapStats.ShuffleMethod = plan.ShuffleMethod_Normal
	}

	switch n.Stats.HashmapStats.ShuffleMethod {
	case plan.ShuffleMethod_Reuse:
		for i := range ss {
			ss[i].appendInstruction(vm.Instruction{
				Op:      vm.Group,
				Idx:     c.anal.curr,
				IsFirst: c.anal.isFirst,
				Arg:     constructGroup(c.ctx, n, ns[n.Children[0]], true, len(ss), c.proc),
			})
		}
		ss = c.compileProjection(n, c.compileRestrict(n, ss))
		return ss

	case plan.ShuffleMethod_Reshuffle:

		dop := plan2.GetShuffleDop()
		parent, children := c.newScopeListForShuffleGroup(1, dop)
		// saving the last operator of all children to make sure the connector setting in
		// the right place
		lastOperator := make([]vm.Instruction, 0, len(children))
		for i := range children {
			ilen := len(children[i].Instructions) - 1
			lastOperator = append(lastOperator, children[i].Instructions[ilen])
			children[i].Instructions = children[i].Instructions[:ilen]
		}

		for i := range children {
			children[i].appendInstruction(vm.Instruction{
				Op:      vm.Group,
				Idx:     c.anal.curr,
				IsFirst: currentIsFirst,
				Arg:     constructGroup(c.ctx, n, ns[n.Children[0]], true, len(children), c.proc),
			})
		}
		children = c.compileProjection(n, c.compileRestrict(n, children))
		// recovery the children's last operator
		for i := range children {
			children[i].appendInstruction(lastOperator[i])
		}

		for i := range ss {
			ss[i].appendInstruction(vm.Instruction{
				Op:      vm.Shuffle,
				Idx:     c.anal.curr,
				IsFirst: currentIsFirst,
				Arg:     constructShuffleGroupArg(children, n),
			})
		}

		mergeScopes := c.newMergeScope(ss)
		mergeScopes.appendInstruction(vm.Instruction{
			Op:      vm.Dispatch,
			Idx:     c.anal.curr,
			IsFirst: currentIsFirst,
			Arg:     constructDispatch(0, children, c.addr, n, false),
		})

		appendIdx := 0
		for i := range children {
			if isSameCN(mergeScopes.NodeInfo.Addr, children[i].NodeInfo.Addr) {
				appendIdx = i
				break
			}
		}
		children[appendIdx].PreScopes = append(children[appendIdx].PreScopes, mergeScopes)

		return parent
	default:
		dop := plan2.GetShuffleDop()
		parent, children := c.newScopeListForShuffleGroup(validScopeCount(ss), dop)
		c.constructShuffleAndDispatch(ss, children, n)

		// saving the last operator of all children to make sure the connector setting in
		// the right place
		lastOperator := make([]vm.Instruction, 0, len(children))
		for i := range children {
			ilen := len(children[i].Instructions) - 1
			lastOperator = append(lastOperator, children[i].Instructions[ilen])
			children[i].Instructions = children[i].Instructions[:ilen]
		}

		for i := range children {
			children[i].appendInstruction(vm.Instruction{
				Op:      vm.Group,
				Idx:     c.anal.curr,
				IsFirst: currentIsFirst,
				Arg:     constructGroup(c.ctx, n, ns[n.Children[0]], true, len(children), c.proc),
			})
		}
		children = c.compileProjection(n, c.compileRestrict(n, children))
		// recovery the children's last operator
		for i := range children {
			children[i].appendInstruction(lastOperator[i])
		}

		for i := range ss {
			appended := false
			for j := range children {
				if isSameCN(children[j].NodeInfo.Addr, ss[i].NodeInfo.Addr) {
					children[j].PreScopes = append(children[j].PreScopes, ss[i])
					appended = true
					break
				}
			}
			if !appended {
				children[0].PreScopes = append(children[0].PreScopes, ss[i])
			}
		}

		return parent
		// return []*Scope{c.newMergeScope(parent)}
	}
}

// DeleteMergeScope need to assure this:
// one block can be only deleted by one and the same
// CN, so we need to transfer the rows from the
// the same block to one and the same CN to perform
// the deletion operators.
func (c *Compile) newDeleteMergeScope(arg *deletion.Argument, ss []*Scope) *Scope {
	// Todo: implemet delete merge
	ss2 := make([]*Scope, 0, len(ss))
	// ends := make([]*Scope, 0, len(ss))
	for _, s := range ss {
		if s.IsEnd {
			// ends = append(ends, s)
			continue
		}
		ss2 = append(ss2, s)
	}

	rs := make([]*Scope, 0, len(ss2))
	uuids := make([]uuid.UUID, 0, len(ss2))
	var uid uuid.UUID
	for i := 0; i < len(ss2); i++ {
		rs = append(rs, newScope(Merge))
		uid, _ = uuid.NewV7()
		uuids = append(uuids, uid)
	}

	// for every scope, it should dispatch its
	// batch to other cn
	for i := 0; i < len(ss2); i++ {
		constructDeleteDispatchAndLocal(i, rs, ss2, uuids, c)
	}
	delete := &vm.Instruction{
		Op:  vm.Deletion,
		Arg: arg,
	}
	for i := range rs {
		// use distributed delete
		arg.RemoteDelete = true
		// maybe just copy only once?
		arg.SegmentMap = colexec.Get().GetCnSegmentMap()
		arg.IBucket = uint32(i)
		arg.Nbucket = uint32(len(rs))
		rs[i].Instructions = append(
			rs[i].Instructions,
			dupInstruction(delete, nil, 0))
	}
	return c.newMergeScope(rs)
}

func (c *Compile) newMergeScope(ss []*Scope) *Scope {
	rs := newScope(Merge)
	rs.NodeInfo = getEngineNode(c)
	rs.PreScopes = ss
	cnt := 0
	for _, s := range ss {
		if s.IsEnd {
			continue
		}
		cnt++
	}
	rs.Proc = process.NewWithAnalyze(c.proc, c.ctx, cnt, c.anal.Nodes())
	if len(ss) > 0 {
		rs.Proc.LoadTag = ss[0].Proc.LoadTag
	}
	rs.Instructions = append(rs.Instructions, vm.Instruction{
		Op:      vm.Merge,
		Idx:     c.anal.curr,
		IsFirst: c.anal.isFirst,
		Arg:     merge.NewArgument(),
	})
	c.anal.isFirst = false

	j := 0
	for i := range ss {
		if !ss[i].IsEnd {
			ss[i].appendInstruction(vm.Instruction{
				Op: vm.Connector,
				Arg: connector.NewArgument().
					WithReg(rs.Proc.Reg.MergeReceivers[j]),
			})
			j++
		}
	}
	return rs
}

func (c *Compile) newMergeRemoteScope(ss []*Scope, nodeinfo engine.Node) *Scope {
	rs := c.newMergeScope(ss)
	// reset rs's info to remote
	rs.Magic = Remote
	rs.NodeInfo.Addr = nodeinfo.Addr
	rs.NodeInfo.Mcpu = nodeinfo.Mcpu

	return rs
}

func (c *Compile) newScopeListOnCurrentCN(childrenCount int, blocks int) []*Scope {
	var ss []*Scope
	currentFirstFlag := c.anal.isFirst
	for _, cn := range c.cnList {
		if !isSameCN(cn.Addr, c.addr) {
			continue
		}
		c.anal.isFirst = currentFirstFlag
		ss = append(ss, c.newScopeListWithNode(c.generateCPUNumber(cn.Mcpu, blocks), childrenCount, cn.Addr)...)
	}
	return ss
}

/*
	func (c *Compile) newScopeList(childrenCount int, blocks int) []*Scope {
		var ss []*Scope

		currentFirstFlag := c.anal.isFirst
		for _, cn := range c.cnList {
			c.anal.isFirst = currentFirstFlag
			ss = append(ss, c.newScopeListWithNode(c.generateCPUNumber(cn.Mcpu, blocks), childrenCount, cn.Addr)...)
		}
		return ss
	}
*/
func (c *Compile) newScopeListForShuffleGroup(childrenCount int, blocks int) ([]*Scope, []*Scope) {
	parent := make([]*Scope, 0, len(c.cnList))
	children := make([]*Scope, 0, len(c.cnList))

	currentFirstFlag := c.anal.isFirst
	for _, n := range c.cnList {
		c.anal.isFirst = currentFirstFlag
		scopes := c.newScopeListWithNode(c.generateCPUNumber(n.Mcpu, blocks), childrenCount, n.Addr)
		for _, s := range scopes {
			for _, rr := range s.Proc.Reg.MergeReceivers {
				rr.Ch = make(chan *process.RegisterMessage, shuffleChannelBufferSize)
			}
		}
		children = append(children, scopes...)
		parent = append(parent, c.newMergeRemoteScope(scopes, n))
	}
	return parent, children
}

func (c *Compile) newScopeListWithNode(mcpu, childrenCount int, addr string) []*Scope {
	ss := make([]*Scope, mcpu)
	currentFirstFlag := c.anal.isFirst
	for i := range ss {
		ss[i] = newScope(Remote)
		ss[i].Magic = Remote
		ss[i].NodeInfo.Addr = addr
		ss[i].NodeInfo.Mcpu = 1 // ss is already the mcpu length so we don't need to parallel it
		ss[i].Proc = process.NewWithAnalyze(c.proc, c.ctx, childrenCount, c.anal.Nodes())
		ss[i].Instructions = append(ss[i].Instructions, vm.Instruction{
			Op:      vm.Merge,
			Idx:     c.anal.curr,
			IsFirst: currentFirstFlag,
			Arg:     merge.NewArgument(),
		})
	}
	c.anal.isFirst = false
	return ss
}

func (c *Compile) newScopeListForRightJoin(childrenCount int, bIdx int, leftScopes []*Scope) []*Scope {
	/*
		ss := make([]*Scope, 0, len(leftScopes))
		for i := range leftScopes {
			tmp := new(Scope)
			tmp.Magic = Remote
			tmp.IsJoin = true
			tmp.Proc = process.NewWithAnalyze(c.proc, c.ctx, childrenCount, c.anal.Nodes())
			tmp.NodeInfo = leftScopes[i].NodeInfo
			ss = append(ss, tmp)
		}
	*/
	// Force right join to execute on one CN due to right join issue
	// Will fix in future
	maxCpuNum := 1
	for _, s := range leftScopes {
		if s.NodeInfo.Mcpu > maxCpuNum {
			maxCpuNum = s.NodeInfo.Mcpu
		}
	}

	ss := make([]*Scope, 1)
	ss[0] = newScope(Remote)
	ss[0].IsJoin = true
	ss[0].Proc = process.NewWithAnalyze(c.proc, c.ctx, childrenCount, c.anal.Nodes())
	ss[0].NodeInfo = engine.Node{Addr: c.addr, Mcpu: c.generateCPUNumber(ncpu, maxCpuNum)}
	ss[0].BuildIdx = bIdx
	return ss
}

func (c *Compile) newJoinScopeListWithBucket(rs, ss, children []*Scope, n *plan.Node) []*Scope {
	currentFirstFlag := c.anal.isFirst
	// construct left
	leftMerge := c.newMergeScope(ss)
	leftMerge.appendInstruction(vm.Instruction{
		Op:  vm.Dispatch,
		Arg: constructDispatch(0, rs, c.addr, n, false),
	})
	leftMerge.IsEnd = true

	// construct right
	c.anal.isFirst = currentFirstFlag
	rightMerge := c.newMergeScope(children)
	rightMerge.appendInstruction(vm.Instruction{
		Op:  vm.Dispatch,
		Arg: constructDispatch(1, rs, c.addr, n, false),
	})
	rightMerge.IsEnd = true

	// append left and right to correspond rs
	idx := 0
	for i := range rs {
		if isSameCN(rs[i].NodeInfo.Addr, c.addr) {
			idx = i
		}
	}
	rs[idx].PreScopes = append(rs[idx].PreScopes, leftMerge, rightMerge)
	return rs
}

func (c *Compile) newBroadcastJoinScopeList(probeScopes []*Scope, buildScopes []*Scope, n *plan.Node) []*Scope {
	length := len(probeScopes)
	rs := make([]*Scope, length)
	idx := 0
	for i := range probeScopes {
		if probeScopes[i].IsEnd {
			rs[i] = probeScopes[i]
			continue
		}
		rs[i] = newScope(Remote)
		rs[i].IsJoin = true
		rs[i].NodeInfo = probeScopes[i].NodeInfo
		rs[i].BuildIdx = 1
		if isSameCN(rs[i].NodeInfo.Addr, c.addr) {
			idx = i
		}
		rs[i].PreScopes = []*Scope{probeScopes[i]}
		rs[i].Proc = process.NewWithAnalyze(c.proc, c.ctx, 2, c.anal.Nodes())
		probeScopes[i].appendInstruction(vm.Instruction{
			Op: vm.Connector,
			Arg: connector.NewArgument().
				WithReg(rs[i].Proc.Reg.MergeReceivers[0]),
		})
	}

	// all join's first flag will setting in newLeftScope and newRightScope
	// so we set it to false now
	if c.execType == plan2.ExecTypeTP {
		rs[0].PreScopes = append(rs[0].PreScopes, buildScopes[0])
	} else {
		c.anal.isFirst = false
		mergeChildren := c.newMergeScope(buildScopes)

		mergeChildren.appendInstruction(vm.Instruction{
			Op:  vm.Dispatch,
			Arg: constructDispatch(1, rs, c.addr, n, false),
		})
		mergeChildren.IsEnd = true
		rs[idx].PreScopes = append(rs[idx].PreScopes, mergeChildren)
	}
	return rs
}

func (c *Compile) newShuffleJoinScopeList(left, right []*Scope, n *plan.Node) ([]*Scope, []*Scope) {
	single := len(c.cnList) <= 1
	if single {
		n.Stats.HashmapStats.ShuffleTypeForMultiCN = plan.ShuffleTypeForMultiCN_Simple
	}

	var parent []*Scope
	children := make([]*Scope, 0, len(c.cnList))
	lnum := len(left)
	sum := lnum + len(right)
	for _, n := range c.cnList {
		dop := c.generateCPUNumber(n.Mcpu, plan2.GetShuffleDop())
		ss := make([]*Scope, dop)
		for i := range ss {
			ss[i] = newScope(Remote)
			ss[i].IsJoin = true
			ss[i].NodeInfo.Addr = n.Addr
			ss[i].NodeInfo.Mcpu = 1
			ss[i].Proc = process.NewWithAnalyze(c.proc, c.ctx, sum, c.anal.Nodes())
			ss[i].BuildIdx = lnum
			ss[i].ShuffleCnt = dop
			for _, rr := range ss[i].Proc.Reg.MergeReceivers {
				rr.Ch = make(chan *process.RegisterMessage, shuffleChannelBufferSize)
			}
		}
		children = append(children, ss...)
		if !single {
			parent = append(parent, c.newMergeRemoteScope(ss, n))
		}
	}

	currentFirstFlag := c.anal.isFirst
	for i, scp := range left {
		scp.appendInstruction(vm.Instruction{
			Op:  vm.Shuffle,
			Idx: c.anal.curr,
			Arg: constructShuffleJoinArg(children, n, true),
		})
		scp.appendInstruction(vm.Instruction{
			Op:  vm.Dispatch,
			Arg: constructDispatch(i, children, scp.NodeInfo.Addr, n, true),
		})
		scp.IsEnd = true

		appended := false
		for _, js := range children {
			if isSameCN(js.NodeInfo.Addr, scp.NodeInfo.Addr) {
				js.PreScopes = append(js.PreScopes, scp)
				appended = true
				break
			}
		}
		if !appended {
			c.proc.Errorf(c.ctx, "no same addr scope to append left scopes")
			children[0].PreScopes = append(children[0].PreScopes, scp)
		}
	}

	c.anal.isFirst = currentFirstFlag
	for i, scp := range right {
		scp.appendInstruction(vm.Instruction{
			Op:  vm.Shuffle,
			Idx: c.anal.curr,
			Arg: constructShuffleJoinArg(children, n, false),
		})
		scp.appendInstruction(vm.Instruction{
			Op:  vm.Dispatch,
			Arg: constructDispatch(i+lnum, children, scp.NodeInfo.Addr, n, false),
		})
		scp.IsEnd = true

		appended := false
		for _, js := range children {
			if isSameCN(js.NodeInfo.Addr, scp.NodeInfo.Addr) {
				js.PreScopes = append(js.PreScopes, scp)
				appended = true
				break
			}
		}
		if !appended {
			c.proc.Errorf(c.ctx, "no same addr scope to append right scopes")
			children[0].PreScopes = append(children[0].PreScopes, scp)
		}
	}
	return parent, children
}

func (c *Compile) newJoinProbeScope(s *Scope, ss []*Scope) *Scope {
	rs := newScope(Merge)
	rs.appendInstruction(vm.Instruction{
		Op:      vm.Merge,
		Idx:     s.Instructions[0].Idx,
		IsFirst: true,
		Arg:     merge.NewArgument(),
	})
	rs.Proc = process.NewWithAnalyze(s.Proc, s.Proc.Ctx, s.BuildIdx, c.anal.Nodes())
	for i := 0; i < s.BuildIdx; i++ {
		regTransplant(s, rs, i, i)
	}

	if ss == nil {
		s.Proc.Reg.MergeReceivers[0] = &process.WaitRegister{
			Ctx: s.Proc.Ctx,
			Ch:  make(chan *process.RegisterMessage, shuffleChannelBufferSize),
		}
		rs.appendInstruction(vm.Instruction{
			Op: vm.Connector,
			Arg: connector.NewArgument().
				WithReg(s.Proc.Reg.MergeReceivers[0]),
		})
		s.Proc.Reg.MergeReceivers = append(s.Proc.Reg.MergeReceivers[:1], s.Proc.Reg.MergeReceivers[s.BuildIdx:]...)
		s.BuildIdx = 1
	} else {
		rs.appendInstruction(vm.Instruction{
			Op:  vm.Dispatch,
			Arg: constructDispatchLocal(false, false, false, extraRegisters(ss, 0)),
		})
	}
	rs.IsEnd = true

	return rs
}

func (c *Compile) newJoinBuildScope(s *Scope, ss []*Scope) *Scope {
	rs := newScope(Merge)
	buildLen := len(s.Proc.Reg.MergeReceivers) - s.BuildIdx
	rs.Proc = process.NewWithAnalyze(s.Proc, s.Proc.Ctx, buildLen, c.anal.Nodes())
	for i := 0; i < buildLen; i++ {
		regTransplant(s, rs, i+s.BuildIdx, i)
	}
	rs.Instructions = append(rs.Instructions, vm.Instruction{
		Op:      vm.Merge,
		Idx:     c.anal.curr,
		IsFirst: c.anal.isFirst,
		Arg:     merge.NewArgument(),
	})
	rs.appendInstruction(constructJoinBuildInstruction(c, s.Instructions[0], ss != nil, s.ShuffleCnt > 0))

	if ss == nil { // unparallel, send the hashtable to join scope directly
		s.Proc.Reg.MergeReceivers[s.BuildIdx] = &process.WaitRegister{
			Ctx: s.Proc.Ctx,
			Ch:  make(chan *process.RegisterMessage, 1),
		}
		rs.appendInstruction(vm.Instruction{
			Op: vm.Connector,
			Arg: connector.NewArgument().
				WithReg(s.Proc.Reg.MergeReceivers[s.BuildIdx]),
		})
		s.Proc.Reg.MergeReceivers = s.Proc.Reg.MergeReceivers[:s.BuildIdx+1]
	} else {
		rs.appendInstruction(vm.Instruction{
			Op:  vm.Dispatch,
			Arg: constructDispatchLocal(true, false, false, extraRegisters(ss, s.BuildIdx)),
		})
	}
	rs.IsEnd = true

	return rs
}

// Transplant the source's RemoteReceivRegInfos which index equal to sourceIdx to
// target with new index targetIdx
func regTransplant(source, target *Scope, sourceIdx, targetIdx int) {
	target.Proc.Reg.MergeReceivers[targetIdx] = source.Proc.Reg.MergeReceivers[sourceIdx]
	target.Proc.Reg.MergeReceivers[targetIdx].Ctx = target.Proc.Ctx
	i := 0
	for i < len(source.RemoteReceivRegInfos) {
		op := &source.RemoteReceivRegInfos[i]
		if op.Idx == sourceIdx {
			target.RemoteReceivRegInfos = append(target.RemoteReceivRegInfos, RemoteReceivRegInfo{
				Idx:      targetIdx,
				Uuid:     op.Uuid,
				FromAddr: op.FromAddr,
			})
			source.RemoteReceivRegInfos = append(source.RemoteReceivRegInfos[:i], source.RemoteReceivRegInfos[i+1:]...)
			continue
		}
		i++
	}
}

func (c *Compile) generateCPUNumber(cpunum, blocks int) int {
	if cpunum <= 0 || blocks <= 0 || c.execType == plan2.ExecTypeTP {
		return 1
	}

	if cpunum <= blocks {
		return cpunum
	}
	return blocks
}

func (c *Compile) initAnalyze(qry *plan.Query) {
	if len(qry.Nodes) == 0 {
		panic("empty plan")
	}

	anals := make([]*process.AnalyzeInfo, len(qry.Nodes))
	for i := range anals {
		anals[i] = reuse.Alloc[process.AnalyzeInfo](nil)
		anals[i].NodeId = int32(i)
	}
	c.anal = newAnaylze()
	c.anal.qry = qry
	c.anal.analInfos = anals
	c.anal.curr = int(qry.Steps[0])
	for _, node := range c.anal.qry.Nodes {
		if node.AnalyzeInfo == nil {
			node.AnalyzeInfo = new(plan.AnalyzeInfo)
		}
	}
	c.proc.AnalInfos = c.anal.analInfos
}

func (c *Compile) fillAnalyzeInfo() {
	// record the number of s3 requests
	c.anal.S3IOInputCount(c.anal.curr, c.counterSet.FileService.S3.Put.Load())
	c.anal.S3IOInputCount(c.anal.curr, c.counterSet.FileService.S3.List.Load())

	c.anal.S3IOOutputCount(c.anal.curr, c.counterSet.FileService.S3.Head.Load())
	c.anal.S3IOOutputCount(c.anal.curr, c.counterSet.FileService.S3.Get.Load())
	c.anal.S3IOOutputCount(c.anal.curr, c.counterSet.FileService.S3.Delete.Load())
	c.anal.S3IOOutputCount(c.anal.curr, c.counterSet.FileService.S3.DeleteMulti.Load())

	for i, anal := range c.anal.analInfos {
		atomic.StoreInt64(&c.anal.qry.Nodes[i].AnalyzeInfo.InputBlocks, atomic.LoadInt64(&anal.InputBlocks))
		atomic.StoreInt64(&c.anal.qry.Nodes[i].AnalyzeInfo.InputRows, atomic.LoadInt64(&anal.InputRows))
		atomic.StoreInt64(&c.anal.qry.Nodes[i].AnalyzeInfo.OutputRows, atomic.LoadInt64(&anal.OutputRows))
		atomic.StoreInt64(&c.anal.qry.Nodes[i].AnalyzeInfo.InputSize, atomic.LoadInt64(&anal.InputSize))
		atomic.StoreInt64(&c.anal.qry.Nodes[i].AnalyzeInfo.OutputSize, atomic.LoadInt64(&anal.OutputSize))
		atomic.StoreInt64(&c.anal.qry.Nodes[i].AnalyzeInfo.TimeConsumed, atomic.LoadInt64(&anal.TimeConsumed))
		atomic.StoreInt64(&c.anal.qry.Nodes[i].AnalyzeInfo.MemorySize, atomic.LoadInt64(&anal.MemorySize))
		atomic.StoreInt64(&c.anal.qry.Nodes[i].AnalyzeInfo.WaitTimeConsumed, atomic.LoadInt64(&anal.WaitTimeConsumed))
		atomic.StoreInt64(&c.anal.qry.Nodes[i].AnalyzeInfo.DiskIO, atomic.LoadInt64(&anal.DiskIO))
		atomic.StoreInt64(&c.anal.qry.Nodes[i].AnalyzeInfo.S3IOByte, atomic.LoadInt64(&anal.S3IOByte))
		atomic.StoreInt64(&c.anal.qry.Nodes[i].AnalyzeInfo.S3IOInputCount, atomic.LoadInt64(&anal.S3IOInputCount))
		atomic.StoreInt64(&c.anal.qry.Nodes[i].AnalyzeInfo.S3IOOutputCount, atomic.LoadInt64(&anal.S3IOOutputCount))
		atomic.StoreInt64(&c.anal.qry.Nodes[i].AnalyzeInfo.NetworkIO, atomic.LoadInt64(&anal.NetworkIO))
		atomic.StoreInt64(&c.anal.qry.Nodes[i].AnalyzeInfo.ScanTime, atomic.LoadInt64(&anal.ScanTime))
		atomic.StoreInt64(&c.anal.qry.Nodes[i].AnalyzeInfo.InsertTime, atomic.LoadInt64(&anal.InsertTime))
		anal.DeepCopyArray(c.anal.qry.Nodes[i].AnalyzeInfo)
	}
}

func (c *Compile) determinExpandRanges(n *plan.Node) bool {
	if c.pn.GetQuery().StmtType != plan.Query_SELECT && len(n.RuntimeFilterProbeList) == 0 {
		return true
	}

	if n.Stats.BlockNum > int32(plan2.BlockThresholdForOneCN) && len(c.cnList) > 1 && !n.Stats.ForceOneCN {
		return true
	}

	if n.AggList != nil { //need to handle partial results
		return true
	}
	return false
}

func (c *Compile) expandRanges(n *plan.Node, rel engine.Relation, blockFilterList []*plan.Expr) (engine.Ranges, error) {
	var err error
	var db engine.Database
	var ranges engine.Ranges
	var txnOp client.TxnOperator

	//-----------------------------------------------------------------------------------------------------
	ctx := c.ctx
	txnOp = c.proc.TxnOperator
	if n.ScanSnapshot != nil && n.ScanSnapshot.TS != nil {
		if !n.ScanSnapshot.TS.Equal(timestamp.Timestamp{LogicalTime: 0, PhysicalTime: 0}) &&
			n.ScanSnapshot.TS.Less(c.proc.TxnOperator.Txn().SnapshotTS) {
			txnOp = c.proc.TxnOperator.CloneSnapshotOp(*n.ScanSnapshot.TS)

			if n.ScanSnapshot.Tenant != nil {
				ctx = context.WithValue(ctx, defines.TenantIDKey{}, n.ScanSnapshot.Tenant.TenantID)
			}
		}
	}
	//-----------------------------------------------------------------------------------------------------

	if util.TableIsClusterTable(n.TableDef.GetTableType()) {
		ctx = defines.AttachAccountId(ctx, catalog.System_Account)
	}
	if n.ObjRef.PubInfo != nil {
		ctx = defines.AttachAccountId(ctx, uint32(n.ObjRef.PubInfo.GetTenantId()))
	}
	if util.TableIsLoggingTable(n.ObjRef.SchemaName, n.ObjRef.ObjName) {
		ctx = defines.AttachAccountId(ctx, catalog.System_Account)
	}

	db, err = c.e.Database(ctx, n.ObjRef.SchemaName, txnOp)
	if err != nil {
		return nil, err
	}
	ranges, err = rel.Ranges(ctx, blockFilterList, c.TxnOffset)
	if err != nil {
		return nil, err
	}

	if n.TableDef.Partition != nil {
		if n.PartitionPrune != nil && n.PartitionPrune.IsPruned {
			for i, partitionItem := range n.PartitionPrune.SelectedPartitions {
				partTableName := partitionItem.PartitionTableName
				subrelation, err := db.Relation(ctx, partTableName, c.proc)
				if err != nil {
					return nil, err
				}
				subranges, err := subrelation.Ranges(ctx, n.BlockFilterList, c.TxnOffset)
				if err != nil {
					return nil, err
				}
				// add partition number into objectio.BlockInfo.
				blkSlice := subranges.(*objectio.BlockInfoSlice)
				for j := 1; j < subranges.Len(); j++ {
					blkInfo := blkSlice.Get(j)
					blkInfo.PartitionNum = i
					ranges.Append(blkSlice.GetBytes(j))
				}
			}
		} else {
			partitionInfo := n.TableDef.Partition
			partitionNum := int(partitionInfo.PartitionNum)
			partitionTableNames := partitionInfo.PartitionTableNames
			for i := 0; i < partitionNum; i++ {
				partTableName := partitionTableNames[i]
				subrelation, err := db.Relation(ctx, partTableName, c.proc)
				if err != nil {
					return nil, err
				}
				subranges, err := subrelation.Ranges(ctx, n.BlockFilterList, c.TxnOffset)
				if err != nil {
					return nil, err
				}
				// add partition number into objectio.BlockInfo.
				blkSlice := subranges.(*objectio.BlockInfoSlice)
				for j := 1; j < subranges.Len(); j++ {
					blkInfo := blkSlice.Get(j)
					blkInfo.PartitionNum = i
					ranges.Append(blkSlice.GetBytes(j))
				}
			}
		}
	}

	return ranges, nil
}

func (c *Compile) generateNodes(n *plan.Node) (engine.Nodes, []any, []types.T, error) {
	var err error
	var db engine.Database
	var rel engine.Relation
	var ranges engine.Ranges
	var partialResults []any
	var partialResultTypes []types.T
	var nodes engine.Nodes
	var txnOp client.TxnOperator

	//------------------------------------------------------------------------------------------------------------------
	ctx := c.ctx
	txnOp = c.proc.TxnOperator
	if n.ScanSnapshot != nil && n.ScanSnapshot.TS != nil {
		if !n.ScanSnapshot.TS.Equal(timestamp.Timestamp{LogicalTime: 0, PhysicalTime: 0}) &&
			n.ScanSnapshot.TS.Less(c.proc.TxnOperator.Txn().SnapshotTS) {
			txnOp = c.proc.TxnOperator.CloneSnapshotOp(*n.ScanSnapshot.TS)

			if n.ScanSnapshot.Tenant != nil {
				ctx = context.WithValue(ctx, defines.TenantIDKey{}, n.ScanSnapshot.Tenant.TenantID)
			}
		}
	}
	//-------------------------------------------------------------------------------------------------------------
	//ctx := c.ctx
	if util.TableIsClusterTable(n.TableDef.GetTableType()) {
		ctx = defines.AttachAccountId(ctx, catalog.System_Account)
	}
	if n.ObjRef.PubInfo != nil {
		ctx = defines.AttachAccountId(ctx, uint32(n.ObjRef.PubInfo.GetTenantId()))
	}
	if util.TableIsLoggingTable(n.ObjRef.SchemaName, n.ObjRef.ObjName) {
		ctx = defines.AttachAccountId(ctx, catalog.System_Account)
	}

	if c.determinExpandRanges(n) {
		db, err = c.e.Database(ctx, n.ObjRef.SchemaName, txnOp)
		if err != nil {
			return nil, nil, nil, err
		}
		rel, err = db.Relation(ctx, n.TableDef.Name, c.proc)
		if err != nil {
			if txnOp.IsSnapOp() {
				return nil, nil, nil, err
			}
			var e error // avoid contamination of error messages
			db, e = c.e.Database(ctx, defines.TEMPORARY_DBNAME, txnOp)
			if e != nil {
				return nil, nil, nil, err
			}

			// if temporary table, just scan at local cn.
			rel, e = db.Relation(ctx, engine.GetTempTableName(n.ObjRef.SchemaName, n.TableDef.Name), c.proc)
			if e != nil {
				return nil, nil, nil, err
			}
			c.cnList = engine.Nodes{
				engine.Node{
					Addr: c.addr,
					Mcpu: 1,
				},
			}
		}
		ranges, err = c.expandRanges(n, rel, n.BlockFilterList)
		if err != nil {
			return nil, nil, nil, err
		}
	} else {
		// add current CN
		nodes = append(nodes, engine.Node{
			Addr: c.addr,
			Mcpu: c.generateCPUNumber(ncpu, int(n.Stats.BlockNum)),
		})
		nodes[0].NeedExpandRanges = true
		return nodes, nil, nil, nil
	}

	if len(n.AggList) > 0 && ranges.Len() > 1 {
		newranges := make([]byte, 0, ranges.Size())
		newranges = append(newranges, ranges.GetBytes(0)...)
		partialResults = make([]any, 0, len(n.AggList))
		partialResultTypes = make([]types.T, len(n.AggList))

		for i := range n.AggList {
			agg := n.AggList[i].Expr.(*plan.Expr_F)
			name := agg.F.Func.ObjName
			switch name {
			case "starcount":
				partialResults = append(partialResults, int64(0))
				partialResultTypes[i] = types.T_int64
			case "count":
				if (uint64(agg.F.Func.Obj) & function.Distinct) != 0 {
					partialResults = nil
				} else {
					partialResults = append(partialResults, int64(0))
					partialResultTypes[i] = types.T_int64
				}
			case "min", "max":
				partialResults = append(partialResults, nil)
			default:
				partialResults = nil
			}
			if partialResults == nil {
				break
			}
		}

		if len(n.AggList) == 1 && n.AggList[0].Expr.(*plan.Expr_F).F.Func.ObjName == "starcount" {
			for i := 1; i < ranges.Len(); i++ {
				blk := ranges.(*objectio.BlockInfoSlice).Get(i)
				if !blk.CanRemote || !blk.DeltaLocation().IsEmpty() {
					newranges = append(newranges, ranges.(*objectio.BlockInfoSlice).GetBytes(i)...)
					continue
				}
				partialResults[0] = partialResults[0].(int64) + int64(blk.MetaLocation().Rows())
			}
		} else if partialResults != nil {
			columnMap := make(map[int]int)
			for i := range n.AggList {
				agg := n.AggList[i].Expr.(*plan.Expr_F)
				if agg.F.Func.ObjName == "starcount" {
					continue
				}
				args := agg.F.Args[0]
				col, ok := args.Expr.(*plan.Expr_Col)
				if !ok {
					if _, ok := args.Expr.(*plan.Expr_Lit); ok {
						if agg.F.Func.ObjName == "count" {
							agg.F.Func.ObjName = "starcount"
							continue
						}
					}
					partialResults = nil
					break
				}
				columnMap[int(col.Col.ColPos)] = int(n.TableDef.Cols[int(col.Col.ColPos)].Seqnum)
			}
			for i := 1; i < ranges.Len(); i++ {
				if partialResults == nil {
					break
				}
				blk := ranges.(*objectio.BlockInfoSlice).Get(i)
				if !blk.CanRemote || !blk.DeltaLocation().IsEmpty() {
					newranges = append(newranges, ranges.GetBytes(i)...)
					continue
				}
				var objMeta objectio.ObjectMeta
				location := blk.MetaLocation()
				var fs fileservice.FileService
				fs, err = fileservice.Get[fileservice.FileService](c.proc.FileService, defines.SharedFileServiceName)
				if err != nil {
					return nil, nil, nil, err
				}
				objMeta, err = objectio.FastLoadObjectMeta(ctx, &location, false, fs)
				if err != nil {
					partialResults = nil
					break
				} else {
					objDataMeta := objMeta.MustDataMeta()
					blkMeta := objDataMeta.GetBlockMeta(uint32(location.ID()))
					for i := range n.AggList {
						agg := n.AggList[i].Expr.(*plan.Expr_F)
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
								partialResults = nil
							} else {
								if partialResults[i] == nil {
									partialResults[i] = zm.GetMin()
									partialResultTypes[i] = zm.GetType()
								} else {
									switch zm.GetType() {
									case types.T_bool:
										partialResults[i] = !partialResults[i].(bool) || !types.DecodeFixed[bool](zm.GetMinBuf())
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
										if min.Less(&ts) {
											partialResults[i] = min
										}
									case types.T_Rowid:
										min := types.DecodeFixed[types.Rowid](zm.GetMinBuf())
										if min.Less(partialResults[i].(types.Rowid)) {
											partialResults[i] = min
										}
									case types.T_Blockid:
										min := types.DecodeFixed[types.Blockid](zm.GetMinBuf())
										if min.Less(partialResults[i].(types.Blockid)) {
											partialResults[i] = min
										}
									}
								}
							}
						case "max":
							col := agg.F.Args[0].Expr.(*plan.Expr_Col)
							zm := blkMeta.ColumnMeta(uint16(columnMap[int(col.Col.ColPos)])).ZoneMap()
							if zm.GetType().FixedLength() < 0 {
								partialResults = nil
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
										if max.Greater(&ts) {
											partialResults[i] = max
										}
									case types.T_Rowid:
										max := types.DecodeFixed[types.Rowid](zm.GetMaxBuf())
										if max.Great(partialResults[i].(types.Rowid)) {
											partialResults[i] = max
										}
									case types.T_Blockid:
										max := types.DecodeFixed[types.Blockid](zm.GetMaxBuf())
										if max.Great(partialResults[i].(types.Blockid)) {
											partialResults[i] = max
										}
									}
								}
							}
						default:
						}
						if partialResults == nil {
							break
						}
					}
					if partialResults == nil {
						break
					}
				}
			}
		}
		if ranges.Size() == len(newranges) {
			partialResults = nil
		} else if partialResults != nil {
			ranges.SetBytes(newranges)
		}
		if partialResults == nil {
			partialResultTypes = nil
		}
	}
	// n.AggList = nil

	// some log for finding a bug.
	tblId := rel.GetTableID(ctx)
	expectedLen := ranges.Len()
	c.proc.Debugf(ctx, "cn generateNodes, tbl %d ranges is %d", tblId, expectedLen)

	// if len(ranges) == 0 indicates that it's a temporary table.
	if ranges.Len() == 0 && n.TableDef.TableType != catalog.SystemOrdinaryRel {
		nodes = make(engine.Nodes, len(c.cnList))
		for i, node := range c.cnList {
			nodes[i] = engine.Node{
				Id:   node.Id,
				Addr: node.Addr,
				Mcpu: c.generateCPUNumber(node.Mcpu, int(n.Stats.BlockNum)),
			}
		}
		return nodes, partialResults, partialResultTypes, nil
	}

	engineType := rel.GetEngineType()
	// for multi cn in launch mode, put all payloads in current CN, maybe delete this in the future
	// for an ordered scan, put all paylonds in current CN
	// or sometimes force on one CN
	if isLaunchMode(c.cnList) || len(n.OrderBy) > 0 || ranges.Len() < plan2.BlockThresholdForOneCN || n.Stats.ForceOneCN {
		return putBlocksInCurrentCN(c, ranges.GetAllBytes(), n), partialResults, partialResultTypes, nil
	}
	// disttae engine
	if engineType == engine.Disttae {
		nodes, err := shuffleBlocksToMultiCN(c, ranges.(*objectio.BlockInfoSlice), n)
		return nodes, partialResults, partialResultTypes, err
	}
	// maybe temp table on memengine , just put payloads in average
	return putBlocksInAverage(c, ranges, n), partialResults, partialResultTypes, nil
}

func putBlocksInAverage(c *Compile, ranges engine.Ranges, n *plan.Node) engine.Nodes {
	var nodes engine.Nodes
	step := (ranges.Len() + len(c.cnList) - 1) / len(c.cnList)
	for i := 0; i < ranges.Len(); i += step {
		j := i / step
		if i+step >= ranges.Len() {
			if isSameCN(c.cnList[j].Addr, c.addr) {
				if len(nodes) == 0 {
					nodes = append(nodes, engine.Node{
						Addr: c.addr,
						Mcpu: c.generateCPUNumber(ncpu, int(n.Stats.BlockNum)),
					})
				}
				nodes[0].Data = append(nodes[0].Data, ranges.Slice(i, ranges.Len())...)
			} else {
				nodes = append(nodes, engine.Node{
					Id:   c.cnList[j].Id,
					Addr: c.cnList[j].Addr,
					Mcpu: c.generateCPUNumber(c.cnList[j].Mcpu, int(n.Stats.BlockNum)),
					Data: ranges.Slice(i, ranges.Len()),
				})
			}
		} else {
			if isSameCN(c.cnList[j].Addr, c.addr) {
				if len(nodes) == 0 {
					nodes = append(nodes, engine.Node{
						Addr: c.addr,
						Mcpu: c.generateCPUNumber(ncpu, int(n.Stats.BlockNum)),
					})
				}
				nodes[0].Data = append(nodes[0].Data, ranges.Slice(i, i+step)...)
			} else {
				nodes = append(nodes, engine.Node{
					Id:   c.cnList[j].Id,
					Addr: c.cnList[j].Addr,
					Mcpu: c.generateCPUNumber(c.cnList[j].Mcpu, int(n.Stats.BlockNum)),
					Data: ranges.Slice(i, i+step),
				})
			}
		}
	}
	return nodes
}

func shuffleBlocksToMultiCN(c *Compile, ranges *objectio.BlockInfoSlice, n *plan.Node) (engine.Nodes, error) {
	var nodes engine.Nodes
	// add current CN
	nodes = append(nodes, engine.Node{
		Addr: c.addr,
		Mcpu: c.generateCPUNumber(ncpu, int(n.Stats.BlockNum)),
	})
	// add memory table block
	nodes[0].Data = append(nodes[0].Data, ranges.GetBytes(0)...)
	*ranges = ranges.Slice(1, ranges.Len())
	// only memory table block
	if ranges.Len() == 0 {
		return nodes, nil
	}
	// only one cn
	if len(c.cnList) == 1 {
		nodes[0].Data = append(nodes[0].Data, ranges.GetAllBytes()...)
		return nodes, nil
	}
	// put dirty blocks which can't be distributed remotely in current CN.
	newRanges := make(objectio.BlockInfoSlice, 0, ranges.Len())
	for i := 0; i < ranges.Len(); i++ {
		if ranges.Get(i).CanRemote {
			newRanges = append(newRanges, ranges.GetBytes(i)...)
		} else {
			nodes[0].Data = append(nodes[0].Data, ranges.GetBytes(i)...)
		}
	}

	// add the rest of CNs in list
	for i := range c.cnList {
		if c.cnList[i].Addr != c.addr {
			nodes = append(nodes, engine.Node{
				Id:   c.cnList[i].Id,
				Addr: c.cnList[i].Addr,
				Mcpu: c.generateCPUNumber(c.cnList[i].Mcpu, int(n.Stats.BlockNum)),
			})
		}
	}

	sort.Slice(nodes, func(i, j int) bool { return nodes[i].Addr < nodes[j].Addr })

	if n.Stats.HashmapStats != nil && n.Stats.HashmapStats.Shuffle && n.Stats.HashmapStats.ShuffleType == plan.ShuffleType_Range {
		err := shuffleBlocksByRange(c, newRanges, n, nodes)
		if err != nil {
			return nil, err
		}
	} else {
		shuffleBlocksByHash(c, newRanges, nodes)
	}

	minWorkLoad := math.MaxInt32
	maxWorkLoad := 0
	// remove empty node from nodes
	var newNodes engine.Nodes
	for i := range nodes {
		if len(nodes[i].Data) > maxWorkLoad {
			maxWorkLoad = len(nodes[i].Data) / objectio.BlockInfoSize
		}
		if len(nodes[i].Data) < minWorkLoad {
			minWorkLoad = len(nodes[i].Data) / objectio.BlockInfoSize
		}
		if len(nodes[i].Data) > 0 {
			newNodes = append(newNodes, nodes[i])
		}
	}
	if minWorkLoad*2 < maxWorkLoad {
		logstring := fmt.Sprintf("read table %v ,workload %v blocks among %v nodes not balanced, max %v, min %v,", n.TableDef.Name, ranges.Len(), len(newNodes), maxWorkLoad, minWorkLoad)
		logstring = logstring + " cnlist: "
		for i := range c.cnList {
			logstring = logstring + c.cnList[i].Addr + " "
		}
		c.proc.Warnf(c.ctx, logstring)
	}
	return newNodes, nil
}

func shuffleBlocksByHash(c *Compile, ranges objectio.BlockInfoSlice, nodes engine.Nodes) {
	for i := 0; i < ranges.Len(); i++ {
		unmarshalledBlockInfo := ranges.Get(i)
		// get timestamp in objName to make sure it is random enough
		objTimeStamp := unmarshalledBlockInfo.MetaLocation().Name()[:7]
		index := plan2.SimpleCharHashToRange(objTimeStamp, uint64(len(c.cnList)))
		nodes[index].Data = append(nodes[index].Data, ranges.GetBytes(i)...)
	}
}

func shuffleBlocksByRange(c *Compile, ranges objectio.BlockInfoSlice, n *plan.Node, nodes engine.Nodes) error {
	var objDataMeta objectio.ObjectDataMeta
	var objMeta objectio.ObjectMeta

	var shuffleRangeUint64 []uint64
	var shuffleRangeInt64 []int64
	var init bool
	var index uint64
	for i := 0; i < ranges.Len(); i++ {
		unmarshalledBlockInfo := ranges.Get(i)
		location := unmarshalledBlockInfo.MetaLocation()
		fs, err := fileservice.Get[fileservice.FileService](c.proc.FileService, defines.SharedFileServiceName)
		if err != nil {
			return err
		}
		if !objectio.IsSameObjectLocVsMeta(location, objDataMeta) {
			if objMeta, err = objectio.FastLoadObjectMeta(c.ctx, &location, false, fs); err != nil {
				return err
			}
			objDataMeta = objMeta.MustDataMeta()
		}
		blkMeta := objDataMeta.GetBlockMeta(uint32(location.ID()))
		zm := blkMeta.MustGetColumn(uint16(n.Stats.HashmapStats.ShuffleColIdx)).ZoneMap()
		if !zm.IsInited() {
			// a block with all null will send to first CN
			nodes[0].Data = append(nodes[0].Data, ranges.GetBytes(i)...)
			continue
		}
		if !init {
			init = true
			switch zm.GetType() {
			case types.T_int64, types.T_int32, types.T_int16:
				shuffleRangeInt64 = plan2.ShuffleRangeReEvalSigned(n.Stats.HashmapStats.Ranges, len(c.cnList), n.Stats.HashmapStats.Nullcnt, int64(n.Stats.TableCnt))
			case types.T_uint64, types.T_uint32, types.T_uint16, types.T_varchar, types.T_char, types.T_text, types.T_bit:
				shuffleRangeUint64 = plan2.ShuffleRangeReEvalUnsigned(n.Stats.HashmapStats.Ranges, len(c.cnList), n.Stats.HashmapStats.Nullcnt, int64(n.Stats.TableCnt))
			}
		}
		if shuffleRangeUint64 != nil {
			index = plan2.GetRangeShuffleIndexForZMUnsignedSlice(shuffleRangeUint64, zm)
		} else if shuffleRangeInt64 != nil {
			index = plan2.GetRangeShuffleIndexForZMSignedSlice(shuffleRangeInt64, zm)
		} else {
			index = plan2.GetRangeShuffleIndexForZM(n.Stats.HashmapStats.ShuffleColMin, n.Stats.HashmapStats.ShuffleColMax, zm, uint64(len(c.cnList)))
		}
		nodes[index].Data = append(nodes[index].Data, ranges.GetBytes(i)...)
	}
	return nil
}

func putBlocksInCurrentCN(c *Compile, ranges []byte, n *plan.Node) engine.Nodes {
	var nodes engine.Nodes
	// add current CN
	nodes = append(nodes, engine.Node{
		Addr: c.addr,
		Mcpu: c.generateCPUNumber(ncpu, int(n.Stats.BlockNum)),
	})
	nodes[0].Data = append(nodes[0].Data, ranges...)
	return nodes
}

func validScopeCount(ss []*Scope) int {
	var cnt int

	for _, s := range ss {
		if s.IsEnd {
			continue
		}
		cnt++
	}
	return cnt
}

func extraRegisters(ss []*Scope, i int) []*process.WaitRegister {
	regs := make([]*process.WaitRegister, 0, len(ss))
	for _, s := range ss {
		if s.IsEnd {
			continue
		}
		regs = append(regs, s.Proc.Reg.MergeReceivers[i])
	}
	return regs
}

func dupType(typ *plan.Type) types.Type {
	return types.New(types.T(typ.Id), typ.Width, typ.Scale)
}

// Update the specific scopes's instruction to true
// then update the current idx
func (c *Compile) setAnalyzeCurrent(updateScopes []*Scope, nextId int) {
	if updateScopes != nil {
		updateScopesLastFlag(updateScopes)
	}

	c.anal.curr = nextId
	c.anal.isFirst = true
}

func updateScopesLastFlag(updateScopes []*Scope) {
	for _, s := range updateScopes {
		if len(s.Instructions) == 0 {
			continue
		}
		last := len(s.Instructions) - 1
		s.Instructions[last].IsLast = true
	}
}

func isLaunchMode(cnlist engine.Nodes) bool {
	for i := range cnlist {
		if !isSameCN(cnlist[0].Addr, cnlist[i].Addr) {
			return false
		}
	}
	return true
}

func isSameCN(addr string, currentCNAddr string) bool {
	// just a defensive judgment. In fact, we shouldn't have received such data.

	parts1 := strings.Split(addr, ":")
	if len(parts1) != 2 {
		logutil.Debugf("compileScope received a malformed cn address '%s', expected 'ip:port'", addr)
		return true
	}
	parts2 := strings.Split(currentCNAddr, ":")
	if len(parts2) != 2 {
		logutil.Debugf("compileScope received a malformed current-cn address '%s', expected 'ip:port'", currentCNAddr)
		return true
	}
	return parts1[0] == parts2[0]
}

func (s *Scope) affectedRows() uint64 {
	affectedRows := uint64(0)
	for _, in := range s.Instructions {
		if arg, ok := in.Arg.(vm.ModificationArgument); ok {
			if marg, ok := arg.(*mergeblock.Argument); ok {
				return marg.AffectedRows()
			}
			affectedRows += arg.AffectedRows()
		}
	}
	return affectedRows
}

func (c *Compile) runSql(sql string) error {
	if sql == "" {
		return nil
	}
	res, err := c.runSqlWithResult(sql)
	if err != nil {
		return err
	}
	res.Close()
	return nil
}

func (c *Compile) runSqlWithResult(sql string) (executor.Result, error) {
	v, ok := moruntime.ProcessLevelRuntime().GetGlobalVariables(moruntime.InternalSQLExecutor)
	if !ok {
		panic("missing lock service")
	}
	exec := v.(executor.SQLExecutor)
	opts := executor.Options{}.
		// All runSql and runSqlWithResult is a part of input sql, can not incr statement.
		// All these sub-sql's need to be rolled back and retried en masse when they conflict in pessimistic mode
		WithDisableIncrStatement().
		WithTxn(c.proc.TxnOperator).
		WithDatabase(c.db).
		WithTimeZone(c.proc.SessionInfo.TimeZone)
	return exec.Exec(c.proc.Ctx, sql, opts)
}

func evalRowsetData(proc *process.Process,
	exprs []*plan.RowsetExpr, vec *vector.Vector, exprExecs []colexec.ExpressionExecutor,
) error {
	var bats []*batch.Batch

	vec.ResetArea()
	bats = []*batch.Batch{batch.EmptyForConstFoldBatch}
	if len(exprExecs) > 0 {
		for i, expr := range exprExecs {
			val, err := expr.Eval(proc, bats)
			if err != nil {
				return err
			}
			if err := vec.Copy(val, int64(exprs[i].RowPos), 0, proc.Mp()); err != nil {
				return err
			}
		}
	} else {
		for _, expr := range exprs {
			if expr.Pos >= 0 {
				continue
			}
			val, err := colexec.EvalExpressionOnce(proc, expr.Expr, bats)
			if err != nil {
				return err
			}
			if err := vec.Copy(val, int64(expr.RowPos), 0, proc.Mp()); err != nil {
				val.Free(proc.Mp())
				return err
			}
			val.Free(proc.Mp())
		}
	}
	return nil
}

func (c *Compile) newInsertMergeScope(arg *insert.Argument, ss []*Scope) *Scope {
	// see errors.Join()
	n := 0
	for _, s := range ss {
		if !s.IsEnd {
			n++
		}
	}
	ss2 := make([]*Scope, 0, n)
	for _, s := range ss {
		if !s.IsEnd {
			ss2 = append(ss2, s)
		}
	}
	insert := &vm.Instruction{
		Op:  vm.Insert,
		Arg: arg,
	}
	for i := range ss2 {
		ss2[i].Instructions = append(ss2[i].Instructions, dupInstruction(insert, nil, i))
	}
	return c.newMergeScope(ss2)
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

	txnTrace.GetService().TxnError(c.proc.TxnOperator, err)

	v, ok := moruntime.ProcessLevelRuntime().
		GetGlobalVariables(moruntime.EnableCheckInvalidRCErrors)
	if !ok || !v.(bool) {
		return
	}

	c.proc.Fatalf(c.ctx, "BUG(RC): txn %s retry %d, error %+v\n",
		hex.EncodeToString(c.proc.TxnOperator.Txn().ID),
		retry,
		err.Error())
}

func (c *Compile) SetOriginSQL(sql string) {
	c.originSQL = sql
}

func (c *Compile) SetBuildPlanFunc(buildPlanFunc func() (*plan2.Plan, error)) {
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
			return err
		}
	}

	return nil
}

// runDetectSql runs the fk detecting sql
func runDetectSql(c *Compile, sql string) error {
	res, err := c.runSqlWithResult(sql)
	if err != nil {
		c.proc.Errorf(c.ctx, "The sql that caused the fk self refer check failed is %s, and generated background sql is %s", c.sql, sql)
		return err
	}
	defer res.Close()

	if res.Batches != nil {
		vs := res.Batches[0].Vecs
		if vs != nil && vs[0].Length() > 0 {
			yes := vector.GetFixedAt[bool](vs[0], 0)
			if !yes {
				return moerr.NewErrFKNoReferencedRow2(c.ctx)
			}
		}
	}
	return nil
}

// runDetectFkReferToDBSql runs the fk detecting sql
func runDetectFkReferToDBSql(c *Compile, sql string) error {
	res, err := c.runSqlWithResult(sql)
	if err != nil {
		c.proc.Errorf(c.ctx, "The sql that caused the fk self refer check failed is %s, and generated background sql is %s", c.sql, sql)
		return err
	}
	defer res.Close()

	if res.Batches != nil {
		vs := res.Batches[0].Vecs
		if vs != nil && vs[0].Length() > 0 {
			yes := vector.GetFixedAt[bool](vs[0], 0)
			if yes {
				return moerr.NewInternalError(c.ctx,
					"can not drop database. It has been referenced by foreign keys")
			}
		}
	}
	return nil
}

func getEngineNode(c *Compile) engine.Node {
	if c.execType == plan2.ExecTypeTP {
		return engine.Node{Addr: c.addr, Mcpu: 1}
	} else {
		return engine.Node{Addr: c.addr, Mcpu: ncpu}
	}
}
