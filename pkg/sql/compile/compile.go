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
	"net"
	"sort"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/cnservice/cnclient"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
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
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/intersect"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/intersectall"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/lockop"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/merge"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergeblock"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergecte"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergedelete"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergerecursive"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/minus"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/multi_update"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/output"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/sample"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/source"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/table_scan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/value_scan"
	"github.com/matrixorigin/matrixone/pkg/sql/crt"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/rule"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
	mokafka "github.com/matrixorigin/matrixone/pkg/stream/adapter/kafka"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	txnTrace "github.com/matrixorigin/matrixone/pkg/txn/trace"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace/statistic"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/panjf2000/ants/v2"
	"go.uber.org/zap"
)

// Note: Now the cost going from stat is actually the number of rows, so we can only estimate a number for the size of each row.
// The current insertion of around 200,000 rows triggers cn to write s3 directly
const (
	DistributedThreshold     uint64 = 10 * mpool.MB
	SingleLineSizeEstimate   uint64 = 300 * mpool.B
	shuffleChannelBufferSize        = 32

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
	c.sql = sql
	c.proc.SetMessageBoard(c.MessageBoard)
	c.stmt = stmt
	c.addr = addr
	c.isInternal = isInternal
	c.cnLabel = cnLabel
	c.startAt = startAt
	c.disableRetry = false
	c.ncpu = system.GoMaxProcs()
	c.lockMeta = NewLockMeta()
	if c.proc.GetTxnOperator() != nil {
		// TODO: The action of updating the WriteOffset logic should be executed in the `func (c *Compile) Run(_ uint64)` method.
		// However, considering that the delay ranges are not completed yet, the UpdateSnapshotWriteOffset() and
		// the assignment of `Compile.TxnOffset` should be moved into the `func (c *Compile) Run(_ uint64)` method in the later stage.
		c.proc.GetTxnOperator().GetWorkspace().UpdateSnapshotWriteOffset()
		c.TxnOffset = c.proc.GetTxnOperator().GetWorkspace().GetSnapshotWriteOffset()
	} else {
		c.TxnOffset = 0
	}
	return c
}

func (c *Compile) Release() {
	if c == nil {
		return
	}
	if c.proc != nil {
		c.proc.ResetQueryContext()
		c.proc.ResetCloneTxnOperator()
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

func (c *Compile) Reset(proc *process.Process, startAt time.Time, fill func(*batch.Batch, *perfcounter.CounterSet) error, sql string) {
	// clean up the process for a new query.
	proc.ResetQueryContext()
	proc.ResetCloneTxnOperator()
	c.proc = proc

	c.fill = fill
	c.sql = sql
	c.affectRows.Store(0)
	c.anal.Reset(c.isPrepare, c.IsTpQuery())

	if c.lockMeta != nil {
		c.lockMeta.reset(c.proc)
	}
	for _, s := range c.scopes {
		s.Reset(c)
	}

	for _, e := range c.filterExprExes {
		e.ResetForNextQuery()
	}

	c.MessageBoard = c.MessageBoard.Reset()
	proc.SetMessageBoard(c.MessageBoard)
	c.counterSet.Reset()

	for _, f := range c.fuzzys {
		f.reset()
	}
	c.startAt = startAt
	if c.proc.GetTxnOperator() != nil {
		c.proc.GetTxnOperator().GetWorkspace().UpdateSnapshotWriteOffset()
		c.TxnOffset = c.proc.GetTxnOperator().GetWorkspace().GetSnapshotWriteOffset()

		// all scopes should update the txn offset, or the reader will receive a 0 txnOffset,
		// that cause a dml statement can not see the previous statements' operations.
		if len(c.scopes) > 0 {
			for i := range c.scopes {
				UpdateScopeTxnOffset(c.scopes[i], c.TxnOffset)
			}
		}
	} else {
		c.TxnOffset = 0
	}
}

func UpdateScopeTxnOffset(scope *Scope, txnOffset int) {
	scope.TxnOffset = txnOffset
	for i := range scope.PreScopes {
		UpdateScopeTxnOffset(scope.PreScopes[i], txnOffset)
	}
}

func (c *Compile) clear() {
	if c.anal != nil {
		c.anal.release()
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
	c.affectRows.Store(0)
	c.addr = ""
	c.db = ""
	c.tenant = ""
	c.uid = ""
	c.sql = ""
	c.originSQL = ""
	c.anal = nil
	c.e = nil

	c.proc.Free()
	c.proc = nil

	c.cnList = c.cnList[:0]
	c.stmt = nil
	c.startAt = time.Time{}
	c.needLockMeta = false
	c.isInternal = false
	c.isPrepare = false
	c.hasMergeOp = false
	c.needBlock = false
	c.ignorePublish = false
	c.adjustTableExtraFunc = nil
	c.disableDropAutoIncrement = false
	c.keepAutoIncrement = 0
	c.disableLock = false

	if c.lockMeta != nil {
		c.lockMeta.clear(c.proc)
		c.lockMeta = nil
	}

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
	case Replace:
		return s.replace(c)
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

func (c *Compile) canRetry(err error) bool {
	return !c.disableRetry && c.isRetryErr(err)
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
func (c *Compile) prePipelineInitializer() (err error) {
	// do table lock.
	if err = c.lockMeta.doLock(c.e, c.proc); err != nil {
		return err
	}
	if err = c.lockTable(); err != nil {
		return err
	}

	// init data source.
	for _, s := range c.scopes {
		if err = s.InitAllDataSource(c); err != nil {
			return err
		}
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

	if c.IsTpQuery() && len(c.scopes) == 1 {
		if err = c.run(c.scopes[0]); err != nil {
			return err
		}
	} else {
		errC := make(chan error, len(c.scopes))
		for i := range c.scopes {
			scope := c.scopes[i]
			errSubmit := ants.Submit(func() {
				defer func() {
					if e := recover(); e != nil {
						err := moerr.ConvertPanicError(c.proc.Ctx, e)
						c.proc.Error(c.proc.Ctx, "panic in run",
							zap.String("sql", commonutil.Abbreviate(c.sql, 500)),
							zap.String("error", err.Error()))
						errC <- err
					}
				}()
				errC <- c.run(scope)
			})
			if errSubmit != nil {
				errC <- errSubmit
			}
		}

		var errToThrowOut error
		for i := 0; i < cap(errC); i++ {
			e := <-errC

			// cancel this query if the first error occurs.
			if e != nil && errToThrowOut == nil {
				errToThrowOut = e

				// cancel all scope tree.
				for j := range c.scopes {
					if c.scopes[j].Proc != nil {
						c.scopes[j].Proc.Cancel(e)
					}
				}
			}

			// if any error already return is retryable, we should throw this one
			// to make sure query will retry.
			if e != nil && c.isRetryErr(e) {
				errToThrowOut = e
			}
		}
		close(errC)

		if errToThrowOut != nil {
			return errToThrowOut
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
		switch qry.Query.StmtType {
		case plan.Query_REPLACE:
			return []*Scope{
				newScope(Replace).
					withPlan(pn),
			}, nil
		}
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
	for _, tbl := range c.lockTables {
		typ := plan2.MakeTypeByPlan2Type(tbl.PrimaryColTyp)
		return lockop.LockTable(
			c.e,
			c.proc,
			tbl.TableId,
			typ,
			false)

	}
	return nil
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

func isAvailable(client morpc.RPCClient, addr string) bool {
	_, _, err := net.SplitHostPort(addr)
	if err != nil {
		logutil.Warnf("compileScope received a malformed cn address '%s', expected 'ip:port'", addr)
		return false
	}
	logutil.Debugf("ping %s start", addr)
	ctx, cancel := context.WithTimeoutCause(context.Background(), 500*time.Millisecond, moerr.CauseIsAvailable)
	defer cancel()
	err = client.Ping(ctx, addr)
	if err != nil {
		err = moerr.AttachCause(ctx, err)
		// ping failed
		logutil.Debugf("ping %s err %+v\n", addr, err)
		return false
	}
	return true
}

func (c *Compile) removeUnavailableCN() {
	client := cnclient.GetPipelineClient(
		c.proc.GetService(),
	)
	if client == nil {
		return
	}
	i := 0
	for _, cn := range c.cnList {
		if isSameCN(c.addr, cn.Addr) || isAvailable(client.Raw(), cn.Addr) {
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
	if c.proc == nil || c.proc.Base.QueryClient == nil {
		return cnList, nil
	}
	cnID := c.proc.GetService()
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

func (c *Compile) compileQuery(qry *plan.Query) ([]*Scope, error) {
	var err error

	start := time.Now()
	defer func() {
		v2.TxnStatementCompileQueryHistogram.Observe(time.Since(start).Seconds())
	}()

	c.execType = plan2.GetExecType(c.pn.GetQuery(), c.getHaveDDL(), c.isPrepare)

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

	if c.isPrepare && !c.IsTpQuery() {
		return nil, cantCompileForPrepareErr
	}

	ncpu := int32(c.ncpu)
	if qry.MaxDop > 0 {
		ncpu = min(ncpu, int32(qry.MaxDop))
	}

	plan2.CalcQueryDOP(c.pn, ncpu, len(c.cnList), c.execType)

	c.initAnalyzeModule(qry)
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
					Ch2: make(chan process.PipelineSignal, c.ncpu),
				}
			} else {
				wr = &process.WaitRegister{
					Ch2: make(chan process.PipelineSignal, 1),
				}
			}
			c.appendStepRegs(s, nodeId, wr)
		}
	}
	return nil
}

func (c *Compile) compileSteps(qry *plan.Query, ss []*Scope, step int32) ([]*Scope, error) {
	if qry.Nodes[step].NodeType == plan.Node_SINK {
		return ss, nil
	}

	switch qry.StmtType {
	case plan.Query_DELETE, plan.Query_INSERT, plan.Query_UPDATE:
		updateScopesLastFlag(ss)
		return ss, nil
	default:
		var rs *Scope
		if c.IsSingleScope(ss) {
			rs = ss[0]
		} else {
			ss = c.mergeShuffleScopesIfNeeded(ss, false)
			rs = c.newMergeScope(ss)
		}
		updateScopesLastFlag([]*Scope{rs})
		c.setAnalyzeCurrent([]*Scope{rs}, c.anal.curNodeIdx)
		rs.setRootOperator(
			output.NewArgument().
				WithFunc(c.fill).
				WithBlock(c.needBlock),
		)
		return []*Scope{rs}, nil
	}
}

func (c *Compile) compilePlanScope(step int32, curNodeIdx int32, nodes []*plan.Node) ([]*Scope, error) {
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

	if node.Limit != nil {
		if cExpr, ok := node.Limit.Expr.(*plan.Expr_Lit); ok {
			if cval, ok := cExpr.Lit.Value.(*plan.Literal_U64Val); ok {
				if cval.U64Val == 0 {
					// optimize for limit 0
					rs := c.newEmptyMergeScope()
					rs.Proc = c.proc.NewNoContextChildProc(0)
					return c.compileLimit(node, []*Scope{rs}), nil
				}
			}
		}
	}

	switch node.NodeType {
	case plan.Node_VALUE_SCAN:
		c.setAnalyzeCurrent(nil, int(curNodeIdx))
		ss, err = c.compileValueScan(node)
		if err != nil {
			return nil, err
		}
		ss = c.compileSort(node, c.compileProjection(node, ss))
		return ss, nil
	case plan.Node_EXTERNAL_SCAN:
		if node.ObjRef != nil {
			c.appendMetaTables(node.ObjRef)
		}
		nodeCopy := plan2.DeepCopyNode(node)

		c.setAnalyzeCurrent(nil, int(curNodeIdx))
		ss, err = c.compileExternScan(nodeCopy)
		if err != nil {
			return nil, err
		}
		ss = c.compileSort(node, c.compileProjection(node, c.compileRestrict(nodeCopy, ss)))
		return ss, nil
	case plan.Node_TABLE_SCAN:
		c.appendMetaTables(node.ObjRef)

		c.setAnalyzeCurrent(nil, int(curNodeIdx))
		ss, err = c.compileTableScan(node)
		if err != nil {
			return nil, err
		}
		ss = c.compileProjection(node, c.compileRestrict(node, ss))
		if node.Offset != nil {
			ss = c.compileOffset(node, ss)
		}
		if node.Limit != nil {
			ss = c.compileLimit(node, ss)
		}
		return ss, nil
	case plan.Node_SOURCE_SCAN:
		c.setAnalyzeCurrent(nil, int(curNodeIdx))
		ss, err = c.compileSourceScan(node)
		if err != nil {
			return nil, err
		}
		ss = c.compileSort(node, c.compileProjection(node, c.compileRestrict(node, ss)))
		return ss, nil
	case plan.Node_FILTER, plan.Node_PROJECT:
		ss, err = c.compilePlanScope(step, node.Children[0], nodes)
		if err != nil {
			return nil, err
		}

		c.setAnalyzeCurrent(ss, int(curNodeIdx))
		ss = c.compileSort(node, c.compileProjection(node, c.compileRestrict(node, ss)))
		return ss, nil
	case plan.Node_AGG:
		ss, err = c.compilePlanScope(step, node.Children[0], nodes)
		if err != nil {
			return nil, err
		}

		groupInfo := constructGroup(c.proc.Ctx, node, nodes[node.Children[0]], false, 0, c.proc)
		defer groupInfo.Release()
		anyDistinctAgg := groupInfo.AnyDistinctAgg()

		c.setAnalyzeCurrent(ss, int(curNodeIdx))
		if node.Stats.HashmapStats != nil && node.Stats.HashmapStats.Shuffle {
			ss = c.compileSort(node, c.compileProjection(node, c.compileRestrict(node, c.compileShuffleGroup(node, ss, nodes))))
			return ss, nil
		} else if c.IsSingleScope(ss) {
			ss = c.compileSort(node, c.compileProjection(node, c.compileRestrict(node, c.compileTPGroup(node, ss, nodes))))
			return ss, nil
		} else {
			ss = c.compileSort(node, c.compileProjection(node, c.compileRestrict(node, c.compileMergeGroup(node, ss, nodes, anyDistinctAgg))))
			return ss, nil
		}
	case plan.Node_SAMPLE:
		ss, err = c.compilePlanScope(step, node.Children[0], nodes)
		if err != nil {
			return nil, err
		}

		c.setAnalyzeCurrent(ss, int(curNodeIdx))
		ss = c.compileSort(node, c.compileProjection(node, c.compileRestrict(node, c.compileSample(node, ss))))
		return ss, nil
	case plan.Node_WINDOW:
		ss, err = c.compilePlanScope(step, node.Children[0], nodes)
		if err != nil {
			return nil, err
		}

		c.setAnalyzeCurrent(ss, int(curNodeIdx))
		ss = c.compileSort(node, c.compileProjection(node, c.compileRestrict(node, c.compileWin(node, ss))))
		return ss, nil
	case plan.Node_TIME_WINDOW:
		ss, err = c.compilePlanScope(step, node.Children[0], nodes)
		if err != nil {
			return nil, err
		}

		c.setAnalyzeCurrent(ss, int(curNodeIdx))
		ss = c.compileProjection(node, c.compileRestrict(node, c.compileTimeWin(node, c.compileSort(node, ss))))
		return ss, nil
	case plan.Node_FILL:
		ss, err = c.compilePlanScope(step, node.Children[0], nodes)
		if err != nil {
			return nil, err
		}

		c.setAnalyzeCurrent(ss, int(curNodeIdx))
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
		ss = c.compileSort(node, c.compileJoin(node, nodes[node.Children[0]], nodes[node.Children[1]], left, right))
		return ss, nil
	case plan.Node_SORT:
		ss, err = c.compilePlanScope(step, node.Children[0], nodes)
		if err != nil {
			return nil, err
		}

		c.setAnalyzeCurrent(ss, int(curNodeIdx))
		ss = c.compileProjection(node, c.compileRestrict(node, c.compileSort(node, ss)))
		return ss, nil
	case plan.Node_PARTITION:
		ss, err = c.compilePlanScope(step, node.Children[0], nodes)
		if err != nil {
			return nil, err
		}

		c.setAnalyzeCurrent(ss, int(curNodeIdx))
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
		ss = c.compileSort(node, c.compileUnion(node, left, right))
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
		ss = c.compileSort(node, c.compileMinusAndIntersect(node, left, right, node.NodeType))
		return ss, nil
	case plan.Node_UNION_ALL:
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
		ss = c.compileSort(node, c.compileUnionAll(node, left, right))
		return ss, nil
	case plan.Node_DELETE:
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
	case plan.Node_ON_DUPLICATE_KEY:
		ss, err = c.compilePlanScope(step, node.Children[0], nodes)
		if err != nil {
			return nil, err
		}

		c.setAnalyzeCurrent(ss, int(curNodeIdx))
		ss, err = c.compileOnduplicateKey(node, ss)
		if err != nil {
			return nil, err
		}
		return ss, nil
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
		c.appendMetaTables(node.ObjRef)
		ss, err = c.compilePlanScope(step, node.Children[0], nodes)
		if err != nil {
			return nil, err
		}

		node.NotCacheable = true
		c.setAnalyzeCurrent(ss, int(curNodeIdx))
		return c.compileInsert(nodes, node, ss)
	case plan.Node_MULTI_UPDATE:
		for _, updateCtx := range node.UpdateCtxList {
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
		ss = c.compileSort(node, c.compileProjection(node, c.compileRestrict(node, ss)))
		return ss, nil
	case plan.Node_SINK_SCAN:
		c.setAnalyzeCurrent(nil, int(curNodeIdx))
		ss, err = c.compileSinkScanNode(node, curNodeIdx)
		if err != nil {
			return nil, err
		}
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
		ss = c.compileSort(node, c.compileApply(node, nodes[node.Children[1]], left))
		return ss, nil
	case plan.Node_POSTDML:
		ss, err = c.compilePlanScope(step, node.Children[0], nodes)
		if err != nil {
			return nil, err
		}

		c.setAnalyzeCurrent(ss, int(curNodeIdx))
		ss = c.compilePostDml(node, ss)
		return ss, nil

	default:
		return nil, moerr.NewNYI(c.proc.Ctx, fmt.Sprintf("query '%s'", node))
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

func (c *Compile) compileSourceScan(node *plan.Node) ([]*Scope, error) {
	_, span := trace.Start(c.proc.Ctx, "compileSourceScan")
	defer span.End()
	configs := make(map[string]interface{})
	for _, def := range node.TableDef.Defs {
		switch v := def.Def.(type) {
		case *plan.TableDef_DefType_Properties:
			for _, p := range v.Properties.Properties {
				configs[p.Key] = p.Value
			}
		}
	}

	end, err := mokafka.GetStreamCurrentSize(c.proc.Ctx, configs, mokafka.NewKafkaAdapter)
	if err != nil {
		return nil, err
	}
	ps := calculatePartitions(0, end, int64(c.ncpu))

	ss := make([]*Scope, len(ps))

	currentFirstFlag := c.anal.isFirst
	for i := range ss {
		ss[i] = newScope(Merge)
		ss[i].NodeInfo = getEngineNode(c)
		ss[i].Proc = c.proc.NewNoContextChildProc(0)
		arg := constructStream(node, ps[i])
		arg.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
		ss[i].setRootOperator(arg)
	}
	c.anal.isFirst = false
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

func StrictSqlMode(proc *process.Process) (error, bool) {
	mode, err := proc.GetResolveVariableFunc()("sql_mode", true, false)
	if err != nil {
		return err, false
	}
	if modeStr, ok := mode.(string); ok {
		if strings.Contains(modeStr, "STRICT_TRANS_TABLES") || strings.Contains(modeStr, "STRICT_ALL_TABLES") {
			return nil, true
		}
	}
	return nil, false
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
	if param.Local || crt.GetCompressType(param.CompressType, fileList[0]) != tree.NOCOMPRESS {
		return false, true
	}
	return true, true
}

func (c *Compile) getExternalFileListAndSize(node *plan.Node, param *tree.ExternParam) (fileList []string, fileSize []int64, err error) {
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
		fileList, fileSize, err = external.FilterFileList(c.proc.Ctx, node, c.proc, fileList, fileSize)
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
		fileList, fileSize, err = external.FilterFileList(c.proc.Ctx, node, c.proc, fileList, fileSize)
		if err != nil {
			return nil, nil, err
		}
	case int32(plan.ExternType_LOAD):
		fileList = []string{param.Filepath}
		fileSize = []int64{param.FileSize}
	}
	return fileList, fileSize, nil
}

func (c *Compile) compileExternScan(node *plan.Node) ([]*Scope, error) {
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

	param, err := c.getExternParam(c.proc, node.ExternScan, node.TableDef.Createsql)
	if err != nil {
		return nil, err
	}

	err, strictSqlMode := StrictSqlMode(c.proc)
	if err != nil {
		return nil, err
	}
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

	readParallel, writeParallel := c.getReadWriteParallelFlag(param, fileList)

	if readParallel && writeParallel {
		return c.compileExternScanParallelReadWrite(node, param, fileList, fileSize, strictSqlMode)
	} else if writeParallel {
		return c.compileExternScanParallelWrite(node, param, fileList, fileSize, strictSqlMode)
	} else {
		return c.compileExternScanSerialReadWrite(node, param, fileList, fileSize, strictSqlMode)
	}
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
	param.Parallel = false
	fileOffsetTmp := make([]*pipeline.FileOffset, len(fileList))
	for i := 0; i < len(fileList); i++ {
		fileOffsetTmp[i] = &pipeline.FileOffset{}
		fileOffsetTmp[i].Offset = make([]int64, 0)
		fileOffsetTmp[i].Offset = append(fileOffsetTmp[i].Offset, []int64{0, -1}...)
	}
	scope := c.constructScopeForExternal("", false)
	currentFirstFlag := c.anal.isFirst
	extern := constructExternal(node, param, c.proc.Ctx, fileList, fileSize, fileOffsetTmp, strictSqlMode)
	extern.Es.ParallelLoad = true
	extern.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
	scope.setRootOperator(extern)
	c.anal.isFirst = false

	mcpu := c.getParallelSizeForExternalScan(node, c.ncpu) // dop of insert scopes
	if mcpu == 1 {
		return []*Scope{scope}, nil
	}

	ss := make([]*Scope, mcpu)
	for i := 0; i < mcpu; i++ {
		ss[i] = c.constructLoadMergeScope()
	}
	_, dispatchOp := constructDispatchLocalAndRemote(0, ss, scope)
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

func (c *Compile) compileExternScanParallelReadWrite(node *plan.Node, param *tree.ExternParam, fileList []string, fileSize []int64, strictSqlMode bool) ([]*Scope, error) {
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

	if param.ScanType == tree.S3 {
		for i := 0; i < len(c.cnList); i++ {
			tmp := mcpu
			if c.cnList[i].Mcpu > external.S3ParallelMaxnum {
				mcpu += external.S3ParallelMaxnum
			} else {
				mcpu += c.cnList[i].Mcpu
			}
			ID2Addr[i] = mcpu - tmp
		}
	} else {
		for i := 0; i < len(c.cnList); i++ {
			tmp := mcpu
			mcpu += c.cnList[i].Mcpu
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
	for i := 0; i < len(c.cnList); i++ {
		scope := c.constructScopeForExternal(c.cnList[i].Addr, param.Parallel)
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
		logutil.Infof("compileExternScanParallelReadWrite, len of cnList is %d, cn addr is %s, mcpu is %d, filepath is %s, file size is %d", len(c.cnList), c.cnList[i].Addr, scope.NodeInfo.Mcpu, param.ExParamConst.Filepath, param.ExParamConst.FileSize)
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
	return txnOp, ctx, nil
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
			if txnOp.IsSnapOp() {
				return err
			}
			return err
		}
		tblDef = rel.GetTableDef(ctx)
		s.DataSource.Rel = rel
	} else {
		s.DataSource.Rel.Reset(txnOp)
		tblDef = s.DataSource.Rel.GetTableDef(ctx)
	}

	if len(node.FilterList) != len(s.DataSource.FilterList) {
		s.DataSource.FilterList = plan2.DeepCopyExprList(node.FilterList)
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

func (c *Compile) compileRestrict(node *plan.Node, ss []*Scope) []*Scope {
	if len(node.FilterList) == 0 && len(node.RuntimeFilterProbeList) == 0 {
		return ss
	}
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
		case *source.Source:
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

func (c *Compile) setProjection(node *plan.Node, s *Scope) {
	op := constructProjection(node)
	op.SetAnalyzeControl(c.anal.curNodeIdx, c.anal.isFirst)
	s.setRootOperator(op)
}

func (c *Compile) compileUnion(node *plan.Node, left []*Scope, right []*Scope) []*Scope {
	left = c.mergeShuffleScopesIfNeeded(left, false)
	right = c.mergeShuffleScopesIfNeeded(right, false)
	left = append(left, right...)
	rs := c.newMergeScope(left)
	gn := new(plan.Node)
	gn.GroupBy = make([]*plan.Expr, len(node.ProjectList))
	for i := range gn.GroupBy {
		gn.GroupBy[i] = plan2.DeepCopyExpr(node.ProjectList[i])
		gn.GroupBy[i].Typ.NotNullable = false
	}
	currentFirstFlag := c.anal.isFirst
	op := constructGroup(c.proc.Ctx, gn, node, true, 0, c.proc)
	op.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
	rs.setRootOperator(op)
	c.anal.isFirst = false
	return []*Scope{rs}
}

func (c *Compile) compileTpMinusAndIntersect(left []*Scope, right []*Scope, nodeType plan.Node_NodeType) []*Scope {
	rs := c.newScopeListOnCurrentCN(2, 1)
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
		arg.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
		rs[0].setRootOperator(arg)
		arg.AppendChild(merge1)
	case plan.Node_INTERSECT:
		arg := intersect.NewArgument()
		arg.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
		rs[0].setRootOperator(arg)
		arg.AppendChild(merge1)
	case plan.Node_INTERSECT_ALL:
		arg := intersectall.NewArgument()
		arg.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
		rs[0].setRootOperator(arg)
		arg.AppendChild(merge1)
	}
	c.anal.isFirst = false
	return rs
}

func (c *Compile) compileMinusAndIntersect(node *plan.Node, left []*Scope, right []*Scope, nodeType plan.Node_NodeType) []*Scope {
	if c.IsSingleScope(left) && c.IsSingleScope(right) {
		return c.compileTpMinusAndIntersect(left, right, nodeType)
	}
	rs := c.newScopeListOnCurrentCN(2, int(node.Stats.Dop))
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
			arg.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
			rs[i].setRootOperator(arg)
			arg.AppendChild(merge1)
		}
	}
	c.anal.isFirst = false
	return rs
}

func (c *Compile) compileUnionAll(node *plan.Node, ss []*Scope, children []*Scope) []*Scope {
	rs := c.newMergeScope(append(ss, children...))

	currentFirstFlag := c.anal.isFirst
	op := constructUnionAll(node)
	op.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
	rs.setRootOperator(op)
	c.anal.isFirst = false

	return []*Scope{rs}
}

func (c *Compile) compileJoin(node, left, right *plan.Node, probeScopes, buildScopes []*Scope) []*Scope {
	if node.Stats.HashmapStats.Shuffle {
		if len(c.cnList) == 1 {
			if node.JoinType == plan.Node_DEDUP && node.Stats.HashmapStats.ShuffleType == plan.ShuffleType_Hash {
				logutil.Infof("not support shuffle v2 for dedup join now")
			} else if probeScopes[0].NodeInfo.Mcpu != int(left.Stats.Dop) || buildScopes[0].NodeInfo.Mcpu != int(right.Stats.Dop) {
				logutil.Infof("not support shuffle v2 after merge")
			} else {
				return c.compileShuffleJoinV2(node, left, right, probeScopes, buildScopes)
			}
		}
		return c.compileShuffleJoin(node, left, right, probeScopes, buildScopes)
	}

	rs := c.compileProbeSideForBroadcastJoin(node, left, right, probeScopes)
	return c.compileBuildSideForBroadcastJoin(node, rs, buildScopes)
}

func (c *Compile) compileShuffleJoinV2(node, left, right *plan.Node, leftscopes, rightscopes []*Scope) []*Scope {
	if node.Stats.Dop != left.Stats.Dop || node.Stats.Dop != right.Stats.Dop {
		panic("wrong dop for shuffle join!")
	}
	if len(leftscopes) != len(rightscopes) {
		panic("wrong scopes for shuffle join!")
	}

	reuse := node.Stats.HashmapStats.ShuffleMethod == plan.ShuffleMethod_Reuse
	bucketNum := len(c.cnList) * int(node.Stats.Dop)
	for i := range leftscopes {
		leftscopes[i].PreScopes = append(leftscopes[i].PreScopes, rightscopes[i])
		if !reuse {
			shuffleOpForProbe := constructShuffleOperatorForJoinV2(int32(bucketNum), node, true)
			shuffleOpForProbe.SetAnalyzeControl(c.anal.curNodeIdx, false)
			leftscopes[i].setRootOperator(shuffleOpForProbe)
		}

		shuffleOpForBuild := constructShuffleOperatorForJoinV2(int32(bucketNum), node, false)
		shuffleOpForBuild.SetAnalyzeControl(c.anal.curNodeIdx, false)
		rightscopes[i].setRootOperator(shuffleOpForBuild)
	}

	constructShuffleJoinOP(c, leftscopes, node, left, right, true)

	for i := range leftscopes {
		buildOp := constructShuffleBuild(leftscopes[i].RootOp, c.proc)
		buildOp.SetAnalyzeControl(c.anal.curNodeIdx, false)
		rightscopes[i].setRootOperator(buildOp)
	}

	return leftscopes
}

func constructShuffleJoinOP(c *Compile, shuffleJoins []*Scope, node, left, right *plan.Node, shuffleV2 bool) {
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
	case plan.Node_INNER, plan.Node_LEFT, plan.Node_RIGHT, plan.Node_SEMI, plan.Node_ANTI:
		for i := range shuffleJoins {
			op := constructHashJoin(node, left, leftTypes, rightTypes, c.proc)
			op.ShuffleIdx = int32(i)
			if shuffleV2 {
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
				if shuffleV2 {
					op.ShuffleIdx = -1
				}
				op.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
				shuffleJoins[i].setRootOperator(op)
			}
		} else {
			for i := range shuffleJoins {
				op := constructDedupJoin(node, leftTypes, rightTypes, c.proc)
				op.ShuffleIdx = int32(i)
				if shuffleV2 {
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

func (c *Compile) compileShuffleJoin(node, left, right *plan.Node, lefts, rights []*Scope) []*Scope {
	shuffleJoins := c.newShuffleJoinScopeList(lefts, rights, node)
	constructShuffleJoinOP(c, shuffleJoins, node, left, right, false)

	//construct shuffle build
	currentFirstFlag := c.anal.isFirst
	for i := range shuffleJoins {
		buildScope := shuffleJoins[i].PreScopes[0]
		mergeOp := merge.NewArgument()
		mergeOp.SetAnalyzeControl(c.anal.curNodeIdx, false)
		buildScope.setRootOperator(mergeOp)

		buildOp := constructShuffleBuild(shuffleJoins[i].RootOp, c.proc)
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
					op := constructLoopJoin(node, rightTypes, c.proc)
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
				op := constructLoopJoin(node, rightTypes, c.proc)
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
			op := constructLoopJoin(node, rightTypes, c.proc)
			op.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
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
		buildScopes[0].setRootOperator(constructJoinBuildOperator(c, rs[0].RootOp, int32(rs[0].NodeInfo.Mcpu)))
		rs[0].PreScopes = append(rs[0].PreScopes, buildScopes[0])
		return rs
	}

	for i := range rs {
		if isSameCN(rs[i].NodeInfo.Addr, buildScopes[0].NodeInfo.Addr) {
			rs[i].PreScopes = append(rs[i].PreScopes, buildScopes[0])
			break
		}
	}

	buildOpScopes := make([]*Scope, 0, len(c.cnList))

	if len(rs) > len(c.cnList) { // probe side is shuffle scopes
		for i := range c.cnList {
			var tmp []*Scope
			for j := range rs {
				if isSameCN(c.cnList[i].Addr, rs[j].NodeInfo.Addr) {
					tmp = append(tmp, rs[j])
				}
			}
			bs := newScope(Remote)
			bs.NodeInfo = engine.Node{Addr: tmp[0].NodeInfo.Addr, Mcpu: 1}
			bs.Proc = c.proc.NewNoContextChildProc(0)
			w := &process.WaitRegister{Ch2: make(chan process.PipelineSignal, 10)}
			bs.Proc.Reg.MergeReceivers = append(bs.Proc.Reg.MergeReceivers, w)

			mergeOp := merge.NewArgument()
			c.hasMergeOp = true
			mergeOp.SetAnalyzeControl(c.anal.curNodeIdx, false)
			bs.setRootOperator(mergeOp)
			bs.setRootOperator(constructJoinBuildOperator(c, tmp[0].RootOp, int32(len(tmp))))
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
		bs.NodeInfo = engine.Node{Addr: rs[i].NodeInfo.Addr, Mcpu: 1}
		bs.Proc = c.proc.NewNoContextChildProc(0)
		w := &process.WaitRegister{Ch2: make(chan process.PipelineSignal, 10)}
		bs.Proc.Reg.MergeReceivers = append(bs.Proc.Reg.MergeReceivers, w)

		mergeOp := merge.NewArgument()
		c.hasMergeOp = true
		mergeOp.SetAnalyzeControl(c.anal.curNodeIdx, false)
		bs.setRootOperator(mergeOp)
		bs.setRootOperator(constructJoinBuildOperator(c, rs[i].RootOp, int32(rs[i].NodeInfo.Mcpu)))
		rs[i].PreScopes = append(rs[i].PreScopes, bs)
		buildOpScopes = append(buildOpScopes, bs)
	}

	dispatchArg := constructDispatch(0, buildOpScopes, buildScopes[0], node, false)
	dispatchArg.SetAnalyzeControl(c.anal.curNodeIdx, false)
	buildScopes[0].setRootOperator(dispatchArg)
	return rs
}

func (c *Compile) compileApply(node, right *plan.Node, rs []*Scope) []*Scope {

	switch node.ApplyType {
	case plan.Node_CROSSAPPLY:
		for i := range rs {
			op := constructApply(node, right, apply.CROSS, c.proc)
			if op.TableFunction.IsSingle {
				rs[i].NodeInfo.Mcpu = 1
			}
			op.SetIdx(c.anal.curNodeIdx)
			rs[i].setRootOperator(op)
		}
	case plan.Node_OUTERAPPLY:
		for i := range rs {
			op := constructApply(node, right, apply.OUTER, c.proc)
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
	currentFirstFlag := c.anal.isFirst
	for i := range ss {
		//c.anal.isFirst = currentFirstFlag
		op := constructOrder(node)
		op.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
		ss[i].setRootOperator(op)
	}
	c.anal.isFirst = false

	rs := c.newMergeScope(ss)

	currentFirstFlag = c.anal.isFirst
	arg := constructPartition(node)
	arg.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
	rs.setRootOperator(arg)
	c.anal.isFirst = false

	return []*Scope{rs}
}

func (c *Compile) compileSort(node *plan.Node, ss []*Scope) []*Scope {
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
			if !overflow && topN <= 8192*2 {
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
	arg := constructMergeTop(node, topN)
	arg.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
	rs.setRootOperator(arg)
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
	rs := c.newMergeScope(ss)

	currentFirstFlag := c.anal.isFirst
	arg := constructWindow(c.proc.Ctx, node, c.proc)
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
		arg := constructMergeGroup(node, op.Aggs)
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

func (c *Compile) compileMergeGroup(node *plan.Node, ss []*Scope, ns []*plan.Node, hasDistinct bool) []*Scope {
	// for less memory usage while merge group,
	// we do not run the group-operator in parallel once this has a distinct aggregation.
	// because the parallel need to store all the source data in the memory for merging.
	// we construct a pipeline like the following description for this case:
	//
	// all the operators from ss[0] to ss[last] send the data to only one group-operator.
	// this group-operator sends its result to the merge-group-operator.
	// todo: I cannot remove the merge-group action directly, because the merge-group action is used to fill the partial result.
	if hasDistinct {
		ss = c.mergeShuffleScopesIfNeeded(ss, false)
		mergeToGroup := c.newMergeScope(ss)

		currentFirstFlag := c.anal.isFirst
		op := constructGroup(c.proc.Ctx, node, ns[node.Children[0]], false, 0, c.proc)
		op.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
		mergeToGroup.setRootOperator(op)
		c.anal.isFirst = false

		rs := c.newMergeScope([]*Scope{mergeToGroup})

		currentFirstFlag = c.anal.isFirst
		arg := constructMergeGroup(node, op.Aggs)
		arg.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
		rs.setRootOperator(arg)
		c.anal.isFirst = false

		return []*Scope{rs}
	} else {
		var aggs []aggexec.AggFuncExecExpression

		currentFirstFlag := c.anal.isFirst
		for i := range ss {
			op := constructGroup(c.proc.Ctx, node, ns[node.Children[0]], false, 0, c.proc)
			op.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
			ss[i].setRootOperator(op)

			if i == 0 {
				aggs = op.Aggs
			}
		}
		c.anal.isFirst = false

		ss = c.mergeShuffleScopesIfNeeded(ss, false)
		rs := c.newMergeScope(ss)

		currentFirstFlag = c.anal.isFirst
		arg := constructMergeGroup(node, aggs)
		arg.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
		rs.setRootOperator(arg)
		c.anal.isFirst = false

		return []*Scope{rs}
	}
}

func (c *Compile) compileShuffleGroupV2(node *plan.Node, inputSS []*Scope, nodes []*plan.Node) []*Scope {
	if node.Stats.Dop != nodes[node.Children[0]].Stats.Dop {
		panic("wrong shuffle dop for shuffle group!")
	}
	if node.Stats.HashmapStats.ShuffleMethod == plan.ShuffleMethod_Reuse {
		currentFirstFlag := c.anal.isFirst
		for i := range inputSS {
			op := constructGroup(c.proc.Ctx, node, nodes[node.Children[0]], true, inputSS[0].NodeInfo.Mcpu, c.proc)
			op.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
			inputSS[i].setRootOperator(op)
		}
		c.anal.isFirst = false
		return inputSS
	}

	shuffleArg := constructShuffleArgForGroupV2(node, node.Stats.Dop)
	shuffleArg.SetAnalyzeControl(c.anal.curNodeIdx, false)
	inputSS[0].setRootOperator(shuffleArg)

	groupOp := constructGroup(c.proc.Ctx, node, nodes[node.Children[0]], true, inputSS[0].NodeInfo.Mcpu, c.proc)
	groupOp.SetAnalyzeControl(c.anal.curNodeIdx, false)
	inputSS[0].setRootOperator(groupOp)

	return inputSS
}

func (c *Compile) compileShuffleGroup(node *plan.Node, inputSS []*Scope, nodes []*plan.Node) []*Scope {
	if len(c.cnList) == 1 {
		return c.compileShuffleGroupV2(node, inputSS, nodes)
	}

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

	inputSS = c.mergeShuffleScopesIfNeeded(inputSS, true)
	if len(c.cnList) > 1 {
		// merge here to avoid bugs, delete this in the future
		for i := range inputSS {
			if inputSS[i].NodeInfo.Mcpu > 1 {
				inputSS[i] = c.newMergeScopeByCN([]*Scope{inputSS[i]}, inputSS[i].NodeInfo)
			}
		}
	}

	shuffleGroups := make([]*Scope, 0, len(c.cnList))
	dop := int(node.Stats.Dop)
	for _, cn := range c.cnList {
		scopes := c.newScopeListWithNode(dop, len(inputSS), cn.Addr)
		for _, s := range scopes {
			for _, rr := range s.Proc.Reg.MergeReceivers {
				rr.Ch2 = make(chan process.PipelineSignal, shuffleChannelBufferSize)
			}
		}
		shuffleGroups = append(shuffleGroups, scopes...)
	}

	j := 0
	for i := range inputSS {
		shuffleArg := constructShuffleArgForGroup(shuffleGroups, node)
		shuffleArg.SetAnalyzeControl(c.anal.curNodeIdx, false)
		inputSS[i].setRootOperator(shuffleArg)
		if len(c.cnList) > 1 && inputSS[i].NodeInfo.Mcpu > 1 { // merge here to avoid bugs, delete this in the future
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
	c.appendPrescopes(shuffleGroups, inputSS)
	return shuffleGroups

}

func (c *Compile) appendPrescopes(parents, children []*Scope) {
	for _, cn := range c.cnList {
		index := 0
		for i := range parents {
			if isSameCN(cn.Addr, parents[i].NodeInfo.Addr) {
				index = i
				break
			}
		}
		for i := range children {
			if isSameCN(cn.Addr, children[i].NodeInfo.Addr) {
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
	// Determine whether to Write S3
	toWriteS3 := node.Stats.GetOutcnt()*float64(SingleLineSizeEstimate) >
		float64(DistributedThreshold) || c.anal.qry.LoadWriteS3

	if !toWriteS3 {
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
		dataScope := c.newMergeScope(ss)
		if c.anal.qry.LoadTag {
			// reset the channel buffer of sink for load
			dataScope.Proc.Reg.MergeReceivers[0].Ch2 = make(chan process.PipelineSignal, dataScope.NodeInfo.Mcpu)
		}
		parallelSize := c.getParallelSizeForExternalScan(node, c.ncpu)
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
					rr.Ch2 = make(chan process.PipelineSignal, shuffleChannelBufferSize)
				}
			}
		}
		if c.anal.qry.LoadTag && node.Stats.HashmapStats != nil && node.Stats.HashmapStats.Shuffle && dataScope.NodeInfo.Mcpu == parallelSize && parallelSize > 1 {
			_, arg := constructDispatchLocalAndRemote(0, scopes, dataScope)
			arg.FuncId = dispatch.ShuffleToAllFunc
			arg.ShuffleType = plan2.ShuffleToLocalMatchedReg
			arg.SetAnalyzeControl(c.anal.curNodeIdx, false)
			dataScope.setRootOperator(arg)
		} else {
			_, dispatchArg := constructDispatchLocalAndRemote(0, scopes, dataScope)
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
	rs := c.newMergeScope(ss)
	rs.Magic = MergeInsert
	mergeInsertArg := constructMergeblock(c.e, node)
	mergeInsertArg.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
	rs.setRootOperator(mergeInsertArg)
	ss = []*Scope{rs}
	return ss, nil
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
					ss[i].NodeInfo = engine.Node{Addr: oldScope.NodeInfo.Addr, Mcpu: 1}
				}
				_, dispatchOp := constructDispatchLocalAndRemote(0, ss, oldScope)
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
	for _, tbl := range node.LockTargets {
		if tbl.LockTable {
			c.lockTables[tbl.TableId] = tbl
		} else {
			if _, ok := c.lockTables[tbl.TableId]; !ok {
				lockRows = append(lockRows, tbl)
			}
		}
	}
	node.LockTargets = lockRows
	if len(node.LockTargets) == 0 {
		return ss, nil
	}

	block := false
	// only pessimistic txn needs to block downstream operators.
	if c.proc.GetTxnOperator().Txn().IsPessimistic() {
		block = node.LockTargets[0].Block
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
	lockOpArg, err = constructLockOp(node, c.e)
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
	mergecteArg := mergecte.NewArgument().WithNodeCnt(len(node.SourceStep) - 1)
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

	var rs *Scope
	if c.IsSingleScope(ss) {
		rs = ss[0]
	} else {
		rs = c.newMergeScope(ss)
	}

	currentFirstFlag := c.anal.isFirst
	dispatchLocal := constructDispatchLocal(true, true, node.RecursiveSink, node.RecursiveCte, receivers)
	dispatchLocal.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
	rs.setRootOperator(dispatchLocal)
	c.anal.isFirst = false

	ss = []*Scope{rs}
	return ss, nil
}
func (c *Compile) compileOnduplicateKey(node *plan.Node, ss []*Scope) ([]*Scope, error) {
	rs := c.newMergeScope(ss)

	currentFirstFlag := c.anal.isFirst
	arg := constructOnduplicateKey(node, c.e)
	arg.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
	rs.setRootOperator(arg)
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
		rs[i].NodeInfo = engine.Node{Addr: ss[i].NodeInfo.Addr, Mcpu: 1}
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
		arg.SegmentMap = colexec.Get().GetCnSegmentMap()
		arg.IBucket = uint32(i)
		arg.Nbucket = uint32(len(rs))
		rs[i].setRootOperator(dupOperator(arg, 0, len(rs)))
	}
	return c.newMergeScope(rs)
}

func (c *Compile) newEmptyMergeScope() *Scope {
	rs := newScope(Merge)
	rs.NodeInfo = engine.Node{Addr: c.addr, Mcpu: 1} //merge scope is single parallel by default
	return rs
}

func (c *Compile) newMergeScope(ss []*Scope) *Scope {
	rs := c.newEmptyMergeScope()
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
		if isSameCN(rs.NodeInfo.Addr, ss[i].NodeInfo.Addr) {
			rs.Proc.Reg.MergeReceivers[j].NilBatchCnt = ss[i].NodeInfo.Mcpu
		} else {
			rs.Proc.Reg.MergeReceivers[j].NilBatchCnt = 1
		}
		rs.Proc.Reg.MergeReceivers[j].Ch2 = make(chan process.PipelineSignal, ss[i].NodeInfo.Mcpu)
		// waring: `connector` operator is not used as an input/output analyze,
		// and `connector` operator cannot play the role of IsFirst/IsLast
		connArg := connector.NewArgument().WithReg(rs.Proc.Reg.MergeReceivers[j])
		connArg.SetAnalyzeControl(c.anal.curNodeIdx, false)
		ss[i].setRootOperator(connArg)
		j++
	}
	return rs
}

// newScopeListOnCurrentCN traverse the cnList and only generate Scope list for the current CN node
// waing: newScopeListOnCurrentCN result is only used to build Scope and add one merge operator.
// If other operators are added, please let @qingxinhome know
func (c *Compile) newScopeListOnCurrentCN(childrenCount int, mcpu int) []*Scope {
	node := getEngineNode(c)
	ss := c.newScopeListWithNode(mcpu, childrenCount, node.Addr)
	return ss
}

// all scopes in ss are on the same CN
func (c *Compile) newMergeScopeByCN(ss []*Scope, nodeinfo engine.Node) *Scope {
	rs := newScope(Remote)
	rs.NodeInfo.Addr = nodeinfo.Addr
	rs.NodeInfo.Mcpu = 1 // merge scope is single parallel by default
	rs.PreScopes = ss
	rs.Proc = c.proc.NewNoContextChildProc(1)
	rs.Proc.Reg.MergeReceivers[0].Ch2 = make(chan process.PipelineSignal, len(ss))

	// waring: `Merge` operator` is not used as an input/output analyze,
	// and `Merge` operator cannot play the role of IsFirst/IsLast
	mergeOp := merge.NewArgument()
	c.hasMergeOp = true
	mergeOp.SetAnalyzeControl(c.anal.curNodeIdx, false)
	rs.setRootOperator(mergeOp)
	for i := range ss {
		rs.Proc.Reg.MergeReceivers[0].NilBatchCnt += ss[i].NodeInfo.Mcpu

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
func (c *Compile) newScopeListWithNode(mcpu, childrenCount int, addr string) []*Scope {
	ss := make([]*Scope, mcpu)
	for i := range ss {
		ss[i] = newScope(Remote)
		ss[i].Magic = Remote
		ss[i].NodeInfo.Addr = addr
		ss[i].NodeInfo.Mcpu = 1 // ss is already the mcpu length so we don't need to parallel it
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
	if len(c.cnList) == 1 && !force {
		return ss
	}
	if len(ss) <= len(c.cnList) {
		return ss
	}
	for i := range ss {
		if ss[i].NodeInfo.Mcpu != 1 {
			return ss
		}
	}
	rs := c.mergeScopesByCN(ss)
	for i := range rs {
		for _, rr := range rs[i].Proc.Reg.MergeReceivers {
			rr.Ch2 = make(chan process.PipelineSignal, shuffleChannelBufferSize)
		}
	}
	return rs
}

func (c *Compile) mergeScopesByCN(ss []*Scope) []*Scope {
	rs := make([]*Scope, 0, len(c.cnList))
	for i := range c.cnList {
		cn := c.cnList[i]
		currentSS := make([]*Scope, 0, cn.Mcpu)
		for j := range ss {
			if isSameCN(ss[j].NodeInfo.Addr, cn.Addr) {
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

func (c *Compile) newShuffleJoinScopeList(probeScopes, buildScopes []*Scope, node *plan.Node) []*Scope {
	cnlist := c.cnList
	if len(cnlist) <= 1 {
		node.Stats.HashmapStats.ShuffleTypeForMultiCN = plan.ShuffleTypeForMultiCN_Simple
	}

	reuse := node.Stats.HashmapStats.ShuffleMethod == plan.ShuffleMethod_Reuse
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

	dop := int(node.Stats.Dop)
	bucketNum := len(cnlist) * dop
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
				probes[i].NodeInfo.Addr = cn.Addr
				probes[i].NodeInfo.Mcpu = 1
				probes[i].Proc = c.proc.NewNoContextChildProc(lenLeft)

				builds[i] = newScope(Remote)
				builds[i].NodeInfo = probes[i].NodeInfo
				builds[i].Proc = c.proc.NewNoContextChildProc(lenRight)

				probes[i].PreScopes = []*Scope{builds[i]}
				for _, rr := range probes[i].Proc.Reg.MergeReceivers {
					rr.Ch2 = make(chan process.PipelineSignal, shuffleChannelBufferSize)
				}
				for _, rr := range builds[i].Proc.Reg.MergeReceivers {
					rr.Ch2 = make(chan process.PipelineSignal, shuffleChannelBufferSize)
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
				rr.Ch2 = make(chan process.PipelineSignal, shuffleChannelBufferSize)
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
			shuffleProbeOp := constructShuffleOperatorForJoin(int32(bucketNum), node, true)
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

			for _, js := range shuffleProbes {
				if isSameCN(js.NodeInfo.Addr, probeScopes[i].NodeInfo.Addr) {
					js.PreScopes = append(js.PreScopes, probeScopes[i])
					break
				}
			}
		}
	}

	c.anal.isFirst = currentFirstFlag
	for i := range buildScopes {
		shuffleBuildOp := constructShuffleOperatorForJoin(int32(bucketNum), node, false)
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

		for _, js := range shuffleBuilds {
			if isSameCN(js.NodeInfo.Addr, buildScopes[i].NodeInfo.Addr) {
				js.PreScopes = append(js.PreScopes, buildScopes[i])
				break
			}
		}
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

	tombstone, err = rel.CollectTombstones(ctx, c.TxnOffset, policy)
	if err != nil {
		return nil, err
	}

	return tombstone, nil
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

func shouldScanOnCurrentCN(c *Compile, node *plan.Node, forceSingle bool) bool {
	if len(c.cnList) == 1 ||
		node.Stats.ForceOneCN ||
		forceSingle {
		return true
	}

	if !plan2.GetForceScanOnMultiCN() &&
		node.Stats.BlockNum <= int32(plan2.BlockThresholdForOneCN) {
		return true
	}

	return false
}

func (c *Compile) generateNodes(node *plan.Node) (engine.Nodes, error) {
	rel, _, _, err := c.handleDbRelContext(node, false)
	if err != nil {
		return nil, err
	}

	forceSingle := false
	if len(node.AggList) > 0 {
		partialResults, _, _ := checkAggOptimize(node)
		if partialResults != nil {
			forceSingle = true
		} else if node.Stats != nil && node.Stats.ForceOneCN {
			// ForceOneCN is already set by CalcNodeDOP for distinct aggregation
			// Use it directly instead of checking again
			forceSingle = true
		} else {
			// Fallback: Check if any aggregation function uses distinct flag
			// This is defensive programming in case CalcNodeDOP didn't set ForceOneCN
			for _, agg := range node.AggList {
				if f, ok := agg.Expr.(*plan.Expr_F); ok {
					if (uint64(f.F.Func.Obj) & function.Distinct) != 0 {
						forceSingle = true
						break
					}
				}
			}
		}
	}
	//if len(n.OrderBy) > 0 {
	//	forceSingle = true
	//}

	if node.NodeType == plan.Node_TABLE_CLONE {
		forceSingle = true
	}

	var engNodes engine.Nodes
	// scan on current CN
	if shouldScanOnCurrentCN(c, node, forceSingle) {
		mcpu := node.Stats.Dop
		if forceSingle {
			mcpu = 1
		}
		engNodes = append(engNodes, engine.Node{
			Addr:  c.addr,
			Mcpu:  int(mcpu),
			CNCNT: 1,
		})
		return engNodes, nil
	}

	// scan on multi CN
	for i := range c.cnList {
		engNode := engine.Node{
			Id:    c.cnList[i].Id,
			Addr:  c.cnList[i].Addr,
			Mcpu:  c.cnList[i].Mcpu,
			CNCNT: int32(len(c.cnList)),
			CNIDX: int32(i),
		}
		if engNode.Addr != c.addr {
			uncommittedTombs, err := collectTombstones(c, node, rel, engine.Policy_CollectAllTombstones)
			if err != nil {
				return nil, err
			}
			engNode.Data = readutil.BuildEmptyRelData()
			engNode.Data.AttachTombstones(uncommittedTombs)
		}
		engNodes = append(engNodes, engNode)
	}
	return engNodes, nil
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
				if _, ok := args.Expr.(*plan.Expr_Lit); ok {
					agg.F.Func.ObjName = "starcount"
					return partialResults, partialResultTypes, columnMap
				}
				return nil, nil, nil
			} else {
				columnMap[int(col.Col.ColPos)] = int(node.TableDef.Cols[int(col.Col.ColPos)].Seqnum)
			}
		case "min", "max":
			partialResults[i] = nil
			col, ok := args.Expr.(*plan.Expr_Col)
			if !ok {
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
	return types.New(types.T(typ.Id), typ.Width, typ.Scale)
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

	if qry, ok := c.pn.Plan.(*plan.Plan_Ddl); ok {
		if qry.Ddl.DdlType == plan.DataDefinition_DROP_DATABASE {
			options = options.WithIgnoreForeignKey()
		}
	}

	exec := v.(executor.SQLExecutor)
	opts := executor.Options{}.
		// All runSql and runSqlWithResult is a part of input sql, can not incr statement.
		// All these sub-sql's need to be rolled back and retried en masse when they conflict in pessimistic mode
		WithDisableIncrStatement().
		WithTxn(c.proc.GetTxnOperator()).
		WithDatabase(c.db).
		WithTimeZone(c.proc.GetSessionInfo().TimeZone).
		WithLowerCaseTableNames(&lower).
		WithStatementOption(options).
		WithResolveVariableFunc(c.proc.GetResolveVariableFunc())

	ctx := c.proc.Ctx
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
	c.originSQL = sql
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
