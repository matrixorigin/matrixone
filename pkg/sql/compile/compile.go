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
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/panjf2000/ants/v2"
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/cnservice/cnclient"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
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
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/apply"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/connector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/deletion"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/dispatch"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/external"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/filter"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/intersect"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/intersectall"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/lockop"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/loopjoin"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/merge"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergeblock"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergecte"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergedelete"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergerecursive"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/minus"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/output"
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
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
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
	ncpu = runtime.GOMAXPROCS(0)

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
	c := GetCompileService().getCompile(proc)

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
	}
	GetCompileService().putCompile(c)
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

func (c *Compile) Reset(proc *process.Process, startAt time.Time, fill func(*batch.Batch) error, sql string) {
	// clean up the process for a new query.
	proc.ResetQueryContext()
	c.proc = proc

	c.fill = fill
	c.sql = sql
	c.affectRows.Store(0)

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
	} else {
		c.TxnOffset = 0
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

	for _, exe := range c.filterExprExes {
		exe.Free()
	}
	c.filterExprExes = nil

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
	for _, s := range c.scopes {
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

	if topContext := c.proc.GetTopContext(); topContext.Value(defines.TemporaryTN{}) == nil {
		c.proc.SaveToTopContext(defines.TemporaryTN{}, tempStorage)
	}
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
			c.addAffectedRows(mergeArg.AffectedRows())
		}
		return nil
	case Remote:
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
	case RenameTable:
		return s.RenameTable(c)
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
	errC := make(chan error, len(c.scopes))
	for _, s := range c.scopes {
		err = s.InitAllDataSource(c)
		if err != nil {
			return err
		}
	}

	if err = GetCompileService().recordRunningCompile(c); err != nil {
		return err
	}
	defer func() {
		_, _ = GetCompileService().removeRunningCompile(c)
	}()

	//c.printPipeline()

	for i := range c.scopes {
		wg.Add(1)
		scope := c.scopes[i]
		errSubmit := ants.Submit(func() {
			defer func() {
				if e := recover(); e != nil {
					err := moerr.ConvertPanicError(c.proc.Ctx, e)
					c.proc.Error(c.proc.Ctx, "panic in run",
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

	errList := make([]error, 0, len(c.scopes))
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
		}
	}
	return nil, moerr.NewNYI(c.proc.Ctx, fmt.Sprintf("query '%s'", pn))
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

	c.execType = plan2.GetExecType(c.pn.GetQuery(), c.getHaveDDL())

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
		var scope *Scope
		scopes, err = c.compilePlanScope(int32(i), qry.Steps[i], qry.Nodes)
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
					Ch2: make(chan process.PipelineSignal, ncpu),
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

func (c *Compile) compileSteps(qry *plan.Query, ss []*Scope, step int32) (*Scope, error) {
	if qry.Nodes[step].NodeType == plan.Node_SINK {
		return ss[0], nil
	}

	switch qry.StmtType {
	case plan.Query_DELETE:
		updateScopesLastFlag(ss)
		return ss[0], nil
	case plan.Query_INSERT:
		updateScopesLastFlag(ss)
		return ss[0], nil
	case plan.Query_UPDATE:
		updateScopesLastFlag(ss)
		return ss[0], nil
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
				WithFunc(c.fill),
		)
		return rs, nil
	}
}

func (c *Compile) compilePlanScope(step int32, curNodeIdx int32, ns []*plan.Node) ([]*Scope, error) {
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

	if n.Limit != nil {
		if cExpr, ok := n.Limit.Expr.(*plan.Expr_Lit); ok {
			if cval, ok := cExpr.Lit.Value.(*plan.Literal_U64Val); ok {
				if cval.U64Val == 0 {
					// optimize for limit 0
					rs := c.newEmptyMergeScope()
					rs.Proc = c.proc.NewNoContextChildProc(0)
					return c.compileLimit(n, []*Scope{rs}), nil
				}
			}
		}
	}

	switch n.NodeType {
	case plan.Node_VALUE_SCAN:
		c.setAnalyzeCurrent(nil, int(curNodeIdx))
		ss, err = c.compileValueScan(n)
		if err != nil {
			return nil, err
		}
		ss = c.compileSort(n, c.compileProjection(n, ss))
		return ss, nil
	case plan.Node_EXTERNAL_SCAN:
		if n.ObjRef != nil {
			c.appendMetaTables(n.ObjRef)
		}
		node := plan2.DeepCopyNode(n)

		c.setAnalyzeCurrent(nil, int(curNodeIdx))
		ss, err = c.compileExternScan(node)
		if err != nil {
			return nil, err
		}
		ss = c.compileSort(n, c.compileProjection(n, c.compileRestrict(node, ss)))
		return ss, nil
	case plan.Node_TABLE_SCAN:
		c.appendMetaTables(n.ObjRef)

		c.setAnalyzeCurrent(nil, int(curNodeIdx))
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
		c.setAnalyzeCurrent(nil, int(curNodeIdx))
		ss, err = c.compileSourceScan(n)
		if err != nil {
			return nil, err
		}
		ss = c.compileSort(n, c.compileProjection(n, c.compileRestrict(n, ss)))
		return ss, nil
	case plan.Node_FILTER, plan.Node_PROJECT, plan.Node_PRE_DELETE:
		ss, err = c.compilePlanScope(step, n.Children[0], ns)
		if err != nil {
			return nil, err
		}

		c.setAnalyzeCurrent(ss, int(curNodeIdx))
		ss = c.compileSort(n, c.compileProjection(n, c.compileRestrict(n, ss)))
		return ss, nil
	case plan.Node_AGG:
		ss, err = c.compilePlanScope(step, n.Children[0], ns)
		if err != nil {
			return nil, err
		}

		groupInfo := constructGroup(c.proc.Ctx, n, ns[n.Children[0]], false, 0, c.proc)
		defer groupInfo.Release()
		anyDistinctAgg := groupInfo.AnyDistinctAgg()

		c.setAnalyzeCurrent(ss, int(curNodeIdx))
		if c.IsSingleScope(ss) && ss[0].PartialResults == nil {
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
		ss, err = c.compilePlanScope(step, n.Children[0], ns)
		if err != nil {
			return nil, err
		}

		c.setAnalyzeCurrent(ss, int(curNodeIdx))
		ss = c.compileSort(n, c.compileProjection(n, c.compileRestrict(n, c.compileSample(n, ss))))
		return ss, nil
	case plan.Node_WINDOW:
		ss, err = c.compilePlanScope(step, n.Children[0], ns)
		if err != nil {
			return nil, err
		}

		c.setAnalyzeCurrent(ss, int(curNodeIdx))
		ss = c.compileSort(n, c.compileProjection(n, c.compileRestrict(n, c.compileWin(n, ss))))
		return ss, nil
	case plan.Node_TIME_WINDOW:
		ss, err = c.compilePlanScope(step, n.Children[0], ns)
		if err != nil {
			return nil, err
		}

		c.setAnalyzeCurrent(ss, int(curNodeIdx))
		ss = c.compileProjection(n, c.compileRestrict(n, c.compileTimeWin(n, c.compileSort(n, ss))))
		return ss, nil
	case plan.Node_FILL:
		ss, err = c.compilePlanScope(step, n.Children[0], ns)
		if err != nil {
			return nil, err
		}

		c.setAnalyzeCurrent(ss, int(curNodeIdx))
		ss = c.compileProjection(n, c.compileRestrict(n, c.compileFill(n, ss)))
		return ss, nil
	case plan.Node_JOIN:
		left, err = c.compilePlanScope(step, n.Children[0], ns)
		if err != nil {
			return nil, err
		}
		right, err = c.compilePlanScope(step, n.Children[1], ns)
		if err != nil {
			return nil, err
		}

		c.setAnalyzeCurrent(left, int(curNodeIdx))
		c.setAnalyzeCurrent(right, int(curNodeIdx))
		ss = c.compileSort(n, c.compileJoin(n, ns[n.Children[0]], ns[n.Children[1]], left, right))
		return ss, nil
	case plan.Node_SORT:
		ss, err = c.compilePlanScope(step, n.Children[0], ns)
		if err != nil {
			return nil, err
		}

		c.setAnalyzeCurrent(ss, int(curNodeIdx))
		ss = c.compileProjection(n, c.compileRestrict(n, c.compileSort(n, ss)))
		return ss, nil
	case plan.Node_PARTITION:
		ss, err = c.compilePlanScope(step, n.Children[0], ns)
		if err != nil {
			return nil, err
		}

		c.setAnalyzeCurrent(ss, int(curNodeIdx))
		ss = c.compileProjection(n, c.compileRestrict(n, c.compilePartition(n, ss)))
		return ss, nil
	case plan.Node_UNION:
		left, err = c.compilePlanScope(step, n.Children[0], ns)
		if err != nil {
			return nil, err
		}
		right, err = c.compilePlanScope(step, n.Children[1], ns)
		if err != nil {
			return nil, err
		}

		c.setAnalyzeCurrent(left, int(curNodeIdx))
		c.setAnalyzeCurrent(right, int(curNodeIdx))
		ss = c.compileSort(n, c.compileUnion(n, left, right))
		return ss, nil
	case plan.Node_MINUS, plan.Node_INTERSECT, plan.Node_INTERSECT_ALL:
		left, err = c.compilePlanScope(step, n.Children[0], ns)
		if err != nil {
			return nil, err
		}
		right, err = c.compilePlanScope(step, n.Children[1], ns)
		if err != nil {
			return nil, err
		}

		c.setAnalyzeCurrent(left, int(curNodeIdx))
		c.setAnalyzeCurrent(right, int(curNodeIdx))
		ss = c.compileSort(n, c.compileMinusAndIntersect(n, left, right, n.NodeType))
		return ss, nil
	case plan.Node_UNION_ALL:
		left, err = c.compilePlanScope(step, n.Children[0], ns)
		if err != nil {
			return nil, err
		}
		right, err = c.compilePlanScope(step, n.Children[1], ns)
		if err != nil {
			return nil, err
		}

		c.setAnalyzeCurrent(left, int(curNodeIdx))
		c.setAnalyzeCurrent(right, int(curNodeIdx))
		ss = c.compileSort(n, c.compileUnionAll(n, left, right))
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
		ss, err = c.compilePlanScope(step, n.Children[0], ns)
		if err != nil {
			return nil, err
		}

		n.NotCacheable = true
		c.setAnalyzeCurrent(ss, int(curNodeIdx))
		return c.compileDelete(n, ss)
	case plan.Node_ON_DUPLICATE_KEY:
		ss, err = c.compilePlanScope(step, n.Children[0], ns)
		if err != nil {
			return nil, err
		}

		c.setAnalyzeCurrent(ss, int(curNodeIdx))
		ss, err = c.compileOnduplicateKey(n, ss)
		if err != nil {
			return nil, err
		}
		return ss, nil
	case plan.Node_FUZZY_FILTER:
		left, err = c.compilePlanScope(step, n.Children[0], ns)
		if err != nil {
			return nil, err
		}
		right, err = c.compilePlanScope(step, n.Children[1], ns)
		if err != nil {
			return nil, err
		}

		c.setAnalyzeCurrent(left, int(curNodeIdx))
		c.setAnalyzeCurrent(right, int(curNodeIdx))
		return c.compileFuzzyFilter(n, ns, left, right)
	case plan.Node_PRE_INSERT_UK:
		ss, err = c.compilePlanScope(step, n.Children[0], ns)
		if err != nil {
			return nil, err
		}

		c.setAnalyzeCurrent(ss, int(curNodeIdx))
		ss = c.compilePreInsertUk(n, ss)
		return ss, nil
	case plan.Node_PRE_INSERT_SK:
		ss, err = c.compilePlanScope(step, n.Children[0], ns)
		if err != nil {
			return nil, err
		}
		c.setAnalyzeCurrent(ss, int(curNodeIdx))
		ss = c.compilePreInsertSK(n, ss)
		return ss, nil
	case plan.Node_PRE_INSERT:
		ss, err = c.compilePlanScope(step, n.Children[0], ns)
		if err != nil {
			return nil, err
		}

		c.setAnalyzeCurrent(ss, int(curNodeIdx))
		return c.compilePreInsert(ns, n, ss)
	case plan.Node_INSERT:
		c.appendMetaTables(n.ObjRef)
		ss, err = c.compilePlanScope(step, n.Children[0], ns)
		if err != nil {
			return nil, err
		}

		n.NotCacheable = true
		c.setAnalyzeCurrent(ss, int(curNodeIdx))
		return c.compileInsert(ns, n, ss)
	case plan.Node_LOCK_OP:
		ss, err = c.compilePlanScope(step, n.Children[0], ns)
		if err != nil {
			return nil, err
		}

		c.setAnalyzeCurrent(ss, int(curNodeIdx))
		ss, err = c.compileLock(n, ss)
		if err != nil {
			return nil, err
		}
		ss = c.compileProjection(n, ss)
		return ss, nil
	case plan.Node_FUNCTION_SCAN:
		ss, err = c.compilePlanScope(step, n.Children[0], ns)
		if err != nil {
			return nil, err
		}
		c.setAnalyzeCurrent(ss, int(curNodeIdx))
		ss = c.compileSort(n, c.compileProjection(n, c.compileRestrict(n, c.compileTableFunction(n, ss))))
		return ss, nil
	case plan.Node_SINK_SCAN:
		c.setAnalyzeCurrent(nil, int(curNodeIdx))
		ss, err = c.compileSinkScanNode(n, curNodeIdx)
		if err != nil {
			return nil, err
		}
		ss = c.compileProjection(n, ss)
		return ss, nil
	case plan.Node_RECURSIVE_SCAN:
		c.setAnalyzeCurrent(ss, int(curNodeIdx))
		return c.compileRecursiveScan(n, curNodeIdx)
	case plan.Node_RECURSIVE_CTE:
		c.setAnalyzeCurrent(ss, int(curNodeIdx))
		ss, err = c.compileRecursiveCte(n, curNodeIdx)
		if err != nil {
			return nil, err
		}
		ss = c.compileSort(n, ss)
		return ss, nil
	case plan.Node_SINK:
		ss, err = c.compilePlanScope(step, n.Children[0], ns)
		if err != nil {
			return nil, err
		}

		c.setAnalyzeCurrent(ss, int(curNodeIdx))
		return c.compileSinkNode(n, ss, step)
	case plan.Node_APPLY:
		left, err = c.compilePlanScope(step, n.Children[0], ns)
		if err != nil {
			return nil, err
		}

		c.setAnalyzeCurrent(left, int(curNodeIdx))
		ss = c.compileSort(n, c.compileApply(n, ns[n.Children[1]], left))
		return ss, nil
	default:
		return nil, moerr.NewNYI(c.proc.Ctx, fmt.Sprintf("query '%s'", n))
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
	arg.SetAnalyzeControl(c.anal.curNodeIdx, false)

	ds.setRootOperator(arg)
	return ds
}

func (c *Compile) compileSourceScan(n *plan.Node) ([]*Scope, error) {
	_, span := trace.Start(c.proc.Ctx, "compileSourceScan")
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

	end, err := mokafka.GetStreamCurrentSize(c.proc.Ctx, configs, mokafka.NewKafkaAdapter)
	if err != nil {
		return nil, err
	}
	ps := calculatePartitions(0, end, int64(ncpu))

	ss := make([]*Scope, len(ps))

	currentFirstFlag := c.anal.isFirst
	for i := range ss {
		ss[i] = newScope(Merge)
		ss[i].NodeInfo = getEngineNode(c)
		ss[i].Proc = c.proc.NewNoContextChildProc(0)
		arg := constructStream(n, ps[i])
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
	if param.Local || external.GetCompressType(param, fileList[0]) != tree.NOCOMPRESS {
		return false, true
	}
	return true, true
}

func (c *Compile) getExternalFileListAndSize(n *plan.Node, param *tree.ExternParam) (fileList []string, fileSize []int64, err error) {
	switch n.ExternScan.Type {
	case int32(plan.ExternType_EXTERNAL_TB):
		t := time.Now()
		_, spanReadDir := trace.Start(c.proc.Ctx, "compileExternScan.ReadDir")
		fileList, fileSize, err = plan2.ReadDir(param)
		if err != nil {
			spanReadDir.End()
			return nil, nil, err
		}
		spanReadDir.End()
		fileList, fileSize, err = external.FilterFileList(c.proc.Ctx, n, c.proc, fileList, fileSize)
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
		fileList, fileSize, err = external.FilterFileList(c.proc.Ctx, n, c.proc, fileList, fileSize)
		if err != nil {
			return nil, nil, err
		}
	case int32(plan.ExternType_LOAD):
		fileList = []string{param.Filepath}
		fileSize = []int64{param.FileSize}
	}
	return fileList, fileSize, nil
}

func (c *Compile) compileExternScan(n *plan.Node) ([]*Scope, error) {
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

	param, err := c.getExternParam(c.proc, n.ExternScan, n.TableDef.Createsql)
	if err != nil {
		return nil, err
	}

	err, strictSqlMode := StrictSqlMode(c.proc)
	if err != nil {
		return nil, err
	}
	if param.ScanType == tree.INLINE {
		return c.compileExternValueScan(n, param, strictSqlMode)
	}

	fileList, fileSize, err := c.getExternalFileListAndSize(n, param)
	if err != nil {
		return nil, err
	}

	if len(fileList) == 0 {
		ret := newScope(Merge)
		ret.NodeInfo = getEngineNode(c)
		ret.NodeInfo.Mcpu = 1
		ret.DataSource = &Source{isConst: true, node: n}

		currentFirstFlag := c.anal.isFirst
		op := constructValueScan()
		op.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
		ret.setRootOperator(op)
		c.anal.isFirst = false

		ret.Proc = c.proc.NewNoContextChildProc(0)
		return []*Scope{ret}, nil
	}

	readParallel, writeParallel := c.getReadWriteParallelFlag(param, fileList)

	if readParallel && writeParallel {
		return c.compileExternScanParallelReadWrite(n, param, fileList, fileSize, strictSqlMode)
	} else if writeParallel {
		return c.compileExternScanParallelWrite(n, param, fileList, fileSize, strictSqlMode)
	} else {
		return c.compileExternScanSerialReadWrite(n, param, fileList, fileSize, strictSqlMode)
	}
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

// load data inline goes here, should always be single parallel
func (c *Compile) compileExternValueScan(n *plan.Node, param *tree.ExternParam, strictSqlMode bool) ([]*Scope, error) {
	s := c.constructScopeForExternal(c.addr, false)
	currentFirstFlag := c.anal.isFirst
	op := constructExternal(n, param, c.proc.Ctx, nil, nil, nil, strictSqlMode)
	op.SetIdx(c.anal.curNodeIdx)
	op.SetIsFirst(currentFirstFlag)
	s.setRootOperator(op)
	c.anal.isFirst = false
	return []*Scope{s}, nil
}

// construct one thread to read the file data, then dispatch to mcpu thread to get the filedata for insert
func (c *Compile) compileExternScanParallelWrite(n *plan.Node, param *tree.ExternParam, fileList []string, fileSize []int64, strictSqlMode bool) ([]*Scope, error) {
	param.Parallel = false
	fileOffsetTmp := make([]*pipeline.FileOffset, len(fileList))
	for i := 0; i < len(fileList); i++ {
		fileOffsetTmp[i] = &pipeline.FileOffset{}
		fileOffsetTmp[i].Offset = make([]int64, 0)
		fileOffsetTmp[i].Offset = append(fileOffsetTmp[i].Offset, []int64{0, -1}...)
	}
	scope := c.constructScopeForExternal("", false)
	currentFirstFlag := c.anal.isFirst
	extern := constructExternal(n, param, c.proc.Ctx, fileList, fileSize, fileOffsetTmp, strictSqlMode)
	extern.Es.ParallelLoad = true
	extern.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
	scope.setRootOperator(extern)
	c.anal.isFirst = false

	mcpu := c.getParallelSizeForExternalScan(n, ncpu) // dop of insert scopes
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

func (c *Compile) compileExternScanParallelReadWrite(n *plan.Node, param *tree.ExternParam, fileList []string, fileSize []int64, strictSqlMode bool) ([]*Scope, error) {
	visibleCols := make([]*plan.ColDef, 0)
	if param.Strict {
		for _, col := range n.TableDef.Cols {
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

	var fileOffset [][]int64
	for i := 0; i < len(fileList); i++ {
		param.Filepath = fileList[i]
		arr, err := external.ReadFileOffset(param, mcpu, fileSize[i], visibleCols)
		fileOffset = append(fileOffset, arr)
		if err != nil {
			return nil, err
		}
	}

	ss := make([]*Scope, len(c.cnList))
	pre := 0
	currentFirstFlag := c.anal.isFirst
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
			}
		}
		op := constructExternal(n, param, c.proc.Ctx, fileList, fileSize, fileOffsetTmp, strictSqlMode)
		op.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
		ss[i].setRootOperator(op)
		pre += count
	}
	c.anal.isFirst = false
	return ss, nil
}

func (c *Compile) compileExternScanSerialReadWrite(n *plan.Node, param *tree.ExternParam, fileList []string, fileSize []int64, strictSqlMode bool) ([]*Scope, error) {
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
	op := constructExternal(n, param, c.proc.Ctx, fileList, fileSize, fileOffsetTmp, strictSqlMode)
	op.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
	ss[0].setRootOperator(op)
	c.anal.isFirst = false
	return ss, nil
}

func (c *Compile) compileTableFunction(n *plan.Node, ss []*Scope) []*Scope {
	currentFirstFlag := c.anal.isFirst
	for i := range ss {
		op := constructTableFunction(n)
		op.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
		ss[i].setRootOperator(op)
	}
	c.anal.isFirst = false

	return ss
}

func (c *Compile) compileValueScan(n *plan.Node) ([]*Scope, error) {
	ds := newScope(Merge)
	ds.NodeInfo = getEngineNode(c)
	ds.DataSource = &Source{isConst: true, node: n}
	ds.NodeInfo = engine.Node{Addr: c.addr, Mcpu: 1}
	ds.Proc = c.proc.NewNoContextChildProc(0)

	currentFirstFlag := c.anal.isFirst
	op := constructValueScan()
	op.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
	op.NodeType = n.NodeType
	if n.RowsetData != nil {
		op.RowsetData = n.RowsetData
		op.ColCount = len(n.TableDef.Cols)
		op.Uuid = n.Uuid
	}

	ds.setRootOperator(op)
	c.anal.isFirst = false

	return []*Scope{ds}, nil
}

func (c *Compile) compileTableScan(n *plan.Node) ([]*Scope, error) {
	nodes, partialResults, partialResultTypes, err := c.generateNodes(n)
	if err != nil {
		return nil, err
	}
	ss := make([]*Scope, 0, len(nodes))

	currentFirstFlag := c.anal.isFirst
	for i := range nodes {
		s, err := c.compileTableScanWithNode(n, nodes[i], currentFirstFlag)
		if err != nil {
			return nil, err
		}
		ss = append(ss, s)
	}
	c.anal.isFirst = false

	if len(n.OrderBy) > 0 {
		ss[0].NodeInfo.Mcpu = 1
	}

	ss[0].PartialResults = partialResults
	ss[0].PartialResultTypes = partialResultTypes
	return ss, nil
}

func (c *Compile) compileTableScanWithNode(n *plan.Node, node engine.Node, firstFlag bool) (*Scope, error) {
	s := newScope(Remote)
	s.NodeInfo = node
	s.TxnOffset = c.TxnOffset
	s.DataSource = &Source{
		node: n,
	}

	op := constructTableScan(n)
	op.SetAnalyzeControl(c.anal.curNodeIdx, firstFlag)
	s.setRootOperator(op)
	s.Proc = c.proc.NewNoContextChildProc(0)
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
		attrs[j] = col.GetOriginCaseName()
	}

	//-----------------------------------------------------------------------------------------------------
	ctx := c.proc.GetTopContext()
	txnOp = c.proc.GetTxnOperator()
	err = disttae.CheckTxnIsValid(txnOp)
	if err != nil {
		return err
	}
	if n.ScanSnapshot != nil && n.ScanSnapshot.TS != nil {
		if !n.ScanSnapshot.TS.Equal(timestamp.Timestamp{LogicalTime: 0, PhysicalTime: 0}) &&
			n.ScanSnapshot.TS.Less(c.proc.GetTxnOperator().Txn().SnapshotTS) {
			if c.proc.GetCloneTxnOperator() != nil {
				txnOp = c.proc.GetCloneTxnOperator()
			} else {
				txnOp = c.proc.GetTxnOperator().CloneSnapshotOp(*n.ScanSnapshot.TS)
				c.proc.SetCloneTxnOperator(txnOp)
			}

			if n.ScanSnapshot.Tenant != nil {
				ctx = context.WithValue(ctx, defines.TenantIDKey{}, n.ScanSnapshot.Tenant.TenantID)
			}
		}
	}
	//-----------------------------------------------------------------------------------------------------

	if c.proc != nil && c.proc.GetTxnOperator() != nil {
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
		if util.TableIsLoggingTable(n.ObjRef.SchemaName, n.ObjRef.ObjName) {
			ctx = defines.AttachAccountId(ctx, catalog.System_Account)
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
			db, e = c.e.Database(c.proc.Ctx, defines.TEMPORARY_DBNAME, txnOp)
			if e != nil {
				panic(e)
			}
			rel, e = db.Relation(c.proc.Ctx, engine.GetTempTableName(n.ObjRef.SchemaName, n.TableDef.Name), c.proc)
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

	if len(n.FilterList) != len(s.DataSource.FilterList) {
		s.DataSource.FilterList = plan2.DeepCopyExprList(n.FilterList)
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

	if len(n.BlockFilterList) != len(s.DataSource.BlockFilterList) {
		s.DataSource.BlockFilterList = plan2.DeepCopyExprList(n.BlockFilterList)
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
	s.DataSource.Rel = rel
	s.DataSource.RelationName = n.TableDef.Name
	s.DataSource.PartitionRelationNames = partitionRelNames
	s.DataSource.SchemaName = n.ObjRef.SchemaName
	s.DataSource.AccountId = n.ObjRef.GetPubInfo()
	s.DataSource.RuntimeFilterSpecs = n.RuntimeFilterProbeList
	s.DataSource.OrderBy = n.OrderBy
	return nil
}

func (c *Compile) compileRestrict(n *plan.Node, ss []*Scope) []*Scope {
	if len(n.FilterList) == 0 && len(n.RuntimeFilterProbeList) == 0 {
		return ss
	}
	currentFirstFlag := c.anal.isFirst
	filterExpr := colexec.RewriteFilterExprList(plan2.DeepCopyExprList(n.FilterList))
	var op *filter.Filter
	for i := range ss {
		op = constructRestrict(n, filterExpr)
		op.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
		ss[i].setRootOperator(op)
	}
	c.anal.isFirst = false
	return ss
}

func (c *Compile) compileProjection(n *plan.Node, ss []*Scope) []*Scope {
	if len(n.ProjectList) == 0 {
		return ss
	}

	for i := range ss {
		c.setProjection(n, ss[i])
	}

	/*for i := range ss {
		if ss[i].RootOp == nil {
			c.setProjection(n, ss[i])
			continue
		}
		_, ok := c.stmt.(*tree.Select)
		if !ok {
			c.setProjection(n, ss[i])
			continue
		}
		switch ss[i].RootOp.(type) {
		case *table_scan.TableScan:
			if ss[i].RootOp.(*table_scan.TableScan).ProjectList == nil {
				ss[i].RootOp.(*table_scan.TableScan).ProjectList = n.ProjectList
			} else {
				c.setProjection(n, ss[i])
			}
		case *value_scan.ValueScan:
			if ss[i].RootOp.(*value_scan.ValueScan).ProjectList == nil {
				ss[i].RootOp.(*value_scan.ValueScan).ProjectList = n.ProjectList
			} else {
				c.setProjection(n, ss[i])
			}
		case *fill.Fill:
			if ss[i].RootOp.(*fill.Fill).ProjectList == nil {
				ss[i].RootOp.(*fill.Fill).ProjectList = n.ProjectList
			} else {
				c.setProjection(n, ss[i])
			}
		case *source.Source:
			if ss[i].RootOp.(*source.Source).ProjectList == nil {
				ss[i].RootOp.(*source.Source).ProjectList = n.ProjectList
			} else {
				c.setProjection(n, ss[i])
			}
		case *external.External:
			if ss[i].RootOp.(*external.External).ProjectList == nil {
				ss[i].RootOp.(*external.External).ProjectList = n.ProjectList
			} else {
				c.setProjection(n, ss[i])
			}
		case *group.Group:
			if ss[i].RootOp.(*group.Group).ProjectList == nil {
				ss[i].RootOp.(*group.Group).ProjectList = n.ProjectList
			} else {
				c.setProjection(n, ss[i])
			}
		case *mergegroup.MergeGroup:
			if ss[i].RootOp.(*mergegroup.MergeGroup).ProjectList == nil {
				ss[i].RootOp.(*mergegroup.MergeGroup).ProjectList = n.ProjectList
			} else {
				c.setProjection(n, ss[i])
			}
		case *anti.AntiJoin:
			if ss[i].RootOp.(*anti.AntiJoin).ProjectList == nil {
				ss[i].RootOp.(*anti.AntiJoin).ProjectList = n.ProjectList
			} else {
				c.setProjection(n, ss[i])
			}
		case *indexjoin.IndexJoin:
			if ss[i].RootOp.(*indexjoin.IndexJoin).ProjectList == nil {
				ss[i].RootOp.(*indexjoin.IndexJoin).ProjectList = n.ProjectList
			} else {
				c.setProjection(n, ss[i])
			}
		case *join.InnerJoin:
			if ss[i].RootOp.(*join.InnerJoin).ProjectList == nil {
				ss[i].RootOp.(*join.InnerJoin).ProjectList = n.ProjectList
			} else {
				c.setProjection(n, ss[i])
			}
		case *left.LeftJoin:
			if ss[i].RootOp.(*left.LeftJoin).ProjectList == nil {
				ss[i].RootOp.(*left.LeftJoin).ProjectList = n.ProjectList
			} else {
				c.setProjection(n, ss[i])
			}
		case *loopanti.LoopAnti:
			if ss[i].RootOp.(*loopanti.LoopAnti).ProjectList == nil {
				ss[i].RootOp.(*loopanti.LoopAnti).ProjectList = n.ProjectList
			} else {
				c.setProjection(n, ss[i])
			}
		case *loopjoin.LoopJoin:
			if ss[i].RootOp.(*loopjoin.LoopJoin).ProjectList == nil {
				ss[i].RootOp.(*loopjoin.LoopJoin).ProjectList = n.ProjectList
			} else {
				c.setProjection(n, ss[i])
			}
		case *loopleft.LoopLeft:
			if ss[i].RootOp.(*loopleft.LoopLeft).ProjectList == nil {
				ss[i].RootOp.(*loopleft.LoopLeft).ProjectList = n.ProjectList
			} else {
				c.setProjection(n, ss[i])
			}
		case *loopmark.LoopMark:
			if ss[i].RootOp.(*loopmark.LoopMark).ProjectList == nil {
				ss[i].RootOp.(*loopmark.LoopMark).ProjectList = n.ProjectList
			} else {
				c.setProjection(n, ss[i])
			}
		case *loopsemi.LoopSemi:
			if ss[i].RootOp.(*loopsemi.LoopSemi).ProjectList == nil {
				ss[i].RootOp.(*loopsemi.LoopSemi).ProjectList = n.ProjectList
			} else {
				c.setProjection(n, ss[i])
			}
		case *loopsingle.LoopSingle:
			if ss[i].RootOp.(*loopsingle.LoopSingle).ProjectList == nil {
				ss[i].RootOp.(*loopsingle.LoopSingle).ProjectList = n.ProjectList
			} else {
				c.setProjection(n, ss[i])
			}
		case *mark.MarkJoin:
			if ss[i].RootOp.(*mark.MarkJoin).ProjectList == nil {
				ss[i].RootOp.(*mark.MarkJoin).ProjectList = n.ProjectList
			} else {
				c.setProjection(n, ss[i])
			}
		case *product.Product:
			if ss[i].RootOp.(*product.Product).ProjectList == nil {
				ss[i].RootOp.(*product.Product).ProjectList = n.ProjectList
			} else {
				c.setProjection(n, ss[i])
			}
		case *productl2.Productl2:
			if ss[i].RootOp.(*productl2.Productl2).ProjectList == nil {
				ss[i].RootOp.(*productl2.Productl2).ProjectList = n.ProjectList
			} else {
				c.setProjection(n, ss[i])
			}
		case *semi.SemiJoin:
			if ss[i].RootOp.(*semi.SemiJoin).ProjectList == nil {
				ss[i].RootOp.(*semi.SemiJoin).ProjectList = n.ProjectList
			} else {
				c.setProjection(n, ss[i])
			}
		case *single.SingleJoin:
			if ss[i].RootOp.(*single.SingleJoin).ProjectList == nil {
				ss[i].RootOp.(*single.SingleJoin).ProjectList = n.ProjectList
			} else {
				c.setProjection(n, ss[i])
			}

		default:
			c.setProjection(n, ss[i])
		}
	}*/
	c.anal.isFirst = false
	return ss
}

func (c *Compile) setProjection(n *plan.Node, s *Scope) {
	op := constructProjection(n)
	op.SetAnalyzeControl(c.anal.curNodeIdx, c.anal.isFirst)
	s.setRootOperator(op)
}

func (c *Compile) compileUnion(n *plan.Node, left []*Scope, right []*Scope) []*Scope {
	left = c.mergeShuffleScopesIfNeeded(left, false)
	right = c.mergeShuffleScopesIfNeeded(right, false)
	left = append(left, right...)
	rs := c.newMergeScope(left)
	gn := new(plan.Node)
	gn.GroupBy = make([]*plan.Expr, len(n.ProjectList))
	for i := range gn.GroupBy {
		gn.GroupBy[i] = plan2.DeepCopyExpr(n.ProjectList[i])
		gn.GroupBy[i].Typ.NotNullable = false
	}
	currentFirstFlag := c.anal.isFirst
	op := constructGroup(c.proc.Ctx, gn, n, true, 0, c.proc)
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

func (c *Compile) compileMinusAndIntersect(n *plan.Node, left []*Scope, right []*Scope, nodeType plan.Node_NodeType) []*Scope {
	if c.IsSingleScope(left) && c.IsSingleScope(right) {
		return c.compileTpMinusAndIntersect(left, right, nodeType)
	}
	rs := c.newScopeListOnCurrentCN(2, int(n.Stats.BlockNum))
	rs = c.newScopeListForMinusAndIntersect(rs, left, right, n)

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
		return c.compileShuffleJoin(node, left, right, probeScopes, buildScopes)
	}

	rs := c.compileProbeSideForBoradcastJoin(node, left, right, probeScopes)
	return c.compileBuildSideForBoradcastJoin(node, rs, buildScopes)
}

func (c *Compile) compileShuffleJoin(node, left, right *plan.Node, lefts, rights []*Scope) []*Scope {
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

	shuffleJoins := c.newShuffleJoinScopeList(lefts, rights, node)

	for i := range shuffleJoins {
		mergeOp := merge.NewArgument()
		mergeOp.SetAnalyzeControl(c.anal.curNodeIdx, false)
		shuffleJoins[i].setRootOperator(mergeOp)
	}

	currentFirstFlag := c.anal.isFirst
	switch node.JoinType {
	case plan.Node_INNER:
		for i := range shuffleJoins {
			op := constructJoin(node, rightTyps, c.proc)
			op.ShuffleIdx = int32(i)
			op.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
			shuffleJoins[i].setRootOperator(op)
		}

	case plan.Node_ANTI:
		if node.BuildOnLeft {
			for i := range shuffleJoins {
				op := constructRightAnti(node, rightTyps, c.proc)
				op.ShuffleIdx = int32(i)
				op.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
				shuffleJoins[i].setRootOperator(op)
			}
		} else {
			for i := range shuffleJoins {
				op := constructAnti(node, rightTyps, c.proc)
				op.ShuffleIdx = int32(i)
				op.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
				shuffleJoins[i].setRootOperator(op)
			}
		}

	case plan.Node_SEMI:
		if node.BuildOnLeft {
			for i := range shuffleJoins {
				op := constructRightSemi(node, rightTyps, c.proc)
				op.ShuffleIdx = int32(i)
				op.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
				shuffleJoins[i].setRootOperator(op)
			}
		} else {
			for i := range shuffleJoins {
				op := constructSemi(node, rightTyps, c.proc)
				op.ShuffleIdx = int32(i)
				op.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
				shuffleJoins[i].setRootOperator(op)
			}
		}

	case plan.Node_LEFT:
		for i := range shuffleJoins {
			op := constructLeft(node, rightTyps, c.proc)
			op.ShuffleIdx = int32(i)
			op.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
			shuffleJoins[i].setRootOperator(op)
		}
	case plan.Node_RIGHT:
		for i := range shuffleJoins {
			op := constructRight(node, leftTyps, rightTyps, c.proc)
			op.ShuffleIdx = int32(i)
			op.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
			shuffleJoins[i].setRootOperator(op)
		}
	default:
		panic(moerr.NewNYI(c.proc.Ctx, fmt.Sprintf("shuffle join do not support join type '%v'", node.JoinType)))
	}
	c.anal.isFirst = false

	//construct shuffle build
	currentFirstFlag = c.anal.isFirst
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

func (c *Compile) compileProbeSideForBoradcastJoin(node, left, right *plan.Node, probeScopes []*Scope) []*Scope {
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
		rs = c.newProbeScopeListForBroadcastJoin(probeScopes, false)
		currentFirstFlag := c.anal.isFirst
		if len(node.OnList) == 0 {
			for i := range rs {
				op := constructProduct(node, rightTyps, c.proc)
				op.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
				rs[i].setRootOperator(op)
			}
		} else {
			for i := range rs {
				if isEq {
					op := constructJoin(node, rightTyps, c.proc)
					op.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
					rs[i].setRootOperator(op)
				} else {
					op := constructLoopJoin(node, rightTyps, c.proc, loopjoin.LoopInner)
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
			if rs[i].NodeInfo.Mcpu > ncpu {
				rs[i].NodeInfo.Mcpu = ncpu
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
	case plan.Node_SEMI:
		if isEq {
			if node.BuildOnLeft {
				rs = c.newProbeScopeListForBroadcastJoin(probeScopes, true)
				currentFirstFlag := c.anal.isFirst
				for i := range rs {
					op := constructRightSemi(node, rightTyps, c.proc)
					op.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
					rs[i].setRootOperator(op)
				}
				c.anal.isFirst = false
			} else {
				rs = c.newProbeScopeListForBroadcastJoin(probeScopes, false)
				currentFirstFlag := c.anal.isFirst
				for i := range rs {
					op := constructSemi(node, rightTyps, c.proc)
					op.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
					rs[i].setRootOperator(op)
				}
				c.anal.isFirst = false
			}
		} else {
			rs = c.newProbeScopeListForBroadcastJoin(probeScopes, false)
			currentFirstFlag := c.anal.isFirst
			for i := range rs {
				op := constructLoopJoin(node, rightTyps, c.proc, loopjoin.LoopSemi)
				op.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
				rs[i].setRootOperator(op)
			}
			c.anal.isFirst = false
		}
	case plan.Node_LEFT:
		rs = c.newProbeScopeListForBroadcastJoin(probeScopes, false)
		currentFirstFlag := c.anal.isFirst
		for i := range rs {
			if isEq {
				op := constructLeft(node, rightTyps, c.proc)
				op.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
				rs[i].setRootOperator(op)
			} else {
				op := constructLoopJoin(node, rightTyps, c.proc, loopjoin.LoopLeft)
				op.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
				rs[i].setRootOperator(op)
			}
		}
		c.anal.isFirst = false
	case plan.Node_RIGHT:
		if isEq {
			rs = c.newProbeScopeListForBroadcastJoin(probeScopes, true)
			currentFirstFlag := c.anal.isFirst
			for i := range rs {
				op := constructRight(node, leftTyps, rightTyps, c.proc)
				op.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
				rs[i].setRootOperator(op)
			}
			c.anal.isFirst = false
		} else {
			panic("dont pass any no-equal right join plan to this function,it should be changed to left join by the planner")
		}
	case plan.Node_SINGLE:
		rs = c.newProbeScopeListForBroadcastJoin(probeScopes, false)
		currentFirstFlag := c.anal.isFirst
		for i := range rs {
			if isEq {
				op := constructSingle(node, rightTyps, c.proc)
				op.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
				rs[i].setRootOperator(op)
			} else {
				op := constructLoopJoin(node, rightTyps, c.proc, loopjoin.LoopSingle)
				op.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
				rs[i].setRootOperator(op)
			}
		}
		c.anal.isFirst = false
	case plan.Node_ANTI:
		if isEq {
			if node.BuildOnLeft {
				rs = c.newProbeScopeListForBroadcastJoin(probeScopes, true)
				currentFirstFlag := c.anal.isFirst
				for i := range rs {
					op := constructRightAnti(node, rightTyps, c.proc)
					op.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
					rs[i].setRootOperator(op)
				}
				c.anal.isFirst = false
			} else {
				rs = c.newProbeScopeListForBroadcastJoin(probeScopes, false)
				currentFirstFlag := c.anal.isFirst
				for i := range rs {
					op := constructAnti(node, rightTyps, c.proc)
					op.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
					rs[i].setRootOperator(op)
				}
				c.anal.isFirst = false
			}
		} else {
			rs = c.newProbeScopeListForBroadcastJoin(probeScopes, false)
			currentFirstFlag := c.anal.isFirst
			for i := range rs {
				op := constructLoopJoin(node, rightTyps, c.proc, loopjoin.LoopAnti)
				op.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
				rs[i].setRootOperator(op)
			}
			c.anal.isFirst = false
		}
	case plan.Node_MARK:
		rs = c.newProbeScopeListForBroadcastJoin(probeScopes, false)
		currentFirstFlag := c.anal.isFirst
		for i := range rs {
			//if isEq {
			//	rs[i].appendInstruction(vm.Instruction{
			//		Op:  vm.Mark,
			//		Idx: c.anal.curNodeIdx,
			//		Arg: constructMark(n, typs, c.proc),
			//	})
			//} else {
			op := constructLoopJoin(node, rightTyps, c.proc, loopjoin.LoopMark)
			op.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
			rs[i].setRootOperator(op)
			//}
		}
		c.anal.isFirst = false
	default:
		panic(moerr.NewNYI(c.proc.Ctx, fmt.Sprintf("join typ '%v'", node.JoinType)))
	}
	return rs
}

func (c *Compile) compileBuildSideForBoradcastJoin(node *plan.Node, rs, buildScopes []*Scope) []*Scope {
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
			op.SetIdx(c.anal.curNodeIdx)
			rs[i].setRootOperator(op)
		}
	default:
		panic("unknown apply")
	}

	return rs
}

func (c *Compile) compilePartition(n *plan.Node, ss []*Scope) []*Scope {
	currentFirstFlag := c.anal.isFirst
	for i := range ss {
		//c.anal.isFirst = currentFirstFlag
		op := constructOrder(n)
		op.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
		ss[i].setRootOperator(op)
	}
	c.anal.isFirst = false

	rs := c.newMergeScope(ss)

	currentFirstFlag = c.anal.isFirst
	arg := constructPartition(n)
	arg.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
	rs.setRootOperator(arg)
	c.anal.isFirst = false

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

			limit, offset := vector.MustFixedColWithTypeCheck[uint64](vec1)[0], vector.MustFixedColWithTypeCheck[uint64](vec2)[0]
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

func (c *Compile) compileTop(n *plan.Node, topN *plan.Expr, ss []*Scope) []*Scope {
	// use topN TO make scope.
	if c.IsSingleScope(ss) {
		currentFirstFlag := c.anal.isFirst
		op := constructTop(n, topN)
		op.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
		ss[0].setRootOperator(op)
		c.anal.isFirst = false
		return ss
	}

	currentFirstFlag := c.anal.isFirst
	for i := range ss {
		//c.anal.isFirst = currentFirstFlag
		op := constructTop(n, topN)
		op.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
		ss[i].setRootOperator(op)
	}
	c.anal.isFirst = false
	ss = c.mergeShuffleScopesIfNeeded(ss, false)
	rs := c.newMergeScope(ss)

	currentFirstFlag = c.anal.isFirst
	arg := constructMergeTop(n, topN)
	arg.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
	rs.setRootOperator(arg)
	c.anal.isFirst = false

	return []*Scope{rs}
}

func (c *Compile) compileOrder(n *plan.Node, ss []*Scope) []*Scope {
	if c.IsSingleScope(ss) {
		currentFirstFlag := c.anal.isFirst
		order := constructOrder(n)
		order.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
		ss[0].setRootOperator(order)
		c.anal.isFirst = false

		currentFirstFlag = c.anal.isFirst
		mergeOrder := constructMergeOrder(n)
		mergeOrder.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
		ss[0].setRootOperator(mergeOrder)
		c.anal.isFirst = false
		return ss
	}

	currentFirstFlag := c.anal.isFirst
	for i := range ss {
		//c.anal.isFirst = currentFirstFlag
		order := constructOrder(n)
		order.SetIdx(c.anal.curNodeIdx)
		order.SetIsFirst(currentFirstFlag)
		ss[i].setRootOperator(order)
	}
	c.anal.isFirst = false

	ss = c.mergeShuffleScopesIfNeeded(ss, false)
	rs := c.newMergeScope(ss)

	currentFirstFlag = c.anal.isFirst
	mergeOrder := constructMergeOrder(n)
	mergeOrder.SetIdx(c.anal.curNodeIdx)
	mergeOrder.SetIsFirst(currentFirstFlag)
	rs.setRootOperator(mergeOrder)
	c.anal.isFirst = false

	return []*Scope{rs}
}

func (c *Compile) compileWin(n *plan.Node, ss []*Scope) []*Scope {
	rs := c.newMergeScope(ss)

	currentFirstFlag := c.anal.isFirst
	arg := constructWindow(c.proc.Ctx, n, c.proc)
	arg.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
	rs.setRootOperator(arg)
	c.anal.isFirst = false

	return []*Scope{rs}
}

func (c *Compile) compileTimeWin(n *plan.Node, ss []*Scope) []*Scope {
	rs := c.newMergeScope(ss)

	currentFirstFlag := c.anal.isFirst
	arg := constructTimeWindow(c.proc.Ctx, n, c.proc)
	arg.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
	rs.setRootOperator(arg)
	c.anal.isFirst = false

	return []*Scope{rs}
}

func (c *Compile) compileFill(n *plan.Node, ss []*Scope) []*Scope {
	rs := c.newMergeScope(ss)

	currentFirstFlag := c.anal.isFirst
	arg := constructFill(n)
	arg.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
	rs.setRootOperator(arg)
	c.anal.isFirst = false

	return []*Scope{rs}
}

func (c *Compile) compileOffset(n *plan.Node, ss []*Scope) []*Scope {
	if c.IsSingleScope(ss) {
		currentFirstFlag := c.anal.isFirst
		op := constructOffset(n)
		op.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
		ss[0].setRootOperator(op)
		c.anal.isFirst = false
		return ss
	}

	rs := c.newMergeScope(ss)

	currentFirstFlag := c.anal.isFirst
	arg := constructOffset(n)
	arg.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
	rs.setRootOperator(arg)
	c.anal.isFirst = false

	return []*Scope{rs}
}

func (c *Compile) compileLimit(n *plan.Node, ss []*Scope) []*Scope {
	if c.IsSingleScope(ss) {
		currentFirstFlag := c.anal.isFirst
		op := constructLimit(n)
		op.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
		ss[0].setRootOperator(op)
		c.anal.isFirst = false
		return ss
	}

	currentFirstFlag := c.anal.isFirst
	for i := range ss {
		//c.anal.isFirst = currentFirstFlag
		op := constructLimit(n)
		op.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
		ss[i].setRootOperator(op)
	}
	c.anal.isFirst = false

	ss = c.mergeShuffleScopesIfNeeded(ss, false)
	rs := c.newMergeScope(ss)

	currentFirstFlag = c.anal.isFirst
	arg := constructLimit(n)
	arg.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
	rs.setRootOperator(arg)
	c.anal.isFirst = false

	return []*Scope{rs}
}

func (c *Compile) compileFuzzyFilter(n *plan.Node, ns []*plan.Node, left []*Scope, right []*Scope) ([]*Scope, error) {
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

	currentFirstFlag := c.anal.isFirst
	op := constructFuzzyFilter(n, ns[n.Children[0]], ns[n.Children[1]])
	op.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
	rs.setRootOperator(op)
	op.AppendChild(merge2)
	c.anal.isFirst = false

	fuzzyCheck, err := newFuzzyCheck(n)
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

func (c *Compile) compileSample(n *plan.Node, ss []*Scope) []*Scope {
	currentFirstFlag := c.anal.isFirst
	isSingle := c.IsSingleScope(ss)
	for i := range ss {
		op := constructSample(n, !isSingle)
		op.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
		ss[i].setRootOperator(op)
	}
	c.anal.isFirst = false
	if isSingle {
		return ss
	}

	rs := c.newMergeScope(ss)
	// should sample again if sample by rows.
	if n.SampleFunc.Rows != plan2.NotSampleByRows {
		currentFirstFlag = c.anal.isFirst
		op := sample.NewMergeSample(constructSample(n, true), false)
		op.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
		rs.setRootOperator(op)
		c.anal.isFirst = false
	}
	return []*Scope{rs}
}

func (c *Compile) compileTPGroup(n *plan.Node, ss []*Scope, ns []*plan.Node) []*Scope {
	currentFirstFlag := c.anal.isFirst
	op := constructGroup(c.proc.Ctx, n, ns[n.Children[0]], true, 0, c.proc)
	op.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
	ss[0].setRootOperator(op)
	c.anal.isFirst = false
	return ss
}

func (c *Compile) compileMergeGroup(n *plan.Node, ss []*Scope, ns []*plan.Node, hasDistinct bool) []*Scope {
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
		op := constructGroup(c.proc.Ctx, n, ns[n.Children[0]], false, 0, c.proc)
		op.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
		mergeToGroup.setRootOperator(op)
		c.anal.isFirst = false

		rs := c.newMergeScope([]*Scope{mergeToGroup})

		currentFirstFlag = c.anal.isFirst
		arg := constructMergeGroup(true)
		if ss[0].PartialResults != nil {
			arg.PartialResults = ss[0].PartialResults
			arg.PartialResultTypes = ss[0].PartialResultTypes
			ss[0].PartialResults = nil
			ss[0].PartialResultTypes = nil
		}
		arg.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
		rs.setRootOperator(arg)
		c.anal.isFirst = false

		return []*Scope{rs}
	} else {
		currentFirstFlag := c.anal.isFirst
		for i := range ss {
			op := constructGroup(c.proc.Ctx, n, ns[n.Children[0]], false, 0, c.proc)
			op.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
			ss[i].setRootOperator(op)
		}
		c.anal.isFirst = false

		ss = c.mergeShuffleScopesIfNeeded(ss, false)
		rs := c.newMergeScope(ss)

		currentFirstFlag = c.anal.isFirst
		arg := constructMergeGroup(true)
		if ss[0].PartialResults != nil {
			arg.PartialResults = ss[0].PartialResults
			arg.PartialResultTypes = ss[0].PartialResultTypes
			ss[0].PartialResults = nil
			ss[0].PartialResultTypes = nil
		}
		arg.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
		rs.setRootOperator(arg)
		c.anal.isFirst = false

		return []*Scope{rs}
	}
}

func (c *Compile) compileShuffleGroup(n *plan.Node, inputSS []*Scope, nodes []*plan.Node) []*Scope {
	if n.Stats.HashmapStats.ShuffleMethod == plan.ShuffleMethod_Reuse {
		currentIsFirst := c.anal.isFirst
		for i := range inputSS {
			op := constructGroup(c.proc.Ctx, n, nodes[n.Children[0]], true, len(inputSS), c.proc)
			op.SetAnalyzeControl(c.anal.curNodeIdx, currentIsFirst)
			inputSS[i].setRootOperator(op)
		}
		c.anal.isFirst = false

		inputSS = c.compileProjection(n, c.compileRestrict(n, inputSS))
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
	for _, cn := range c.cnList {
		scopes := c.newScopeListWithNode(plan2.GetShuffleDop(cn.Mcpu), len(inputSS), cn.Addr)
		for _, s := range scopes {
			for _, rr := range s.Proc.Reg.MergeReceivers {
				rr.Ch2 = make(chan process.PipelineSignal, shuffleChannelBufferSize)
			}
		}
		shuffleGroups = append(shuffleGroups, scopes...)
	}

	j := 0
	for i := range inputSS {
		shuffleArg := constructShuffleArgForGroup(shuffleGroups, n)
		shuffleArg.SetAnalyzeControl(c.anal.curNodeIdx, false)
		inputSS[i].setRootOperator(shuffleArg)
		if len(c.cnList) > 1 && inputSS[i].NodeInfo.Mcpu > 1 { // merge here to avoid bugs, delete this in the future
			inputSS[i] = c.newMergeScopeByCN([]*Scope{inputSS[i]}, inputSS[i].NodeInfo)
		}
		dispatchArg := constructDispatch(j, shuffleGroups, inputSS[i], n, false)
		dispatchArg.SetAnalyzeControl(c.anal.curNodeIdx, false)
		inputSS[i].setRootOperator(dispatchArg)
		j++
		inputSS[i].IsEnd = true
	}

	currentIsFirst := c.anal.isFirst
	for i := range shuffleGroups {
		groupOp := constructGroup(c.proc.Ctx, n, nodes[n.Children[0]], true, len(shuffleGroups), c.proc)
		groupOp.SetAnalyzeControl(c.anal.curNodeIdx, currentIsFirst)
		shuffleGroups[i].setRootOperator(groupOp)
	}
	c.anal.isFirst = false
	shuffleGroups = c.compileProjection(n, c.compileRestrict(n, shuffleGroups))

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
func (c *Compile) compilePreInsert(ns []*plan.Node, n *plan.Node, ss []*Scope) ([]*Scope, error) {
	currentFirstFlag := c.anal.isFirst
	for i := range ss {
		preInsertArg, err := constructPreInsert(ns, n, c.e, c.proc)
		if err != nil {
			return nil, err
		}
		preInsertArg.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
		ss[i].setRootOperator(preInsertArg)
	}
	c.anal.isFirst = false
	return ss, nil
}

func (c *Compile) compileInsert(ns []*plan.Node, n *plan.Node, ss []*Scope) ([]*Scope, error) {
	// Determine whether to Write S3
	toWriteS3 := n.Stats.GetCost()*float64(SingleLineSizeEstimate) >
		float64(DistributedThreshold) || c.anal.qry.LoadWriteS3

	if !toWriteS3 {
		currentFirstFlag := c.anal.isFirst
		// Not write S3
		for i := range ss {
			insertArg := constructInsert(n, c.e)
			insertArg.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
			ss[i].setRootOperator(insertArg)
		}
		c.anal.isFirst = false
		return ss, nil
	}

	// to write S3
	if haveSinkScanInPlan(ns, n.Children[0]) {
		// todo : pipelines with sink scan ,must refactor this in the future
		currentFirstFlag := c.anal.isFirst
		c.anal.isFirst = false
		dataScope := c.newMergeScope(ss)
		if c.anal.qry.LoadTag {
			// reset the channel buffer of sink for load
			dataScope.Proc.Reg.MergeReceivers[0].Ch2 = make(chan process.PipelineSignal, dataScope.NodeInfo.Mcpu)
		}
		parallelSize := c.getParallelSizeForExternalScan(n, ncpu)
		scopes := make([]*Scope, 0, parallelSize)
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
		if c.anal.qry.LoadTag && n.Stats.HashmapStats != nil && n.Stats.HashmapStats.Shuffle && dataScope.NodeInfo.Mcpu == parallelSize && parallelSize > 1 {
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
			insertArg := constructInsert(n, c.e)
			insertArg.ToWriteS3 = true
			insertArg.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
			scopes[i].setRootOperator(insertArg)
		}
		currentFirstFlag = false
		rs := c.newMergeScope(scopes)
		rs.PreScopes = append(rs.PreScopes, dataScope)
		rs.Magic = MergeInsert
		mergeInsertArg := constructMergeblock(c.e, n)
		mergeInsertArg.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
		rs.setRootOperator(mergeInsertArg)
		ss = []*Scope{rs}
		return ss, nil
	}

	c.proc.Debugf(c.proc.Ctx, "insert of '%s' write s3\n", c.sql)
	currentFirstFlag := c.anal.isFirst
	c.anal.isFirst = false
	for i := range ss {
		insertArg := constructInsert(n, c.e)
		insertArg.ToWriteS3 = true
		insertArg.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
		ss[i].setRootOperator(insertArg)
	}
	currentFirstFlag = false
	rs := c.newMergeScope(ss)
	rs.Magic = MergeInsert
	mergeInsertArg := constructMergeblock(c.e, n)
	mergeInsertArg.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
	rs.setRootOperator(mergeInsertArg)
	ss = []*Scope{rs}
	return ss, nil
}

func (c *Compile) compilePreInsertUk(n *plan.Node, ss []*Scope) []*Scope {
	currentFirstFlag := c.anal.isFirst
	for i := range ss {
		preInsertUkArg := constructPreInsertUk(n)
		preInsertUkArg.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
		ss[i].setRootOperator(preInsertUkArg)
	}
	c.anal.isFirst = false
	return ss
}

func (c *Compile) compilePreInsertSK(n *plan.Node, ss []*Scope) []*Scope {
	currentFirstFlag := c.anal.isFirst
	for i := range ss {
		preInsertSkArg := constructPreInsertSk(n)
		preInsertSkArg.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
		ss[i].setRootOperator(preInsertSkArg)
	}
	c.anal.isFirst = false
	return ss
}

func (c *Compile) compileDelete(n *plan.Node, ss []*Scope) ([]*Scope, error) {
	var arg *deletion.Deletion
	currentFirstFlag := c.anal.isFirst
	arg, err := constructDeletion(n, c.e)
	if err != nil {
		return nil, err
	}
	arg.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
	c.anal.isFirst = false

	if n.Stats.Cost*float64(SingleLineSizeEstimate) > float64(DistributedThreshold) && !arg.DeleteCtx.CanTruncate {
		rs := c.newDeleteMergeScope(arg, ss, n)
		rs.Magic = MergeDelete

		mergeDeleteArg := mergedelete.NewArgument().
			WithObjectRef(arg.DeleteCtx.Ref).
			WithParitionNames(arg.DeleteCtx.PartitionTableNames).
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

		rs.setRootOperator(arg)
		ss = []*Scope{rs}
		return ss, nil
	}
}

func (c *Compile) compileLock(n *plan.Node, ss []*Scope) ([]*Scope, error) {
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
	if c.proc.GetTxnOperator().Txn().IsPessimistic() {
		block = n.LockTargets[0].Block
		if block {
			ss = []*Scope{c.newMergeScope(ss)}
		}
	}

	currentFirstFlag := c.anal.isFirst
	for i := range ss {
		var err error
		var lockOpArg *lockop.LockOp
		lockOpArg, err = constructLockOp(n, c.e)
		if err != nil {
			return nil, err
		}
		lockOpArg.SetBlock(block)
		lockOpArg.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
		ss[i].doSetRootOperator(lockOpArg)

	}
	c.anal.isFirst = false
	return ss, nil
}

func (c *Compile) compileRecursiveCte(n *plan.Node, curNodeIdx int32) ([]*Scope, error) {
	receivers := make([]*process.WaitRegister, len(n.SourceStep))
	for i, step := range n.SourceStep {
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
	mergecteArg := mergecte.NewArgument().WithNodeCnt(len(n.SourceStep) - 1)
	mergecteArg.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
	rs.setRootOperator(mergecteArg)
	c.anal.isFirst = false

	mergeOp2 := merge.NewArgument()
	mergeOp2.WithPartial(1, int32(len(receivers)))
	mergecteArg.SetAnalyzeControl(c.anal.curNodeIdx, false)
	mergecteArg.AppendChild(mergeOp2)
	c.anal.isFirst = false

	return []*Scope{rs}, nil
}

func (c *Compile) compileRecursiveScan(n *plan.Node, curNodeIdx int32) ([]*Scope, error) {
	receivers := make([]*process.WaitRegister, len(n.SourceStep))
	for i, step := range n.SourceStep {
		receivers[i] = c.getNodeReg(step, curNodeIdx)
		if receivers[i] == nil {
			return nil, moerr.NewInternalError(c.proc.Ctx, "no data sender for sinkScan node")
		}
	}
	rs := c.newEmptyMergeScope()
	rs.Proc = c.proc.NewNoContextChildProc(len(receivers))
	rs.Proc.Reg.MergeReceivers = receivers

	mergeOp := merge.NewArgument()
	rs.setRootOperator(mergeOp)
	currentFirstFlag := c.anal.isFirst
	mergeRecursiveArg := mergerecursive.NewArgument()
	mergeRecursiveArg.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
	rs.setRootOperator(mergeRecursiveArg)
	c.anal.isFirst = false
	return []*Scope{rs}, nil
}

func (c *Compile) compileSinkScanNode(n *plan.Node, curNodeIdx int32) ([]*Scope, error) {
	receivers := make([]*process.WaitRegister, len(n.SourceStep))
	for i, step := range n.SourceStep {
		receivers[i] = c.getNodeReg(step, curNodeIdx)
		if receivers[i] == nil {
			return nil, moerr.NewInternalError(c.proc.Ctx, "no data sender for sinkScan node")
		}
	}
	rs := c.newEmptyMergeScope()
	rs.Proc = c.proc.NewNoContextChildProc(1)

	currentFirstFlag := c.anal.isFirst
	mergeArg := merge.NewArgument().WithSinkScan(true)
	mergeArg.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
	rs.setRootOperator(mergeArg)
	c.anal.isFirst = false

	rs.Proc.Reg.MergeReceivers = receivers
	return []*Scope{rs}, nil
}

func (c *Compile) compileSinkNode(n *plan.Node, ss []*Scope, step int32) ([]*Scope, error) {
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
	dispatchLocal := constructDispatchLocal(true, true, n.RecursiveSink, n.RecursiveCte, receivers)
	dispatchLocal.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
	rs.setRootOperator(dispatchLocal)
	c.anal.isFirst = false

	ss = []*Scope{rs}
	return ss, nil
}
func (c *Compile) compileOnduplicateKey(n *plan.Node, ss []*Scope) ([]*Scope, error) {
	rs := c.newMergeScope(ss)

	currentFirstFlag := c.anal.isFirst
	arg := constructOnduplicateKey(n, c.e)
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
func (c *Compile) newDeleteMergeScope(arg *deletion.Deletion, ss []*Scope, n *plan.Node) *Scope {
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

	for i := 0; i < len(ss); i++ {
		dispatchArg := constructDispatch(i, rs, ss[i], n, false)
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
func (c *Compile) newScopeListOnCurrentCN(childrenCount int, blocks int) []*Scope {
	node := getEngineNode(c)
	mcpu := c.generateCPUNumber(node.Mcpu, blocks)
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
	//c.anal.isFirst = false
	return ss
}

func (c *Compile) newScopeListForMinusAndIntersect(rs, left, right []*Scope, n *plan.Node) []*Scope {
	// construct left
	left = c.mergeShuffleScopesIfNeeded(left, false)
	leftMerge := c.newMergeScope(left)
	leftDispatch := constructDispatch(0, rs, leftMerge, n, false)
	leftDispatch.SetAnalyzeControl(c.anal.curNodeIdx, false)
	leftMerge.setRootOperator(leftDispatch)
	leftMerge.IsEnd = true

	// construct right
	right = c.mergeShuffleScopesIfNeeded(right, false)
	rightMerge := c.newMergeScope(right)
	rightDispatch := constructDispatch(1, rs, rightMerge, n, false)
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

func (c *Compile) newShuffleJoinScopeList(probeScopes, buildScopes []*Scope, n *plan.Node) []*Scope {
	if len(c.cnList) <= 1 {
		n.Stats.HashmapStats.ShuffleTypeForMultiCN = plan.ShuffleTypeForMultiCN_Simple
	}

	probeScopes = c.mergeShuffleScopesIfNeeded(probeScopes, true)
	buildScopes = c.mergeShuffleScopesIfNeeded(buildScopes, true)

	dop := plan2.GetShuffleDop(ncpu)
	shuffleJoins := make([]*Scope, 0, len(c.cnList)*dop)
	shuffleBuilds := make([]*Scope, 0, len(c.cnList)*dop)

	lenLeft := len(probeScopes)
	lenRight := len(buildScopes)

	for _, cn := range c.cnList {
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
		shuffleJoins = append(shuffleJoins, probes...)
		shuffleBuilds = append(shuffleBuilds, builds...)
	}

	currentFirstFlag := c.anal.isFirst
	for i := range probeScopes {
		shuffleProbeOp := constructShuffleJoinArg(shuffleJoins, n, true)
		//shuffleProbeOp.SetIdx(c.anal.curNodeIdx)
		shuffleProbeOp.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
		probeScopes[i].setRootOperator(shuffleProbeOp)

		if len(c.cnList) > 1 && probeScopes[i].NodeInfo.Mcpu > 1 { // merge here to avoid bugs, delete this in the future
			probeScopes[i] = c.newMergeScopeByCN([]*Scope{probeScopes[i]}, probeScopes[i].NodeInfo)
		}

		dispatchArg := constructDispatch(i, shuffleJoins, probeScopes[i], n, true)
		dispatchArg.SetAnalyzeControl(c.anal.curNodeIdx, false)
		probeScopes[i].setRootOperator(dispatchArg)
		probeScopes[i].IsEnd = true

		for _, js := range shuffleJoins {
			if isSameCN(js.NodeInfo.Addr, probeScopes[i].NodeInfo.Addr) {
				js.PreScopes = append(js.PreScopes, probeScopes[i])
				break
			}
		}
	}

	c.anal.isFirst = currentFirstFlag
	for i := range buildScopes {
		shuffleBuildOp := constructShuffleJoinArg(shuffleJoins, n, false)
		//shuffleBuildOp.SetIdx(c.anal.curNodeIdx)
		shuffleBuildOp.SetAnalyzeControl(c.anal.curNodeIdx, currentFirstFlag)
		buildScopes[i].setRootOperator(shuffleBuildOp)

		if len(c.cnList) > 1 && buildScopes[i].NodeInfo.Mcpu > 1 { // merge here to avoid bugs, delete this in the future
			buildScopes[i] = c.newMergeScopeByCN([]*Scope{buildScopes[i]}, buildScopes[i].NodeInfo)
		}

		dispatchArg := constructDispatch(i, shuffleBuilds, buildScopes[i], n, false)
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
	return shuffleJoins
}

func (c *Compile) generateCPUNumber(cpunum, blocks int) int {
	if cpunum <= 0 || blocks <= 16 || c.IsTpQuery() {
		return 1
	}
	ret := blocks/16 + 1
	if ret < cpunum {
		return ret
	}
	return cpunum
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

func collectTombstones(
	c *Compile,
	n *plan.Node,
	rel engine.Relation,
) (engine.Tombstoner, error) {
	var err error
	var db engine.Database
	//var relData engine.RelData
	var tombstone engine.Tombstoner
	var txnOp client.TxnOperator

	//-----------------------------------------------------------------------------------------------------
	ctx := c.proc.GetTopContext()
	txnOp = c.proc.GetTxnOperator()
	if n.ScanSnapshot != nil && n.ScanSnapshot.TS != nil {
		zeroTS := timestamp.Timestamp{LogicalTime: 0, PhysicalTime: 0}
		snapTS := c.proc.GetTxnOperator().Txn().SnapshotTS
		if !n.ScanSnapshot.TS.Equal(zeroTS) && n.ScanSnapshot.TS.Less(snapTS) {
			if c.proc.GetCloneTxnOperator() != nil {
				txnOp = c.proc.GetCloneTxnOperator()
			} else {
				txnOp = c.proc.GetTxnOperator().CloneSnapshotOp(*n.ScanSnapshot.TS)
				c.proc.SetCloneTxnOperator(txnOp)
			}

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
	tombstone, err = rel.CollectTombstones(ctx, c.TxnOffset, engine.Policy_CollectAllTombstones)
	if err != nil {
		return nil, err
	}

	if n.TableDef.Partition != nil {
		if n.PartitionPrune != nil && n.PartitionPrune.IsPruned {
			for _, partitionItem := range n.PartitionPrune.SelectedPartitions {
				partTableName := partitionItem.PartitionTableName
				subrelation, err := db.Relation(ctx, partTableName, c.proc)
				if err != nil {
					return nil, err
				}
				subTombstone, err := subrelation.CollectTombstones(ctx, c.TxnOffset, engine.Policy_CollectAllTombstones)
				if err != nil {
					return nil, err
				}
				err = tombstone.Merge(subTombstone)
				if err != nil {
					return nil, err
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
				subTombstone, err := subrelation.CollectTombstones(ctx, c.TxnOffset, engine.Policy_CollectAllTombstones)
				if err != nil {
					return nil, err
				}
				err = tombstone.Merge(subTombstone)
				if err != nil {
					return nil, err
				}

			}
		}
	}
	return tombstone, nil
}

func (c *Compile) expandRanges(
	n *plan.Node,
	rel engine.Relation,
	blockFilterList []*plan.Expr) (engine.RelData, error) {
	var err error
	var db engine.Database
	var relData engine.RelData
	var txnOp client.TxnOperator

	//-----------------------------------------------------------------------------------------------------
	ctx := c.proc.Ctx
	txnOp = c.proc.GetTxnOperator()
	if n.ScanSnapshot != nil && n.ScanSnapshot.TS != nil {
		if !n.ScanSnapshot.TS.Equal(timestamp.Timestamp{LogicalTime: 0, PhysicalTime: 0}) &&
			n.ScanSnapshot.TS.Less(c.proc.GetTxnOperator().Txn().SnapshotTS) {
			if c.proc.GetCloneTxnOperator() != nil {
				txnOp = c.proc.GetCloneTxnOperator()
			} else {
				txnOp = c.proc.GetTxnOperator().CloneSnapshotOp(*n.ScanSnapshot.TS)
				c.proc.SetCloneTxnOperator(txnOp)
			}

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
	relData, err = rel.Ranges(ctx, blockFilterList, c.TxnOffset)
	if err != nil {
		return nil, err
	}
	//tombstones, err := rel.CollectTombstones(ctx, c.TxnOffset)

	if n.TableDef.Partition != nil {
		if n.PartitionPrune != nil && n.PartitionPrune.IsPruned {
			for i, partitionItem := range n.PartitionPrune.SelectedPartitions {
				partTableName := partitionItem.PartitionTableName
				subrelation, err := db.Relation(ctx, partTableName, c.proc)
				if err != nil {
					return nil, err
				}
				subRelData, err := subrelation.Ranges(ctx, blockFilterList, c.TxnOffset)
				if err != nil {
					return nil, err
				}

				engine.ForRangeBlockInfo(1, subRelData.DataCnt(), subRelData,
					func(blk objectio.BlockInfo) (bool, error) {
						blk.PartitionNum = int16(i)
						relData.AppendBlockInfo(&blk)
						return true, nil
					})
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
				subRelData, err := subrelation.Ranges(ctx, blockFilterList, c.TxnOffset)
				if err != nil {
					return nil, err
				}

				engine.ForRangeBlockInfo(1, subRelData.DataCnt(), subRelData,
					func(blk objectio.BlockInfo) (bool, error) {
						blk.PartitionNum = int16(i)
						relData.AppendBlockInfo(&blk)
						return true, nil
					})
			}
		}
	}
	return relData, nil

}

func (c *Compile) generateNodes(n *plan.Node) (engine.Nodes, []any, []types.T, error) {
	var err error
	var db engine.Database
	var rel engine.Relation
	//var ranges engine.Ranges
	var relData engine.RelData
	var partialResults []any
	var partialResultTypes []types.T
	var nodes engine.Nodes
	var txnOp client.TxnOperator

	//------------------------------------------------------------------------------------------------------------------
	ctx := c.proc.GetTopContext()
	txnOp = c.proc.GetTxnOperator()
	if n.ScanSnapshot != nil && n.ScanSnapshot.TS != nil {
		if !n.ScanSnapshot.TS.Equal(timestamp.Timestamp{LogicalTime: 0, PhysicalTime: 0}) &&
			n.ScanSnapshot.TS.Less(c.proc.GetTxnOperator().Txn().SnapshotTS) {

			txnOp = c.proc.GetTxnOperator().CloneSnapshotOp(*n.ScanSnapshot.TS)
			c.proc.SetCloneTxnOperator(txnOp)

			if n.ScanSnapshot.Tenant != nil {
				ctx = context.WithValue(ctx, defines.TenantIDKey{}, n.ScanSnapshot.Tenant.TenantID)
			}
		}
	}
	//-------------------------------------------------------------------------------------------------------------
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
		if c.isPrepare {
			return nil, nil, nil, cantCompileForPrepareErr
		}
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
		//@todo need remove expandRanges from Compile.
		// all expandRanges should be called by Run
		var filterExpr []*plan.Expr
		if len(n.BlockFilterList) > 0 {
			filterExpr = plan2.DeepCopyExprList(n.BlockFilterList)
			for _, e := range filterExpr {
				_, err := plan2.ReplaceFoldExpr(c.proc, e, &c.filterExprExes)
				if err != nil {
					return nil, nil, nil, err
				}
			}
			for _, e := range filterExpr {
				err = plan2.EvalFoldExpr(c.proc, e, &c.filterExprExes)
				if err != nil {
					return nil, nil, nil, err
				}
			}
		}

		relData, err = c.expandRanges(n, rel, filterExpr)
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

	if len(n.AggList) > 0 && relData.DataCnt() > 1 {
		var columnMap map[int]int
		partialResults, partialResultTypes, columnMap = checkAggOptimize(n)
		if partialResults != nil {
			newRelData := relData.BuildEmptyRelData()
			blk := relData.GetBlockInfo(0)
			newRelData.AppendBlockInfo(&blk)

			tombstones, err := collectTombstones(c, n, rel)
			if err != nil {
				return nil, nil, nil, err
			}

			fs, err := fileservice.Get[fileservice.FileService](c.proc.GetFileService(), defines.SharedFileServiceName)
			if err != nil {
				return nil, nil, nil, err
			}
			//For each blockinfo in relData, if blk has no tombstones, then compute the agg result,
			//otherwise put it into newRelData.
			var (
				hasTombstone bool
				err2         error
			)
			if err = engine.ForRangeBlockInfo(1, relData.DataCnt(), relData, func(blk objectio.BlockInfo) (bool, error) {
				if hasTombstone, err2 = tombstones.HasBlockTombstone(
					ctx, &blk.BlockID, fs,
				); err2 != nil {
					return false, err2
				} else if blk.IsAppendable() || hasTombstone {
					newRelData.AppendBlockInfo(&blk)
					return true, nil
				}
				if c.evalAggOptimize(n, blk, partialResults, partialResultTypes, columnMap) != nil {
					partialResults = nil
					return false, nil
				}
				return true, nil
			}); err != nil {
				return nil, nil, nil, err
			}
			if partialResults != nil {
				relData = newRelData
			}
		}
	}

	// some log for finding a bug.
	tblId := rel.GetTableID(ctx)
	expectedLen := relData.DataCnt()
	c.proc.Debugf(ctx, "cn generateNodes, tbl %d ranges is %d", tblId, expectedLen)

	// if len(ranges) == 0 indicates that it's a temporary table.
	if relData.DataCnt() == 0 && n.TableDef.TableType != catalog.SystemOrdinaryRel {
		nodes = make(engine.Nodes, len(c.cnList))
		for i, node := range c.cnList {
			nodes[i] = engine.Node{
				Id:   node.Id,
				Addr: node.Addr,
				Mcpu: c.generateCPUNumber(node.Mcpu, int(n.Stats.BlockNum)),
				Data: engine.BuildEmptyRelData(),
			}
		}
		return nodes, partialResults, partialResultTypes, nil
	}

	engineType := rel.GetEngineType()
	// for an ordered scan, put all paylonds in current CN
	// or sometimes force on one CN
	if len(n.OrderBy) > 0 || relData.DataCnt() < plan2.BlockThresholdForOneCN || n.Stats.ForceOneCN {
		return putBlocksInCurrentCN(c, relData, n), partialResults, partialResultTypes, nil
	}
	// disttae engine
	if engineType == engine.Disttae {
		nodes, err := shuffleBlocksToMultiCN(c, rel, relData, n)
		return nodes, partialResults, partialResultTypes, err
	}
	// maybe temp table on memengine , just put payloads in average
	return putBlocksInAverage(c, relData, n), partialResults, partialResultTypes, nil
}

func checkAggOptimize(n *plan.Node) ([]any, []types.T, map[int]int) {
	partialResults := make([]any, len(n.AggList))
	partialResultTypes := make([]types.T, len(n.AggList))
	columnMap := make(map[int]int)
	for i := range n.AggList {
		agg := n.AggList[i].Expr.(*plan.Expr_F)
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
				}
				return nil, nil, nil
			} else {
				columnMap[int(col.Col.ColPos)] = int(n.TableDef.Cols[int(col.Col.ColPos)].Seqnum)
			}
		case "min", "max":
			partialResults[i] = nil
			col, ok := args.Expr.(*plan.Expr_Col)
			if !ok {
				return nil, nil, nil
			}
			columnMap[int(col.Col.ColPos)] = int(n.TableDef.Cols[int(col.Col.ColPos)].Seqnum)
		default:
			return nil, nil, nil
		}
	}
	return partialResults, partialResultTypes, columnMap
}

func (c *Compile) evalAggOptimize(n *plan.Node, blk objectio.BlockInfo, partialResults []any, partialResultTypes []types.T, columnMap map[int]int) error {
	if len(n.AggList) == 1 && n.AggList[0].Expr.(*plan.Expr_F).F.Func.ObjName == "starcount" {
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

func putBlocksInAverage(c *Compile, relData engine.RelData, n *plan.Node) engine.Nodes {
	var nodes engine.Nodes
	step := (relData.DataCnt() + len(c.cnList) - 1) / len(c.cnList)
	for i := 0; i < relData.DataCnt(); i += step {
		j := i / step
		if i+step >= relData.DataCnt() {
			if isSameCN(c.cnList[j].Addr, c.addr) {
				if len(nodes) == 0 {
					nodes = append(nodes, engine.Node{
						Addr: c.addr,
						Mcpu: c.generateCPUNumber(ncpu, int(n.Stats.BlockNum)),
						Data: relData.BuildEmptyRelData(),
					})
				}

				engine.ForRangeShardID(i, relData.DataCnt(), relData,
					func(shardID uint64) (bool, error) {
						nodes[0].Data.AppendShardID(shardID)
						return true, nil
					})

			} else {

				node := engine.Node{
					Id:   c.cnList[j].Id,
					Addr: c.cnList[j].Addr,
					Mcpu: c.generateCPUNumber(c.cnList[j].Mcpu, int(n.Stats.BlockNum)),
					Data: relData.BuildEmptyRelData(),
				}

				engine.ForRangeShardID(i, relData.DataCnt(), relData,
					func(shardID uint64) (bool, error) {
						node.Data.AppendShardID(shardID)
						return true, nil
					})

				nodes = append(nodes, node)
			}
		} else {
			if isSameCN(c.cnList[j].Addr, c.addr) {
				if len(nodes) == 0 {
					nodes = append(nodes, engine.Node{
						Addr: c.addr,
						Mcpu: c.generateCPUNumber(ncpu, int(n.Stats.BlockNum)),
						Data: relData.BuildEmptyRelData(),
					})
				}
				//nodes[0].Data = append(nodes[0].Data, ranges.Slice(i, i+step)...)

				engine.ForRangeShardID(i, i+step, relData,
					func(shardID uint64) (bool, error) {
						nodes[0].Data.AppendShardID(shardID)
						return true, nil
					})

			} else {
				node := engine.Node{
					Id:   c.cnList[j].Id,
					Addr: c.cnList[j].Addr,
					Mcpu: c.generateCPUNumber(c.cnList[j].Mcpu, int(n.Stats.BlockNum)),
					Data: relData.BuildEmptyRelData(),
				}

				engine.ForRangeShardID(i, i+step, relData,
					func(shardID uint64) (bool, error) {
						node.Data.AppendShardID(shardID)
						return true, nil
					})

				nodes = append(nodes, node)
			}
		}
	}
	return nodes
}

func removeEmtpyNodes(
	c *Compile,
	n *plan.Node,
	rel engine.Relation,
	relData engine.RelData,
	nodes engine.Nodes) (engine.Nodes, error) {
	minWorkLoad := math.MaxInt32
	maxWorkLoad := 0
	// remove empty node from nodes
	var newNodes engine.Nodes
	for i := range nodes {
		if nodes[i].Data.DataCnt() > maxWorkLoad {
			maxWorkLoad = nodes[i].Data.DataCnt() / objectio.BlockInfoSize
		}
		if nodes[i].Data.DataCnt() < minWorkLoad {
			minWorkLoad = nodes[i].Data.DataCnt() / objectio.BlockInfoSize
		}
		if nodes[i].Data.DataCnt() > 0 {
			if nodes[i].Addr != c.addr {
				tombstone, err := collectTombstones(c, n, rel)
				if err != nil {
					return nil, err
				}
				nodes[i].Data.AttachTombstones(tombstone)
			}
			newNodes = append(newNodes, nodes[i])
		}
	}
	if minWorkLoad*2 < maxWorkLoad {
		logstring := fmt.Sprintf("read table %v ,workload %v blocks among %v nodes not balanced, max %v, min %v,",
			n.TableDef.Name,
			relData.DataCnt(),
			len(newNodes),
			maxWorkLoad,
			minWorkLoad)
		logstring = logstring + " cnlist: "
		for i := range c.cnList {
			logstring = logstring + c.cnList[i].Addr + " "
		}
		c.proc.Warn(c.proc.Ctx, logstring)
	}
	return newNodes, nil
}

func shuffleBlocksToMultiCN(c *Compile, rel engine.Relation, relData engine.RelData, n *plan.Node) (engine.Nodes, error) {
	var nodes engine.Nodes
	// add current CN
	nodes = append(nodes, engine.Node{
		Addr: c.addr,
		Mcpu: c.generateCPUNumber(ncpu, int(n.Stats.BlockNum)),
	})
	// add memory table block
	nodes[0].Data = relData.BuildEmptyRelData()
	nodes[0].Data.AppendBlockInfo(&objectio.EmptyBlockInfo)
	// only memory table block
	if relData.DataCnt() == 1 {
		return nodes, nil
	}
	// only one cn
	if len(c.cnList) == 1 {
		engine.ForRangeBlockInfo(1, relData.DataCnt(), relData,
			func(blk objectio.BlockInfo) (bool, error) {
				nodes[0].Data.AppendBlockInfo(&blk)
				return true, nil
			})

		return nodes, nil
	}

	// add the rest of CNs in list
	for i := range c.cnList {
		if c.cnList[i].Addr != c.addr {
			nodes = append(nodes, engine.Node{
				Id:   c.cnList[i].Id,
				Addr: c.cnList[i].Addr,
				Mcpu: c.generateCPUNumber(c.cnList[i].Mcpu, int(n.Stats.BlockNum)),
				Data: relData.BuildEmptyRelData(),
			})
		}
	}

	if force, tids, cnt := engine.GetForceShuffleReader(); force {
		for _, tid := range tids {
			if tid == n.TableDef.TblId {
				shuffleBlocksByMoCtl(relData, cnt, nodes)
				return removeEmtpyNodes(c, n, rel, relData, nodes)
			}
		}
	}

	sort.Slice(nodes, func(i, j int) bool { return nodes[i].Addr < nodes[j].Addr })

	if n.Stats.HashmapStats != nil && n.Stats.HashmapStats.Shuffle && n.Stats.HashmapStats.ShuffleType == plan.ShuffleType_Range {
		err := shuffleBlocksByRange(c, relData, n, nodes)
		if err != nil {
			return nil, err
		}
	} else {
		shuffleBlocksByHash(c, relData, nodes)
	}

	return removeEmtpyNodes(c, n, rel, relData, nodes)

	//minWorkLoad := math.MaxInt32
	//maxWorkLoad := 0
	//// remove empty node from nodes
	//var newNodes engine.Nodes
	//for i := range nodes {
	//	if nodes[i].Data.DataCnt() > maxWorkLoad {
	//		maxWorkLoad = nodes[i].Data.DataCnt() / objectio.BlockInfoSize
	//	}
	//	if nodes[i].Data.DataCnt() < minWorkLoad {
	//		minWorkLoad = nodes[i].Data.DataCnt() / objectio.BlockInfoSize
	//	}
	//	if nodes[i].Data.DataCnt() > 0 {
	//		if nodes[i].Addr != c.addr {
	//			tombstone, err := collectTombstones(c, n, rel)
	//			if err != nil {
	//				return nil, err
	//			}
	//			nodes[i].Data.AttachTombstones(tombstone)
	//		}
	//		newNodes = append(newNodes, nodes[i])
	//	}
	//}
	//if minWorkLoad*2 < maxWorkLoad {
	//	logstring := fmt.Sprintf("read table %v ,workload %v blocks among %v nodes not balanced, max %v, min %v,",
	//		n.TableDef.Name,
	//		relData.DataCnt(),
	//		len(newNodes),
	//		maxWorkLoad,
	//		minWorkLoad)
	//	logstring = logstring + " cnlist: "
	//	for i := range c.cnList {
	//		logstring = logstring + c.cnList[i].Addr + " "
	//	}
	//	c.proc.Warnf(c.proc.Ctx, logstring)
	//}
	//return newNodes, nil
}

func shuffleBlocksByHash(c *Compile, relData engine.RelData, nodes engine.Nodes) {
	engine.ForRangeBlockInfo(1, relData.DataCnt(), relData,
		func(blk objectio.BlockInfo) (bool, error) {
			location := blk.MetaLocation()
			objTimeStamp := location.Name()[:7]
			index := plan2.SimpleCharHashToRange(objTimeStamp, uint64(len(c.cnList)))
			nodes[index].Data.AppendBlockInfo(&blk)
			return true, nil
		})
}

// Just for test
func shuffleBlocksByMoCtl(relData engine.RelData, cnt int, nodes engine.Nodes) error {
	if cnt > relData.DataCnt()-1 {
		return moerr.NewInternalErrorNoCtxf(
			"Invalid Parameter, distribute count:%d, block count:%d",
			cnt,
			relData.DataCnt()-1)
	}

	if len(nodes) < 2 {
		return moerr.NewInternalErrorNoCtx("Invalid count of nodes")
	}

	engine.ForRangeBlockInfo(
		1,
		cnt,
		relData,
		func(blk objectio.BlockInfo) (bool, error) {
			nodes[1].Data.AppendBlockInfo(&blk)
			return true, nil
		})

	return nil
}

func shuffleBlocksByRange(c *Compile, relData engine.RelData, n *plan.Node, nodes engine.Nodes) error {
	var objDataMeta objectio.ObjectDataMeta
	var objMeta objectio.ObjectMeta

	var shuffleRangeUint64 []uint64
	var shuffleRangeInt64 []int64
	var init bool
	var index uint64

	engine.ForRangeBlockInfo(1, relData.DataCnt(), relData,
		func(blk objectio.BlockInfo) (bool, error) {
			location := blk.MetaLocation()
			fs, err := fileservice.Get[fileservice.FileService](c.proc.Base.FileService, defines.SharedFileServiceName)
			if err != nil {
				return false, err
			}
			if !objectio.IsSameObjectLocVsMeta(location, objDataMeta) {
				if objMeta, err = objectio.FastLoadObjectMeta(c.proc.Ctx, &location, false, fs); err != nil {
					return false, err
				}
				objDataMeta = objMeta.MustDataMeta()
			}
			blkMeta := objDataMeta.GetBlockMeta(uint32(location.ID()))
			zm := blkMeta.MustGetColumn(uint16(n.Stats.HashmapStats.ShuffleColIdx)).ZoneMap()
			if !zm.IsInited() {
				// a block with all null will send to first CN
				nodes[0].Data.AppendBlockInfo(&blk)
				return false, nil
			}
			if !init {
				init = true
				switch zm.GetType() {
				case types.T_int64, types.T_int32, types.T_int16:
					shuffleRangeInt64 = plan2.ShuffleRangeReEvalSigned(n.Stats.HashmapStats.Ranges, len(c.cnList), n.Stats.HashmapStats.Nullcnt, int64(n.Stats.TableCnt))
				case types.T_uint64, types.T_uint32, types.T_uint16, types.T_varchar, types.T_char, types.T_text, types.T_bit, types.T_datalink:
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
			nodes[index].Data.AppendBlockInfo(&blk)
			return true, nil
		})

	return nil
}

func putBlocksInCurrentCN(c *Compile, relData engine.RelData, n *plan.Node) engine.Nodes {
	var nodes engine.Nodes
	// add current CN
	nodes = append(nodes, engine.Node{
		Addr: c.addr,
		Mcpu: c.generateCPUNumber(ncpu, int(n.Stats.BlockNum)),
	})
	nodes[0].Data = relData
	return nodes
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
				return marg.AffectedRows()
			}
			affectedRows += arg.AffectedRows()
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

func (c *Compile) runSqlWithAccountId(sql string, accountId int32) error {
	if sql == "" {
		return nil
	}
	res, err := c.runSqlWithResult(sql, accountId)
	if err != nil {
		return err
	}
	res.Close()
	return nil
}

func (c *Compile) runSqlWithResult(sql string, accountId int32) (executor.Result, error) {
	v, ok := moruntime.ServiceRuntime(c.proc.GetService()).GetGlobalVariables(moruntime.InternalSQLExecutor)
	if !ok {
		panic("missing lock service")
	}

	// default 1
	var lower int64 = 1
	if resolveVariableFunc := c.proc.GetResolveVariableFunc(); resolveVariableFunc != nil {
		lowerVar, err := resolveVariableFunc("lower_case_table_names", true, false)
		if err != nil {
			return executor.Result{}, err
		}
		lower = lowerVar.(int64)
	}

	exec := v.(executor.SQLExecutor)
	opts := executor.Options{}.
		// All runSql and runSqlWithResult is a part of input sql, can not incr statement.
		// All these sub-sql's need to be rolled back and retried en masse when they conflict in pessimistic mode
		WithDisableIncrStatement().
		WithTxn(c.proc.GetTxnOperator()).
		WithDatabase(c.db).
		WithTimeZone(c.proc.GetSessionInfo().TimeZone).
		WithLowerCaseTableNames(&lower)

	ctx := c.proc.Ctx
	if accountId >= 0 {
		ctx = defines.AttachAccountId(c.proc.Ctx, uint32(accountId))
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
			return err
		}
	}

	return nil
}

// runDetectSql runs the fk detecting sql
func runDetectSql(c *Compile, sql string) error {
	res, err := c.runSqlWithResult(sql, NoAccountId)
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
	res, err := c.runSqlWithResult(sql, NoAccountId)
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
		return engine.Node{Addr: c.addr, Mcpu: ncpu}
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
