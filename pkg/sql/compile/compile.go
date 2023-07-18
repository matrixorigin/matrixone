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
	"encoding/json"
	"fmt"
	"math"
	"net"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
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
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/merge"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergeblock"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergedelete"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/output"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// Note: Now the cost going from stat is actually the number of rows, so we can only estimate a number for the size of each row.
// The current insertion of around 200,000 rows triggers cn to write s3 directly
const (
	DistributedThreshold   uint64 = 10 * mpool.MB
	SingleLineSizeEstimate uint64 = 300 * mpool.B
)

var (
	ncpu = runtime.NumCPU()
)

var pool = sync.Pool{
	New: func() any {
		return new(Compile)
	},
}

var analPool = sync.Pool{
	New: func() any {
		return new(process.AnalyzeInfo)
	},
}

// New is used to new an object of compile
func New(addr, db string, sql string, tenant, uid string, ctx context.Context,
	e engine.Engine, proc *process.Process, stmt tree.Statement, isInternal bool, cnLabel map[string]string) *Compile {
	c := pool.Get().(*Compile)
	c.clear()
	c.e = e
	c.db = db
	c.ctx = ctx
	c.tenant = tenant
	c.uid = uid
	c.sql = sql
	c.proc = proc
	c.stmt = stmt
	c.addr = addr
	c.nodeRegs = make(map[int32]*process.WaitRegister)
	c.stepRegs = make(map[int32][]int32)
	c.isInternal = isInternal
	c.cnLabel = cnLabel
	c.runtimeFilterReceiverMap = make(map[int32]chan *pipeline.RuntimeFilter)
	return c
}

func (c *Compile) clear() {
	c.scope = c.scope[:0]
	c.pn = nil
	c.u = nil
	c.fill = nil
	c.affectRows.Store(0)
	c.addr = ""
	c.db = ""
	c.tenant = ""
	c.uid = ""
	c.sql = ""
	c.anal = nil
	c.e = nil
	c.ctx = nil
	c.proc = nil
	c.cnList = nil
	c.stmt = nil
	for k := range c.nodeRegs {
		delete(c.nodeRegs, k)
	}
	for k := range c.stepRegs {
		delete(c.stepRegs, k)
	}
	for k := range c.runtimeFilterReceiverMap {
		delete(c.runtimeFilterReceiverMap, k)
	}
	c.isInternal = false
	for k := range c.cnLabel {
		delete(c.cnLabel, k)
	}
}

// helper function to judge if init temporary engine is needed
func (c *Compile) NeedInitTempEngine(InitTempEngine bool) bool {
	if InitTempEngine {
		return false
	}
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

func (c *Compile) SetTempEngine(ctx context.Context, te engine.Engine) {
	e := c.e.(*engine.EntireEngine)
	e.TempEngine = te
	c.ctx = ctx
}

// Compile is the entrance of the compute-execute-layer.
// It generates a scope (logic pipeline) for a query plan.
func (c *Compile) Compile(ctx context.Context, pn *plan.Plan, u any, fill func(any, *batch.Batch) error) (err error) {
	defer func() {
		if e := recover(); e != nil {
			err = moerr.ConvertPanicError(ctx, e)
		}
	}()

	// with values
	c.proc.Ctx = perfcounter.WithCounterSet(c.proc.Ctx, &c.s3CounterSet)
	c.ctx = c.proc.Ctx

	// session info and callback function to write back query result.
	// XXX u is really a bad name, I'm not sure if `session` or `user` will be more suitable.
	c.u = u
	c.fill = fill

	c.pn = pn
	// get execute related information
	// about ap or tp, what and how many compute resource we can use.
	c.info = plan2.GetExecTypeFromPlan(pn)
	if pn.IsPrepare {
		c.info.Typ = plan2.ExecTypeTP
	}

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
	return nil
}

func (c *Compile) addAffectedRows(n uint64) {
	c.affectRows.Add(n)
}

func (c *Compile) setAffectedRows(n uint64) {
	c.affectRows.Store(n)
}

func (c *Compile) GetAffectedRows() uint64 {
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
		c.setAffectedRows(s.Instructions[len(s.Instructions)-1].Arg.(*mergedelete.Argument).AffectedRows)
		return nil
	case Remote:
		defer c.fillAnalyzeInfo()
		return s.RemoteRun(c)
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
	case CreateIndex:
		return s.CreateIndex(c)
	case DropIndex:
		return s.DropIndex(c)
	case TruncateTable:
		return s.TruncateTable(c)
	case Deletion:
		defer c.fillAnalyzeInfo()
		affectedRows, err := s.Delete(c)
		if err != nil {
			return err
		}
		c.setAffectedRows(affectedRows)
		return nil
	case Insert:
		defer c.fillAnalyzeInfo()
		affectedRows, err := s.Insert(c)
		if err != nil {
			return err
		}
		c.setAffectedRows(affectedRows)
		return nil
	}
	return nil
}

// Run is an important function of the compute-layer, it executes a single sql according to its scope
func (c *Compile) Run(_ uint64) error {
	defer func() {
		if c.anal != nil {
			for i := range c.anal.analInfos {
				analPool.Put(c.anal.analInfos[i])
			}
			c.anal.analInfos = nil
		}

		c.proc.CleanValueScanBatchs()
		pool.Put(c)
	}()
	if c.proc.TxnOperator != nil {
		c.proc.TxnOperator.ResetRetry(false)
	}
	if err := c.runOnce(); err != nil {
		//  if the error is ErrTxnNeedRetry and the transaction is RC isolation, we need to retry the statement
		if moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetry) &&
			c.proc.TxnOperator.Txn().IsRCIsolation() {
			c.proc.TxnOperator.ResetRetry(true)

			// clear the workspace of the failed statement
			if err = c.proc.TxnOperator.GetWorkspace().RollbackLastStatement(c.ctx); err != nil {
				return err
			}
			//  increase the statement id
			if err = c.proc.TxnOperator.GetWorkspace().IncrStatementID(c.ctx, false); err != nil {
				return err
			}

			// FIXME: the current retry method is quite bad, the overhead is relatively large, and needs to be
			// improved to refresh expression in the future.
			cc := New(
				c.addr,
				c.db,
				c.sql,
				c.tenant,
				c.uid,
				c.proc.Ctx,
				c.e,
				c.proc,
				c.stmt,
				c.isInternal,
				c.cnLabel)
			if err := cc.Compile(c.proc.Ctx, c.pn, c.u, c.fill); err != nil {
				return err
			}
			if err := cc.runOnce(); err != nil {
				return err
			}
			// set affectedRows to old compile to return
			c.setAffectedRows(cc.GetAffectedRows())
			return nil
		}
		return err
	}
	return nil
}

// run once
func (c *Compile) runOnce() error {
	var wg sync.WaitGroup

	errC := make(chan error, len(c.scope))
	for _, s := range c.scope {
		s.SetContextRecursively(c.proc.Ctx)
	}
	for _, s := range c.scope {
		wg.Add(1)
		go func(scope *Scope) {
			errC <- c.run(scope)
			wg.Done()
		}(s)
	}
	wg.Wait()
	c.scope = nil
	close(errC)
	for e := range errC {
		if e != nil {
			return e
		}
	}
	return nil
}

func (c *Compile) compileScope(ctx context.Context, pn *plan.Plan) ([]*Scope, error) {
	switch qry := pn.Plan.(type) {
	case *plan.Plan_Query:
		scopes, err := c.compileQuery(ctx, qry.Query)
		if err != nil {
			return nil, err
		}
		for _, s := range scopes {
			s.Plan = pn
		}
		return scopes, nil
	case *plan.Plan_Ddl:
		switch qry.Ddl.DdlType {
		case plan.DataDefinition_CREATE_DATABASE:
			return []*Scope{{
				Magic: CreateDatabase,
				Plan:  pn,
			}}, nil
		case plan.DataDefinition_DROP_DATABASE:
			return []*Scope{{
				Magic: DropDatabase,
				Plan:  pn,
			}}, nil
		case plan.DataDefinition_CREATE_TABLE:
			return []*Scope{{
				Magic: CreateTable,
				Plan:  pn,
			}}, nil
		case plan.DataDefinition_ALTER_VIEW:
			return []*Scope{{
				Magic: AlterView,
				Plan:  pn,
			}}, nil
		case plan.DataDefinition_ALTER_TABLE:
			return []*Scope{{
				Magic: AlterTable,
				Plan:  pn,
			}}, nil
		case plan.DataDefinition_DROP_TABLE:
			return []*Scope{{
				Magic: DropTable,
				Plan:  pn,
			}}, nil
		case plan.DataDefinition_DROP_SEQUENCE:
			return []*Scope{{
				Magic: DropSequence,
				Plan:  pn,
			}}, nil
		case plan.DataDefinition_TRUNCATE_TABLE:
			return []*Scope{{
				Magic: TruncateTable,
				Plan:  pn,
			}}, nil
		case plan.DataDefinition_CREATE_SEQUENCE:
			return []*Scope{{
				Magic: CreateSequence,
				Plan:  pn,
			}}, nil
		case plan.DataDefinition_CREATE_INDEX:
			return []*Scope{{
				Magic: CreateIndex,
				Plan:  pn,
			}}, nil
		case plan.DataDefinition_DROP_INDEX:
			return []*Scope{{
				Magic: DropIndex,
				Plan:  pn,
			}}, nil
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

func (c *Compile) cnListStrategy() {
	if len(c.cnList) == 0 {
		c.cnList = append(c.cnList, engine.Node{
			Addr: c.addr,
			Mcpu: ncpu,
		})
	} else if len(c.cnList) > c.info.CnNumbers {
		c.cnList = c.cnList[:c.info.CnNumbers]
	}
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

func (c *Compile) compileQuery(ctx context.Context, qry *plan.Query) ([]*Scope, error) {
	var err error
	c.cnList, err = c.e.Nodes(c.isInternal, c.tenant, c.uid, c.cnLabel)
	if err != nil {
		return nil, err
	}
	// sort by addr to get fixed order of CN list
	sort.Slice(c.cnList, func(i, j int) bool { return c.cnList[i].Addr < c.cnList[j].Addr })

	if c.info.Typ == plan2.ExecTypeAP {
		c.removeUnavailableCN()
	}

	c.info.CnNumbers = len(c.cnList)
	blkNum := 0
	cost := float64(0.0)
	for _, n := range qry.Nodes {
		if n.Stats == nil {
			continue
		}
		if n.NodeType == plan.Node_TABLE_SCAN {
			blkNum += int(n.Stats.BlockNum)
		}
		if n.NodeType == plan.Node_INSERT {
			cost += n.Stats.GetCost()
		}
	}
	switch qry.StmtType {
	case plan.Query_INSERT:
		if cost*float64(SingleLineSizeEstimate) > float64(DistributedThreshold) || qry.LoadTag || blkNum >= plan2.BlockNumForceOneCN {
			c.cnListStrategy()
		} else {
			c.cnList = engine.Nodes{engine.Node{
				Addr: c.addr,
				Mcpu: c.generateCPUNumber(ncpu, blkNum)},
			}
		}
		// insertNode := qry.Nodes[qry.Steps[0]]
		// nodeStats := qry.Nodes[insertNode.Children[0]].Stats
		// if nodeStats.GetCost()*float64(SingleLineSizeEstimate) > float64(DistributedThreshold) || qry.LoadTag || blkNum >= MinBlockNum {
		// 	if len(insertNode.InsertCtx.OnDuplicateIdx) > 0 {
		// 		c.cnList = engine.Nodes{
		// 			engine.Node{
		// 				Addr: c.addr,
		// 				Mcpu: c.generateCPUNumber(1, blkNum)},
		// 		}
		// 	} else {
		// 		c.cnListStrategy()
		// 	}
		// } else {
		// 	if len(insertNode.InsertCtx.OnDuplicateIdx) > 0 {
		// 		c.cnList = engine.Nodes{
		// 			engine.Node{
		// 				Addr: c.addr,
		// 				Mcpu: c.generateCPUNumber(1, blkNum)},
		// 		}
		// 	} else {
		// 		c.cnList = engine.Nodes{engine.Node{
		// 			Addr: c.addr,
		// 			Mcpu: c.generateCPUNumber(c.NumCPU(), blkNum)},
		// 		}
		// 	}
		// }
	default:
		if blkNum < plan2.BlockNumForceOneCN {
			c.cnList = engine.Nodes{engine.Node{
				Addr: c.addr,
				Mcpu: c.generateCPUNumber(ncpu, blkNum)},
			}
		} else {
			c.cnListStrategy()
		}
	}
	if c.info.Typ == plan2.ExecTypeTP && len(c.cnList) > 1 {
		c.cnList = engine.Nodes{engine.Node{
			Addr: c.addr,
			Mcpu: c.generateCPUNumber(ncpu, blkNum)},
		}
	}

	c.initAnalyze(qry)

	//deal with sink scan first.
	for i := len(qry.Steps) - 1; i >= 0; i-- {
		err := c.compileSinkScan(qry, qry.Steps[i])
		if err != nil {
			return nil, err
		}
	}

	steps := make([]*Scope, 0, len(qry.Steps))
	for i := len(qry.Steps) - 1; i >= 0; i-- {
		scopes, err := c.compilePlanScope(ctx, int32(i), qry.Steps[i], qry.Nodes)
		if err != nil {
			return nil, err
		}
		scope, err := c.compileApQuery(qry, scopes)
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

	if n.NodeType == plan.Node_SINK_SCAN {
		var wr *process.WaitRegister
		if c.anal.qry.LoadTag {
			wr = &process.WaitRegister{
				Ctx: c.ctx,
				Ch:  make(chan *batch.Batch, ncpu),
			}
		} else {
			wr = &process.WaitRegister{
				Ctx: c.ctx,
				Ch:  make(chan *batch.Batch, 1),
			}

		}
		c.appendStepRegs(n.SourceStep, nodeId, wr)
	}
	return nil
}

func (c *Compile) compileApQuery(qry *plan.Query, ss []*Scope) (*Scope, error) {
	var rs *Scope
	switch qry.StmtType {
	case plan.Query_DELETE:
		return ss[0], nil
	case plan.Query_INSERT:
		return ss[0], nil
	case plan.Query_UPDATE:
		return ss[0], nil
	default:
		rs = c.newMergeScope(ss)
		updateScopesLastFlag([]*Scope{rs})
		c.setAnalyzeCurrent([]*Scope{rs}, c.anal.curr)
		rs.Instructions = append(rs.Instructions, vm.Instruction{
			Op: vm.Output,
			Arg: &output.Argument{
				Data: c.u,
				Func: c.fill,
			},
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
		bat.InitZsOne(1)
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
						str := vs[row.Pos-1].GetString(params.GetArea())
						if err := util.SetBytesToAnyVector(ctx, str, int(row.RowPos), isNull, bat.Vecs[i],
							proc); err != nil {
							return nil, err
						}
					}
				}
			}
			if err := evalRowsetData(ctx, proc, colsData[i].Data, bat.Vecs[i], exprList); err != nil {
				bat.Clean(proc.Mp())
				return nil, err
			}
		}
	}
	return bat, nil
}

func (c *Compile) compilePlanScope(ctx context.Context, step int32, curNodeIdx int32, ns []*plan.Node) ([]*Scope, error) {
	n := ns[curNodeIdx]
	switch n.NodeType {
	case plan.Node_VALUE_SCAN:
		bat, err := constructValueScanBatch(ctx, c.proc, n)
		if err != nil {
			return nil, err
		}
		ds := &Scope{
			Magic:      Normal,
			DataSource: &Source{Bat: bat},
			NodeInfo:   engine.Node{Addr: c.addr, Mcpu: 1},
			Proc:       process.NewWithAnalyze(c.proc, c.ctx, 0, c.anal.Nodes()),
		}
		return c.compileSort(n, c.compileProjection(n, []*Scope{ds})), nil
	case plan.Node_EXTERNAL_SCAN:
		node := plan2.DeepCopyNode(n)
		ss, err := c.compileExternScan(ctx, node)
		if err != nil {
			return nil, err
		}
		return c.compileSort(n, c.compileProjection(n, c.compileRestrict(node, ss))), nil
	case plan.Node_TABLE_SCAN:
		ss, err := c.compileTableScan(n)
		if err != nil {
			return nil, err
		}

		// RelationName
		return c.compileSort(n, c.compileProjection(n, c.compileRestrict(n, ss))), nil
	case plan.Node_FILTER, plan.Node_PROJECT, plan.Node_PRE_DELETE:
		curr := c.anal.curr
		c.setAnalyzeCurrent(nil, int(n.Children[0]))
		ss, err := c.compilePlanScope(ctx, step, n.Children[0], ns)
		if err != nil {
			return nil, err
		}
		c.setAnalyzeCurrent(ss, curr)
		return c.compileSort(n, c.compileProjection(n, c.compileRestrict(n, ss))), nil
	case plan.Node_AGG:
		curr := c.anal.curr
		c.setAnalyzeCurrent(nil, int(n.Children[0]))
		ss, err := c.compilePlanScope(ctx, step, n.Children[0], ns)
		if err != nil {
			return nil, err
		}
		c.setAnalyzeCurrent(ss, curr)

		if n.Stats.Shuffle {
			ss = c.compileShuffleGroup(n, ss, ns)
			return c.compileSort(n, ss), nil
		} else {
			ss = c.compileMergeGroup(n, ss, ns)
			return c.compileSort(n, c.compileProjection(n, c.compileRestrict(n, ss))), nil
		}
	case plan.Node_WINDOW:
		curr := c.anal.curr
		c.setAnalyzeCurrent(nil, int(n.Children[0]))
		ss, err := c.compilePlanScope(ctx, step, n.Children[0], ns)
		if err != nil {
			return nil, err
		}
		c.setAnalyzeCurrent(ss, curr)
		ss = c.compileWin(n, ss)
		return c.compileSort(n, c.compileProjection(n, c.compileRestrict(n, ss))), nil
	case plan.Node_JOIN:
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
		return c.compileSort(n, c.compileJoin(ctx, n, ns[n.Children[0]], ns[n.Children[1]], left, right)), nil
	case plan.Node_SORT:
		curr := c.anal.curr
		c.setAnalyzeCurrent(nil, int(n.Children[0]))
		ss, err := c.compilePlanScope(ctx, step, n.Children[0], ns)
		if err != nil {
			return nil, err
		}
		c.setAnalyzeCurrent(ss, curr)
		return c.compileProjection(n, c.compileRestrict(n, c.compileSort(n, ss))), nil
	case plan.Node_UNION:
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
		return c.compileSort(n, c.compileUnion(n, left, right)), nil
	case plan.Node_MINUS, plan.Node_INTERSECT, plan.Node_INTERSECT_ALL:
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
		return c.compileSort(n, c.compileMinusAndIntersect(n, left, right, n.NodeType)), nil
	case plan.Node_UNION_ALL:
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
		return c.compileSort(n, c.compileUnionAll(left, right)), nil
	case plan.Node_DELETE:
		curr := c.anal.curr
		c.setAnalyzeCurrent(nil, int(n.Children[0]))

		ss, err := c.compilePlanScope(ctx, step, n.Children[0], ns)
		if err != nil {
			return nil, err
		}

		n.NotCacheable = true
		nodeStats := ns[n.Children[0]].Stats

		arg, err := constructDeletion(n, c.e, c.proc)
		if err != nil {
			return nil, err
		}

		if nodeStats.GetCost()*float64(SingleLineSizeEstimate) >
			float64(DistributedThreshold) &&
			!arg.DeleteCtx.CanTruncate {
			rs := c.newDeleteMergeScope(arg, ss)
			rs.Instructions = append(rs.Instructions, vm.Instruction{
				Op: vm.MergeDelete,
				Arg: &mergedelete.Argument{
					DelSource:        arg.DeleteCtx.Source,
					PartitionSources: arg.DeleteCtx.PartitionSources,
				},
			})
			rs.Magic = MergeDelete
			ss = []*Scope{rs}
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
		ss, err := c.compilePlanScope(ctx, step, n.Children[0], ns)
		if err != nil {
			return nil, err
		}
		rs := c.newMergeScope(ss)
		rs.Magic = Merge
		c.setAnalyzeCurrent([]*Scope{rs}, c.anal.curr)
		onDuplicateKeyArg, err := constructOnduplicateKey(n, c.e)
		if err != nil {
			return nil, err
		}
		rs.Instructions = append(rs.Instructions, vm.Instruction{
			Op:  vm.OnDuplicateKey,
			Arg: onDuplicateKeyArg,
		})
		ss = []*Scope{rs}
		c.setAnalyzeCurrent(ss, curr)
		return ss, nil
	case plan.Node_PRE_INSERT_UK:
		curr := c.anal.curr
		ss, err := c.compilePlanScope(ctx, step, n.Children[0], ns)
		if err != nil {
			return nil, err
		}
		currentFirstFlag := c.anal.isFirst
		for i := range ss {
			preInsertUkArg, err := constructPreInsertUk(n, c.proc)
			if err != nil {
				return nil, err
			}
			ss[i].appendInstruction(vm.Instruction{
				Op:      vm.PreInsertUnique,
				Idx:     c.anal.curr,
				IsFirst: currentFirstFlag,
				Arg:     preInsertUkArg,
			})
		}
		c.setAnalyzeCurrent(ss, curr)
		return ss, nil
	case plan.Node_PRE_INSERT:
		curr := c.anal.curr
		ss, err := c.compilePlanScope(ctx, step, n.Children[0], ns)
		if err != nil {
			return nil, err
		}
		currentFirstFlag := c.anal.isFirst
		for i := range ss {
			preInsertArg, err := constructPreInsert(n, c.e, c.proc)
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
		curr := c.anal.curr
		n.NotCacheable = true
		ss, err := c.compilePlanScope(ctx, step, n.Children[0], ns)
		if err != nil {
			return nil, err
		}

		currentFirstFlag := c.anal.isFirst
		toWriteS3 := n.Stats.GetCost()*float64(SingleLineSizeEstimate) >
			float64(DistributedThreshold) || c.anal.qry.LoadTag

		if toWriteS3 {
			if !haveSinkScanInPlan(ns, n.Children[0]) && len(ss) != 1 {
				insertArg, err := constructInsert(n, c.e, c.proc)
				if err != nil {
					return nil, err
				}
				insertArg.ToWriteS3 = true
				rs := c.newInsertMergeScope(insertArg, ss)
				rs.Magic = MergeInsert
				rs.Instructions = append(rs.Instructions, vm.Instruction{
					Op: vm.MergeBlock,
					Arg: &mergeblock.Argument{
						Tbl:              insertArg.InsertCtx.Rel,
						PartitionSources: insertArg.InsertCtx.PartitionSources,
					},
				})
				ss = []*Scope{rs}
			} else {
				dataScope := c.newMergeScope(ss)
				dataScope.IsEnd = true
				if c.anal.qry.LoadTag {
					dataScope.Proc.Reg.MergeReceivers[0].Ch = make(chan *batch.Batch, dataScope.NodeInfo.Mcpu) // reset the channel buffer of sink for load
				}
				mcpu := dataScope.NodeInfo.Mcpu
				scopes := make([]*Scope, 0, mcpu)
				regs := make([]*process.WaitRegister, 0, mcpu)
				for i := 0; i < mcpu; i++ {
					scopes = append(scopes, &Scope{
						Magic:        Merge,
						Instructions: []vm.Instruction{{Op: vm.Merge, Arg: &merge.Argument{}}},
					})
					scopes[i].Proc = process.NewFromProc(c.proc, c.ctx, 1)
					regs = append(regs, scopes[i].Proc.Reg.MergeReceivers...)
				}

				dataScope.Instructions = append(dataScope.Instructions, vm.Instruction{
					Op:  vm.Dispatch,
					Arg: constructDispatchLocal(false, false, regs),
				})
				for i := range scopes {
					insertArg, err := constructInsert(n, c.e, c.proc)
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

				insertArg, err := constructInsert(n, c.e, c.proc)
				if err != nil {
					return nil, err
				}
				insertArg.ToWriteS3 = true
				rs := c.newMergeScope(scopes)
				rs.PreScopes = append(rs.PreScopes, dataScope)
				rs.Magic = MergeInsert
				rs.Instructions = append(rs.Instructions, vm.Instruction{
					Op: vm.MergeBlock,
					Arg: &mergeblock.Argument{
						Tbl:              insertArg.InsertCtx.Rel,
						PartitionSources: insertArg.InsertCtx.PartitionSources,
					},
				})
				ss = []*Scope{rs}
			}
		} else {
			for i := range ss {
				insertArg, err := constructInsert(n, c.e, c.proc)
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
		ss, err := c.compilePlanScope(ctx, step, n.Children[0], ns)
		if err != nil {
			return nil, err
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
			lockOpArg, err := constructLockOp(n, ss[i].Proc, c.e)
			if err != nil {
				return nil, err
			}
			lockOpArg.SetBlock(block)
			if block {
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
		ss, err := c.compilePlanScope(ctx, step, n.Children[0], ns)
		if err != nil {
			return nil, err
		}
		c.setAnalyzeCurrent(ss, curr)
		return c.compileSort(n, c.compileProjection(n, c.compileRestrict(n, c.compileTableFunction(n, ss)))), nil
	case plan.Node_SINK_SCAN:
		rs := &Scope{
			Magic:        Merge,
			NodeInfo:     engine.Node{Addr: c.addr, Mcpu: ncpu},
			Proc:         process.NewWithAnalyze(c.proc, c.ctx, 1, c.anal.Nodes()),
			Instructions: []vm.Instruction{{Op: vm.Merge, Arg: &merge.Argument{}}},
		}
		receiver, ok := c.getNodeReg(curNodeIdx)
		if !ok {
			return nil, moerr.NewInternalError(c.ctx, "no data sender for sinkScan node")
		}
		receiver.Ctx = rs.Proc.Ctx
		rs.Proc.Reg.MergeReceivers[0] = receiver
		return []*Scope{rs}, nil
	case plan.Node_SINK:
		receivers := c.getStepRegs(step, ns)
		if len(receivers) == 0 {
			return nil, moerr.NewInternalError(c.ctx, "no data receiver for sink node")
		}
		ss, err := c.compilePlanScope(ctx, step, n.Children[0], ns)
		if err != nil {
			return nil, err
		}
		rs := c.newMergeScope(ss)
		rs.appendInstruction(vm.Instruction{
			Op:  vm.Dispatch,
			Arg: constructDispatchLocal(true, true, receivers),
		})

		return []*Scope{rs}, nil
	default:
		return nil, moerr.NewNYI(ctx, fmt.Sprintf("query '%s'", n))
	}
}

func (c *Compile) appendStepRegs(step int32, nodeId int32, reg *process.WaitRegister) {
	if _, ok := c.nodeRegs[nodeId]; !ok {
		c.nodeRegs[nodeId] = reg
	}
	if _, ok := c.stepRegs[step]; !ok {
		c.stepRegs[step] = []int32{nodeId}
	} else {
		c.stepRegs[step] = append(c.stepRegs[step], nodeId)
	}
}

func (c *Compile) getNodeReg(nodeId int32) (*process.WaitRegister, bool) {
	if channels, ok := c.nodeRegs[nodeId]; !ok {
		return nil, false
	} else {
		return channels, true
	}
}

func (c *Compile) getStepRegs(step int32, ns []*plan.Node) []*process.WaitRegister {
	if _, ok := c.stepRegs[step]; !ok {
		return nil
	} else {
		var wrs []*process.WaitRegister
		for _, nodeId := range c.stepRegs[step] {
			wrs = append(wrs, c.nodeRegs[nodeId])
		}
		return wrs
	}
}

func (c *Compile) constructScopeForExternal(addr string, parallel bool) *Scope {
	ds := &Scope{Magic: Normal}
	if parallel {
		ds.Magic = Remote
	}
	ds.NodeInfo = engine.Node{Addr: addr, Mcpu: ncpu}
	ds.Proc = process.NewWithAnalyze(c.proc, c.ctx, 0, c.anal.Nodes())
	c.proc.LoadTag = c.anal.qry.LoadTag
	ds.Proc.LoadTag = true
	bat := batch.NewWithSize(1)
	{
		bat.Vecs[0] = vector.NewConstNull(types.T_int64.ToType(), 1, c.proc.Mp())
		bat.InitZsOne(1)
	}
	ds.DataSource = &Source{Bat: bat}
	return ds
}

func (c *Compile) constructLoadMergeScope() *Scope {
	ds := &Scope{Magic: Merge}
	ds.Proc = process.NewWithAnalyze(c.proc, c.ctx, 1, c.anal.Nodes())
	ds.Proc.LoadTag = true
	ds.appendInstruction(vm.Instruction{
		Op:      vm.Merge,
		Idx:     c.anal.curr,
		IsFirst: c.anal.isFirst,
		Arg:     &merge.Argument{},
	})
	return ds
}

func (c *Compile) compileExternScan(ctx context.Context, n *plan.Node) ([]*Scope, error) {
	ctx, span := trace.Start(ctx, "compileExternScan")
	defer span.End()

	// lock table
	if n.ObjRef != nil && c.proc.TxnOperator.Txn().IsPessimistic() {
		db, err := c.e.Database(ctx, n.ObjRef.SchemaName, c.proc.TxnOperator)
		if err != nil {
			panic(err)
		}
		rel, err := db.Relation(ctx, n.ObjRef.ObjName, c.proc)
		if err != nil {
			return nil, err
		}
		err = lockTable(c.e, c.proc, rel)
		if err != nil && !moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetry) {
			return nil, err
		}
	}

	ID2Addr := make(map[int]int, 0)
	mcpu := 0
	for i := 0; i < len(c.cnList); i++ {
		tmp := mcpu
		mcpu += c.cnList[i].Mcpu
		ID2Addr[i] = mcpu - tmp
	}
	param := &tree.ExternParam{}
	err := json.Unmarshal([]byte(n.TableDef.Createsql), param)
	if err != nil {
		return nil, err
	}
	if param.ScanType == tree.S3 {
		if err := plan2.InitS3Param(param); err != nil {
			return nil, err
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
	} else {
		if err := plan2.InitInfileParam(param); err != nil {
			return nil, err
		}
	}

	param.FileService = c.proc.FileService
	param.Ctx = c.ctx
	var fileList []string
	var fileSize []int64
	if !param.Local {
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
	}

	if len(fileList) == 0 {
		ret := &Scope{
			Magic:      Normal,
			DataSource: nil,
			Proc:       process.NewWithAnalyze(c.proc, c.ctx, 0, c.anal.Nodes()),
		}

		return []*Scope{ret}, nil
	}

	if param.Parallel && (external.GetCompressType(param, fileList[0]) != tree.NOCOMPRESS || param.Local) {
		return c.compileExternScanParallel(n, param, fileList, fileSize)
	}

	var fileOffset [][]int64
	for i := 0; i < len(fileList); i++ {
		param.Filepath = fileList[i]
		if param.Parallel {
			arr, err := external.ReadFileOffset(param, mcpu, fileSize[i])
			fileOffset = append(fileOffset, arr)
			if err != nil {
				return nil, err
			}
		}
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
				fileOffsetTmp[j].Offset = append(fileOffsetTmp[j].Offset, fileOffset[j][2*preIndex:2*preIndex+2*count]...)
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
	nodes, err := c.generateNodes(n)
	if err != nil {
		return nil, err
	}
	ss := make([]*Scope, 0, len(nodes))

	filterExpr := colexec.RewriteFilterExprList(n.FilterList)
	if filterExpr != nil {
		filterExpr, err = plan2.ConstantFold(batch.EmptyForConstFoldBatch, plan2.DeepCopyExpr(filterExpr), c.proc, true)
		if err != nil {
			return nil, err
		}
	}

	for i := range nodes {
		ss = append(ss, c.compileTableScanWithNode(n, nodes[i], filterExpr))
	}
	return ss, nil
}

func (c *Compile) compileTableScanWithNode(n *plan.Node, node engine.Node, filterExpr *plan.Expr) *Scope {
	var err error
	var s *Scope
	var tblDef *plan.TableDef
	var ts timestamp.Timestamp
	var db engine.Database
	var rel engine.Relation
	var pkey *plan.PrimaryKeyDef

	attrs := make([]string, len(n.TableDef.Cols))
	for j, col := range n.TableDef.Cols {
		attrs[j] = col.Name
	}
	if c.proc != nil && c.proc.TxnOperator != nil {
		ts = c.proc.TxnOperator.Txn().SnapshotTS
	}
	{
		var cols []*plan.ColDef
		ctx := c.ctx
		if util.TableIsClusterTable(n.TableDef.GetTableType()) {
			ctx = context.WithValue(ctx, defines.TenantIDKey{}, catalog.System_Account)
		}
		if n.ObjRef.PubInfo != nil {
			ctx = context.WithValue(ctx, defines.TenantIDKey{}, uint32(n.ObjRef.PubInfo.TenantId))
		}
		db, err = c.e.Database(ctx, n.ObjRef.SchemaName, c.proc.TxnOperator)
		if err != nil {
			panic(err)
		}
		rel, err = db.Relation(ctx, n.TableDef.Name, c.proc)
		if err != nil {
			var e error // avoid contamination of error messages
			db, e = c.e.Database(c.ctx, defines.TEMPORARY_DBNAME, c.proc.TxnOperator)
			if e != nil {
				panic(e)
			}
			rel, e = db.Relation(c.ctx, engine.GetTempTableName(n.ObjRef.SchemaName, n.TableDef.Name), c.proc)
			if e != nil {
				panic(e)
			}
		}
		// defs has no rowid
		defs, err := rel.TableDefs(ctx)
		if err != nil {
			panic(err)
		}
		i := int32(0)
		name2index := make(map[string]int32)
		for _, def := range defs {
			if attr, ok := def.(*engine.AttributeDef); ok {
				name2index[attr.Attr.Name] = i
				cols = append(cols, &plan.ColDef{
					ColId: attr.Attr.ID,
					Name:  attr.Attr.Name,
					Typ: &plan.Type{
						Id:       int32(attr.Attr.Type.Oid),
						Width:    attr.Attr.Type.Width,
						Scale:    attr.Attr.Type.Scale,
						AutoIncr: attr.Attr.AutoIncrement,
					},
					Primary:   attr.Attr.Primary,
					Default:   attr.Attr.Default,
					OnUpdate:  attr.Attr.OnUpdate,
					Comment:   attr.Attr.Comment,
					ClusterBy: attr.Attr.ClusterBy,
					Seqnum:    uint32(attr.Attr.Seqnum),
				})
				i++
			} else if c, ok := def.(*engine.ConstraintDef); ok {
				for _, ct := range c.Cts {
					switch k := ct.(type) {
					case *engine.PrimaryKeyDef:
						pkey = k.Pkey
					}
				}
			}
		}
		tblDef = &plan.TableDef{
			Cols:          cols,
			Name2ColIndex: name2index,
			Version:       n.TableDef.Version,
			Name:          n.TableDef.Name,
			TableType:     n.TableDef.GetTableType(),
			Pkey:          pkey,
		}
	}

	// prcoess partitioned table
	var partitionRelNames []string
	if n.TableDef.Partition != nil {
		partitionRelNames = append(partitionRelNames, n.TableDef.Partition.PartitionTableNames...)
	}

	s = &Scope{
		Magic:    Remote,
		NodeInfo: node,
		DataSource: &Source{
			Timestamp:              ts,
			Attributes:             attrs,
			TableDef:               tblDef,
			RelationName:           n.TableDef.Name,
			PartitionRelationNames: partitionRelNames,
			SchemaName:             n.ObjRef.SchemaName,
			AccountId:              n.ObjRef.GetPubInfo(),
			Expr:                   plan2.DeepCopyExpr(filterExpr),
			RuntimeFilterSpecs:     n.RuntimeFilterProbeList,
		},
	}
	s.Proc = process.NewWithAnalyze(c.proc, c.ctx, 0, c.anal.Nodes())

	return s
}

func (c *Compile) compileRestrict(n *plan.Node, ss []*Scope) []*Scope {
	if len(n.FilterList) == 0 {
		return ss
	}
	currentFirstFlag := c.anal.isFirst
	filterExpr := colexec.RewriteFilterExprList(n.FilterList)
	for i := range ss {
		ss[i].appendInstruction(vm.Instruction{
			Op:      vm.Restrict,
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
	rs := c.newScopeList(1, int(n.Stats.BlockNum))
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
			Arg: constructGroup(c.ctx, gn, n, i, len(rs), true, c.proc),
		})
		if isSameCN(rs[i].NodeInfo.Addr, c.addr) {
			idx = i
		}
	}
	mergeChildren := c.newMergeScope(ss)
	mergeChildren.appendInstruction(vm.Instruction{
		Op:  vm.Dispatch,
		Arg: constructBroadcastDispatch(0, rs, c.addr, n),
	})
	rs[idx].PreScopes = append(rs[idx].PreScopes, mergeChildren)
	return rs
}

func (c *Compile) compileMinusAndIntersect(n *plan.Node, ss []*Scope, children []*Scope, nodeType plan.Node_NodeType) []*Scope {
	rs := c.newJoinScopeListWithBucket(c.newScopeList(2, int(n.Stats.BlockNum)), ss, children, n)
	switch nodeType {
	case plan.Node_MINUS:
		for i := range rs {
			rs[i].Instructions[0] = vm.Instruction{
				Op:  vm.Minus,
				Idx: c.anal.curr,
				Arg: constructMinus(i, len(rs)),
			}
		}
	case plan.Node_INTERSECT:
		for i := range rs {
			rs[i].Instructions[0] = vm.Instruction{
				Op:  vm.Intersect,
				Idx: c.anal.curr,
				Arg: constructIntersect(i, len(rs)),
			}
		}
	case plan.Node_INTERSECT_ALL:
		for i := range rs {
			rs[i].Instructions[0] = vm.Instruction{
				Op:  vm.IntersectAll,
				Idx: c.anal.curr,
				Arg: constructIntersectAll(i, len(rs)),
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

func (c *Compile) compileJoin(ctx context.Context, node, left, right *plan.Node, ss []*Scope, children []*Scope) []*Scope {
	var rs []*Scope
	isEq := plan2.IsEquiJoin2(node.OnList)

	rightTyps := make([]types.Type, len(right.ProjectList))
	for i, expr := range right.ProjectList {
		rightTyps[i] = dupType(expr.Typ)
	}

	leftTyps := make([]types.Type, len(left.ProjectList))
	for i, expr := range left.ProjectList {
		leftTyps[i] = dupType(expr.Typ)
	}

	switch node.JoinType {
	case plan.Node_INNER:
		rs = c.newBroadcastJoinScopeList(ss, children, node)
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
	case plan.Node_SEMI:
		if isEq {
			if node.BuildOnLeft {
				rs = c.newJoinScopeListWithBucket(c.newScopeListForRightJoin(2, ss), ss, children, node)
				for i := range rs {
					rs[i].appendInstruction(vm.Instruction{
						Op:  vm.RightSemi,
						Idx: c.anal.curr,
						Arg: constructRightSemi(node, rightTyps, uint64(i), uint64(len(rs)), c.proc),
					})
				}
			} else {
				rs = c.newBroadcastJoinScopeList(ss, children, node)
				for i := range rs {
					rs[i].appendInstruction(vm.Instruction{
						Op:  vm.Semi,
						Idx: c.anal.curr,
						Arg: constructSemi(node, rightTyps, c.proc),
					})
				}
			}
		} else {
			rs = c.newBroadcastJoinScopeList(ss, children, node)
			for i := range rs {
				rs[i].appendInstruction(vm.Instruction{
					Op:  vm.LoopSemi,
					Idx: c.anal.curr,
					Arg: constructLoopSemi(node, rightTyps, c.proc),
				})
			}
		}
	case plan.Node_LEFT:
		rs = c.newBroadcastJoinScopeList(ss, children, node)
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
			rs = c.newJoinScopeListWithBucket(c.newScopeListForRightJoin(2, ss), ss, children, node)
			for i := range rs {
				rs[i].appendInstruction(vm.Instruction{
					Op:  vm.Right,
					Idx: c.anal.curr,
					Arg: constructRight(node, leftTyps, rightTyps, uint64(i), uint64(len(rs)), c.proc),
				})
			}
		} else {
			panic("dont pass any no-equal right join plan to this function,it should be changed to left join by the planner")
		}
	case plan.Node_SINGLE:
		rs = c.newBroadcastJoinScopeList(ss, children, node)
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
				rs = c.newJoinScopeListWithBucket(c.newScopeListForRightJoin(2, ss), ss, children, node)
				for i := range rs {
					rs[i].appendInstruction(vm.Instruction{
						Op:  vm.RightAnti,
						Idx: c.anal.curr,
						Arg: constructRightAnti(node, rightTyps, uint64(i), uint64(len(rs)), c.proc),
					})
				}
			} else {
				rs = c.newBroadcastJoinScopeList(ss, children, node)
				for i := range rs {
					rs[i].appendInstruction(vm.Instruction{
						Op:  vm.Anti,
						Idx: c.anal.curr,
						Arg: constructAnti(node, rightTyps, c.proc),
					})
				}
			}
		} else {
			rs = c.newBroadcastJoinScopeList(ss, children, node)
			for i := range rs {
				rs[i].appendInstruction(vm.Instruction{
					Op:  vm.LoopAnti,
					Idx: c.anal.curr,
					Arg: constructLoopAnti(node, rightTyps, c.proc),
				})
			}
		}
	case plan.Node_MARK:
		rs = c.newBroadcastJoinScopeList(ss, children, node)
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

func (c *Compile) compileSort(n *plan.Node, ss []*Scope) []*Scope {
	switch {
	case n.Limit != nil && n.Offset == nil && len(n.OrderBy) > 0: // top
		vec, err := colexec.EvalExpressionOnce(c.proc, n.Limit, []*batch.Batch{constBat})
		if err != nil {
			panic(err)
		}
		defer vec.Free(c.proc.Mp())
		return c.compileTop(n, vector.MustFixedCol[int64](vec)[0], ss)

	case n.Limit == nil && n.Offset == nil && len(n.OrderBy) > 0: // top
		return c.compileOrder(n, ss)

	case n.Limit != nil && n.Offset != nil && len(n.OrderBy) > 0:
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

		limit, offset := vector.MustFixedCol[int64](vec1)[0], vector.MustFixedCol[int64](vec2)[0]
		topN := limit + offset
		if topN <= 8192*2 {
			// if n is small, convert `order by col limit m offset n` to `top m+n offset n`
			return c.compileOffset(n, c.compileTop(n, topN, ss))
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

func (c *Compile) compileTop(n *plan.Node, topN int64, ss []*Scope) []*Scope {
	// use topN TO make scope.
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
	rs.Instructions[0] = vm.Instruction{
		Op:  vm.MergeTop,
		Idx: c.anal.curr,
		Arg: constructMergeTop(n, topN),
	}
	return []*Scope{rs}
}

func (c *Compile) compileOrder(n *plan.Node, ss []*Scope) []*Scope {
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
	rs.Instructions[0] = vm.Instruction{
		Op:  vm.MergeOrder,
		Idx: c.anal.curr,
		Arg: constructMergeOrder(n),
	}
	return []*Scope{rs}
}

func (c *Compile) compileWin(n *plan.Node, ss []*Scope) []*Scope {
	rs := c.newMergeScope(ss)
	rs.Instructions[0] = vm.Instruction{
		Op:  vm.Window,
		Idx: c.anal.curr,
		Arg: constructWindow(c.ctx, n, c.proc),
	}
	return []*Scope{rs}
}

func (c *Compile) compileOffset(n *plan.Node, ss []*Scope) []*Scope {
	currentFirstFlag := c.anal.isFirst
	for i := range ss {
		if containBrokenNode(ss[i]) {
			c.anal.isFirst = currentFirstFlag
			ss[i] = c.newMergeScope([]*Scope{ss[i]})
		}
	}

	rs := c.newMergeScope(ss)
	rs.Instructions[0] = vm.Instruction{
		Op:  vm.MergeOffset,
		Idx: c.anal.curr,
		Arg: constructMergeOffset(n, c.proc),
	}
	return []*Scope{rs}
}

func (c *Compile) compileLimit(n *plan.Node, ss []*Scope) []*Scope {
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
			Arg:     constructLimit(n, c.proc),
		})
	}
	c.anal.isFirst = false

	rs := c.newMergeScope(ss)
	rs.Instructions[0] = vm.Instruction{
		Op:  vm.MergeLimit,
		Idx: c.anal.curr,
		Arg: constructMergeLimit(n, c.proc),
	}
	return []*Scope{rs}
}

func (c *Compile) compileMergeGroup(n *plan.Node, ss []*Scope, ns []*plan.Node) []*Scope {
	currentFirstFlag := c.anal.isFirst
	for i := range ss {
		c.anal.isFirst = currentFirstFlag
		if containBrokenNode(ss[i]) {
			ss[i] = c.newMergeScope([]*Scope{ss[i]})
		}
		ss[i].appendInstruction(vm.Instruction{
			Op:      vm.Group,
			Idx:     c.anal.curr,
			IsFirst: c.anal.isFirst,
			Arg:     constructGroup(c.ctx, n, ns[n.Children[0]], 0, 0, false, c.proc),
		})
	}
	c.anal.isFirst = false

	rs := c.newMergeScope(ss)
	rs.Instructions[0] = vm.Instruction{
		Op:  vm.MergeGroup,
		Idx: c.anal.curr,
		Arg: constructMergeGroup(true),
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
				Arg: constructShuffleArg(children, n),
			})

			ss[i].appendInstruction(vm.Instruction{
				Op:  vm.Dispatch,
				Arg: constructBroadcastDispatch(j, children, ss[i].NodeInfo.Addr, n),
			})
			j++
			ss[i].IsEnd = true
		}
	}
}

func (c *Compile) compileShuffleGroup(n *plan.Node, ss []*Scope, ns []*plan.Node) []*Scope {
	currentIsFirst := c.anal.isFirst
	c.anal.isFirst = false
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
			Arg:     constructGroup(c.ctx, n, ns[n.Children[0]], 0, 0, true, c.proc),
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
			if children[j].NodeInfo.Addr == ss[i].NodeInfo.Addr {
				children[j].PreScopes = append(children[j].PreScopes, ss[i])
				appended = true
				break
			}
		}
		if !appended {
			children[0].PreScopes = append(children[0].PreScopes, ss[i])
		}
	}

	return []*Scope{c.newMergeScope(parent)}
}

//func (c *Compile) newInsertMergeScope(arg *insert.Argument, ss []*Scope) *Scope {
//	ss2 := make([]*Scope, 0, len(ss))
//	for _, s := range ss {
//		if s.IsEnd {
//			continue
//		}
//		ss2 = append(ss2, s)
//	}
//	insert := &vm.Instruction{
//		Op:  vm.Insert,
//		Arg: arg,
//	}
//	for i := range ss2 {
//		ss2[i].Instructions = append(ss2[i].Instructions, dupInstruction(insert, nil, i))
//	}
//	return c.newMergeScope(ss2)
//}

// DeleteMergeScope need to assure this:
// one block can be only deleted by one and the same
// CN, so we need to transfer the rows from the
// the same block to one and the same CN to perform
// the deletion operators.
func (c *Compile) newDeleteMergeScope(arg *deletion.Argument, ss []*Scope) *Scope {
	//Todo: implemet delete merge
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
	for i := 0; i < len(ss2); i++ {
		rs = append(rs, new(Scope))
		uuids = append(uuids, uuid.New())
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
		arg.SegmentMap = colexec.Srv.GetCnSegmentMap()
		arg.IBucket = uint32(i)
		arg.Nbucket = uint32(len(rs))
		rs[i].Instructions = append(
			rs[i].Instructions,
			dupInstruction(delete, nil, 0))
	}
	return c.newMergeScope(rs)
}

func (c *Compile) newMergeScope(ss []*Scope) *Scope {
	rs := &Scope{
		PreScopes: ss,
		Magic:     Merge,
		NodeInfo: engine.Node{
			Addr: c.addr,
			Mcpu: ncpu,
		},
	}
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
		Arg:     &merge.Argument{},
	})
	c.anal.isFirst = false

	j := 0
	for i := range ss {
		if !ss[i].IsEnd {
			ss[i].appendInstruction(vm.Instruction{
				Op: vm.Connector,
				Arg: &connector.Argument{
					Reg: rs.Proc.Reg.MergeReceivers[j],
				},
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

func (c *Compile) newScopeList(childrenCount int, blocks int) []*Scope {
	var ss []*Scope

	currentFirstFlag := c.anal.isFirst
	for _, n := range c.cnList {
		c.anal.isFirst = currentFirstFlag
		ss = append(ss, c.newScopeListWithNode(c.generateCPUNumber(n.Mcpu, blocks), childrenCount, n.Addr)...)
	}
	return ss
}

func (c *Compile) newScopeListForShuffleGroup(childrenCount int, blocks int) ([]*Scope, []*Scope) {
	var parent = make([]*Scope, 0, len(c.cnList))
	var children = make([]*Scope, 0, len(c.cnList))

	currentFirstFlag := c.anal.isFirst
	for _, n := range c.cnList {
		c.anal.isFirst = currentFirstFlag
		scopes := c.newScopeListWithNode(c.generateCPUNumber(n.Mcpu, blocks), childrenCount, n.Addr)
		for _, s := range scopes {
			for _, rr := range s.Proc.Reg.MergeReceivers {
				rr.Ch = make(chan *batch.Batch, 16)
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
		ss[i] = new(Scope)
		ss[i].Magic = Remote
		ss[i].NodeInfo.Addr = addr
		ss[i].NodeInfo.Mcpu = 1 // ss is already the mcpu length so we don't need to parallel it
		ss[i].Proc = process.NewWithAnalyze(c.proc, c.ctx, childrenCount, c.anal.Nodes())
		ss[i].Instructions = append(ss[i].Instructions, vm.Instruction{
			Op:      vm.Merge,
			Idx:     c.anal.curr,
			IsFirst: currentFirstFlag,
			Arg:     &merge.Argument{},
		})
	}
	c.anal.isFirst = false
	return ss
}

func (c *Compile) newScopeListForRightJoin(childrenCount int, leftScopes []*Scope) []*Scope {
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
	ss[0] = &Scope{
		Magic:    Remote,
		IsJoin:   true,
		Proc:     process.NewWithAnalyze(c.proc, c.ctx, childrenCount, c.anal.Nodes()),
		NodeInfo: engine.Node{Addr: c.addr, Mcpu: c.generateCPUNumber(ncpu, maxCpuNum)},
	}
	return ss
}

func (c *Compile) newJoinScopeListWithBucket(rs, ss, children []*Scope, n *plan.Node) []*Scope {
	currentFirstFlag := c.anal.isFirst
	// construct left
	leftMerge := c.newMergeScope(ss)
	leftMerge.appendInstruction(vm.Instruction{
		Op:  vm.Dispatch,
		Arg: constructBroadcastDispatch(0, rs, c.addr, n),
	})
	leftMerge.IsEnd = true

	// construct right
	c.anal.isFirst = currentFirstFlag
	rightMerge := c.newMergeScope(children)
	rightMerge.appendInstruction(vm.Instruction{
		Op:  vm.Dispatch,
		Arg: constructBroadcastDispatch(1, rs, c.addr, n),
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

//func (c *Compile) newJoinScopeList(ss []*Scope, children []*Scope) []*Scope {
//rs := make([]*Scope, len(ss))
//// join's input will record in the left/right scope when JoinRun
//// so set it to false here.
//c.anal.isFirst = false
//for i := range ss {
//if ss[i].IsEnd {
//rs[i] = ss[i]
//continue
//}
//chp := c.newMergeScope(dupScopeList(children))
//rs[i] = new(Scope)
//rs[i].Magic = Remote
//rs[i].IsJoin = true
//rs[i].NodeInfo = ss[i].NodeInfo
//rs[i].PreScopes = []*Scope{ss[i], chp}
//rs[i].Proc = process.NewWithAnalyze(c.proc, c.ctx, 2, c.anal.Nodes())
//ss[i].appendInstruction(vm.Instruction{
//Op: vm.Connector,
//Arg: &connector.Argument{
//Reg: rs[i].Proc.Reg.MergeReceivers[0],
//},
//})
//chp.appendInstruction(vm.Instruction{
//Op: vm.Connector,
//Arg: &connector.Argument{
//Reg: rs[i].Proc.Reg.MergeReceivers[1],
//},
//})
//chp.IsEnd = true
//}
//return rs
//}

func (c *Compile) newBroadcastJoinScopeList(ss []*Scope, children []*Scope, n *plan.Node) []*Scope {
	length := len(ss)
	rs := make([]*Scope, length)
	idx := 0
	for i := range ss {
		if ss[i].IsEnd {
			rs[i] = ss[i]
			continue
		}
		rs[i] = new(Scope)
		rs[i].Magic = Remote
		rs[i].IsJoin = true
		rs[i].NodeInfo = ss[i].NodeInfo
		if isSameCN(rs[i].NodeInfo.Addr, c.addr) {
			idx = i
		}
		rs[i].PreScopes = []*Scope{ss[i]}
		rs[i].Proc = process.NewWithAnalyze(c.proc, c.ctx, 2, c.anal.Nodes())
		ss[i].appendInstruction(vm.Instruction{
			Op: vm.Connector,
			Arg: &connector.Argument{
				Reg: rs[i].Proc.Reg.MergeReceivers[0],
			},
		})
	}

	// all join's first flag will setting in newLeftScope and newRightScope
	// so we set it to false now
	c.anal.isFirst = false
	mergeChildren := c.newMergeScope(children)

	// a hack here to stop shuffle join, delete this in the future
	n.Stats.Shuffle = false

	mergeChildren.appendInstruction(vm.Instruction{
		Op:  vm.Dispatch,
		Arg: constructBroadcastDispatch(1, rs, c.addr, n),
	})
	mergeChildren.IsEnd = true
	rs[idx].PreScopes = append(rs[idx].PreScopes, mergeChildren)

	return rs
}

func (c *Compile) newJoinProbeScope(s *Scope, ss []*Scope) *Scope {
	rs := &Scope{
		Magic: Merge,
	}
	rs.appendInstruction(vm.Instruction{
		Op:      vm.Merge,
		Idx:     s.Instructions[0].Idx,
		IsFirst: true,
		Arg:     &merge.Argument{},
	})
	rs.appendInstruction(vm.Instruction{
		Op:  vm.Dispatch,
		Arg: constructDispatchLocal(false, false, extraRegisters(ss, 0)),
	})
	rs.IsEnd = true
	rs.Proc = process.NewWithAnalyze(s.Proc, s.Proc.Ctx, 1, c.anal.Nodes())
	regTransplant(s, rs, 0, 0)
	return rs
}

func (c *Compile) newJoinBuildScope(s *Scope, ss []*Scope) *Scope {
	rs := &Scope{
		Magic: Merge,
	}
	rs.Proc = process.NewWithAnalyze(s.Proc, s.Proc.Ctx, 1, c.anal.Nodes())
	regTransplant(s, rs, 1, 0)

	rs.appendInstruction(vm.Instruction{
		Op:      vm.HashBuild,
		Idx:     s.Instructions[0].Idx,
		IsFirst: true,
		Arg:     constructHashBuild(c, s.Instructions[0], c.proc, ss != nil),
	})

	if ss == nil { // unparallel, send the hashtable to s directly
		s.Proc.Reg.MergeReceivers[1] = &process.WaitRegister{
			Ctx: s.Proc.Ctx,
			Ch:  make(chan *batch.Batch, 1),
		}
		rs.appendInstruction(vm.Instruction{
			Op: vm.Connector,
			Arg: &connector.Argument{
				Reg: s.Proc.Reg.MergeReceivers[1],
			},
		})
	} else {
		rs.appendInstruction(vm.Instruction{
			Op:  vm.Dispatch,
			Arg: constructDispatchLocal(true, false, extraRegisters(ss, 1)),
		})
	}
	rs.IsEnd = true

	return rs
}

// Transplant the source's RemoteReceivRegInfos which index equal to sourceIdx to
// target with new index targetIdx
func regTransplant(source, target *Scope, sourceIdx, targetIdx int) {
	target.Proc.Reg.MergeReceivers[targetIdx] = source.Proc.Reg.MergeReceivers[sourceIdx]
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
	if cpunum <= 0 || blocks <= 0 {
		return 1
	}

	if cpunum <= blocks {
		return cpunum
	}
	return blocks
}

func (c *Compile) initAnalyze(qry *plan.Query) {
	anals := make([]*process.AnalyzeInfo, len(qry.Nodes))
	for i := range anals {
		//anals[i] = new(process.AnalyzeInfo)
		anals[i] = analPool.Get().(*process.AnalyzeInfo)
		anals[i].Reset()
	}
	c.anal = &anaylze{
		qry:       qry,
		analInfos: anals,
		curr:      int(qry.Steps[0]),
	}
	for _, node := range c.anal.qry.Nodes {
		if node.AnalyzeInfo == nil {
			node.AnalyzeInfo = new(plan.AnalyzeInfo)
		}
	}
	c.proc.AnalInfos = c.anal.analInfos
}

func (c *Compile) fillAnalyzeInfo() {
	// record the number of s3 requests
	c.anal.S3IOInputCount(c.anal.curr, c.s3CounterSet.FileService.S3.Put.Load())
	c.anal.S3IOInputCount(c.anal.curr, c.s3CounterSet.FileService.S3.List.Load())

	c.anal.S3IOOutputCount(c.anal.curr, c.s3CounterSet.FileService.S3.Head.Load())
	c.anal.S3IOOutputCount(c.anal.curr, c.s3CounterSet.FileService.S3.Get.Load())
	c.anal.S3IOOutputCount(c.anal.curr, c.s3CounterSet.FileService.S3.Delete.Load())
	c.anal.S3IOOutputCount(c.anal.curr, c.s3CounterSet.FileService.S3.DeleteMulti.Load())

	for i, anal := range c.anal.analInfos {
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
	}
}

func (c *Compile) generateNodes(n *plan.Node) (engine.Nodes, error) {
	var err error
	var db engine.Database
	var rel engine.Relation
	var ranges [][]byte
	var nodes engine.Nodes
	isPartitionTable := false

	ctx := c.ctx
	if util.TableIsClusterTable(n.TableDef.GetTableType()) {
		ctx = context.WithValue(ctx, defines.TenantIDKey{}, catalog.System_Account)
	}
	if n.ObjRef.PubInfo != nil {
		ctx = context.WithValue(ctx, defines.TenantIDKey{}, uint32(n.ObjRef.PubInfo.GetTenantId()))
	}
	if util.TableIsLoggingTable(n.ObjRef.SchemaName, n.ObjRef.ObjName) {
		ctx = context.WithValue(ctx, defines.TenantIDKey{}, catalog.System_Account)
	}
	db, err = c.e.Database(ctx, n.ObjRef.SchemaName, c.proc.TxnOperator)
	if err != nil {
		return nil, err
	}
	rel, err = db.Relation(ctx, n.TableDef.Name, c.proc)
	if err != nil {
		var e error // avoid contamination of error messages
		db, e = c.e.Database(ctx, defines.TEMPORARY_DBNAME, c.proc.TxnOperator)
		if e != nil {
			return nil, err
		}

		// if temporary table, just scan at local cn.
		rel, e = db.Relation(ctx, engine.GetTempTableName(n.ObjRef.SchemaName, n.TableDef.Name), c.proc)
		if e != nil {
			return nil, err
		}
		c.cnList = engine.Nodes{
			engine.Node{
				Addr: c.addr,
				Rel:  rel,
				Mcpu: 1,
			},
		}
	}

	ranges, err = rel.Ranges(ctx, n.BlockFilterList)
	if err != nil {
		return nil, err
	}

	if n.TableDef.Partition != nil {
		isPartitionTable = true
		partitionInfo := n.TableDef.Partition
		partitionNum := int(partitionInfo.PartitionNum)
		partitionTableNames := partitionInfo.PartitionTableNames
		for i := 0; i < partitionNum; i++ {
			partTableName := partitionTableNames[i]
			subrelation, err := db.Relation(ctx, partTableName, c.proc)
			if err != nil {
				return nil, err
			}
			subranges, err := subrelation.Ranges(ctx, n.BlockFilterList)
			if err != nil {
				return nil, err
			}
			//add partition number into catalog.BlockInfo.
			for _, r := range subranges[1:] {
				blkInfo := catalog.DecodeBlockInfo(r)
				blkInfo.PartitionNum = i
				ranges = append(ranges, r)
			}
			//ranges = append(ranges, subranges[1:]...)
		}
	}

	// some log for finding a bug.
	tblId := rel.GetTableID(ctx)
	expectedLen := len(ranges)
	logutil.Debugf("cn generateNodes, tbl %d ranges is %d", tblId, expectedLen)

	//if len(ranges) == 0 indicates that it's a temporary table.
	if len(ranges) == 0 && n.TableDef.TableType != catalog.SystemOrdinaryRel {
		nodes = make(engine.Nodes, len(c.cnList))
		for i, node := range c.cnList {
			if isPartitionTable {
				nodes[i] = engine.Node{
					Id:   node.Id,
					Addr: node.Addr,
					Mcpu: c.generateCPUNumber(node.Mcpu, int(n.Stats.BlockNum)),
				}
			} else {
				nodes[i] = engine.Node{
					Rel:  rel,
					Id:   node.Id,
					Addr: node.Addr,
					Mcpu: c.generateCPUNumber(node.Mcpu, int(n.Stats.BlockNum)),
				}
			}
		}
		return nodes, nil
	}

	engineType := rel.GetEngineType()
	if isPartitionTable {
		rel = nil
	}
	// for multi cn in launch mode, put all payloads in current CN
	// maybe delete this in the future
	if isLaunchMode(c.cnList) {
		return putBlocksInCurrentCN(c, ranges, rel, n), nil
	}
	// disttae engine
	if engineType == engine.Disttae {
		return shuffleBlocksToMultiCN(c, ranges, rel, n)
	}
	// maybe temp table on memengine , just put payloads in average
	return putBlocksInAverage(c, ranges, rel, n), nil
}

func putBlocksInAverage(c *Compile, ranges [][]byte, rel engine.Relation, n *plan.Node) engine.Nodes {
	var nodes engine.Nodes
	step := (len(ranges) + len(c.cnList) - 1) / len(c.cnList)
	for i := 0; i < len(ranges); i += step {
		j := i / step
		if i+step >= len(ranges) {
			if isSameCN(c.cnList[j].Addr, c.addr) {
				if len(nodes) == 0 {
					nodes = append(nodes, engine.Node{
						Addr: c.addr,
						Rel:  rel,
						Mcpu: c.generateCPUNumber(ncpu, int(n.Stats.BlockNum)),
					})
				}
				nodes[0].Data = append(nodes[0].Data, ranges[i:]...)
			} else {
				nodes = append(nodes, engine.Node{
					Rel:  rel,
					Id:   c.cnList[j].Id,
					Addr: c.cnList[j].Addr,
					Mcpu: c.generateCPUNumber(c.cnList[j].Mcpu, int(n.Stats.BlockNum)),
					Data: ranges[i:],
				})
			}
		} else {
			if isSameCN(c.cnList[j].Addr, c.addr) {
				if len(nodes) == 0 {
					nodes = append(nodes, engine.Node{
						Rel:  rel,
						Addr: c.addr,
						Mcpu: c.generateCPUNumber(ncpu, int(n.Stats.BlockNum)),
					})
				}
				nodes[0].Data = append(nodes[0].Data, ranges[i:i+step]...)
			} else {
				nodes = append(nodes, engine.Node{
					Rel:  rel,
					Id:   c.cnList[j].Id,
					Addr: c.cnList[j].Addr,
					Mcpu: c.generateCPUNumber(c.cnList[j].Mcpu, int(n.Stats.BlockNum)),
					Data: ranges[i : i+step],
				})
			}
		}
	}
	return nodes
}

func shuffleBlocksToMultiCN(c *Compile, ranges [][]byte, rel engine.Relation, n *plan.Node) (engine.Nodes, error) {
	var nodes engine.Nodes
	//add current CN
	nodes = append(nodes, engine.Node{
		Addr: c.addr,
		Rel:  rel,
		Mcpu: c.generateCPUNumber(ncpu, int(n.Stats.BlockNum)),
	})
	//add memory table block
	nodes[0].Data = append(nodes[0].Data, ranges[:1]...)
	ranges = ranges[1:]
	// only memory table block
	if len(ranges) == 0 {
		return nodes, nil
	}
	//only one cn
	if len(c.cnList) == 1 {
		nodes[0].Data = append(nodes[0].Data, ranges...)
		return nodes, nil
	}
	// put dirty blocks which can't be distributed remotely in current CN.
	newRanges := make([][]byte, 0, len(ranges))
	for _, blk := range ranges {
		blkInfo := catalog.DecodeBlockInfo(blk)
		if blkInfo.CanRemote {
			newRanges = append(newRanges, blk)
			continue
		}
		nodes[0].Data = append(nodes[0].Data, blk)
	}

	//add the rest of CNs in list
	for i := range c.cnList {
		if c.cnList[i].Addr != c.addr {
			nodes = append(nodes, engine.Node{
				Rel:  rel,
				Id:   c.cnList[i].Id,
				Addr: c.cnList[i].Addr,
				Mcpu: c.generateCPUNumber(c.cnList[i].Mcpu, int(n.Stats.BlockNum)),
			})
		}
	}

	sort.Slice(nodes, func(i, j int) bool { return nodes[i].Addr < nodes[j].Addr })

	if n.Stats.Shuffle && n.Stats.ShuffleType == plan.ShuffleType_Range {
		err := shuffleBlocksByRange(c, newRanges, n, nodes)
		if err != nil {
			return nil, err
		}
	} else {
		shuffleBlocksByHash(c, newRanges, nodes)
	}

	minWorkLoad := math.MaxInt32
	maxWorkLoad := 0
	//remove empty node from nodes
	var newNodes engine.Nodes
	for i := range nodes {
		if len(nodes[i].Data) > maxWorkLoad {
			maxWorkLoad = len(nodes[i].Data)
		}
		if len(nodes[i].Data) < minWorkLoad {
			minWorkLoad = len(nodes[i].Data)
		}
		if len(nodes[i].Data) > 0 {
			newNodes = append(newNodes, nodes[i])
		}
	}
	if minWorkLoad*2 < maxWorkLoad {
		logutil.Warnf("workload among CNs not balanced, max %v, min %v", maxWorkLoad, minWorkLoad)
	}
	return newNodes, nil
}

func shuffleBlocksByHash(c *Compile, ranges [][]byte, nodes engine.Nodes) {
	for i, blk := range ranges {
		unmarshalledBlockInfo := catalog.DecodeBlockInfo(ranges[i])
		// get timestamp in objName to make sure it is random enough
		objTimeStamp := unmarshalledBlockInfo.MetaLocation().Name()[:7]
		index := plan2.SimpleCharHashToRange(objTimeStamp, uint64(len(c.cnList)))
		nodes[index].Data = append(nodes[index].Data, blk)
	}
}

func shuffleBlocksByRange(c *Compile, ranges [][]byte, n *plan.Node, nodes engine.Nodes) error {
	var objMeta objectio.ObjectMeta

	for i, blk := range ranges {
		unmarshalledBlockInfo := catalog.DecodeBlockInfo(ranges[i])
		location := unmarshalledBlockInfo.MetaLocation()
		fs, err := fileservice.Get[fileservice.FileService](c.proc.FileService, defines.SharedFileServiceName)
		if err != nil {
			return err
		}
		if !objectio.IsSameObjectLocVsMeta(location, objMeta) {
			if objMeta, err = objectio.FastLoadObjectMeta(c.ctx, &location, fs); err != nil {
				return err
			}
		}
		blkMeta := objMeta.GetBlockMeta(uint32(location.ID()))
		zm := blkMeta.MustGetColumn(uint16(n.Stats.ShuffleColIdx)).ZoneMap()
		index := plan2.GetRangeShuffleIndexForZM(n.Stats.ShuffleColMin, n.Stats.ShuffleColMax, zm, uint64(len(c.cnList)))
		nodes[index].Data = append(nodes[index].Data, blk)
	}
	return nil
}

func putBlocksInCurrentCN(c *Compile, ranges [][]byte, rel engine.Relation, n *plan.Node) engine.Nodes {
	var nodes engine.Nodes
	//add current CN
	nodes = append(nodes, engine.Node{
		Addr: c.addr,
		Rel:  rel,
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
		WithTxn(c.proc.TxnOperator).
		WithDatabase(c.db)
	return exec.Exec(c.proc.Ctx, sql, opts)
}

func evalRowsetData(ctx context.Context, proc *process.Process,
	exprs []*plan.RowsetExpr, vec *vector.Vector, exprExecs []colexec.ExpressionExecutor) error {
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
	ss2 := make([]*Scope, 0, len(ss))
	for _, s := range ss {
		if s.IsEnd {
			continue
		}
		ss2 = append(ss2, s)
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
