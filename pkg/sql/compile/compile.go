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
	"runtime"
	"strings"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/connector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/external"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/insert"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/merge"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergeblock"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/output"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
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

// New is used to new an object of compile
func New(addr, db string, sql string, uid string, ctx context.Context,
	e engine.Engine, proc *process.Process, stmt tree.Statement) *Compile {
	return &Compile{
		e:    e,
		db:   db,
		ctx:  ctx,
		uid:  uid,
		sql:  sql,
		proc: proc,
		stmt: stmt,
		addr: addr,
	}
}

// helper function to judge if init temporary engine is needed
func (c *Compile) NeedInitTempEngine(InitTempEngine bool) bool {
	if InitTempEngine {
		return false
	}
	ddl := c.scope.Plan.GetDdl()
	if ddl != nil {
		qry := ddl.GetCreateTable()
		if qry != nil && qry.Temporary {
			e := c.e.(*engine.EntireEngine).TempEngine
			if e == nil {
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

// Compile is the entrance of the compute-layer, it compiles AST tree to scope list.
// A scope is an execution unit.
func (c *Compile) Compile(ctx context.Context, pn *plan.Plan, u any, fill func(any, *batch.Batch) error) (err error) {
	defer func() {
		if e := recover(); e != nil {
			err = moerr.ConvertPanicError(ctx, e)
		}
	}()
	c.u = u
	c.fill = fill
	c.info = plan2.GetExecTypeFromPlan(pn)
	// build scope for a single sql
	s, err := c.compileScope(ctx, pn)
	if err != nil {
		return err
	}
	c.scope = s
	c.scope.Plan = pn
	return nil
}

func (c *Compile) setAffectedRows(n uint64) {
	c.affectRows = n
}

func (c *Compile) GetAffectedRows() uint64 {
	return c.affectRows
}

// Run is an important function of the compute-layer, it executes a single sql according to its scope
func (c *Compile) Run(_ uint64) (err error) {
	if c.scope == nil {
		return nil
	}
	defer func() { c.scope = nil }() // free pipeline

	// XXX PrintScope has a none-trivial amount of logging
	// PrintScope(nil, []*Scope{c.scope})
	switch c.scope.Magic {
	case Normal:
		defer c.fillAnalyzeInfo()
		return c.scope.Run(c)
	case Merge:
		defer c.fillAnalyzeInfo()
		return c.scope.MergeRun(c)
	case MergeInsert:
		defer c.fillAnalyzeInfo()
		err := c.scope.MergeRun(c)
		if err != nil {
			return err
		}
		c.setAffectedRows(c.scope.Instructions[len(c.scope.Instructions)-1].Arg.(*mergeblock.Argument).AffectedRows)
		return nil
	case Remote:
		defer c.fillAnalyzeInfo()
		return c.scope.RemoteRun(c)
	case CreateDatabase:
		err := c.scope.CreateDatabase(c)
		if err != nil {
			return err
		}
		c.setAffectedRows(1)
		return nil
	case DropDatabase:
		err := c.scope.DropDatabase(c)
		if err != nil {
			return err
		}
		c.setAffectedRows(1)
		return nil
	case CreateTable:
		qry := c.scope.Plan.GetDdl().GetCreateTable()
		if qry.Temporary {
			return c.scope.CreateTempTable(c)
		} else {
			return c.scope.CreateTable(c)
		}
	case AlterView:
		return c.scope.AlterView(c)
	case DropTable:
		return c.scope.DropTable(c)
	case CreateIndex:
		return c.scope.CreateIndex(c)
	case DropIndex:
		return c.scope.DropIndex(c)
	case TruncateTable:
		return c.scope.TruncateTable(c)
	case Deletion:
		defer c.fillAnalyzeInfo()
		affectedRows, err := c.scope.Delete(c)
		if err != nil {
			return err
		}
		c.setAffectedRows(affectedRows)
		return nil
	case Insert:
		defer c.fillAnalyzeInfo()
		affectedRows, err := c.scope.Insert(c)
		if err != nil {
			return err
		}
		c.setAffectedRows(affectedRows)
		return nil
	case Update:
		defer c.fillAnalyzeInfo()
		affectedRows, err := c.scope.Update(c)
		if err != nil {
			return err
		}
		c.setAffectedRows(affectedRows)
		return nil
	}
	return nil
}

func (c *Compile) compileScope(ctx context.Context, pn *plan.Plan) (*Scope, error) {
	switch qry := pn.Plan.(type) {
	case *plan.Plan_Query:
		return c.compileQuery(ctx, qry.Query)
	case *plan.Plan_Ddl:
		switch qry.Ddl.DdlType {
		case plan.DataDefinition_CREATE_DATABASE:
			return &Scope{
				Magic: CreateDatabase,
				Plan:  pn,
			}, nil
		case plan.DataDefinition_DROP_DATABASE:
			return &Scope{
				Magic: DropDatabase,
				Plan:  pn,
			}, nil
		case plan.DataDefinition_CREATE_TABLE:
			return &Scope{
				Magic: CreateTable,
				Plan:  pn,
			}, nil
		case plan.DataDefinition_ALTER_VIEW:
			return &Scope{
				Magic: AlterView,
				Plan:  pn,
			}, nil
		case plan.DataDefinition_DROP_TABLE:
			return &Scope{
				Magic: DropTable,
				Plan:  pn,
			}, nil
		case plan.DataDefinition_TRUNCATE_TABLE:
			return &Scope{
				Magic: TruncateTable,
				Plan:  pn,
			}, nil
		case plan.DataDefinition_CREATE_INDEX:
			return &Scope{
				Magic: CreateIndex,
				Plan:  pn,
			}, nil
		case plan.DataDefinition_DROP_INDEX:
			return &Scope{
				Magic: DropIndex,
				Plan:  pn,
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

func (c *Compile) cnListStrategy() {
	if len(c.cnList) == 0 {
		c.cnList = append(c.cnList, engine.Node{Mcpu: c.NumCPU()})
	} else if len(c.cnList) > c.info.CnNumbers {
		c.cnList = c.cnList[:c.info.CnNumbers]
	}
}

func (c *Compile) compileQuery(ctx context.Context, qry *plan.Query) (*Scope, error) {
	if len(qry.Steps) != 1 {
		return nil, moerr.NewNYI(ctx, fmt.Sprintf("query '%s'", qry))
	}
	var err error
	c.cnList, err = c.e.Nodes()
	if err != nil {
		return nil, err
	}
	blkNum := 0
	for _, n := range qry.Nodes {
		if n.NodeType == plan.Node_TABLE_SCAN {
			if n.Stats != nil {
				blkNum += int(n.Stats.BlockNum)
			}
		}
	}
	switch qry.StmtType {
	case plan.Query_INSERT:
		insertNode := qry.Nodes[qry.Steps[0]]
		nodeStats := qry.Nodes[insertNode.Children[0]].Stats
		if nodeStats.GetCost()*float64(SingleLineSizeEstimate) > float64(DistributedThreshold) || qry.LoadTag || blkNum >= MinBlockNum {
			c.cnListStrategy()
		} else {
			c.cnList = engine.Nodes{engine.Node{Mcpu: c.generateCPUNumber(c.NumCPU(), blkNum)}}
		}
	default:
		if blkNum < MinBlockNum {
			c.cnList = engine.Nodes{engine.Node{Mcpu: c.generateCPUNumber(c.NumCPU(), blkNum)}}
		} else {
			c.cnListStrategy()
		}
	}

	c.initAnalyze(qry)
	ss, err := c.compilePlanScope(ctx, qry.Nodes[qry.Steps[0]], qry.Nodes)
	if err != nil {
		return nil, err
	}
	return c.compileApQuery(qry, ss)
}

func (c *Compile) compileApQuery(qry *plan.Query, ss []*Scope) (*Scope, error) {
	var rs *Scope
	switch qry.StmtType {
	case plan.Query_DELETE:
		rs = c.newMergeScope(ss)
		updateScopesLastFlag([]*Scope{rs})
		rs.Magic = Deletion
		c.SetAnalyzeCurrent([]*Scope{rs}, c.anal.curr)
		scp, err := constructDeletion(qry.Nodes[qry.Steps[0]], c.e, c.proc)
		if err != nil {
			return nil, err
		}
		rs.Instructions = append(rs.Instructions, vm.Instruction{
			Op:  vm.Deletion,
			Arg: scp,
		})
	case plan.Query_INSERT:
		insertNode := qry.Nodes[qry.Steps[0]]
		insertNode.NotCacheable = true
		arg, err := constructInsert(insertNode, c.e, c.proc)
		if err != nil {
			return nil, err
		}
		nodeStats := qry.Nodes[insertNode.Children[0]].Stats
		if nodeStats.GetCost()*float64(SingleLineSizeEstimate) > float64(DistributedThreshold) || qry.LoadTag {
			// use distributed-insert
			arg.IsRemote = true
			rs = c.newInsertMergeScope(arg, ss)
			rs.Magic = MergeInsert
			rs.Instructions = append(rs.Instructions, vm.Instruction{
				Op: vm.MergeBlock,
				Arg: &mergeblock.Argument{
					Tbl:         arg.InsertCtx.Source,
					Unique_tbls: arg.InsertCtx.UniqueSource,
				},
			})
		} else {
			rs = c.newMergeScope(ss)
			rs.Magic = Insert
			c.SetAnalyzeCurrent([]*Scope{rs}, c.anal.curr)
			rs.Instructions = append(rs.Instructions, vm.Instruction{
				Op:  vm.Insert,
				Arg: arg,
			})
		}
	case plan.Query_UPDATE:
		scp, err := constructUpdate(qry.Nodes[qry.Steps[0]], c.e, c.proc)
		if err != nil {
			return nil, err
		}
		rs = c.newMergeScope(ss)
		updateScopesLastFlag([]*Scope{rs})
		rs.Magic = Update
		c.SetAnalyzeCurrent([]*Scope{rs}, c.anal.curr)
		rs.Instructions = append(rs.Instructions, vm.Instruction{
			Op:  vm.Update,
			Arg: scp,
		})
	default:
		rs = c.newMergeScope(ss)
		updateScopesLastFlag([]*Scope{rs})
		c.SetAnalyzeCurrent([]*Scope{rs}, c.anal.curr)
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
	if node == nil || node.TableDef == nil { // like : select 1, 2
		bat := batch.NewWithSize(1)
		bat.Vecs[0] = vector.NewConst(types.Type{Oid: types.T_int64}, 1)
		bat.Vecs[0].Col = make([]int64, 1)
		bat.InitZsOne(1)
		return bat, nil
	}
	// select * from (values row(1,1), row(2,2), row(3,3)) a;
	tableDef := node.TableDef
	colCount := len(tableDef.Cols)
	colsData := node.RowsetData.Cols
	rowCount := len(colsData[0].Data)
	bat := batch.NewWithSize(colCount)
	for i := 0; i < colCount; i++ {
		vec, err := rowsetDataToVector(ctx, proc, colsData[i].Data)
		if err != nil {
			return nil, err
		}
		bat.Vecs[i] = vec
	}
	bat.SetZs(rowCount, proc.Mp())
	return bat, nil
}

func (c *Compile) compilePlanScope(ctx context.Context, n *plan.Node, ns []*plan.Node) ([]*Scope, error) {
	switch n.NodeType {
	case plan.Node_VALUE_SCAN:
		ds := &Scope{Magic: Normal}
		ds.Proc = process.NewWithAnalyze(c.proc, c.ctx, 0, c.anal.Nodes())
		bat, err := constructValueScanBatch(ctx, c.proc, n)
		if err != nil {
			return nil, err
		}
		ds.DataSource = &Source{Bat: bat}
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
	case plan.Node_FILTER:
		curr := c.anal.curr
		c.SetAnalyzeCurrent(nil, int(n.Children[0]))
		ss, err := c.compilePlanScope(ctx, ns[n.Children[0]], ns)
		if err != nil {
			return nil, err
		}
		c.SetAnalyzeCurrent(ss, curr)
		return c.compileSort(n, c.compileProjection(n, c.compileRestrict(n, ss))), nil
	case plan.Node_PROJECT:
		curr := c.anal.curr
		c.SetAnalyzeCurrent(nil, int(n.Children[0]))
		ss, err := c.compilePlanScope(ctx, ns[n.Children[0]], ns)
		if err != nil {
			return nil, err
		}
		c.SetAnalyzeCurrent(ss, curr)
		return c.compileSort(n, c.compileProjection(n, c.compileRestrict(n, ss))), nil
	case plan.Node_AGG:
		curr := c.anal.curr
		c.SetAnalyzeCurrent(nil, int(n.Children[0]))
		ss, err := c.compilePlanScope(ctx, ns[n.Children[0]], ns)
		if err != nil {
			return nil, err
		}
		c.SetAnalyzeCurrent(ss, curr)
		if len(n.GroupBy) == 0 || !c.info.WithBigMem {
			ss = c.compileAgg(n, ss, ns)
		} else {
			ss = c.compileGroup(n, ss, ns)
		}
		return c.compileSort(n, c.compileProjection(n, c.compileRestrict(n, ss))), nil
	case plan.Node_JOIN:
		needSwap, joinTyp := joinType(ctx, n, ns)
		curr := c.anal.curr
		c.SetAnalyzeCurrent(nil, int(n.Children[0]))
		ss, err := c.compilePlanScope(ctx, ns[n.Children[0]], ns)
		if err != nil {
			return nil, err
		}
		c.SetAnalyzeCurrent(ss, int(n.Children[1]))
		children, err := c.compilePlanScope(ctx, ns[n.Children[1]], ns)
		if err != nil {
			return nil, err
		}
		c.SetAnalyzeCurrent(children, curr)
		if needSwap {
			return c.compileSort(n, c.compileJoin(ctx, n, ns[n.Children[0]], children, ss, joinTyp)), nil
		}
		return c.compileSort(n, c.compileJoin(ctx, n, ns[n.Children[1]], ss, children, joinTyp)), nil
	case plan.Node_SORT:
		curr := c.anal.curr
		c.SetAnalyzeCurrent(nil, int(n.Children[0]))
		ss, err := c.compilePlanScope(ctx, ns[n.Children[0]], ns)
		if err != nil {
			return nil, err
		}
		c.SetAnalyzeCurrent(ss, curr)
		ss = c.compileSort(n, ss)
		return c.compileProjection(n, c.compileRestrict(n, ss)), nil
	case plan.Node_UNION:
		curr := c.anal.curr
		c.SetAnalyzeCurrent(nil, int(n.Children[0]))
		ss, err := c.compilePlanScope(ctx, ns[n.Children[0]], ns)
		if err != nil {
			return nil, err
		}
		c.SetAnalyzeCurrent(ss, int(n.Children[1]))
		children, err := c.compilePlanScope(ctx, ns[n.Children[1]], ns)
		if err != nil {
			return nil, err
		}
		c.SetAnalyzeCurrent(children, curr)
		return c.compileSort(n, c.compileUnion(n, ss, children, ns)), nil
	case plan.Node_MINUS, plan.Node_INTERSECT, plan.Node_INTERSECT_ALL:
		curr := c.anal.curr
		c.SetAnalyzeCurrent(nil, int(n.Children[0]))
		ss, err := c.compilePlanScope(ctx, ns[n.Children[0]], ns)
		if err != nil {
			return nil, err
		}
		c.SetAnalyzeCurrent(ss, int(n.Children[1]))
		children, err := c.compilePlanScope(ctx, ns[n.Children[1]], ns)
		if err != nil {
			return nil, err
		}
		c.SetAnalyzeCurrent(children, curr)
		return c.compileSort(n, c.compileMinusAndIntersect(n, ss, children, n.NodeType)), nil
	case plan.Node_UNION_ALL:
		curr := c.anal.curr
		c.SetAnalyzeCurrent(nil, int(n.Children[0]))
		ss, err := c.compilePlanScope(ctx, ns[n.Children[0]], ns)
		if err != nil {
			return nil, err
		}
		c.SetAnalyzeCurrent(ss, int(n.Children[1]))
		children, err := c.compilePlanScope(ctx, ns[n.Children[1]], ns)
		if err != nil {
			return nil, err
		}
		c.SetAnalyzeCurrent(children, curr)
		return c.compileSort(n, c.compileUnionAll(n, ss, children)), nil
	case plan.Node_DELETE:
		if n.DeleteCtx.CanTruncate {
			return nil, nil
		}
		ss, err := c.compilePlanScope(ctx, ns[n.Children[0]], ns)
		if err != nil {
			return nil, err
		}
		return ss, nil
	case plan.Node_INSERT:
		ss, err := c.compilePlanScope(ctx, ns[n.Children[0]], ns)
		if err != nil {
			return nil, err
		}
		return ss, nil
		// return c.compileProjection(n, c.compileRestrict(n, ss)), nil
	case plan.Node_UPDATE:
		ss, err := c.compilePlanScope(ctx, ns[n.Children[0]], ns)
		if err != nil {
			return nil, err
		}
		return ss, nil
	case plan.Node_FUNCTION_SCAN:
		var (
			pre []*Scope
			err error
		)
		curr := c.anal.curr
		c.SetAnalyzeCurrent(nil, int(n.Children[0]))
		pre, err = c.compilePlanScope(ctx, ns[n.Children[0]], ns)
		if err != nil {
			return nil, err
		}
		c.SetAnalyzeCurrent(pre, curr)
		ss, err := c.compileTableFunction(n, pre)
		if err != nil {
			return nil, err
		}
		return c.compileSort(n, c.compileProjection(n, c.compileRestrict(n, ss))), nil
	default:
		return nil, moerr.NewNYI(ctx, fmt.Sprintf("query '%s'", n))
	}
}

func (c *Compile) ConstructScope() *Scope {
	ds := &Scope{Magic: Normal}
	ds.Proc = process.NewWithAnalyze(c.proc, c.ctx, 0, c.anal.Nodes())
	ds.Proc.LoadTag = true
	bat := batch.NewWithSize(1)
	{
		bat.Vecs[0] = vector.NewConst(types.Type{Oid: types.T_int64}, 1)
		bat.Vecs[0].Col = make([]int64, 1)
		bat.InitZsOne(1)
	}
	ds.DataSource = &Source{Bat: bat}
	return ds
}

func (c *Compile) compileExternScan(ctx context.Context, n *plan.Node) ([]*Scope, error) {
	mcpu := c.cnList[0].Mcpu
	param := &tree.ExternParam{}
	err := json.Unmarshal([]byte(n.TableDef.Createsql), param)
	if param.Local {
		mcpu = 1
	}
	if err != nil {
		return nil, err
	}
	if param.ScanType == tree.S3 {
		if err := plan2.InitS3Param(param); err != nil {
			return nil, err
		}
		if param.Parallel {
			if mcpu > external.S3_PARALLEL_MAXNUM {
				mcpu = external.S3_PARALLEL_MAXNUM
			}
		}
	} else {
		if err := plan2.InitInfileParam(param); err != nil {
			return nil, err
		}
	}

	if n.ObjRef != nil {
		param.SysTable = external.IsSysTable(n.ObjRef.SchemaName, n.TableDef.Name)
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
			fileList, fileSize, err = plan2.ReadDir(param)
			if err != nil {
				return nil, err
			}
		}
		fileList, fileSize, err = external.FilterFileList(n, c.proc, fileList, fileSize)
		if err != nil {
			return nil, err
		}
		if param.LoadFile && len(fileList) == 0 {
			return nil, moerr.NewInvalidInput(ctx, "the file does not exist in load flow")
		}
	} else {
		fileList = []string{param.Filepath}
	}

	var fileOffset [][][2]int
	for i := 0; i < len(fileList); i++ {
		param.Filepath = fileList[i]
		if param.Parallel {
			arr, err := external.ReadFileOffset(param, c.proc, mcpu, fileSize[i])
			fileOffset = append(fileOffset, arr)
			if err != nil {
				return nil, err
			}
		}
	}

	cnt := len(fileList) / mcpu
	tag := len(fileList) % mcpu
	index := 0
	currentFirstFlag := c.anal.isFirst
	ss := make([]*Scope, mcpu)
	for i := 0; i < mcpu; i++ {
		ss[i] = c.ConstructScope()
		var fileListTmp []string
		if i < tag {
			fileListTmp = fileList[index : index+cnt+1]
			index += cnt + 1
		} else {
			fileListTmp = fileList[index : index+cnt]
			index += cnt
		}
		offset := make([][2]int, 0)
		for j := 0; j < len(fileOffset); j++ {
			offset = append(offset, [2]int{fileOffset[j][i][0], fileOffset[j][i][1]})
		}
		ss[i].appendInstruction(vm.Instruction{
			Op:      vm.External,
			Idx:     c.anal.curr,
			IsFirst: currentFirstFlag,
			Arg:     constructExternal(n, param, c.ctx, fileListTmp, fileSize, offset),
		})
		if param.Parallel {
			ss[i].Instructions[0].Arg.(*external.Argument).Es.FileList = fileList
		}
	}
	c.anal.isFirst = false
	return ss, nil
}

func (c *Compile) compileTableFunction(n *plan.Node, ss []*Scope) ([]*Scope, error) {
	currentFirstFlag := c.anal.isFirst
	for i := range ss {
		ss[i].appendInstruction(vm.Instruction{
			Op:      vm.TableFunction,
			Idx:     c.anal.curr,
			IsFirst: currentFirstFlag,
			Arg:     constructTableFunction(n, c.ctx, n.TableDef.TblFunc.Name),
		})
	}
	c.anal.isFirst = false

	return ss, nil
}

func (c *Compile) compileTableScan(n *plan.Node) ([]*Scope, error) {
	nodes, err := c.generateNodes(n)
	if err != nil {
		return nil, err
	}
	ss := make([]*Scope, 0, len(nodes))
	for i := range nodes {
		ss = append(ss, c.compileTableScanWithNode(n, nodes[i]))
	}
	return ss, nil
}

func (c *Compile) compileTableScanWithNode(n *plan.Node, node engine.Node) *Scope {
	var s *Scope
	var tblDef *plan.TableDef
	var ts timestamp.Timestamp
	var db engine.Database
	var rel engine.Relation
	var err error

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
		db, err = c.e.Database(ctx, n.ObjRef.SchemaName, c.proc.TxnOperator)
		if err != nil {
			panic(err)
		}
		rel, err = db.Relation(ctx, n.TableDef.Name)
		if err != nil {
			var e error // avoid contamination of error messages
			db, e = c.e.Database(c.ctx, defines.TEMPORARY_DBNAME, c.proc.TxnOperator)
			if e != nil {
				panic(e)
			}
			rel, e = db.Relation(c.ctx, engine.GetTempTableName(n.ObjRef.SchemaName, n.TableDef.Name))
			if e != nil {
				panic(e)
			}
		}
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
					Name: attr.Attr.Name,
					Typ: &plan.Type{
						Id:        int32(attr.Attr.Type.Oid),
						Width:     attr.Attr.Type.Width,
						Size:      attr.Attr.Type.Size,
						Precision: attr.Attr.Type.Precision,
						Scale:     attr.Attr.Type.Scale,
						AutoIncr:  attr.Attr.AutoIncrement,
					},
					Primary:   attr.Attr.Primary,
					Default:   attr.Attr.Default,
					OnUpdate:  attr.Attr.OnUpdate,
					Comment:   attr.Attr.Comment,
					ClusterBy: attr.Attr.ClusterBy,
				})
				i++
			}
		}
		tblDef = &plan.TableDef{
			Cols:          cols,
			Name2ColIndex: name2index,
			Name:          n.TableDef.Name,
			TableType:     n.TableDef.GetTableType(),
		}
	}
	s = &Scope{
		Magic:    Remote,
		NodeInfo: node,
		DataSource: &Source{
			Timestamp:    ts,
			Attributes:   attrs,
			TableDef:     tblDef,
			RelationName: n.TableDef.Name,
			SchemaName:   n.ObjRef.SchemaName,
			Expr:         colexec.RewriteFilterExprList(n.FilterList),
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
	for i := range ss {
		ss[i].appendInstruction(vm.Instruction{
			Op:      vm.Restrict,
			Idx:     c.anal.curr,
			IsFirst: currentFirstFlag,
			Arg:     constructRestrict(n),
		})
	}
	c.anal.isFirst = false
	return ss
}

func (c *Compile) compileProjection(n *plan.Node, ss []*Scope) []*Scope {
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

func (c *Compile) compileUnion(n *plan.Node, ss []*Scope, children []*Scope, ns []*plan.Node) []*Scope {
	ss = append(ss, children...)
	rs := c.newScopeList(1, int(n.Stats.BlockNum))
	gn := new(plan.Node)
	gn.GroupBy = make([]*plan.Expr, len(n.ProjectList))
	copy(gn.GroupBy, n.ProjectList)
	for i := range rs {
		ch := c.newMergeScope(dupScopeList(ss))
		ch.appendInstruction(vm.Instruction{
			Op: vm.Connector,
			Arg: &connector.Argument{
				Reg: rs[i].Proc.Reg.MergeReceivers[0],
			},
		})
		ch.IsEnd = true
		rs[i].PreScopes = []*Scope{ch}
		rs[i].Instructions = append(rs[i].Instructions, vm.Instruction{
			Op:  vm.Group,
			Idx: c.anal.curr,
			Arg: constructGroup(c.ctx, gn, n, i, len(rs), true, c.proc),
		})
	}
	return rs
}

func (c *Compile) compileMinusAndIntersect(n *plan.Node, ss []*Scope, children []*Scope, nodeType plan.Node_NodeType) []*Scope {
	rs := c.newJoinScopeListWithBucket(c.newScopeList(2, int(n.Stats.BlockNum)), ss, children)
	switch nodeType {
	case plan.Node_MINUS:
		for i := range rs {
			rs[i].Instructions[0] = vm.Instruction{
				Op:  vm.Minus,
				Idx: c.anal.curr,
				Arg: constructMinus(n, c.proc, i, len(rs)),
			}
		}
	case plan.Node_INTERSECT:
		for i := range rs {
			rs[i].Instructions[0] = vm.Instruction{
				Op:  vm.Intersect,
				Idx: c.anal.curr,
				Arg: constructIntersect(n, c.proc, i, len(rs)),
			}
		}
	case plan.Node_INTERSECT_ALL:
		for i := range rs {
			rs[i].Instructions[0] = vm.Instruction{
				Op:  vm.IntersectAll,
				Idx: c.anal.curr,
				Arg: constructIntersectAll(n, c.proc, i, len(rs)),
			}
		}

	}
	return rs
}

func (c *Compile) compileUnionAll(n *plan.Node, ss []*Scope, children []*Scope) []*Scope {
	rs := c.newMergeScope(append(ss, children...))
	rs.Instructions[0].Idx = c.anal.curr
	return []*Scope{rs}
}

func (c *Compile) compileJoin(ctx context.Context, n, right *plan.Node, ss []*Scope, children []*Scope, joinTyp plan.Node_JoinFlag) []*Scope {
	var rs []*Scope

	isEq := isEquiJoin(n.OnList)
	typs := make([]types.Type, len(right.ProjectList))
	for i, expr := range right.ProjectList {
		typs[i] = dupType(expr.Typ)
	}
	switch joinTyp {
	case plan.Node_INNER:
		rs = c.newJoinScopeList(ss, children)
		if len(n.OnList) == 0 {
			for i := range rs {
				rs[i].appendInstruction(vm.Instruction{
					Op:  vm.Product,
					Idx: c.anal.curr,
					Arg: constructProduct(n, typs, c.proc),
				})
			}
		} else {
			for i := range rs {
				if isEq {
					rs[i].appendInstruction(vm.Instruction{
						Op:  vm.Join,
						Idx: c.anal.curr,
						Arg: constructJoin(n, typs, c.proc),
					})
				} else {
					rs[i].appendInstruction(vm.Instruction{
						Op:  vm.LoopJoin,
						Idx: c.anal.curr,
						Arg: constructLoopJoin(n, typs, c.proc),
					})
				}
			}
		}
	case plan.Node_SEMI:
		rs = c.newJoinScopeList(ss, children)
		for i := range rs {
			if isEq {
				rs[i].appendInstruction(vm.Instruction{
					Op:  vm.Semi,
					Idx: c.anal.curr,
					Arg: constructSemi(n, typs, c.proc),
				})
			} else {
				rs[i].appendInstruction(vm.Instruction{
					Op:  vm.LoopSemi,
					Idx: c.anal.curr,
					Arg: constructLoopSemi(n, typs, c.proc),
				})
			}
		}
	case plan.Node_LEFT:
		rs = c.newJoinScopeList(ss, children)
		for i := range rs {
			if isEq {
				rs[i].appendInstruction(vm.Instruction{
					Op:  vm.Left,
					Idx: c.anal.curr,
					Arg: constructLeft(n, typs, c.proc),
				})
			} else {
				rs[i].appendInstruction(vm.Instruction{
					Op:  vm.LoopLeft,
					Idx: c.anal.curr,
					Arg: constructLoopLeft(n, typs, c.proc),
				})
			}
		}
	case plan.Node_SINGLE:
		rs = c.newJoinScopeList(ss, children)
		for i := range rs {
			if isEq {
				rs[i].appendInstruction(vm.Instruction{
					Op:  vm.Single,
					Idx: c.anal.curr,
					Arg: constructSingle(n, typs, c.proc),
				})
			} else {
				rs[i].appendInstruction(vm.Instruction{
					Op:  vm.LoopSingle,
					Idx: c.anal.curr,
					Arg: constructLoopSingle(n, typs, c.proc),
				})
			}
		}
	case plan.Node_ANTI:
		rs = c.newJoinScopeList(ss, children)
		_, conds := extraJoinConditions(n.OnList)
		for i := range rs {
			if isEq && len(conds) == 1 {
				rs[i].appendInstruction(vm.Instruction{
					Op:  vm.Anti,
					Idx: c.anal.curr,
					Arg: constructAnti(n, typs, c.proc),
				})
			} else {
				rs[i].appendInstruction(vm.Instruction{
					Op:  vm.LoopAnti,
					Idx: c.anal.curr,
					Arg: constructLoopAnti(n, typs, c.proc),
				})
			}
		}
	case plan.Node_MARK:
		rs = c.newJoinScopeList(ss, children)
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
				Arg: constructLoopMark(n, typs, c.proc),
			})
			//}
		}
	default:
		panic(moerr.NewNYI(ctx, fmt.Sprintf("join typ '%v'", n.JoinType)))
	}
	return rs
}

func (c *Compile) compileSort(n *plan.Node, ss []*Scope) []*Scope {
	switch {
	case n.Limit != nil && n.Offset == nil && len(n.OrderBy) > 0: // top
		vec, err := colexec.EvalExpr(constBat, c.proc, n.Limit)
		if err != nil {
			panic(err)
		}
		defer vec.Free(c.proc.Mp())
		return c.compileTop(n, vec.Col.([]int64)[0], ss)
	case n.Limit == nil && n.Offset == nil && len(n.OrderBy) > 0: // top
		return c.compileOrder(n, ss)
	case n.Limit != nil && n.Offset != nil && len(n.OrderBy) > 0:
		vec1, err := colexec.EvalExpr(constBat, c.proc, n.Limit)
		if err != nil {
			panic(err)
		}
		defer vec1.Free(c.proc.Mp())
		vec2, err := colexec.EvalExpr(constBat, c.proc, n.Offset)
		if err != nil {
			panic(err)
		}
		defer vec2.Free(c.proc.Mp())
		limit, offset := vec1.Col.([]int64)[0], vec2.Col.([]int64)[0]
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
			Arg:     constructOrder(n, c.proc),
		})
	}
	c.anal.isFirst = false

	rs := c.newMergeScope(ss)
	rs.Instructions[0] = vm.Instruction{
		Op:  vm.MergeOrder,
		Idx: c.anal.curr,
		Arg: constructMergeOrder(n, c.proc),
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

func (c *Compile) compileAgg(n *plan.Node, ss []*Scope, ns []*plan.Node) []*Scope {
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
		Arg: constructMergeGroup(n, true),
	}
	return []*Scope{rs}
}

func (c *Compile) compileGroup(n *plan.Node, ss []*Scope, ns []*plan.Node) []*Scope {
	currentIsFirst := c.anal.isFirst
	c.anal.isFirst = false
	rs := c.newScopeList(validScopeCount(ss), int(n.Stats.BlockNum))
	j := 0
	for i := range ss {
		if containBrokenNode(ss[i]) {
			isEnd := ss[i].IsEnd
			ss[i] = c.newMergeScope([]*Scope{ss[i]})
			ss[i].IsEnd = isEnd
		}
		if !ss[i].IsEnd {
			ss[i].appendInstruction(vm.Instruction{
				Op:  vm.Dispatch,
				Arg: constructDispatch(true, extraRegisters(rs, j)),
			})
			j++
			ss[i].IsEnd = true
		}
	}

	for i := range rs {
		rs[i].Instructions = append(rs[i].Instructions, vm.Instruction{
			Op:      vm.Group,
			Idx:     c.anal.curr,
			IsFirst: currentIsFirst,
			Arg:     constructGroup(c.ctx, n, ns[n.Children[0]], i, len(rs), true, c.proc),
		})
	}
	return []*Scope{c.newMergeScope(append(rs, ss...))}
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
		ss2[i].Instructions = append(ss2[i].Instructions, dupInstruction(insert, nil))
	}
	return c.newMergeScope(ss2)
}

func (c *Compile) newMergeScope(ss []*Scope) *Scope {
	rs := &Scope{
		PreScopes: ss,
		Magic:     Merge,
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

func (c *Compile) newScopeList(childrenCount int, blocks int) []*Scope {
	var ss []*Scope

	currentFirstFlag := c.anal.isFirst
	for _, n := range c.cnList {
		c.anal.isFirst = currentFirstFlag
		ss = append(ss, c.newScopeListWithNode(c.generateCPUNumber(n.Mcpu, blocks), childrenCount)...)
	}
	return ss
}

func (c *Compile) newScopeListWithNode(mcpu, childrenCount int) []*Scope {
	ss := make([]*Scope, mcpu)
	currentFirstFlag := c.anal.isFirst
	for i := range ss {
		ss[i] = new(Scope)
		ss[i].Magic = Remote
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

func (c *Compile) newJoinScopeListWithBucket(rs, ss, children []*Scope) []*Scope {
	currentFirstFlag := c.anal.isFirst
	for i := range rs {
		c.anal.isFirst = currentFirstFlag
		left := c.newMergeScope(dupScopeList(ss))

		c.anal.isFirst = currentFirstFlag
		right := c.newMergeScope(dupScopeList(children))

		rs[i].PreScopes = []*Scope{left, right}
		left.appendInstruction(vm.Instruction{
			Op: vm.Connector,
			Arg: &connector.Argument{
				Reg: rs[i].Proc.Reg.MergeReceivers[0],
			},
		})
		right.appendInstruction(vm.Instruction{
			Op: vm.Connector,
			Arg: &connector.Argument{
				Reg: rs[i].Proc.Reg.MergeReceivers[1],
			},
		})
		left.IsEnd = true
		right.IsEnd = true
	}
	return rs
}

func (c *Compile) newJoinScopeList(ss []*Scope, children []*Scope) []*Scope {
	rs := make([]*Scope, len(ss))
	// join's input will record in the left/right scope when JoinRun
	// so set it to false here.
	c.anal.isFirst = false
	for i := range ss {
		if ss[i].IsEnd {
			rs[i] = ss[i]
			continue
		}
		chp := c.newMergeScope(dupScopeList(children))
		rs[i] = new(Scope)
		rs[i].Magic = Remote
		rs[i].IsJoin = true
		rs[i].NodeInfo = ss[i].NodeInfo
		rs[i].PreScopes = []*Scope{ss[i], chp}
		rs[i].Proc = process.NewWithAnalyze(c.proc, c.ctx, 2, c.anal.Nodes())
		ss[i].appendInstruction(vm.Instruction{
			Op: vm.Connector,
			Arg: &connector.Argument{
				Reg: rs[i].Proc.Reg.MergeReceivers[0],
			},
		})
		chp.appendInstruction(vm.Instruction{
			Op: vm.Connector,
			Arg: &connector.Argument{
				Reg: rs[i].Proc.Reg.MergeReceivers[1],
			},
		})
		chp.IsEnd = true
	}
	return rs
}

//func (c *Compile) newBroadcastJoinScopeList(ss []*Scope, children []*Scope) []*Scope {
//len := len(ss)
//rs := make([]*Scope, len)
//idx := 0
//for i := range ss {
//if ss[i].IsEnd {
//rs[i] = ss[i]
//continue
//}
//rs[i] = new(Scope)
//rs[i].Magic = Remote
//rs[i].IsJoin = true
//rs[i].NodeInfo = ss[i].NodeInfo
//if isCurrentCN(rs[i].NodeInfo.Addr, c.addr) {
//idx = i
//}
//rs[i].PreScopes = []*Scope{ss[i]}
//rs[i].Proc = process.NewWithAnalyze(c.proc, c.ctx, 2, c.anal.Nodes())
//ss[i].appendInstruction(vm.Instruction{
//Op: vm.Connector,
//Arg: &connector.Argument{
//Reg: rs[i].Proc.Reg.MergeReceivers[0],
//},
//})
//}

//mergeChildren := c.newMergeScope(children)
//mergeChildren.appendInstruction(vm.Instruction{
//Op:  vm.Dispatch,
//Arg: constructBroadcastJoinDispatch(1, rs, c.addr, mergeChildren.Proc),
//})
//rs[idx].PreScopes = append(rs[idx].PreScopes, mergeChildren)

//return rs
//}

func (c *Compile) newLeftScope(s *Scope, ss []*Scope) *Scope {
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
		Arg: constructDispatch(false, extraRegisters(ss, 0)),
	})
	rs.IsEnd = true
	rs.Proc = process.NewWithAnalyze(s.Proc, c.ctx, 1, c.anal.Nodes())
	rs.Proc.Reg.MergeReceivers[0] = s.Proc.Reg.MergeReceivers[0]
	return rs
}

func (c *Compile) newRightScope(s *Scope, ss []*Scope) *Scope {
	rs := &Scope{
		Magic: Merge,
	}
	rs.appendInstruction(vm.Instruction{
		Op:      vm.HashBuild,
		Idx:     s.Instructions[0].Idx,
		IsFirst: true,
		Arg:     constructHashBuild(s.Instructions[0], c.proc),
	})
	rs.appendInstruction(vm.Instruction{
		Op:  vm.Dispatch,
		Arg: constructDispatch(true, extraRegisters(ss, 1)),
	})
	rs.IsEnd = true
	rs.Proc = process.NewWithAnalyze(s.Proc, c.ctx, 1, c.anal.Nodes())
	rs.Proc.Reg.MergeReceivers[0] = s.Proc.Reg.MergeReceivers[1]

	for i, u := range s.RemoteReceivRegInfos {
		if u.Idx == 1 {
			rs.RemoteReceivRegInfos = append(rs.RemoteReceivRegInfos, RemoteReceivRegInfo{
				Idx:      0,
				Uuid:     u.Uuid,
				FromAddr: u.FromAddr,
			})
			s.RemoteReceivRegInfos = append(s.RemoteReceivRegInfos[:i], s.RemoteReceivRegInfos[i+1:]...)
			break
		}
	}

	return rs
}

// Number of cpu's available on the current machine
func (c *Compile) NumCPU() int {
	return runtime.NumCPU()
}

func (c *Compile) generateCPUNumber(cpunum, blocks int) int {
	if blocks < cpunum {
		if blocks <= 0 {
			return 1
		}
		return blocks
	}
	if cpunum <= 0 {
		return 1
	}
	return cpunum
}

func (c *Compile) initAnalyze(qry *plan.Query) {
	anals := make([]*process.AnalyzeInfo, len(qry.Nodes))
	for i := range anals {
		anals[i] = new(process.AnalyzeInfo)
	}
	c.anal = &anaylze{
		qry:       qry,
		analInfos: anals,
		curr:      int(qry.Steps[0]),
	}
}

func (c *Compile) fillAnalyzeInfo() {
	for i, anal := range c.anal.analInfos {
		if c.anal.qry.Nodes[i].AnalyzeInfo == nil {
			c.anal.qry.Nodes[i].AnalyzeInfo = new(plan.AnalyzeInfo)
		}
		c.anal.qry.Nodes[i].AnalyzeInfo.InputRows = atomic.LoadInt64(&anal.InputRows)
		c.anal.qry.Nodes[i].AnalyzeInfo.OutputRows = atomic.LoadInt64(&anal.OutputRows)
		c.anal.qry.Nodes[i].AnalyzeInfo.InputSize = atomic.LoadInt64(&anal.InputSize)
		c.anal.qry.Nodes[i].AnalyzeInfo.OutputSize = atomic.LoadInt64(&anal.OutputSize)
		c.anal.qry.Nodes[i].AnalyzeInfo.TimeConsumed = atomic.LoadInt64(&anal.TimeConsumed)
		c.anal.qry.Nodes[i].AnalyzeInfo.MemorySize = atomic.LoadInt64(&anal.MemorySize)
		c.anal.qry.Nodes[i].AnalyzeInfo.WaitTimeConsumed = atomic.LoadInt64(&anal.WaitTimeConsumed)
		c.anal.qry.Nodes[i].AnalyzeInfo.DiskIO = atomic.LoadInt64(&anal.DiskIO)
		c.anal.qry.Nodes[i].AnalyzeInfo.S3IOByte = atomic.LoadInt64(&anal.S3IOByte)
		c.anal.qry.Nodes[i].AnalyzeInfo.S3IOCount = atomic.LoadInt64(&anal.S3IOCount)
		c.anal.qry.Nodes[i].AnalyzeInfo.NetworkIO = atomic.LoadInt64(&anal.NetworkIO)
		c.anal.qry.Nodes[i].AnalyzeInfo.ScanTime = atomic.LoadInt64(&anal.ScanTime)
		c.anal.qry.Nodes[i].AnalyzeInfo.InsertTime = atomic.LoadInt64(&anal.InsertTime)
	}
}

func (c *Compile) generateNodes(n *plan.Node) (engine.Nodes, error) {
	var err error
	var db engine.Database
	var rel engine.Relation
	var ranges [][]byte
	var nodes engine.Nodes
	ctx := c.ctx
	if util.TableIsClusterTable(n.TableDef.GetTableType()) {
		ctx = context.WithValue(ctx, defines.TenantIDKey{}, catalog.System_Account)
	}

	db, err = c.e.Database(ctx, n.ObjRef.SchemaName, c.proc.TxnOperator)
	if err != nil {
		return nil, err
	}
	rel, err = db.Relation(ctx, n.TableDef.Name)
	if err != nil {
		var e error // avoid contamination of error messages
		db, e = c.e.Database(ctx, defines.TEMPORARY_DBNAME, c.proc.TxnOperator)
		if e != nil {
			return nil, err
		}

		rel, e = db.Relation(ctx, engine.GetTempTableName(n.ObjRef.SchemaName, n.TableDef.Name))
		if e != nil {
			return nil, err
		}
		c.isTemporaryScan = true
	}
	if c.isTemporaryScan {
		c.isTemporaryScan = false
		for i := 0; i < len(c.cnList); i++ {
			c.cnList[i].Addr = ""
		}
	}
	ranges, err = rel.Ranges(ctx, plan2.HandleFiltersForZM(n.FilterList, c.proc))
	if err != nil {
		return nil, err
	}
	if len(ranges) == 0 {
		nodes = make(engine.Nodes, len(c.cnList))
		for i, node := range c.cnList {
			nodes[i] = engine.Node{
				Rel:  rel,
				Id:   node.Id,
				Addr: node.Addr,
				Mcpu: c.generateCPUNumber(node.Mcpu, int(n.Stats.BlockNum)),
			}
		}
		return nodes, nil
	}
	// ranges[0] means memtable
	if len(ranges[0]) == 0 {
		if c.info.Typ == plan2.ExecTypeTP {
			nodes = append(nodes, engine.Node{
				Rel:  rel,
				Mcpu: 1,
			})
		} else {
			nodes = append(nodes, engine.Node{
				Rel:  rel,
				Mcpu: c.generateCPUNumber(runtime.NumCPU(), int(n.Stats.BlockNum)),
			})
		}
		ranges = ranges[1:]
	}
	if len(ranges) == 0 {
		return nodes, nil
	}
	step := (len(ranges) + len(c.cnList) - 1) / len(c.cnList)
	for i := 0; i < len(ranges); i += step {
		j := i / step
		if i+step >= len(ranges) {
			nodes = append(nodes, engine.Node{
				Rel:  rel,
				Id:   c.cnList[j].Id,
				Addr: c.cnList[j].Addr,
				Mcpu: c.generateCPUNumber(c.cnList[j].Mcpu, int(n.Stats.BlockNum)),
				Data: ranges[i:],
			})
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
	return nodes, nil
}

func (anal *anaylze) Nodes() []*process.AnalyzeInfo {
	return anal.analInfos
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

func joinType(ctx context.Context, n *plan.Node, ns []*plan.Node) (bool, plan.Node_JoinFlag) {
	switch n.JoinType {
	case plan.Node_INNER:
		return false, plan.Node_INNER
	case plan.Node_LEFT:
		return false, plan.Node_LEFT
	case plan.Node_SEMI:
		return false, plan.Node_SEMI
	case plan.Node_ANTI:
		return false, plan.Node_ANTI
	case plan.Node_RIGHT:
		return true, plan.Node_LEFT
	case plan.Node_SINGLE:
		return false, plan.Node_SINGLE
	case plan.Node_MARK:
		return false, plan.Node_MARK
	default:
		panic(moerr.NewNYI(ctx, fmt.Sprintf("join typ '%v'", n.JoinType)))
	}
}

func dupType(typ *plan.Type) types.Type {
	return types.Type{
		Oid:       types.T(typ.Id),
		Size:      typ.Size,
		Width:     typ.Width,
		Scale:     typ.Scale,
		Precision: typ.Precision,
	}
}

// Update the specific scopes's instruction to true
// then update the current idx
func (c *Compile) SetAnalyzeCurrent(updateScopes []*Scope, nextId int) {
	if updateScopes != nil {
		updateScopesLastFlag(updateScopes)
	}

	c.anal.curr = nextId
	c.anal.isFirst = true
}

func updateScopesLastFlag(updateScopes []*Scope) {
	for _, s := range updateScopes {
		last := len(s.Instructions) - 1
		s.Instructions[last].IsLast = true
	}
}

//func isCurrentCN(addr string, currentCNAddr string) bool {
//return strings.Split(addr, ":")[0] == strings.Split(currentCNAddr, ":")[0]
//}

func rowsetDataToVector(ctx context.Context, proc *process.Process, exprs []*plan.Expr) (*vector.Vector, error) {
	rowCount := len(exprs)
	if rowCount == 0 {
		return nil, moerr.NewInternalError(ctx, "rowsetData do not have rows")
	}
	var typ types.Type
	var vec *vector.Vector
	for _, e := range exprs {
		if e.Typ.Id != int32(types.T_any) {
			typ = plan2.MakeTypeByPlan2Type(e.Typ)
			vec = vector.New(typ)
			break
		}
	}
	if vec == nil {
		typ = types.T_int32.ToType()
		vec = vector.New(typ)
	}
	bat := batch.NewWithSize(0)
	bat.Zs = []int64{1}
	defer bat.Clean(proc.Mp())

	for _, e := range exprs {
		tmp, err := colexec.EvalExpr(bat, proc, e)
		if err != nil {
			return nil, err
		}
		if tmp.IsScalarNull() {
			vec.Append(vector.GetInitConstVal(typ), true, proc.Mp())
			continue
		}
		switch typ.Oid {
		case types.T_bool:
			vec.Append(vector.MustTCols[bool](tmp)[0], false, proc.Mp())
		case types.T_int8:
			vec.Append(vector.MustTCols[int8](tmp)[0], false, proc.Mp())
		case types.T_int16:
			vec.Append(vector.MustTCols[int16](tmp)[0], false, proc.Mp())
		case types.T_int32:
			vec.Append(vector.MustTCols[int32](tmp)[0], false, proc.Mp())
		case types.T_int64:
			vec.Append(vector.MustTCols[int64](tmp)[0], false, proc.Mp())
		case types.T_uint8:
			vec.Append(vector.MustTCols[uint8](tmp)[0], false, proc.Mp())
		case types.T_uint16:
			vec.Append(vector.MustTCols[uint16](tmp)[0], false, proc.Mp())
		case types.T_uint32:
			vec.Append(vector.MustTCols[uint32](tmp)[0], false, proc.Mp())
		case types.T_uint64:
			vec.Append(vector.MustTCols[uint64](tmp)[0], false, proc.Mp())
		case types.T_float32:
			vec.Append(vector.MustTCols[float32](tmp)[0], false, proc.Mp())
		case types.T_float64:
			vec.Append(vector.MustTCols[float64](tmp)[0], false, proc.Mp())
		case types.T_char, types.T_varchar, types.T_json, types.T_blob, types.T_text:
			vec.Append(vector.MustBytesCols(tmp)[0], false, proc.Mp())
		case types.T_date:
			vec.Append(vector.MustTCols[types.Date](tmp)[0], false, proc.Mp())
		case types.T_datetime:
			vec.Append(vector.MustTCols[types.Datetime](tmp)[0], false, proc.Mp())
		case types.T_time:
			vec.Append(vector.MustTCols[types.Time](tmp)[0], false, proc.Mp())
		case types.T_timestamp:
			vec.Append(vector.MustTCols[types.Timestamp](tmp)[0], false, proc.Mp())
		case types.T_decimal64:
			vec.Append(vector.MustTCols[types.Decimal64](tmp)[0], false, proc.Mp())
		case types.T_decimal128:
			vec.Append(vector.MustTCols[types.Decimal128](tmp)[0], false, proc.Mp())
		case types.T_uuid:
			vec.Append(vector.MustTCols[types.Uuid](tmp)[0], false, proc.Mp())
		default:
			return nil, moerr.NewNYI(ctx, fmt.Sprintf("expression %v can not eval to constant and append to rowsetData", e))
		}
	}
	return vec, nil
}
