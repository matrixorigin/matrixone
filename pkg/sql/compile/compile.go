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
	"fmt"
	"runtime"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/errno"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/connector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/merge"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/output"
	"github.com/matrixorigin/matrixone/pkg/sql/errors"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// New is used to new an object of compile
func New(db string, sql string, uid string,
	e engine.Engine, proc *process.Process, stmt tree.Statement) *Compile {
	return &Compile{
		e:      e,
		db:     db,
		uid:    uid,
		sql:    sql,
		proc:   proc,
		stmt:   stmt,
		cnList: e.Nodes(),
	}
}

// Compile is the entrance of the compute-layer, it compiles AST tree to scope list.
// A scope is an execution unit.
func (c *Compile) Compile(pn *plan.Plan, u interface{}, fill func(interface{}, *batch.Batch) error) (err error) {
	defer func() {
		if e := recover(); e != nil {
			err = moerr.NewPanicError(e)
		}
	}()
	c.u = u
	c.fill = fill
	// build scope for a single sql
	s, err := c.compileScope(pn)
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
func (c *Compile) Run(ts uint64) (err error) {
	if c.scope == nil {
		return nil
	}

	//	PrintScope(nil, []*Scope{c.scope})

	switch c.scope.Magic {
	case Normal:
		return c.scope.Run(c.e)
	case Merge:
		return c.scope.MergeRun(c.e)
	case Remote:
		return c.scope.RemoteRun(c.e)
	case CreateDatabase:
		return c.scope.CreateDatabase(ts, c.proc.Snapshot, c.e)
	case DropDatabase:
		return c.scope.DropDatabase(ts, c.proc.Snapshot, c.e)
	case CreateTable:
		return c.scope.CreateTable(ts, c.proc.Snapshot, c.e, c.db)
	case DropTable:
		return c.scope.DropTable(ts, c.proc.Snapshot, c.e)
	case CreateIndex:
		return c.scope.CreateIndex(ts, c.proc.Snapshot, c.e)
	case DropIndex:
		return c.scope.DropIndex(ts, c.proc.Snapshot, c.e)
	case Deletion:
		affectedRows, err := c.scope.Delete(ts, c.proc.Snapshot, c.e)
		if err != nil {
			return err
		}
		c.setAffectedRows(affectedRows)
		return nil
	case Insert:
		affectedRows, err := c.scope.Insert(ts, c.proc.Snapshot, c.e)
		if err != nil {
			return err
		}
		c.setAffectedRows(affectedRows)
		return nil
	case Update:
		affectedRows, err := c.scope.Update(ts, c.proc.Snapshot, c.e)
		if err != nil {
			return err
		}
		c.setAffectedRows(affectedRows)
		return nil
	case InsertValues:
		affectedRows, err := c.scope.InsertValues(ts, c.proc.Snapshot, c.e, c.stmt.(*tree.Insert))
		if err != nil {
			return err
		}
		c.setAffectedRows(affectedRows)
		return nil
	}
	return nil
}

func (c *Compile) compileScope(pn *plan.Plan) (*Scope, error) {
	switch qry := pn.Plan.(type) {
	case *plan.Plan_Query:
		return c.compileQuery(qry.Query)
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
		case plan.DataDefinition_DROP_TABLE:
			return &Scope{
				Magic: DropTable,
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
			return c.compileQuery(pn.GetDdl().GetQuery())
			// 1、not supported: show arnings/errors/status/processlist
			// 2、show variables will not return query
			// 3、show create database/table need rewrite to create sql
		}
	case *plan.Plan_Ins:
		return &Scope{
			Magic: InsertValues,
			Plan:  pn,
		}, nil
	}
	return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("query '%s' not support now", pn))
}

func (c *Compile) compileQuery(qry *plan.Query) (*Scope, error) {
	if len(qry.Steps) != 1 {
		return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("query '%s' not support now", qry))
	}
	ss, err := c.compilePlanScope(qry.Nodes[qry.Steps[0]], qry.Nodes)
	if err != nil {
		return nil, err
	}
	rs := c.newMergeScope(ss)
	switch qry.StmtType {
	case plan.Query_DELETE:
		rs.Magic = Deletion
	case plan.Query_INSERT:
		rs.Magic = Insert
	case plan.Query_UPDATE:
		rs.Magic = Update
	default:
	}
	switch qry.StmtType {
	case plan.Query_DELETE:
		scp, err := constructDeletion(qry.Nodes[qry.Steps[0]], c.e, c.proc.Snapshot)
		if err != nil {
			return nil, err
		}
		rs.Instructions = append(rs.Instructions, vm.Instruction{
			Op:  vm.Deletion,
			Arg: scp,
		})
	case plan.Query_INSERT:
		arg, err := constructInsert(qry.Nodes[qry.Steps[0]], c.e, c.proc.Snapshot)
		if err != nil {
			return nil, err
		}
		rs.Instructions = append(rs.Instructions, vm.Instruction{
			Op:  vm.Insert,
			Arg: arg,
		})
	case plan.Query_UPDATE:
		scp, err := constructUpdate(qry.Nodes[qry.Steps[0]], c.e, c.proc.Snapshot)
		if err != nil {
			return nil, err
		}
		rs.Instructions = append(rs.Instructions, vm.Instruction{
			Op:  vm.Update,
			Arg: scp,
		})
	default:
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

func (c *Compile) compilePlanScope(n *plan.Node, ns []*plan.Node) ([]*Scope, error) {
	switch n.NodeType {
	case plan.Node_VALUE_SCAN:
		ds := &Scope{Magic: Normal}
		ds.Proc = process.NewFromProc(c.proc, 0)
		bat := batch.NewWithSize(1)
		{
			bat.Vecs[0] = vector.NewConst(types.Type{Oid: types.T_int64}, 1)
			bat.Vecs[0].Col = make([]int64, 1)
			bat.InitZsOne(1)
		}
		ds.DataSource = &Source{Bat: bat}
		return c.compileSort(n, c.compileProjection(n, []*Scope{ds})), nil
	case plan.Node_TABLE_SCAN:
		ss := c.compileTableScan(n)
		return c.compileSort(n, c.compileProjection(n, c.compileRestrict(n, ss))), nil
	case plan.Node_FILTER:
		ss, err := c.compilePlanScope(ns[n.Children[0]], ns)
		if err != nil {
			return nil, err
		}
		return c.compileSort(n, c.compileProjection(n, c.compileRestrict(n, ss))), nil
	case plan.Node_PROJECT:
		ss, err := c.compilePlanScope(ns[n.Children[0]], ns)
		if err != nil {
			return nil, err
		}
		return c.compileSort(n, c.compileProjection(n, c.compileRestrict(n, ss))), nil
	case plan.Node_AGG:
		ss, err := c.compilePlanScope(ns[n.Children[0]], ns)
		if err != nil {
			return nil, err
		}
		if len(n.GroupBy) == 0 {
			ss = c.compileAgg(n, ss, ns)
		} else {
			ss = c.compileGroup(n, ss, ns)
		}
		rewriteExprListForAggNode(n.FilterList, int32(len(n.GroupBy)))
		rewriteExprListForAggNode(n.ProjectList, int32(len(n.GroupBy)))
		return c.compileSort(n, c.compileProjection(n, c.compileRestrict(n, ss))), nil
	case plan.Node_JOIN:
		needSwap, joinTyp := joinType(n, ns)
		ss, err := c.compilePlanScope(ns[n.Children[0]], ns)
		if err != nil {
			return nil, err
		}
		children, err := c.compilePlanScope(ns[n.Children[1]], ns)
		if err != nil {
			return nil, err
		}
		if needSwap {
			return c.compileSort(n, c.compileJoin(n, ns[n.Children[0]], children, ss, joinTyp)), nil
		}
		return c.compileSort(n, c.compileJoin(n, ns[n.Children[1]], ss, children, joinTyp)), nil
	case plan.Node_SORT:
		ss, err := c.compilePlanScope(ns[n.Children[0]], ns)
		if err != nil {
			return nil, err
		}
		ss = c.compileSort(n, ss)
		return c.compileProjection(n, c.compileRestrict(n, ss)), nil
	case plan.Node_DELETE:
		if n.DeleteTablesCtx[0].CanTruncate {
			return nil, nil
		}
		ss, err := c.compilePlanScope(ns[n.Children[0]], ns)
		if err != nil {
			return nil, err
		}
		return ss, nil
	case plan.Node_INSERT:
		ss, err := c.compilePlanScope(ns[n.Children[0]], ns)
		if err != nil {
			return nil, err
		}
		return c.compileProjection(n, c.compileRestrict(n, ss)), nil
	case plan.Node_UPDATE:
		ss, err := c.compilePlanScope(ns[n.Children[0]], ns)
		if err != nil {
			return nil, err
		}
		return ss, nil
	default:
		return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("query '%s' not support now", n))
	}
}

func (c *Compile) compileTableScan(n *plan.Node) []*Scope {
	ss := make([]*Scope, 0, len(c.cnList))
	if len(c.cnList) == 0 {
		ss = append(ss, c.compileTableScanWithNode(n, engine.Node{Mcpu: c.NumCPU()}))
		return ss
	}
	for i := range c.cnList {
		ss = append(ss, c.compileTableScanWithNode(n, c.cnList[i]))
	}
	return ss
}

func (c *Compile) compileTableScanWithNode(n *plan.Node, node engine.Node) *Scope {
	var s *Scope

	attrs := make([]string, len(n.TableDef.Cols))
	for j, col := range n.TableDef.Cols {
		attrs[j] = col.Name
	}
	s = &Scope{
		Magic:    Remote,
		NodeInfo: node,
		DataSource: &Source{
			Attributes:   attrs,
			RelationName: n.TableDef.Name,
			SchemaName:   n.ObjRef.SchemaName,
		},
	}
	s.Proc = process.NewFromProc(c.proc, 0)
	return s
}

func (c *Compile) compileRestrict(n *plan.Node, ss []*Scope) []*Scope {
	if len(n.FilterList) == 0 {
		return ss
	}
	for i := range ss {
		ss[i].appendInstruction(vm.Instruction{
			Op:  vm.Restrict,
			Arg: constructRestrict(n),
		})
	}
	return ss
}

func (c *Compile) compileProjection(n *plan.Node, ss []*Scope) []*Scope {
	for i := range ss {
		ss[i].appendInstruction(vm.Instruction{
			Op:  vm.Projection,
			Arg: constructProjection(n),
		})
	}
	return ss
}

func (c *Compile) compileJoin(n, right *plan.Node, ss []*Scope, children []*Scope, joinTyp plan.Node_JoinFlag) []*Scope {
	rs, chp := c.newJoinScopeList(ss, children)
	isEq := isEquiJoin(n.OnList)
	switch joinTyp {
	case plan.Node_INNER:
		if len(n.OnList) == 0 {
			for i := range rs {
				rs[i].appendInstruction(vm.Instruction{
					Op:  vm.Product,
					Arg: constructProduct(n, c.proc),
				})
			}
		} else {
			for i := range rs {
				if isEq {
					rs[i].appendInstruction(vm.Instruction{
						Op:  vm.Join,
						Arg: constructJoin(n, c.proc),
					})
				} else {
					rs[i].appendInstruction(vm.Instruction{
						Op:  vm.LoopJoin,
						Arg: constructLoopJoin(n, c.proc),
					})
				}
			}
		}
	case plan.Node_SEMI:
		for i := range rs {
			if isEq {
				rs[i].appendInstruction(vm.Instruction{
					Op:  vm.Semi,
					Arg: constructSemi(n, c.proc),
				})
			} else {
				rs[i].appendInstruction(vm.Instruction{
					Op:  vm.LoopSemi,
					Arg: constructLoopSemi(n, c.proc),
				})
			}
		}
	case plan.Node_LEFT:
		typs := make([]types.Type, len(right.ProjectList))
		for i, expr := range right.ProjectList {
			typs[i] = dupType(expr.Typ)
		}
		for i := range rs {
			if isEq {
				rs[i].appendInstruction(vm.Instruction{
					Op:  vm.Left,
					Arg: constructLeft(n, typs, c.proc),
				})
			} else {
				rs[i].appendInstruction(vm.Instruction{
					Op:  vm.LoopLeft,
					Arg: constructLoopLeft(n, typs, c.proc),
				})
			}
		}
	case plan.Node_SINGLE:
		typs := make([]types.Type, len(right.ProjectList))
		for i, expr := range right.ProjectList {
			typs[i] = dupType(expr.Typ)
		}
		for i := range rs {
			if isEq {
				rs[i].appendInstruction(vm.Instruction{
					Op:  vm.Single,
					Arg: constructSingle(n, typs, c.proc),
				})
			} else {
				rs[i].appendInstruction(vm.Instruction{
					Op:  vm.LoopSingle,
					Arg: constructLoopSingle(n, typs, c.proc),
				})
			}
		}
	case plan.Node_ANTI:
		for i := range rs {
			if isEq && len(n.OnList) == 1 {
				rs[i].appendInstruction(vm.Instruction{
					Op:  vm.Anti,
					Arg: constructAnti(n, c.proc),
				})
			} else {
				rs[i].appendInstruction(vm.Instruction{
					Op:  vm.LoopAnti,
					Arg: constructLoopAnti(n, c.proc),
				})
			}
		}
	default:
		panic(errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("join typ '%v' not support now", n.JoinType)))
	}
	return append(rs, chp)
}

func (c *Compile) compileSort(n *plan.Node, ss []*Scope) []*Scope {
	switch {
	case n.Limit != nil && n.Offset == nil && len(n.OrderBy) > 0: // top
		return c.compileTop(n, ss)
	case n.Limit == nil && n.Offset == nil && len(n.OrderBy) > 0: // top
		return c.compileOrder(n, ss)
	case n.Limit == nil && n.Offset != nil && len(n.OrderBy) > 0: // order and offset
		return c.compileOffset(n, c.compileOrder(n, ss))
	case n.Limit != nil && n.Offset != nil && len(n.OrderBy) > 0: // order and offset and limit
		return c.compileLimit(n, c.compileOffset(n, c.compileOrder(n, ss)))
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

func (c *Compile) compileTop(n *plan.Node, ss []*Scope) []*Scope {
	for i := range ss {
		ss[i].appendInstruction(vm.Instruction{
			Op:  vm.Top,
			Arg: constructTop(n, c.proc),
		})
	}
	rs := c.newMergeScope(ss)
	rs.Instructions[0] = vm.Instruction{
		Op:  vm.MergeTop,
		Arg: constructMergeTop(n, c.proc),
	}
	return []*Scope{rs}
}

func (c *Compile) compileOrder(n *plan.Node, ss []*Scope) []*Scope {
	for i := range ss {
		ss[i].appendInstruction(vm.Instruction{
			Op:  vm.Order,
			Arg: constructOrder(n, c.proc),
		})
	}
	rs := c.newMergeScope(ss)
	rs.Instructions[0] = vm.Instruction{
		Op:  vm.MergeOrder,
		Arg: constructMergeOrder(n, c.proc),
	}
	return []*Scope{rs}
}

func (c *Compile) compileOffset(n *plan.Node, ss []*Scope) []*Scope {
	rs := c.newMergeScope(ss)
	rs.Instructions[0] = vm.Instruction{
		Op:  vm.MergeOffset,
		Arg: constructMergeOffset(n, c.proc),
	}
	return []*Scope{rs}
}

func (c *Compile) compileLimit(n *plan.Node, ss []*Scope) []*Scope {
	for i := range ss {
		ss[i].appendInstruction(vm.Instruction{
			Op:  vm.Limit,
			Arg: constructLimit(n, c.proc),
		})
	}
	rs := c.newMergeScope(ss)
	rs.Instructions[0] = vm.Instruction{
		Op:  vm.MergeLimit,
		Arg: constructMergeLimit(n, c.proc),
	}
	return []*Scope{rs}
}

func (c *Compile) compileAgg(n *plan.Node, ss []*Scope, ns []*plan.Node) []*Scope {
	for i := range ss {
		ss[i].appendInstruction(vm.Instruction{
			Op:  vm.Group,
			Arg: constructGroup(n, ns[n.Children[0]], 0, 0),
		})
	}
	rs := c.newMergeScope(ss)
	rs.Instructions[0] = vm.Instruction{
		Op:  vm.MergeGroup,
		Arg: constructMergeGroup(n, true),
	}
	return []*Scope{rs}
}

func (c *Compile) compileGroup(n *plan.Node, ss []*Scope, ns []*plan.Node) []*Scope {
	rs := c.newGroupScopeList(validScopeCount(ss))
	regs := extraGroupRegisters(rs)
	for i := range ss {
		if !ss[i].IsEnd {
			ss[i].appendInstruction(vm.Instruction{
				Op:  vm.Dispatch,
				Arg: constructDispatch(true, regs),
			})
			ss[i].IsEnd = true
		}
	}
	for i := range rs {
		rs[i].Instructions = append(rs[i].Instructions, vm.Instruction{
			Op:  vm.Group,
			Arg: constructGroup(n, ns[n.Children[0]], i, len(rs)),
		})
	}
	return []*Scope{c.newMergeScope(append(rs, ss...))}
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
	rs.Proc = process.NewFromProc(c.proc, cnt)
	rs.Instructions = append(rs.Instructions, vm.Instruction{
		Op:  vm.Merge,
		Arg: &merge.Argument{},
	})
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

func (c *Compile) newGroupScopeList(childrenCount int) []*Scope {
	var ss []*Scope

	if len(c.cnList) == 0 {
		return c.newGroupScopeListWithNode(c.NumCPU(), childrenCount)
	}
	for _, n := range c.cnList {
		ss = append(ss, c.newGroupScopeListWithNode(n.Mcpu, childrenCount)...)
	}
	return ss
}

func (c *Compile) newGroupScopeListWithNode(mcpu, childrenCount int) []*Scope {
	ss := make([]*Scope, mcpu)
	for i := range ss {
		ss[i] = new(Scope)
		ss[i].Magic = Remote
		ss[i].Proc = process.NewFromProc(c.proc, childrenCount)
		ss[i].Instructions = append(ss[i].Instructions, vm.Instruction{
			Op:  vm.Merge,
			Arg: &merge.Argument{},
		})
	}
	return ss
}

func (c *Compile) newJoinScopeList(ss []*Scope, children []*Scope) ([]*Scope, *Scope) {
	regs := make([]*process.WaitRegister, 0, len(ss))
	chp := c.newMergeScope(children)
	chp.IsEnd = true
	rs := make([]*Scope, len(ss))
	for i := range ss {
		if ss[i].IsEnd {
			rs[i] = ss[i]
			continue
		}
		rs[i] = new(Scope)
		rs[i].Magic = Merge
		rs[i].PreScopes = []*Scope{ss[i]}
		rs[i].Proc = process.NewFromProc(c.proc, 2)
		regs = append(regs, rs[i].Proc.Reg.MergeReceivers[1])
		ss[i].appendInstruction(vm.Instruction{
			Op: vm.Connector,
			Arg: &connector.Argument{
				Reg: rs[i].Proc.Reg.MergeReceivers[0],
			},
		})
	}
	chp.Instructions = append(chp.Instructions, vm.Instruction{
		Op:  vm.Dispatch,
		Arg: constructDispatch(true, regs),
	})
	return rs, chp
}

// Number of cpu's available on the current machine
func (c *Compile) NumCPU() int {
	return runtime.NumCPU()
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

func extraGroupRegisters(ss []*Scope) []*process.WaitRegister {
	var regs []*process.WaitRegister

	for _, s := range ss {
		if s.IsEnd {
			continue
		}
		regs = append(regs, s.Proc.Reg.MergeReceivers...)
	}
	return regs
}

func rewriteExprListForAggNode(es []*plan.Expr, groupSize int32) {
	for i := range es {
		rewriteExprForAggNode(es[i], groupSize)
	}
}

func rewriteExprForAggNode(expr *plan.Expr, groupSize int32) {
	switch e := expr.Expr.(type) {
	case *plan.Expr_Col:
		if e.Col.RelPos == -2 {
			e.Col.ColPos += groupSize
		}
	case *plan.Expr_F:
		for i := range e.F.Args {
			rewriteExprForAggNode(e.F.Args[i], groupSize)
		}
	default:
		return
	}
}

func joinType(n *plan.Node, ns []*plan.Node) (bool, plan.Node_JoinFlag) {
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
	default:
		panic(errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("join typ '%v' not support now", n.JoinType)))
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
