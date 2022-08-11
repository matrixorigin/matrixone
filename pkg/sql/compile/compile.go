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
	"fmt"
	"runtime"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/errno"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/connector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/merge"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/output"
	"github.com/matrixorigin/matrixone/pkg/sql/errors"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// New is used to new an object of compile
func New(db string, sql string, uid string, ctx context.Context,
	e engine.Engine, proc *process.Process, stmt tree.Statement) *Compile {
	return &Compile{
		e:    e,
		db:   db,
		ctx:  ctx,
		uid:  uid,
		sql:  sql,
		proc: proc,
		stmt: stmt,
	}
}

// Compile is the entrance of the compute-layer, it compiles AST tree to scope list.
// A scope is an execution unit.
func (c *Compile) Compile(pn *plan.Plan, u any, fill func(any, *batch.Batch) error) (err error) {
	defer func() {
		if e := recover(); e != nil {
			err = moerr.NewPanicError(e)
		}
	}()
	c.u = u
	c.fill = fill
	c.info = plan2.GetExecTypeFromPlan(pn)
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
func (c *Compile) Run(_ uint64) (err error) {
	if c.scope == nil {
		return nil
	}

	//PrintScope(nil, []*Scope{c.scope})

	switch c.scope.Magic {
	case Normal:
		defer c.fillAnalyzeInfo()
		return c.scope.Run(c)
	case Merge:
		defer c.fillAnalyzeInfo()
		return c.scope.MergeRun(c)
	case Remote:
		defer c.fillAnalyzeInfo()
		return c.scope.RemoteRun(c)
	case CreateDatabase:
		return c.scope.CreateDatabase(c)
	case DropDatabase:
		return c.scope.DropDatabase(c)
	case CreateTable:
		return c.scope.CreateTable(c)
	case DropTable:
		return c.scope.DropTable(c)
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
	case InsertValues:
		affectedRows, err := c.scope.InsertValues(c, c.stmt.(*tree.Insert))
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
	var err error
	c.cnList, err = c.e.Nodes()
	if err != nil {
		return nil, err
	}
	if c.info.Typ == plan2.ExecTypeTP {
		c.cnList = engine.Nodes{engine.Node{Mcpu: 1}}
	} else {
		if len(c.cnList) == 0 {
			c.cnList = append(c.cnList, engine.Node{Mcpu: c.NumCPU()})
		} else if len(c.cnList) > c.info.CnNumbers {
			c.cnList = c.cnList[:c.info.CnNumbers]
		}
	}
	c.initAnalyze(qry)
	ss, err := c.compilePlanScope(qry.Nodes[qry.Steps[0]], qry.Nodes)
	if err != nil {
		return nil, err
	}
	if c.info.Typ == plan2.ExecTypeTP {
		return c.compileTpQuery(qry, ss)
	}
	return c.compileApQuery(qry, ss)
}

func (c *Compile) compileTpQuery(qry *plan.Query, ss []*Scope) (*Scope, error) {
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

func (c *Compile) compileApQuery(qry *plan.Query, ss []*Scope) (*Scope, error) {
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
		ds.Proc = process.NewWithAnalyze(c.proc, c.ctx, 0, c.anal.Nodes())
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
		curr := c.anal.curr
		c.anal.curr = int(n.Children[0])
		ss, err := c.compilePlanScope(ns[n.Children[0]], ns)
		if err != nil {
			return nil, err
		}
		c.anal.curr = curr
		return c.compileSort(n, c.compileProjection(n, c.compileRestrict(n, ss))), nil
	case plan.Node_PROJECT:
		curr := c.anal.curr
		c.anal.curr = int(n.Children[0])
		ss, err := c.compilePlanScope(ns[n.Children[0]], ns)
		if err != nil {
			return nil, err
		}
		c.anal.curr = curr
		return c.compileSort(n, c.compileProjection(n, c.compileRestrict(n, ss))), nil
	case plan.Node_AGG:
		curr := c.anal.curr
		c.anal.curr = int(n.Children[0])
		ss, err := c.compilePlanScope(ns[n.Children[0]], ns)
		if err != nil {
			return nil, err
		}
		c.anal.curr = curr
		if len(n.GroupBy) == 0 || !c.info.WithBigMem {
			ss = c.compileAgg(n, ss, ns)
		} else {
			ss = c.compileGroup(n, ss, ns)
		}
		rewriteExprListForAggNode(n.FilterList, int32(len(n.GroupBy)))
		rewriteExprListForAggNode(n.ProjectList, int32(len(n.GroupBy)))
		return c.compileSort(n, c.compileProjection(n, c.compileRestrict(n, ss))), nil
	case plan.Node_JOIN:
		needSwap, joinTyp := joinType(n, ns)
		curr := c.anal.curr
		c.anal.curr = int(n.Children[0])
		ss, err := c.compilePlanScope(ns[n.Children[0]], ns)
		if err != nil {
			return nil, err
		}
		c.anal.curr = int(n.Children[1])
		children, err := c.compilePlanScope(ns[n.Children[1]], ns)
		if err != nil {
			return nil, err
		}
		c.anal.curr = curr
		if needSwap {
			return c.compileSort(n, c.compileJoin(n, ns[n.Children[0]], children, ss, joinTyp)), nil
		}
		return c.compileSort(n, c.compileJoin(n, ns[n.Children[1]], ss, children, joinTyp)), nil
	case plan.Node_SORT:
		curr := c.anal.curr
		c.anal.curr = int(n.Children[0])
		ss, err := c.compilePlanScope(ns[n.Children[0]], ns)
		if err != nil {
			return nil, err
		}
		c.anal.curr = curr
		ss = c.compileSort(n, ss)
		return c.compileProjection(n, c.compileRestrict(n, ss)), nil
	case plan.Node_UNION:
		curr := c.anal.curr
		c.anal.curr = int(n.Children[0])
		ss, err := c.compilePlanScope(ns[n.Children[0]], ns)
		if err != nil {
			return nil, err
		}
		c.anal.curr = int(n.Children[1])
		children, err := c.compilePlanScope(ns[n.Children[1]], ns)
		if err != nil {
			return nil, err
		}
		c.anal.curr = curr
		return c.compileSort(n, c.compileUnion(n, ss, children, ns)), nil
	case plan.Node_MINUS:
		curr := c.anal.curr
		c.anal.curr = int(n.Children[0])
		ss, err := c.compilePlanScope(ns[n.Children[0]], ns)
		if err != nil {
			return nil, err
		}
		c.anal.curr = int(n.Children[1])
		children, err := c.compilePlanScope(ns[n.Children[1]], ns)
		if err != nil {
			return nil, err
		}
		c.anal.curr = curr
		return c.compileSort(n, c.compileMinusAndIntersect(n, ss, children, n.NodeType)), nil
	case plan.Node_UNION_ALL:
		curr := c.anal.curr
		c.anal.curr = int(n.Children[0])
		ss, err := c.compilePlanScope(ns[n.Children[0]], ns)
		if err != nil {
			return nil, err
		}
		c.anal.curr = int(n.Children[1])
		children, err := c.compilePlanScope(ns[n.Children[1]], ns)
		if err != nil {
			return nil, err
		}
		c.anal.curr = curr
		return c.compileSort(n, c.compileUnionAll(n, ss, children)), nil
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
	s.Proc = process.NewWithAnalyze(c.proc, c.ctx, 0, c.anal.Nodes())
	return s
}

func (c *Compile) compileRestrict(n *plan.Node, ss []*Scope) []*Scope {
	if len(n.FilterList) == 0 {
		return ss
	}
	for i := range ss {
		ss[i].appendInstruction(vm.Instruction{
			Op:  vm.Restrict,
			Idx: c.anal.curr,
			Arg: constructRestrict(n),
		})
	}
	return ss
}

func (c *Compile) compileProjection(n *plan.Node, ss []*Scope) []*Scope {
	for i := range ss {
		ss[i].appendInstruction(vm.Instruction{
			Op:  vm.Projection,
			Idx: c.anal.curr,
			Arg: constructProjection(n),
		})
	}
	return ss
}

func (c *Compile) compileUnion(n *plan.Node, ss []*Scope, children []*Scope, ns []*plan.Node) []*Scope {
	ss = append(ss, children...)
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
	gn := new(plan.Node)
	gn.GroupBy = make([]*plan.Expr, len(n.ProjectList))
	copy(gn.GroupBy, n.ProjectList)
	for i := range rs {
		rs[i].Instructions = append(rs[i].Instructions, vm.Instruction{
			Op:  vm.Group,
			Idx: c.anal.curr,
			Arg: constructGroup(gn, n, i, len(rs), true),
		})
	}
	return []*Scope{c.newMergeScope(append(rs, ss...))}
}

func (c *Compile) compileMinusAndIntersect(n *plan.Node, ss []*Scope, children []*Scope, nodeType plan.Node_NodeType) []*Scope {
	rs, left, right := c.newJoinScopeListWithBucket(c.newGroupScopeList(2), ss, children)
	switch nodeType {
	case plan.Node_MINUS:
		for i := range rs {
			rs[i].Instructions[0] = vm.Instruction{
				Op:  vm.Minus,
				Idx: c.anal.curr,
				Arg: constructMinus(n, c.proc, i, len(rs)),
			}
		}
	}
	return []*Scope{c.newMergeScope(append(append(rs, left), right))}
}

func (c *Compile) compileUnionAll(n *plan.Node, ss []*Scope, children []*Scope) []*Scope {
	rs := c.newMergeScope(append(ss, children...))
	rs.Instructions[0].Idx = c.anal.curr
	return []*Scope{rs}
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
					Idx: c.anal.curr,
					Arg: constructProduct(n, c.proc),
				})
			}
		} else {
			for i := range rs {
				if isEq {
					rs[i].appendInstruction(vm.Instruction{
						Op:  vm.Join,
						Idx: c.anal.curr,
						Arg: constructJoin(n, c.proc),
					})
				} else {
					rs[i].appendInstruction(vm.Instruction{
						Op:  vm.LoopJoin,
						Idx: c.anal.curr,
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
					Idx: c.anal.curr,
					Arg: constructSemi(n, c.proc),
				})
			} else {
				rs[i].appendInstruction(vm.Instruction{
					Op:  vm.LoopSemi,
					Idx: c.anal.curr,
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
		typs := make([]types.Type, len(right.ProjectList))
		for i, expr := range right.ProjectList {
			typs[i] = dupType(expr.Typ)
		}
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
		for i := range rs {
			if isEq && len(n.OnList) == 1 {
				rs[i].appendInstruction(vm.Instruction{
					Op:  vm.Anti,
					Idx: c.anal.curr,
					Arg: constructAnti(n, c.proc),
				})
			} else {
				rs[i].appendInstruction(vm.Instruction{
					Op:  vm.LoopAnti,
					Idx: c.anal.curr,
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
			Idx: c.anal.curr,
			Arg: constructTop(n, c.proc),
		})
	}
	rs := c.newMergeScope(ss)
	rs.Instructions[0] = vm.Instruction{
		Op:  vm.MergeTop,
		Idx: c.anal.curr,
		Arg: constructMergeTop(n, c.proc),
	}
	return []*Scope{rs}
}

func (c *Compile) compileOrder(n *plan.Node, ss []*Scope) []*Scope {
	for i := range ss {
		ss[i].appendInstruction(vm.Instruction{
			Op:  vm.Order,
			Idx: c.anal.curr,
			Arg: constructOrder(n, c.proc),
		})
	}
	rs := c.newMergeScope(ss)
	rs.Instructions[0] = vm.Instruction{
		Op:  vm.MergeOrder,
		Idx: c.anal.curr,
		Arg: constructMergeOrder(n, c.proc),
	}
	return []*Scope{rs}
}

func (c *Compile) compileOffset(n *plan.Node, ss []*Scope) []*Scope {
	rs := c.newMergeScope(ss)
	rs.Instructions[0] = vm.Instruction{
		Op:  vm.MergeOffset,
		Idx: c.anal.curr,
		Arg: constructMergeOffset(n, c.proc),
	}
	return []*Scope{rs}
}

func (c *Compile) compileLimit(n *plan.Node, ss []*Scope) []*Scope {
	for i := range ss {
		ss[i].appendInstruction(vm.Instruction{
			Op:  vm.Limit,
			Idx: c.anal.curr,
			Arg: constructLimit(n, c.proc),
		})
	}
	rs := c.newMergeScope(ss)
	rs.Instructions[0] = vm.Instruction{
		Op:  vm.MergeLimit,
		Idx: c.anal.curr,
		Arg: constructMergeLimit(n, c.proc),
	}
	return []*Scope{rs}
}

func (c *Compile) compileAgg(n *plan.Node, ss []*Scope, ns []*plan.Node) []*Scope {
	for i := range ss {
		ss[i].appendInstruction(vm.Instruction{
			Op:  vm.Group,
			Idx: c.anal.curr,
			Arg: constructGroup(n, ns[n.Children[0]], 0, 0, false),
		})
	}
	rs := c.newMergeScope(ss)
	rs.Instructions[0] = vm.Instruction{
		Op:  vm.MergeGroup,
		Idx: c.anal.curr,
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
			Idx: c.anal.curr,
			Arg: constructGroup(n, ns[n.Children[0]], i, len(rs), true),
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
	rs.Proc = process.NewWithAnalyze(c.proc, c.ctx, cnt, c.anal.Nodes())
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
		ss[i].Proc = process.NewWithAnalyze(c.proc, c.ctx, childrenCount, c.anal.Nodes())
		ss[i].Instructions = append(ss[i].Instructions, vm.Instruction{
			Op:  vm.Merge,
			Arg: &merge.Argument{},
		})
	}
	return ss
}

func (c *Compile) newJoinScopeListWithBucket(rs, ss, children []*Scope) ([]*Scope, *Scope, *Scope) {
	left := c.newMergeScope(ss)
	right := c.newMergeScope(children)
	leftRegs := extraJoinRegisters(rs, 0)
	left.appendInstruction(vm.Instruction{
		Op:  vm.Dispatch,
		Arg: constructDispatch(true, leftRegs),
	})
	rightRegs := extraJoinRegisters(rs, 1)
	right.appendInstruction(vm.Instruction{
		Op:  vm.Dispatch,
		Arg: constructDispatch(true, rightRegs),
	})
	left.IsEnd = true
	right.IsEnd = true
	return rs, left, right
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
		rs[i].Proc = process.NewWithAnalyze(c.proc, c.ctx, 2, c.anal.Nodes())
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
	}
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

func extraJoinRegisters(ss []*Scope, i int) []*process.WaitRegister {
	regs := make([]*process.WaitRegister, 0, len(ss))
	for _, s := range ss {
		if s.IsEnd {
			continue
		}
		regs = append(regs, s.Proc.Reg.MergeReceivers[i])
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
