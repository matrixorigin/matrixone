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
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// InitAddress is used to set address of local node
func InitAddress(addr string) {
	Address = addr
}

// New is used to new an object of compile
func New(db string, sql string, uid string,
	e engine.Engine, proc *process.Process) *Compile {
	return &Compile{
		e:    e,
		db:   db,
		uid:  uid,
		sql:  sql,
		proc: proc,
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
	var rs *Scope
	switch qry.StmtType {
	case plan.Query_DELETE:
		rs = &Scope{
			PreScopes: ss,
			Magic:     Deletion,
		}
	case plan.Query_INSERT:
		// needn't do merge work
		rs = &Scope{
			PreScopes: ss,
			Magic:     Insert,
		}
	case plan.Query_UPDATE:
		rs = &Scope{
			PreScopes: ss,
			Magic:     Update,
		}
	default:
		rs = &Scope{
			PreScopes: ss,
			Magic:     Merge,
		}
	}

	rs.Proc = process.NewFromProc(mheap.New(c.proc.Mp.Gm), c.proc, len(ss))
	rs.Instructions = append(rs.Instructions, vm.Instruction{
		Op:  vm.Merge,
		Arg: &merge.Argument{},
	})
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

	for i := range ss {
		ss[i].Instructions = append(ss[i].Instructions, vm.Instruction{
			Op: vm.Connector,
			Arg: &connector.Argument{
				Mmu: rs.Proc.Mp.Gm,
				Reg: rs.Proc.Reg.MergeReceivers[i],
			},
		})
	}
	return rs, nil
}

func (c *Compile) compilePlanScope(n *plan.Node, ns []*plan.Node) ([]*Scope, error) {
	switch n.NodeType {
	case plan.Node_VALUE_SCAN:
		ds := &Scope{Magic: Normal}
		ds.Proc = process.NewFromProc(mheap.New(c.proc.Mp.Gm), c.proc, 0)
		bat := batch.NewWithSize(1)
		{
			bat.Vecs[0] = vector.NewConst(types.Type{Oid: types.T_int64}, 1)
			bat.Vecs[0].Col = make([]int64, 1)
			bat.InitZsOne(1)
		}
		ds.DataSource = &Source{Bat: bat}
		return c.compileSort(n, c.compileProjection(n, []*Scope{ds})), nil
	case plan.Node_TABLE_SCAN:
		src := &Source{
			RelationName: n.TableDef.Name,
			SchemaName:   n.ObjRef.SchemaName,
			Attributes:   make([]string, len(n.TableDef.Cols)),
		}
		for i, col := range n.TableDef.Cols {
			src.Attributes[i] = col.Name
		}
		nodes := make([]engine.Node, 1)
		ss := make([]*Scope, len(nodes))
		for i := range nodes {
			ss[i] = &Scope{
				DataSource: src,
				Magic:      Remote,
				NodeInfo:   nodes[i],
			}
			ss[i].Proc = process.NewFromProc(mheap.New(c.proc.Mp.Gm), c.proc, 0)
		}
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
		ss = c.compileGroup(n, ss, ns)
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
			return c.compileSort(n, c.compileJoin(n, children, ss, joinTyp)), nil
		}
		return c.compileSort(n, c.compileJoin(n, ss, children, joinTyp)), nil
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

func (c *Compile) compileRestrict(n *plan.Node, ss []*Scope) []*Scope {
	if len(n.FilterList) == 0 {
		return ss
	}
	for i := range ss {
		ss[i].Instructions = append(ss[i].Instructions, vm.Instruction{
			Op:  vm.Restrict,
			Arg: constructRestrict(n),
		})
	}
	return ss
}

func (c *Compile) compileProjection(n *plan.Node, ss []*Scope) []*Scope {
	for i := range ss {
		ss[i].Instructions = append(ss[i].Instructions, vm.Instruction{
			Op:  vm.Projection,
			Arg: constructProjection(n),
		})
	}
	return ss
}

func (c *Compile) compileJoin(n *plan.Node, ss []*Scope, children []*Scope, joinTyp plan.Node_JoinFlag) []*Scope {
	rs := make([]*Scope, len(ss))
	for i := range ss {
		chp := &Scope{
			PreScopes:   children,
			Magic:       Merge,
			DispatchAll: true,
		}
		{ // build merge scope for children
			chp.Proc = process.NewFromProc(mheap.New(c.proc.Mp.Gm), c.proc, len(children))
			for j := range children {
				children[j].Instructions = append(children[j].Instructions, vm.Instruction{
					Op: vm.Connector,
					Arg: &connector.Argument{
						Mmu: chp.Proc.Mp.Gm,
						Reg: chp.Proc.Reg.MergeReceivers[j],
					},
				})
			}
			chp.Instructions = append(chp.Instructions, vm.Instruction{
				Op:  vm.Merge,
				Arg: &merge.Argument{},
			})
		}
		rs[i] = &Scope{
			Magic:     Remote,
			PreScopes: []*Scope{ss[i], chp},
		}
		rs[i].Proc = process.NewFromProc(mheap.New(c.proc.Mp.Gm), c.proc, 2)
		ss[i].Instructions = append(ss[i].Instructions, vm.Instruction{
			Op: vm.Connector,
			Arg: &connector.Argument{
				Mmu: rs[i].Proc.Mp.Gm,
				Reg: rs[i].Proc.Reg.MergeReceivers[0],
			},
		})
		chp.Instructions = append(chp.Instructions, vm.Instruction{
			Op: vm.Connector,
			Arg: &connector.Argument{
				Mmu: rs[i].Proc.Mp.Gm,
				Reg: rs[i].Proc.Reg.MergeReceivers[1],
			},
		})
	}
	isEq := isEquiJoin(n.OnList)
	switch joinTyp {
	case plan.Node_INNER:
		if len(n.OnList) == 0 {
			for i := range rs {
				rs[i].Instructions = append(rs[i].Instructions, vm.Instruction{
					Op:  vm.Product,
					Arg: constructProduct(n, c.proc),
				})
			}
		} else {
			for i := range rs {
				if isEq {
					rs[i].Instructions = append(rs[i].Instructions, vm.Instruction{
						Op:  vm.Join,
						Arg: constructJoin(n, c.proc),
					})
				} else {
					rs[i].Instructions = append(rs[i].Instructions, vm.Instruction{
						Op:  vm.LoopJoin,
						Arg: constructLoopJoin(n, c.proc),
					})
				}
			}
		}
	case plan.Node_SEMI:
		for i := range rs {
			if isEq {
				rs[i].Instructions = append(rs[i].Instructions, vm.Instruction{
					Op:  vm.Semi,
					Arg: constructSemi(n, c.proc),
				})
			} else {
				rs[i].Instructions = append(rs[i].Instructions, vm.Instruction{
					Op:  vm.LoopSemi,
					Arg: constructLoopSemi(n, c.proc),
				})
			}
		}
	case plan.Node_LEFT:
		for i := range rs {
			if isEq {
				rs[i].Instructions = append(rs[i].Instructions, vm.Instruction{
					Op:  vm.Left,
					Arg: constructLeft(n, c.proc),
				})
			} else {
				rs[i].Instructions = append(rs[i].Instructions, vm.Instruction{
					Op:  vm.LoopLeft,
					Arg: constructLoopLeft(n, c.proc),
				})
			}
		}
	case plan.Node_ANTI:
		for i := range rs {
			if isEq && len(n.OnList) == 1 {
				rs[i].Instructions = append(rs[i].Instructions, vm.Instruction{
					Op:  vm.Complement,
					Arg: constructComplement(n, c.proc),
				})
			} else {
				rs[i].Instructions = append(rs[i].Instructions, vm.Instruction{
					Op:  vm.LoopComplement,
					Arg: constructLoopComplement(n, c.proc),
				})
			}
		}
	default:
		panic(errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("join typ '%v' not support now", n.JoinType)))
	}
	return rs
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
		ss[i].Instructions = append(ss[i].Instructions, vm.Instruction{
			Op:  vm.Top,
			Arg: constructTop(n, c.proc),
		})
	}
	rs := &Scope{
		PreScopes: ss,
		Magic:     Merge,
	}
	rs.Proc = process.NewFromProc(mheap.New(c.proc.Mp.Gm), c.proc, len(ss))
	rs.Instructions = append(rs.Instructions, vm.Instruction{
		Op:  vm.MergeTop,
		Arg: constructMergeTop(n, c.proc),
	})

	for i := range ss {
		ss[i].Instructions = append(ss[i].Instructions, vm.Instruction{
			Op: vm.Connector,
			Arg: &connector.Argument{
				Mmu: rs.Proc.Mp.Gm,
				Reg: rs.Proc.Reg.MergeReceivers[i],
			},
		})
	}
	return []*Scope{rs}
}

func (c *Compile) compileOrder(n *plan.Node, ss []*Scope) []*Scope {
	for i := range ss {
		ss[i].Instructions = append(ss[i].Instructions, vm.Instruction{
			Op:  vm.Order,
			Arg: constructOrder(n, c.proc),
		})
	}
	rs := &Scope{
		PreScopes: ss,
		Magic:     Merge,
	}
	rs.Proc = process.NewFromProc(mheap.New(c.proc.Mp.Gm), c.proc, len(ss))
	rs.Instructions = append(rs.Instructions, vm.Instruction{
		Op:  vm.MergeOrder,
		Arg: constructMergeOrder(n, c.proc),
	})

	for i := range ss {
		ss[i].Instructions = append(ss[i].Instructions, vm.Instruction{
			Op: vm.Connector,
			Arg: &connector.Argument{
				Mmu: rs.Proc.Mp.Gm,
				Reg: rs.Proc.Reg.MergeReceivers[i],
			},
		})
	}
	return []*Scope{rs}
}

func (c *Compile) compileOffset(n *plan.Node, ss []*Scope) []*Scope {
	rs := &Scope{
		PreScopes: ss,
		Magic:     Merge,
	}
	rs.Proc = process.NewFromProc(mheap.New(c.proc.Mp.Gm), c.proc, len(ss))
	rs.Instructions = append(rs.Instructions, vm.Instruction{
		Op:  vm.MergeOffset,
		Arg: constructMergeOffset(n, c.proc),
	})

	for i := range ss {
		ss[i].Instructions = append(ss[i].Instructions, vm.Instruction{
			Op: vm.Connector,
			Arg: &connector.Argument{
				Mmu: rs.Proc.Mp.Gm,
				Reg: rs.Proc.Reg.MergeReceivers[i],
			},
		})
	}
	return []*Scope{rs}
}

func (c *Compile) compileLimit(n *plan.Node, ss []*Scope) []*Scope {
	for i := range ss {
		ss[i].Instructions = append(ss[i].Instructions, vm.Instruction{
			Op:  vm.Limit,
			Arg: constructLimit(n, c.proc),
		})
	}
	rs := &Scope{
		PreScopes: ss,
		Magic:     Merge,
	}
	rs.Proc = process.NewFromProc(mheap.New(c.proc.Mp.Gm), c.proc, len(ss))
	rs.Instructions = append(rs.Instructions, vm.Instruction{
		Op:  vm.MergeLimit,
		Arg: constructMergeLimit(n, c.proc),
	})

	for i := range ss {
		ss[i].Instructions = append(ss[i].Instructions, vm.Instruction{
			Op: vm.Connector,
			Arg: &connector.Argument{
				Mmu: rs.Proc.Mp.Gm,
				Reg: rs.Proc.Reg.MergeReceivers[i],
			},
		})
	}
	return []*Scope{rs}
}

func (c *Compile) compileGroup(n *plan.Node, ss []*Scope, ns []*plan.Node) []*Scope {
	for i := range ss {
		ss[i].Instructions = append(ss[i].Instructions, vm.Instruction{
			Op:  vm.Group,
			Arg: constructGroup(n, ns[n.Children[0]]),
		})
	}
	rs := &Scope{
		PreScopes: ss,
		Magic:     Merge,
	}
	rs.Proc = process.NewFromProc(mheap.New(c.proc.Mp.Gm), c.proc, len(ss))
	rs.Instructions = append(rs.Instructions, vm.Instruction{
		Op:  vm.MergeGroup,
		Arg: constructMergeGroup(n, true),
	})

	for i := range ss {
		ss[i].Instructions = append(ss[i].Instructions, vm.Instruction{
			Op: vm.Connector,
			Arg: &connector.Argument{
				Mmu: rs.Proc.Mp.Gm,
				Reg: rs.Proc.Reg.MergeReceivers[i],
			},
		})
	}
	return []*Scope{rs}
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
	default:
		panic(errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("join typ '%v' not support now", n.JoinType)))
	}
}
