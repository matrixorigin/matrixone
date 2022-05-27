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

package compile2

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/errno"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec2/connector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec2/merge"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec2/output"
	"github.com/matrixorigin/matrixone/pkg/sql/errors"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
	"github.com/matrixorigin/matrixone/pkg/vm/overload"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// InitAddress is used to set address of local node
func InitAddress(addr string) {
	Address = addr
}

// New is used to new an object of compile
func New(db string, sql string, uid string,
	e engine.Engine, proc *process.Process) *compile {
	return &compile{
		e:    e,
		db:   db,
		uid:  uid,
		sql:  sql,
		proc: proc,
	}
}

// Compile is the entrance of the compute-layer, it compiles AST tree to scope list.
// A scope is an execution unit.
func (c *compile) Compile(pn *plan.Plan, u interface{}, fill func(interface{}, *batch.Batch) error) (err error) {
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

// Run is an important function of the compute-layer, it executes a single sql according to its scope
func (c *compile) Run(ts uint64) (err error) {
	defer func() {
		if e := recover(); e != nil {
			err = moerr.NewPanicError(e)
		}
	}()

	if c.scope == nil {
		return nil
	}

	PrintScope(nil, []*Scope{c.scope})

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
	}
	return nil
}

func (c *compile) compileScope(pn *plan.Plan) (*Scope, error) {
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
		}
	}
	return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("query '%s' not support now", pn))
}

func (c *compile) compileQuery(qry *plan.Query) (*Scope, error) {
	if len(qry.Steps) != 1 {
		return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("query '%s' not support now", qry))
	}
	ss, err := c.compilePlanScope(qry.Nodes[qry.Steps[0]], qry.Nodes)
	if err != nil {
		return nil, err
	}
	rs := &Scope{
		PreScopes: ss,
		Magic:     Merge,
	}
	rs.Proc = process.NewFromProc(mheap.New(c.proc.Mp.Gm), c.proc, len(ss))
	rs.Instructions = append(rs.Instructions, vm.Instruction{
		Op:  overload.Merge,
		Arg: &merge.Argument{},
	})
	rs.Instructions = append(rs.Instructions, vm.Instruction{
		Op: overload.Output,
		Arg: &output.Argument{
			Data: c.u,
			Func: c.fill,
		},
	})

	for i := range ss {
		ss[i].Instructions = append(ss[i].Instructions, vm.Instruction{
			Op: overload.Connector,
			Arg: &connector.Argument{
				Mmu: rs.Proc.Mp.Gm,
				Reg: rs.Proc.Reg.MergeReceivers[i],
			},
		})
	}
	return rs, nil
}

func (c *compile) compilePlanScope(n *plan.Node, ns []*plan.Node) ([]*Scope, error) {
	switch n.NodeType {
	//	case plan.Node_VALUE_SCAN:
	case plan.Node_TABLE_SCAN:
		snap := engine.Snapshot(c.proc.Snapshot)
		db, err := c.e.Database(n.ObjRef.SchemaName, snap)
		if err != nil {
			return nil, err
		}
		rel, err := db.Relation(n.TableDef.Name, snap)
		if err != nil {
			return nil, err
		}
		src := &Source{
			RelationName: n.TableDef.Name,
			SchemaName:   n.ObjRef.SchemaName,
			Attributes:   make([]string, len(n.TableDef.Cols)),
		}
		for i, col := range n.TableDef.Cols {
			src.Attributes[i] = col.Name
		}
		nodes := rel.Nodes(snap)
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
		ss = c.compileGroup(n, ss)
		return c.compileSort(n, c.compileProjection(n, c.compileRestrict(n, ss))), nil
	case plan.Node_JOIN:
		ss, err := c.compilePlanScope(ns[n.Children[0]], ns)
		if err != nil {
			return nil, err
		}
		children, err := c.compilePlanScope(ns[n.Children[1]], ns)
		if err != nil {
			return nil, err
		}
		return c.compileSort(n, c.compileJoin(n, ss, children)), nil
	case plan.Node_SORT:
		ss, err := c.compilePlanScope(ns[n.Children[0]], ns)
		if err != nil {
			return nil, err
		}
		ss = c.compileSort(n, ss)
		return c.compileProjection(n, c.compileRestrict(n, ss)), nil
	default:
		return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("query '%s' not support now", n))
	}
}

func (c *compile) compileRestrict(n *plan.Node, ss []*Scope) []*Scope {
	if len(n.WhereList) == 0 {
		return ss
	}
	for i := range ss {
		ss[i].Instructions = append(ss[i].Instructions, vm.Instruction{
			Op:  overload.Restrict,
			Arg: constructRestrict(n),
		})
	}
	return ss
}

func (c *compile) compileProjection(n *plan.Node, ss []*Scope) []*Scope {
	for i := range ss {
		ss[i].Instructions = append(ss[i].Instructions, vm.Instruction{
			Op:  overload.Projection,
			Arg: constructProjection(n),
		})
	}
	return ss
}

func (c *compile) compileJoin(n *plan.Node, ss []*Scope, children []*Scope) []*Scope {
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
					Op: overload.Connector,
					Arg: &connector.Argument{
						Mmu: chp.Proc.Mp.Gm,
						Reg: chp.Proc.Reg.MergeReceivers[j],
					},
				})
			}
		}
		rs[i] = &Scope{
			Magic:     Remote,
			PreScopes: []*Scope{ss[i], chp},
		}
		rs[i].Proc = process.NewFromProc(mheap.New(c.proc.Mp.Gm), c.proc, 2)
		ss[i].Instructions = append(ss[i].Instructions, vm.Instruction{
			Op: overload.Connector,
			Arg: &connector.Argument{
				Mmu: rs[i].Proc.Mp.Gm,
				Reg: rs[i].Proc.Reg.MergeReceivers[0],
			},
		})
		chp.Instructions = append(chp.Instructions, vm.Instruction{
			Op: overload.Connector,
			Arg: &connector.Argument{
				Mmu: rs[i].Proc.Mp.Gm,
				Reg: rs[i].Proc.Reg.MergeReceivers[1],
			},
		})
	}
	switch n.JoinType {
	case plan.Node_INNER, plan.Node_SEMI:
		if len(n.OnList) == 0 {
			for i := range rs {
				rs[i].Instructions = append(rs[i].Instructions, vm.Instruction{
					Op:  overload.Product,
					Arg: constructProduct(n, c.proc),
				})
			}
		} else {
			for i := range rs {
				rs[i].Instructions = append(rs[i].Instructions, vm.Instruction{
					Op:  overload.Join,
					Arg: constructJoin(n, c.proc),
				})
			}
		}
	case plan.Node_OUTER:
		for i := range rs {
			rs[i].Instructions = append(rs[i].Instructions, vm.Instruction{
				Op:  overload.Left,
				Arg: constructLeft(n, c.proc),
			})
		}
	case plan.Node_ANTI:
		for i := range rs {
			rs[i].Instructions = append(rs[i].Instructions, vm.Instruction{
				Op:  overload.Complement,
				Arg: constructComplement(n, c.proc),
			})
		}
	default:
		panic(errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("join typ '%v' not support now", n.JoinType)))
	}
	return rs
}

func (c *compile) compileSort(n *plan.Node, ss []*Scope) []*Scope {
	switch {
	case n.Limit != nil && n.Offset == nil && len(n.OrderBy) > 0: // top
		return c.compileTop(n, ss)
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

func (c *compile) compileTop(n *plan.Node, ss []*Scope) []*Scope {
	for i := range ss {
		ss[i].Instructions = append(ss[i].Instructions, vm.Instruction{
			Op:  overload.Top,
			Arg: constructTop(n, c.proc),
		})
	}
	rs := &Scope{
		PreScopes: ss,
		Magic:     Merge,
	}
	rs.Proc = process.NewFromProc(mheap.New(c.proc.Mp.Gm), c.proc, len(ss))
	rs.Instructions = append(rs.Instructions, vm.Instruction{
		Op:  overload.MergeTop,
		Arg: constructMergeTop(n, c.proc),
	})

	for i := range ss {
		ss[i].Instructions = append(ss[i].Instructions, vm.Instruction{
			Op: overload.Connector,
			Arg: &connector.Argument{
				Mmu: rs.Proc.Mp.Gm,
				Reg: rs.Proc.Reg.MergeReceivers[i],
			},
		})
	}
	return []*Scope{rs}
}

func (c *compile) compileOrder(n *plan.Node, ss []*Scope) []*Scope {
	for i := range ss {
		ss[i].Instructions = append(ss[i].Instructions, vm.Instruction{
			Op:  overload.Order,
			Arg: constructOrder(n, c.proc),
		})
	}
	rs := &Scope{
		PreScopes: ss,
		Magic:     Merge,
	}
	rs.Proc = process.NewFromProc(mheap.New(c.proc.Mp.Gm), c.proc, len(ss))
	rs.Instructions = append(rs.Instructions, vm.Instruction{
		Op:  overload.MergeOrder,
		Arg: constructMergeOrder(n, c.proc),
	})

	for i := range ss {
		ss[i].Instructions = append(ss[i].Instructions, vm.Instruction{
			Op: overload.Connector,
			Arg: &connector.Argument{
				Mmu: rs.Proc.Mp.Gm,
				Reg: rs.Proc.Reg.MergeReceivers[i],
			},
		})
	}
	return []*Scope{rs}
}

func (c *compile) compileOffset(n *plan.Node, ss []*Scope) []*Scope {
	for i := range ss {
		ss[i].Instructions = append(ss[i].Instructions, vm.Instruction{
			Op:  overload.Offset,
			Arg: constructOffset(n, c.proc),
		})
	}
	rs := &Scope{
		PreScopes: ss,
		Magic:     Merge,
	}
	rs.Proc = process.NewFromProc(mheap.New(c.proc.Mp.Gm), c.proc, len(ss))
	rs.Instructions = append(rs.Instructions, vm.Instruction{
		Op:  overload.MergeOffset,
		Arg: constructMergeOffset(n, c.proc),
	})

	for i := range ss {
		ss[i].Instructions = append(ss[i].Instructions, vm.Instruction{
			Op: overload.Connector,
			Arg: &connector.Argument{
				Mmu: rs.Proc.Mp.Gm,
				Reg: rs.Proc.Reg.MergeReceivers[i],
			},
		})
	}
	return []*Scope{rs}
}

func (c *compile) compileLimit(n *plan.Node, ss []*Scope) []*Scope {
	for i := range ss {
		ss[i].Instructions = append(ss[i].Instructions, vm.Instruction{
			Op:  overload.Limit,
			Arg: constructLimit(n, c.proc),
		})
	}
	rs := &Scope{
		PreScopes: ss,
		Magic:     Merge,
	}
	rs.Proc = process.NewFromProc(mheap.New(c.proc.Mp.Gm), c.proc, len(ss))
	rs.Instructions = append(rs.Instructions, vm.Instruction{
		Op:  overload.MergeLimit,
		Arg: constructMergeLimit(n, c.proc),
	})

	for i := range ss {
		ss[i].Instructions = append(ss[i].Instructions, vm.Instruction{
			Op: overload.Connector,
			Arg: &connector.Argument{
				Mmu: rs.Proc.Mp.Gm,
				Reg: rs.Proc.Reg.MergeReceivers[i],
			},
		})
	}
	return []*Scope{rs}
}

func (c *compile) compileGroup(n *plan.Node, ss []*Scope) []*Scope {
	for i := range ss {
		ss[i].Instructions = append(ss[i].Instructions, vm.Instruction{
			Op:  overload.Group,
			Arg: constructGroup(n),
		})
	}
	rs := &Scope{
		PreScopes: ss,
		Magic:     Merge,
	}
	rs.Proc = process.NewFromProc(mheap.New(c.proc.Mp.Gm), c.proc, len(ss))
	rs.Instructions = append(rs.Instructions, vm.Instruction{
		Op:  overload.MergeGroup,
		Arg: constructMergeGroup(n, true),
	})

	for i := range ss {
		ss[i].Instructions = append(ss[i].Instructions, vm.Instruction{
			Op: overload.Connector,
			Arg: &connector.Argument{
				Mmu: rs.Proc.Mp.Gm,
				Reg: rs.Proc.Reg.MergeReceivers[i],
			},
		})
	}
	return []*Scope{rs}
}
