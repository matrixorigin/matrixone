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
	"context"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/errno"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/connector"
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
	return nil
}

// Run is an important function of the compute-layer, it executes a single sql according to its scope
func (c *compile) Run(ts uint64) (err error) {
	defer func() {
		if e := recover(); e != nil {
			err = moerr.NewPanicError(e)
		}
	}()

	switch c.scope.Magic {
	case Merge:
		return nil
	}
	return nil
}

func (c *compile) compileScope(pn *plan.Plan) (*Scope, error) {
	switch pn.Plan.(type) {
	case *plan.Plan_Query:
		return nil, nil
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
	ctx, cancel := context.WithCancel(context.Background())
	rs.Proc = process.New(mheap.New(c.proc.Mp.Gm))
	rs.Proc.Cancel = cancel
	rs.Proc.Id = c.proc.Id
	rs.Proc.Lim = c.proc.Lim
	rs.Proc.UnixTime = c.proc.UnixTime
	rs.Proc.Snapshot = c.proc.Snapshot
	rs.Proc.Reg.MergeReceivers = make([]*process.WaitRegister, len(ss))
	{
		for i := 0; i < len(ss); i++ {
			rs.Proc.Reg.MergeReceivers[i] = &process.WaitRegister{
				Ctx: ctx,
				Ch:  make(chan *batch.Batch, 1),
			}
		}
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

func (c *compile) compilePlanScope(n *plan.Node, ns []*plan.Node) ([]*Scope, error) {
	switch n.NodeType {
	case plan.Node_VALUE_SCAN:
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
			ss[i].Proc = process.New(mheap.New(c.proc.Mp.Gm))
			ss[i].Proc.Id = c.proc.Id
			ss[i].Proc.Lim = c.proc.Lim
			ss[i].Proc.UnixTime = c.proc.UnixTime
			ss[i].Proc.Snapshot = c.proc.Snapshot
		}
		return ss, nil
	case plan.Node_PROJECT:
		ss, err := c.compilePlanScope(ns[n.Children[0]], ns)
		if err != nil {
			return nil, err
		}
		return compileProjection(n, compileRestrict(n, ss)), nil
	case plan.Node_AGG:
	case plan.Node_JOIN:
	case plan.Node_SORT:
	}
	return nil, nil
}

func compileRestrict(n *plan.Node, ss []*Scope) []*Scope {
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

func compileProjection(n *plan.Node, ss []*Scope) []*Scope {
	for i := range ss {
		ss[i].Instructions = append(ss[i].Instructions, vm.Instruction{
			Op:  overload.Projection,
			Arg: constructProjection(n),
		})
	}
	return ss
}
