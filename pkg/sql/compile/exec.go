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

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/errno"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/connector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/deleteTag"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/output"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/updateTag"
	"github.com/matrixorigin/matrixone/pkg/sql/errors"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/rewrite"
	"github.com/matrixorigin/matrixone/pkg/sql/viewexec/oplus"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/guest"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// Compile is the entrance of the compute-layer, it compiles AST tree to scope list.
// A scope is an execution unit.
func (e *Exec) Compile(u interface{}, fill func(interface{}, *batch.Batch) error) error {
	// do semantic analysis and build plan for sql
	// do ast rewrite
	e.stmt = rewrite.AstRewrite(e.stmt)

	pn, err := plan.New(e.c.db, e.c.sql, e.c.e).BuildStatement(e.stmt)
	if err != nil {
		return err
	}
	{
		attrs := pn.ResultColumns()
		cols := make([]*Col, len(attrs))
		for i, attr := range attrs {
			cols[i] = &Col{
				Name: attr.Name,
				Typ:  attr.Type.Oid,
			}
		}
		e.resultCols = cols
	}
	e.u = u
	e.e = e.c.e
	e.fill = fill

	// build scope for a single sql
	s, err := e.compileScope(pn)
	if err != nil {
		return err
	}
	e.scope = s
	return nil
}

// Run is an important function of the compute-layer, it executes a single sql according to its scope
func (e *Exec) Run(ts uint64) error {
	if e.scope == nil {
		return nil
	}

	switch e.scope.Magic {
	case Normal:
		return e.scope.Run(e.c.e)
	case Merge:
		return e.scope.MergeRun(e.c.e)
	case Remote:
		return e.scope.RemoteRun(e.c.e)
	case Parallel:
		return e.scope.ParallelRun(e.c.e)
	case Insert:
		affectedRows, err := e.scope.Insert(ts)
		if err != nil {
			return err
		}
		e.setAffectedRows(affectedRows)
		return nil
	case CreateDatabase:
		return e.scope.CreateDatabase(ts)
	case CreateTable:
		return e.scope.CreateTable(ts)
	case CreateIndex:
		return e.scope.CreateIndex(ts)
	case DropDatabase:
		return e.scope.DropDatabase(ts)
	case DropTable:
		return e.scope.DropTable(ts)
	case DropIndex:
		return e.scope.DropIndex(ts)
	case ShowDatabases:
		return e.scope.ShowDatabases(e.u, e.fill)
	case ShowTables:
		return e.scope.ShowTables(e.u, e.fill)
	case ShowColumns:
		return e.scope.ShowColumns(e.u, e.fill)
	case ShowCreateTable:
		return e.scope.ShowCreateTable(e.u, e.fill)
	case ShowCreateDatabase:
		return e.scope.ShowCreateDatabase(e.u, e.fill)
	case Delete:
		affectedRows, err := e.scope.Delete(ts, e.c.e)
		if err != nil {
			return err
		}
		e.setAffectedRows(affectedRows)
		return nil
	case Update:
		affectedRows, err := e.scope.Update(ts, e.c.e)
		if err != nil {
			return err
		}
		e.setAffectedRows(affectedRows)
		return nil
	}
	return nil
}

func (e *Exec) compileScope(pn plan.Plan) (*Scope, error) {
	switch qry := pn.(type) {
	case *plan.Query:
		return e.compileQuery(qry)
	case *plan.Insert:
		// todo: insert into tbl select a, b from tbl2 should deal next time.
		return &Scope{
			Magic: Insert,
			Plan:  pn,
			Proc:  e.c.proc,
		}, nil
	case *plan.CreateDatabase:
		return &Scope{
			Magic: CreateDatabase,
			Plan:  pn,
			Proc:  e.c.proc,
		}, nil
	case *plan.CreateTable:
		return &Scope{
			Magic: CreateTable,
			Plan:  pn,
			Proc:  e.c.proc,
			// todo: create table t1 as select a, b, c from t2 ?
			DataSource: nil,
			PreScopes:  nil,
		}, nil
	case *plan.CreateIndex:
		return &Scope{
			Magic: CreateIndex,
			Plan:  pn,
			Proc:  e.c.proc,
		}, nil
	case *plan.DropDatabase:
		return &Scope{
			Magic: DropDatabase,
			Plan:  pn,
			Proc:  e.c.proc,
		}, nil
	case *plan.DropTable:
		return &Scope{
			Magic: DropTable,
			Plan:  pn,
			Proc:  e.c.proc,
		}, nil
	case *plan.DropIndex:
		return &Scope{
			Magic: DropIndex,
			Plan:  pn,
			Proc:  e.c.proc,
		}, nil
	case *plan.ShowDatabases:
		return &Scope{
			Magic: ShowDatabases,
			Plan:  pn,
			Proc:  e.c.proc,
		}, nil
	case *plan.ShowTables:
		return &Scope{
			Magic: ShowTables,
			Plan:  pn,
			Proc:  e.c.proc,
		}, nil
	case *plan.ShowColumns:
		return &Scope{
			Magic: ShowColumns,
			Plan:  pn,
			Proc:  e.c.proc,
		}, nil
	case *plan.ShowCreateTable:
		return &Scope{
			Magic: ShowCreateTable,
			Plan:  pn,
			Proc:  e.c.proc,
		}, nil
	case *plan.ShowCreateDatabase:
		return &Scope{
			Magic: ShowCreateDatabase,
			Plan:  pn,
			Proc:  e.c.proc,
		}, nil
	case *plan.Delete:
		return e.compileDelete(qry.Qry)
	case *plan.Update:
		return e.compileUpdate(qry.Qry)
	}
	return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("query '%s' not support now", pn))
}

func (e *Exec) Statement() tree.Statement {
	return e.stmt
}

func (e *Exec) SetSchema(db string) error {
	e.c.db = db
	return nil
}

func (e *Exec) Columns() []*Col {
	return e.resultCols
}

func (e *Exec) increaseAffectedRows(n uint64) {
	e.affectRows += n
}

func (e *Exec) setAffectedRows(n uint64) {
	e.affectRows = n
}

func (e *Exec) GetAffectedRows() uint64 {
	return e.affectRows
}

func (e *Exec) compileQuery(qry *plan.Query) (*Scope, error) {
	s, err := e.compilePlanScope(qry.Scope)
	if err != nil {
		return nil, err
	}
	if s == nil {
		return s, nil
	}
	attrs := make([]string, len(e.resultCols))
	for i, col := range e.resultCols {
		attrs[i] = col.Name
	}
	s.Instructions = append(s.Instructions, vm.Instruction{
		Op: vm.Output,
		Arg: &output.Argument{
			Attrs: attrs,
			Data:  e.u,
			Func:  e.fill,
		},
	})
	e.scope = s
	return s, nil
}

func (e *Exec) compileDelete(qry *plan.Query) (*Scope, error) {
	if e.checkPlanScope(qry.Scope) != BQ {
		return nil, errors.New(errno.FeatureNotSupported, fmt.Sprintf("Only single table delete is supported"))
	}
	rel := e.getRelationFromPlanScope(qry.Scope)
	if rel == nil {
		return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("cannot find table for delete"))
	}
	s, err := e.compilePlanScope(qry.Scope)
	if err != nil {
		return nil, err
	}
	if s == nil {
		return s, nil
	}
	attrs := make([]string, len(e.resultCols))
	for i, col := range e.resultCols {
		attrs[i] = col.Name
	}
	s.Instructions = append(s.Instructions, vm.Instruction{
		Op: vm.DeleteTag,
		Arg: &deleteTag.Argument{
			Relation:     rel,
			AffectedRows: 0,
		},
	})
	e.scope = s
	return s, nil
}

func (e *Exec) compileUpdate(qry *plan.Query) (*Scope, error) {
	if e.checkPlanScope(qry.Scope) != BQ {
		return nil, errors.New(errno.FeatureNotSupported, fmt.Sprintf("Only single table update is supported"))
	}
	rel := e.getRelationFromPlanScope(qry.Scope)
	if rel == nil {
		return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("cannot find table for update"))
	}
	s, err := e.compilePlanScope(qry.Scope)
	if err != nil {
		return nil, err
	}
	if s == nil {
		return s, nil
	}
	attrs := make([]string, len(e.resultCols))
	for i, col := range e.resultCols {
		attrs[i] = col.Name
	}
	s.Instructions = append(s.Instructions, vm.Instruction{
		Op: vm.UpdateTag,
		Arg: &updateTag.Argument{
			Relation:     rel,
			AffectedRows: 0,
		},
	})
	e.scope = s
	return s, nil
}

func (e *Exec) compilePlanScope(s *plan.Scope) (*Scope, error) {
	switch e.checkPlanScope(s) {
	case BQ:
		ss, err := e.compileQ(s)
		if err != nil {
			return nil, err
		}
		if len(ss) == 0 {
			return nil, nil
		}
		if len(ss) == 1 && ss[0].Magic == Merge {
			return ss[0], nil
		}
		rs := &Scope{Magic: Merge}
		rs.PreScopes = ss
		rs.Instructions = append(rs.Instructions, vm.Instruction{
			Op: vm.Merge,
		})
		ctx, cancel := context.WithCancel(context.Background())
		rs.Proc = process.New(mheap.New(guest.New(e.c.proc.Mp.Gm.Limit, e.c.proc.Mp.Gm.Mmu)))
		rs.Proc.Cancel = cancel
		rs.Proc.Id = e.c.proc.Id
		rs.Proc.Lim = e.c.proc.Lim
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
	case AQ:
		return e.compileAQ(s)
	case CQ:
		return e.compileCQ(s)
	case CAQ:
		return e.compileCAQ(s)
	}
	return nil, nil
}

func (e *Exec) checkPlanScope(s *plan.Scope) int {
	switch op := s.Op.(type) {
	case *plan.Join:
		typ := CQ
		ts := make([]int, len(s.Children))
		for i := range s.Children {
			ts[i] = e.checkPlanScope(s.Children[i])
		}
		for i := range ts {
			switch ts[i] {
			case BQ:
			case AQ:
				typ = CAQ
			case CQ:
			case CAQ:
				typ = CAQ
			}
		}
		return typ
	case *plan.Order:
		return e.checkPlanScope(s.Children[0])
	case *plan.Dedup:
		return e.checkPlanScope(s.Children[0])
	case *plan.Limit:
		return e.checkPlanScope(s.Children[0])
	case *plan.Offset:
		return e.checkPlanScope(s.Children[0])
	case *plan.Restrict:
		return e.checkPlanScope(s.Children[0])
	case *plan.Projection:
		return e.checkPlanScope(s.Children[0])
	case *plan.Relation:
		if len(op.BoundVars) > 0 {
			return AQ
		}
		return BQ
	case *plan.DerivedRelation:
		if len(op.BoundVars) > 0 {
			return AQ
		}
		return BQ
	case *plan.Untransform:
		switch typ := e.checkPlanScope(s.Children[0]); typ {
		case BQ:
			return AQ
		case AQ:
			return AQ
		case CQ:
			return CAQ
		case CAQ:
			return CAQ
		}
	case *plan.Rename:
		return e.checkPlanScope(s.Children[0])
	case *plan.ResultProjection:
		return e.checkPlanScope(s.Children[0])
	}
	return BQ
}

func (e *Exec) getRelationFromPlanScope(s *plan.Scope) engine.Relation {
	switch op := s.Op.(type) {
	case *plan.Join:
		return nil
	case *plan.Order:
		return e.getRelationFromPlanScope(s.Children[0])
	case *plan.Dedup:
		return e.getRelationFromPlanScope(s.Children[0])
	case *plan.Limit:
		return e.getRelationFromPlanScope(s.Children[0])
	case *plan.Offset:
		return e.getRelationFromPlanScope(s.Children[0])
	case *plan.Restrict:
		return e.getRelationFromPlanScope(s.Children[0])
	case *plan.Projection:
		return e.getRelationFromPlanScope(s.Children[0])
	case *plan.Relation:
		db, err := e.e.Database(op.Schema)
		if err != nil {
			return nil
		}
		rel, err := db.Relation(op.Name)
		if err != nil {
			return nil
		}
		return rel
	case *plan.DerivedRelation:
		return nil
	case *plan.Untransform:
		return e.getRelationFromPlanScope(s.Children[0])
	case *plan.Rename:
		return e.getRelationFromPlanScope(s.Children[0])
	case *plan.ResultProjection:
		return e.getRelationFromPlanScope(s.Children[0])
	}
	return nil
}

// compileQ builds the scope which sql is a query for single table and without any aggregate functions
// In this function, we push down an operator like order, deduplicate, transform, limit or top.
// And do merge work for them at the top scope
// There is an example:
//      the sql is
//          select * from t1 order by uname
//      assume that nodes number at this cluster is 3
//  we push down the order operator, order work will be done at pre-Scopes, and we do merge-order at top-Scope.
//  For this case, compileQ will return the top scope
//  scopeA {
//      instruction: mergeOrder -> output
//      pre-scopes:
//          scope1: transform -> order -> push to scopeA
//          scope2: transform -> order -> push to scopeA
//          scope3: transform -> order -> push to scopeA
//  }
// and scope1, 2, 3's structure will get further optimization when they are running.
func (e *Exec) compileQ(ps *plan.Scope) ([]*Scope, error) {
	switch op := ps.Op.(type) {
	case *plan.Order:
		ss, err := e.compileQ(ps.Children[0])
		if err != nil {
			return nil, err
		}
		if len(ss) == 0 {
			return nil, nil
		}
		if len(ss) == 1 && ss[0].Magic == Merge {
			ss[0].Instructions = append(ss[0].Instructions, vm.Instruction{
				Op:  vm.Order,
				Arg: constructOrder(op),
			})
			return ss, nil
		}
		for i := range ss {
			ss[i].Instructions = append(ss[i].Instructions, vm.Instruction{
				Op:  vm.Order,
				Arg: constructOrder(op),
			})
		}
		// init rs
		rs := &Scope{Magic: Merge}
		rs.PreScopes = ss
		rs.Instructions = append(rs.Instructions, vm.Instruction{
			Op:  vm.MergeOrder,
			Arg: constructMergeOrder(op),
		})
		ctx, cancel := context.WithCancel(context.Background())
		rs.Proc = process.New(mheap.New(guest.New(e.c.proc.Mp.Gm.Limit, e.c.proc.Mp.Gm.Mmu)))
		rs.Proc.Cancel = cancel
		rs.Proc.Id = e.c.proc.Id
		rs.Proc.Lim = e.c.proc.Lim
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
		return []*Scope{rs}, nil
	case *plan.Dedup:
		ss, err := e.compileQ(ps.Children[0])
		if err != nil {
			return nil, err
		}
		if len(ss) == 0 {
			return nil, nil
		}
		if len(ss) == 1 && ss[0].Magic == Merge {
			ss[0].Instructions = append(ss[0].Instructions, vm.Instruction{
				Op:  vm.Dedup,
				Arg: constructDedup(),
			})
			return ss, nil
		}
		for i := range ss {
			ss[i].Instructions = append(ss[i].Instructions, vm.Instruction{
				Op:  vm.Dedup,
				Arg: constructDedup(),
			})
		}
		// init rs
		rs := &Scope{Magic: Merge}
		rs.PreScopes = ss
		rs.Instructions = append(rs.Instructions, vm.Instruction{
			Op:  vm.MergeDedup,
			Arg: constructMergeDedup(),
		})
		ctx, cancel := context.WithCancel(context.Background())
		rs.Proc = process.New(mheap.New(guest.New(e.c.proc.Mp.Gm.Limit, e.c.proc.Mp.Gm.Mmu)))
		rs.Proc.Cancel = cancel
		rs.Proc.Id = e.c.proc.Id
		rs.Proc.Lim = e.c.proc.Lim
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
		return []*Scope{rs}, nil
	case *plan.Limit:
		ss, err := e.compileQ(ps.Children[0])
		if err != nil {
			return nil, err
		}
		if len(ss) == 0 {
			return nil, nil
		}
		if len(ss) == 1 && ss[0].Magic == Merge {
			if n := len(ss[0].Instructions); n >= 2 && ss[0].Instructions[n-2].Op == vm.MergeOrder {
				pss := ss[0].PreScopes
				for i := range pss {
					m := len(pss[i].Instructions) - 2
					pss[i].Instructions[m] = vm.Instruction{
						Op:  vm.Top,
						Arg: constructTop(pss[i].Instructions[m].Arg, op.Limit),
					}
				}
				ss[0].Instructions[n-2] = vm.Instruction{
					Op:  vm.MergeTop,
					Arg: constructMergeTop(ss[0].Instructions[n-2].Arg, op.Limit),
				}
				return ss, nil
			}
			ss[0].Instructions = append(ss[0].Instructions, vm.Instruction{
				Op:  vm.Limit,
				Arg: constructLimit(op),
			})
			return ss, nil
		}
		for i := range ss {
			ss[i].Instructions = append(ss[i].Instructions, vm.Instruction{
				Op:  vm.Limit,
				Arg: constructLimit(op),
			})
		}
		// init rs
		rs := &Scope{Magic: Merge}
		rs.PreScopes = ss
		rs.Instructions = append(rs.Instructions, vm.Instruction{
			Op:  vm.MergeLimit,
			Arg: constructMergeLimit(op),
		})
		ctx, cancel := context.WithCancel(context.Background())
		rs.Proc = process.New(mheap.New(guest.New(e.c.proc.Mp.Gm.Limit, e.c.proc.Mp.Gm.Mmu)))
		rs.Proc.Cancel = cancel
		rs.Proc.Id = e.c.proc.Id
		rs.Proc.Lim = e.c.proc.Lim
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
		return []*Scope{rs}, nil
	case *plan.Offset:
		ss, err := e.compileQ(ps.Children[0])
		if err != nil {
			return nil, err
		}
		if len(ss) == 0 {
			return nil, nil
		}
		if len(ss) == 1 && ss[0].Magic == Merge {
			ss[0].Instructions = append(ss[0].Instructions, vm.Instruction{
				Op:  vm.Offset,
				Arg: constructOffset(op),
			})
			return ss, nil
		}
		// init rs
		rs := &Scope{Magic: Merge}
		rs.PreScopes = ss
		rs.Instructions = append(rs.Instructions, vm.Instruction{
			Op:  vm.MergeOffset,
			Arg: constructMergeOffset(op),
		})
		ctx, cancel := context.WithCancel(context.Background())
		rs.Proc = process.New(mheap.New(guest.New(e.c.proc.Mp.Gm.Limit, e.c.proc.Mp.Gm.Mmu)))
		rs.Proc.Cancel = cancel
		rs.Proc.Id = e.c.proc.Id
		rs.Proc.Lim = e.c.proc.Lim
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
		return []*Scope{rs}, nil
	case *plan.Restrict:
		ss, err := e.compileQ(ps.Children[0])
		if err != nil {
			return nil, err
		}
		for i := range ss {
			ss[i].Instructions = append(ss[i].Instructions, vm.Instruction{
				Op:  vm.Restrict,
				Arg: constructRestrict(op),
			})
		}
		return ss, nil
	case *plan.Projection:
		ss, err := e.compileQ(ps.Children[0])
		if err != nil {
			return nil, err
		}
		for i := range ss {
			ss[i].Instructions = append(ss[i].Instructions, vm.Instruction{
				Op:  vm.Projection,
				Arg: constructProjection(op),
			})
		}
		return ss, nil
	case *plan.Relation:
		db, err := e.c.e.Database(op.Schema)
		if err != nil {
			return nil, err
		}
		rel, err := db.Relation(op.Name)
		if err != nil {
			return nil, err
		}
		defer rel.Close()
		// init date source
		src := &Source{
			IsMerge:      false,
			RelationName: op.Name,
			SchemaName:   op.Schema,
			RefCounts:    make([]uint64, 0, len(op.Attrs)),
			Attributes:   make([]string, 0, len(op.Attrs)),
		}
		for k, v := range op.Attrs {
			src.Attributes = append(src.Attributes, k)
			src.RefCounts = append(src.RefCounts, uint64(v.Ref))
		}
		nodes := rel.Nodes()
		ss := make([]*Scope, len(nodes))
		for i := range nodes {
			ss[i] = &Scope{
				DataSource: src,
				NodeInfo:   nodes[i],
				Magic:      Remote,
			}
			ss[i].Proc = process.New(mheap.New(guest.New(e.c.proc.Mp.Gm.Limit, e.c.proc.Mp.Gm.Mmu)))
			ss[i].Proc.Id = e.c.proc.Id
			ss[i].Proc.Lim = e.c.proc.Lim
			ss[i].Instructions = append(ss[i].Instructions, vm.Instruction{
				Op:  vm.Transform,
				Arg: constructBareTransform(op),
			})
		}
		return ss, nil
	case *plan.DerivedRelation:
		child, err := e.compilePlanScope(ps.Children[0])
		if err != nil {
			return nil, err
		}
		rs := &Scope{Magic: Merge}
		rs.PreScopes = []*Scope{child}
		rs.Instructions = append(rs.Instructions, vm.Instruction{
			Op: vm.Merge,
		})
		rs.Instructions = append(rs.Instructions, vm.Instruction{
			Op:  vm.Transform,
			Arg: constructBareTransformFromDerived(op),
		})
		ctx, cancel := context.WithCancel(context.Background())
		rs.Proc = process.New(mheap.New(guest.New(e.c.proc.Mp.Gm.Limit, e.c.proc.Mp.Gm.Mmu)))
		rs.Proc.Cancel = cancel
		rs.Proc.Id = e.c.proc.Id
		rs.Proc.Lim = e.c.proc.Lim
		rs.Proc.Reg.MergeReceivers = make([]*process.WaitRegister, 1)
		rs.Proc.Reg.MergeReceivers[0] = &process.WaitRegister{
			Ctx: ctx,
			Ch:  make(chan *batch.Batch, 1),
		}
		child.Instructions = append(child.Instructions, vm.Instruction{
			Op: vm.Connector,
			Arg: &connector.Argument{
				Mmu: rs.Proc.Mp.Gm,
				Reg: rs.Proc.Reg.MergeReceivers[0],
			},
		})
		return []*Scope{rs}, nil
	case *plan.Rename:
		ss, err := e.compileQ(ps.Children[0])
		if err != nil {
			return nil, err
		}
		for i := range ss {
			ss[i].Instructions = append(ss[i].Instructions, vm.Instruction{
				Op:  vm.Projection,
				Arg: constructRename(op),
			})
		}
		return ss, nil
	case *plan.ResultProjection:
		ss, err := e.compileQ(ps.Children[0])
		if err != nil {
			return nil, err
		}
		if len(ss) == 0 {
			return nil, nil
		}
		if len(ss) == 1 && ss[0].Magic == Merge {
			ss[0].Instructions = append(ss[0].Instructions, vm.Instruction{
				Op:  vm.Projection,
				Arg: constructResultProjection(op),
			})
			return ss, nil
		}
		rs := &Scope{Magic: Merge}
		rs.PreScopes = ss
		rs.Instructions = append(rs.Instructions, vm.Instruction{
			Op: vm.Merge,
		})
		rs.Instructions = append(rs.Instructions, vm.Instruction{
			Op:  vm.Projection,
			Arg: constructResultProjection(op),
		})
		ctx, cancel := context.WithCancel(context.Background())
		rs.Proc = process.New(mheap.New(guest.New(e.c.proc.Mp.Gm.Limit, e.c.proc.Mp.Gm.Mmu)))
		rs.Proc.Cancel = cancel
		rs.Proc.Id = e.c.proc.Id
		rs.Proc.Lim = e.c.proc.Lim
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
		return []*Scope{rs}, nil
	}
	return nil, nil
}

// compileAQ builds the scope for aggregation query
func (e *Exec) compileAQ(ps *plan.Scope) (*Scope, error) {
	switch op := ps.Op.(type) {
	case *plan.Order:
		s, err := e.compileAQ(ps.Children[0])
		if err != nil {
			return nil, err
		}
		if s == nil {
			return nil, nil
		}
		s.Instructions = append(s.Instructions, vm.Instruction{
			Op:  vm.Order,
			Arg: constructOrder(op),
		})
		return s, nil
	case *plan.Dedup:
		s, err := e.compileAQ(ps.Children[0])
		if err != nil {
			return nil, err
		}
		if s == nil {
			return nil, nil
		}
		s.Instructions = append(s.Instructions, vm.Instruction{
			Op:  vm.Dedup,
			Arg: constructDedup(),
		})
		return s, nil
	case *plan.Limit:
		s, err := e.compileAQ(ps.Children[0])
		if err != nil {
			return nil, err
		}
		if s == nil {
			return nil, nil
		}
		s.Instructions = append(s.Instructions, vm.Instruction{
			Op:  vm.Limit,
			Arg: constructLimit(op),
		})
		return s, nil
	case *plan.Offset:
		s, err := e.compileAQ(ps.Children[0])
		if err != nil {
			return nil, err
		}
		if s == nil {
			return nil, nil
		}
		s.Instructions = append(s.Instructions, vm.Instruction{
			Op:  vm.Offset,
			Arg: constructOffset(op),
		})
		return s, nil
	case *plan.Restrict:
		s, err := e.compileAQ(ps.Children[0])
		if err != nil {
			return nil, err
		}
		if s == nil {
			return nil, nil
		}
		s.Instructions = append(s.Instructions, vm.Instruction{
			Op:  vm.Restrict,
			Arg: constructRestrict(op),
		})
		return s, nil
	case *plan.Projection:
		s, err := e.compileAQ(ps.Children[0])
		if err != nil {
			return nil, err
		}
		if s == nil {
			return nil, nil
		}
		s.Instructions = append(s.Instructions, vm.Instruction{
			Op:  vm.Projection,
			Arg: constructProjection(op),
		})
		return s, nil
	case *plan.Untransform:
		s, err := e.compileAQ(ps.Children[0])
		if err != nil {
			return nil, err
		}
		if s == nil {
			return nil, nil
		}
		rs := &Scope{Magic: Merge}
		rs.PreScopes = []*Scope{s}
		rs.Instructions = append(rs.Instructions, vm.Instruction{
			Op:  vm.UnTransform,
			Arg: constructUntransform(op),
		})
		ctx, cancel := context.WithCancel(context.Background())
		rs.Proc = process.New(mheap.New(guest.New(e.c.proc.Mp.Gm.Limit, e.c.proc.Mp.Gm.Mmu)))
		rs.Proc.Cancel = cancel
		rs.Proc.Id = e.c.proc.Id
		rs.Proc.Lim = e.c.proc.Lim
		rs.Proc.Reg.MergeReceivers = make([]*process.WaitRegister, 1)
		rs.Proc.Reg.MergeReceivers[0] = &process.WaitRegister{
			Ctx: ctx,
			Ch:  make(chan *batch.Batch, 1),
		}
		s.Instructions = append(s.Instructions, vm.Instruction{
			Op: vm.Connector,
			Arg: &connector.Argument{
				Mmu: rs.Proc.Mp.Gm,
				Reg: rs.Proc.Reg.MergeReceivers[0],
			},
		})
		return rs, nil
	case *plan.Relation:
		db, err := e.c.e.Database(op.Schema)
		if err != nil {
			return nil, err
		}
		rel, err := db.Relation(op.Name)
		if err != nil {
			return nil, err
		}
		defer rel.Close()
		// init date source
		src := &Source{
			IsMerge:      false,
			RelationName: op.Name,
			SchemaName:   op.Schema,
			RefCounts:    make([]uint64, 0, len(op.Attrs)),
			Attributes:   make([]string, 0, len(op.Attrs)),
		}
		for k, v := range op.Attrs {
			src.Attributes = append(src.Attributes, k)
			src.RefCounts = append(src.RefCounts, uint64(v.Ref))
		}
		nodes := rel.Nodes()
		ss := make([]*Scope, len(nodes))
		for i := range nodes {
			ss[i] = &Scope{
				DataSource: src,
				NodeInfo:   nodes[i],
				Magic:      Remote,
			}
			ss[i].Proc = process.New(mheap.New(guest.New(e.c.proc.Mp.Gm.Limit, e.c.proc.Mp.Gm.Mmu)))
			ss[i].Proc.Id = e.c.proc.Id
			ss[i].Proc.Lim = e.c.proc.Lim
			ss[i].Instructions = append(ss[i].Instructions, vm.Instruction{
				Op:  vm.Transform,
				Arg: constructTransform(op),
			})
		}
		arg := constructTransform(op)
		rs := &Scope{Magic: Merge}
		rs.PreScopes = ss
		rs.Instructions = append(rs.Instructions, vm.Instruction{
			Op:  vm.Oplus,
			Arg: &oplus.Argument{Typ: arg.Typ},
		})
		ctx, cancel := context.WithCancel(context.Background())
		rs.Proc = process.New(mheap.New(guest.New(e.c.proc.Mp.Gm.Limit, e.c.proc.Mp.Gm.Mmu)))
		rs.Proc.Cancel = cancel
		rs.Proc.Id = e.c.proc.Id
		rs.Proc.Lim = e.c.proc.Lim
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
	case *plan.DerivedRelation:
		child, err := e.compilePlanScope(ps.Children[0])
		if err != nil {
			return nil, err
		}
		rs := &Scope{Magic: Merge}
		rs.PreScopes = []*Scope{child}
		rs.Instructions = append(rs.Instructions, vm.Instruction{
			Op: vm.Merge,
		})
		rs.Instructions = append(rs.Instructions, vm.Instruction{
			Op:  vm.Transform,
			Arg: constructTransformFromDerived(op),
		})
		ctx, cancel := context.WithCancel(context.Background())
		rs.Proc = process.New(mheap.New(guest.New(e.c.proc.Mp.Gm.Limit, e.c.proc.Mp.Gm.Mmu)))
		rs.Proc.Cancel = cancel
		rs.Proc.Id = e.c.proc.Id
		rs.Proc.Lim = e.c.proc.Lim
		rs.Proc.Reg.MergeReceivers = make([]*process.WaitRegister, 1)
		rs.Proc.Reg.MergeReceivers[0] = &process.WaitRegister{
			Ctx: ctx,
			Ch:  make(chan *batch.Batch, 1),
		}
		child.Instructions = append(child.Instructions, vm.Instruction{
			Op: vm.Connector,
			Arg: &connector.Argument{
				Mmu: rs.Proc.Mp.Gm,
				Reg: rs.Proc.Reg.MergeReceivers[0],
			},
		})
		return rs, nil
	case *plan.Rename:
		s, err := e.compileAQ(ps.Children[0])
		if err != nil {
			return nil, err
		}
		if s == nil {
			return nil, nil
		}
		s.Instructions = append(s.Instructions, vm.Instruction{
			Op:  vm.Projection,
			Arg: constructRename(op),
		})
		return s, nil
	case *plan.ResultProjection:
		s, err := e.compileAQ(ps.Children[0])
		if err != nil {
			return nil, err
		}
		if s == nil {
			return nil, nil
		}
		s.Instructions = append(s.Instructions, vm.Instruction{
			Op:  vm.Projection,
			Arg: constructResultProjection(op),
		})
		return s, nil
	}
	return nil, nil
}

// compileCQ builds the scope for conjunctive query.
func (e *Exec) compileCQ(ps *plan.Scope) (*Scope, error) {
	switch op := ps.Op.(type) {
	case *plan.Join:
		var children []*Scope

		switch op.Type {
		case plan.INNER:
		case plan.NATURAL:
		default:
			return nil, errors.New(errno.FeatureNotSupported, fmt.Sprintf("Unsupport join type '%v'", op.Type))
		}
		for i := 1; i < len(ps.Children); i++ {
			s, err := e.compilePlanScope(ps.Children[i])
			if err != nil {
				return nil, err
			}
			if s == nil {
				return nil, nil
			}
			children = append(children, s)
		}
		ss, err := e.compileFact(ps.Children[0])
		if err != nil {
			return nil, err
		}
		for i := range ss {
			ss[i].PreScopes = append(ss[i].PreScopes, children...)
			ss[i].Instructions = append(ss[i].Instructions, vm.Instruction{
				Op:  vm.Join,
				Arg: constructJoin(op),
			})
		}
		rs := &Scope{Magic: Merge}
		rs.PreScopes = ss
		rs.Instructions = append(rs.Instructions, vm.Instruction{
			Op: vm.Merge,
		})
		ctx, cancel := context.WithCancel(context.Background())
		rs.Proc = process.New(mheap.New(guest.New(e.c.proc.Mp.Gm.Limit, e.c.proc.Mp.Gm.Mmu)))
		rs.Proc.Cancel = cancel
		rs.Proc.Id = e.c.proc.Id
		rs.Proc.Lim = e.c.proc.Lim
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
	case *plan.Order:
		s, err := e.compileCQ(ps.Children[0])
		if err != nil {
			return nil, err
		}
		if s == nil {
			return nil, nil
		}
		s.Instructions = append(s.Instructions, vm.Instruction{
			Op:  vm.Order,
			Arg: constructOrder(op),
		})
		// init rs
		rs := &Scope{Magic: Merge}
		rs.PreScopes = []*Scope{s}
		rs.Instructions = append(rs.Instructions, vm.Instruction{
			Op:  vm.MergeOrder,
			Arg: constructMergeOrder(op),
		})
		ctx, cancel := context.WithCancel(context.Background())
		rs.Proc = process.New(mheap.New(guest.New(e.c.proc.Mp.Gm.Limit, e.c.proc.Mp.Gm.Mmu)))
		rs.Proc.Cancel = cancel
		rs.Proc.Id = e.c.proc.Id
		rs.Proc.Lim = e.c.proc.Lim
		rs.Proc.Reg.MergeReceivers = make([]*process.WaitRegister, 1)
		{
			rs.Proc.Reg.MergeReceivers[0] = &process.WaitRegister{
				Ctx: ctx,
				Ch:  make(chan *batch.Batch, 1),
			}
		}
		s.Instructions = append(s.Instructions, vm.Instruction{
			Op: vm.Connector,
			Arg: &connector.Argument{
				Mmu: rs.Proc.Mp.Gm,
				Reg: rs.Proc.Reg.MergeReceivers[0],
			},
		})
		return rs, nil
	case *plan.Dedup:
		s, err := e.compileCQ(ps.Children[0])
		if err != nil {
			return nil, err
		}
		if s == nil {
			return nil, nil
		}
		s.Instructions = append(s.Instructions, vm.Instruction{
			Op:  vm.Dedup,
			Arg: constructDedup(),
		})
		// init rs
		rs := &Scope{Magic: Merge}
		rs.PreScopes = []*Scope{s}
		rs.Instructions = append(rs.Instructions, vm.Instruction{
			Op:  vm.MergeDedup,
			Arg: constructMergeDedup(),
		})
		ctx, cancel := context.WithCancel(context.Background())
		rs.Proc = process.New(mheap.New(guest.New(e.c.proc.Mp.Gm.Limit, e.c.proc.Mp.Gm.Mmu)))
		rs.Proc.Cancel = cancel
		rs.Proc.Id = e.c.proc.Id
		rs.Proc.Lim = e.c.proc.Lim
		rs.Proc.Reg.MergeReceivers = make([]*process.WaitRegister, 1)
		{
			rs.Proc.Reg.MergeReceivers[0] = &process.WaitRegister{
				Ctx: ctx,
				Ch:  make(chan *batch.Batch, 1),
			}
		}
		s.Instructions = append(s.Instructions, vm.Instruction{
			Op: vm.Connector,
			Arg: &connector.Argument{
				Mmu: rs.Proc.Mp.Gm,
				Reg: rs.Proc.Reg.MergeReceivers[0],
			},
		})
		return rs, nil
	case *plan.Limit:
		s, err := e.compileCQ(ps.Children[0])
		if err != nil {
			return nil, err
		}
		if s == nil {
			return nil, nil
		}
		s.Instructions = append(s.Instructions, vm.Instruction{
			Op:  vm.Limit,
			Arg: constructLimit(op),
		})
		// init rs
		rs := &Scope{Magic: Merge}
		rs.PreScopes = []*Scope{s}
		rs.Instructions = append(rs.Instructions, vm.Instruction{
			Op:  vm.MergeLimit,
			Arg: constructMergeLimit(op),
		})
		ctx, cancel := context.WithCancel(context.Background())
		rs.Proc = process.New(mheap.New(guest.New(e.c.proc.Mp.Gm.Limit, e.c.proc.Mp.Gm.Mmu)))
		rs.Proc.Cancel = cancel
		rs.Proc.Id = e.c.proc.Id
		rs.Proc.Lim = e.c.proc.Lim
		rs.Proc.Reg.MergeReceivers = make([]*process.WaitRegister, 1)
		{
			rs.Proc.Reg.MergeReceivers[0] = &process.WaitRegister{
				Ctx: ctx,
				Ch:  make(chan *batch.Batch, 1),
			}
		}
		s.Instructions = append(s.Instructions, vm.Instruction{
			Op: vm.Connector,
			Arg: &connector.Argument{
				Mmu: rs.Proc.Mp.Gm,
				Reg: rs.Proc.Reg.MergeReceivers[0],
			},
		})
		return rs, nil
	case *plan.Offset:
		s, err := e.compileCQ(ps.Children[0])
		if err != nil {
			return nil, err
		}
		if s == nil {
			return nil, nil
		}
		s.Instructions = append(s.Instructions, vm.Instruction{
			Op:  vm.Offset,
			Arg: constructOffset(op),
		})
		// init rs
		rs := &Scope{Magic: Merge}
		rs.PreScopes = []*Scope{s}
		rs.Instructions = append(rs.Instructions, vm.Instruction{
			Op:  vm.MergeOffset,
			Arg: constructMergeOffset(op),
		})
		ctx, cancel := context.WithCancel(context.Background())
		rs.Proc = process.New(mheap.New(guest.New(e.c.proc.Mp.Gm.Limit, e.c.proc.Mp.Gm.Mmu)))
		rs.Proc.Cancel = cancel
		rs.Proc.Id = e.c.proc.Id
		rs.Proc.Lim = e.c.proc.Lim
		rs.Proc.Reg.MergeReceivers = make([]*process.WaitRegister, 1)
		{
			rs.Proc.Reg.MergeReceivers[0] = &process.WaitRegister{
				Ctx: ctx,
				Ch:  make(chan *batch.Batch, 1),
			}
		}
		s.Instructions = append(s.Instructions, vm.Instruction{
			Op: vm.Connector,
			Arg: &connector.Argument{
				Mmu: rs.Proc.Mp.Gm,
				Reg: rs.Proc.Reg.MergeReceivers[0],
			},
		})
		return rs, nil
	case *plan.Restrict:
		s, err := e.compileCQ(ps.Children[0])
		if err != nil {
			return nil, err
		}
		if s == nil {
			return nil, nil
		}
		s.Instructions = append(s.Instructions, vm.Instruction{
			Op:  vm.Restrict,
			Arg: constructRestrict(op),
		})
		return s, nil
	case *plan.Projection:
		s, err := e.compileCQ(ps.Children[0])
		if err != nil {
			return nil, err
		}
		if s == nil {
			return nil, nil
		}
		s.Instructions = append(s.Instructions, vm.Instruction{
			Op:  vm.Projection,
			Arg: constructProjection(op),
		})
		return s, nil
	case *plan.Rename:
		s, err := e.compileCQ(ps.Children[0])
		if err != nil {
			return nil, err
		}
		if s == nil {
			return nil, nil
		}
		s.Instructions = append(s.Instructions, vm.Instruction{
			Op:  vm.Projection,
			Arg: constructRename(op),
		})
		return s, nil
	case *plan.ResultProjection:
		s, err := e.compileCQ(ps.Children[0])
		if err != nil {
			return nil, err
		}
		if s == nil {
			return nil, nil
		}
		s.Instructions = append(s.Instructions, vm.Instruction{
			Op:  vm.Projection,
			Arg: constructResultProjection(op),
		})
		return s, nil
	}
	return nil, nil
}

// compileCAQ builds the scope for conjunctive aggregation query.
func (e *Exec) compileCAQ(ps *plan.Scope) (*Scope, error) {
	switch op := ps.Op.(type) {
	case *plan.Order:
		s, err := e.compileCAQ(ps.Children[0])
		if err != nil {
			return nil, err
		}
		if s == nil {
			return nil, nil
		}
		s.Instructions = append(s.Instructions, vm.Instruction{
			Op:  vm.Order,
			Arg: constructOrder(op),
		})
		return s, nil
	case *plan.Dedup:
		s, err := e.compileCAQ(ps.Children[0])
		if err != nil {
			return nil, err
		}
		if s == nil {
			return nil, nil
		}
		s.Instructions = append(s.Instructions, vm.Instruction{
			Op:  vm.Dedup,
			Arg: constructDedup(),
		})
		return s, nil
	case *plan.Limit:
		s, err := e.compileCAQ(ps.Children[0])
		if err != nil {
			return nil, err
		}
		if s == nil {
			return nil, nil
		}
		s.Instructions = append(s.Instructions, vm.Instruction{
			Op:  vm.Limit,
			Arg: constructLimit(op),
		})
		return s, nil
	case *plan.Offset:
		s, err := e.compileCAQ(ps.Children[0])
		if err != nil {
			return nil, err
		}
		if s == nil {
			return nil, nil
		}
		s.Instructions = append(s.Instructions, vm.Instruction{
			Op:  vm.Offset,
			Arg: constructOffset(op),
		})
		return s, nil
	case *plan.Restrict:
		s, err := e.compileCAQ(ps.Children[0])
		if err != nil {
			return nil, err
		}
		if s == nil {
			return nil, nil
		}
		s.Instructions = append(s.Instructions, vm.Instruction{
			Op:  vm.Restrict,
			Arg: constructRestrict(op),
		})
		return s, nil
	case *plan.Projection:
		s, err := e.compileCAQ(ps.Children[0])
		if err != nil {
			return nil, err
		}
		if s == nil {
			return nil, nil
		}
		s.Instructions = append(s.Instructions, vm.Instruction{
			Op:  vm.Projection,
			Arg: constructProjection(op),
		})
		return s, nil
	case *plan.Untransform:
		ss, err := e.compileJoin(ps.Children[0])
		if err != nil {
			return nil, err
		}
		rs := &Scope{Magic: Merge}
		rs.PreScopes = ss
		rs.Instructions = append(rs.Instructions, vm.Instruction{
			Op:  vm.UnTransform,
			Arg: constructCAQUntransform(op),
		})
		ctx, cancel := context.WithCancel(context.Background())
		rs.Proc = process.New(mheap.New(guest.New(e.c.proc.Mp.Gm.Limit, e.c.proc.Mp.Gm.Mmu)))
		rs.Proc.Cancel = cancel
		rs.Proc.Id = e.c.proc.Id
		rs.Proc.Lim = e.c.proc.Lim
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
	case *plan.Rename:
		s, err := e.compileCAQ(ps.Children[0])
		if err != nil {
			return nil, err
		}
		if s == nil {
			return nil, nil
		}
		s.Instructions = append(s.Instructions, vm.Instruction{
			Op:  vm.Projection,
			Arg: constructRename(op),
		})
		return s, nil
	case *plan.ResultProjection:
		s, err := e.compileCAQ(ps.Children[0])
		if err != nil {
			return nil, err
		}
		if s == nil {
			return nil, nil
		}
		s.Instructions = append(s.Instructions, vm.Instruction{
			Op:  vm.Projection,
			Arg: constructResultProjection(op),
		})
		return s, nil
	}
	return nil, nil

}

func (e *Exec) compileJoin(ps *plan.Scope) ([]*Scope, error) {
	var children []*Scope

	op, ok := ps.Op.(*plan.Join)
	if !ok {
		return nil, errors.New(errno.FeatureNotSupported, fmt.Sprintf("Unsupport join plan '%T'", ps))
	}
	switch op.Type {
	case plan.INNER:
	case plan.NATURAL:
	default:
		return nil, errors.New(errno.FeatureNotSupported, fmt.Sprintf("Unsupport join type '%v'", op.Type))
	}
	for i := 1; i < len(ps.Children); i++ {
		s, err := e.compilePlanScope(ps.Children[i])
		if err != nil {
			return nil, err
		}
		if s == nil {
			return nil, nil
		}
		children = append(children, s)
	}
	ss, err := e.compileCAQFact(ps.Children[0])
	if err != nil {
		return nil, err
	}
	for i := range ss {
		ss[i].PreScopes = append(ss[i].PreScopes, children...)
		ss[i].Instructions = append(ss[i].Instructions, vm.Instruction{
			Op:  vm.Join,
			Arg: constructJoin(op),
		})
	}
	return ss, nil
}

func (e *Exec) compileFact(ps *plan.Scope) ([]*Scope, error) {
	switch op := ps.Op.(type) {
	case *plan.Relation:
		db, err := e.c.e.Database(op.Schema)
		if err != nil {
			return nil, err
		}
		rel, err := db.Relation(op.Name)
		if err != nil {
			return nil, err
		}
		defer rel.Close()
		// init date source
		src := &Source{
			IsMerge:      false,
			RelationName: op.Name,
			SchemaName:   op.Schema,
			RefCounts:    make([]uint64, 0, len(op.Attrs)),
			Attributes:   make([]string, 0, len(op.Attrs)),
		}
		for k, v := range op.Attrs {
			src.Attributes = append(src.Attributes, k)
			src.RefCounts = append(src.RefCounts, uint64(v.Ref))
		}
		nodes := rel.Nodes()
		ss := make([]*Scope, len(nodes))
		for i := range nodes {
			ss[i] = &Scope{
				DataSource: src,
				NodeInfo:   nodes[i],
				Magic:      Remote,
			}
			ss[i].Proc = process.New(mheap.New(guest.New(e.c.proc.Mp.Gm.Limit, e.c.proc.Mp.Gm.Mmu)))
			ss[i].Proc.Id = e.c.proc.Id
			ss[i].Proc.Lim = e.c.proc.Lim
			ss[i].Instructions = append(ss[i].Instructions, vm.Instruction{
				Op:  vm.Transform,
				Arg: constructBareTransform(op),
			})
		}
		return ss, nil
	case *plan.DerivedRelation:
		child, err := e.compilePlanScope(ps.Children[0])
		if err != nil {
			return nil, err
		}
		rs := &Scope{Magic: Merge}
		rs.PreScopes = []*Scope{child}
		rs.Instructions = append(rs.Instructions, vm.Instruction{
			Op: vm.Merge,
		})
		rs.Instructions = append(rs.Instructions, vm.Instruction{
			Op:  vm.Transform,
			Arg: constructBareTransformFromDerived(op),
		})
		ctx, cancel := context.WithCancel(context.Background())
		rs.Proc = process.New(mheap.New(guest.New(e.c.proc.Mp.Gm.Limit, e.c.proc.Mp.Gm.Mmu)))
		rs.Proc.Cancel = cancel
		rs.Proc.Id = e.c.proc.Id
		rs.Proc.Lim = e.c.proc.Lim
		rs.Proc.Reg.MergeReceivers = make([]*process.WaitRegister, 1)
		rs.Proc.Reg.MergeReceivers[0] = &process.WaitRegister{
			Ctx: ctx,
			Ch:  make(chan *batch.Batch, 1),
		}
		child.Instructions = append(child.Instructions, vm.Instruction{
			Op: vm.Connector,
			Arg: &connector.Argument{
				Mmu: rs.Proc.Mp.Gm,
				Reg: rs.Proc.Reg.MergeReceivers[0],
			},
		})
		return []*Scope{rs}, nil
	case *plan.Rename:
		ss, err := e.compileFact(ps.Children[0])
		if err != nil {
			return nil, err
		}
		for i := range ss {
			ss[i].Instructions = append(ss[i].Instructions, vm.Instruction{
				Op:  vm.Projection,
				Arg: constructRename(op),
			})
		}
		return ss, nil
	case *plan.Restrict:
		ss, err := e.compileFact(ps.Children[0])
		if err != nil {
			return nil, err
		}
		for i := range ss {
			ss[i].Instructions = append(ss[i].Instructions, vm.Instruction{
				Op:  vm.Restrict,
				Arg: constructRestrict(op),
			})
		}
		return ss, nil
	case *plan.Projection:
		ss, err := e.compileFact(ps.Children[0])
		if err != nil {
			return nil, err
		}
		for i := range ss {
			ss[i].Instructions = append(ss[i].Instructions, vm.Instruction{
				Op:  vm.Projection,
				Arg: constructProjection(op),
			})
		}
		return ss, nil
	}
	return nil, nil
}

func (e *Exec) compileCAQFact(ps *plan.Scope) ([]*Scope, error) {
	switch op := ps.Op.(type) {
	case *plan.Relation:
		db, err := e.c.e.Database(op.Schema)
		if err != nil {
			return nil, err
		}
		rel, err := db.Relation(op.Name)
		if err != nil {
			return nil, err
		}
		defer rel.Close()
		// init date source
		src := &Source{
			IsMerge:      false,
			RelationName: op.Name,
			SchemaName:   op.Schema,
			RefCounts:    make([]uint64, 0, len(op.Attrs)),
			Attributes:   make([]string, 0, len(op.Attrs)),
		}
		for k, v := range op.Attrs {
			src.Attributes = append(src.Attributes, k)
			src.RefCounts = append(src.RefCounts, uint64(v.Ref))
		}
		nodes := rel.Nodes()
		ss := make([]*Scope, len(nodes))
		for i := range nodes {
			ss[i] = &Scope{
				DataSource: src,
				NodeInfo:   nodes[i],
				Magic:      Remote,
			}
			ss[i].Proc = process.New(mheap.New(guest.New(e.c.proc.Mp.Gm.Limit, e.c.proc.Mp.Gm.Mmu)))
			ss[i].Proc.Id = e.c.proc.Id
			ss[i].Proc.Lim = e.c.proc.Lim
			ss[i].Instructions = append(ss[i].Instructions, vm.Instruction{
				Op:  vm.Transform,
				Arg: constructCAQTransform(op),
			})
		}
		return ss, nil
	case *plan.DerivedRelation:
		child, err := e.compilePlanScope(ps.Children[0])
		if err != nil {
			return nil, err
		}
		rs := &Scope{Magic: Merge}
		rs.PreScopes = []*Scope{child}
		rs.Instructions = append(rs.Instructions, vm.Instruction{
			Op: vm.Merge,
		})
		rs.Instructions = append(rs.Instructions, vm.Instruction{
			Op:  vm.Transform,
			Arg: constructCAQTransformFromDerived(op),
		})
		ctx, cancel := context.WithCancel(context.Background())
		rs.Proc = process.New(mheap.New(guest.New(e.c.proc.Mp.Gm.Limit, e.c.proc.Mp.Gm.Mmu)))
		rs.Proc.Cancel = cancel
		rs.Proc.Id = e.c.proc.Id
		rs.Proc.Lim = e.c.proc.Lim
		rs.Proc.Reg.MergeReceivers = make([]*process.WaitRegister, 1)
		rs.Proc.Reg.MergeReceivers[0] = &process.WaitRegister{
			Ctx: ctx,
			Ch:  make(chan *batch.Batch, 1),
		}
		child.Instructions = append(child.Instructions, vm.Instruction{
			Op: vm.Connector,
			Arg: &connector.Argument{
				Mmu: rs.Proc.Mp.Gm,
				Reg: rs.Proc.Reg.MergeReceivers[0],
			},
		})
		return []*Scope{rs}, nil
	case *plan.Rename:
		ss, err := e.compileCAQFact(ps.Children[0])
		if err != nil {
			return nil, err
		}
		for i := range ss {
			ss[i].Instructions = append(ss[i].Instructions, vm.Instruction{
				Op:  vm.Projection,
				Arg: constructRename(op),
			})
		}
		return ss, nil
	case *plan.Restrict:
		ss, err := e.compileCAQFact(ps.Children[0])
		if err != nil {
			return nil, err
		}
		for i := range ss {
			ss[i].Instructions = append(ss[i].Instructions, vm.Instruction{
				Op:  vm.Restrict,
				Arg: constructRestrict(op),
			})
		}
		return ss, nil
	case *plan.Projection:
		ss, err := e.compileCAQFact(ps.Children[0])
		if err != nil {
			return nil, err
		}
		for i := range ss {
			ss[i].Instructions = append(ss[i].Instructions, vm.Instruction{
				Op:  vm.Projection,
				Arg: constructProjection(op),
			})
		}
		return ss, nil
	}
	return nil, nil
}
