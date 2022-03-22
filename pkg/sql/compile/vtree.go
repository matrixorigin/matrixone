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
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/errno"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/connector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/deleteTag"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergededup"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergelimit"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergeoffset"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergeorder"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergetop"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/output"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/updateTag"
	"github.com/matrixorigin/matrixone/pkg/sql/errors"
	"github.com/matrixorigin/matrixone/pkg/sql/viewexec/oplus"
	"github.com/matrixorigin/matrixone/pkg/sql/viewexec/times"
	"github.com/matrixorigin/matrixone/pkg/sql/viewexec/transform"
	"github.com/matrixorigin/matrixone/pkg/sql/viewexec/untransform"
	"github.com/matrixorigin/matrixone/pkg/sql/vtree"
	"github.com/matrixorigin/matrixone/pkg/vm"
	cc "github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tpe/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/guest"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func (e *Exec) compileVTree(vt *vtree.ViewTree, varsMap map[string]int) (*Scope, error) {
	var s *Scope
	var rs *Scope
	var err error

	if vt == nil {
		return nil, nil
	}
	fvarsMap := make(map[string]int)
	{
		for _, fvar := range vt.FreeVars {
			fvarsMap[fvar] = 0
		}
	}

	isB := vt.IsBare() // if true, it is a query without aggregate functions
	d := depth(vt.Views)
	switch {
	case d == 0 && isB:
		// Single table queries without aggregate functions
		if s, err = e.compileQ(vt, vt.Views[len(vt.Views)-1]); err != nil {
			return nil, err
		}
		return s, nil
	case d == 0 && !isB:
		// Queries with aggregate functions
		if s, err = e.compileAQ(vt.Views[len(vt.Views)-1]); err != nil {
			return nil, err
		}
	case d > 0 && !isB:
		// Multi-table queries with aggregate functions
		if d > 1 {
			return nil, errors.New(errno.SQLStatementNotYetComplete, "not support now")
		}
		if s, err = e.compileCAQ(vt.FreeVars, vt.Views, varsMap, fvarsMap); err != nil {
			return nil, err
		}
	default:
		return nil, errors.New(errno.SQLStatementNotYetComplete, "not support now")
	}
	rs = &Scope{
		Magic:     Merge,
		PreScopes: []*Scope{s},
	}
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
	switch {
	case isB:
		rs.Instructions = append(rs.Instructions, vm.Instruction{
			Op: vm.UnTransform,
			Arg: &untransform.Argument{
				FreeVars: vt.FreeVars,
				Type:     untransform.Bare,
			},
		})
	default:
		rs.Instructions = append(rs.Instructions, vm.Instruction{
			Op: vm.UnTransform,
			Arg: &untransform.Argument{
				FreeVars: vt.FreeVars,
				Type:     untransform.FreeVarsAndBoundVars,
			},
		})
	}
	if vt.Projection != nil {
		rs.Instructions = append(rs.Instructions, vm.Instruction{
			Op:  vm.Projection,
			Arg: vt.Projection,
		})
	}
	if vt.Restrict != nil {
		rs.Instructions = append(rs.Instructions, vm.Instruction{
			Op:  vm.Restrict,
			Arg: vt.Restrict,
		})
	}
	if vt.Top != nil {
		rs.Instructions = append(rs.Instructions, vm.Instruction{
			Op:  vm.Top,
			Arg: vt.Top,
		})
	}
	if vt.Dedup != nil {
		rs.Instructions = append(rs.Instructions, vm.Instruction{
			Op:  vm.Dedup,
			Arg: vt.Dedup,
		})
	}
	if vt.Order != nil {
		rs.Instructions = append(rs.Instructions, vm.Instruction{
			Op:  vm.Order,
			Arg: vt.Order,
		})
	}
	if vt.Offset != nil {
		rs.Instructions = append(rs.Instructions, vm.Instruction{
			Op:  vm.Offset,
			Arg: vt.Offset,
		})
	}
	if vt.Limit != nil {
		rs.Instructions = append(rs.Instructions, vm.Instruction{
			Op:  vm.Limit,
			Arg: vt.Limit,
		})
	}
	attrs := make([]string, len(e.resultCols))
	for i, col := range e.resultCols {
		attrs[i] = col.Name
	}
	rs.Instructions = append(rs.Instructions, vm.Instruction{
		Op: vm.Output,
		Arg: &output.Argument{
			Attrs: attrs,
			Data:  e.u,
			Func:  e.fill,
		},
	})
	return rs, nil
}

// compileCAQ builds the scope which sql is a query with both aggregate functions and join operators.
func (e *Exec) compileCAQ(freeVars []string, vs []*vtree.View, varsMap, fvarsMap map[string]int) (*Scope, error) {
	var ss []*Scope

	if d := depth(vs); d == 0 {
		return e.compileAQ(vs[len(vs)-1])
	}
	arg := &times.Argument{
		VarsMap:  varsMap,
		FreeVars: freeVars,
	}
	for i := 0; i < len(vs)-1; i++ {
		arg.Rvars = append(arg.Rvars, vs[i].Var.Name)
		if n := len(vs[i].Children); n > 0 {
			s, err := e.compileCAQ(nil, vs[i].Children, varsMap, fvarsMap)
			if err != nil {
				return nil, err
			}
			ss = append(ss, s)
			arg.Ss = append(arg.Ss, vs[i].Children[n-1].Rel.Alias)
			arg.Svars = append(arg.Svars, vs[i].Children[0].Var.Name)
		}
	}
	s, err := e.compileTimes(vs[len(vs)-1], ss, arg)
	if err != nil {
		return nil, err
	}
	return s, nil
}

func (e *Exec) compileTimes(v *vtree.View, children []*Scope, arg *times.Argument) (*Scope, error) {
	var ins vm.Instructions

	db, err := e.c.e.Database(v.Rel.Schema)
	if err != nil {
		return nil, err
	}
	rel, err := db.Relation(v.Rel.Name)
	if err != nil {
		return nil, err
	}
	defer rel.Close()
	arg.R = v.Rel.Alias
	src := &Source{
		RelationName: v.Rel.Name,
		SchemaName:   v.Rel.Schema,
		RefCounts:    make([]uint64, len(v.Rel.Vars)),
		Attributes:   make([]string, len(v.Rel.Vars)),
	}
	for i := range v.Rel.Vars {
		src.Attributes[i] = v.Rel.Vars[i].Name
		src.RefCounts[i] = uint64(v.Rel.Vars[i].Ref)
	}
	v.Arg.IsMerge = true
	v.Arg.Typ = transform.FreeVarsAndBoundVars
	if len(v.Arg.FreeVars) == 0 {
		v.Arg.Typ = transform.BoundVars
	}
	ins = append(ins, vm.Instruction{
		Arg: v.Arg,
		Op:  vm.Transform,
	})
	ns := rel.Nodes()
	ss := make([]*Scope, len(ns))
	for i := range ns {
		s := &Scope{
			DataSource:   src,
			Instructions: ins,
			NodeInfo:     ns[i],
			Magic:        Normal,
		}
		s.Proc = process.New(mheap.New(guest.New(e.c.proc.Mp.Gm.Limit, e.c.proc.Mp.Gm.Mmu)))
		s.Proc.Id = e.c.proc.Id
		s.Proc.Lim = e.c.proc.Lim
		ss[i] = &Scope{
			NodeInfo:  ns[i],
			PreScopes: append([]*Scope{s}, children...),
			Magic:     Remote,
			Instructions: vm.Instructions{vm.Instruction{
				Arg: arg,
				Op:  vm.Times,
			}},
		}
		ss[i].Proc = process.New(mheap.New(guest.New(e.c.proc.Mp.Gm.Limit, e.c.proc.Mp.Gm.Mmu)))
		ss[i].Proc.Id = e.c.proc.Id
		ss[i].Proc.Lim = e.c.proc.Lim
	}
	rs := &Scope{
		PreScopes: ss,
		Magic:     Merge,
	}
	rs.Instructions = append(rs.Instructions, vm.Instruction{
		Op:  vm.Oplus,
		Arg: &oplus.Argument{Typ: v.Arg.Typ},
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
}

// compileQ builds the scope which sql is a query for single table and without any aggregate functions
// In this function, we push down an operator like order, deduplicate, transform, limit or top.
// And do merge work for them at the top scope
// There is an example:
//		the sql is
//			select * from t1 order by uname
//		assume that nodes number at this cluster is 3
//	we push down the order operator, order work will be done at pre-Scopes, and we do merge-order at top-Scope.
//  For this case, compileQ will return the top scope
// 	scopeA {
//		instruction: mergeOrder -> output
//		pre-scopes:
//			scope1: transform -> order -> push to scopeA
//			scope2: transform -> order -> push to scopeA
//			scope3: transform -> order -> push to scopeA
//	}
// and scope1, 2, 3's structure will get further optimization when they are running.
func (e *Exec) compileQ(vt *vtree.ViewTree, v *vtree.View) (*Scope, error) {
	var ins vm.Instructions

	if len(v.Rel.Vars) == 0 {
		return nil, errors.New(errno.FeatureNotSupported, "projection attributes is empty")
	}
	db, err := e.c.e.Database(v.Rel.Schema)
	if err != nil {
		return nil, err
	}
	rel, err := db.Relation(v.Rel.Name)
	if err != nil {
		return nil, err
	}
	defer rel.Close()
	// init date source
	src := &Source{
		IsMerge:      false,
		RelationName: v.Rel.Name,
		SchemaName:   v.Rel.Schema,
		RefCounts:    make([]uint64, len(v.Rel.Vars)),
		Attributes:   make([]string, len(v.Rel.Vars)),
	}
	for i := range v.Rel.Vars {
		src.RefCounts[i] = uint64(v.Rel.Vars[i].Ref)
		src.Attributes[i] = v.Rel.Vars[i].Name
	}

	v.Arg.Typ = transform.Bare

	rs := &Scope{Magic: Merge}
	// push down operators
	ins = append(ins, vm.Instruction{Arg: v.Arg, Op: vm.Transform})
	switch {
	case vt.Order != nil && vt.Dedup != nil:
		// TODO: this case should push down a new operator to do both order and deduplication,
		//   and we push down the order operator temporarily
		ins = append(ins, vm.Instruction{Arg: vt.Order, Op: vm.Order})
		rs.Instructions = append(rs.Instructions, vm.Instruction{
			Op: vm.MergeOrder,
			Arg: &mergeorder.Argument{Fields: vt.Order.Fs},
		})
		vt.Order = nil
	case vt.Order != nil:
		// push down the order operator
		ins = append(ins, vm.Instruction{Arg: vt.Order, Op: vm.Order})
		rs.Instructions = append(rs.Instructions, vm.Instruction{
			Op: vm.MergeOrder,
			Arg: &mergeorder.Argument{Fields: vt.Order.Fs},
		})
		vt.Order = nil
	case vt.Dedup != nil:
		// push down the deduplication operator
		ins = append(ins, vm.Instruction{Arg: vt.Dedup, Op: vm.Dedup})
		rs.Instructions = append(rs.Instructions, vm.Instruction{
			Op: vm.MergeDedup,
			Arg: &mergededup.Argument{},
		})
		vt.Dedup = nil
	case vt.Top != nil:
		// push down the top operator
		ins = append(ins, vm.Instruction{Arg: vt.Top, Op: vm.Top})
		rs.Instructions = append(rs.Instructions, vm.Instruction{
			Op: vm.MergeTop,
			Arg: &mergetop.Argument{
				Fields: vt.Top.Fs,
				Limit: vt.Top.Limit,
			},
		})
		vt.Top = nil
	case vt.Limit != nil && vt.Offset == nil:
		// push down the limit operator
		ins = append(ins, vm.Instruction{Arg: vt.Limit, Op: vm.Limit})
		rs.Instructions = append(rs.Instructions, vm.Instruction{
			Op: vm.MergeLimit,
			Arg: &mergelimit.Argument{
				Limit: vt.Limit.Limit,
			},
		})
		vt.Limit = nil
	case vt.Limit != nil && vt.Offset != nil:
		// needn't push down the operator but should do merge-offset work
		rs.Instructions = append(rs.Instructions, vm.Instruction{
			Op: vm.MergeOffset,
			Arg: &mergeoffset.Argument{
				Offset: vt.Offset.Offset,
			},
		})
		vt.Offset = nil
	default:
		rs.Instructions = append(rs.Instructions, vm.Instruction{
			Op: vm.Merge,
		})
	}

	nodes := rel.Nodes()
	ss := make([]*Scope, len(nodes))
	for i := range nodes {
		ss[i] = &Scope{
			DataSource:   src,
			Instructions: ins,
			NodeInfo:     nodes[i],
			Magic:        Remote,
		}
		ss[i].Proc = process.New(mheap.New(guest.New(e.c.proc.Mp.Gm.Limit, e.c.proc.Mp.Gm.Mmu)))
		ss[i].Proc.Id = e.c.proc.Id
		ss[i].Proc.Lim = e.c.proc.Lim
	}

	// init rs
	rs.PreScopes = ss
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
	// init instructions for top scope
	if vt.Projection != nil {
		rs.Instructions = append(rs.Instructions, vm.Instruction{
			Op:  vm.Projection,
			Arg: vt.Projection,
		})
	}
	if vt.Restrict != nil {
		rs.Instructions = append(rs.Instructions, vm.Instruction{
			Op:  vm.Restrict,
			Arg: vt.Restrict,
		})
	}
	if vt.Top != nil {
		rs.Instructions = append(rs.Instructions, vm.Instruction{
			Op:  vm.Top,
			Arg: vt.Top,
		})
	}
	if vt.Order != nil {
		rs.Instructions = append(rs.Instructions, vm.Instruction{
			Op:  vm.Order,
			Arg: vt.Order,
		})
	}
	if vt.Dedup != nil {
		rs.Instructions = append(rs.Instructions, vm.Instruction{
			Op:  vm.Dedup,
			Arg: vt.Dedup,
		})
	}
	if vt.Offset != nil {
		rs.Instructions = append(rs.Instructions, vm.Instruction{
			Op:  vm.Offset,
			Arg: vt.Offset,
		})
	}
	if vt.Limit != nil {
		rs.Instructions = append(rs.Instructions, vm.Instruction{
			Op:  vm.Limit,
			Arg: vt.Limit,
		})
	}
	attrs := make([]string, len(e.resultCols))
	for i, col := range e.resultCols {
		attrs[i] = col.Name
	}
	rs.Instructions = append(rs.Instructions, vm.Instruction{
		Op: vm.Output,
		Arg: &output.Argument{
			Attrs: attrs,
			Data:  e.u,
			Func:  e.fill,
		},
	})
	return rs, nil
}

// compileAQ builds the scope which sql is a query for single table with aggregate functions.
func (e *Exec) compileAQ(v *vtree.View) (*Scope, error) {
	var ins vm.Instructions

	db, err := e.c.e.Database(v.Rel.Schema)
	if err != nil {
		return nil, err
	}
	rel, err := db.Relation(v.Rel.Name)
	if err != nil {
		return nil, err
	}
	defer rel.Close()
	src := &Source{
		IsMerge:      false,
		RelationName: v.Rel.Name,
		SchemaName:   v.Rel.Schema,
		RefCounts:    make([]uint64, len(v.Rel.Vars)),
		Attributes:   make([]string, len(v.Rel.Vars)),
	}
	for i := range v.Rel.Vars {
		src.Attributes[i] = v.Rel.Vars[i].Name
		src.RefCounts[i] = uint64(v.Rel.Vars[i].Ref)
	}
	v.Arg.Typ = transform.FreeVarsAndBoundVars
	if len(v.Arg.FreeVars) == 0 {
		v.Arg.Typ = transform.BoundVars
	}
	ins = append(ins, vm.Instruction{
		Arg: v.Arg,
		Op:  vm.Transform,
	})
	ns := rel.Nodes()
	ss := make([]*Scope, len(ns))
	for i := range ns {
		ss[i] = &Scope{
			DataSource:   src,
			Instructions: ins,
			NodeInfo:     ns[i],
			Magic:        Remote,
		}
		ss[i].Proc = process.New(mheap.New(guest.New(e.c.proc.Mp.Gm.Limit, e.c.proc.Mp.Gm.Mmu)))
		ss[i].Proc.Id = e.c.proc.Id
		ss[i].Proc.Lim = e.c.proc.Lim
	}
	rs := &Scope{
		PreScopes: ss,
		Magic:     Merge,
	}
	rs.Instructions = append(rs.Instructions, vm.Instruction{
		Op:  vm.Oplus,
		Arg: &oplus.Argument{Typ: v.Arg.Typ},
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
}

func (e *Exec) compileDelete(vt *vtree.ViewTree, v *vtree.View) (*Scope, error ) {
	var ins vm.Instructions

	db, err := e.c.e.Database(v.Rel.Schema)
	if err != nil {
		return nil, err
	}
	rel, err := db.Relation(v.Rel.Name)
	if err != nil {
		return nil, err
	}
	// if it is tpe engine, it will check primary key and get the definition of hide column
	if tpeRelation, ok := rel.(*engine.TpeRelation); ok {
		tableDefs := tpeRelation.TableDefs()
		hasPriKey := false
		for _, def := range tableDefs {
			if _, hasPriKey = def.(*cc.PrimaryIndexDef); hasPriKey {
				break
			}
		}
		if !hasPriKey {
			colDef := tpeRelation.GetHideColDef()
			if colDef == nil {
				return nil, errors.New(errno.DataException, "Get the definition of hide column failed")
			}
			v.Rel.Vars = append(v.Rel.Vars, &vtree.Variable{Name: colDef.Name, Type: colDef.Type.Oid})
		}
	} else {
		return nil, errors.New(errno.CaseNotFound, "Do not support deletion for other engine except Tpe")
	}
	// init date source
	src := &Source{
		IsMerge:      false,
		RelationName: v.Rel.Name,
		SchemaName:   v.Rel.Schema,
		RefCounts:    make([]uint64, len(v.Rel.Vars)),
		Attributes:   make([]string, len(v.Rel.Vars)),
	}
	for i := range v.Rel.Vars {
		src.RefCounts[i] = uint64(v.Rel.Vars[i].Ref)
		src.Attributes[i] = v.Rel.Vars[i].Name
	}

	v.Arg.Typ = transform.Bare

	rs := &Scope{Magic: Delete}
	// push down operators
	ins = append(ins, vm.Instruction{Arg: v.Arg, Op: vm.Transform})
	switch {
	case vt.Order != nil && vt.Dedup != nil:
		// TODO: this case should push down a new operator to do both order and deduplication,
		//   and we push down the order operator temporarily
		ins = append(ins, vm.Instruction{Arg: vt.Order, Op: vm.Order})
		rs.Instructions = append(rs.Instructions, vm.Instruction{
			Op: vm.MergeOrder,
			Arg: &mergeorder.Argument{Fields: vt.Order.Fs},
		})
		vt.Order = nil
	case vt.Order != nil:
		// push down the order operator
		ins = append(ins, vm.Instruction{Arg: vt.Order, Op: vm.Order})
		rs.Instructions = append(rs.Instructions, vm.Instruction{
			Op: vm.MergeOrder,
			Arg: &mergeorder.Argument{Fields: vt.Order.Fs},
		})
		vt.Order = nil
	case vt.Dedup != nil:
		// push down the deduplication operator
		ins = append(ins, vm.Instruction{Arg: vt.Dedup, Op: vm.Dedup})
		rs.Instructions = append(rs.Instructions, vm.Instruction{
			Op: vm.MergeDedup,
			Arg: &mergededup.Argument{},
		})
		vt.Dedup = nil
	case vt.Top != nil:
		// push down the top operator
		ins = append(ins, vm.Instruction{Arg: vt.Top, Op: vm.Top})
		rs.Instructions = append(rs.Instructions, vm.Instruction{
			Op: vm.MergeTop,
			Arg: &mergetop.Argument{
				Fields: vt.Top.Fs,
				Limit: vt.Top.Limit,
			},
		})
		vt.Top = nil
	case vt.Limit != nil && vt.Offset == nil:
		// push down the limit operator
		ins = append(ins, vm.Instruction{Arg: vt.Limit, Op: vm.Limit})
		rs.Instructions = append(rs.Instructions, vm.Instruction{
			Op: vm.MergeLimit,
			Arg: &mergelimit.Argument{
				Limit: vt.Limit.Limit,
			},
		})
		vt.Limit = nil
	case vt.Limit != nil && vt.Offset != nil:
		// needn't push down the operator but should do merge-offset work
		rs.Instructions = append(rs.Instructions, vm.Instruction{
			Op: vm.MergeOffset,
			Arg: &mergeoffset.Argument{
				Offset: vt.Offset.Offset,
			},
		})
		vt.Offset = nil
	default:
		rs.Instructions = append(rs.Instructions, vm.Instruction{
			Op: vm.Merge,
		})
	}

	nodes := rel.Nodes()
	ss := make([]*Scope, len(nodes))
	for i := range nodes {
		ss[i] = &Scope{
			DataSource:   src,
			Instructions: ins,
			NodeInfo:     nodes[i],
			Magic:        Remote,
		}
		ss[i].Proc = process.New(mheap.New(guest.New(e.c.proc.Mp.Gm.Limit, e.c.proc.Mp.Gm.Mmu)))
		ss[i].Proc.Id = e.c.proc.Id
		ss[i].Proc.Lim = e.c.proc.Lim
	}

	// init rs
	rs.PreScopes = ss
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
	// init instructions for top scope
	if vt.Projection != nil {
		rs.Instructions = append(rs.Instructions, vm.Instruction{
			Op:  vm.Projection,
			Arg: vt.Projection,
		})
	}
	if vt.Restrict != nil {
		rs.Instructions = append(rs.Instructions, vm.Instruction{
			Op:  vm.Restrict,
			Arg: vt.Restrict,
		})
	}
	if vt.Top != nil {
		rs.Instructions = append(rs.Instructions, vm.Instruction{
			Op:  vm.Top,
			Arg: vt.Top,
		})
	}
	if vt.Order != nil {
		rs.Instructions = append(rs.Instructions, vm.Instruction{
			Op:  vm.Order,
			Arg: vt.Order,
		})
	}
	if vt.Dedup != nil {
		rs.Instructions = append(rs.Instructions, vm.Instruction{
			Op:  vm.Dedup,
			Arg: vt.Dedup,
		})
	}
	if vt.Offset != nil {
		rs.Instructions = append(rs.Instructions, vm.Instruction{
			Op:  vm.Offset,
			Arg: vt.Offset,
		})
	}
	if vt.Limit != nil {
		rs.Instructions = append(rs.Instructions, vm.Instruction{
			Op:  vm.Limit,
			Arg: vt.Limit,
		})
	}
	attrs := make([]string, len(e.resultCols))
	for i, col := range e.resultCols {
		attrs[i] = col.Name
	}
	rs.Instructions = append(rs.Instructions, vm.Instruction{
		Op: vm.DeleteTag,
		Arg: &deleteTag.Argument{
			Relation: rel,
			AffectedRows: 0,
		},
	})
	return rs, nil
}

func (e *Exec) compileUpdate(vt *vtree.ViewTree, v *vtree.View) (*Scope, error ) {
	var ins vm.Instructions

	db, err := e.c.e.Database(v.Rel.Schema)
	if err != nil {
		return nil, err
	}
	rel, err := db.Relation(v.Rel.Name)
	if err != nil {
		return nil, err
	}
	// init date source
	src := &Source{
		IsMerge:      false,
		RelationName: v.Rel.Name,
		SchemaName:   v.Rel.Schema,
		RefCounts:    make([]uint64, len(v.Rel.Vars)),
		Attributes:   make([]string, len(v.Rel.Vars)),
	}
	for i := range v.Rel.Vars {
		src.RefCounts[i] = uint64(v.Rel.Vars[i].Ref)
		src.Attributes[i] = v.Rel.Vars[i].Name
	}

	v.Arg.Typ = transform.Bare

	rs := &Scope{Magic: Delete}
	// push down operators
	ins = append(ins, vm.Instruction{Arg: v.Arg, Op: vm.Transform})
	switch {
	case vt.Order != nil && vt.Dedup != nil:
		// TODO: this case should push down a new operator to do both order and deduplication,
		//   and we push down the order operator temporarily
		ins = append(ins, vm.Instruction{Arg: vt.Order, Op: vm.Order})
		rs.Instructions = append(rs.Instructions, vm.Instruction{
			Op: vm.MergeOrder,
			Arg: &mergeorder.Argument{Fields: vt.Order.Fs},
		})
		vt.Order = nil
	case vt.Order != nil:
		// push down the order operator
		ins = append(ins, vm.Instruction{Arg: vt.Order, Op: vm.Order})
		rs.Instructions = append(rs.Instructions, vm.Instruction{
			Op: vm.MergeOrder,
			Arg: &mergeorder.Argument{Fields: vt.Order.Fs},
		})
		vt.Order = nil
	case vt.Dedup != nil:
		// push down the deduplication operator
		ins = append(ins, vm.Instruction{Arg: vt.Dedup, Op: vm.Dedup})
		rs.Instructions = append(rs.Instructions, vm.Instruction{
			Op: vm.MergeDedup,
			Arg: &mergededup.Argument{},
		})
		vt.Dedup = nil
	case vt.Top != nil:
		// push down the top operator
		ins = append(ins, vm.Instruction{Arg: vt.Top, Op: vm.Top})
		rs.Instructions = append(rs.Instructions, vm.Instruction{
			Op: vm.MergeTop,
			Arg: &mergetop.Argument{
				Fields: vt.Top.Fs,
				Limit: vt.Top.Limit,
			},
		})
		vt.Top = nil
	case vt.Limit != nil && vt.Offset == nil:
		// push down the limit operator
		ins = append(ins, vm.Instruction{Arg: vt.Limit, Op: vm.Limit})
		rs.Instructions = append(rs.Instructions, vm.Instruction{
			Op: vm.MergeLimit,
			Arg: &mergelimit.Argument{
				Limit: vt.Limit.Limit,
			},
		})
		vt.Limit = nil
	case vt.Limit != nil && vt.Offset != nil:
		// needn't push down the operator but should do merge-offset work
		rs.Instructions = append(rs.Instructions, vm.Instruction{
			Op: vm.MergeOffset,
			Arg: &mergeoffset.Argument{
				Offset: vt.Offset.Offset,
			},
		})
		vt.Offset = nil
	default:
		rs.Instructions = append(rs.Instructions, vm.Instruction{
			Op: vm.Merge,
		})
	}

	nodes := rel.Nodes()
	ss := make([]*Scope, len(nodes))
	for i := range nodes {
		ss[i] = &Scope{
			DataSource:   src,
			Instructions: ins,
			NodeInfo:     nodes[i],
			Magic:        Remote,
		}
		ss[i].Proc = process.New(mheap.New(guest.New(e.c.proc.Mp.Gm.Limit, e.c.proc.Mp.Gm.Mmu)))
		ss[i].Proc.Id = e.c.proc.Id
		ss[i].Proc.Lim = e.c.proc.Lim
	}

	// init rs
	rs.PreScopes = ss
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
	// init instructions for top scope
	if vt.Projection != nil {
		rs.Instructions = append(rs.Instructions, vm.Instruction{
			Op:  vm.Projection,
			Arg: vt.Projection,
		})
	}
	if vt.Restrict != nil {
		rs.Instructions = append(rs.Instructions, vm.Instruction{
			Op:  vm.Restrict,
			Arg: vt.Restrict,
		})
	}
	if vt.Top != nil {
		rs.Instructions = append(rs.Instructions, vm.Instruction{
			Op:  vm.Top,
			Arg: vt.Top,
		})
	}
	if vt.Order != nil {
		rs.Instructions = append(rs.Instructions, vm.Instruction{
			Op:  vm.Order,
			Arg: vt.Order,
		})
	}
	if vt.Dedup != nil {
		rs.Instructions = append(rs.Instructions, vm.Instruction{
			Op:  vm.Dedup,
			Arg: vt.Dedup,
		})
	}
	if vt.Offset != nil {
		rs.Instructions = append(rs.Instructions, vm.Instruction{
			Op:  vm.Offset,
			Arg: vt.Offset,
		})
	}
	if vt.Limit != nil {
		rs.Instructions = append(rs.Instructions, vm.Instruction{
			Op:  vm.Limit,
			Arg: vt.Limit,
		})
	}
	attrs := make([]string, len(e.resultCols))
	for i, col := range e.resultCols {
		attrs[i] = col.Name
	}
	rs.Instructions = append(rs.Instructions, vm.Instruction{
		Op: vm.UpdateTag,
		Arg: &updateTag.Argument{
			Relation: rel,
			AffectedRows: 0,
		},
	})
	return rs, nil
}

func depth(vs []*vtree.View) int {
	if len(vs) == 0 {
		return 0
	}
	d := 0
	flg := false
	for _, v := range vs {
		if len(v.Children) > 0 {
			flg = true
			if d0 := depth(v.Children); d0 > d {
				d = d0
			}
		}
	}
	if flg {
		return d + 1
	}
	return d
}
