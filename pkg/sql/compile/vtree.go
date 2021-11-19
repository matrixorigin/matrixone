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
	"matrixone/pkg/container/batch"
	"matrixone/pkg/errno"
	"matrixone/pkg/sql/colexec/connector"
	"matrixone/pkg/sql/colexec/merge"
	"matrixone/pkg/sql/colexec/output"
	"matrixone/pkg/sql/errors"
	"matrixone/pkg/sql/viewexec/plus"
	"matrixone/pkg/sql/viewexec/times"
	"matrixone/pkg/sql/viewexec/transform"
	"matrixone/pkg/sql/viewexec/untransform"
	"matrixone/pkg/sql/vtree"
	"matrixone/pkg/vm"
	"matrixone/pkg/vm/mheap"
	"matrixone/pkg/vm/mmu/guest"
	"matrixone/pkg/vm/process"
)

func (e *Exec) compileVTree(vt *vtree.ViewTree, varsMap map[string]int) (*Scope, error) {
	isB := vt.IsBare()
	if isB {
		return nil, errors.New(errno.SQLStatementNotYetComplete, "not support now")
	}
	s, err := e.compileView(vt.Views, varsMap)
	if err != nil {
		return nil, err
	}
	switch {
	case depth(vt.Views) == 1:
		s.Instructions = append(s.Instructions, vm.Instruction{
			Op: vm.UnTransform,
			Arg: &untransform.Argument{
				FreeVars: vt.FreeVars,
				Type:     untransform.Single,
			},
		})
	case isB:
		s.Instructions = append(s.Instructions, vm.Instruction{
			Op: vm.UnTransform,
			Arg: &untransform.Argument{
				FreeVars: vt.FreeVars,
				Type:     untransform.Bare,
			},
		})
	default:
		s.Instructions = append(s.Instructions, vm.Instruction{
			Op: vm.UnTransform,
			Arg: &untransform.Argument{
				FreeVars: vt.FreeVars,
				Type:     untransform.FreeVarsAndBoundVars,
			},
		})
	}
	if vt.Projection != nil {
		s.Instructions = append(s.Instructions, vm.Instruction{
			Op:  vm.Projection,
			Arg: vt.Projection,
		})
	}
	if vt.Restrict != nil {
		s.Instructions = append(s.Instructions, vm.Instruction{
			Op:  vm.Restrict,
			Arg: vt.Restrict,
		})
	}
	if vt.Top != nil {
		s.Instructions = append(s.Instructions, vm.Instruction{
			Op:  vm.Top,
			Arg: vt.Top,
		})
	}
	if vt.Order != nil {
		s.Instructions = append(s.Instructions, vm.Instruction{
			Op:  vm.Order,
			Arg: vt.Order,
		})
	}
	if vt.Offset != nil {
		s.Instructions = append(s.Instructions, vm.Instruction{
			Op:  vm.Offset,
			Arg: vt.Offset,
		})
	}
	if vt.Limit != nil {
		s.Instructions = append(s.Instructions, vm.Instruction{
			Op:  vm.Limit,
			Arg: vt.Limit,
		})
	}
	if vt.Dedup != nil {
		s.Instructions = append(s.Instructions, vm.Instruction{
			Op:  vm.Dedup,
			Arg: vt.Dedup,
		})
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
	return s, nil
}

func (e *Exec) compileView(vs []*vtree.View, varsMap map[string]int) (*Scope, error) {
	var isM bool
	var ss []*Scope

	arg := &times.Argument{
		VarsMap: varsMap,
	}
	for i := 0; i < len(vs)-1; i++ {
		arg.Rvars = append(arg.Rvars, vs[i].Var.Name)
		if n := len(vs[i].Children); n > 0 {
			isM = true
			s, err := e.compileView(vs[i].Children, varsMap)
			if err != nil {
				return nil, err
			}
			ss = append(ss, s)
			arg.Ss = append(arg.Ss, vs[i].Children[n-1].Rel.Alias)
			arg.Svars = append(arg.Svars, vs[i].Children[0].Var.Name)
		}
	}
	if len(ss) == 0 {
		return e.compileBaseView(isM, vs[len(vs)-1])
	}
	s, err := e.compileBaseView(isM, vs[len(vs)-1])
	if err != nil {
		return nil, err
	}
	ss = append([]*Scope{s}, ss...)
	arg.R = vs[len(vs)-1].Rel.Alias
	rs := &Scope{
		PreScopes: ss,
		Magic:     Merge,
		Instructions: vm.Instructions{vm.Instruction{
			Arg: arg,
			Op:  vm.Times,
		}},
	}
	ctx, cancel := context.WithCancel(context.Background())
	rs.Proc = process.New(mheap.New(guest.New(e.c.proc.Mp.Gm.Limit, e.c.proc.Mp.Gm.Mmu)))
	rs.Proc.Cancel = cancel
	rs.Proc.Id = e.c.proc.Id
	rs.Proc.Lim = e.c.proc.Lim
	rs.Proc.Reg.MergeReceivers = make([]*process.WaitRegister, len(ss))
	for i := 0; i < len(ss); i++ {
		rs.Proc.Reg.MergeReceivers[i] = &process.WaitRegister{
			Ctx: ctx,
			Ch:  make(chan *batch.Batch, 4),
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

func (e *Exec) compileBaseView(isM bool, v *vtree.View) (*Scope, error) {
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
		IsMerge:      isM,
		RelationName: v.Rel.Name,
		SchemaName:   v.Rel.Schema,
		RefCounts:    make([]uint64, len(v.Rel.Vars)),
		Attributes:   make([]string, len(v.Rel.Vars)),
	}
	for i := range v.Rel.Vars {
		src.Attributes[i] = v.Rel.Vars[i].Name
		src.RefCounts[i] = uint64(v.Rel.Vars[i].Ref)
	}
	v.Arg.IsMerge = isM
	switch {
	case len(v.Arg.FreeVars) != 0:
		v.Arg.Typ = transform.FreeVarsAndBoundVars
	case len(v.Arg.FreeVars) == 0 && len(v.Arg.BoundVars) != 0:
		v.Arg.Typ = transform.BoundVars
	default:
		v.Arg.Typ = transform.Bare
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
	if len(ss) == 1 {
		return ss[0], nil
	}
	rs := &Scope{
		PreScopes: ss,
		Magic:     Merge,
	}
	if isM {
		rs.Instructions = append(rs.Instructions, vm.Instruction{
			Op:  vm.Merge,
			Arg: &merge.Argument{},
		})
	} else {
		rs.Instructions = append(rs.Instructions, vm.Instruction{
			Op:  vm.Plus,
			Arg: &plus.Argument{Typ: v.Arg.Typ},
		})
	}
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
				Ch:  make(chan *batch.Batch, 4),
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

func depth(vs []*vtree.View) int {
	if len(vs) == 0 {
		return 0
	}
	d := 0
	for _, v := range vs {
		if len(v.Children) > 0 {
			if d0 := depth(v.Children); d0 > d {
				d = d0
			}
		}
	}
	return d + 1
}
