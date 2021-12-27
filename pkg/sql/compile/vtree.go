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
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/output"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/splice"
	"github.com/matrixorigin/matrixone/pkg/sql/errors"
	"github.com/matrixorigin/matrixone/pkg/sql/viewexec/oplus"
	"github.com/matrixorigin/matrixone/pkg/sql/viewexec/times"
	"github.com/matrixorigin/matrixone/pkg/sql/viewexec/transform"
	"github.com/matrixorigin/matrixone/pkg/sql/viewexec/untransform"
	"github.com/matrixorigin/matrixone/pkg/sql/vtree"
	"github.com/matrixorigin/matrixone/pkg/vm"
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
	isB := vt.IsBare()
	d := depth(vt.Views)
	switch {
	case d == 0 && isB:
		if s, err = e.compileQ(vt.Views[len(vt.Views)-1]); err != nil {
			return nil, err
		}
	case d == 0 && !isB:
		if s, err = e.compileAQ(vt.Views[len(vt.Views)-1]); err != nil {
			return nil, err
		}
	case d > 0 && !isB:
		if d > 1 { // only for test
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
	case d == 0 && isB: // needn't do un-transform for simple query without group by and aggregation
		rs.Instructions = append(rs.Instructions, vm.Instruction{
			Op:  vm.Splice,
			Arg: &splice.Argument{},
		})
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
	if vt.Dedup != nil {
		rs.Instructions = append(rs.Instructions, vm.Instruction{
			Op:  vm.Dedup,
			Arg: vt.Dedup,
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

func (e *Exec) compileQ(v *vtree.View) (*Scope, error) {
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
	ins = append(ins, vm.Instruction{Arg: v.Arg, Op: vm.Transform})

	ns := rel.Nodes()
	if len(ns) == 0 {
		return nil, errors.New(errno.FeatureNotSupported, "not support select from empty table now")
		// todo: should support it when AOE NewReader() return reader correctly
		//rs := &Scope{
		//	PreScopes:    nil,
		//	Magic:        Normal,
		//	DataSource:   src,
		//	Instructions: ins,
		//}
		//rs.DataSource.R = rel.NewReader(1)[0]
		//rs.Proc = process.New(mheap.New(guest.New(e.c.proc.Mp.Gm.Limit, e.c.proc.Mp.Gm.Mmu)))
		//rs.Proc.Id = e.c.proc.Id
		//rs.Proc.Lim = e.c.proc.Lim
		//return rs, nil
	}
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

}

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
