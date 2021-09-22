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
	"matrixone/pkg/sql/colexec/mergetop"
	vtop "matrixone/pkg/sql/colexec/top"
	"matrixone/pkg/sql/colexec/transfer"
	"matrixone/pkg/sql/op/projection"
	"matrixone/pkg/sql/op/top"
	"matrixone/pkg/vm"
	"matrixone/pkg/vm/mmu/guest"
	"matrixone/pkg/vm/process"
	"sync"
)

func (c *compile) compileTopOutput(o *top.Top, mp map[string]uint64) ([]*Scope, error) {
	{
		for _, g := range o.Gs {
			mp[g.Name]++
		}
	}
	ss, err := c.compile(o.Prev.(*projection.Projection), mp)
	if err != nil {
		return nil, err
	}
	fs := make([]vtop.Field, len(o.Gs))
	{
		for i, g := range o.Gs {
			fs[i] = vtop.Field{
				Attr: g.Name,
				Type: vtop.Direction(g.Dirt),
			}
		}
	}
	rs := new(Scope)
	rs.Proc = process.New(guest.New(c.proc.Gm.Limit, c.proc.Gm.Mmu))
	rs.Proc.Lim = c.proc.Lim
	rs.Proc.Reg.MergeReceivers = make([]*process.WaitRegister, len(ss))
	{
		for i, j := 0, len(ss); i < j; i++ {
			rs.Proc.Reg.MergeReceivers[i] = &process.WaitRegister{
				Wg: new(sync.WaitGroup),
				Ch: make(chan interface{}, 8),
			}
		}
	}
	if o.IsPD {
		arg := &vtop.Argument{Fs: fs, Limit: o.Limit}
		for i, s := range ss {
			ss[i] = pushTop(s, arg)
		}
	}
	for i, s := range ss {
		ss[i].Instructions = append(s.Instructions, vm.Instruction{
			Code: vm.Transfer,
			Arg: &transfer.Argument{
				Proc: rs.Proc,
				Reg:  rs.Proc.Reg.MergeReceivers[i],
			},
		})
	}
	rs.PreScopes = ss
	rs.Magic = Merge
	mfs := make([]mergetop.Field, len(o.Gs))
	{
		for i, g := range o.Gs {
			mfs[i] = mergetop.Field{
				Attr: g.Name,
				Type: mergetop.Direction(g.Dirt),
			}
		}
	}
	rs.Instructions = append(rs.Instructions, vm.Instruction{
		Code: vm.MergeTop,
		Arg: &mergetop.Argument{
			Fs:    mfs,
			Limit: o.Limit,
		},
	})
	return []*Scope{rs}, nil
}

func (c *compile) compileTop(o *top.Top, mp map[string]uint64) ([]*Scope, error) {
	{
		for _, g := range o.Gs {
			mp[g.Name]++
		}
	}
	ss, err := c.compile(o.Prev, mp)
	if err != nil {
		return nil, err
	}
	fs := make([]vtop.Field, len(o.Gs))
	{
		for i, g := range o.Gs {
			fs[i] = vtop.Field{
				Attr: g.Name,
				Type: vtop.Direction(g.Dirt),
			}
		}
	}
	rs := new(Scope)
	rs.Proc = process.New(guest.New(c.proc.Gm.Limit, c.proc.Gm.Mmu))
	rs.Proc.Lim = c.proc.Lim
	rs.Proc.Reg.MergeReceivers = make([]*process.WaitRegister, len(ss))
	{
		for i, j := 0, len(ss); i < j; i++ {
			rs.Proc.Reg.MergeReceivers[i] = &process.WaitRegister{
				Wg: new(sync.WaitGroup),
				Ch: make(chan interface{}, 8),
			}
		}
	}
	if o.IsPD {
		arg := &vtop.Argument{Fs: fs, Limit: o.Limit}
		for i, s := range ss {
			ss[i] = pushTop(s, arg)
		}
	}
	for i, s := range ss {
		ss[i].Instructions = append(s.Instructions, vm.Instruction{
			Code: vm.Transfer,
			Arg: &transfer.Argument{
				Proc: rs.Proc,
				Reg:  rs.Proc.Reg.MergeReceivers[i],
			},
		})
	}
	rs.PreScopes = ss
	rs.Magic = Merge
	mfs := make([]mergetop.Field, len(o.Gs))
	{
		for i, g := range o.Gs {
			mfs[i] = mergetop.Field{
				Attr: g.Name,
				Type: mergetop.Direction(g.Dirt),
			}
		}
	}
	rs.Instructions = append(rs.Instructions, vm.Instruction{
		Code: vm.MergeTop,
		Arg: &mergetop.Argument{
			Fs:    mfs,
			Limit: o.Limit,
		},
	})
	return []*Scope{rs}, nil
}

func pushTop(s *Scope, arg *vtop.Argument) *Scope {
	if s.Magic == Merge || s.Magic == Remote {
		for i := range s.PreScopes {
			s.PreScopes[i] = pushTop(s.PreScopes[i], arg)
		}
		fs := make([]mergetop.Field, len(arg.Fs))
		{
			for i, f := range arg.Fs {
				fs[i] = mergetop.Field{
					Attr: f.Attr,
					Type: mergetop.Direction(f.Type),
				}
			}
		}
		s.Instructions[len(s.Instructions)-1] = vm.Instruction{
			Code: vm.MergeTop,
			Arg: &mergetop.Argument{
				Fs:    fs,
				Flg:   true,
				Limit: arg.Limit,
			},
		}
	} else {
		n := len(s.Instructions) - 1
		s.Instructions = append(s.Instructions, vm.Instruction{
			Arg:  arg,
			Code: vm.Top,
		})
		s.Instructions[n], s.Instructions[n+1] = s.Instructions[n+1], s.Instructions[n]
	}
	return s
}
