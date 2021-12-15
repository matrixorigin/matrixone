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
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergeorder"
	vorder "github.com/matrixorigin/matrixone/pkg/sql/colexec/order"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/transfer"
	"github.com/matrixorigin/matrixone/pkg/sql/op/order"
	"github.com/matrixorigin/matrixone/pkg/sql/op/projection"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/guest"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"sync"
)

func (c *compile) compileOrderOutput(o *order.Order, mp map[string]uint64) ([]*Scope, error) {
	{
		for _, g := range o.Attributes {
			mp[g.Name]++
		}
	}
	ss, err := c.compileOutput(o.Prev.(*projection.Projection), mp)
	if err != nil {
		return nil, err
	}
	fs := make([]vorder.Field, len(o.Attributes))
	{
		for i, g := range o.Attributes {
			fs[i] = vorder.Field{
				Attr: g.Name,
				Type: vorder.Direction(g.Dirt),
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
		arg := &vorder.Argument{Fs: fs}
		for i, s := range ss {
			ss[i] = pushOrder(s, arg)
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
	mfs := make([]mergeorder.Field, len(o.Attributes))
	{
		for i, g := range o.Attributes {
			mfs[i] = mergeorder.Field{
				Attr: g.Name,
				Type: mergeorder.Direction(g.Dirt),
			}
		}
	}
	rs.Instructions = append(rs.Instructions, vm.Instruction{
		Code: vm.MergeOrder,
		Arg: &mergeorder.Argument{
			Fs: mfs,
		},
	})
	return []*Scope{rs}, nil
}

func (c *compile) compileOrder(o *order.Order, mp map[string]uint64) ([]*Scope, error) {
	{
		for _, g := range o.Attributes {
			mp[g.Name]++
		}
	}
	ss, err := c.compile(o.Prev, mp)
	if err != nil {
		return nil, err
	}
	fs := make([]vorder.Field, len(o.Attributes))
	{
		for i, g := range o.Attributes {
			fs[i] = vorder.Field{
				Attr: g.Name,
				Type: vorder.Direction(g.Dirt),
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
		arg := &vorder.Argument{Fs: fs}
		for i, s := range ss {
			ss[i] = pushOrder(s, arg)
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
	mfs := make([]mergeorder.Field, len(o.Attributes))
	{
		for i, g := range o.Attributes {
			mfs[i] = mergeorder.Field{
				Attr: g.Name,
				Type: mergeorder.Direction(g.Dirt),
			}
		}
	}
	rs.Instructions = append(rs.Instructions, vm.Instruction{
		Code: vm.MergeOrder,
		Arg: &mergeorder.Argument{
			Fs: mfs,
		},
	})
	return []*Scope{rs}, nil
}

func pushOrder(s *Scope, arg *vorder.Argument) *Scope {
	if s.Magic == Merge || s.Magic == Remote {
		for i := range s.PreScopes {
			s.PreScopes[i] = pushOrder(s.PreScopes[i], arg)
		}
		fs := make([]mergeorder.Field, len(arg.Fs))
		{
			for i, f := range arg.Fs {
				fs[i] = mergeorder.Field{
					Attr: f.Attr,
					Type: mergeorder.Direction(f.Type),
				}
			}
		}
		s.Instructions[len(s.Instructions)-1] = vm.Instruction{
			Code: vm.MergeOrder,
			Arg: &mergeorder.Argument{
				Fs:  fs,
				Flg: true,
			},
		}
	} else {
		n := len(s.Instructions) - 1
		s.Instructions = append(s.Instructions, vm.Instruction{
			Arg:  arg,
			Code: vm.Order,
		})
		s.Instructions[n], s.Instructions[n+1] = s.Instructions[n+1], s.Instructions[n]
	}
	return s
}
