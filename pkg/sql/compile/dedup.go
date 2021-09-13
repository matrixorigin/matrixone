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
	vdedup "matrixone/pkg/sql/colexec/dedup"
	"matrixone/pkg/sql/colexec/mergededup"
	"matrixone/pkg/sql/colexec/transfer"
	"matrixone/pkg/sql/op/dedup"
	"matrixone/pkg/vm"
	"matrixone/pkg/vm/mmu/guest"
	"matrixone/pkg/vm/process"
	"sync"
)

func (c *compile) compileDedup(o *dedup.Dedup, mp map[string]uint64) ([]*Scope, error) {
	{
		for _, g := range o.Gs {
			mp[g.Name]++
		}
	}
	ss, err := c.compile(o.Prev, mp)
	if err != nil {
		return nil, err
	}
	attrs := make([]string, len(o.Gs))
	{
		for i, g := range o.Gs {
			attrs[i] = g.Name
		}
	}
	rs := new(Scope)
	rs.Proc = process.New(guest.New(c.proc.Gm.Limit, c.proc.Gm.Mmu))
	rs.Proc.Lim = c.proc.Lim
	rs.Proc.Reg.Ws = make([]*process.WaitRegister, len(ss))
	{
		for i, j := 0, len(ss); i < j; i++ {
			rs.Proc.Reg.Ws[i] = &process.WaitRegister{
				Wg: new(sync.WaitGroup),
				Ch: make(chan interface{}, 8),
			}
		}
	}
	if o.IsPD {
		arg := &vdedup.Argument{Attrs: attrs}
		for i, s := range ss {
			ss[i] = pushDedup(s, arg)
		}
	}
	for i, s := range ss {
		ss[i].Ins = append(s.Ins, vm.Instruction{
			Op: vm.Transfer,
			Arg: &transfer.Argument{
				Proc: rs.Proc,
				Reg:  rs.Proc.Reg.Ws[i],
			},
		})

	}
	rs.Ss = ss
	rs.Magic = Merge
	rs.Ins = append(rs.Ins, vm.Instruction{
		Op:  vm.MergeDedup,
		Arg: &mergededup.Argument{Attrs: attrs},
	})
	return []*Scope{rs}, nil
}

func pushDedup(s *Scope, arg *vdedup.Argument) *Scope {
	if s.Magic == Merge || s.Magic == Remote {
		for i := range s.Ss {
			s.Ss[i] = pushDedup(s.Ss[i], arg)
		}
		s.Ins[len(s.Ins)-1] = vm.Instruction{
			Op: vm.MergeDedup,
			Arg: &mergededup.Argument{
				Flg:   true,
				Attrs: arg.Attrs,
			},
		}
	} else {
		n := len(s.Ins) - 1
		s.Ins = append(s.Ins, vm.Instruction{
			Arg: arg,
			Op:  vm.Dedup,
		})
		s.Ins[n], s.Ins[n+1] = s.Ins[n+1], s.Ins[n]
	}
	return s
}
