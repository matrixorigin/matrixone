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
	"matrixone/pkg/sql/colexec/merge"
	"matrixone/pkg/sql/colexec/transfer"
	"matrixone/pkg/sql/op/relation"
	"matrixone/pkg/vm"
	"matrixone/pkg/vm/mmu/guest"
	"matrixone/pkg/vm/process"
	"sync"
)

func (c *compile) compileRelation(o *relation.Relation, mp map[string]uint64) ([]*Scope, error) {
	if len(o.Us) == 0 {
		return nil, nil
	}
	ss := make([]*Scope, len(o.Us))
	for i, u := range o.Us {
		ss[i] = c.compileUnit(u, o, mp)
	}
	return ss, nil
}

func (c *compile) compileUnit(u *relation.Unit, o *relation.Relation, mp map[string]uint64) *Scope {
	n := len(u.Segs)
	mcpu := c.e.Node(u.N.Id).Mcpu
	if n < mcpu {
		ss := make([]*Scope, n)
		for i, seg := range u.Segs {
			proc := process.New(guest.New(c.proc.Gm.Limit, c.proc.Gm.Mmu))
			proc.Lim = c.proc.Lim
			ss[i] = &Scope{
				Proc:  proc,
				Magic: Normal,
				Data: &Source{
					Refs: mp,
					DB:   o.DB,
					ID:   o.Rid,
					Segs: []*relation.Segment{seg},
				},
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
		rs.N = u.N
		rs.Magic = Remote
		rs.Ins = append(rs.Ins, vm.Instruction{
			Op:  vm.Merge,
			Arg: &merge.Argument{},
		})
		return rs
	}
	m := n / mcpu
	segs := u.Segs
	ss := make([]*Scope, mcpu)
	for i := 0; i < mcpu; i++ {
		proc := process.New(guest.New(c.proc.Gm.Limit, c.proc.Gm.Mmu))
		proc.Lim = c.proc.Lim
		if i == mcpu-1 {
			ss[i] = &Scope{
				Proc:  proc,
				Magic: Normal,
				Data: &Source{
					Refs: mp,
					DB:   o.DB,
					ID:   o.Rid,
					Segs: segs[i*m:],
				},
			}
		} else {
			ss[i] = &Scope{
				Proc:  proc,
				Magic: Normal,
				Data: &Source{
					Refs: mp,
					DB:   o.DB,
					ID:   o.Rid,
					Segs: segs[i*m : (i+1)*m],
				},
			}
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
	rs.N = u.N
	rs.Magic = Remote
	rs.Ins = append(rs.Ins, vm.Instruction{
		Op:  vm.Merge,
		Arg: &merge.Argument{},
	})
	return rs
}
