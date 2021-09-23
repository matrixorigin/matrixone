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
	"matrixone/pkg/sql/colexec/bag/inner"
	"matrixone/pkg/sql/colexec/merge"
	"matrixone/pkg/sql/colexec/transfer"
	"matrixone/pkg/sql/op/innerJoin"
	"matrixone/pkg/vm"
	"matrixone/pkg/vm/mmu/guest"
	"matrixone/pkg/vm/process"
	"strings"
	"sync"
)

func (c *compile) compileInnerJoin(o *innerJoin.Join, mp map[string]uint64) ([]*Scope, error) {
	rmp, smp := make(map[string]uint64), make(map[string]uint64)
	{
		ap := o.R.Attribute()
		for k, v := range mp {
			ss := strings.Split(k, ".")
			if ss[0] != o.R.String() {
				continue
			}
			if _, ok := ap[ss[1]]; ok {
				rmp[ss[1]] = v
			}
		}
		for _, attr := range o.Rattrs {
			rmp[attr]++
		}
	}
	{
		ap := o.S.Attribute()
		for k, v := range mp {
			ss := strings.Split(k, ".")
			if ss[0] != o.S.String() {
				continue
			}
			if _, ok := ap[ss[1]]; ok {
				smp[ss[1]] = v
			}
		}
		for _, attr := range o.Sattrs {
			smp[attr]++
		}
	}
	rs, err := c.compile(o.R, rmp)
	if err != nil {
		return nil, err
	}
	ss, err := c.compile(o.S, smp)
	if err != nil {
		return nil, err
	}
	s := new(Scope)
	s.Proc = process.New(guest.New(c.proc.Gm.Limit, c.proc.Gm.Mmu))
	s.Proc.Lim = c.proc.Lim
	s.Proc.Reg.MergeReceivers = make([]*process.WaitRegister, 2)
	{
		s.Proc.Reg.MergeReceivers[0] = &process.WaitRegister{
			Wg: new(sync.WaitGroup),
			Ch: make(chan interface{}, 8),
		}
		s.Proc.Reg.MergeReceivers[1] = &process.WaitRegister{
			Wg: new(sync.WaitGroup),
			Ch: make(chan interface{}, 8),
		}
	}
	rms := new(Scope)
	{
		rms.Proc = process.New(guest.New(c.proc.Gm.Limit, c.proc.Gm.Mmu))
		rms.Proc.Lim = c.proc.Lim
		rms.Proc.Reg.MergeReceivers = make([]*process.WaitRegister, len(rs))
		{
			for i, j := 0, len(rs); i < j; i++ {
				rms.Proc.Reg.MergeReceivers[i] = &process.WaitRegister{
					Wg: new(sync.WaitGroup),
					Ch: make(chan interface{}, 8),
				}
			}
		}
		{
			for i, s := range rs {
				s.Instructions = append(s.Instructions, vm.Instruction{
					Code: vm.Transfer,
					Arg: &transfer.Argument{
						Proc: rms.Proc,
						Reg:  rms.Proc.Reg.MergeReceivers[i],
					},
				})
			}
		}
		rms.PreScopes = rs
		rms.Magic = Merge
		rms.Instructions = append(rms.Instructions, vm.Instruction{
			Code: vm.Merge,
			Arg:  &merge.Argument{},
		})
		rms.Instructions = append(rms.Instructions, vm.Instruction{
			Code: vm.Transfer,
			Arg: &transfer.Argument{
				Proc: s.Proc,
				Reg:  s.Proc.Reg.MergeReceivers[0],
			},
		})
	}
	sms := new(Scope)
	{
		sms.Proc = process.New(guest.New(c.proc.Gm.Limit, c.proc.Gm.Mmu))
		sms.Proc.Lim = c.proc.Lim
		sms.Proc.Reg.MergeReceivers = make([]*process.WaitRegister, len(ss))
		{
			for i, j := 0, len(ss); i < j; i++ {
				sms.Proc.Reg.MergeReceivers[i] = &process.WaitRegister{
					Wg: new(sync.WaitGroup),
					Ch: make(chan interface{}, 8),
				}
			}
		}
		{
			for i, s := range ss {
				s.Instructions = append(s.Instructions, vm.Instruction{
					Code: vm.Transfer,
					Arg: &transfer.Argument{
						Proc: sms.Proc,
						Reg:  sms.Proc.Reg.MergeReceivers[i],
					},
				})
			}
		}
		sms.PreScopes = ss
		sms.Magic = Merge
		sms.Instructions = append(sms.Instructions, vm.Instruction{
			Code: vm.Merge,
			Arg:  &merge.Argument{},
		})
		sms.Instructions = append(sms.Instructions, vm.Instruction{
			Code: vm.Transfer,
			Arg: &transfer.Argument{
				Proc: s.Proc,
				Reg:  s.Proc.Reg.MergeReceivers[1],
			},
		})
	}
	s.Magic = Merge
	s.PreScopes = []*Scope{rms, sms}
	s.Instructions = append(s.Instructions, vm.Instruction{
		Code: vm.BagInnerJoin,
		Arg: &inner.Argument{
			Rattrs: o.Rattrs,
			Sattrs: o.Sattrs,
			R:      o.R.String(),
			S:      o.S.String(),
		},
	})
	return []*Scope{s}, nil
}
