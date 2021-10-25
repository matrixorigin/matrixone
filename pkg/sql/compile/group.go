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
	vgroup "github.com/matrixorigin/matrixone/pkg/sql/colexec/group"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergegroup"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/transfer"
	"github.com/matrixorigin/matrixone/pkg/sql/op/group"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/guest"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"sync"
)

func (c *compile) compileGroup(o *group.Group, mp map[string]uint64) ([]*Scope, error) {
	var gs []string

	refer := make(map[string]uint64)
	{
		for _, g := range o.Gs {
			gs = append(gs, g.Name)
			mp[g.Name]++
		}
		for _, attr := range o.As {
			if v, ok := mp[attr]; ok {
				refer[attr] = v + 1
				delete(mp, attr)
			} else {
				refer[attr]++
			}
		}
		for _, e := range o.Es {
			mp[e.Name]++
		}
	}
	ss, err := c.compile(o.Prev, mp)
	if err != nil {
		return nil, err
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
		for i, s := range ss {
			ss[i] = pushGroup(s, refer, gs, o)
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
	if o.IsPD {
		rs.Instructions = append(rs.Instructions, vm.Instruction{
			Code: vm.MergeGroup,
			Arg: &mergegroup.Argument{
				Gs:    gs,
				Refer: refer,
				Es:    mergeAggregates(o.Es),
			},
		})
	} else {
		rs.Instructions = append(rs.Instructions, vm.Instruction{
			Code: vm.MergeGroup,
			Arg: &mergegroup.Argument{
				Gs:    gs,
				Refer: refer,
				Es:    unitAggregates(o.Es),
			},
		})
	}
	return []*Scope{rs}, nil
}

func pushGroup(s *Scope, refer map[string]uint64, gs []string, o *group.Group) *Scope {
	if s.Magic == Merge || s.Magic == Remote {
		for i := range s.PreScopes {
			s.PreScopes[i] = pushGroup(s.PreScopes[i], refer, gs, o)
		}
		s.Instructions[len(s.Instructions)-1] = vm.Instruction{
			Code: vm.MergeGroup,
			Arg: &mergegroup.Argument{
				Gs:    gs,
				Flg:   true,
				Refer: refer,
				Es:    remoteAggregates(o.Es),
			},
		}
	} else {
		n := len(s.Instructions) - 1
		s.Instructions = append(s.Instructions, vm.Instruction{
			Code: vm.Group,
			Arg: &vgroup.Argument{
				Gs:    gs,
				Refer: refer,
				Es:    unitAggregates(o.Es),
			},
		})
		s.Instructions[n], s.Instructions[n+1] = s.Instructions[n+1], s.Instructions[n]
	}
	return s
}
