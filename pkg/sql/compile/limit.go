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
	vlimit "github.com/matrixorigin/matrixone/pkg/sql/colexec/limit"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/merge"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/transfer"
	"github.com/matrixorigin/matrixone/pkg/sql/op/limit"
	"github.com/matrixorigin/matrixone/pkg/sql/op/offset"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/guest"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"sync"
)

func (c *compile) compileLimit(o *limit.Limit, mp map[string]uint64) ([]*Scope, error) {
	if _, ok := o.Prev.(*offset.Offset); ok {
		return c.compileFetch(o, mp)
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
		arg := &vlimit.Argument{Limit: uint64(o.Limit)}
		for i, s := range ss {
			ss[i] = pushLimit(s, arg)
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
	rs.Instructions = append(rs.Instructions, vm.Instruction{
		Code: vm.Merge,
		Arg:  &merge.Argument{},
	})
	rs.Instructions = append(rs.Instructions, vm.Instruction{
		Code: vm.Limit,
		Arg: &vlimit.Argument{
			Limit: uint64(o.Limit),
		},
	})
	return []*Scope{rs}, nil
}

func pushLimit(s *Scope, arg *vlimit.Argument) *Scope {
	if s.Magic == Merge || s.Magic == Remote {
		for i := range s.PreScopes {
			s.PreScopes[i] = pushLimit(s.PreScopes[i], arg)
		}
	}
	n := len(s.Instructions) - 1
	s.Instructions = append(s.Instructions, vm.Instruction{
		Arg:  arg,
		Code: vm.Limit,
	})
	s.Instructions[n], s.Instructions[n+1] = s.Instructions[n+1], s.Instructions[n]
	return s
}
