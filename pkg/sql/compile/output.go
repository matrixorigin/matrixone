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
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/merge"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/output"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/transfer"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/guest"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"sync"
)

// fillOutput
func fillOutput(ss []*Scope, arg *output.Argument, proc *process.Process) []*Scope {
	if len(ss) == 0 {
		return ss
	}
	// illegal Magic
	// TODO: need refactor
	if ss[0].Magic > Remote {
		return ss
	}
	if ss[0].Magic == Merge {
		// add output instruction for each scope.
		for i, s := range ss {
			ss[i].Instructions = append(s.Instructions, vm.Instruction{
				Arg:  arg,
				Code: vm.Output,
			})
		}
		return ss
	}
	// unreached code for now
	rs := new(Scope)
	rs.Proc = process.New(guest.New(proc.Gm.Limit, proc.Gm.Mmu))
	rs.Proc.Lim = proc.Lim
	rs.Proc.Reg.MergeReceivers = make([]*process.WaitRegister, len(ss))
	{
		for i, j := 0, len(ss); i < j; i++ {
			rs.Proc.Reg.MergeReceivers[i] = &process.WaitRegister{
				Wg: new(sync.WaitGroup),
				Ch: make(chan interface{}, 8),
			}
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
		Arg:  arg,
		Code: vm.Output,
	})
	return []*Scope{rs}
}
