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
	"matrixone/pkg/sql/colexec/output"
	"matrixone/pkg/sql/colexec/transfer"
	"matrixone/pkg/vm"
	"matrixone/pkg/vm/mmu/guest"
	"matrixone/pkg/vm/process"
	"sync"
)

func fillOutput(ss []*Scope, arg *output.Argument, proc *process.Process) []*Scope {
	if len(ss) == 0 {
		return ss
	}
	if ss[0].Magic > Remote {
		return ss
	}
	if ss[0].Magic == Merge {
		for i, s := range ss {
			ss[i].Ins = append(s.Ins, vm.Instruction{
				Arg: arg,
				Op:  vm.Output,
			})
		}
		return ss
	}
	rs := new(Scope)
	gm := guest.New(proc.Gm.Limit, proc.Gm.Mmu)
	rs.Proc = process.New(gm, proc.Mp)
	rs.Proc.Lim = proc.Lim
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
				Mmu: gm,
				Reg: rs.Proc.Reg.Ws[i],
			},
		})
	}
	rs.Ss = ss
	rs.Magic = Merge
	rs.Ins = append(rs.Ins, vm.Instruction{
		Op:  vm.Merge,
		Arg: &merge.Argument{},
	})
	rs.Ins = append(rs.Ins, vm.Instruction{
		Arg: arg,
		Op:  vm.Output,
	})
	return []*Scope{rs}
}
