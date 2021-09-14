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

package handler

import (
	"matrixone/pkg/sql/colexec/transfer"
	"matrixone/pkg/sql/compile"
	"matrixone/pkg/sql/op/relation"
	"matrixone/pkg/sql/protocol"
	"matrixone/pkg/vm"
	"matrixone/pkg/vm/mmu/guest"
	"matrixone/pkg/vm/process"
	"sync"
)

func recoverScope(ps protocol.Scope, proc *process.Process) *compile.Scope {
	s := new(compile.Scope)
	s.Ins = ps.Ins
	s.Magic = ps.Magic
	if s.Magic == compile.Remote {
		s.Magic = compile.Merge
	}
	s.Proc = process.New(guest.New(proc.Gm.Limit, proc.Gm.Mmu))
	s.Proc.Lim = proc.Lim
	s.Proc.Reg.MergeReceivers = make([]*process.WaitRegister, len(ps.Ss))
	{
		for i, j := 0, len(ps.Ss); i < j; i++ {
			s.Proc.Reg.MergeReceivers[i] = &process.WaitRegister{
				Wg: new(sync.WaitGroup),
				Ch: make(chan interface{}, 8),
			}
		}
	}
	if len(ps.Data.Segs) > 0 {
		s.Data = new(compile.Source)
		s.Data.ID = ps.Data.ID
		s.Data.DB = ps.Data.DB
		s.Data.Refs = ps.Data.Refer
		s.Data.Segs = make([]*relation.Segment, len(ps.Data.Segs))
		for i, seg := range ps.Data.Segs {
			s.Data.Segs[i] = &relation.Segment{
				Id:       seg.Id,
				GroupId:  seg.GroupId,
				Version:  seg.Version,
				IsRemote: seg.IsRemote,
				TabletId: seg.TabletId,
			}
		}
	}
	s.Ss = make([]*compile.Scope, len(ps.Ss))
	for i := range ps.Ss {
		ps.Ss[i].Ins = recoverInstructions(ps.Ss[i].Ins, s.Proc, s.Proc.Reg.MergeReceivers[i])
		s.Ss[i] = recoverScope(ps.Ss[i], proc)
	}
	return s
}

func recoverInstructions(ins vm.Instructions, proc *process.Process, reg *process.WaitRegister) vm.Instructions {
	for i, in := range ins {
		if in.Op == vm.Transfer {
			in.Arg = &transfer.Argument{Proc: proc, Reg: reg}
		}
		ins[i] = in
	}
	return ins
}
