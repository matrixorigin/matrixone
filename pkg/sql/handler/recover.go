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
	gm := guest.New(proc.Gm.Limit, proc.Gm.Mmu)
	s.Proc = process.New(gm, proc.Mp)
	s.Proc.Lim = proc.Lim
	s.Proc.Reg.Ws = make([]*process.WaitRegister, len(ps.Ss))
	{
		for i, j := 0, len(ps.Ss); i < j; i++ {
			s.Proc.Reg.Ws[i] = &process.WaitRegister{
				Wg: new(sync.WaitGroup),
				Ch: make(chan interface{}),
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
		ps.Ss[i].Ins = recoverInstructions(ps.Ss[i].Ins, gm, s.Proc.Reg.Ws[i])
		s.Ss[i] = recoverScope(ps.Ss[i], proc)
	}
	return s
}

func recoverInstructions(ins vm.Instructions, mmu *guest.Mmu, reg *process.WaitRegister) vm.Instructions {
	for i, in := range ins {
		if in.Op == vm.Transfer {
			in.Arg = &transfer.Argument{Mmu: mmu, Reg: reg}
		}
		ins[i] = in
	}
	return ins
}
