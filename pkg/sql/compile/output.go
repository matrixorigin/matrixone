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
	rs.Proc = process.New(guest.New(proc.Gm.Limit, proc.Gm.Mmu))
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
				Proc: rs.Proc,
				Reg:  rs.Proc.Reg.Ws[i],
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
