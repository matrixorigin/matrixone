package compile

import (
	vlimit "matrixone/pkg/sql/colexec/limit"
	"matrixone/pkg/sql/colexec/merge"
	"matrixone/pkg/sql/colexec/transfer"
	"matrixone/pkg/sql/op/limit"
	"matrixone/pkg/sql/op/offset"
	"matrixone/pkg/vm"
	"matrixone/pkg/vm/mmu/guest"
	"matrixone/pkg/vm/process"
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
	gm := guest.New(c.proc.Gm.Limit, c.proc.Gm.Mmu)
	rs.Proc = process.New(gm, c.proc.Mp)
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
		arg := &vlimit.Argument{Limit: uint64(o.Limit)}
		for i, s := range ss {
			ss[i] = pushLimit(s, arg)
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
		Op: vm.Limit,
		Arg: &vlimit.Argument{
			Limit: uint64(o.Limit),
		},
	})
	return []*Scope{rs}, nil
}

func pushLimit(s *Scope, arg *vlimit.Argument) *Scope {
	if s.Magic == Merge || s.Magic == Remote {
		for i := range s.Ss {
			s.Ss[i] = pushLimit(s.Ss[i], arg)
		}
		s.Ins[len(s.Ins)-1] = vm.Instruction{
			Op: vm.Limit,
			Arg: &vlimit.Argument{
				Limit: arg.Limit,
			},
		}
	} else {
		n := len(s.Ins) - 1
		s.Ins = append(s.Ins, vm.Instruction{
			Arg: arg,
			Op:  vm.Limit,
		})
		s.Ins[n], s.Ins[n+1] = s.Ins[n+1], s.Ins[n]
	}
	return s
}
