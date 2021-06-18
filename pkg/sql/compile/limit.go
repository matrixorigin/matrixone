package compile

import (
	vlimit "matrixone/pkg/sql/colexec/limit"
	"matrixone/pkg/sql/colexec/transfer"
	"matrixone/pkg/sql/op/limit"
	"matrixone/pkg/vm"
	"matrixone/pkg/vm/mmu/guest"
	"matrixone/pkg/vm/process"
	"sync"
)

func (c *compile) compileLimit(o *limit.Limit, mp map[string]uint64) ([]*Scope, error) {
	ss, err := c.compile(o.Prev, mp)
	if err != nil {
		return nil, err
	}
	if len(ss) == 1 {
		ss[0].Ins = append(ss[0].Ins, vm.Instruction{
			Op: vm.Limit,
			Arg: &vlimit.Argument{
				Limit: uint64(o.Limit),
			},
		})
		return ss, nil
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
				Ch: make(chan interface{}),
			}
		}
	}
	for i, s := range ss {
		s.Ins = append(s.Ins, vm.Instruction{
			Op: vm.Limit,
			Arg: &vlimit.Argument{
				Limit: uint64(o.Limit),
			},
		})
		s.Ins = append(s.Ins, vm.Instruction{
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
		Op: vm.Merge,
	})
	rs.Ins = append(rs.Ins, vm.Instruction{
		Op: vm.Limit,
		Arg: &vlimit.Argument{
			Limit: uint64(o.Limit),
		},
	})
	return []*Scope{rs}, nil
}
