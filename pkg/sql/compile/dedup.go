package compile

import (
	vdedup "matrixone/pkg/sql/colexec/dedup"
	"matrixone/pkg/sql/colexec/mergededup"
	"matrixone/pkg/sql/colexec/transfer"
	"matrixone/pkg/sql/op/dedup"
	"matrixone/pkg/vm"
	"matrixone/pkg/vm/mmu/guest"
	"matrixone/pkg/vm/process"
	"sync"
)

func (c *compile) compileDedup(o *dedup.Dedup, mp map[string]uint64) ([]*Scope, error) {
	{
		for _, g := range o.Gs {
			mp[g.Name]++
		}
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
				Ch: make(chan interface{}),
			}
		}
	}
	attrs := make([]string, len(o.Gs))
	{
		for i, g := range o.Gs {
			attrs[i] = g.Name
		}
	}
	for i, s := range ss {
		s.Ins = append(s.Ins, vm.Instruction{
			Op:  vm.Dedup,
			Arg: &vdedup.Argument{Attrs: attrs},
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
		Op:  vm.MergeDedup,
		Arg: &mergededup.Argument{Attrs: attrs},
	})
	return []*Scope{rs}, nil
}
