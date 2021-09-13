package compile

import (
	vlimit "matrixone/pkg/sql/colexec/limit"
	"matrixone/pkg/sql/colexec/merge"
	voffset "matrixone/pkg/sql/colexec/offset"
	"matrixone/pkg/sql/colexec/transfer"
	"matrixone/pkg/sql/op/limit"
	"matrixone/pkg/sql/op/offset"
	"matrixone/pkg/vm"
	"matrixone/pkg/vm/mmu/guest"
	"matrixone/pkg/vm/process"
	"sync"
)

func (c *compile) compileFetch(o *limit.Limit, mp map[string]uint64) ([]*Scope, error) {
	prev := o.Prev.(*offset.Offset)
	ss, err := c.compile(prev.Prev, mp)
	if err != nil {
		return nil, err
	}
	rs := new(Scope)
	rs.Proc = process.New(guest.New(c.proc.Gm.Limit, c.proc.Gm.Mmu))
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
		Op: vm.Offset,
		Arg: &voffset.Argument{
			Offset: uint64(prev.Offset),
		},
	})
	rs.Ins = append(rs.Ins, vm.Instruction{
		Op: vm.Limit,
		Arg: &vlimit.Argument{
			Limit: uint64(o.Limit),
		},
	})
	return []*Scope{rs}, nil
}
