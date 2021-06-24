package compile

import (
	"matrixone/pkg/sql/colexec/bag/inner"
	"matrixone/pkg/sql/colexec/merge"
	"matrixone/pkg/sql/colexec/transfer"
	"matrixone/pkg/sql/op/innerJoin"
	"matrixone/pkg/vm"
	"matrixone/pkg/vm/mmu/guest"
	"matrixone/pkg/vm/process"
	"strings"
	"sync"
)

func (c *compile) compileInnerJoin(o *innerJoin.Join, mp map[string]uint64) ([]*Scope, error) {
	rmp, smp := make(map[string]uint64), make(map[string]uint64)
	{
		ap := o.R.Attribute()
		for k, v := range mp {
			ss := strings.Split(k, ".")
			if ss[0] != o.R.String() {
				continue
			}
			if _, ok := ap[ss[1]]; ok {
				rmp[ss[1]] = v
			}
		}
		for _, attr := range o.Rattrs {
			rmp[attr]++
		}
	}
	{
		ap := o.S.Attribute()
		for k, v := range mp {
			ss := strings.Split(k, ".")
			if ss[0] != o.S.String() {
				continue
			}
			if _, ok := ap[ss[1]]; ok {
				smp[ss[1]] = v
			}
		}
		for _, attr := range o.Sattrs {
			smp[attr]++
		}
	}
	rs, err := c.compile(o.R, rmp)
	if err != nil {
		return nil, err
	}
	ss, err := c.compile(o.S, smp)
	if err != nil {
		return nil, err
	}
	s := new(Scope)
	gm := guest.New(c.proc.Gm.Limit, c.proc.Gm.Mmu)
	s.Proc = process.New(gm, c.proc.Mp)
	s.Proc.Lim = c.proc.Lim
	s.Proc.Reg.Ws = make([]*process.WaitRegister, 2)
	{
		s.Proc.Reg.Ws[0] = &process.WaitRegister{
			Wg: new(sync.WaitGroup),
			Ch: make(chan interface{}),
		}
		s.Proc.Reg.Ws[1] = &process.WaitRegister{
			Wg: new(sync.WaitGroup),
			Ch: make(chan interface{}),
		}
	}
	rms := new(Scope)
	{
		gm := guest.New(c.proc.Gm.Limit, c.proc.Gm.Mmu)
		rms.Proc = process.New(gm, c.proc.Mp)
		rms.Proc.Lim = c.proc.Lim
		rms.Proc.Reg.Ws = make([]*process.WaitRegister, len(rs))
		{
			for i, j := 0, len(rs); i < j; i++ {
				rms.Proc.Reg.Ws[i] = &process.WaitRegister{
					Wg: new(sync.WaitGroup),
					Ch: make(chan interface{}),
				}
			}
		}
		{
			for i, s := range rs {
				s.Ins = append(s.Ins, vm.Instruction{
					Op: vm.Transfer,
					Arg: &transfer.Argument{
						Mmu: gm,
						Reg: rms.Proc.Reg.Ws[i],
					},
				})
			}
		}
		rms.Ss = rs
		rms.Magic = Merge
		rms.Ins = append(rms.Ins, vm.Instruction{
			Op:  vm.Merge,
			Arg: &merge.Argument{},
		})
		rms.Ins = append(rms.Ins, vm.Instruction{
			Op: vm.Transfer,
			Arg: &transfer.Argument{
				Mmu: gm,
				Reg: s.Proc.Reg.Ws[0],
			},
		})
	}
	sms := new(Scope)
	{
		gm := guest.New(c.proc.Gm.Limit, c.proc.Gm.Mmu)
		sms.Proc = process.New(gm, c.proc.Mp)
		sms.Proc.Lim = c.proc.Lim
		sms.Proc.Reg.Ws = make([]*process.WaitRegister, len(ss))
		{
			for i, j := 0, len(ss); i < j; i++ {
				sms.Proc.Reg.Ws[i] = &process.WaitRegister{
					Wg: new(sync.WaitGroup),
					Ch: make(chan interface{}),
				}
			}
		}
		{
			for i, s := range ss {
				s.Ins = append(s.Ins, vm.Instruction{
					Op: vm.Transfer,
					Arg: &transfer.Argument{
						Mmu: gm,
						Reg: sms.Proc.Reg.Ws[i],
					},
				})
			}
		}
		sms.Ss = ss
		sms.Magic = Merge
		sms.Ins = append(sms.Ins, vm.Instruction{
			Op:  vm.Merge,
			Arg: &merge.Argument{},
		})
		sms.Ins = append(sms.Ins, vm.Instruction{
			Op: vm.Transfer,
			Arg: &transfer.Argument{
				Mmu: gm,
				Reg: s.Proc.Reg.Ws[1],
			},
		})
	}
	s.Magic = Merge
	s.Ss = []*Scope{rms, sms}
	s.Ins = append(s.Ins, vm.Instruction{
		Op: vm.BagInnerJoin,
		Arg: &inner.Argument{
			Rattrs: o.Rattrs,
			Sattrs: o.Sattrs,
			R:      o.R.String(),
			S:      o.S.String(),
		},
	})
	return []*Scope{s}, nil
}
