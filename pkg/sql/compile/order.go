package compile

import (
	"matrixone/pkg/sql/colexec/mergeorder"
	vorder "matrixone/pkg/sql/colexec/order"
	"matrixone/pkg/sql/colexec/transfer"
	"matrixone/pkg/sql/op/order"
	"matrixone/pkg/vm"
	"matrixone/pkg/vm/mmu/guest"
	"matrixone/pkg/vm/process"
	"sync"
)

func (c *compile) compileOrder(o *order.Order, mp map[string]uint64) ([]*Scope, error) {
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
	fs := make([]vorder.Field, len(o.Gs))
	{
		for i, g := range o.Gs {
			fs[i] = vorder.Field{
				Attr: g.Name,
				Type: vorder.Direction(g.Dirt),
			}
		}
	}
	for i, s := range ss {
		s.Ins = append(s.Ins, vm.Instruction{
			Op: vm.Order,
			Arg: &vorder.Argument{
				Fs: fs,
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
	mfs := make([]mergeorder.Field, len(o.Gs))
	{
		for i, g := range o.Gs {
			mfs[i] = mergeorder.Field{
				Attr: g.Name,
				Type: mergeorder.Direction(g.Dirt),
			}
		}
	}
	rs.Ins = append(rs.Ins, vm.Instruction{
		Op: vm.MergeOrder,
		Arg: &mergeorder.Argument{
			Fs: mfs,
		},
	})
	return []*Scope{rs}, nil
}
