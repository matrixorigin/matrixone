package compile

import (
	vgroup "matrixone/pkg/sql/colexec/group"
	"matrixone/pkg/sql/colexec/mergegroup"
	"matrixone/pkg/sql/colexec/transfer"
	"matrixone/pkg/sql/op/group"
	"matrixone/pkg/vm"
	"matrixone/pkg/vm/mmu/guest"
	"matrixone/pkg/vm/process"
	"sync"
)

func (c *compile) compileGroup(o *group.Group, mp map[string]uint64) ([]*Scope, error) {
	var gs []string

	refer := make(map[string]uint64)
	{
		for _, attr := range o.As {
			if v, ok := mp[attr]; ok {
				refer[attr] = v + 1
				delete(mp, attr)
			} else {
				refer[attr]++
			}
		}
		for _, g := range o.Gs {
			gs = append(gs, g.Name)
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
	for i, s := range ss {
		s.Ins = append(s.Ins, vm.Instruction{
			Op: vm.Group,
			Arg: &vgroup.Argument{
				Gs:    gs,
				Refer: refer,
				Es:    unitAggregates(o.Es),
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
		Op: vm.MergeGroup,
		Arg: &mergegroup.Argument{
			Gs:    gs,
			Refer: refer,
			Es:    mergeAggregates(o.Es),
		},
	})
	return []*Scope{rs}, nil
}
