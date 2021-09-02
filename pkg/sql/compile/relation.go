package compile

import (
	"matrixone/pkg/sql/colexec/merge"
	"matrixone/pkg/sql/colexec/transfer"
	"matrixone/pkg/sql/op/relation"
	"matrixone/pkg/vm"
	"matrixone/pkg/vm/mmu/guest"
	"matrixone/pkg/vm/process"
	"sync"
)

func (c *compile) compileRelation(o *relation.Relation, mp map[string]uint64) ([]*Scope, error) {
	if len(o.Us) == 0 {
		return nil, nil
	}
	ss := make([]*Scope, len(o.Us))
	for i, u := range o.Us {
		ss[i] = c.compileUnit(u, o, mp)
	}
	return ss, nil
}

func (c *compile) compileUnit(u *relation.Unit, o *relation.Relation, mp map[string]uint64) *Scope {
	n := len(u.Segs)
	mcpu := c.e.Node(u.N.Id).Mcpu
	if n < mcpu {
		ss := make([]*Scope, n)
		for i, seg := range u.Segs {
			proc := process.New(c.proc.Gm, c.proc.Mp)
			proc.Lim = c.proc.Lim
			ss[i] = &Scope{
				Proc:  proc,
				Magic: Normal,
				Data: &Source{
					Refs: mp,
					DB:   o.DB,
					ID:   o.Rid,
					Segs: []*relation.Segment{seg},
				},
			}
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
		rs.N = u.N
		rs.Magic = Remote
		rs.Ins = append(rs.Ins, vm.Instruction{
			Op:  vm.Merge,
			Arg: &merge.Argument{},
		})
		return rs
	}
	m := n / mcpu
	segs := u.Segs
	ss := make([]*Scope, mcpu)
	for i := 0; i < mcpu; i++ {
		proc := process.New(c.proc.Gm, c.proc.Mp)
		proc.Lim = c.proc.Lim
		if i == mcpu-1 {
			ss[i] = &Scope{
				Proc:  proc,
				Magic: Normal,
				Data: &Source{
					Refs: mp,
					DB:   o.DB,
					ID:   o.Rid,
					Segs: segs[i*m:],
				},
			}
		} else {
			ss[i] = &Scope{
				Proc:  proc,
				Magic: Normal,
				Data: &Source{
					Refs: mp,
					DB:   o.DB,
					ID:   o.Rid,
					Segs: segs[i*m : (i+1)*m],
				},
			}
		}
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
	rs.N = u.N
	rs.Magic = Remote
	rs.Ins = append(rs.Ins, vm.Instruction{
		Op:  vm.Merge,
		Arg: &merge.Argument{},
	})
	return rs
}
