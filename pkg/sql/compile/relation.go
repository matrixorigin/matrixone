package compile

import (
	"matrixone/pkg/sql/op/relation"
	"matrixone/pkg/vm/mmu/guest"
	"matrixone/pkg/vm/process"
)

func (c *compile) compileRelation(o *relation.Relation, mp map[string]uint64) ([]*Scope, error) {
	n := len(o.Us[0].Segs)
	mcpu := c.e.Node(o.Us[0].N.Id).Mcpu
	if n < mcpu {
		us := make([]*Scope, n)
		for i, seg := range o.Us[0].Segs {
			proc := process.New(c.proc.Gm, c.proc.Mp)
			proc.Lim = c.proc.Lim
			us[i] = &Scope{
				Proc:  proc,
				Magic: Normal,
				Data: &Source{
					Refs: mp,
					DB:   o.DB,
					ID:   o.Rid,
					N:    o.Us[0].N,
					Segs: []*relation.Segment{seg},
				},
			}
		}
		return us, nil
	}
	m := n / mcpu
	segs := o.Us[0].Segs
	us := make([]*Scope, mcpu)
	for i := 0; i < mcpu; i++ {
		proc := process.New(guest.New(c.proc.Gm.Limit, c.proc.Gm.Mmu), c.proc.Mp)
		proc.Lim = c.proc.Lim
		if i == mcpu-1 {
			us[i] = &Scope{
				Proc:  proc,
				Magic: Normal,
				Data: &Source{
					Refs: mp,
					DB:   o.DB,
					ID:   o.Rid,
					N:    o.Us[0].N,
					Segs: segs[i*m:],
				},
			}
		} else {
			us[i] = &Scope{
				Proc:  proc,
				Magic: Normal,
				Data: &Source{
					Refs: mp,
					DB:   o.DB,
					ID:   o.Rid,
					N:    o.Us[0].N,
					Segs: segs[i*m : (i+1)*m],
				},
			}
		}
	}
	return us, nil
}
