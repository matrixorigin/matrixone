package compile

import (
	"matrixone/pkg/sql/op/relation"
	"matrixone/pkg/vm/process"
	"runtime"
)

func (c *compile) compileRelation(o *relation.Relation, mp map[string]uint64) ([]*Scope, error) {
	if len(o.Segs) == 0 {
		return nil, nil
	}
	return c.compileUnit(o, mp), nil
}

func (c *compile) compileUnit(o *relation.Relation, mp map[string]uint64) []*Scope {
	n := len(o.Segs)
	mcpu := runtime.NumCPU()
	if n < mcpu {
		ss := make([]*Scope, n)
		for i, seg := range o.Segs {
			proc := process.New(c.proc.Gm, c.proc.Mp)
			proc.Lim = c.proc.Lim
			ss[i] = &Scope{
				Proc:  proc,
				Magic: Normal,
				Data: &Source{
					Refs: mp,
					ID:   o.Rid,
					Segs: []string{seg},
				},
			}
		}
		return ss
	}
	m := n / mcpu
	segs := o.Segs
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
					ID:   o.Rid,
					Segs: segs[i*m : (i+1)*m],
				},
			}
		}
	}
	return ss
}
