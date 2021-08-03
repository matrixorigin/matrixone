package compile

import (
	vrestrict "matrixone/pkg/sql/colexec/restrict"
	"matrixone/pkg/sql/op/restrict"
	"matrixone/pkg/vm"
)

func (c *compile) compileRestrict(o *restrict.Restrict, mp map[string]uint64) ([]*Scope, error) {
	{
		attrs := o.E.Attributes()
		for _, attr := range attrs {
			mp[attr]++
		}
	}
	ss, err := c.compile(o.Prev, mp)
	if err != nil {
		return nil, err
	}
	arg := &vrestrict.Argument{E: o.E}
	if o.IsPD {
		for i, s := range ss {
			ss[i] = pushRestrict(s, arg)
		}
	} else {
		for i, s := range ss {
			ss[i].Ins = append(s.Ins, vm.Instruction{
				Arg: arg,
				Op:  vm.Restrict,
			})
		}
	}
	return ss, nil
}

func pushRestrict(s *Scope, arg *vrestrict.Argument) *Scope {
	if s.Magic == Merge || s.Magic == Remote {
		for i := range s.Ss {
			s.Ss[i] = pushRestrict(s.Ss[i], arg)
		}
	} else {
		n := len(s.Ins) - 1
		s.Ins = append(s.Ins, vm.Instruction{
			Arg: arg,
			Op:  vm.Restrict,
		})
		s.Ins[n], s.Ins[n+1] = s.Ins[n+1], s.Ins[n]
	}
	return s

}
