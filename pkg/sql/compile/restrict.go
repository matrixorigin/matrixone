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
	for _, s := range ss {
		s.Ins = append(s.Ins, vm.Instruction{
			Op: vm.Restrict,
			Arg: &vrestrict.Argument{
				E: o.E,
			},
		})
	}
	return ss, nil
}
