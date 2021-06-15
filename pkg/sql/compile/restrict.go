package compile

import (
	vrestrict "matrixone/pkg/sql/colexec/restrict"
	"matrixone/pkg/sql/op/restrict"
	"matrixone/pkg/vm"
)

func (c *compile) compileRestrict(o *restrict.Restrict) ([]*Scope, error) {
	ss, err := c.compile(o.Prev)
	if err != nil {
		return nil, err
	}
	attrs := o.E.Attributes()
	for _, s := range ss {
		s.Ins = append(s.Ins, vm.Instruction{
			Op: vm.Restrict,
			Arg: &vrestrict.Argument{
				E: o.E,
			},
		})
		IncRef(s, attrs)
	}
	return ss, nil
}
