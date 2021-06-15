package compile

import (
	"matrixone/pkg/sql/colexec/extend"
	vprojection "matrixone/pkg/sql/colexec/projection"
	"matrixone/pkg/sql/op/projection"
	"matrixone/pkg/vm"
)

func (c *compile) compileProjection(o *projection.Projection) ([]*Scope, error) {
	var attrs []string

	ss, err := c.compile(o.Prev)
	if err != nil {
		return nil, err
	}
	arg := &vprojection.Argument{
		Attrs: make([]string, len(o.Es)),
		Es:    make([]extend.Extend, len(o.Es)),
	}
	{
		for i, e := range o.Es {
			arg.Es[i] = e.E
			arg.Attrs[i] = e.Alias
			attrs = append(attrs, e.E.Attributes()...)
		}
	}
	for _, s := range ss {
		s.Ins = append(s.Ins, vm.Instruction{
			Arg: arg,
			Op:  vm.Projection,
		})
		IncRef(s, attrs)
		{
			for _, attr := range o.As {
				s.Proc.Refer[attr] = 1
			}
		}
	}
	return ss, nil
}
