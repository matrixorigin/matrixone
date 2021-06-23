package compile

import (
	"matrixone/pkg/sql/colexec/extend"
	vprojection "matrixone/pkg/sql/colexec/projection"
	"matrixone/pkg/sql/op/projection"
	"matrixone/pkg/vm"
)

func (c *compile) compileProjection(o *projection.Projection, mp map[string]uint64) ([]*Scope, error) {
	refer := make(map[string]uint64)
	{
		for i, e := range o.Es {
			if name, ok := e.E.(*extend.Attribute); ok {
				if IsSource(o.Prev) {
					mp[name.Name]++
				}
			} else {
				attr := o.As[i]
				if v, ok := mp[attr]; ok {
					refer[attr] = v
					delete(mp, attr)
				} else {
					refer[attr]++
				}
				IncRef(e.E, mp)
			}
		}
	}
	ss, err := c.compile(o.Prev, mp)
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
		}
	}
	arg.Refer = refer
	for _, s := range ss {
		s.Ins = append(s.Ins, vm.Instruction{
			Arg: arg,
			Op:  vm.Projection,
		})
	}
	return ss, nil
}
