package build

import (
	"matrixone/pkg/sql/colexec/extend"
	"matrixone/pkg/sql/op"
	"matrixone/pkg/sql/op/dedup"
)

func (b *build) buildDedup(o op.OP) (op.OP, error) {
	var gs []*extend.Attribute

	attrs := o.Attribute()
	for k, v := range attrs {
		gs = append(gs, &extend.Attribute{
			Name: k,
			Type: v.Oid,
		})
	}
	return dedup.New(o, gs), nil
}
