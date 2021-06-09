package build

import (
	"matrixone/pkg/sql/colexec/extend"
	"matrixone/pkg/sql/op"
	"matrixone/pkg/sql/op/order"
	"matrixone/pkg/sql/op/projection"
	"matrixone/pkg/sql/op/top"
	"matrixone/pkg/sql/tree"
)

func (b *build) buildTop(o op.OP, ns tree.OrderBy, limit int64) (op.OP, error) {
	o, gs, err := b.stripOrderBy(o, ns)
	if err != nil {
		return nil, err
	}
	return top.New(o, limit, gs), nil
}

func (b *build) buildOrderBy(o op.OP, ns tree.OrderBy) (op.OP, error) {
	o, gs, err := b.stripOrderBy(o, ns)
	if err != nil {
		return nil, err
	}
	return order.New(o, gs), nil
}

func (b *build) stripOrderBy(o op.OP, ns tree.OrderBy) (op.OP, []order.Attribute, error) {
	var es []extend.Extend
	var attrs []*extend.Attribute

	rs := make([]order.Attribute, 0, len(ns))
	for _, n := range ns {
		e, err := b.buildExtend(o, n.Expr)
		if err != nil {
			return nil, nil, err
		}
		if attr, ok := e.(*extend.Attribute); !ok {
			es = append(es, e)
		} else {
			attrs = append(attrs, attr)
		}
	}
	if len(es) == 0 {
		for i, attr := range attrs {
			rs = append(rs, order.Attribute{
				Name: attr.Name,
				Type: attr.Type,
				Dirt: getDirection(ns[i].Direction),
			})
		}
		return o, rs, nil
	}
	pes := make([]*projection.Extend, 0, len(es)+len(attrs))
	{
		i := 0
		for _, e := range es {
			pes = append(pes, &projection.Extend{E: e})
			rs = append(rs, order.Attribute{
				Name: e.String(),
				Type: e.ReturnType(),
				Dirt: getDirection(ns[i].Direction),
			})
			i++
		}
		for _, attr := range attrs {
			pes = append(pes, &projection.Extend{E: attr})
			rs = append(rs, order.Attribute{
				Name: attr.Name,
				Type: attr.Type,
				Dirt: getDirection(ns[i].Direction),
			})
			i++
		}
	}
	return projection.New(o, pes), rs, nil
}

func getDirection(d tree.Direction) order.Direction {
	switch d {
	case tree.Ascending:
		return order.Ascending
	case tree.Descending:
		return order.Descending
	}
	return order.DefaultDirection
}
