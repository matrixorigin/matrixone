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
	var es []*projection.Extend

	mp := make(map[string]uint8)
	rs := make([]order.Attribute, 0, len(ns))
	for _, n := range ns {
		e, err := b.buildExtend(o, n.Expr)
		if err != nil {
			return nil, nil, err
		}
		if _, ok := mp[e.String()]; !ok {
			mp[e.String()] = 0
			es = append(es, &projection.Extend{E: e})
		}
		rs = append(rs, order.Attribute{
			Name: e.String(),
			Type: e.ReturnType(),
			Dirt: getDirection(n.Direction),
		})
	}
	attrs := o.Attribute()
	for attr, typ := range attrs {
		if _, ok := mp[attr]; !ok {
			es = append(es, &projection.Extend{
				E: &extend.Attribute{
					Name: attr,
					Type: typ.Oid,
				},
			})
		}
	}
	o, err := projection.New(o, es)
	if err != nil {
		return nil, nil, err
	}
	return o, rs, nil
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
