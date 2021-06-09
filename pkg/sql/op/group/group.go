package group

import (
	"fmt"
	"matrixone/pkg/container/types"
	"matrixone/pkg/sql/colexec/aggregation"
	"matrixone/pkg/sql/colexec/extend"
	"matrixone/pkg/sql/op"
)

func New(prev op.OP, gs []*extend.Attribute, es []aggregation.Extend) *Group {
	attrs := make(map[string]types.Type)
	{
		for _, g := range gs {
			attrs[g.Name] = g.Type.ToType()
		}
	}
	{
		for _, e := range es {
			if len(e.Alias) == 0 {
				e.Alias = fmt.Sprintf("%s(%s)", aggregation.AggName[e.Op], e.Name)
			}
			attrs[e.Alias] = e.Agg.Type()
		}
	}
	return &Group{
		Es:    es,
		Gs:    gs,
		Prev:  prev,
		Attrs: attrs,
	}
}

func (n *Group) String() string {
	r := fmt.Sprintf("%s -> γ([", n.Prev)
	for i, g := range n.Gs {
		switch i {
		case 0:
			r += fmt.Sprintf("%s", g.Name)
		default:
			r += fmt.Sprintf(", %s", g.Name)
		}
	}
	r += "], ["
	for i, e := range n.Es {
		switch i {
		case 0:
			r += fmt.Sprintf("%s(%s) -> %s", aggregation.AggName[e.Op], e.Name, e.Alias)
		default:
			r += fmt.Sprintf(", %s(%s) -> %s", aggregation.AggName[e.Op], e.Name, e.Alias)
		}
	}
	r += fmt.Sprintf("])")
	return r
}

func (n *Group) Name() string {
	return n.ID
}

func (n *Group) Rename(name string) {
	n.ID = name
}

func (n *Group) Attribute() map[string]types.Type {
	return n.Attrs
}
