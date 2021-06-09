package summarize

import (
	"fmt"
	"matrixone/pkg/container/types"
	"matrixone/pkg/sql/colexec/aggregation"
	"matrixone/pkg/sql/op"
)

func New(prev op.OP, es []aggregation.Extend) *Summarize {
	attrs := make(map[string]types.Type)
	for _, e := range es {
		if len(e.Alias) == 0 {
			e.Alias = fmt.Sprintf("%s(%s)", aggregation.AggName[e.Op], e.Name)
		}
		attrs[e.Alias] = e.Agg.Type()
	}
	return &Summarize{
		Es:    es,
		Prev:  prev,
		Attrs: attrs,
	}
}

func (n *Summarize) String() string {
	r := fmt.Sprintf("%s -> γ([", n.Prev)
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

func (n *Summarize) Name() string {
	return n.ID
}

func (n *Summarize) Rename(name string) {
	n.ID = name
}

func (n *Summarize) Attribute() map[string]types.Type {
	return n.Attrs
}
