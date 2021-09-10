package top

import (
	"fmt"
	"matrixone/pkg/container/types"
	"matrixone/pkg/sql/op"
	"matrixone/pkg/sql/op/order"
)

func New(prev op.OP, limit int64, gs []order.Attribute) *Top {
	return &Top{
		Gs:    gs,
		Prev:  prev,
		Limit: limit,
	}
}

func (n *Top) String() string {
	r := fmt.Sprintf("%s -> τ([", n.Prev)
	for i, g := range n.Gs {
		switch i {
		case 0:
			r += fmt.Sprintf("%s", g.Name)
		default:
			r += fmt.Sprintf(", %s", g.Name)
		}
	}
	r += fmt.Sprintf("], %v)", n.Limit)
	return r
}

func (n *Top) Name() string {
	return n.ID
}

func (n *Top) Rename(name string) {
	n.ID = name
}

func (n *Top) Columns() []string {
	return n.Prev.Columns()
}

func (n *Top) Attribute() map[string]types.Type {
	return n.Prev.Attribute()
}
