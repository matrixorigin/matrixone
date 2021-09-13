package order

import (
	"fmt"
	"matrixone/pkg/container/types"
	"matrixone/pkg/sql/op"
)

func New(prev op.OP, gs []Attribute) *Order {
	return &Order{
		Gs:   gs,
		Prev: prev,
	}
}

func (n *Order) String() string {
	r := fmt.Sprintf("%s -> τ([", n.Prev)
	for i, g := range n.Gs {
		switch i {
		case 0:
			r += fmt.Sprintf("%s", g.Name)
		default:
			r += fmt.Sprintf(", %s", g.Name)
		}
	}
	r += fmt.Sprintf("])")
	return r
}

func (n *Order) Name() string {
	return n.ID
}

func (n *Order) Rename(name string) {
	n.ID = name
}

func (n *Order) Columns() []string {
	return n.Prev.Columns()
}

func (n *Order) Attribute() map[string]types.Type {
	return n.Prev.Attribute()
}
