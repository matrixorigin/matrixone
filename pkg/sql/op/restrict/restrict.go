package restrict

import (
	"fmt"
	"matrixone/pkg/container/types"
	"matrixone/pkg/sql/colexec/extend"
	"matrixone/pkg/sql/op"
)

func New(prev op.OP, e extend.Extend) *Restrict {
	return &Restrict{
		E:    e,
		Prev: prev,
	}
}

func (n *Restrict) String() string {
	return fmt.Sprintf("%s -> σ(%s)", n.Prev, n.E)
}

func (n *Restrict) Name() string {
	return n.ID
}

func (n *Restrict) Rename(name string) {
	n.ID = name
}

func (n *Restrict) Columns() []string {
	return n.Prev.Columns()
}

func (n *Restrict) Attribute() map[string]types.Type {
	return n.Prev.Attribute()
}
