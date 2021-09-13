package limit

import (
	"fmt"
	"matrixone/pkg/container/types"
	"matrixone/pkg/sql/op"
)

func New(prev op.OP, limit int64) *Limit {
	return &Limit{
		Prev:  prev,
		Limit: limit,
	}
}

func (n *Limit) String() string {
	return fmt.Sprintf("%s -> limit(%v)", n.Prev, n.Limit)
}

func (n *Limit) Name() string {
	return n.ID
}

func (n *Limit) Rename(name string) {
	n.ID = name
}

func (n *Limit) Columns() []string {
	return n.Prev.Columns()
}

func (n *Limit) Attribute() map[string]types.Type {
	return n.Prev.Attribute()
}
