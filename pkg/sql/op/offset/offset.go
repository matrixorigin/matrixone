package offset

import (
	"fmt"
	"matrixone/pkg/container/types"
	"matrixone/pkg/sql/op"
)

func New(prev op.OP, offset int64) *Offset {
	return &Offset{
		Prev:   prev,
		Offset: offset,
		Attrs:  prev.Attribute(),
	}
}

func (n *Offset) String() string {
	return fmt.Sprintf("%s -> offset(%v)", n.Prev, n.Offset)
}

func (n *Offset) Name() string {
	return n.ID
}

func (n *Offset) Rename(name string) {
	n.ID = name
}

func (n *Offset) Attribute() map[string]types.Type {
	return n.Attrs
}
