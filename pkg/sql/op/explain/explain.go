package explain

import (
	"matrixone/pkg/container/types"
	"matrixone/pkg/sql/op"
)

func New(o op.OP) *Explain {
	return &Explain{o}
}

func (n *Explain) String() string                   { return n.O.String() }
func (n *Explain) Name() string                     { return "" }
func (n *Explain) Rename(_ string)                  {}
func (n *Explain) Columns() []string                { return nil }
func (n *Explain) Attribute() map[string]types.Type { return nil }
