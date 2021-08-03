package top

import (
	"matrixone/pkg/container/types"
	"matrixone/pkg/sql/op"
	"matrixone/pkg/sql/op/order"
)

type Top struct {
	Prev  op.OP
	IsPD  bool // can be push down?
	Limit int64
	ID    string
	Gs    []order.Attribute
	Attrs map[string]types.Type
}
