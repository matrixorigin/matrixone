package product

import (
	"matrixone/pkg/container/types"
	"matrixone/pkg/sql/op"
)

type Product struct {
	IsPD  bool // can be push down?
	R     op.OP
	S     op.OP
	ID    string
	Attrs map[string]types.Type
}
