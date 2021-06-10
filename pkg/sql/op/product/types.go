package product

import (
	"matrixone/pkg/container/types"
	"matrixone/pkg/sql/op"
)

type Product struct {
	R     op.OP
	S     op.OP
	ID    string
	Attrs map[string]types.Type
}
