package innerJoin

import (
	"matrixone/pkg/container/types"
	"matrixone/pkg/sql/op"
)

type Join struct {
	R      op.OP
	S      op.OP
	ID     string
	Rattrs []string
	Sattrs []string
	Attrs  map[string]types.Type
}
