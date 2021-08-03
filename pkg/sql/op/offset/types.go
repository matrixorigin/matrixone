package offset

import (
	"matrixone/pkg/container/types"
	"matrixone/pkg/sql/op"
)

type Offset struct {
	Prev   op.OP
	IsPD   bool // can be push down?
	Offset int64
	ID     string
	Attrs  map[string]types.Type
}
