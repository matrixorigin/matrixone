package offset

import (
	"matrixone/pkg/sql/op"
)

type Offset struct {
	Prev   op.OP
	IsPD   bool // can be push down?
	Offset int64
	ID     string
}
