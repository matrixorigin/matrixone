package limit

import (
	"matrixone/pkg/sql/op"
)

type Limit struct {
	Prev  op.OP
	IsPD  bool // can be push down?
	Limit int64
	ID    string
}
