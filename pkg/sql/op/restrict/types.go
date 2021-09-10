package restrict

import (
	"matrixone/pkg/sql/colexec/extend"
	"matrixone/pkg/sql/op"
)

type Restrict struct {
	Prev op.OP
	IsPD bool // can be push down?
	ID   string
	E    extend.Extend
}
