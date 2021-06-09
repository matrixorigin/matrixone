package restrict

import (
	"matrixone/pkg/container/types"
	"matrixone/pkg/sql/colexec/extend"
	"matrixone/pkg/sql/op"
)

type Restrict struct {
	Prev  op.OP
	ID    string
	E     extend.Extend
	Attrs map[string]types.Type
}
