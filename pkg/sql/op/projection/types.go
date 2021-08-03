package projection

import (
	"matrixone/pkg/container/types"
	"matrixone/pkg/sql/colexec/extend"
	"matrixone/pkg/sql/op"
)

type Extend struct {
	Alias string
	E     extend.Extend
}

type Projection struct {
	Prev  op.OP
	IsPD  bool // can be push down?
	ID    string
	As    []string
	Es    []*Extend
	Attrs map[string]types.Type
}
