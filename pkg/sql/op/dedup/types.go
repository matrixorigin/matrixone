package dedup

import (
	"matrixone/pkg/container/types"
	"matrixone/pkg/sql/colexec/extend"
	"matrixone/pkg/sql/op"
)

type Dedup struct {
	Prev  op.OP
	IsPD  bool // can be push down?
	ID    string
	Cs    []string // columns
	Gs    []*extend.Attribute
	Attrs map[string]types.Type
}
