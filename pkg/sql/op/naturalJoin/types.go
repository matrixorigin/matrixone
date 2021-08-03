package naturalJoin

import (
	"matrixone/pkg/container/types"
	"matrixone/pkg/sql/op"
)

type Join struct {
	IsPD  bool // can be push down?
	R     op.OP
	S     op.OP
	ID    string
	Pub   []string
	Attrs map[string]types.Type
}
