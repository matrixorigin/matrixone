package group

import (
	"matrixone/pkg/container/types"
	"matrixone/pkg/sql/colexec/aggregation"
	"matrixone/pkg/sql/colexec/extend"
	"matrixone/pkg/sql/op"
)

type Group struct {
	Prev  op.OP
	IsPD  bool // can be push down?
	ID    string
	As    []string
	Gs    []*extend.Attribute
	Es    []aggregation.Extend
	Attrs map[string]types.Type
}
