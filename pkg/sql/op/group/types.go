package group

import (
	"matrixone/pkg/container/types"
	"matrixone/pkg/sql/colexec/aggregation"
	"matrixone/pkg/sql/colexec/extend"
	"matrixone/pkg/sql/op"
)

type Group struct {
	Prev  op.OP
	ID    string
	Gs    []*extend.Attribute
	Es    []aggregation.Extend
	Attrs map[string]types.Type
}
