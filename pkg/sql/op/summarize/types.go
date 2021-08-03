package summarize

import (
	"matrixone/pkg/container/types"
	"matrixone/pkg/sql/colexec/aggregation"
	"matrixone/pkg/sql/op"
)

type Summarize struct {
	Prev  op.OP
	IsPD  bool // can be push down?
	ID    string
	As    []string
	Es    []aggregation.Extend
	Attrs map[string]types.Type
}
