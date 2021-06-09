package summarize

import (
	"matrixone/pkg/container/types"
	"matrixone/pkg/sql/colexec/aggregation"
	"matrixone/pkg/sql/op"
)

type Summarize struct {
	Prev  op.OP
	ID    string
	Es    []aggregation.Extend
	Attrs map[string]types.Type
}
