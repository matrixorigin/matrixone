package summarize

import (
	"matrixone/pkg/container/batch"
	"matrixone/pkg/sql/colexec/aggregation"
)

type Container struct {
	attrs []string
	bat   *batch.Batch
	refer map[string]uint64
}

type Argument struct {
	Ctr   Container
	Refer map[string]uint64
	Es    []aggregation.Extend
}
