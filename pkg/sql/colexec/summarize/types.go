package summarize

import (
	"matrixone/pkg/container/batch"
	"matrixone/pkg/sql/colexec/aggregation"
)

type Container struct {
	attrs []string
	bat   *batch.Batch
}

type Argument struct {
	Ctr Container
	Es  []aggregation.Extend
}
