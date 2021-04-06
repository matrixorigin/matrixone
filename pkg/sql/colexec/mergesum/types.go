package mergesum

import (
	"matrixbase/pkg/container/batch"
	"matrixbase/pkg/sql/colexec/aggregation"
)

type Container struct {
	attrs []string
	bat   *batch.Batch
}

type Argument struct {
	Ctr Container
	Es  []aggregation.Extend
}
