package pipeline

import (
	"matrixone/pkg/container/batch"
	"matrixone/pkg/vm"
	"matrixone/pkg/vm/engine"
)

type Pipeline struct {
	cs    []uint64
	attrs []string
	ins   vm.Instructions
}

type block struct {
	siz int64
	bat *batch.Batch
	blk engine.Segment
}

type queue struct {
	pi  int // prefetch index
	siz int64
	bs  []block
}
