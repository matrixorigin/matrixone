package block

import (
	"matrixone/pkg/container/batch"
	"matrixone/pkg/vm/engine"
)

type Block struct {
	Seg   string
	Cs    []uint64
	Attrs []string
	Bat   *batch.Batch
	R     engine.Relation
}
