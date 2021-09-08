package pipeline

import (
	"bytes"
	"matrixone/pkg/vm"
	"matrixone/pkg/vm/engine"
)

const (
	PrefetchNum           = 4
	CompressedBlockSize   = 1024
	UncompressedBlockSize = 4096
)

type Pipeline struct {
	cs    []uint64
	attrs []string
	cds   []*bytes.Buffer
	dds   []*bytes.Buffer
	ins   vm.Instructions
}

type block struct {
	blk engine.Block
}

type queue struct {
	pi  int // prefetch index
	siz int64
	bs  []block
}
