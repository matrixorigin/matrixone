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
	cs    []uint64        // reference count for attribute
	attrs []string        // attribute list
	cds   []*bytes.Buffer // buffers for compressed data
	dds   []*bytes.Buffer // buffers for decompressed data
	ins   vm.Instructions // orders to be executed
}

type block struct {
	blk engine.Block
}

type queue struct {
	pi  int   // prefetch index
	siz int64 // not used now
	bs  []block
}
