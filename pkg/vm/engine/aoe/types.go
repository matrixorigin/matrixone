package aoe

import (
	"bytes"
	"matrixone/pkg/container/batch"
	"matrixone/pkg/vm/engine"
)

type Segment interface {
	engine.Statistics

	ID() string
	Blocks() []string
	Block(string) Block
}

type Block interface {
	engine.Statistics

	ID() string
	Prefetch([]string)
	Read([]uint64, []string, []*bytes.Buffer, []*bytes.Buffer) (*batch.Batch, error) // read only arguments
}