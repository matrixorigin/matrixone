package aoe

import (
	"bytes"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
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