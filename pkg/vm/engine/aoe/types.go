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
	NewSparseFilter() SparseFilter
}

type Block interface {
	engine.Statistics

	ID() string
	Prefetch([]string)
	Read([]uint64, []string, []*bytes.Buffer, []*bytes.Buffer) (*batch.Batch, error) // read only arguments
}

type Store interface {
	Blocks() []Block
	SparseFilterBlocks() []Block
}

type SparseFilter interface {
	Eq(string, interface{}) ([]string, error)
	Ne(string, interface{}) ([]string, error)
	Lt(string, interface{}) ([]string, error)
	Le(string, interface{}) ([]string, error)
	Gt(string, interface{}) ([]string, error)
	Ge(string, interface{}) ([]string, error)
	Btw(string, interface{}, interface{}) ([]string, error)
}