package op

import (
	"matrixbase/pkg/container/batch"
	"matrixbase/pkg/mempool"
	"matrixbase/pkg/vm/metadata"
)

type OP interface {
	Op() int
	String() string
	Attributes() []metadata.Attribute
	Read([]string, *mempool.Mempool) (*batch.Batch, error)
}
