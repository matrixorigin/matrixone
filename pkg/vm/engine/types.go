package engine

import (
	"matrixbase/pkg/container/batch"
	"matrixbase/pkg/vm/metadata"
	"matrixbase/pkg/vm/process"
)

type Unit struct {
	Segs []string
	N    metadata.Node
}

type Statistics interface {
	Rows() int64
}

type Relation interface {
	Statistics

	ID() string

	Segments() []string
	Attribute() []metadata.Attribute

	Scheduling(metadata.Nodes) []*Unit

	Segment(string, *process.Process) Segment

	Write(*batch.Batch) error

	AddAttribute(metadata.Attribute) error
	DelAttribute(metadata.Attribute) error
}

type Node interface {
	Support(int) bool // supported relational algebra
}

type Segment interface {
	ID() string
	Read([]uint64, []string, *process.Process) (*batch.Batch, error)
}

type Engine interface {
	Relations() []Relation
	Relation(string) (Relation, error)

	Delete(string) error
	Create(string, []metadata.Attribute) error
}
