package engine

import (
	"matrixone/pkg/container/batch"
	"matrixone/pkg/vm/metadata"
	"matrixone/pkg/vm/process"
)

type Statistics interface {
	Rows() int64
	Size(string) int64
}

type Relation interface {
	Statistics

	ID() string

	Segments() []string

	Attribute() []metadata.Attribute

	Segment(string, *process.Process) Segment

	Write(*batch.Batch) error

	AddAttribute(metadata.Attribute) error
	DelAttribute(metadata.Attribute) error
}

type Segment interface {
	ID() string
	Read([]uint64, []string, *process.Process) (*batch.Batch, error) // read only arguments
}

type Engine interface {
	Relations() []string
	Relation(string) (Relation, error)

	Create(string, []metadata.Attribute) error
}
