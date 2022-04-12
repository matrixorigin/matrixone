package handle

import (
	"io"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

type Reader interface {
	Next(ctx interface{}, attrs []string) (*batch.Batch, error)
}

type Relation interface {
	io.Closer
	ID() uint64
	Rows() int64
	Size(attr string) int64
	GetCardinality(attr string) int64
	Schema() interface{}
	MakeSegmentIt() SegmentIt
	MakeReader() Reader

	BatchDedup(col *vector.Vector) error
	Append(data *batch.Batch) error
	String() string

	GetMeta() interface{}
	CreateSegment() (Segment, error)
}

type RelationIt interface {
	Iterator
	GetRelation() Relation
}
