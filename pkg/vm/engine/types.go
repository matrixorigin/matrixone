package engine

import (
	"matrixone/pkg/container/batch"
	"matrixone/pkg/vm/metadata"
	"matrixone/pkg/vm/process"

	"github.com/pilosa/pilosa/roaring"
)

type Unit struct {
	Segs []string
	N    metadata.Node
}

type Statistics interface {
	Rows() int64
	Size(string) int64
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

type Filter interface {
	Eq(string, interface{}) (*roaring.Bitmap, error)
	Ne(string, interface{}) (*roaring.Bitmap, error)
	Lt(string, interface{}) (*roaring.Bitmap, error)
	Le(string, interface{}) (*roaring.Bitmap, error)
	Gt(string, interface{}) (*roaring.Bitmap, error)
	Ge(string, interface{}) (*roaring.Bitmap, error)
	Btw(string, interface{}, interface{}) (*roaring.Bitmap, error)
}

type Summarizer interface {
	Count(string, *roaring.Bitmap) (uint64, error)
	NullCount(string, *roaring.Bitmap) (uint64, error)
	Max(string, *roaring.Bitmap) (interface{}, error)
	Min(string, *roaring.Bitmap) (interface{}, error)
	Sum(string, *roaring.Bitmap) (int64, uint64, error)
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

type Segment interface {
	Statistics

	ID() string
	Blocks() []string
	Block(string, *process.Process) Block

	NewFilter() Filter
	NewSummarizer() Summarizer
	NewSparseFilter() SparseFilter
}

type Block interface {
	Statistics
	batch.Reader

	ID() string
	Prefetch([]uint64, []string, *process.Process) (*batch.Batch, error) // read only arguments
}

type Engine interface {
	Relations() []Relation
	Relation(string) (Relation, error)

	Delete(string) error
	Create(string, []metadata.Attribute) error
}

type DB interface {
	Close() error
	NewBatch() (Batch, error)
	NewIterator([]byte) (Iterator, error)

	Del([]byte) error
	Set([]byte, []byte) error
	Get([]byte) ([]byte, error)
}

type Batch interface {
	Cancel() error
	Commit() error
	Del([]byte) error
	Set([]byte, []byte) error
}

type Iterator interface {
	Next() error
	Valid() bool
	Close() error
	Seek([]byte) error
	Key() []byte
	Value() ([]byte, error)
}

type SpillEngine interface {
	DB
	Engine
}
