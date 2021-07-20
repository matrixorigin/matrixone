package engine

import (
	"matrixone/pkg/container/batch"
	"matrixone/pkg/sql/colexec/extend"
	"matrixone/pkg/vm/metadata"
	"matrixone/pkg/vm/process"

	roaring "github.com/RoaringBitmap/roaring/roaring64"
)

const (
	RSE = iota
	AOE
	Spill
)

const (
	Sparse = iota
	Bsi
	Inverted
)

type SegmentInfo struct {
	Version    uint64
	Ids        []uint64
	GroupId    string
	TabletName string
	Node       metadata.Node
}

type Unit struct {
	Segs []SegmentInfo
	N    metadata.Node
}

type NodeInfo struct {
	Mcpu int
}

type Statistics interface {
	Rows() int64
	Size(string) int64
}

type ListPartition struct {
	Name         string
	Extends      []extend.Extend
	Subpartition *PartitionBy
}

type RangePartition struct {
	Name         string
	From         []extend.Extend
	To           []extend.Extend
	Subpartition *PartitionBy
}

type PartitionBy struct {
	Fields []string
	List   []ListPartition
	Range  []RangePartition
}

type DistributionBy struct {
	Num    int
	Group  string
	Fields []string
}

type IndexTableDef struct {
	Typ   int
	Names []string
}

type AttributeDef struct {
	Attr metadata.Attribute
}

type TableDef interface {
	tableDef()
}

func (*AttributeDef) tableDef()  {}
func (*IndexTableDef) tableDef() {}

type Relation interface {
	Statistics

	ID() string

	Segments() []SegmentInfo

	Index() []*IndexTableDef
	Attribute() []metadata.Attribute

	Segment(SegmentInfo, *process.Process) Segment

	Write(*batch.Batch) error

	AddAttribute(TableDef) error
	DelAttribute(TableDef) error
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

type Database interface {
	Type() int // engine type of database

	Relations() []string
	Relation(string) (Relation, error)

	Delete(string) error
	Create(string, []TableDef, *PartitionBy, *DistributionBy, string) error // Create Table - (name, table define, partition define, distribution define, comment)
}

type Engine interface {
	Delete(string) error
	Create(string, int) error // Create Database - (name, engine type)

	Databases() []string
	Database(string) (Database, error)

	Node(string) *NodeInfo
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
	Database
}
