package engine

import (
	"matrixone/pkg/container/batch"
	"matrixone/pkg/sql/colexec/extend"
	"matrixone/pkg/vm/metadata"
	"matrixone/pkg/vm/process"

	"github.com/pilosa/pilosa/roaring"
)

const (
	Sparse = iota
	Bsi
	Inverted
)

type SchemaState byte

const (
	// StateNone means this schema element is absent and can't be used.
	StateNone SchemaState = iota
	// StateDeleteOnly means we can only delete items for this schema element.
	StateDeleteOnly
	// StatePublic means this schema element is ok for all write and read operations.
	StatePublic
)

type CatalogInfo struct {
	Id   uint64
	Name string
}

// SchemaInfo stores the information of a schema(database).
type SchemaInfo struct {
	Catalog CatalogInfo
	Id      uint64
	Name    string
}

// TableInfo stores the information of a table or view.
type TableInfo struct {
	Catalog CatalogInfo
	Schema  SchemaInfo
	Id      uint64
	Name    string
	// Type of the table: BASE TABLE for a normal table, VIEW for a view, etc.
	Type string
	// Column is listed in order in which they appear in schema
	Columns []*ColumnInfo
	Comment string
}

// TabletInfo stores the information of one tablet.
type TabletInfo struct {
}

// PartitionInfo stores the information of a partition.
type PartitionInfo struct {
}

// SegmentInfo stores the information of a segment.
type SegmentInfo struct {
	Id          string
	GroupId     string
	TabletId    string
	PartitionId string
}

// ColumnInfo stores the information of a column.
type ColumnInfo struct {
}

type Unit struct {
	Segs []SegmentInfo
	N    metadata.Node
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
	Typ         int
	Name        string
	PartitionBy *PartitionBy
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
	Attribute() []*AttributeDef

	Partition() *PartitionBy
	Distribution() *DistributionBy

	Scheduling(metadata.Nodes) []*Unit

	Segment(SegmentInfo, *process.Process) Segment

	Write(*batch.Batch) error

	AddTableDef(TableDef) error
	DelTableDef(TableDef) error
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
	Relations() []Relation
	Relation(string) (Relation, error)

	Delete(string) error
	Create(string, []TableDef, *PartitionBy, *DistributionBy) error
}

type Engine interface {
	Create(string) error // 权限之类的参数怎么填
	Delete(string) error
	Database(string) (Database, error)
}
