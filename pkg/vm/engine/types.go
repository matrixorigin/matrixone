// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package engine

import (
	"bytes"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/extend"
	"github.com/matrixorigin/matrixone/pkg/vm/metadata"
	"github.com/matrixorigin/matrixone/pkg/vm/process"

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
	Version  uint64
	Id       string
	GroupId  string
	TabletId string
	Node     metadata.Node
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
	Typ   IndexT
	ColNames []string
	Name string
}

type IndexT int

const (
	ZoneMap IndexT = iota
	Invalid
	BsiIndex
)
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

	Close()

	ID() string

	Segments() []SegmentInfo

	Index() []*IndexTableDef
	Attribute() []metadata.Attribute

	Segment(SegmentInfo, *process.Process) Segment

	Write(uint64, *batch.Batch) error

	AddAttribute(uint64, TableDef) error
	DelAttribute(uint64, TableDef) error
	CreateIndex(epoch uint64, defs []TableDef) error
	DropIndex(epoch uint64, name string) error
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

// A Block represents an implementation of block reader.
type Block interface {
	Statistics

	ID() string
	Prefetch([]string)
	Read([]uint64, []string, []*bytes.Buffer, []*bytes.Buffer) (*batch.Batch, error) // read only arguments
}

// Database consists of functions that reference the session database
type Database interface {
	// Type returns the engine type of database.
	// For now, we only support aoe engine.
	Type() int

	// Relations returns a string array containing all relations
	// in the given database.
	Relations() []string

	// Relation looks up the relation with the given name
	// returns a relation with properties if it exists,
	// or returns error.
	Relation(string) (Relation, error)

	// Delete
	Delete(uint64, string) error
	Create(uint64, string, []TableDef, *PartitionBy, *DistributionBy, string) error // Create Table - (name, table define, partition define, distribution define, comment)
}

type Engine interface {
	Delete(uint64, string) error
	Create(uint64, string, int) error // Create Database - (name, engine type)

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
