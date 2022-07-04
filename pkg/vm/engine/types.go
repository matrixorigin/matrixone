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
	roaring "github.com/RoaringBitmap/roaring/roaring64"
	"github.com/matrixorigin/matrixone/pkg/compress"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/extend"
)

type Snapshot []byte

type Nodes []Node

type Node struct {
	Id   string `json:"id"`
	Addr string `json:"address"`
	Data []byte `json:"payload"`
}

type Attribute struct {
	Name    string      // name of attribute
	Alg     compress.T  // compression algorithm
	Type    types.Type  // type of attribute
	Default DefaultExpr // default value of this attribute.
	Primary bool        // if true, it is primary key
}

type DefaultExpr struct {
	Exist  bool
	Value  interface{} // int64, float32, float64, string, types.Date, types.Datetime
	IsNull bool
}

type PrimaryIndexDef struct {
	TableDef
	Names []string
}

type PropertiesDef struct {
	TableDef
	Properties []Property
}

type Property struct {
	Key   string
	Value string
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
	Subpartition *PartitionByDef
}

type RangePartition struct {
	Name         string
	From         []extend.Extend
	To           []extend.Extend
	Subpartition *PartitionByDef
}

type PartitionByDef struct {
	Fields []string
	List   []ListPartition
	Range  []RangePartition
}

type IndexTableDef struct {
	Typ      IndexT
	ColNames []string
	Name     string
}

type IndexT int

func (node IndexT) ToString() string {
	switch node {
	case ZoneMap:
		return "ZONEMAP"
	case BsiIndex:
		return "BSI"
	default:
		return "INVAILD"
	}
}

const (
	Invalid IndexT = iota
	ZoneMap
	BsiIndex
)

type AttributeDef struct {
	Attr Attribute
}

type CommentDef struct {
	Comment string
}

type TableDef interface {
	tableDef()
}

func (*CommentDef) tableDef()     {}
func (*AttributeDef) tableDef()   {}
func (*IndexTableDef) tableDef()  {}
func (*PartitionByDef) tableDef() {}
func (*PropertiesDef) tableDef()  {}

type Relation interface {
	Statistics

	Close(Snapshot)

	ID(Snapshot) string

	Nodes(Snapshot) Nodes

	TableDefs(Snapshot) []TableDef

	GetPrimaryKeys(Snapshot) []*Attribute

	GetHideKey(Snapshot) *Attribute
	// true: primary key, false: hide key
	GetPriKeyOrHideKey(Snapshot) ([]Attribute, bool)

	Write(uint64, *batch.Batch, Snapshot) error

	Update(uint64, *batch.Batch, Snapshot) error

	Delete(uint64, *vector.Vector, string, Snapshot) error

	Truncate(Snapshot) (uint64, error)

	AddTableDef(uint64, TableDef, Snapshot) error
	DelTableDef(uint64, TableDef, Snapshot) error

	// first argument is the number of reader, second argument is the filter extend,  third parameter is the payload required by the engine
	NewReader(int, extend.Extend, []byte, Snapshot) []Reader
}

type Reader interface {
	Read([]uint64, []string) (*batch.Batch, error)
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
	Eq(string, interface{}) (Reader, error)
	Ne(string, interface{}) (Reader, error)
	Lt(string, interface{}) (Reader, error)
	Le(string, interface{}) (Reader, error)
	Gt(string, interface{}) (Reader, error)
	Ge(string, interface{}) (Reader, error)
	Btw(string, interface{}, interface{}) (Reader, error)
}

type Database interface {
	Relations(Snapshot) []string
	Relation(string, Snapshot) (Relation, error)

	Delete(uint64, string, Snapshot) error
	Create(uint64, string, []TableDef, Snapshot) error // Create Table - (name, table define)
}

type Engine interface {
	Delete(uint64, string, Snapshot) error
	Create(uint64, string, int, Snapshot) error // Create Database - (name, engine type)

	Databases(Snapshot) []string
	Database(string, Snapshot) (Database, error)

	Node(string, Snapshot) *NodeInfo
}

// MakeDefaultExpr returns a new DefaultExpr
func MakeDefaultExpr(exist bool, value interface{}, isNull bool) DefaultExpr {
	return DefaultExpr{
		Exist:  exist,
		Value:  value,
		IsNull: isNull,
	}
}

// EmptyDefaultExpr means there is no definition for default expr
var EmptyDefaultExpr = DefaultExpr{Exist: false}

func (node Attribute) HasDefaultExpr() bool {
	return node.Default.Exist
}

func (node Attribute) GetDefaultExpr() (interface{}, bool) {
	return node.Default.Value, node.Default.IsNull
}
