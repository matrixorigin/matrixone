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
	"github.com/matrixorigin/matrixone/pkg/compress"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/extend"

	roaring "github.com/RoaringBitmap/roaring/roaring64"
)

type Nodes []Node

type Node struct {
	Id   string `json:"id"`
	Addr string `json:"address"`
}

type Attribute struct {
	Name    string      // name of attribute
	Alg     compress.T  // compression algorithm
	Type    types.Type  // type of attribute
	Default DefaultExpr // default value of this attribute.
}

type DefaultExpr struct {
	Exist  bool
	Value  interface{} // int64, float32, float64, string, types.Date
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
	Typ      int
	ColNames []string
	Name     string
}

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

	Close()

	ID() string

	Nodes() Nodes

	TableDefs() []TableDef

	Write(uint64, *batch.Batch) error

	AddTableDef(uint64, TableDef) error
	DelTableDef(uint64, TableDef) error

	NewReader(int) []Reader // first argument is the number of reader
}

type Reader interface {
	NewFilter() Filter
	NewSummarizer() Summarizer
	NewSparseFilter() SparseFilter

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
	Relations() []string
	Relation(string) (Relation, error)

	Delete(uint64, string) error
	Create(uint64, string, []TableDef) error // Create Table - (name, table define)
}

type Engine interface {
	Delete(uint64, string) error
	Create(uint64, string, int) error // Create Database - (name, engine type)

	Databases() []string
	Database(string) (Database, error)

	Node(string) *NodeInfo
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

func (attr Attribute) HasDefaultExpr() bool {
	return attr.Default.Exist
}

func (attr Attribute) GetDefaultExpr() (interface{}, bool) {
	return attr.Default.Value, attr.Default.IsNull
}
