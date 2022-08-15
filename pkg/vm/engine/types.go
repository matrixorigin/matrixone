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
	"context"

	"github.com/matrixorigin/matrixone/pkg/compress"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
)

// Snapshot a tricky approach
type Snapshot []byte

type Nodes []Node

type Node struct {
	Mcpu int
	Id   string   `json:"id"`
	Addr string   `json:"address"`
	Data [][]byte `json:"payload"`
}

// Attribute is a column
type Attribute struct {
	// IsHide whether the attribute is hidden or not
	IsHidden bool
	// IsRowId whether the attribute is rowid or not
	IsRowId bool
	// Name name of attribute
	Name string
	// Alg compression algorithm
	Alg compress.T
	// Type attribute's type
	Type types.Type
	// DefaultExpr default value of this attribute
	Default *plan.Default
	// Primary is primary key or not
	Primary bool
	// Comment of attribute
	Comment string
}

type PrimaryIndexDef struct {
	Names []string
}

type PropertiesDef struct {
	Properties []Property
}

type Property struct {
	Key   string
	Value string
}

type Statistics interface {
	Rows() int64
	Size(string) int64
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

type ViewDef struct {
	View string
}

type TableDef interface {
	tableDef()
}

func (*CommentDef) tableDef()      {}
func (*ViewDef) tableDef()         {}
func (*AttributeDef) tableDef()    {}
func (*IndexTableDef) tableDef()   {}
func (*PropertiesDef) tableDef()   {}
func (*PrimaryIndexDef) tableDef() {}

type Relation interface {
	Statistics

	Ranges(context.Context) ([][]byte, error)

	TableDefs(context.Context) ([]TableDef, error)

	GetPrimaryKeys(context.Context) ([]*Attribute, error)

	GetHideKeys(context.Context) ([]*Attribute, error)

	Write(context.Context, *batch.Batch) error

	Update(context.Context, *batch.Batch) error

	Delete(context.Context, *vector.Vector, string) error

	Truncate(context.Context) (uint64, error)

	AddTableDef(context.Context, TableDef) error
	DelTableDef(context.Context, TableDef) error

	// second argument is the number of reader, third argument is the filter extend, foruth parameter is the payload required by the engine
	NewReader(context.Context, int, *plan.Expr, [][]byte) ([]Reader, error)
}

type Reader interface {
	Close() error
	Read([]string, *plan.Expr, *mheap.Mheap) (*batch.Batch, error)
}

type Database interface {
	Relations(context.Context) ([]string, error)
	Relation(context.Context, string) (Relation, error)

	Delete(context.Context, string) error
	Create(context.Context, string, []TableDef) error // Create Table - (name, table define)
}

type Engine interface {
	Delete(context.Context, string, client.TxnOperator) error

	Create(context.Context, string, client.TxnOperator) error // Create Database - (name, engine type)

	Databases(context.Context, client.TxnOperator) ([]string, error)
	Database(context.Context, string, client.TxnOperator) (Database, error)

	Nodes() (Nodes, error)
}
