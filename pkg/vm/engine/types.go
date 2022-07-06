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
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
)

type Nodes []Node

type Node struct {
	Id   string `json:"id"`
	Addr string `json:"address"`
	Data []byte `json:"payload"`
}

type Attribute struct {
	IsHide  bool
	Name    string      // name of attribute
	Type    types.Type  // type of attribute
	Default DefaultExpr // default value of this attribute.
	Primary bool        // if true, it is primary key
}

type DefaultExpr struct {
	Exist  bool
	Expr   *plan.Expr
	IsNull bool
}

type Statistics interface {
	Rows() int64
	Size(string) int64
	Cardinal(string) int64
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

func (*CommentDef) tableDef()   {}
func (*AttributeDef) tableDef() {}

type Relation interface {
	Statistics

	Nodes(*plan.Expr) Nodes

	TableDefs() []TableDef

	GetPrimaryKey() Attribute

	Write(uint64, *batch.Batch) error

	Update(uint64, *batch.Batch) error

	Delete(uint64, vector.AnyVector, string) error

	Truncate() (uint64, error)

	AddTableDef(TableDef) error
	DelTableDef(TableDef) error

	// first argument is the number of reader, second argument is the filter extend,  third parameter is the payload required by the engine
	NewReader(int, *plan.Expr, []byte) []Reader
}

type Reader interface {
	Read([]string) (*batch.Batch, error)
}

type Database interface {
	Relations() []string
	Relation(string) (Relation, error)

	Delete(string) error
	Create(string, []TableDef) error // Create Table - (name, table define)
}

type Engine interface {
	Delete(string, client.TxnOperator) error
	Create(string, client.TxnOperator) error // Create Database

	Databases(client.TxnOperator) []string
	Database(string, client.TxnOperator) (Database, error)
}
