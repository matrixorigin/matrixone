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

package plan

import (
	"bytes"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/extend"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

const (
	FULL = iota
	LEFT
	SEMI
	ANTI
	INNER
	CROSS
	RIGHT
	NATURAL
	RELATION
)

// Direction for ordering results.
type Direction int8

// Direction values.
const (
	DefaultDirection Direction = iota
	Ascending
	Descending
)

type Plan interface {
	fmt.Stringer
	ResultColumns() []*Attribute
}

type Attribute struct {
	Ref  int        // reference count
	Name string     // name of attribute
	Type types.Type // type of attribute
}

type Aggregation struct {
	Op    int     //  opcode of aggregation function
	Ref   int     // reference count
	Type  types.T //  return type of aggregation function
	Name  string  //  name of attribute
	Alias string
	E     extend.Extend
}

type Relation struct {
	Rows   int64
	Name   string                // table name
	Schema string                // schema name
	Attrs  map[string]*Attribute // table's column information

	Flg  bool // indicate if transform is required
	Proj Projection
	Cond extend.Extend

	FreeVars  []string
	BoundVars []*Aggregation
}

type DerivedRelation struct {
	Proj Projection
	Cond extend.Extend

	Flg       bool // indicate if transform is required
	FreeVars  []string
	BoundVars []*Aggregation
}

type SymbolTable struct {
	Entries map[string]*Attribute
}

type ResultAttributes struct {
	Attrs    []string
	AttrsMap map[string]*Attribute
}

type Scope struct {
	Name     string
	Children []*Scope
	Op       interface{}
	Result   ResultAttributes
}

// R.Rattr = S.Sattr
type JoinCondition struct {
	R     int
	S     int
	Rattr string
	Sattr string

	Alias int
}

type ScopeSet struct {
	JoinType int
	Scopes   []*Scope
	Conds    []*JoinCondition
}

type Query struct {
	Flg        bool // rebuild flag
	Scope      *Scope
	Result     []string
	Stack      []*ScopeSet
	Aggs       []*Aggregation
	RenameRels map[string]*Scope
	Rels       map[string]map[string]*Scope

	Children []*Scope // subquery
}

type Field struct {
	Attr string
	Type Direction
}

type Join struct {
	Type   int // join type
	Vars   [][]int
	Result []string
}

type Dedup struct {
}

type Order struct {
	Fs []*Field
}

type Limit struct {
	Limit int64
}

type Offset struct {
	Offset int64
}

type Restrict struct {
	E extend.Extend
}

type Untransform struct {
	FreeVars []string
}

type Rename struct {
	Rs []uint64 // reference count list
	As []string // alias name list
	Es []extend.Extend
}

type Projection struct {
	Rs []uint64 // reference count list
	As []string // alias name list
	Es []extend.Extend
}

type ResultProjection struct {
	Rs []uint64 // reference count list
	As []string // alias name list
	Es []extend.Extend
}

type Edge struct {
	Vs []int
}

type Graph struct {
	Es []*Edge
}

type VertexSet struct {
	Is []int
	Es []*Edge
}

type EdgeSet struct {
	W      int // weight
	I1, I2 int // subscript for E1 and E2
	E1, E2 *Edge
}

type CreateDatabase struct {
	IfNotExistFlag bool
	Id             string
	E              engine.Engine
}

type CreateTable struct {
	IfNotExistFlag bool
	Id             string
	Db             engine.Database
	Defs           []engine.TableDef
	PartitionBy    *engine.PartitionByDef
}

type CreateIndex struct {
	IfNotExistFlag bool
	HasExist       bool // if true, means this index has existed.
	Id             string
	Relation       engine.Relation
	Defs           []engine.TableDef
}

type DropDatabase struct {
	IfExistFlag bool
	Id          string
	E           engine.Engine
}

type DropTable struct {
	IfExistFlag bool
	Ids         []string
	Dbs         []string
	E           engine.Engine
}

type DropIndex struct {
	IfExistFlag bool
	NotExisted  bool // if true, means this index does not exist.
	Id          string
	Relation    engine.Relation
}

type ShowDatabases struct {
	E    engine.Engine
	Like []byte
	//Where     extend.Extend
}

type ShowTables struct {
	Db   engine.Database
	Like []byte
}

type ShowColumns struct {
	Relation engine.Relation
	Like     []byte
}

type ShowCreateTable struct {
	Relation  engine.Relation
	TableName string
}

type ShowCreateDatabase struct {
	IfNotExistFlag bool
	Id             string
	E              engine.Engine
}

type Insert struct {
	Id       string
	Db       string
	Bat      *batch.Batch
	Relation engine.Relation
}

type Delete struct {
	Qry *Query
}

type Update struct {
	Qry *Query
}

type build struct {
	flg bool   // use for having clause
	db  string // name of schema
	sql string
	e   engine.Engine
}

func (qry *Query) ResultColumns() []*Attribute {
	attrs := make([]*Attribute, len(qry.Result))
	for i, attr := range qry.Result {
		attrs[i] = qry.Scope.Result.AttrsMap[attr]
	}
	return attrs
}

func (qry *Query) String() string {
	var buf bytes.Buffer

	printScopes(nil, []*Scope{qry.Scope}, &buf)
	return buf.String()
}

func (n *Field) String() string {
	s := n.Attr
	if n.Type != DefaultDirection {
		s += " " + n.Type.String()
	}
	return s
}

func (attr *Attribute) String() string {
	return attr.Name
}

var directionName = [...]string{
	DefaultDirection: "",
	Ascending:        "ASC",
	Descending:       "DESC",
}

func (i Direction) String() string {
	if i < 0 || i > Direction(len(directionName)-1) {
		return fmt.Sprintf("Direction(%d)", i)
	}
	return directionName[i]
}

func (c CreateDatabase) String() string {
	var buf bytes.Buffer
	buf.WriteString("create database ")
	if c.IfNotExistFlag {
		buf.WriteString("if not exists ")
	}
	buf.WriteString(fmt.Sprintf("%s", c.Id))
	return buf.String()
}

func (c CreateDatabase) ResultColumns() []*Attribute {
	return nil
}

func (c CreateTable) String() string {
	var buf bytes.Buffer
	buf.WriteString("create table ")
	if c.IfNotExistFlag {
		buf.WriteString("if not exists ")
	}
	// todo: db name lost.
	buf.WriteString(fmt.Sprintf("%s (", c.Id))
	for i := range c.Defs {
		_ = i
		buf.WriteString("\n")
		// column def
		// index def
		// constraint def
	}
	buf.WriteString(")")
	if c.PartitionBy != nil {
		// list
		// range
		// hash
	}
	return buf.String()
}

func (c CreateTable) ResultColumns() []*Attribute {
	return nil
}

func (c CreateIndex) String() string {
	var buf bytes.Buffer
	buf.WriteString("create index ")
	if c.IfNotExistFlag {
		buf.WriteString("if not exists ")
	}
	for _, def := range c.Defs {
		buf.WriteString(fmt.Sprintf("%s", def.(*engine.AttributeDef).Attr.Name))
	}
	buf.WriteString(fmt.Sprintf("on %s", c.Relation))
	return buf.String()
}

func (c CreateIndex) ResultColumns() []*Attribute {
	return nil
}

func (d DropDatabase) String() string {
	var buf bytes.Buffer
	buf.WriteString("drop database ")
	if d.IfExistFlag {
		buf.WriteString("if exists ")
	}
	buf.WriteString(d.Id)
	return buf.String()
}

func (d DropDatabase) ResultColumns() []*Attribute {
	return nil
}

func (d DropTable) String() string {
	var buf bytes.Buffer
	buf.WriteString("drop table ")
	if d.IfExistFlag {
		buf.WriteString("if exists")
	}
	for i := 0; i < len(d.Dbs); i++ {
		buf.WriteString(d.Dbs[i] + "." + d.Ids[i])
	}
	return buf.String()
}

func (d DropTable) ResultColumns() []*Attribute {
	return nil
}

func (d DropIndex) String() string {
	var buf bytes.Buffer
	buf.WriteString("drop index ")
	if d.IfExistFlag {
		buf.WriteString("if exists ")
	}
	buf.WriteString(d.Id)
	return buf.String()
}

func (d DropIndex) ResultColumns() []*Attribute {
	return nil
}

func (s ShowDatabases) String() string {
	var buf bytes.Buffer
	buf.WriteString("show databases")
	if s.Like != nil {
		buf.WriteString(fmt.Sprintf(" likes %s", string(s.Like)))
	}
	return buf.String()
}

func (s ShowDatabases) ResultColumns() []*Attribute {
	return []*Attribute{
		&Attribute{
			Ref:  1,
			Name: "Databases",
			Type: types.Type{
				Oid:  types.T_varchar,
				Size: 24,
			},
		},
	}
}

func (s ShowTables) String() string {
	var buf bytes.Buffer
	buf.WriteString("show tables")
	if s.Like != nil {
		buf.WriteString(fmt.Sprintf(" likes %s", string(s.Like)))
	}
	return buf.String()
}

func (s ShowTables) ResultColumns() []*Attribute {
	return []*Attribute{
		&Attribute{
			Ref:  1,
			Name: fmt.Sprintf("Tables"),
			Type: types.Type{
				Oid:  types.T_varchar,
				Size: 24,
			},
		},
	}
}

func (s ShowColumns) String() string {
	var buf bytes.Buffer
	buf.WriteString("show columns")
	return buf.String()
}

func (s ShowColumns) ResultColumns() []*Attribute {
	attrs := []*Attribute{
		&Attribute{Ref: 1, Name: "Field", Type: types.Type{Oid: types.T_varchar, Size: 24}},
		&Attribute{Ref: 1, Name: "Type", Type: types.Type{Oid: types.T_varchar, Size: 24}},
		&Attribute{Ref: 1, Name: "Null", Type: types.Type{Oid: types.T_varchar, Size: 24}},
		&Attribute{Ref: 1, Name: "Key", Type: types.Type{Oid: types.T_varchar, Size: 24}},
		&Attribute{Ref: 1, Name: "Default", Type: types.Type{Oid: types.T_varchar, Size: 24}},
		&Attribute{Ref: 1, Name: "Extra", Type: types.Type{Oid: types.T_varchar, Size: 24}},
	}
	return attrs
}

func (s ShowCreateTable) String() string {
	var buf bytes.Buffer
	buf.WriteString("show create table")
	return buf.String()
}

func (s ShowCreateTable) ResultColumns() []*Attribute {
	attrs := []*Attribute{
		&Attribute{Ref: 1, Name: "Table", Type: types.Type{Oid: types.T_varchar, Size: 24}},
		&Attribute{Ref: 1, Name: "Create Table", Type: types.Type{Oid: types.T_varchar, Size: 24}},
	}
	return attrs
}

func (d ShowCreateDatabase) String() string {
	var buf bytes.Buffer
	buf.WriteString("show create database ")
	if d.IfNotExistFlag {
		buf.WriteString("if not exists ")
	}
	buf.WriteString(d.Id)
	return buf.String()
}

func (d ShowCreateDatabase) ResultColumns() []*Attribute {
	attrs := []*Attribute{
		&Attribute{Ref: 1, Name: "Database", Type: types.Type{Oid: types.T_varchar, Size: 24}},
		&Attribute{Ref: 1, Name: "Show Database", Type: types.Type{Oid: types.T_varchar, Size: 24}},
	}
	return attrs
}

func (i Insert) String() string {
	var buf bytes.Buffer
	buf.WriteString("insert into")
	// index
	// values
	return buf.String()
}

func (i Insert) ResultColumns() []*Attribute {
	return nil
}

func (d Delete) String() string {
	var buf bytes.Buffer
	buf.WriteString("delete from")
	// TODO: where, Order, Limit
	return buf.String()
}

func (d Delete) ResultColumns() []*Attribute {
	return nil
}

func (p Update) String() string {
	var buf bytes.Buffer
	buf.WriteString("update table")
	// TODO: where, Order, Limit
	return buf.String()
}

func (p Update) ResultColumns() []*Attribute {
	return nil
}
