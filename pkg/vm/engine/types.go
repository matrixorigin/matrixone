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
	"context"
	"encoding/binary"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/compress"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	logservicepb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
)

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
	// Column ID
	ID uint64
	// Name name of attribute
	Name string
	// Alg compression algorithm
	Alg compress.T
	// Type attribute's type
	Type types.Type
	// DefaultExpr default value of this attribute
	Default *plan.Default
	// to update col when define in create table
	OnUpdate *plan.OnUpdate
	// Primary is primary key or not
	Primary bool
	// Clusterby means sort by this column
	ClusterBy bool
	// Comment of attribute
	Comment string
	// AutoIncrement is auto incr or not
	AutoIncrement bool
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

type ClusterByDef struct {
	Name string
}

type Statistics interface {
	FilteredStats(ctx context.Context, expr *plan.Expr) (int32, int64, error)
	Stats(ctx context.Context) (int32, int64, error)
	Rows(ctx context.Context) (int64, error)
	Size(ctx context.Context, columnName string) (int64, error)
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

type PartitionDef struct {
	Partition string
}

type ViewDef struct {
	View string
}

type UniqueIndexDef struct {
	UniqueIndex string
}

type SecondaryIndexDef struct {
	SecondaryIndex string
}

type TableDef interface {
	tableDef()
}

func (*CommentDef) tableDef()      {}
func (*PartitionDef) tableDef()    {}
func (*ViewDef) tableDef()         {}
func (*AttributeDef) tableDef()    {}
func (*IndexTableDef) tableDef()   {}
func (*PropertiesDef) tableDef()   {}
func (*PrimaryIndexDef) tableDef() {}
func (*ClusterByDef) tableDef()    {}
func (*ConstraintDef) tableDef()   {}

type ConstraintDef struct {
	Cts []Constraint
}

type ConstraintType int8

const (
	UniqueIndex ConstraintType = iota
	SecondaryIndex
)

func (c *ConstraintDef) MarshalBinary() (data []byte, err error) {
	buf := bytes.NewBuffer(make([]byte, 0))
	for _, ct := range c.Cts {
		switch def := ct.(type) {
		case *UniqueIndexDef:
			if err := binary.Write(buf, binary.BigEndian, UniqueIndex); err != nil {
				return nil, err
			}
			if err := binary.Write(buf, binary.BigEndian, uint64(len([]byte(def.UniqueIndex)))); err != nil {
				return nil, err
			}
			buf.Write([]byte(def.UniqueIndex))
		case *SecondaryIndexDef:
			if err := binary.Write(buf, binary.BigEndian, SecondaryIndex); err != nil {
				return nil, err
			}
			if err := binary.Write(buf, binary.BigEndian, uint64(len([]byte(def.SecondaryIndex)))); err != nil {
				return nil, err
			}
			buf.Write([]byte(def.SecondaryIndex))
		}
	}
	return buf.Bytes(), nil
}

func (c *ConstraintDef) UnmarshalBinary(data []byte) error {
	l := 0
	var length uint64
	for l < len(data) {
		typ := ConstraintType(data[l])
		l += 1
		switch typ {
		case UniqueIndex:
			length = binary.BigEndian.Uint64(data[l : l+8])
			l += 8
			c.Cts = append(c.Cts, &UniqueIndexDef{UniqueIndex: string(data[l : l+int(length)])})
			l += int(length)
		case SecondaryIndex:
			length = binary.BigEndian.Uint64(data[l : l+8])
			l += 8
			c.Cts = append(c.Cts, &SecondaryIndexDef{SecondaryIndex: string(data[l : l+int(length)])})
			l += int(length)
		}
	}
	return nil
}

type Constraint interface {
	constraint()
}

// TODO: UniqueIndexDef, SecondaryIndexDef will not be tabledef and need to be moved in Constraint to be able modified
func (*UniqueIndexDef) constraint()    {}
func (*SecondaryIndexDef) constraint() {}

type Relation interface {
	Statistics

	Ranges(context.Context, *plan.Expr) ([][]byte, error)

	TableDefs(context.Context) ([]TableDef, error)

	GetPrimaryKeys(context.Context) ([]*Attribute, error)

	GetHideKeys(context.Context) ([]*Attribute, error)

	Write(context.Context, *batch.Batch) error

	Update(context.Context, *batch.Batch) error

	// Delete(context.Context, *vector.Vector, string) error
	Delete(context.Context, *batch.Batch, string) error

	AddTableDef(context.Context, TableDef) error
	DelTableDef(context.Context, TableDef) error

	// only ConstraintDef can be modified
	UpdateConstraint(context.Context, *ConstraintDef) error

	GetTableID(context.Context) uint64

	// second argument is the number of reader, third argument is the filter extend, foruth parameter is the payload required by the engine
	NewReader(context.Context, int, *plan.Expr, [][]byte) ([]Reader, error)

	TableColumns(ctx context.Context) ([]*Attribute, error)
}

type Reader interface {
	Close() error
	Read(context.Context, []string, *plan.Expr, *mpool.MPool) (*batch.Batch, error)
}

type Database interface {
	Relations(context.Context) ([]string, error)
	Relation(context.Context, string) (Relation, error)

	Delete(context.Context, string) error
	Create(context.Context, string, []TableDef) error // Create Table - (name, table define)
	Truncate(context.Context, string) error
	GetDatabaseId(context.Context) string
}

type Engine interface {
	// transaction interface
	New(ctx context.Context, op client.TxnOperator) error
	Commit(ctx context.Context, op client.TxnOperator) error
	Rollback(ctx context.Context, op client.TxnOperator) error

	// Delete deletes a database
	Delete(ctx context.Context, databaseName string, op client.TxnOperator) error

	// Create creates a database
	Create(ctx context.Context, databaseName string, op client.TxnOperator) error

	// Databases returns all database names
	Databases(ctx context.Context, op client.TxnOperator) (databaseNames []string, err error)

	// Database creates a handle for a database
	Database(ctx context.Context, databaseName string, op client.TxnOperator) (Database, error)

	// Nodes returns all nodes for worker jobs
	Nodes() (cnNodes Nodes, err error)

	// Hints returns hints of engine features
	// return value should not be cached
	// since implementations may update hints after engine had initialized
	Hints() Hints

	NewBlockReader(ctx context.Context, num int, ts timestamp.Timestamp,
		expr *plan.Expr, ranges [][]byte, tblDef *plan.TableDef) ([]Reader, error)
}

type Hints struct {
	CommitOrRollbackTimeout time.Duration
}

type GetClusterDetailsFunc = func() (logservicepb.ClusterDetails, error)
