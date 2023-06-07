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
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
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
	Rel  Relation // local relation
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
	// Seqnum, do not change during the whole lifetime of the table
	Seqnum uint16
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
	Stats(ctx context.Context, partitionTables []any, statsInfoMap any) bool
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

type VersionDef struct {
	Version uint32
}

type PartitionDef struct {
	Partitioned int8
	Partition   string
}

type ViewDef struct {
	View string
}

type IndexDef struct {
	Indexes []*plan.IndexDef
}

type ForeignKeyDef struct {
	Fkeys []*plan.ForeignKeyDef
}

type PrimaryKeyDef struct {
	Pkey *plan.PrimaryKeyDef
}

type RefChildTableDef struct {
	Tables []uint64
}

type TableDef interface {
	tableDef()

	// ToPBVersion returns corresponding PB struct.
	ToPBVersion() TableDefPB
}

func (*CommentDef) tableDef()    {}
func (*VersionDef) tableDef()    {}
func (*PartitionDef) tableDef()  {}
func (*ViewDef) tableDef()       {}
func (*AttributeDef) tableDef()  {}
func (*IndexTableDef) tableDef() {}
func (*PropertiesDef) tableDef() {}
func (*ClusterByDef) tableDef()  {}
func (*ConstraintDef) tableDef() {}

func (def *CommentDef) ToPBVersion() TableDefPB {
	return TableDefPB{
		Def: &TableDefPB_CommentDef{
			CommentDef: def,
		},
	}
}

func (def *VersionDef) ToPBVersion() TableDefPB {
	return TableDefPB{
		Def: &TableDefPB_VersionDef{
			VersionDef: def,
		},
	}
}

func (def *PartitionDef) ToPBVersion() TableDefPB {
	return TableDefPB{
		Def: &TableDefPB_PartitionDef{
			PartitionDef: def,
		},
	}
}

func (def *ViewDef) ToPBVersion() TableDefPB {
	return TableDefPB{
		Def: &TableDefPB_ViewDef{
			ViewDef: def,
		},
	}
}

func (def *AttributeDef) ToPBVersion() TableDefPB {
	return TableDefPB{
		Def: &TableDefPB_AttributeDef{
			AttributeDef: def,
		},
	}
}

func (def *IndexTableDef) ToPBVersion() TableDefPB {
	return TableDefPB{
		Def: &TableDefPB_IndexTableDef{
			IndexTableDef: def,
		},
	}
}

func (def *PropertiesDef) ToPBVersion() TableDefPB {
	return TableDefPB{
		Def: &TableDefPB_PropertiesDef{
			PropertiesDef: def,
		},
	}
}

func (def *ClusterByDef) ToPBVersion() TableDefPB {
	return TableDefPB{
		Def: &TableDefPB_ClusterByDef{
			ClusterByDef: def,
		},
	}
}

func (def *ConstraintDef) ToPBVersion() TableDefPB {
	cts := make([]ConstraintPB, 0, len(def.Cts))
	for i := 0; i < len(def.Cts); i++ {
		cts = append(cts, def.Cts[i].ToPBVersion())
	}

	return TableDefPB{
		Def: &TableDefPB_ConstraintDefPB{
			ConstraintDefPB: &ConstraintDefPB{
				Cts: cts,
			},
		},
	}
}

func (def *TableDefPB) FromPBVersion() TableDef {
	if r := def.GetCommentDef(); r != nil {
		return r
	}
	if r := def.GetPartitionDef(); r != nil {
		return r
	}
	if r := def.GetViewDef(); r != nil {
		return r
	}
	if r := def.GetAttributeDef(); r != nil {
		return r
	}
	if r := def.GetIndexTableDef(); r != nil {
		return r
	}
	if r := def.GetPropertiesDef(); r != nil {
		return r
	}
	if r := def.GetClusterByDef(); r != nil {
		return r
	}
	if r := def.GetConstraintDefPB(); r != nil {
		return r.FromPBVersion()
	}
	panic("no corresponding type")
}

type ConstraintDef struct {
	Cts []Constraint
}

type ConstraintType int8

const (
	Index ConstraintType = iota
	RefChildTable
	ForeignKey
	PrimaryKey
)

type EngineType int8

const (
	Disttae EngineType = iota
	Memory
	UNKNOWN
)

func (def *ConstraintDef) MarshalBinary() (data []byte, err error) {
	buf := bytes.NewBuffer(make([]byte, 0))
	for _, ct := range def.Cts {
		switch def := ct.(type) {
		case *IndexDef:
			if err := binary.Write(buf, binary.BigEndian, Index); err != nil {
				return nil, err
			}
			if err := binary.Write(buf, binary.BigEndian, uint64(len(def.Indexes))); err != nil {
				return nil, err
			}

			for _, indexdef := range def.Indexes {
				bytes, err := indexdef.Marshal()
				if err != nil {
					return nil, err
				}
				if err := binary.Write(buf, binary.BigEndian, uint64(len(bytes))); err != nil {
					return nil, err
				}
				buf.Write(bytes)
			}
		case *RefChildTableDef:
			if err := binary.Write(buf, binary.BigEndian, RefChildTable); err != nil {
				return nil, err
			}
			if err := binary.Write(buf, binary.BigEndian, uint64(len(def.Tables))); err != nil {
				return nil, err
			}
			for _, tblId := range def.Tables {
				if err := binary.Write(buf, binary.BigEndian, tblId); err != nil {
					return nil, err
				}
			}

		case *ForeignKeyDef:
			if err := binary.Write(buf, binary.BigEndian, ForeignKey); err != nil {
				return nil, err
			}
			if err := binary.Write(buf, binary.BigEndian, uint64(len(def.Fkeys))); err != nil {
				return nil, err
			}
			for _, fk := range def.Fkeys {
				bytes, err := fk.Marshal()
				if err != nil {
					return nil, err
				}

				if err := binary.Write(buf, binary.BigEndian, uint64(len(bytes))); err != nil {
					return nil, err
				}
				buf.Write(bytes)
			}
		case *PrimaryKeyDef:
			if err := binary.Write(buf, binary.BigEndian, PrimaryKey); err != nil {
				return nil, err
			}
			bytes, err := def.Pkey.Marshal()
			if err != nil {
				return nil, err
			}
			if err := binary.Write(buf, binary.BigEndian, uint64((len(bytes)))); err != nil {
				return nil, err
			}
			buf.Write(bytes)
		}
	}
	return buf.Bytes(), nil
}

func (def *ConstraintDef) UnmarshalBinary(data []byte) error {
	l := 0
	var length uint64
	for l < len(data) {
		typ := ConstraintType(data[l])
		l += 1
		switch typ {
		case Index:
			length = binary.BigEndian.Uint64(data[l : l+8])
			l += 8
			indexes := make([]*plan.IndexDef, length)

			for i := 0; i < int(length); i++ {
				dataLength := binary.BigEndian.Uint64(data[l : l+8])
				l += 8
				indexdef := &plan.IndexDef{}
				err := indexdef.Unmarshal(data[l : l+int(dataLength)])
				if err != nil {
					return err
				}
				l += int(dataLength)
				indexes[i] = indexdef
			}
			def.Cts = append(def.Cts, &IndexDef{indexes})
		case RefChildTable:
			length = binary.BigEndian.Uint64(data[l : l+8])
			l += 8
			tables := make([]uint64, length)
			for i := 0; i < int(length); i++ {
				tblId := binary.BigEndian.Uint64(data[l : l+8])
				l += 8
				tables[i] = tblId
			}
			def.Cts = append(def.Cts, &RefChildTableDef{tables})

		case ForeignKey:
			length = binary.BigEndian.Uint64(data[l : l+8])
			l += 8
			fKeys := make([]*plan.ForeignKeyDef, length)

			for i := 0; i < int(length); i++ {
				dataLength := binary.BigEndian.Uint64(data[l : l+8])
				l += 8
				fKey := &plan.ForeignKeyDef{}
				err := fKey.Unmarshal(data[l : l+int(dataLength)])
				if err != nil {
					return err
				}
				l += int(dataLength)
				fKeys[i] = fKey
			}
			def.Cts = append(def.Cts, &ForeignKeyDef{fKeys})

		case PrimaryKey:
			length = binary.BigEndian.Uint64(data[l : l+8])
			l += 8
			pkey := &plan.PrimaryKeyDef{}
			err := pkey.Unmarshal(data[l : l+int(length)])
			if err != nil {
				return err
			}
			l += int(length)
			def.Cts = append(def.Cts, &PrimaryKeyDef{pkey})
		}
	}
	return nil
}

func (def *ConstraintDefPB) FromPBVersion() *ConstraintDef {
	cts := make([]Constraint, 0, len(def.Cts))
	for i := 0; i < len(def.Cts); i++ {
		cts = append(cts, def.Cts[i].FromPBVersion())
	}
	return &ConstraintDef{
		Cts: cts,
	}
}

func (def *ConstraintPB) FromPBVersion() Constraint {
	if r := def.GetForeignKeyDef(); r != nil {
		return r
	}
	if r := def.GetPrimaryKeyDef(); r != nil {
		return r
	}
	if r := def.GetRefChildTableDef(); r != nil {
		return r
	}
	if r := def.GetIndexDef(); r != nil {
		return r
	}
	panic("no corresponding type")
}

// get the primary key definition in the constraint, and return null if there is no primary key
func (def *ConstraintDef) GetPrimaryKeyDef() *PrimaryKeyDef {
	for _, ct := range def.Cts {
		if ctVal, ok := ct.(*PrimaryKeyDef); ok {
			return ctVal
		}
	}
	return nil
}

type Constraint interface {
	constraint()

	// ToPBVersion returns corresponding PB struct.
	ToPBVersion() ConstraintPB
}

// TODO: UniqueIndexDef, SecondaryIndexDef will not be tabledef and need to be moved in Constraint to be able modified
func (*ForeignKeyDef) constraint()    {}
func (*PrimaryKeyDef) constraint()    {}
func (*RefChildTableDef) constraint() {}
func (*IndexDef) constraint()         {}

func (def *ForeignKeyDef) ToPBVersion() ConstraintPB {
	return ConstraintPB{
		Ct: &ConstraintPB_ForeignKeyDef{
			ForeignKeyDef: def,
		},
	}
}
func (def *PrimaryKeyDef) ToPBVersion() ConstraintPB {
	return ConstraintPB{
		Ct: &ConstraintPB_PrimaryKeyDef{
			PrimaryKeyDef: def,
		},
	}
}
func (def *RefChildTableDef) ToPBVersion() ConstraintPB {
	return ConstraintPB{
		Ct: &ConstraintPB_RefChildTableDef{
			RefChildTableDef: def,
		},
	}
}
func (def *IndexDef) ToPBVersion() ConstraintPB {
	return ConstraintPB{
		Ct: &ConstraintPB_IndexDef{
			IndexDef: def,
		},
	}
}

type Relation interface {
	Statistics

	Ranges(context.Context, []*plan.Expr) ([][]byte, error)

	ApplyRuntimeFilters(context.Context, [][]byte, []*plan.Expr, []*pipeline.RuntimeFilter) ([][]byte, error)

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

	AlterTable(ctx context.Context, c *ConstraintDef, constraint [][]byte) error

	GetTableID(context.Context) uint64

	GetDBID(context.Context) uint64

	// second argument is the number of reader, third argument is the filter extend, foruth parameter is the payload required by the engine
	NewReader(context.Context, int, *plan.Expr, [][]byte) ([]Reader, error)

	TableColumns(ctx context.Context) ([]*Attribute, error)

	//max and min values
	MaxAndMinValues(ctx context.Context) ([][2]any, []uint8, error)

	GetEngineType() EngineType

	GetMetadataScanInfoBytes(ctx context.Context, name string) ([][]byte, error)
}

type Reader interface {
	Close() error
	Read(context.Context, []string, *plan.Expr, *mpool.MPool, VectorPool) (*batch.Batch, error)
}

type Database interface {
	Relations(context.Context) ([]string, error)
	Relation(context.Context, string) (Relation, error)

	Delete(context.Context, string) error
	Create(context.Context, string, []TableDef) error // Create Table - (name, table define)
	Truncate(context.Context, string) (uint64, error)
	GetDatabaseId(context.Context) string
	IsSubscription(context.Context) bool
	GetCreateSql(context.Context) string
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

	// Nodes returns all nodes for worker jobs. isInternal, tenant, cnLabel are
	// used to filter CN servers.
	Nodes(isInternal bool, tenant string, username string, cnLabel map[string]string) (cnNodes Nodes, err error)

	// Hints returns hints of engine features
	// return value should not be cached
	// since implementations may update hints after engine had initialized
	Hints() Hints

	NewBlockReader(ctx context.Context, num int, ts timestamp.Timestamp,
		expr *plan.Expr, ranges [][]byte, tblDef *plan.TableDef) ([]Reader, error)

	// Get database name & table name by table id
	GetNameById(ctx context.Context, op client.TxnOperator, tableId uint64) (dbName string, tblName string, err error)

	// Get relation by table id
	GetRelationById(ctx context.Context, op client.TxnOperator, tableId uint64) (dbName string, tblName string, rel Relation, err error)

	// AllocateIDByKey allocate a globally unique ID by key.
	AllocateIDByKey(ctx context.Context, key string) (uint64, error)
}

type VectorPool interface {
	PutBatch(bat *batch.Batch)
	GetVector(typ types.Type) *vector.Vector
}

type Hints struct {
	CommitOrRollbackTimeout time.Duration
}

// EntireEngine is a wrapper for Engine to support temporary table
type EntireEngine struct {
	Engine     Engine // original engine
	TempEngine Engine // new engine for temporarily table
}

func IsMemtable(tblRange []byte) bool {
	return len(tblRange) == 0
}
