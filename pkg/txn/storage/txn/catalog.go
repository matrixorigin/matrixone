// Copyright 2022 Matrix Origin
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

package txnstorage

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/txn/storage/txn/memtable"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	txnengine "github.com/matrixorigin/matrixone/pkg/vm/engine/txn"
)

var (
	index_AccountID            = Text("account id")
	index_AccountID_Name       = Text("account id, name")
	index_DatabaseID           = Text("database id")
	index_DatabaseID_Name      = Text("database id, name")
	index_RelationID           = Text("relation id")
	index_RelationID_IsHidden  = Text("relation id, is hidden")
	index_RelationID_IsPrimary = Text("relation id, is primary")
	index_RelationID_Name      = Text("relation id, name")
	index_RowID                = Text("row id")
)

type (
	DatabaseRowIter  = Iter[ID, *DatabaseRow]
	RelationRowIter  = Iter[ID, *RelationRow]
	AttributeRowIter = Iter[ID, *AttributeRow]
)

type DatabaseRow struct {
	ID        ID
	AccountID uint32 // 0 is the sys account
	Name      string
}

func (d *DatabaseRow) Key() ID {
	return d.ID
}

func (d *DatabaseRow) Value() *DatabaseRow {
	return d
}

func (d *DatabaseRow) Indexes() []Tuple {
	return []Tuple{
		{index_AccountID, Uint(d.AccountID)},
		{index_AccountID_Name, Uint(d.AccountID), Text(d.Name)},
	}
}

var _ NamedRow = new(DatabaseRow)

func (d *DatabaseRow) AttrByName(tx *Transaction, name string) (ret Nullable, err error) {
	switch name {
	case catalog.SystemDBAttr_ID:
		ret.Value = uint64(d.ID)
	case catalog.SystemDBAttr_Name:
		ret.Value = d.Name
	case catalog.SystemDBAttr_CatalogName:
		ret.Value = ""
	case catalog.SystemDBAttr_CreateSQL:
		ret.Value = ""
	case catalog.SystemDBAttr_Owner:
		ret.Value = uint32(d.AccountID)
	case catalog.SystemDBAttr_Creator:
		ret.Value = uint32(d.AccountID)
	case catalog.SystemDBAttr_CreateAt:
		ret.Value = types.Timestamp(0)
	case catalog.SystemDBAttr_AccID:
		ret.Value = uint32(d.AccountID)
	default:
		panic(fmt.Sprintf("fixme: %s", name))
	}
	verifyAttr(catalog.SystemDBSchema.ColDefs, name, ret.Value)
	return
}

func (d *DatabaseRow) SetHandler(handler *MemHandler) {
}

type RelationRow struct {
	ID           ID
	DatabaseID   ID
	Name         string
	Type         txnengine.RelationType
	Comments     string
	Properties   map[string]string
	PartitionDef string
	ViewDef      string

	handler *MemHandler
}

func (r *RelationRow) Key() ID {
	return r.ID
}

func (r *RelationRow) Value() *RelationRow {
	return r
}

func (r *RelationRow) Indexes() []Tuple {
	return []Tuple{
		{index_DatabaseID, r.DatabaseID},
		{index_DatabaseID_Name, r.DatabaseID, Text(r.Name)},
	}
}

var _ NamedRow = new(RelationRow)

func (r *RelationRow) AttrByName(tx *Transaction, name string) (ret Nullable, err error) {
	switch name {
	case catalog.SystemRelAttr_ID:
		ret.Value = uint64(r.ID)
	case catalog.SystemRelAttr_Name:
		ret.Value = r.Name
	case catalog.SystemRelAttr_DBID:
		if r.DatabaseID.IsEmpty() {
			ret.Value = ""
			return
		}
		db, err := r.handler.databases.Get(tx, r.DatabaseID)
		if err != nil {
			return ret, err
		}
		ret.Value = uint64(db.ID)
	case catalog.SystemRelAttr_DBName:
		if r.DatabaseID.IsEmpty() {
			ret.Value = ""
			return
		}
		db, err := r.handler.databases.Get(tx, r.DatabaseID)
		if err != nil {
			return ret, err
		}
		ret.Value = db.Name
	case catalog.SystemRelAttr_Persistence:
		ret.Value = ""
	case catalog.SystemRelAttr_Kind:
		ret.Value = "r"
	case catalog.SystemRelAttr_Comment:
		ret.Value = r.Comments
	case catalog.SystemRelAttr_Partition:
		ret.Value = r.PartitionDef
	case catalog.SystemRelAttr_CreateSQL:
		ret.Value = ""
	case catalog.SystemRelAttr_Owner:
		ret.Value = uint32(0) //TODO
	case catalog.SystemRelAttr_Creator:
		ret.Value = uint32(0) //TODO
	case catalog.SystemRelAttr_CreateAt:
		ret.Value = types.Timestamp(0) //TODO
	case catalog.SystemRelAttr_AccID:
		ret.Value = uint32(0)
	default:
		panic(fmt.Sprintf("fixme: %s", name))
	}
	verifyAttr(catalog.SystemTableSchema.ColDefs, name, ret.Value)
	return
}

func (r *RelationRow) SetHandler(handler *MemHandler) {
	r.handler = handler
}

type AttributeRow struct {
	ID         ID
	RelationID ID
	Order      int
	Nullable   bool
	engine.Attribute

	handler *MemHandler
}

func (a *AttributeRow) Key() ID {
	return a.ID
}

func (a *AttributeRow) Value() *AttributeRow {
	return a
}

func (a *AttributeRow) Indexes() []Tuple {
	return []Tuple{
		{index_RelationID, a.RelationID},
		{index_RelationID_Name, a.RelationID, Text(a.Name)},
		{index_RelationID_IsPrimary, a.RelationID, Bool(a.Primary)},
		{index_RelationID_IsHidden, a.RelationID, Bool(a.IsHidden)},
	}
}

var _ NamedRow = new(AttributeRow)

func (a *AttributeRow) AttrByName(tx *Transaction, name string) (ret Nullable, err error) {
	switch name {
	case catalog.SystemColAttr_UniqName:
		ret.Value = a.Name
	case catalog.SystemColAttr_AccID:
		ret.Value = uint32(0)
	case catalog.SystemColAttr_Name:
		ret.Value = a.Name
	case catalog.SystemColAttr_DBID:
		rel, err := a.handler.relations.Get(tx, a.RelationID)
		if err != nil {
			return ret, err
		}
		if rel.DatabaseID.IsEmpty() {
			ret.Value = ""
			return ret, nil
		}
		db, err := a.handler.databases.Get(tx, rel.DatabaseID)
		if err != nil {
			return ret, err
		}
		ret.Value = uint64(db.ID)
	case catalog.SystemColAttr_DBName:
		rel, err := a.handler.relations.Get(tx, a.RelationID)
		if err != nil {
			return ret, err
		}
		if rel.DatabaseID.IsEmpty() {
			ret.Value = ""
			return ret, nil
		}
		db, err := a.handler.databases.Get(tx, rel.DatabaseID)
		if err != nil {
			return ret, err
		}
		ret.Value = db.Name
	case catalog.SystemColAttr_RelID:
		ret.Value = uint64(a.RelationID)
	case catalog.SystemColAttr_RelName:
		rel, err := a.handler.relations.Get(tx, a.RelationID)
		if err != nil {
			return ret, err
		}
		ret.Value = rel.Name
	case catalog.SystemColAttr_Type:
		ret.Value = int32(a.Type.Oid)
	case catalog.SystemColAttr_Num:
		ret.Value = int32(a.Order)
	case catalog.SystemColAttr_Length:
		ret.Value = int32(a.Type.Size)
	case catalog.SystemColAttr_NullAbility:
		ret.Value = boolToInt8(a.Nullable)
	case catalog.SystemColAttr_HasExpr:
		ret.Value = boolToInt8(a.Default.Expr != nil)
	case catalog.SystemColAttr_DefaultExpr:
		ret.Value = a.Default.Expr.String()
	case catalog.SystemColAttr_IsDropped:
		ret.Value = boolToInt8(false)
	case catalog.SystemColAttr_ConstraintType:
		if a.Primary {
			ret.Value = "p"
		} else {
			ret.Value = "n"
		}
	case catalog.SystemColAttr_IsUnsigned:
		ret.Value = boolToInt8(a.Type.Oid == types.T_uint8 ||
			a.Type.Oid == types.T_uint16 ||
			a.Type.Oid == types.T_uint32 ||
			a.Type.Oid == types.T_uint64 ||
			a.Type.Oid == types.T_uint128)
	case catalog.SystemColAttr_IsAutoIncrement:
		ret.Value = boolToInt8(false)
	case catalog.SystemColAttr_IsHidden:
		ret.Value = boolToInt8(a.IsHidden)
	case catalog.SystemColAttr_Comment:
		ret.Value = a.Comment
	default:
		panic(fmt.Sprintf("fixme: %s", name))
	}
	verifyAttr(catalog.SystemColumnSchema.ColDefs, name, ret.Value)
	return
}

func (a *AttributeRow) SetHandler(handler *MemHandler) {
	a.handler = handler
}

type IndexRow struct {
	ID         ID
	RelationID ID
	engine.IndexTableDef
}

func (i *IndexRow) Key() ID {
	return i.ID
}

func (i *IndexRow) Value() *IndexRow {
	return i
}

func (i *IndexRow) Indexes() []Tuple {
	return []Tuple{
		{index_RelationID, i.RelationID},
		{index_RelationID_Name, i.RelationID, Text(i.Name)},
	}
}

func verifyAttr(
	defs []*catalog.ColDef,
	name string,
	value any,
) {
	for _, attr := range defs {
		if attr.Name != name {
			continue
		}
		if value == nil {
			panic(fmt.Sprintf("%s should not be nil", attr.Name))
		}
		if !memtable.TypeMatch(value, attr.Type.Oid) {
			panic(fmt.Sprintf("%s should be %v typed", name, attr.Type))
		}
	}
}
