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

type namedAttrGetter interface {
	attrByName(name string, tx *Transaction, handler *MemHandler, typ types.T) (any, error)
}

type DatabaseRow struct {
	ID        string
	NumberID  uint64
	AccountID uint32 // 0 is the sys account
	Name      string
}

func (d DatabaseRow) PrimaryKey() Text {
	return Text(d.ID)
}

func (d DatabaseRow) Indexes() []AnyKey {
	return []AnyKey{
		{index_AccountID, Uint(d.AccountID)},
		{index_AccountID_Name, Uint(d.AccountID), Text(d.Name)},
	}
}

var _ namedAttrGetter = DatabaseRow{}

func (d DatabaseRow) attrByName(name string, tx *Transaction, handler *MemHandler, typ types.T) (any, error) {
	switch name {
	case catalog.SystemDBAttr_Name:
		return d.Name, nil
	case catalog.SystemDBAttr_CatalogName:
		return "", nil
	case catalog.SystemDBAttr_CreateSQL:
		return "", nil
	case catalog.SystemDBAttr_ID:
		return d.NumberID, nil
	}
	panic(fmt.Errorf("fixme: %s", name))
}

type RelationRow struct {
	ID           string
	NumberID     uint64
	DatabaseID   string
	Name         string
	Type         txnengine.RelationType
	Comments     string
	Properties   map[string]string
	PartitionDef string
	ViewDef      string
}

func (r RelationRow) PrimaryKey() Text {
	return Text(r.ID)
}

func (r RelationRow) Indexes() []AnyKey {
	return []AnyKey{
		{index_DatabaseID, Text(r.DatabaseID)},
		{index_DatabaseID_Name, Text(r.DatabaseID), Text(r.Name)},
	}
}

var _ namedAttrGetter = RelationRow{}

func (r RelationRow) attrByName(name string, tx *Transaction, handler *MemHandler, typ types.T) (any, error) {
	switch name {
	case catalog.SystemRelAttr_ID:
		return r.NumberID, nil
	case catalog.SystemRelAttr_Name:
		return r.Name, nil
	case catalog.SystemRelAttr_DBName:
		if r.DatabaseID == "" {
			return "", nil
		}
		db, err := handler.databases.Get(tx, Text(r.DatabaseID))
		if err != nil {
			return true, err
		}
		return db.Name, nil
	case catalog.SystemRelAttr_Persistence:
		return "", nil
	case catalog.SystemRelAttr_Kind:
		return "r", nil
	case catalog.SystemRelAttr_Comment:
		return r.Comments, nil
	case catalog.SystemRelAttr_CreateSQL:
		return "", nil
	case catalog.SystemRelAttr_DBID:
		if r.DatabaseID == "" {
			return "", nil
		}
		db, err := handler.databases.Get(tx, Text(r.DatabaseID))
		if err != nil {
			return true, err
		}
		return db.NumberID, nil
	}
	panic(fmt.Errorf("fixme: %s", name))
}

type AttributeRow struct {
	ID         string
	RelationID string
	Order      int
	Nullable   bool
	engine.Attribute
}

func (a AttributeRow) PrimaryKey() Text {
	return Text(a.ID)
}

func (a AttributeRow) Indexes() []AnyKey {
	return []AnyKey{
		{index_RelationID, Text(a.RelationID)},
		{index_RelationID_Name, Text(a.RelationID), Text(a.Name)},
		{index_RelationID_IsPrimary, Text(a.RelationID), Bool(a.Primary)},
		{index_RelationID_IsHidden, Text(a.RelationID), Bool(a.IsHidden)},
	}
}

func (a AttributeRow) attrByName(name string, tx *Transaction, handler *MemHandler, typ types.T) (any, error) {
	switch name {
	case catalog.SystemColAttr_DBName:
		rel, err := handler.relations.Get(tx, Text(a.RelationID))
		if err != nil {
			return true, err
		}
		if rel.DatabaseID == "" {
			return "", nil
		}
		db, err := handler.databases.Get(tx, Text(rel.DatabaseID))
		if err != nil {
			return true, err
		}
		return db.Name, nil
	case catalog.SystemColAttr_RelName:
		rel, err := handler.relations.Get(tx, Text(a.RelationID))
		if err != nil {
			return true, err
		}
		return rel.Name, nil
	case catalog.SystemColAttr_Name:
		return a.Name, nil
	case catalog.SystemColAttr_Type:
		return int32(a.Type.Oid), nil
	case catalog.SystemColAttr_Num:
		return int32(a.Order), nil
	case catalog.SystemColAttr_Length:
		return int32(a.Type.Size), nil
	case catalog.SystemColAttr_NullAbility:
		return boolToInt8(a.Nullable), nil
	case catalog.SystemColAttr_HasExpr:
		return boolToInt8(a.Default.Expr != nil), nil
	case catalog.SystemColAttr_DefaultExpr:
		return a.Default.Expr.String(), nil
	case catalog.SystemColAttr_IsDropped:
		return boolToInt8(false), nil
	case catalog.SystemColAttr_ConstraintType:
		if a.Primary {
			return "p", nil
		} else {
			return "n", nil
		}
	case catalog.SystemColAttr_IsUnsigned:
		return boolToInt8(a.Type.Oid == types.T_uint8 ||
			a.Type.Oid == types.T_uint16 ||
			a.Type.Oid == types.T_uint32 ||
			a.Type.Oid == types.T_uint64 ||
			a.Type.Oid == types.T_uint128), nil
	case catalog.SystemColAttr_IsAutoIncrement:
		return boolToInt8(false), nil //TODO
	case catalog.SystemColAttr_Comment:
		return a.Comment, nil
	case catalog.SystemColAttr_IsHidden:
		return boolToInt8(a.IsHidden), nil
	}
	panic(fmt.Errorf("fixme: %s", name))
}

type IndexRow struct {
	ID         string
	RelationID string
	engine.IndexTableDef
}

func (i IndexRow) PrimaryKey() Text {
	return Text(i.ID)
}

func (i IndexRow) Indexes() []AnyKey {
	return []AnyKey{
		{index_RelationID, Text(i.RelationID)},
		{index_RelationID_Name, Text(i.RelationID), Text(i.Name)},
	}
}
