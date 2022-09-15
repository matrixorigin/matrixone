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
	"database/sql"
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"sync"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	txnengine "github.com/matrixorigin/matrixone/pkg/vm/engine/txn"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
)

type MemHandler struct {

	// catalog
	databases  *Table[Text, DatabaseRow]
	relations  *Table[Text, RelationRow]
	attributes *Table[Text, AttributeRow]
	indexes    *Table[Text, IndexRow]

	// transactions
	transactions struct {
		sync.Mutex
		// transaction id -> transaction
		Map map[string]*Transaction
	}

	// tables
	tables struct {
		sync.Mutex
		// relation id -> table
		Map map[string]*Table[AnyKey, AnyRow]
	}

	// iterators
	iterators struct {
		sync.Mutex
		// iterator id -> iterator
		Map map[string]*Iter[AnyKey, AnyRow]
	}

	// misc
	mheap                  *mheap.Mheap
	defaultIsolationPolicy IsolationPolicy
	clock                  clock.Clock
}

type Iter[
	K Ordered[K],
	R Row[K],
] struct {
	TableIter   *TableIter[K, R]
	AttrsMap    map[string]*AttributeRow
	Expr        *plan.Expr
	FirstCalled bool
}

func NewMemHandler(
	mheap *mheap.Mheap,
	defaultIsolationPolicy IsolationPolicy,
	clock clock.Clock,
) *MemHandler {
	h := &MemHandler{
		databases:              NewTable[Text, DatabaseRow](),
		relations:              NewTable[Text, RelationRow](),
		attributes:             NewTable[Text, AttributeRow](),
		indexes:                NewTable[Text, IndexRow](),
		mheap:                  mheap,
		defaultIsolationPolicy: defaultIsolationPolicy,
		clock:                  clock,
	}
	h.transactions.Map = make(map[string]*Transaction)
	h.tables.Map = make(map[string]*Table[AnyKey, AnyRow])
	h.iterators.Map = make(map[string]*Iter[AnyKey, AnyRow])
	return h
}

var _ Handler = new(MemHandler)

func (m *MemHandler) HandleAddTableDef(meta txn.TxnMeta, req txnengine.AddTableDefReq, resp *txnengine.AddTableDefResp) error {
	tx := m.getTx(meta)

	maxAttributeOrder := 0
	if err := m.iterRelationAttributes(
		tx, req.TableID,
		func(_ Text, row *AttributeRow) error {
			if row.Order > maxAttributeOrder {
				maxAttributeOrder = row.Order
			}
			return nil
		},
	); err != nil {
		return err
	}

	switch def := req.Def.(type) {

	case *engine.CommentDef:
		// update comments
		row, err := m.relations.Get(tx, Text(req.TableID))
		if errors.Is(err, sql.ErrNoRows) {
			resp.ErrTableNotFound.ID = req.TableID
			return nil
		}
		if err != nil {
			return err
		}
		row.Comments = def.Comment
		if err := m.relations.Update(tx, *row); err != nil {
			return err
		}

	case *engine.PartitionDef:
		// update
		row, err := m.relations.Get(tx, Text(req.TableID))
		if errors.Is(err, sql.ErrNoRows) {
			resp.ErrTableNotFound.ID = req.TableID
			return nil
		}
		if err != nil {
			return err
		}
		row.PartitionDef = def.Partition
		if err := m.relations.Update(tx, *row); err != nil {
			return err
		}

	case *engine.ViewDef:
		// update
		row, err := m.relations.Get(tx, Text(req.TableID))
		if errors.Is(err, sql.ErrNoRows) {
			resp.ErrTableNotFound.ID = req.TableID
			return nil
		}
		if err != nil {
			return err
		}
		row.ViewDef = def.View
		if err := m.relations.Update(tx, *row); err != nil {
			return err
		}

	case *engine.AttributeDef:
		// add attribute
		// check existence
		keys, err := m.attributes.Index(tx, AnyKey{
			index_RelationID_Name,
			Text(req.TableID),
			Text(def.Attr.Name),
		})
		if err != nil {
			return err
		}
		if len(keys) > 0 {
			resp.ErrExisted = true
			return nil
		}
		// insert
		attrRow := AttributeRow{
			ID:         uuid.NewString(),
			RelationID: req.TableID,
			Order:      maxAttributeOrder + 1,
			Nullable:   true, //TODO fix
			Attribute:  def.Attr,
		}
		if err := m.attributes.Insert(tx, attrRow); err != nil {
			return err
		}

	case *engine.IndexTableDef:
		// add index
		// check existence
		keys, err := m.indexes.Index(tx, AnyKey{
			index_RelationID_Name,
			Text(req.TableID),
			Text(def.Name),
		})
		if err != nil {
			return err
		}
		if len(keys) > 0 {
			resp.ErrExisted = true
			return nil
		}
		// insert
		idxRow := IndexRow{
			ID:            uuid.NewString(),
			RelationID:    req.TableID,
			IndexTableDef: *def,
		}
		if err := m.indexes.Insert(tx, idxRow); err != nil {
			return err
		}

	case *engine.PropertiesDef:
		// update properties
		row, err := m.relations.Get(tx, Text(req.TableID))
		if errors.Is(err, sql.ErrNoRows) {
			resp.ErrTableNotFound.ID = req.TableID
			return nil
		}
		for _, prop := range def.Properties {
			row.Properties[prop.Key] = prop.Value
		}
		if err := m.relations.Update(tx, *row); err != nil {
			return err
		}

	case *engine.PrimaryIndexDef:
		// set primary index
		if err := m.iterRelationAttributes(
			tx, req.TableID,
			func(_ Text, row *AttributeRow) error {
				isPrimary := false
				for _, name := range def.Names {
					if name == row.Name {
						isPrimary = true
						break
					}
				}
				if isPrimary == row.Primary {
					return nil
				}
				row.Primary = isPrimary
				if err := m.attributes.Update(tx, *row); err != nil {
					return err
				}
				return nil
			},
		); err != nil {
			return err
		}

	default:
		return fmt.Errorf("unknown table def: %T", req.Def)

	}

	return nil
}

func (m *MemHandler) HandleCloseTableIter(meta txn.TxnMeta, req txnengine.CloseTableIterReq, resp *txnengine.CloseTableIterResp) error {
	m.iterators.Lock()
	defer m.iterators.Unlock()
	iter, ok := m.iterators.Map[req.IterID]
	if !ok {
		resp.ErrIterNotFound.ID = req.IterID
		return nil
	}
	delete(m.iterators.Map, req.IterID)
	if err := iter.TableIter.Close(); err != nil {
		return err
	}
	return nil
}

func (m *MemHandler) HandleCreateDatabase(meta txn.TxnMeta, req txnengine.CreateDatabaseReq, resp *txnengine.CreateDatabaseResp) error {
	tx := m.getTx(meta)

	keys, err := m.databases.Index(tx, AnyKey{
		index_AccountID_Name,
		Uint(req.AccessInfo.AccountID),
		Text(req.Name),
	})
	if err != nil {
		return err
	}
	if len(keys) > 0 {
		resp.ErrExisted = true
		return nil
	}

	id := uuid.NewString()
	err = m.databases.Insert(tx, DatabaseRow{
		ID:        id,
		NumberID:  rand.Uint64(),
		AccountID: req.AccessInfo.AccountID,
		Name:      req.Name,
	})
	if err != nil {
		return err
	}

	resp.ID = id
	return nil
}

func (m *MemHandler) HandleCreateRelation(meta txn.TxnMeta, req txnengine.CreateRelationReq, resp *txnengine.CreateRelationResp) error {
	tx := m.getTx(meta)

	// validate database id
	if req.DatabaseID != "" {
		_, err := m.databases.Get(tx, Text(req.DatabaseID))
		if errors.Is(err, sql.ErrNoRows) {
			resp.ErrDatabaseNotFound.ID = req.DatabaseID
			return nil
		}
		if err != nil {
			return err
		}
	}

	// check existence
	keys, err := m.relations.Index(tx, AnyKey{
		index_DatabaseID_Name,
		Text(req.DatabaseID),
		Text(req.Name),
	})
	if err != nil {
		return err
	}
	if len(keys) > 0 {
		resp.ErrExisted = true
		return nil
	}

	// row
	row := RelationRow{
		ID:         uuid.NewString(),
		NumberID:   rand.Uint64(),
		DatabaseID: req.DatabaseID,
		Name:       req.Name,
		Type:       req.Type,
		Properties: make(map[string]string),
	}

	// handle defs
	var relAttrs []engine.Attribute
	var relIndexes []engine.IndexTableDef
	var primaryColumnNames []string
	for _, def := range req.Defs {
		switch def := def.(type) {

		case *engine.CommentDef:
			row.Comments = def.Comment

		case *engine.PartitionDef:
			row.PartitionDef = def.Partition

		case *engine.ViewDef:
			row.ViewDef = def.View

		case *engine.AttributeDef:
			relAttrs = append(relAttrs, def.Attr)

		case *engine.IndexTableDef:
			relIndexes = append(relIndexes, *def)

		case *engine.PropertiesDef:
			for _, prop := range def.Properties {
				row.Properties[prop.Key] = prop.Value
			}

		case *engine.PrimaryIndexDef:
			primaryColumnNames = def.Names

		default:
			panic(fmt.Errorf("unknown table def: %T", def))
		}
	}

	// add row id
	relAttrs = append(relAttrs, engine.Attribute{
		IsHidden: true,
		IsRowId:  true,
		Name:     rowIDColumnName,
		Type:     types.T_Rowid.ToType(),
		Default: &plan.Default{
			NullAbility: false,
		},
	})

	// insert relation attributes
	for i, attr := range relAttrs {
		if len(primaryColumnNames) > 0 {
			isPrimary := false
			for _, name := range primaryColumnNames {
				if name == attr.Name {
					isPrimary = true
					break
				}
			}
			attr.Primary = isPrimary
		}
		attrRow := AttributeRow{
			ID:         uuid.NewString(),
			RelationID: row.ID,
			Order:      i + 1,
			Nullable:   true, //TODO fix
			Attribute:  attr,
		}
		if err := m.attributes.Insert(tx, attrRow); err != nil {
			return err
		}
	}

	// insert relation indexes
	for _, idx := range relIndexes {
		idxRow := IndexRow{
			ID:            uuid.NewString(),
			RelationID:    row.ID,
			IndexTableDef: idx,
		}
		if err := m.indexes.Insert(tx, idxRow); err != nil {
			return err
		}
	}

	// insert relation
	if err := m.relations.Insert(tx, row); err != nil {
		return err
	}

	// create table
	m.tables.Lock()
	defer m.tables.Unlock()
	m.tables.Map[row.ID] = NewTable[AnyKey, AnyRow]()

	resp.ID = row.ID
	return nil
}

const rowIDColumnName = "__rowid"

func (m *MemHandler) HandleDelTableDef(meta txn.TxnMeta, req txnengine.DelTableDefReq, resp *txnengine.DelTableDefResp) error {
	tx := m.getTx(meta)
	switch def := req.Def.(type) {

	case *engine.CommentDef:
		// del comments
		row, err := m.relations.Get(tx, Text(req.TableID))
		if errors.Is(err, sql.ErrNoRows) {
			resp.ErrTableNotFound.ID = req.TableID
			return nil
		}
		if err != nil {
			return err
		}
		row.Comments = ""
		if err := m.relations.Update(tx, *row); err != nil {
			return err
		}

	case *engine.AttributeDef:
		// delete attribute
		keys, err := m.attributes.Index(tx, AnyKey{
			index_RelationID_Name,
			Text(req.TableID),
			Text(def.Attr.Name),
		})
		if err != nil {
			return err
		}
		for _, key := range keys {
			if err := m.attributes.Delete(tx, key); err != nil {
				return err
			}
		}

	case *engine.IndexTableDef:
		// delete index
		keys, err := m.indexes.Index(tx, AnyKey{
			index_RelationID_Name,
			Text(req.TableID),
			Text(def.Name),
		})
		if err != nil {
			return err
		}
		for _, key := range keys {
			if err := m.indexes.Delete(tx, key); err != nil {
				return err
			}
		}

	case *engine.PropertiesDef:
		// delete properties
		row, err := m.relations.Get(tx, Text(req.TableID))
		if errors.Is(err, sql.ErrNoRows) {
			resp.ErrTableNotFound.ID = req.TableID
			return nil
		}
		for _, prop := range def.Properties {
			delete(row.Properties, prop.Key)
		}
		if err := m.relations.Update(tx, *row); err != nil {
			return err
		}

	case *engine.PrimaryIndexDef:
		// delete primary index
		if err := m.iterRelationAttributes(
			tx, req.TableID,
			func(key Text, row *AttributeRow) error {
				if !row.Primary {
					return nil
				}
				row.Primary = false
				if err := m.attributes.Update(tx, *row); err != nil {
					return err
				}
				return nil
			},
		); err != nil {
			return err
		}

	default:
		return fmt.Errorf("unknown table def: %T", req.Def)

	}

	return nil
}

func (m *MemHandler) HandleDelete(meta txn.TxnMeta, req txnengine.DeleteReq, resp *txnengine.DeleteResp) error {
	m.tables.Lock()
	defer m.tables.Unlock()

	table, ok := m.tables.Map[req.TableID]
	if !ok {
		// the caller expects no error if table not exist
		//resp.ErrTableNotFound.ID = req.TableID
		return nil
	}

	tx := m.getTx(meta)
	reqVecLen := req.Vector.Length()

	// by row id
	if req.ColumnName == rowIDColumnName {
		for i := 0; i < reqVecLen; i++ {
			value := vectorAt(req.Vector, i)
			rowID := value.Value.(types.Rowid)
			keys, err := table.Index(tx, AnyKey{
				index_RowID, typeConv(rowID),
			})
			if err != nil {
				return err
			}
			if len(keys) == 0 {
				continue
			}
			if len(keys) != 1 {
				panic("impossible")
			}
			if err := table.Delete(tx, keys[0]); err != nil {
				return err
			}
		}
		return nil
	}

	// by primary keys
	rows, err := m.attributes.IndexRows(tx, AnyKey{
		index_RelationID_IsPrimary,
		Text(req.TableID),
		Bool(true),
	})
	if err != nil {
		return err
	}
	if len(rows) == 1 && rows[0].Name == req.ColumnName {
		// by primary key
		for i := 0; i < reqVecLen; i++ {
			value := vectorAt(req.Vector, i)
			primaryKey := AnyKey{typeConv(value.Value)}
			if err := table.Delete(tx, primaryKey); err != nil {
				return err
			}
		}
		return nil
	}

	// by non-primary key, slow but works
	rows, err = m.attributes.IndexRows(tx, AnyKey{
		index_RelationID_Name,
		Text(req.TableID),
		Text(req.ColumnName),
	})
	if err != nil {
		return err
	}
	if len(rows) != 1 {
		resp.ErrColumnNotFound.Name = req.ColumnName
		return nil
	}
	attrID := rows[0].ID
	iter := table.NewIter(tx)
	defer iter.Close()
	for ok := iter.First(); ok; ok = iter.Next() {
		key, row, err := iter.Read()
		if err != nil {
			return err
		}
		for i := 0; i < reqVecLen; i++ {
			value := vectorAt(req.Vector, i)
			attrInRow, ok := (*row).attributes[attrID]
			if !ok {
				// attr not in row
				continue
			}
			if value.Equal(attrInRow) {
				if err := table.Delete(tx, key); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func (m *MemHandler) HandleDeleteDatabase(meta txn.TxnMeta, req txnengine.DeleteDatabaseReq, resp *txnengine.DeleteDatabaseResp) error {
	tx := m.getTx(meta)

	rows, err := m.databases.IndexRows(tx, AnyKey{
		index_AccountID_Name,
		Uint(req.AccessInfo.AccountID),
		Text(req.Name),
	})
	if err != nil {
		return err
	}
	if len(rows) == 0 {
		resp.ErrNotFound.Name = req.Name
		return nil
	}

	for _, row := range rows {
		if err := m.databases.Delete(tx, row.PrimaryKey()); err != nil {
			return err
		}
		if err := m.deleteRelationsByDBID(tx, row.ID); err != nil {
			return err
		}
		resp.ID = row.ID
	}

	return nil
}

func (m *MemHandler) deleteRelationsByDBID(tx *Transaction, dbID string) error {
	rows, err := m.relations.IndexRows(tx, AnyKey{
		index_DatabaseID,
		Text(dbID),
	})
	if err != nil {
		return err
	}
	for _, row := range rows {
		if err := m.relations.Delete(tx, row.PrimaryKey()); err != nil {
			return err
		}
		if err := m.deleteAttributesByRelationID(tx, row.ID); err != nil {
			return err
		}
	}
	return nil
}

func (m *MemHandler) deleteAttributesByRelationID(tx *Transaction, relationID string) error {
	keys, err := m.attributes.Index(tx, AnyKey{
		index_RelationID,
		Text(relationID),
	})
	if err != nil {
		return err
	}
	for _, key := range keys {
		if err := m.attributes.Delete(tx, key); err != nil {
			return err
		}
	}
	return nil
}

func (m *MemHandler) HandleDeleteRelation(meta txn.TxnMeta, req txnengine.DeleteRelationReq, resp *txnengine.DeleteRelationResp) error {
	tx := m.getTx(meta)
	rows, err := m.relations.IndexRows(tx, AnyKey{
		index_DatabaseID_Name,
		Text(req.DatabaseID),
		Text(req.Name),
	})
	if err != nil {
		return err
	}
	if len(rows) == 0 {
		// the caller expects no error if table not exist
		//resp.ErrNotFound.Name = req.Name
		return nil
	}
	if len(rows) != 1 {
		panic("impossible")
	}
	row := rows[0]
	if err := m.relations.Delete(tx, row.PrimaryKey()); err != nil {
		return err
	}
	if err := m.deleteAttributesByRelationID(tx, row.ID); err != nil {
		return err
	}
	resp.ID = row.ID
	return nil
}

func (m *MemHandler) HandleGetDatabases(meta txn.TxnMeta, req txnengine.GetDatabasesReq, resp *txnengine.GetDatabasesResp) error {
	tx := m.getTx(meta)

	rows, err := m.databases.IndexRows(tx, AnyKey{
		index_AccountID,
		Uint(req.AccessInfo.AccountID),
	})
	if err != nil {
		return err
	}

	for _, row := range rows {
		resp.Names = append(resp.Names, row.Name)
	}

	return nil
}

func (m *MemHandler) HandleGetPrimaryKeys(meta txn.TxnMeta, req txnengine.GetPrimaryKeysReq, resp *txnengine.GetPrimaryKeysResp) error {
	tx := m.getTx(meta)
	rows, err := m.attributes.IndexRows(tx, AnyKey{
		index_RelationID_IsPrimary,
		Text(req.TableID),
		Bool(true),
	})
	if err != nil {
		return err
	}
	for _, row := range rows {
		resp.Attrs = append(resp.Attrs, &row.Attribute)
	}
	return nil
}

func (m *MemHandler) HandleGetRelations(meta txn.TxnMeta, req txnengine.GetRelationsReq, resp *txnengine.GetRelationsResp) error {
	tx := m.getTx(meta)
	rows, err := m.relations.IndexRows(tx, AnyKey{
		index_DatabaseID,
		Text(req.DatabaseID),
	})
	if err != nil {
		return err
	}
	for _, row := range rows {
		resp.Names = append(resp.Names, row.Name)
	}
	return nil
}

func (m *MemHandler) HandleGetTableDefs(meta txn.TxnMeta, req txnengine.GetTableDefsReq, resp *txnengine.GetTableDefsResp) error {
	tx := m.getTx(meta)

	relRow, err := m.relations.Get(tx, Text(req.TableID))
	if errors.Is(err, sql.ErrNoRows) {
		// the caller expects no error if table not exist
		//resp.ErrTableNotFound.ID = req.TableID
		return nil
	}
	if err != nil {
		return err
	}

	// comments
	if relRow.Comments != "" {
		resp.Defs = append(resp.Defs, &engine.CommentDef{
			Comment: relRow.Comments,
		})
	}

	// partiton
	if relRow.PartitionDef != "" {
		resp.Defs = append(resp.Defs, &engine.PartitionDef{
			Partition: relRow.PartitionDef,
		})
	}

	// view
	if relRow.ViewDef != "" {
		resp.Defs = append(resp.Defs, &engine.ViewDef{
			View: relRow.ViewDef,
		})
	}

	// attributes and primary index
	{
		var primaryAttrNames []string
		var attrRows []*AttributeRow
		if err := m.iterRelationAttributes(
			tx, req.TableID,
			func(key Text, row *AttributeRow) error {
				if row.IsHidden {
					return nil
				}
				attrRows = append(attrRows, row)
				if row.Primary {
					primaryAttrNames = append(primaryAttrNames, row.Name)
				}
				return nil
			},
		); err != nil {
			return err
		}

		if len(primaryAttrNames) > 0 {
			resp.Defs = append(resp.Defs, &engine.PrimaryIndexDef{
				Names: primaryAttrNames,
			})
		}
		sort.Slice(attrRows, func(i, j int) bool {
			return attrRows[i].Order < attrRows[j].Order
		})
		for _, row := range attrRows {
			resp.Defs = append(resp.Defs, &engine.AttributeDef{
				Attr: row.Attribute,
			})

		}
	}

	// indexes
	{
		rows, err := m.indexes.IndexRows(tx, AnyKey{
			index_RelationID, Text(req.TableID),
		})
		if err != nil {
			return err
		}
		for _, row := range rows {
			resp.Defs = append(resp.Defs, &row.IndexTableDef)
		}
	}

	// properties
	if len(relRow.Properties) > 0 {
		propertiesDef := new(engine.PropertiesDef)
		for key, value := range relRow.Properties {
			propertiesDef.Properties = append(propertiesDef.Properties, engine.Property{
				Key:   key,
				Value: value,
			})
		}
		resp.Defs = append(resp.Defs, propertiesDef)
	}

	return nil
}

func (m *MemHandler) HandleGetHiddenKeys(meta txn.TxnMeta, req txnengine.GetHiddenKeysReq, resp *txnengine.GetHiddenKeysResp) error {
	tx := m.getTx(meta)
	rows, err := m.attributes.IndexRows(tx, AnyKey{
		index_RelationID_IsHidden,
		Text(req.TableID),
		Bool(true),
	})
	if err != nil {
		return err
	}
	for _, row := range rows {
		resp.Attrs = append(resp.Attrs, &row.Attribute)
	}
	return nil
}

func (m *MemHandler) HandleNewTableIter(meta txn.TxnMeta, req txnengine.NewTableIterReq, resp *txnengine.NewTableIterResp) error {
	tx := m.getTx(meta)

	m.tables.Lock()
	defer m.tables.Unlock()
	table, ok := m.tables.Map[req.TableID]
	if !ok {
		resp.ErrTableNotFound.ID = req.TableID
		return nil
	}

	tableIter := table.NewIter(tx)
	attrsMap := make(map[string]*AttributeRow)
	if err := m.iterRelationAttributes(
		tx, req.TableID,
		func(_ Text, row *AttributeRow) error {
			attrsMap[row.Name] = row
			return nil
		},
	); err != nil {
		return err
	}

	iter := &Iter[AnyKey, AnyRow]{
		TableIter: tableIter,
		AttrsMap:  attrsMap,
		Expr:      req.Expr,
	}

	m.iterators.Lock()
	defer m.iterators.Unlock()
	id := uuid.NewString()
	resp.IterID = id
	m.iterators.Map[id] = iter

	return nil
}

func (m *MemHandler) HandleOpenDatabase(meta txn.TxnMeta, req txnengine.OpenDatabaseReq, resp *txnengine.OpenDatabaseResp) error {
	tx := m.getTx(meta)

	rows, err := m.databases.IndexRows(tx, AnyKey{
		index_AccountID_Name,
		Uint(req.AccessInfo.AccountID),
		Text(req.Name),
	})
	if err != nil {
		return err
	}

	for _, row := range rows {
		resp.ID = row.ID
		return nil
	}

	resp.ErrNotFound.Name = req.Name
	return nil
}

func (m *MemHandler) HandleOpenRelation(meta txn.TxnMeta, req txnengine.OpenRelationReq, resp *txnengine.OpenRelationResp) error {
	tx := m.getTx(meta)
	rows, err := m.relations.IndexRows(tx, AnyKey{
		index_DatabaseID_Name,
		Text(req.DatabaseID),
		Text(req.Name),
	})
	if err != nil {
		return err
	}
	for _, row := range rows {
		resp.ID = row.ID
		resp.Type = row.Type
		return nil
	}
	resp.ErrNotFound.Name = req.Name
	return nil
}

func (m *MemHandler) HandleRead(meta txn.TxnMeta, req txnengine.ReadReq, resp *txnengine.ReadResp) error {
	m.iterators.Lock()
	iter, ok := m.iterators.Map[req.IterID]
	if !ok {
		m.iterators.Unlock()
		resp.ErrIterNotFound.ID = req.IterID
		return nil
	}
	m.iterators.Unlock()

	b := batch.New(false, req.ColNames)

	for i, name := range req.ColNames {
		b.Vecs[i] = vector.New(iter.AttrsMap[name].Type)
	}

	fn := iter.TableIter.First
	if iter.FirstCalled {
		fn = iter.TableIter.Next
	} else {
		iter.FirstCalled = true
	}

	maxRows := 4096
	type Row struct {
		Value       *AnyRow
		PhysicalRow *PhysicalRow[AnyKey, AnyRow]
	}
	var rows []Row

	for ok := fn(); ok; ok = iter.TableIter.Next() {
		item := iter.TableIter.Item()
		row, err := item.Values.Read(iter.TableIter.readTime, iter.TableIter.tx)
		if err != nil {
			return err
		}

		//TODO handle iter.Expr
		if iter.Expr != nil {
			panic(iter.Expr)
		}

		rows = append(rows, Row{
			Value:       row,
			PhysicalRow: item,
		})
		if len(rows) >= maxRows {
			break
		}
	}

	// sort to emulate TAE behavior TODO remove this after BVT fixes
	sort.Slice(rows, func(i, j int) bool {
		return rows[i].PhysicalRow.LastUpdate.Load().Before(
			rows[j].PhysicalRow.LastUpdate.Load(),
		)
	})

	tx := m.getTx(meta)
	for _, row := range rows {
		namedRow := &NamedAnyRow{
			Row:      row.Value,
			AttrsMap: iter.AttrsMap,
		}
		if err := appendNamedRow(tx, m.mheap, b, namedRow); err != nil {
			return err
		}
	}

	if len(rows) > 0 {
		b.InitZsOne(len(rows))
		for _, vec := range b.Vecs {
			nulls.TryExpand(vec.GetNulls(), len(rows))
		}
		resp.Batch = b
	}

	return nil
}

func (m *MemHandler) HandleTruncate(meta txn.TxnMeta, req txnengine.TruncateReq, resp *txnengine.TruncateResp) error {
	tx := m.getTx(meta)
	_, err := m.relations.Get(tx, Text(req.TableID))
	if errors.Is(err, sql.ErrNoRows) {
		resp.ErrTableNotFound.ID = req.TableID
		return nil
	}
	m.tables.Lock()
	defer m.tables.Unlock()
	m.tables.Map[req.TableID] = NewTable[AnyKey, AnyRow]()
	return nil
}

func (m *MemHandler) HandleUpdate(meta txn.TxnMeta, req txnengine.UpdateReq, resp *txnengine.UpdateResp) error {
	tx := m.getTx(meta)

	if err := m.rangeBatchPhysicalRows(
		tx,
		req.TableID,
		req.Batch,
		&resp.ErrTableNotFound,
		func(
			table *Table[AnyKey, AnyRow],
			row *AnyRow,
			rowID types.Rowid,
		) error {
			if err := table.Update(tx, *row); err != nil {
				return err
			}
			return nil
		},
	); err != nil {
		return err
	}

	return nil
}

func (m *MemHandler) HandleWrite(meta txn.TxnMeta, req txnengine.WriteReq, resp *txnengine.WriteResp) error {
	tx := m.getTx(meta)

	if err := m.rangeBatchPhysicalRows(
		tx,
		req.TableID,
		req.Batch,
		&resp.ErrTableNotFound,
		func(
			table *Table[AnyKey, AnyRow],
			row *AnyRow,
			rowID types.Rowid,
		) error {
			if err := table.Insert(tx, *row); err != nil {
				return err
			}
			return nil
		},
	); err != nil {
		return err
	}

	return nil
}

func (m *MemHandler) rangeBatchPhysicalRows(
	tx *Transaction,
	tableID string,
	b *batch.Batch,
	errTableNotFound *txnengine.ErrRelationNotFound,
	fn func(
		*Table[AnyKey, AnyRow],
		*AnyRow,
		types.Rowid,
	) error,
) error {

	// load attributes
	nameToAttrs := make(map[string]*AttributeRow)
	if err := m.iterRelationAttributes(
		tx, tableID,
		func(_ Text, row *AttributeRow) error {
			nameToAttrs[row.Name] = row
			return nil
		},
	); err != nil {
		return err
	}

	if len(nameToAttrs) == 0 {
		errTableNotFound.ID = tableID
		return nil
	}

	// write
	m.tables.Lock()
	table, ok := m.tables.Map[tableID]
	m.tables.Unlock()
	if !ok {
		panic("impossible")
	}
	batchIter := NewBatchIter(b)
	for {
		row := batchIter()
		if len(row) == 0 {
			break
		}

		rowID := newRowID()
		physicalRow := NewAnyRow([]AnyKey{
			{index_RowID, typeConv(rowID)},
		})
		physicalRow.attributes[nameToAttrs[rowIDColumnName].ID] = Nullable{
			Value: rowID,
		}

		for i, col := range row {
			name := b.Attrs[i]

			attr, ok := nameToAttrs[name]
			if !ok {
				return fmt.Errorf("unknown attr: %s", name)
			}

			if attr.Primary {
				physicalRow.primaryKey = append(physicalRow.primaryKey, typeConv(col.Value))
			}

			physicalRow.attributes[attr.ID] = col
		}

		// use row id as primary key if no primary key is provided
		if len(physicalRow.primaryKey) == 0 {
			physicalRow.primaryKey = append(physicalRow.primaryKey, typeConv(rowID))
		}

		if err := fn(table, physicalRow, rowID); err != nil {
			return err
		}

	}

	return nil
}

func (m *MemHandler) getTx(meta txn.TxnMeta) *Transaction {
	id := string(meta.ID)
	m.transactions.Lock()
	defer m.transactions.Unlock()
	tx, ok := m.transactions.Map[id]
	if !ok {
		tx = NewTransaction(
			id,
			Time{
				Timestamp: meta.SnapshotTS,
			},
			m.defaultIsolationPolicy,
		)
		m.transactions.Map[id] = tx
	}
	return tx
}

func (*MemHandler) HandleClose() error {
	return nil
}

func (m *MemHandler) HandleCommit(meta txn.TxnMeta) error {
	tx := m.getTx(meta)
	if err := tx.Commit(); err != nil {
		return err
	}
	return nil
}

func (m *MemHandler) HandleCommitting(meta txn.TxnMeta) error {
	return nil
}

func (m *MemHandler) HandleDestroy() error {
	*m = *NewMemHandler(m.mheap, m.defaultIsolationPolicy, m.clock)
	return nil
}

func (m *MemHandler) HandlePrepare(meta txn.TxnMeta) (timestamp.Timestamp, error) {
	now, _ := m.clock.Now()
	return now, nil
}

func (m *MemHandler) HandleRollback(meta txn.TxnMeta) error {
	tx := m.getTx(meta)
	tx.Abort()
	return nil
}

func (m *MemHandler) HandleStartRecovery(ch chan txn.TxnMeta) {
	// no recovery
	close(ch)
}

func (m *MemHandler) iterRelationAttributes(
	tx *Transaction,
	relationID string,
	fn func(key Text, row *AttributeRow) error,
) error {
	rows, err := m.attributes.IndexRows(tx, AnyKey{
		index_RelationID,
		Text(relationID),
	})
	if err != nil {
		return err
	}
	for _, row := range rows {
		if err := fn(row.PrimaryKey(), row); err != nil {
			return err
		}
	}
	return nil
}

func (m *MemHandler) HandleTableStats(meta txn.TxnMeta, req txnengine.TableStatsReq, resp *txnengine.TableStatsResp) (err error) {
	tx := m.getTx(meta)

	m.tables.Lock()
	table, ok := m.tables.Map[req.TableID]
	if !ok {
		m.tables.Unlock()
		resp.ErrTableNotFound.ID = req.TableID
		return nil
	}
	m.tables.Unlock()

	// maybe an estimation is enough
	iter := table.NewIter(tx)
	defer iter.Close()
	n := 0
	for ok := iter.First(); ok; ok = iter.Next() {
		_, _, err := iter.Read()
		if err != nil {
			return err
		}
		n++
	}
	resp.Rows = n

	return nil
}
