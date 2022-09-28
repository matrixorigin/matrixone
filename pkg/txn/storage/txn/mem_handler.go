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
	crand "crypto/rand"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/txn/storage/txn/memtable"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	txnengine "github.com/matrixorigin/matrixone/pkg/vm/engine/txn"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
)

type MemHandler struct {

	// catalog
	databases  *memtable.Table[ID, *DatabaseRow, *DatabaseRow]
	relations  *memtable.Table[ID, *RelationRow, *RelationRow]
	attributes *memtable.Table[ID, *AttributeRow, *AttributeRow]
	indexes    *memtable.Table[ID, *IndexRow, *IndexRow]

	// data
	data *memtable.Table[DataKey, DataValue, DataRow]

	// transactions
	transactions struct {
		sync.Mutex
		// transaction id -> transaction
		Map map[string]*Transaction
	}

	// iterators
	iterators struct {
		sync.Mutex
		// iterator id -> iterator
		Map map[ID]*Iter[DataKey, DataValue]
	}

	// misc
	mheap                  *mheap.Mheap
	defaultIsolationPolicy IsolationPolicy
	clock                  clock.Clock
}

type Iter[
	K memtable.Ordered[K],
	V any,
] struct {
	TableIter *memtable.TableIter[K, V]
	TableID   ID
	AttrsMap  map[string]*AttributeRow
	Expr      *plan.Expr
	nextFunc  func() bool
	ReadTime  Time
	Tx        *Transaction
}

func NewMemHandler(
	mheap *mheap.Mheap,
	defaultIsolationPolicy IsolationPolicy,
	clock clock.Clock,
) *MemHandler {
	h := &MemHandler{
		databases:              memtable.NewTable[ID, *DatabaseRow, *DatabaseRow](),
		relations:              memtable.NewTable[ID, *RelationRow, *RelationRow](),
		attributes:             memtable.NewTable[ID, *AttributeRow, *AttributeRow](),
		indexes:                memtable.NewTable[ID, *IndexRow, *IndexRow](),
		data:                   memtable.NewTable[DataKey, DataValue, DataRow](),
		mheap:                  mheap,
		defaultIsolationPolicy: defaultIsolationPolicy,
		clock:                  clock,
	}
	h.transactions.Map = make(map[string]*Transaction)
	h.iterators.Map = make(map[ID]*Iter[DataKey, DataValue])
	return h
}

var _ Handler = new(MemHandler)

func (m *MemHandler) HandleAddTableDef(meta txn.TxnMeta, req txnengine.AddTableDefReq, resp *txnengine.AddTableDefResp) error {
	tx := m.getTx(meta)

	maxAttributeOrder := 0
	if err := m.iterRelationAttributes(
		tx, req.TableID,
		func(_ ID, row *AttributeRow) error {
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
		row, err := m.relations.Get(tx, req.TableID)
		if errors.Is(err, sql.ErrNoRows) {
			return moerr.NewNoSuchTable(req.DatabaseName, req.TableName)
		}
		if err != nil {
			return err
		}
		row.Comments = def.Comment
		if err := m.relations.Update(tx, row); err != nil {
			return err
		}

	case *engine.PartitionDef:
		// update
		row, err := m.relations.Get(tx, req.TableID)
		if errors.Is(err, sql.ErrNoRows) {
			return moerr.NewNoSuchTable(req.DatabaseName, req.TableName)
		}
		if err != nil {
			return err
		}
		row.PartitionDef = def.Partition
		if err := m.relations.Update(tx, row); err != nil {
			return err
		}

	case *engine.ViewDef:
		// update
		row, err := m.relations.Get(tx, req.TableID)
		if errors.Is(err, sql.ErrNoRows) {
			return moerr.NewNoSuchTable(req.DatabaseName, req.TableName)
		}
		if err != nil {
			return err
		}
		row.ViewDef = def.View
		if err := m.relations.Update(tx, row); err != nil {
			return err
		}

	case *engine.AttributeDef:
		// add attribute
		// check existence
		entries, err := m.attributes.Index(tx, Tuple{
			index_RelationID_Name,
			req.TableID,
			Text(def.Attr.Name),
		})
		if err != nil {
			return err
		}
		if len(entries) > 0 {
			return moerr.NewDuplicate()
		}
		// insert
		attrRow := &AttributeRow{
			ID:         txnengine.NewID(),
			RelationID: req.TableID,
			Order:      maxAttributeOrder + 1,
			Nullable:   def.Attr.Default != nil && def.Attr.Default.NullAbility,
			Attribute:  def.Attr,
		}
		if err := m.attributes.Insert(tx, attrRow); err != nil {
			return err
		}

	case *engine.IndexTableDef:
		// add index
		// check existence
		entries, err := m.indexes.Index(tx, Tuple{
			index_RelationID_Name,
			req.TableID,
			Text(def.Name),
		})
		if err != nil {
			return err
		}
		if len(entries) > 0 {
			return moerr.NewDuplicate()
		}
		// insert
		idxRow := &IndexRow{
			ID:            txnengine.NewID(),
			RelationID:    req.TableID,
			IndexTableDef: *def,
		}
		if err := m.indexes.Insert(tx, idxRow); err != nil {
			return err
		}

	case *engine.PropertiesDef:
		// update properties
		row, err := m.relations.Get(tx, req.TableID)
		if errors.Is(err, sql.ErrNoRows) {
			return moerr.NewNoSuchTable(req.DatabaseName, req.TableName)
		}
		for _, prop := range def.Properties {
			row.Properties[prop.Key] = prop.Value
		}
		if err := m.relations.Update(tx, row); err != nil {
			return err
		}

	case *engine.PrimaryIndexDef:
		// set primary index
		if err := m.iterRelationAttributes(
			tx, req.TableID,
			func(_ ID, row *AttributeRow) error {
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
				if err := m.attributes.Update(tx, row); err != nil {
					return err
				}
				return nil
			},
		); err != nil {
			return err
		}

	default:
		panic(fmt.Sprintf("unknown table def: %T", req.Def))

	}

	return nil
}

func (m *MemHandler) HandleCloseTableIter(meta txn.TxnMeta, req txnengine.CloseTableIterReq, resp *txnengine.CloseTableIterResp) error {
	m.iterators.Lock()
	defer m.iterators.Unlock()
	iter, ok := m.iterators.Map[req.IterID]
	if !ok {
		return moerr.NewInternalError("no such iter: %v", req.IterID)
	}
	delete(m.iterators.Map, req.IterID)
	if err := iter.TableIter.Close(); err != nil {
		return err
	}
	return nil
}

func (m *MemHandler) HandleCreateDatabase(meta txn.TxnMeta, req txnengine.CreateDatabaseReq, resp *txnengine.CreateDatabaseResp) error {
	tx := m.getTx(meta)

	entries, err := m.databases.Index(tx, Tuple{
		index_AccountID_Name,
		Uint(req.AccessInfo.AccountID),
		Text(req.Name),
	})
	if err != nil {
		return err
	}
	if len(entries) > 0 {
		return moerr.NewDBAlreadyExists(req.Name)
	}

	id := txnengine.NewID()
	err = m.databases.Insert(tx, &DatabaseRow{
		ID:        id,
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
	if !req.DatabaseID.IsEmpty() {
		_, err := m.databases.Get(tx, req.DatabaseID)
		if errors.Is(err, sql.ErrNoRows) {
			return moerr.NewNoDB()
		}
		if err != nil {
			return err
		}
	}

	// check existence
	entries, err := m.relations.Index(tx, Tuple{
		index_DatabaseID_Name,
		req.DatabaseID,
		Text(req.Name),
	})
	if err != nil {
		return err
	}
	if len(entries) > 0 {
		return moerr.NewTableAlreadyExists(req.Name)
	}

	// row
	row := &RelationRow{
		ID:         txnengine.NewID(),
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
			panic(fmt.Sprintf("unknown table def: %T", def))
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
		attrRow := &AttributeRow{
			ID:         txnengine.NewID(),
			RelationID: row.ID,
			Order:      i + 1,
			Nullable:   attr.Default != nil && attr.Default.NullAbility,
			Attribute:  attr,
		}
		if err := m.attributes.Insert(tx, attrRow); err != nil {
			return err
		}
	}

	// insert relation indexes
	for _, idx := range relIndexes {
		idxRow := &IndexRow{
			ID:            txnengine.NewID(),
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

	resp.ID = row.ID
	return nil
}

const rowIDColumnName = "__rowid"

func (m *MemHandler) HandleDelTableDef(meta txn.TxnMeta, req txnengine.DelTableDefReq, resp *txnengine.DelTableDefResp) error {
	tx := m.getTx(meta)
	switch def := req.Def.(type) {

	case *engine.CommentDef:
		// del comments
		row, err := m.relations.Get(tx, req.TableID)
		if errors.Is(err, sql.ErrNoRows) {
			return moerr.NewNoSuchTable(req.DatabaseName, req.TableName)
		}
		if err != nil {
			return err
		}
		row.Comments = ""
		if err := m.relations.Update(tx, row); err != nil {
			return err
		}

	case *engine.AttributeDef:
		// delete attribute
		entries, err := m.attributes.Index(tx, Tuple{
			index_RelationID_Name,
			req.TableID,
			Text(def.Attr.Name),
		})
		if err != nil {
			return err
		}
		for _, entry := range entries {
			if err := m.attributes.Delete(tx, entry.Key); err != nil {
				return err
			}
			//TODO update DataValue
		}

	case *engine.IndexTableDef:
		// delete index
		entries, err := m.indexes.Index(tx, Tuple{
			index_RelationID_Name,
			req.TableID,
			Text(def.Name),
		})
		if err != nil {
			return err
		}
		for _, entry := range entries {
			if err := m.indexes.Delete(tx, entry.Key); err != nil {
				return err
			}
		}

	case *engine.PropertiesDef:
		// delete properties
		row, err := m.relations.Get(tx, req.TableID)
		if errors.Is(err, sql.ErrNoRows) {
			return moerr.NewNoSuchTable(req.DatabaseName, req.TableName)
		}
		for _, prop := range def.Properties {
			delete(row.Properties, prop.Key)
		}
		if err := m.relations.Update(tx, row); err != nil {
			return err
		}

	case *engine.PrimaryIndexDef:
		// delete primary index
		if err := m.iterRelationAttributes(
			tx, req.TableID,
			func(key ID, row *AttributeRow) error {
				if !row.Primary {
					return nil
				}
				row.Primary = false
				if err := m.attributes.Update(tx, row); err != nil {
					return err
				}
				return nil
			},
		); err != nil {
			return err
		}

	default:
		panic(fmt.Sprintf("unknown table def: %T", req.Def))

	}

	return nil
}

func (m *MemHandler) HandleDelete(meta txn.TxnMeta, req txnengine.DeleteReq, resp *txnengine.DeleteResp) error {
	tx := m.getTx(meta)
	reqVecLen := req.Vector.Length()

	// by row id
	if req.ColumnName == rowIDColumnName {
		for i := 0; i < reqVecLen; i++ {
			value := vectorAt(req.Vector, i)
			rowID := value.Value.(types.Rowid)
			entries, err := m.data.Index(tx, Tuple{
				index_RowID, memtable.ToOrdered(rowID),
			})
			if err != nil {
				return err
			}
			if len(entries) == 0 {
				continue
			}
			if len(entries) != 1 {
				panic("impossible")
			}
			if err := m.data.Delete(tx, entries[0].Key); err != nil {
				return err
			}
		}
		return nil
	}

	// by primary keys
	entries, err := m.attributes.Index(tx, Tuple{
		index_RelationID_IsPrimary,
		req.TableID,
		Bool(true),
	})
	if err != nil {
		return err
	}
	if len(entries) == 1 && entries[0].Value.Name == req.ColumnName {
		// by primary key
		for i := 0; i < reqVecLen; i++ {
			value := vectorAt(req.Vector, i)
			key := DataKey{
				tableID:    req.TableID,
				primaryKey: Tuple{memtable.ToOrdered(value.Value)},
			}
			if err := m.data.Delete(tx, key); err != nil {
				return err
			}
		}
		return nil
	}

	// by non-primary key, slow but works
	entries, err = m.attributes.Index(tx, Tuple{
		index_RelationID_Name,
		req.TableID,
		Text(req.ColumnName),
	})
	if err != nil {
		return err
	}
	if len(entries) != 1 {
		return moerr.NewInternalError("wrong column name: %s", req.ColumnName)
	}
	attrIndex := entries[0].Value.Order
	iter := m.data.NewIter(tx)
	defer iter.Close()
	tableKey := DataKey{
		tableID: req.TableID,
	}
	for ok := iter.Seek(tableKey); ok; ok = iter.Next() {
		key, dataValue, err := iter.Read()
		if err != nil {
			return err
		}
		if key.tableID != req.TableID {
			break
		}
		for i := 0; i < reqVecLen; i++ {
			value := vectorAt(req.Vector, i)
			if attrIndex >= len(dataValue) {
				// attr not in row
				continue
			}
			attrInRow := dataValue[attrIndex]
			if value.Equal(attrInRow) {
				if err := m.data.Delete(tx, key); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func (m *MemHandler) HandleDeleteDatabase(meta txn.TxnMeta, req txnengine.DeleteDatabaseReq, resp *txnengine.DeleteDatabaseResp) error {
	tx := m.getTx(meta)

	entries, err := m.databases.Index(tx, Tuple{
		index_AccountID_Name,
		Uint(req.AccessInfo.AccountID),
		Text(req.Name),
	})
	if err != nil {
		return err
	}
	if len(entries) == 0 {
		return moerr.NewNoDB()
	}

	for _, entry := range entries {
		if err := m.databases.Delete(tx, entry.Key); err != nil {
			return err
		}
		if err := m.deleteRelationsByDBID(tx, entry.Value.ID); err != nil {
			return err
		}
		resp.ID = entry.Value.ID
	}

	return nil
}

func (m *MemHandler) deleteRelationsByDBID(tx *Transaction, dbID ID) error {
	entries, err := m.relations.Index(tx, Tuple{
		index_DatabaseID,
		dbID,
	})
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if err := m.relations.Delete(tx, entry.Key); err != nil {
			return err
		}
		if err := m.deleteAttributesByRelationID(tx, entry.Value.ID); err != nil {
			return err
		}
		if err := m.deleteRelationData(tx, entry.Value.ID); err != nil {
			return err
		}
	}
	return nil
}

func (m *MemHandler) deleteAttributesByRelationID(tx *Transaction, relationID ID) error {
	entries, err := m.attributes.Index(tx, Tuple{
		index_RelationID,
		relationID,
	})
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if err := m.attributes.Delete(tx, entry.Key); err != nil {
			return err
		}
	}
	return nil
}

func (m *MemHandler) deleteRelationData(tx *Transaction, relationID ID) error {
	iter := m.data.NewIter(tx)
	defer iter.Close()
	tableKey := DataKey{
		tableID: relationID,
	}
	for ok := iter.Seek(tableKey); ok; ok = iter.Next() {
		key, _, err := iter.Read()
		if err != nil {
			return err
		}
		if key.tableID != relationID {
			break
		}
		if err := m.data.Delete(tx, key); err != nil {
			return err
		}
	}
	return nil
}

func (m *MemHandler) HandleDeleteRelation(meta txn.TxnMeta, req txnengine.DeleteRelationReq, resp *txnengine.DeleteRelationResp) error {
	tx := m.getTx(meta)
	entries, err := m.relations.Index(tx, Tuple{
		index_DatabaseID_Name,
		req.DatabaseID,
		Text(req.Name),
	})
	if err != nil {
		return err
	}
	if len(entries) == 0 {
		// the caller expects no error if table not exist
		//resp.ErrNotFound.Name = req.Name
		return nil
	}
	if len(entries) != 1 {
		panic("impossible")
	}
	entry := entries[0]
	if err := m.relations.Delete(tx, entry.Key); err != nil {
		return err
	}
	if err := m.deleteAttributesByRelationID(tx, entry.Value.ID); err != nil {
		return err
	}
	resp.ID = entry.Value.ID
	return nil
}

func (m *MemHandler) HandleGetDatabases(meta txn.TxnMeta, req txnengine.GetDatabasesReq, resp *txnengine.GetDatabasesResp) error {
	tx := m.getTx(meta)

	entries, err := m.databases.Index(tx, Tuple{
		index_AccountID,
		Uint(req.AccessInfo.AccountID),
	})
	if err != nil {
		return err
	}

	for _, entry := range entries {
		resp.Names = append(resp.Names, entry.Value.Name)
	}

	return nil
}

func (m *MemHandler) HandleGetPrimaryKeys(meta txn.TxnMeta, req txnengine.GetPrimaryKeysReq, resp *txnengine.GetPrimaryKeysResp) error {
	tx := m.getTx(meta)
	entries, err := m.attributes.Index(tx, Tuple{
		index_RelationID_IsPrimary,
		req.TableID,
		Bool(true),
	})
	if err != nil {
		return err
	}
	for _, entry := range entries {
		resp.Attrs = append(resp.Attrs, &entry.Value.Attribute)
	}
	return nil
}

func (m *MemHandler) HandleGetRelations(meta txn.TxnMeta, req txnengine.GetRelationsReq, resp *txnengine.GetRelationsResp) error {
	tx := m.getTx(meta)
	entries, err := m.relations.Index(tx, Tuple{
		index_DatabaseID,
		req.DatabaseID,
	})
	if err != nil {
		return err
	}
	for _, entry := range entries {
		resp.Names = append(resp.Names, entry.Value.Name)
	}
	return nil
}

func (m *MemHandler) HandleGetTableDefs(meta txn.TxnMeta, req txnengine.GetTableDefsReq, resp *txnengine.GetTableDefsResp) error {
	tx := m.getTx(meta)

	relRow, err := m.relations.Get(tx, req.TableID)
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
			func(key ID, row *AttributeRow) error {
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
		entries, err := m.indexes.Index(tx, Tuple{
			index_RelationID, req.TableID,
		})
		if err != nil {
			return err
		}
		for _, entry := range entries {
			resp.Defs = append(resp.Defs, &entry.Value.IndexTableDef)
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
	entries, err := m.attributes.Index(tx, Tuple{
		index_RelationID_IsHidden,
		req.TableID,
		Bool(true),
	})
	if err != nil {
		return err
	}
	for _, entry := range entries {
		resp.Attrs = append(resp.Attrs, &entry.Value.Attribute)
	}
	return nil
}

func (m *MemHandler) HandleNewTableIter(meta txn.TxnMeta, req txnengine.NewTableIterReq, resp *txnengine.NewTableIterResp) error {
	tx := m.getTx(meta)

	tableIter := m.data.NewIter(tx)
	attrsMap := make(map[string]*AttributeRow)
	if err := m.iterRelationAttributes(
		tx, req.TableID,
		func(_ ID, row *AttributeRow) error {
			attrsMap[row.Name] = row
			return nil
		},
	); err != nil {
		return err
	}

	iter := &Iter[DataKey, DataValue]{
		TableIter: tableIter,
		TableID:   req.TableID,
		AttrsMap:  attrsMap,
		Expr:      req.Expr,
		nextFunc: func() bool {
			tableKey := DataKey{
				tableID: req.TableID,
			}
			return tableIter.Seek(tableKey)
		},
		ReadTime: tx.Time,
		Tx:       tx,
	}

	m.iterators.Lock()
	defer m.iterators.Unlock()
	id := txnengine.NewID()
	resp.IterID = id
	m.iterators.Map[id] = iter

	return nil
}

func (m *MemHandler) HandleOpenDatabase(meta txn.TxnMeta, req txnengine.OpenDatabaseReq, resp *txnengine.OpenDatabaseResp) error {
	tx := m.getTx(meta)

	entries, err := m.databases.Index(tx, Tuple{
		index_AccountID_Name,
		Uint(req.AccessInfo.AccountID),
		Text(req.Name),
	})
	if err != nil {
		return err
	}

	for _, entry := range entries {
		resp.ID = entry.Value.ID
		resp.Name = entry.Value.Name
		return nil
	}

	return moerr.NewNoDB()
}

func (m *MemHandler) HandleOpenRelation(meta txn.TxnMeta, req txnengine.OpenRelationReq, resp *txnengine.OpenRelationResp) error {
	tx := m.getTx(meta)
	entries, err := m.relations.Index(tx, Tuple{
		index_DatabaseID_Name,
		req.DatabaseID,
		Text(req.Name),
	})
	if err != nil {
		return err
	}
	if len(entries) == 0 {
		return moerr.NewNoSuchTable(req.DatabaseName, req.Name)
	}
	entry := entries[0]
	resp.ID = entry.Value.ID
	resp.Type = entry.Value.Type
	resp.RelationName = entry.Value.Name
	db, err := m.databases.Get(tx, entry.Value.DatabaseID)
	if err != nil {
		return err
	}
	resp.DatabaseName = db.Name
	return nil
}

func (m *MemHandler) HandleRead(meta txn.TxnMeta, req txnengine.ReadReq, resp *txnengine.ReadResp) error {
	resp.SetHeap(m.mheap)

	m.iterators.Lock()
	iter, ok := m.iterators.Map[req.IterID]
	if !ok {
		m.iterators.Unlock()
		return moerr.NewInternalError("no such iter: %v", req.IterID)
	}
	m.iterators.Unlock()

	b := batch.New(false, req.ColNames)

	for i, name := range req.ColNames {
		b.Vecs[i] = vector.New(iter.AttrsMap[name].Type)
	}

	fn := iter.TableIter.Next
	if iter.nextFunc != nil {
		fn = iter.nextFunc
		iter.nextFunc = nil
	}

	maxRows := 4096
	type Row struct {
		Value       DataValue
		PhysicalRow *memtable.PhysicalRow[DataKey, DataValue]
	}
	var rows []Row

	for ok := fn(); ok; ok = iter.TableIter.Next() {
		item := iter.TableIter.Item()
		value, err := item.Read(iter.ReadTime, iter.Tx)
		if err != nil {
			return err
		}
		if item.Key.tableID != iter.TableID {
			break
		}

		//TODO handle iter.Expr
		if iter.Expr != nil {
			panic(iter.Expr)
		}

		rows = append(rows, Row{
			Value:       value,
			PhysicalRow: item,
		})
		if len(rows) >= maxRows {
			break
		}
	}

	// sort to emulate TAE behavior TODO remove this after BVT fixes
	sort.Slice(rows, func(i, j int) bool {
		return rows[i].PhysicalRow.LastUpdate.Before(
			rows[j].PhysicalRow.LastUpdate,
		)
	})

	tx := m.getTx(meta)
	for _, row := range rows {
		namedRow := &NamedDataRow{
			Value:    row.Value,
			AttrsMap: iter.AttrsMap,
		}
		if err := appendNamedRow(tx, m, b, namedRow); err != nil {
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
	_, err := m.relations.Get(tx, req.TableID)
	if errors.Is(err, sql.ErrNoRows) {
		return moerr.NewNoSuchTable(req.DatabaseName, req.TableName)
	}
	return m.deleteRelationData(tx, req.TableID)
}

func (m *MemHandler) HandleUpdate(meta txn.TxnMeta, req txnengine.UpdateReq, resp *txnengine.UpdateResp) error {
	tx := m.getTx(meta)

	if err := m.rangeBatchPhysicalRows(
		tx,
		req.TableID,
		req.DatabaseName,
		req.TableName,
		req.Batch,
		func(
			row *DataRow,
			rowID types.Rowid,
		) error {
			if err := m.data.Update(tx, *row); err != nil {
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
		req.DatabaseName,
		req.TableName,
		req.Batch,
		func(
			row *DataRow,
			rowID types.Rowid,
		) error {
			if err := m.data.Insert(tx, *row); err != nil {
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
	tableID ID,
	dbName string,
	tableName string,
	b *batch.Batch,
	fn func(
		*DataRow,
		types.Rowid,
	) error,
) error {

	// load attributes
	nameToAttrs := make(map[string]*AttributeRow)
	if err := m.iterRelationAttributes(
		tx, tableID,
		func(_ ID, row *AttributeRow) error {
			nameToAttrs[row.Name] = row
			return nil
		},
	); err != nil {
		return err
	}

	if len(nameToAttrs) == 0 {
		return moerr.NewNoSuchTable(dbName, tableName)
	}

	// iter
	batchIter := NewBatchIter(b)
	for {
		row := batchIter()
		if len(row) == 0 {
			break
		}

		rowID := newRowID()
		physicalRow := NewDataRow(
			tableID,
			[]Tuple{
				{index_RowID, memtable.ToOrdered(rowID)},
			},
		)
		physicalRow.value = make(DataValue, 0, len(nameToAttrs))
		idx := nameToAttrs[rowIDColumnName].Order
		for idx >= len(physicalRow.value) {
			physicalRow.value = append(physicalRow.value, Nullable{})
		}
		physicalRow.value[idx] = Nullable{
			Value: rowID,
		}

		for i, col := range row {
			name := b.Attrs[i]

			attr, ok := nameToAttrs[name]
			if !ok {
				panic(fmt.Sprintf("unknown attr: %s", name))
			}

			if attr.Primary {
				physicalRow.key.primaryKey = append(
					physicalRow.key.primaryKey,
					memtable.ToOrdered(col.Value),
				)
			}

			idx := attr.Order
			for idx >= len(physicalRow.value) {
				physicalRow.value = append(physicalRow.value, Nullable{})
			}
			physicalRow.value[idx] = col
		}

		// use row id as primary key if no primary key is provided
		if len(physicalRow.key.primaryKey) == 0 {
			physicalRow.key.primaryKey = append(
				physicalRow.key.primaryKey,
				memtable.ToOrdered(rowID),
			)
		}

		if err := fn(physicalRow, rowID); err != nil {
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
		tx = memtable.NewTransaction(
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
	if err := tx.Commit(memtable.Now(m.clock)); err != nil {
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
	relationID ID,
	fn func(key ID, row *AttributeRow) error,
) error {
	entries, err := m.attributes.Index(tx, Tuple{
		index_RelationID,
		relationID,
	})
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if err := fn(entry.Key, entry.Value); err != nil {
			return err
		}
	}
	return nil
}

func (m *MemHandler) HandleTableStats(meta txn.TxnMeta, req txnengine.TableStatsReq, resp *txnengine.TableStatsResp) (err error) {
	tx := m.getTx(meta)

	// maybe an estimation is enough
	iter := m.data.NewIter(tx)
	defer iter.Close()
	n := 0
	tableKey := DataKey{
		tableID: req.TableID,
	}
	for ok := iter.Seek(tableKey); ok; ok = iter.Next() {
		key, _, err := iter.Read()
		if err != nil {
			return err
		}
		if key.tableID != req.TableID {
			break
		}
		n++
	}
	resp.Rows = n

	return nil
}

func newRowID() types.Rowid {
	var rowid types.Rowid
	err := binary.Read(crand.Reader, binary.LittleEndian, &rowid)
	if err != nil {
		panic(err)
	}
	return rowid
}
