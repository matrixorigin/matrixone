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

package memorystorage

import (
	"context"
	crand "crypto/rand"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/txn/storage/memorystorage/memtable"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/memoryengine"
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
	mheap                  *mpool.MPool
	defaultIsolationPolicy IsolationPolicy
	clock                  clock.Clock
	idGenerator            memoryengine.IDGenerator
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
	mp *mpool.MPool,
	defaultIsolationPolicy IsolationPolicy,
	clock clock.Clock,
	idGenerator memoryengine.IDGenerator,
) *MemHandler {
	h := &MemHandler{
		databases:              memtable.NewTable[ID, *DatabaseRow, *DatabaseRow](),
		relations:              memtable.NewTable[ID, *RelationRow, *RelationRow](),
		attributes:             memtable.NewTable[ID, *AttributeRow, *AttributeRow](),
		indexes:                memtable.NewTable[ID, *IndexRow, *IndexRow](),
		data:                   memtable.NewTable[DataKey, DataValue, DataRow](),
		mheap:                  mp,
		defaultIsolationPolicy: defaultIsolationPolicy,
		clock:                  clock,
		idGenerator:            idGenerator,
	}
	h.transactions.Map = make(map[string]*Transaction)
	h.iterators.Map = make(map[ID]*Iter[DataKey, DataValue])
	return h
}

var _ Handler = new(MemHandler)

func (m *MemHandler) HandleAddTableDef(ctx context.Context, meta txn.TxnMeta, req memoryengine.AddTableDefReq, resp *memoryengine.AddTableDefResp) error {
	tx := m.getTx(meta)

	table, err := m.relations.Get(tx, req.TableID)
	if errors.Is(err, sql.ErrNoRows) {
		return moerr.NewInternalError(
			"invalid table id %v, db %v, name %v",
			req.TableID,
			req.DatabaseName,
			req.TableName,
		)
	}
	if err != nil {
		return err
	}

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
		table.Comments = []byte(def.Comment)
		if err := m.relations.Update(tx, table); err != nil {
			return err
		}

	case *engine.PartitionDef:
		// update
		table.PartitionDef = []byte(def.Partition)
		if err := m.relations.Update(tx, table); err != nil {
			return err
		}

	case *engine.ViewDef:
		// update
		table.ViewDef = []byte(def.View)
		if err := m.relations.Update(tx, table); err != nil {
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
			return moerr.NewConstraintViolation(`duplicate column "%s"`, def.Attr.Name)
		}
		// insert
		id, err := m.idGenerator.NewID(ctx)
		if err != nil {
			return err
		}
		attrRow := &AttributeRow{
			ID:         id,
			RelationID: req.TableID,
			Order:      maxAttributeOrder + 1,
			Nullable:   def.Attr.Default != nil && def.Attr.Default.NullAbility,
			Attribute:  def.Attr,
		}
		if err := m.attributes.Insert(tx, attrRow); err != nil {
			return err
		}

	case *engine.IndexTableDef:
		// tea & mem do not use this def now.
	case *engine.ComputeIndexDef:
		// add index
		// check existence
		for i := 0; i < len(def.IndexNames); i++ {
			entries, err := m.indexes.Index(tx, Tuple{
				index_RelationID_Name,
				req.TableID,
				Text(def.IndexNames[i]),
			})
			if err != nil {
				return err
			}
			if len(entries) > 0 {
				return moerr.NewDuplicate()
			}
			// insert
			id, err := m.idGenerator.NewID(ctx)
			if err != nil {
				return err
			}
			idxRow := &IndexRow{
				ID:         id,
				RelationID: req.TableID,
				IndexName:  def.IndexNames[i],
				Unique:     def.Uniques[i],
				TableName:  def.TableNames[i],
				Field:      def.Fields[i],
			}
			if err := m.indexes.Insert(tx, idxRow); err != nil {
				return err
			}
		}

	case *engine.PropertiesDef:
		// update properties
		for _, prop := range def.Properties {
			table.Properties[prop.Key] = prop.Value
		}
		if err := m.relations.Update(tx, table); err != nil {
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

func (m *MemHandler) HandleCloseTableIter(ctx context.Context, meta txn.TxnMeta, req memoryengine.CloseTableIterReq, resp *memoryengine.CloseTableIterResp) error {
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

func (m *MemHandler) HandleCreateDatabase(ctx context.Context, meta txn.TxnMeta, req memoryengine.CreateDatabaseReq, resp *memoryengine.CreateDatabaseResp) error {
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

	if req.ID.IsEmpty() {
		req.ID, err = m.idGenerator.NewID(ctx)
		if err != nil {
			return err
		}
	}
	err = m.databases.Insert(tx, &DatabaseRow{
		ID:        req.ID,
		AccountID: req.AccessInfo.AccountID,
		Name:      []byte(req.Name),
	})
	if err != nil {
		return err
	}

	resp.ID = req.ID
	return nil
}

func (m *MemHandler) HandleCreateRelation(ctx context.Context, meta txn.TxnMeta, req memoryengine.CreateRelationReq, resp *memoryengine.CreateRelationResp) error {
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
	if req.ID.IsEmpty() {
		req.ID, err = m.idGenerator.NewID(ctx)
		if err != nil {
			return err
		}
	}
	row := &RelationRow{
		ID:         req.ID,
		DatabaseID: req.DatabaseID,
		Name:       []byte(req.Name),
		Type:       req.Type,
		Properties: make(map[string]string),
	}

	// handle defs
	var relAttrs []engine.Attribute
	var relIndexes *engine.ComputeIndexDef
	var primaryColumnNames []string
	for _, def := range req.Defs {
		switch def := def.(type) {

		case *engine.CommentDef:
			row.Comments = []byte(def.Comment)

		case *engine.PartitionDef:
			row.PartitionDef = []byte(def.Partition)

		case *engine.ViewDef:
			row.ViewDef = []byte(def.View)

		case *engine.AttributeDef:
			relAttrs = append(relAttrs, def.Attr)

		case *engine.IndexTableDef:
			// do nothing

		case *engine.ComputeIndexDef:
			relIndexes = def

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

	if len(relAttrs) == 0 && len(row.ViewDef) == 0 {
		return moerr.NewConstraintViolation("no schema")
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
	nameSet := make(map[string]bool)
	for i, attr := range relAttrs {
		if _, ok := nameSet[attr.Name]; ok {
			return moerr.NewConstraintViolation(`duplicate column "%s"`, attr.Name)
		}
		nameSet[attr.Name] = true
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
		id, err := m.idGenerator.NewID(ctx)
		if err != nil {
			return err
		}
		attrRow := &AttributeRow{
			ID:         id,
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
	if relIndexes != nil {
		for i := 0; i < len(relIndexes.IndexNames); i++ {
			id, err := m.idGenerator.NewID(ctx)
			if err != nil {
				return err
			}
			idxRow := &IndexRow{
				ID:         id,
				RelationID: row.ID,
				IndexName:  relIndexes.IndexNames[i],
				Unique:     relIndexes.Uniques[i],
				TableName:  relIndexes.TableNames[i],
				Field:      relIndexes.Fields[i],
			}
			if err := m.indexes.Insert(tx, idxRow); err != nil {
				return err
			}
		}
	}

	// insert relation
	if err := m.relations.Insert(tx, row); err != nil {
		return err
	}

	resp.ID = row.ID
	return nil
}

const rowIDColumnName = catalog.Row_ID

func (m *MemHandler) HandleDelTableDef(ctx context.Context, meta txn.TxnMeta, req memoryengine.DelTableDefReq, resp *memoryengine.DelTableDefResp) error {
	tx := m.getTx(meta)

	table, err := m.relations.Get(tx, req.TableID)
	if errors.Is(err, sql.ErrNoRows) {
		return moerr.NewInternalError(
			"invalid table id %v, db %v, name %v",
			req.TableID,
			req.DatabaseName,
			req.TableName,
		)
	}
	if err != nil {
		return err
	}

	switch def := req.Def.(type) {

	case *engine.CommentDef:
		// del comments
		table.Comments = nil
		if err := m.relations.Update(tx, table); err != nil {
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
		for _, prop := range def.Properties {
			delete(table.Properties, prop.Key)
		}
		if err := m.relations.Update(tx, table); err != nil {
			return err
		}

	default:
		panic(fmt.Sprintf("invalid table def: %T", req.Def))

	}

	return nil
}

func (m *MemHandler) HandleDelete(ctx context.Context, meta txn.TxnMeta, req memoryengine.DeleteReq, resp *memoryengine.DeleteResp) error {
	tx := m.getTx(meta)
	reqVecLen := req.Vector.Length()

	// check table existence
	_, err := m.relations.Get(tx, ID(req.TableID))
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			panic(fmt.Sprintf("no such table: %v, %v", req.TableID, req.TableName))
		}
		return err
	}

	// by row id
	if req.ColumnName == rowIDColumnName {
		for i := 0; i < reqVecLen; i++ {
			value := memtable.VectorAt(req.Vector, i)
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
	if len(entries) == 1 {
		attr, err := m.attributes.Get(tx, entries[0].Key)
		if err != nil {
			return err
		}
		if attr.Name == req.ColumnName {
			// by primary key
			for i := 0; i < reqVecLen; i++ {
				value := memtable.VectorAt(req.Vector, i)
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
	if len(entries) == 0 {
		return moerr.NewInternalError("no such column: %s", req.ColumnName)
	}
	if len(entries) != 1 {
		panic("impossible")
	}
	attr, err := m.attributes.Get(tx, entries[0].Key)
	if err != nil {
		return err
	}
	attrIndex := attr.Order
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
			value := memtable.VectorAt(req.Vector, i)
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

func (m *MemHandler) HandleDeleteDatabase(ctx context.Context, meta txn.TxnMeta, req memoryengine.DeleteDatabaseReq, resp *memoryengine.DeleteDatabaseResp) error {
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
		db, err := m.databases.Get(tx, entry.Key)
		if err != nil {
			return err
		}
		if err := m.databases.Delete(tx, entry.Key); err != nil {
			return err
		}
		if err := m.deleteRelationsByDBID(tx, db.ID); err != nil {
			return err
		}
		resp.ID = db.ID
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
		rel, err := m.relations.Get(tx, entry.Key)
		if err != nil {
			return err
		}
		if err := m.relations.Delete(tx, entry.Key); err != nil {
			return err
		}
		if err := m.deleteAttributesByRelationID(tx, rel.ID); err != nil {
			return err
		}
		if err := m.deleteRelationData(tx, rel.ID); err != nil {
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

func (m *MemHandler) HandleDeleteRelation(ctx context.Context, meta txn.TxnMeta, req memoryengine.DeleteRelationReq, resp *memoryengine.DeleteRelationResp) error {
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
	rel, err := m.relations.Get(tx, entry.Key)
	if err != nil {
		return err
	}
	if err := m.relations.Delete(tx, entry.Key); err != nil {
		return err
	}
	if err := m.deleteAttributesByRelationID(tx, rel.ID); err != nil {
		return err
	}
	resp.ID = rel.ID
	return nil
}

func (m *MemHandler) HandleTruncateRelation(ctx context.Context, meta txn.TxnMeta, req memoryengine.TruncateRelationReq, resp *memoryengine.TruncateRelationResp) error {
	tx := m.getTx(meta)
	_, err := m.relations.Get(tx, req.OldTableID)
	if errors.Is(err, sql.ErrNoRows) {
		return moerr.NewNoSuchTable(req.DatabaseName, req.Name)
	}
	return m.deleteRelationData(tx, req.OldTableID)
}

func (m *MemHandler) HandleGetDatabases(ctx context.Context, meta txn.TxnMeta, req memoryengine.GetDatabasesReq, resp *memoryengine.GetDatabasesResp) error {
	tx := m.getTx(meta)

	entries, err := m.databases.Index(tx, Tuple{
		index_AccountID,
		Uint(req.AccessInfo.AccountID),
	})
	if err != nil {
		return err
	}

	for _, entry := range entries {
		db, err := m.databases.Get(tx, entry.Key)
		if err != nil {
			return err
		}
		resp.Names = append(resp.Names, string(db.Name))
	}

	return nil
}

func (m *MemHandler) HandleGetPrimaryKeys(ctx context.Context, meta txn.TxnMeta, req memoryengine.GetPrimaryKeysReq, resp *memoryengine.GetPrimaryKeysResp) error {
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
		attr, err := m.attributes.Get(tx, entry.Key)
		if err != nil {
			return err
		}
		resp.Attrs = append(resp.Attrs, &attr.Attribute)
	}
	return nil
}

func (m *MemHandler) HandleGetRelations(ctx context.Context, meta txn.TxnMeta, req memoryengine.GetRelationsReq, resp *memoryengine.GetRelationsResp) error {
	tx := m.getTx(meta)
	entries, err := m.relations.Index(tx, Tuple{
		index_DatabaseID,
		req.DatabaseID,
	})
	if err != nil {
		return err
	}
	for _, entry := range entries {
		rel, err := m.relations.Get(tx, entry.Key)
		if err != nil {
			return err
		}
		resp.Names = append(resp.Names, string(rel.Name))
	}
	return nil
}

func (m *MemHandler) HandleGetTableColumns(ctx context.Context, meta txn.TxnMeta, req memoryengine.GetTableColumnsReq, resp *memoryengine.GetTableColumnsResp) error {
	tx := m.getTx(meta)

	_, err := m.relations.Get(tx, req.TableID)
	if errors.Is(err, sql.ErrNoRows) {
		// the caller expects no error if table not exist
		//resp.ErrTableNotFound.ID = req.TableID
		return nil
	}
	if err != nil {
		return err
	}

	var attrRows []*AttributeRow
	if err := m.iterRelationAttributes(
		tx, req.TableID,
		func(_ ID, row *AttributeRow) error {
			attrRows = append(attrRows, row)
			return nil
		},
	); err != nil {
		return err
	}
	sort.Slice(attrRows, func(i, j int) bool {
		return attrRows[i].Order < attrRows[j].Order
	})
	for _, row := range attrRows {
		resp.Attrs = append(resp.Attrs, &row.Attribute)
	}
	return nil
}

func (m *MemHandler) HandleGetTableDefs(ctx context.Context, meta txn.TxnMeta, req memoryengine.GetTableDefsReq, resp *memoryengine.GetTableDefsResp) error {
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
	if len(relRow.Comments) != 0 {
		resp.Defs = append(resp.Defs, &engine.CommentDef{
			Comment: string(relRow.Comments),
		})
	}

	// partiton
	if len(relRow.PartitionDef) != 0 {
		resp.Defs = append(resp.Defs, &engine.PartitionDef{
			Partition: string(relRow.PartitionDef),
		})
	}

	// view
	if len(relRow.ViewDef) != 0 {
		resp.Defs = append(resp.Defs, &engine.ViewDef{
			View: string(relRow.ViewDef),
		})
	}

	// attributes and primary index
	{
		var primaryAttrNames []string
		var attrRows []*AttributeRow
		if err := m.iterRelationAttributes(
			tx, req.TableID,
			func(_ ID, row *AttributeRow) error {
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
		indexLen := len(entries)
		if indexLen > 0 {
			computeIndexDef := &engine.ComputeIndexDef{
				IndexNames: make([]string, indexLen),
				Uniques:    make([]bool, indexLen),
				TableNames: make([]string, indexLen),
				Fields:     make([][]string, indexLen),
			}
			for i, entry := range entries {
				index, err := m.indexes.Get(tx, entry.Key)
				if err != nil {
					return err
				}
				computeIndexDef.IndexNames[i] = index.IndexName
				computeIndexDef.Uniques[i] = index.Unique
				computeIndexDef.TableNames[i] = index.TableName
				computeIndexDef.Fields[i] = index.Field
			}
			resp.Defs = append(resp.Defs, computeIndexDef)
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

func (m *MemHandler) HandleGetHiddenKeys(ctx context.Context, meta txn.TxnMeta, req memoryengine.GetHiddenKeysReq, resp *memoryengine.GetHiddenKeysResp) error {
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
		attr, err := m.attributes.Get(tx, entry.Key)
		if err != nil {
			return err
		}
		resp.Attrs = append(resp.Attrs, &attr.Attribute)
	}
	return nil
}

func (m *MemHandler) HandleNewTableIter(ctx context.Context, meta txn.TxnMeta, req memoryengine.NewTableIterReq, resp *memoryengine.NewTableIterResp) error {
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
	id, err := m.idGenerator.NewID(ctx)
	if err != nil {
		return err
	}
	resp.IterID = id
	m.iterators.Map[id] = iter

	return nil
}

func (m *MemHandler) HandleOpenDatabase(ctx context.Context, meta txn.TxnMeta, req memoryengine.OpenDatabaseReq, resp *memoryengine.OpenDatabaseResp) error {
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

	entry := entries[0]
	db, err := m.databases.Get(tx, entry.Key)
	if err != nil {
		return err
	}
	resp.ID = db.ID
	resp.Name = string(db.Name)

	return nil
}

func (m *MemHandler) HandleOpenRelation(ctx context.Context, meta txn.TxnMeta, req memoryengine.OpenRelationReq, resp *memoryengine.OpenRelationResp) error {
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
	rel, err := m.relations.Get(tx, entry.Key)
	if err != nil {
		return err
	}
	resp.ID = rel.ID
	resp.Type = rel.Type
	resp.RelationName = string(rel.Name)
	db, err := m.databases.Get(tx, rel.DatabaseID)
	if err != nil {
		return err
	}
	resp.DatabaseName = string(db.Name)
	return nil
}

func (m *MemHandler) HandleRead(ctx context.Context, meta txn.TxnMeta, req memoryengine.ReadReq, resp *memoryengine.ReadResp) error {
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
		if err := appendNamedRowToBatch(tx, m, 0, b, namedRow); err != nil {
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

func (m *MemHandler) HandleUpdate(ctx context.Context, meta txn.TxnMeta, req memoryengine.UpdateReq, resp *memoryengine.UpdateResp) error {
	tx := m.getTx(meta)

	if err := m.rangeBatchPhysicalRows(
		tx,
		req.TableID,
		req.DatabaseName,
		req.TableName,
		req.Batch,
		func(
			row *DataRow,
			_ types.Rowid,
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

func (m *MemHandler) HandleWrite(ctx context.Context, meta txn.TxnMeta, req memoryengine.WriteReq, resp *memoryengine.WriteResp) error {
	tx := m.getTx(meta)

	if err := m.rangeBatchPhysicalRows(
		tx,
		req.TableID,
		req.DatabaseName,
		req.TableName,
		req.Batch,
		func(
			row *DataRow,
			_ types.Rowid,
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
		return moerr.NewInternalError(
			"invalid table id %v, db %v, name %v",
			tableID,
			dbName,
			tableName,
		)
	}

	// iter
	batchIter := memtable.NewBatchIter(b)
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

func (*MemHandler) HandleClose(ctx context.Context) error {
	return nil
}

func (m *MemHandler) HandleCommit(ctx context.Context, meta txn.TxnMeta) error {
	tx := m.getTx(meta)
	commitTS := meta.CommitTS
	if commitTS.IsEmpty() {
		commitTS = tx.Time.Timestamp
	}
	if err := tx.Commit(Time{
		Timestamp: commitTS,
	}); err != nil {
		return err
	}
	return nil
}

func (m *MemHandler) HandleCommitting(ctx context.Context, meta txn.TxnMeta) error {
	return nil
}

func (m *MemHandler) HandleDestroy(ctx context.Context) error {
	*m = *NewMemHandler(m.mheap, m.defaultIsolationPolicy, m.clock, m.idGenerator)
	return nil
}

func (m *MemHandler) HandlePrepare(ctx context.Context, meta txn.TxnMeta) (timestamp.Timestamp, error) {
	now, _ := m.clock.Now()
	return now, nil
}

func (m *MemHandler) HandleRollback(ctx context.Context, meta txn.TxnMeta) error {
	tx := m.getTx(meta)
	tx.Abort()
	return nil
}

func (m *MemHandler) HandleStartRecovery(ctx context.Context, ch chan txn.TxnMeta) {
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
		attr, err := m.attributes.Get(tx, entry.Key)
		if err != nil {
			return err
		}
		if err := fn(entry.Key, attr); err != nil {
			return err
		}
	}
	return nil
}

func (m *MemHandler) HandleTableStats(ctx context.Context, meta txn.TxnMeta, req memoryengine.TableStatsReq, resp *memoryengine.TableStatsResp) (err error) {
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
