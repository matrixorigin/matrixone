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
	"sync"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/storage"
	txnengine "github.com/matrixorigin/matrixone/pkg/storage/txn"
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
		Map map[string]*TableIter[AnyKey, AnyRow]
	}
}

func NewMemHandler() *MemHandler {
	h := &MemHandler{}
	h.transactions.Map = make(map[string]*Transaction)
	h.tables.Map = make(map[string]*Table[AnyKey, AnyRow])
	h.iterators.Map = make(map[string]*TableIter[AnyKey, AnyRow])
	h.databases = NewTable[Text, DatabaseRow]()
	h.relations = NewTable[Text, RelationRow]()
	h.attributes = NewTable[Text, AttributeRow]()
	h.indexes = NewTable[Text, IndexRow]()
	return h
}

var _ Handler = new(MemHandler)

func (m *MemHandler) HandleAddTableDef(meta txn.TxnMeta, req txnengine.AddTableDefReq, resp *txnengine.AddTableDefResp) error {
	tx := m.getTx(meta)
	switch def := req.Def.(type) {

	case *storage.CommentDef:
		// update comments
		row, err := m.relations.Get(tx, Text(req.TableID))
		if errors.Is(err, sql.ErrNoRows) {
			resp.ErrTableNotFound = true
			return nil
		}
		if err != nil {
			return err
		}
		row.Comments = def.Comment
		if err := m.relations.Update(tx, row); err != nil {
			return err
		}

	case *storage.AttributeDef:
		// add attribute
		// check existence
		iter := m.attributes.NewIter(tx)
		defer iter.Close()
		for ok := iter.First(); ok; ok = iter.Next() {
			_, row := iter.Read()
			if row.RelationID == req.TableID &&
				row.Name == def.Attr.Name {
				resp.ErrExisted = true
				return nil
			}
		}
		// insert
		attrRow := AttributeRow{
			ID:         uuid.NewString(),
			RelationID: req.TableID,
			Attribute:  def.Attr,
		}
		if err := m.attributes.Insert(tx, attrRow); err != nil {
			return err
		}

	case *storage.IndexTableDef:
		// add index
		// check existence
		iter := m.indexes.NewIter(tx)
		defer iter.Close()
		for ok := iter.First(); ok; ok = iter.Next() {
			_, row := iter.Read()
			if row.RelationID == req.TableID &&
				row.Name == def.Name {
				resp.ErrExisted = true
				return nil
			}
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

	case *storage.PropertiesDef:
		// update properties
		row, err := m.relations.Get(tx, Text(req.TableID))
		if errors.Is(err, sql.ErrNoRows) {
			resp.ErrTableNotFound = true
			return nil
		}
		for _, prop := range def.Properties {
			row.Properties[prop.Key] = prop.Value
		}
		if err := m.relations.Update(tx, row); err != nil {
			return err
		}

	case *storage.PrimaryIndexDef:
		// set primary index
		iter := m.attributes.NewIter(tx)
		defer iter.Close()
		for ok := iter.First(); ok; ok = iter.Next() {
			_, attrRow := iter.Read()
			if attrRow.RelationID != req.TableID {
				continue
			}
			isPrimary := false
			for _, name := range def.Names {
				if name == attrRow.Name {
					isPrimary = true
					break
				}
			}
			if isPrimary == attrRow.Primary {
				continue
			}
			attrRow.Primary = isPrimary
			if err := m.attributes.Update(tx, *attrRow); err != nil {
				return err
			}
		}

	default:
		return fmt.Errorf("unknown table def: %T", req.Def)

	}

	return nil
}

// HandleCloseTableIter implements Handler
func (*MemHandler) HandleCloseTableIter(meta txn.TxnMeta, req txnengine.CloseTableIterReq, resp *txnengine.CloseTableIterResp) error {
	//TODO
	panic("unimplemented")
}

func (m *MemHandler) HandleCreateDatabase(meta txn.TxnMeta, req txnengine.CreateDatabaseReq, resp *txnengine.CreateDatabaseResp) error {
	tx := m.getTx(meta)
	iter := m.databases.NewIter(tx)
	defer iter.Close()
	existed := false
	for ok := iter.First(); ok; ok = iter.Next() {
		_, row := iter.Read()
		if row.Name == req.Name {
			existed = true
			break
		}
	}
	if existed {
		resp.ErrExisted = true
		return nil
	}
	err := m.databases.Insert(tx, DatabaseRow{
		ID:   uuid.NewString(),
		Name: req.Name,
	})
	if err != nil {
		return err
	}
	return nil
}

func (m *MemHandler) HandleCreateRelation(meta txn.TxnMeta, req txnengine.CreateRelationReq, resp *txnengine.CreateRelationResp) error {
	tx := m.getTx(meta)

	// check existence
	iter := m.relations.NewIter(tx)
	defer iter.Close()
	for ok := iter.First(); ok; ok = iter.Next() {
		_, row := iter.Read()
		if row.DatabaseID == req.DatabaseID &&
			row.Name == req.Name {
			resp.ErrExisted = true
			return nil
		}
	}

	// row
	row := RelationRow{
		ID:         uuid.NewString(),
		DatabaseID: req.DatabaseID,
		Name:       req.Name,
		Type:       req.Type,
		Properties: make(map[string]string),
	}

	// handle defs
	var relAttrs []storage.Attribute
	var relIndexes []storage.IndexTableDef
	var primaryColumnNames []string
	for _, def := range req.Defs {
		switch def := def.(type) {

		case *storage.CommentDef:
			row.Comments = def.Comment

		case *storage.AttributeDef:
			relAttrs = append(relAttrs, def.Attr)

		case *storage.IndexTableDef:
			relIndexes = append(relIndexes, *def)

		case *storage.PropertiesDef:
			for _, prop := range def.Properties {
				row.Properties[prop.Key] = prop.Value
			}

		case *storage.PrimaryIndexDef:
			primaryColumnNames = def.Names

		}
	}

	// insert relation attributes
	for _, attr := range relAttrs {
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

	return nil
}

func (m *MemHandler) HandleDelTableDef(meta txn.TxnMeta, req txnengine.DelTableDefReq, resp *txnengine.DelTableDefResp) error {
	tx := m.getTx(meta)
	switch def := req.Def.(type) {

	case *storage.CommentDef:
		// del comments
		row, err := m.relations.Get(tx, Text(req.TableID))
		if errors.Is(err, sql.ErrNoRows) {
			resp.ErrTableNotFound = true
			return nil
		}
		if err != nil {
			return err
		}
		row.Comments = ""
		if err := m.relations.Update(tx, row); err != nil {
			return err
		}

	case *storage.AttributeDef:
		// delete attribute
		iter := m.attributes.NewIter(tx)
		defer iter.Close()
		for ok := iter.First(); ok; ok = iter.Next() {
			key, row := iter.Read()
			if row.RelationID == req.TableID &&
				row.Name == def.Attr.Name {
				if err := m.attributes.Delete(tx, key); err != nil {
					return err
				}
			}
		}

	case *storage.IndexTableDef:
		// delete index
		iter := m.indexes.NewIter(tx)
		defer iter.Close()
		for ok := iter.First(); ok; ok = iter.Next() {
			key, row := iter.Read()
			if row.RelationID == req.TableID &&
				row.Name == def.Name {
				if err := m.indexes.Delete(tx, key); err != nil {
					return err
				}
			}
		}

	case *storage.PropertiesDef:
		// delete properties
		row, err := m.relations.Get(tx, Text(req.TableID))
		if errors.Is(err, sql.ErrNoRows) {
			resp.ErrTableNotFound = true
			return nil
		}
		for _, prop := range def.Properties {
			delete(row.Properties, prop.Key)
		}
		if err := m.relations.Update(tx, row); err != nil {
			return err
		}

	case *storage.PrimaryIndexDef:
		// delete primary index
		iter := m.attributes.NewIter(tx)
		defer iter.Close()
		for ok := iter.First(); ok; ok = iter.Next() {
			_, attrRow := iter.Read()
			if !attrRow.Primary {
				continue
			}
			attrRow.Primary = false
			if err := m.attributes.Update(tx, *attrRow); err != nil {
				return err
			}
		}

	default:
		return fmt.Errorf("unknown table def: %T", req.Def)

	}

	return nil
}

// HandleDelete implements Handler
func (*MemHandler) HandleDelete(meta txn.TxnMeta, req txnengine.DeleteReq, resp *txnengine.DeleteResp) error {
	//TODO
	panic("unimplemented")
}

func (m *MemHandler) HandleDeleteDatabase(meta txn.TxnMeta, req txnengine.DeleteDatabaseReq, resp *txnengine.DeleteDatabaseResp) error {
	tx := m.getTx(meta)
	iter := m.databases.NewIter(tx)
	defer iter.Close()
	for ok := iter.First(); ok; ok = iter.Next() {
		key, row := iter.Read()
		if row.Name != req.Name {
			continue
		}
		if err := m.databases.Delete(tx, key); err != nil {
			return err
		}
		return nil
	}
	resp.ErrNotFound = true
	return nil
}

func (m *MemHandler) HandleDeleteRelation(meta txn.TxnMeta, req txnengine.DeleteRelationReq, resp *txnengine.DeleteRelationResp) error {
	tx := m.getTx(meta)
	iter := m.relations.NewIter(tx)
	defer iter.Close()
	for ok := iter.First(); ok; ok = iter.Next() {
		key, row := iter.Read()
		if row.DatabaseID != req.DatabaseID ||
			row.Name != req.Name {
			continue
		}
		if err := m.relations.Delete(tx, key); err != nil {
			return err
		}
		return nil
	}
	resp.ErrNotFound = true
	return nil
}

func (m *MemHandler) HandleGetDatabases(meta txn.TxnMeta, req txnengine.GetDatabasesReq, resp *txnengine.GetDatabasesResp) error {
	tx := m.getTx(meta)
	iter := m.databases.NewIter(tx)
	defer iter.Close()
	for ok := iter.First(); ok; ok = iter.Next() {
		_, row := iter.Read()
		resp.Names = append(resp.Names, row.Name)
	}
	return nil
}

func (m *MemHandler) HandleGetPrimaryKeys(meta txn.TxnMeta, req txnengine.GetPrimaryKeysReq, resp *txnengine.GetPrimaryKeysResp) error {
	tx := m.getTx(meta)
	iter := m.attributes.NewIter(tx)
	defer iter.Close()
	for ok := iter.First(); ok; ok = iter.Next() {
		_, row := iter.Read()
		if row.RelationID != req.TableID {
			continue
		}
		if !row.Primary {
			continue
		}
		resp.Attrs = append(resp.Attrs, &row.Attribute)
	}
	return nil
}

func (m *MemHandler) HandleGetRelations(meta txn.TxnMeta, req txnengine.GetRelationsReq, resp *txnengine.GetRelationsResp) error {
	tx := m.getTx(meta)
	iter := m.relations.NewIter(tx)
	defer iter.Close()
	for ok := iter.First(); ok; ok = iter.Next() {
		_, row := iter.Read()
		resp.Names = append(resp.Names, row.Name)
	}
	return nil
}

func (m *MemHandler) HandleGetTableDefs(meta txn.TxnMeta, req txnengine.GetTableDefsReq, resp *txnengine.GetTableDefsResp) error {
	tx := m.getTx(meta)

	relRow, err := m.relations.Get(tx, Text(req.TableID))
	if errors.Is(err, sql.ErrNoRows) {
		resp.ErrTableNotFound = true
		return nil
	}
	if err != nil {
		return err
	}

	// comments
	resp.Defs = append(resp.Defs, &storage.CommentDef{
		Comment: relRow.Comments,
	})

	// attributes and primary index
	{
		var primaryAttrNames []string
		iter := m.attributes.NewIter(tx)
		defer iter.Close()
		for ok := iter.First(); ok; ok = iter.Next() {
			_, attrRow := iter.Read()
			resp.Defs = append(resp.Defs, &storage.AttributeDef{
				Attr: attrRow.Attribute,
			})
			if attrRow.Primary {
				primaryAttrNames = append(primaryAttrNames, attrRow.Name)
			}
		}
		if len(primaryAttrNames) > 0 {
			resp.Defs = append(resp.Defs, &storage.PrimaryIndexDef{
				Names: primaryAttrNames,
			})
		}
	}

	// indexes
	{
		iter := m.indexes.NewIter(tx)
		defer iter.Close()
		for ok := iter.First(); ok; ok = iter.Next() {
			_, indexRow := iter.Read()
			resp.Defs = append(resp.Defs, &indexRow.IndexTableDef)
		}
	}

	// properties
	propertiesDef := new(storage.PropertiesDef)
	for key, value := range relRow.Properties {
		propertiesDef.Properties = append(propertiesDef.Properties, storage.Property{
			Key:   key,
			Value: value,
		})
	}
	resp.Defs = append(resp.Defs, propertiesDef)

	return nil
}

// HandleNewTableIter implements Handler
func (*MemHandler) HandleNewTableIter(meta txn.TxnMeta, req txnengine.NewTableIterReq, resp *txnengine.NewTableIterResp) error {
	//TODO
	panic("unimplemented")
}

func (m *MemHandler) HandleOpenDatabase(meta txn.TxnMeta, req txnengine.OpenDatabaseReq, resp *txnengine.OpenDatabaseResp) error {
	tx := m.getTx(meta)
	iter := m.databases.NewIter(tx)
	defer iter.Close()
	for ok := iter.First(); ok; ok = iter.Next() {
		_, row := iter.Read()
		if row.Name == req.Name {
			resp.ID = row.ID
			return nil
		}
	}
	resp.ErrNotFound = true
	return nil
}

func (m *MemHandler) HandleOpenRelation(meta txn.TxnMeta, req txnengine.OpenRelationReq, resp *txnengine.OpenRelationResp) error {
	tx := m.getTx(meta)
	iter := m.relations.NewIter(tx)
	defer iter.Close()
	for ok := iter.First(); ok; ok = iter.Next() {
		_, row := iter.Read()
		if row.DatabaseID == req.DatabaseID &&
			row.Name == req.Name {
			resp.ID = row.ID
			resp.Type = row.Type
			return nil
		}
	}
	resp.ErrNotFound = true
	return nil
}

// HandleRead implements Handler
func (*MemHandler) HandleRead(meta txn.TxnMeta, req txnengine.ReadReq, resp *txnengine.ReadResp) error {
	//TODO
	panic("unimplemented")
}

// HandleTruncate implements Handler
func (*MemHandler) HandleTruncate(meta txn.TxnMeta, req txnengine.TruncateReq, resp *txnengine.TruncateResp) error {
	//TODO
	panic("unimplemented")
}

// HandleUpdate implements Handler
func (*MemHandler) HandleUpdate(meta txn.TxnMeta, req txnengine.UpdateReq, resp *txnengine.UpdateResp) error {
	//TODO
	panic("unimplemented")
}

// HandleWrite implements Handler
func (*MemHandler) HandleWrite(meta txn.TxnMeta, req txnengine.WriteReq, resp *txnengine.WriteResp) error {
	//TODO
	panic("unimplemented")
}

func (m *MemHandler) getTx(meta txn.TxnMeta) *Transaction {
	id := string(meta.ID)
	m.transactions.Lock()
	defer m.transactions.Unlock()
	tx, ok := m.transactions.Map[id]
	if !ok {
		tx = NewTransaction(id, meta.SnapshotTS)
		m.transactions.Map[id] = tx
	}
	return tx
}
