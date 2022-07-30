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
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	txnengine "github.com/matrixorigin/matrixone/pkg/vm/engine/txn"
)

type MemHandler struct {
	transactions struct {
		sync.Mutex
		Map map[string]*Transaction
	}

	databases  *Table[Text, DatabaseAttrs]
	relations  *Table[Text, RelationAttrs]
	attributes *Table[Text, AttributeAttrs]
	indexes    *Table[Text, IndexAttrs]
}

func NewMemHandler() *MemHandler {
	h := &MemHandler{}
	h.transactions.Map = make(map[string]*Transaction)
	h.databases = NewTable[Text, DatabaseAttrs]()
	h.relations = NewTable[Text, RelationAttrs]()
	h.attributes = NewTable[Text, AttributeAttrs]()
	h.indexes = NewTable[Text, IndexAttrs]()
	return h
}

var _ Handler = new(MemHandler)

func (m *MemHandler) HandleAddTableDef(meta txn.TxnMeta, req txnengine.AddTableDefReq, resp *txnengine.AddTableDefResp) error {
	tx := m.getTx(meta)
	switch def := req.Def.(type) {

	case *engine.CommentDef:
		// update comments
		attrs, err := m.relations.Get(tx, Text(req.TableID))
		if errors.Is(err, sql.ErrNoRows) {
			resp.ErrTableNotFound = true
			return nil
		}
		if err != nil {
			return err
		}
		attrs.Comments = def.Comment
		if err := m.relations.Update(tx, attrs); err != nil {
			return err
		}

	case *engine.AttributeDef:
		// add attribute
		// check existence
		iter := m.attributes.NewIter(tx)
		defer iter.Close()
		for ok := iter.First(); ok; ok = iter.Next() {
			_, attrs := iter.Read()
			if attrs.RelationID == req.TableID &&
				attrs.Name == def.Attr.Name {
				resp.ErrExisted = true
				return nil
			}
		}
		// insert
		attrAttrs := AttributeAttrs{
			ID:         uuid.NewString(),
			RelationID: req.TableID,
			Attribute:  def.Attr,
		}
		if err := m.attributes.Insert(tx, attrAttrs); err != nil {
			return err
		}

	case *engine.IndexTableDef:
		// add index
		// check existence
		iter := m.indexes.NewIter(tx)
		defer iter.Close()
		for ok := iter.First(); ok; ok = iter.Next() {
			_, attrs := iter.Read()
			if attrs.RelationID == req.TableID &&
				attrs.Name == def.Name {
				resp.ErrExisted = true
				return nil
			}
		}
		// insert
		idxAttrs := IndexAttrs{
			ID:            uuid.NewString(),
			RelationID:    req.TableID,
			IndexTableDef: *def,
		}
		if err := m.indexes.Insert(tx, idxAttrs); err != nil {
			return err
		}

	case *engine.PropertiesDef:
		// update properties
		attrs, err := m.relations.Get(tx, Text(req.TableID))
		if errors.Is(err, sql.ErrNoRows) {
			resp.ErrTableNotFound = true
			return nil
		}
		for _, prop := range def.Properties {
			attrs.Properties[prop.Key] = prop.Value
		}
		if err := m.relations.Update(tx, attrs); err != nil {
			return err
		}

	case *engine.PrimaryIndexDef:
		// set primary index
		// check existence
		attrs, err := m.relations.Get(tx, Text(req.TableID))
		if errors.Is(err, sql.ErrNoRows) {
			resp.ErrTableNotFound = true
			return nil
		}
		if len(attrs.PrimaryColumnIDs) > 0 {
			resp.ErrExisted = true
			return nil
		}
		// set
		iter := m.attributes.NewIter(tx)
		defer iter.Close()
		nameToID := make(map[string]string)
		for ok := iter.First(); ok; ok = iter.Next() {
			_, attrAttrs := iter.Read()
			if attrAttrs.RelationID != req.TableID {
				continue
			}
			nameToID[attrAttrs.Name] = attrAttrs.ID
		}
		var ids []string
		for _, name := range def.Names {
			id, ok := nameToID[name]
			if !ok {
				resp.ErrColumnNotFound = name
				return nil
			}
			ids = append(ids, id)
		}
		attrs.PrimaryColumnIDs = ids
		if err := m.relations.Update(tx, attrs); err != nil {
			return err
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
		_, attrs := iter.Read()
		if attrs.Name == req.Name {
			existed = true
			break
		}
	}
	if existed {
		resp.ErrExisted = true
		return nil
	}
	err := m.databases.Insert(tx, DatabaseAttrs{
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
		_, attrs := iter.Read()
		if attrs.DatabaseID == req.DatabaseID &&
			attrs.Name == req.Name {
			resp.ErrExisted = true
			return nil
		}
	}

	// attrs
	attrs := RelationAttrs{
		ID:         uuid.NewString(),
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
			attrs.Comments = def.Comment

		case *engine.AttributeDef:
			relAttrs = append(relAttrs, def.Attr)

		case *engine.IndexTableDef:
			relIndexes = append(relIndexes, *def)

		case *engine.PropertiesDef:
			for _, prop := range def.Properties {
				attrs.Properties[prop.Key] = prop.Value
			}

		case *engine.PrimaryIndexDef:
			primaryColumnNames = def.Names

		}
	}

	// insert relation attributes
	attrNameIDMap := make(map[string]string)
	for _, attr := range relAttrs {
		attrAttrs := AttributeAttrs{
			ID:         uuid.NewString(),
			RelationID: attrs.ID,
			Attribute:  attr,
		}
		attrNameIDMap[attr.Name] = attrAttrs.ID
		if err := m.attributes.Insert(tx, attrAttrs); err != nil {
			return err
		}
	}

	// set primary column ids
	ids := make([]string, 0, len(primaryColumnNames))
	for _, name := range primaryColumnNames {
		ids = append(ids, attrNameIDMap[name])
	}
	attrs.PrimaryColumnIDs = ids

	// insert relation indexes
	for _, idx := range relIndexes {
		idxAttrs := IndexAttrs{
			ID:            uuid.NewString(),
			RelationID:    attrs.ID,
			IndexTableDef: idx,
		}
		if err := m.indexes.Insert(tx, idxAttrs); err != nil {
			return err
		}
	}

	// insert relation
	if err := m.relations.Insert(tx, attrs); err != nil {
		return err
	}

	return nil
}

// HandleDelTableDef implements Handler
func (*MemHandler) HandleDelTableDef(meta txn.TxnMeta, req txnengine.DelTableDefReq, resp *txnengine.DelTableDefResp) error {
	//TODO
	panic("unimplemented")
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
		key, attrs := iter.Read()
		if attrs.Name != req.Name {
			continue
		}

		// delete database
		if err := m.databases.Delete(tx, key); err != nil {
			return err
		}

		//TODO delete related

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
		key, attrs := iter.Read()
		if attrs.DatabaseID != req.DatabaseID ||
			attrs.Name != req.Name {
			continue
		}

		// delete relation
		if err := m.relations.Delete(tx, key); err != nil {
			return err
		}

		//TODO delete related

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
		_, attrs := iter.Read()
		resp.Names = append(resp.Names, attrs.Name)
	}
	return nil
}

// HandleGetPrimaryKeys implements Handler
func (*MemHandler) HandleGetPrimaryKeys(meta txn.TxnMeta, req txnengine.GetPrimaryKeysReq, resp *txnengine.GetPrimaryKeysResp) error {
	//TODO
	panic("unimplemented")
}

func (m *MemHandler) HandleGetRelations(meta txn.TxnMeta, req txnengine.GetRelationsReq, resp *txnengine.GetRelationsResp) error {
	tx := m.getTx(meta)
	iter := m.relations.NewIter(tx)
	defer iter.Close()
	for ok := iter.First(); ok; ok = iter.Next() {
		_, attrs := iter.Read()
		resp.Names = append(resp.Names, attrs.Name)
	}
	return nil
}

// HandleGetTableDefs implements Handler
func (*MemHandler) HandleGetTableDefs(meta txn.TxnMeta, req txnengine.GetTableDefsReq, resp *txnengine.GetTableDefsResp) error {
	//TODO
	panic("unimplemented")
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
		_, attrs := iter.Read()
		if attrs.Name == req.Name {
			resp.ID = attrs.ID
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
		_, attrs := iter.Read()
		if attrs.DatabaseID == req.DatabaseID &&
			attrs.Name == req.Name {
			resp.ID = attrs.ID
			resp.Type = attrs.Type
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
