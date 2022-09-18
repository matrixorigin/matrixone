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
	"errors"
	"fmt"
	"math"
	"sync"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	taedata "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/moengine"
	txnengine "github.com/matrixorigin/matrixone/pkg/vm/engine/txn"
)

//TODO system table accessing for non-sys account

// CatalogHandler handles read-only requests for catalog
type CatalogHandler struct {
	upstream       *MemHandler
	dbID           string
	sysRelationIDs map[string]string
	iterators      struct {
		sync.Mutex
		Map map[string]any // id -> Iterator
	}
}

var _ Handler = new(CatalogHandler)

func NewCatalogHandler(upstream *MemHandler) *CatalogHandler {

	handler := &CatalogHandler{
		upstream:       upstream,
		sysRelationIDs: make(map[string]string),
	}
	handler.iterators.Map = make(map[string]any)

	now := Time{
		Timestamp: timestamp.Timestamp{
			PhysicalTime: math.MinInt,
		},
	}
	tx := NewTransaction(uuid.NewString(), now, SnapshotIsolation)
	defer func() {
		if err := tx.Commit(); err != nil {
			panic(err)
		}
	}()

	// database
	db := DatabaseRow{
		ID:        uuid.NewString(),
		NumberID:  catalog.SystemDBID,
		AccountID: 0,
		Name:      catalog.SystemDBName,
	}
	if err := upstream.databases.Insert(tx, db); err != nil {
		panic(err)
	}
	handler.dbID = db.ID

	// relations
	databasesRelRow := RelationRow{
		ID:         uuid.NewString(),
		NumberID:   catalog.SystemTable_DB_ID,
		DatabaseID: db.ID,
		Name:       catalog.SystemTable_DB_Name,
		Type:       txnengine.RelationTable,
	}
	if err := upstream.relations.Insert(tx, databasesRelRow); err != nil {
		panic(err)
	}
	handler.sysRelationIDs[databasesRelRow.ID] = databasesRelRow.Name

	tablesRelRow := RelationRow{
		ID:         uuid.NewString(),
		NumberID:   catalog.SystemTable_Table_ID,
		DatabaseID: db.ID,
		Name:       catalog.SystemTable_Table_Name,
		Type:       txnengine.RelationTable,
	}
	if err := upstream.relations.Insert(tx, tablesRelRow); err != nil {
		panic(err)
	}
	handler.sysRelationIDs[tablesRelRow.ID] = tablesRelRow.Name

	attributesRelRow := RelationRow{
		ID:         uuid.NewString(),
		NumberID:   catalog.SystemTable_Columns_ID,
		DatabaseID: db.ID,
		Name:       catalog.SystemTable_Columns_Name,
		Type:       txnengine.RelationTable,
	}
	if err := upstream.relations.Insert(tx, attributesRelRow); err != nil {
		panic(err)
	}
	handler.sysRelationIDs[attributesRelRow.ID] = attributesRelRow.Name

	// attributes
	// databases
	defs, err := moengine.SchemaToDefs(catalog.SystemDBSchema)
	if err != nil {
		panic(err)
	}
	for i, def := range defs {
		attr, ok := def.(*engine.AttributeDef)
		if !ok {
			continue
		}
		row := AttributeRow{
			ID:         uuid.NewString(),
			RelationID: databasesRelRow.ID,
			Order:      i,
			Nullable:   true,
			Attribute:  attr.Attr,
		}
		if err := upstream.attributes.Insert(tx, row); err != nil {
			panic(err)
		}
	}
	// relations
	defs, err = moengine.SchemaToDefs(catalog.SystemTableSchema)
	if err != nil {
		panic(err)
	}
	for i, def := range defs {
		attr, ok := def.(*engine.AttributeDef)
		if !ok {
			continue
		}
		row := AttributeRow{
			ID:         uuid.NewString(),
			RelationID: tablesRelRow.ID,
			Order:      i,
			Nullable:   true,
			Attribute:  attr.Attr,
		}
		if err := upstream.attributes.Insert(tx, row); err != nil {
			panic(err)
		}
	}
	// attributes
	defs, err = moengine.SchemaToDefs(catalog.SystemColumnSchema)
	if err != nil {
		panic(err)
	}
	for i, def := range defs {
		attr, ok := def.(*engine.AttributeDef)
		if !ok {
			continue
		}
		row := AttributeRow{
			ID:         uuid.NewString(),
			RelationID: attributesRelRow.ID,
			Order:      i,
			Nullable:   true,
			Attribute:  attr.Attr,
		}
		if err := upstream.attributes.Insert(tx, row); err != nil {
			panic(err)
		}
	}

	return handler
}

func (c *CatalogHandler) HandleAddTableDef(meta txn.TxnMeta, req txnengine.AddTableDefReq, resp *txnengine.AddTableDefResp) (err error) {
	if name, ok := c.sysRelationIDs[req.TableID]; ok {
		defer logReq("catalog", req, meta, resp, &err)()
		resp.ErrReadOnly.Why = fmt.Sprintf("%s is system table", name)
		return nil
	}
	return c.upstream.HandleAddTableDef(meta, req, resp)
}

func (c *CatalogHandler) HandleClose() error {
	return c.upstream.HandleClose()
}

func (c *CatalogHandler) HandleCloseTableIter(meta txn.TxnMeta, req txnengine.CloseTableIterReq, resp *txnengine.CloseTableIterResp) (err error) {

	c.iterators.Lock()
	v, ok := c.iterators.Map[req.IterID]
	c.iterators.Unlock()
	if ok {
		defer logReq("catalog", req, meta, resp, &err)()
		switch v := v.(type) {
		case *Iter[Text, DatabaseRow]:
			if err := v.TableIter.Close(); err != nil {
				return err
			}
		case *Iter[Text, RelationRow]:
			if err := v.TableIter.Close(); err != nil {
				return err
			}
		case *Iter[Text, AttributeRow]:
			if err := v.TableIter.Close(); err != nil {
				return err
			}
		default:
			panic(fmt.Errorf("fixme: %T", v))
		}
		c.iterators.Lock()
		delete(c.iterators.Map, req.IterID)
		c.iterators.Unlock()
		return nil
	}

	return c.upstream.HandleCloseTableIter(meta, req, resp)
}

func (c *CatalogHandler) HandleCommit(meta txn.TxnMeta) error {
	err := c.upstream.HandleCommit(meta)
	err = toTAEError(err)
	return err
}

func (c *CatalogHandler) HandleCommitting(meta txn.TxnMeta) error {
	return c.upstream.HandleCommitting(meta)
}

func (c *CatalogHandler) HandleCreateDatabase(meta txn.TxnMeta, req txnengine.CreateDatabaseReq, resp *txnengine.CreateDatabaseResp) (err error) {

	tx := c.upstream.getTx(meta)
	if err := c.upstream.ensureAccount(tx, req.AccessInfo); err != nil {
		return err
	}

	if req.Name == catalog.SystemDBName {
		defer logReq("catalog", req, meta, resp, &err)()
		resp.ErrReadOnly.Why = req.Name + " is system database"
		return nil
	}
	return c.upstream.HandleCreateDatabase(meta, req, resp)
}

func (c *CatalogHandler) HandleCreateRelation(meta txn.TxnMeta, req txnengine.CreateRelationReq, resp *txnengine.CreateRelationResp) (err error) {
	return c.upstream.HandleCreateRelation(meta, req, resp)
}

func (c *CatalogHandler) HandleDelTableDef(meta txn.TxnMeta, req txnengine.DelTableDefReq, resp *txnengine.DelTableDefResp) (err error) {
	if name, ok := c.sysRelationIDs[req.TableID]; ok {
		defer logReq("catalog", req, meta, resp, &err)()
		resp.ErrReadOnly.Why = fmt.Sprintf("%s is system table", name)
		return nil
	}
	return c.upstream.HandleDelTableDef(meta, req, resp)
}

func (c *CatalogHandler) HandleDelete(meta txn.TxnMeta, req txnengine.DeleteReq, resp *txnengine.DeleteResp) (err error) {
	if name, ok := c.sysRelationIDs[req.TableID]; ok {
		defer logReq("catalog", req, meta, resp, &err)()
		resp.ErrReadOnly.Why = fmt.Sprintf("%s is system table", name)
		return nil
	}
	return c.upstream.HandleDelete(meta, req, resp)
}

func (c *CatalogHandler) HandleDeleteDatabase(meta txn.TxnMeta, req txnengine.DeleteDatabaseReq, resp *txnengine.DeleteDatabaseResp) (err error) {

	tx := c.upstream.getTx(meta)
	if err := c.upstream.ensureAccount(tx, req.AccessInfo); err != nil {
		return err
	}

	if req.Name == catalog.SystemDBName {
		defer logReq("catalog", req, meta, resp, &err)()
		resp.ErrReadOnly.Why = fmt.Sprintf("%s is system database", req.Name)
		return nil
	}
	return c.upstream.HandleDeleteDatabase(meta, req, resp)
}

func (c *CatalogHandler) HandleDeleteRelation(meta txn.TxnMeta, req txnengine.DeleteRelationReq, resp *txnengine.DeleteRelationResp) (err error) {
	if req.DatabaseID == c.dbID {
		for _, name := range c.sysRelationIDs {
			if req.Name == name {
				defer logReq("catalog", req, meta, resp, &err)()
				resp.ErrReadOnly.Why = "can't delete this system table"
				return nil
			}
		}
	}
	return c.upstream.HandleDeleteRelation(meta, req, resp)
}

func (c *CatalogHandler) HandleDestroy() error {
	return c.upstream.HandleDestroy()
}

func (c *CatalogHandler) HandleGetDatabases(meta txn.TxnMeta, req txnengine.GetDatabasesReq, resp *txnengine.GetDatabasesResp) error {

	tx := c.upstream.getTx(meta)
	if err := c.upstream.ensureAccount(tx, req.AccessInfo); err != nil {
		return err
	}

	if err := c.upstream.HandleGetDatabases(meta, req, resp); err != nil {
		return err
	}
	return nil
}

func (c *CatalogHandler) HandleGetPrimaryKeys(meta txn.TxnMeta, req txnengine.GetPrimaryKeysReq, resp *txnengine.GetPrimaryKeysResp) (err error) {
	return c.upstream.HandleGetPrimaryKeys(meta, req, resp)
}

func (c *CatalogHandler) HandleGetRelations(meta txn.TxnMeta, req txnengine.GetRelationsReq, resp *txnengine.GetRelationsResp) (err error) {
	return c.upstream.HandleGetRelations(meta, req, resp)
}

func (c *CatalogHandler) HandleGetTableDefs(meta txn.TxnMeta, req txnengine.GetTableDefsReq, resp *txnengine.GetTableDefsResp) (err error) {
	return c.upstream.HandleGetTableDefs(meta, req, resp)
}

func (c *CatalogHandler) HandleGetHiddenKeys(meta txn.TxnMeta, req txnengine.GetHiddenKeysReq, resp *txnengine.GetHiddenKeysResp) (err error) {
	return c.upstream.HandleGetHiddenKeys(meta, req, resp)
}

func (c *CatalogHandler) HandleNewTableIter(meta txn.TxnMeta, req txnengine.NewTableIterReq, resp *txnengine.NewTableIterResp) (err error) {

	if name, ok := c.sysRelationIDs[req.TableID]; ok {
		defer logReq("catalog", req, meta, resp, &err)()
		tx := c.upstream.getTx(meta)

		attrsMap := make(map[string]*AttributeRow)
		if err := c.upstream.iterRelationAttributes(
			tx, req.TableID,
			func(_ Text, row *AttributeRow) error {
				attrsMap[row.Name] = row
				return nil
			},
		); err != nil {
			return err
		}

		var iter any
		switch name {
		case catalog.SystemTable_DB_Name:
			tableIter := c.upstream.databases.NewIter(tx)
			iter = &Iter[Text, DatabaseRow]{
				TableIter: tableIter,
				AttrsMap:  attrsMap,
				nextFunc:  tableIter.First,
			}
		case catalog.SystemTable_Table_Name:
			tableIter := c.upstream.relations.NewIter(tx)
			iter = &Iter[Text, RelationRow]{
				TableIter: tableIter,
				AttrsMap:  attrsMap,
				nextFunc:  tableIter.First,
			}
		case catalog.SystemTable_Columns_Name:
			tableIter := c.upstream.attributes.NewIter(tx)
			iter = &Iter[Text, AttributeRow]{
				TableIter: tableIter,
				AttrsMap:  attrsMap,
				nextFunc:  tableIter.First,
			}
		default:
			panic(fmt.Errorf("fixme: %s", name))
		}

		id := uuid.NewString()
		resp.IterID = id
		c.iterators.Lock()
		c.iterators.Map[id] = iter
		c.iterators.Unlock()

		return nil
	}

	return c.upstream.HandleNewTableIter(meta, req, resp)
}

func (c *CatalogHandler) HandleOpenDatabase(meta txn.TxnMeta, req txnengine.OpenDatabaseReq, resp *txnengine.OpenDatabaseResp) (err error) {

	tx := c.upstream.getTx(meta)
	if err := c.upstream.ensureAccount(tx, req.AccessInfo); err != nil {
		return err
	}

	return c.upstream.HandleOpenDatabase(meta, req, resp)
}

func (c *CatalogHandler) HandleOpenRelation(meta txn.TxnMeta, req txnengine.OpenRelationReq, resp *txnengine.OpenRelationResp) (err error) {
	return c.upstream.HandleOpenRelation(meta, req, resp)
}

func (c *CatalogHandler) HandlePrepare(meta txn.TxnMeta) (timestamp.Timestamp, error) {
	return c.upstream.HandlePrepare(meta)
}

func (c *CatalogHandler) HandleRead(meta txn.TxnMeta, req txnengine.ReadReq, resp *txnengine.ReadResp) (err error) {
	tx := c.upstream.getTx(meta)
	resp.SetHeap(c.upstream.mheap)

	c.iterators.Lock()
	v, ok := c.iterators.Map[req.IterID]
	c.iterators.Unlock()
	if ok {
		defer logReq("catalog", req, meta, resp, &err)()

		b := batch.New(false, req.ColNames)
		maxRows := 4096
		rows := 0

		handleRow := func(
			row NamedRow,
		) (bool, error) {
			if err := appendNamedRow(
				tx,
				c.upstream.mheap,
				b,
				row,
			); err != nil {
				return true, err
			}
			rows++
			if rows >= maxRows {
				return true, nil
			}
			return false, nil
		}

		switch iter := v.(type) {

		case *Iter[Text, DatabaseRow]:
			for i, name := range req.ColNames {
				b.Vecs[i] = vector.New(iter.AttrsMap[name].Type)
			}

			fn := iter.TableIter.Next
			if iter.nextFunc != nil {
				fn = iter.nextFunc
				iter.nextFunc = nil
			}
			for ok := fn(); ok; ok = iter.TableIter.Next() {
				_, row, err := iter.TableIter.Read()
				if err != nil {
					return err
				}
				if end, err := handleRow(row); err != nil {
					return err
				} else if end {
					return nil
				}
			}

		case *Iter[Text, RelationRow]:
			for i, name := range req.ColNames {
				b.Vecs[i] = vector.New(iter.AttrsMap[name].Type)
			}

			fn := iter.TableIter.Next
			if iter.nextFunc != nil {
				fn = iter.nextFunc
				iter.nextFunc = nil
			}
			for ok := fn(); ok; ok = iter.TableIter.Next() {
				_, row, err := iter.TableIter.Read()
				if err != nil {
					return err
				}
				row.handler = c.upstream
				if end, err := handleRow(row); err != nil {
					return err
				} else if end {
					return nil
				}
			}

		case *Iter[Text, AttributeRow]:
			for i, name := range req.ColNames {
				b.Vecs[i] = vector.New(iter.AttrsMap[name].Type)
			}

			fn := iter.TableIter.Next
			if iter.nextFunc != nil {
				fn = iter.nextFunc
				iter.nextFunc = nil
			}
			for ok := fn(); ok; ok = iter.TableIter.Next() {
				_, row, err := iter.TableIter.Read()
				if err != nil {
					return err
				}
				if row.IsHidden {
					continue
				}
				row.handler = c.upstream
				if end, err := handleRow(row); err != nil {
					return err
				} else if end {
					return nil
				}
			}

		default:
			panic(fmt.Errorf("fixme: %T", v))
		}

		if rows > 0 {
			b.InitZsOne(rows)
			for _, vec := range b.Vecs {
				nulls.TryExpand(vec.GetNulls(), rows)
			}
			resp.Batch = b
		}

		return nil
	}

	return c.upstream.HandleRead(meta, req, resp)
}

func (c *CatalogHandler) HandleRollback(meta txn.TxnMeta) error {
	return c.upstream.HandleRollback(meta)
}

func (c *CatalogHandler) HandleStartRecovery(ch chan txn.TxnMeta) {
	c.upstream.HandleStartRecovery(ch)
}

func (c *CatalogHandler) HandleTruncate(meta txn.TxnMeta, req txnengine.TruncateReq, resp *txnengine.TruncateResp) (err error) {
	if name, ok := c.sysRelationIDs[req.TableID]; ok {
		defer logReq("catalog", req, meta, resp, &err)()
		resp.ErrReadOnly.Why = fmt.Sprintf("%s is system table", name)
	}
	return c.upstream.HandleTruncate(meta, req, resp)
}

func (c *CatalogHandler) HandleUpdate(meta txn.TxnMeta, req txnengine.UpdateReq, resp *txnengine.UpdateResp) (err error) {
	if name, ok := c.sysRelationIDs[req.TableID]; ok {
		defer logReq("catalog", req, meta, resp, &err)()
		resp.ErrReadOnly.Why = fmt.Sprintf("%s is system table", name)
	}
	return c.upstream.HandleUpdate(meta, req, resp)
}

func (c *CatalogHandler) HandleWrite(meta txn.TxnMeta, req txnengine.WriteReq, resp *txnengine.WriteResp) (err error) {
	if name, ok := c.sysRelationIDs[req.TableID]; ok {
		defer logReq("catalog", req, meta, resp, &err)()
		resp.ErrReadOnly.Why = fmt.Sprintf("%s is system table", name)
	}
	err = c.upstream.HandleWrite(meta, req, resp)
	err = toTAEError(err)
	return
}

func (c *CatalogHandler) HandleTableStats(meta txn.TxnMeta, req txnengine.TableStatsReq, resp *txnengine.TableStatsResp) (err error) {
	return c.upstream.HandleTableStats(meta, req, resp)
}

func toTAEError(err error) error {
	if err == nil {
		return nil
	}
	var dup *ErrPrimaryKeyDuplicated
	if errors.As(err, &dup) {
		err = taedata.ErrDuplicate
	}
	var writeConflict *ErrWriteConflict
	if errors.As(err, &writeConflict) {
		err = txnif.ErrTxnWWConflict
	}
	return err
}
