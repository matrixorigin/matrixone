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
	"math"
	"sync"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/storage/txn/memtable"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	txnengine "github.com/matrixorigin/matrixone/pkg/vm/engine/txn"
)

// CatalogHandler handles read-only requests for catalog
type CatalogHandler struct {
	upstream       *MemHandler
	dbID           ID
	sysRelationIDs map[ID]string
	iterators      struct {
		sync.Mutex
		Map map[ID]any // id -> Iterator
	}
}

var _ Handler = new(CatalogHandler)

func NewCatalogHandler(upstream *MemHandler) *CatalogHandler {

	handler := &CatalogHandler{
		upstream:       upstream,
		sysRelationIDs: make(map[ID]string),
	}
	handler.iterators.Map = make(map[ID]any)

	now := Time{
		Timestamp: timestamp.Timestamp{
			PhysicalTime: math.MinInt64,
		},
	}
	tx := memtable.NewTransaction(uuid.NewString(), now, memtable.SnapshotIsolation)
	defer func() {
		if err := tx.Commit(); err != nil {
			panic(err)
		}
	}()

	// database
	db := &DatabaseRow{
		ID:        ID(catalog.MO_CATALOG_ID),
		AccountID: 0,
		Name:      catalog.MO_CATALOG,
	}
	if err := upstream.databases.Insert(tx, db); err != nil {
		panic(err)
	}
	handler.dbID = db.ID

	// relations
	databasesRelRow := &RelationRow{
		ID:         ID(catalog.MO_DATABASE_ID),
		DatabaseID: db.ID,
		Name:       catalog.MO_DATABASE,
		Type:       txnengine.RelationTable,
	}
	if err := upstream.relations.Insert(tx, databasesRelRow); err != nil {
		panic(err)
	}
	handler.sysRelationIDs[databasesRelRow.ID] = databasesRelRow.Name

	tablesRelRow := &RelationRow{
		ID:         ID(catalog.MO_TABLES_ID),
		DatabaseID: db.ID,
		Name:       catalog.MO_TABLES,
		Type:       txnengine.RelationTable,
	}
	if err := upstream.relations.Insert(tx, tablesRelRow); err != nil {
		panic(err)
	}
	handler.sysRelationIDs[tablesRelRow.ID] = tablesRelRow.Name

	attributesRelRow := &RelationRow{
		ID:         ID(catalog.MO_COLUMNS_ID),
		DatabaseID: db.ID,
		Name:       catalog.MO_COLUMNS,
		Type:       txnengine.RelationTable,
	}
	if err := upstream.relations.Insert(tx, attributesRelRow); err != nil {
		panic(err)
	}
	handler.sysRelationIDs[attributesRelRow.ID] = attributesRelRow.Name

	// attributes
	// databases
	for i, def := range catalog.MoDatabaseTableDefs {
		attr, ok := def.(*engine.AttributeDef)
		if !ok {
			continue
		}
		row := &AttributeRow{
			ID:         txnengine.NewID(),
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
	for i, def := range catalog.MoTablesTableDefs {
		attr, ok := def.(*engine.AttributeDef)
		if !ok {
			continue
		}
		row := &AttributeRow{
			ID:         txnengine.NewID(),
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
	for i, def := range catalog.MoColumnsTableDefs {
		attr, ok := def.(*engine.AttributeDef)
		if !ok {
			continue
		}
		row := &AttributeRow{
			ID:         txnengine.NewID(),
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
	if _, ok := c.sysRelationIDs[req.TableID]; ok {
		defer logReq("catalog", req, meta, resp, &err)()
		return moerr.NewNoSuchTable(req.DatabaseName, req.TableName)
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
		case *DatabaseRowIter:
			if err := v.TableIter.Close(); err != nil {
				return err
			}
		case *RelationRowIter:
			if err := v.TableIter.Close(); err != nil {
				return err
			}
		case *AttributeRowIter:
			if err := v.TableIter.Close(); err != nil {
				return err
			}
		default:
			panic(fmt.Sprintf("fixme: %T", v))
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

	if req.Name == catalog.MO_CATALOG {
		defer logReq("catalog", req, meta, resp, &err)()
		return moerr.NewDBAlreadyExists(req.Name)
	}
	return c.upstream.HandleCreateDatabase(meta, req, resp)
}

func (c *CatalogHandler) HandleCreateRelation(meta txn.TxnMeta, req txnengine.CreateRelationReq, resp *txnengine.CreateRelationResp) (err error) {
	return c.upstream.HandleCreateRelation(meta, req, resp)
}

func (c *CatalogHandler) HandleDelTableDef(meta txn.TxnMeta, req txnengine.DelTableDefReq, resp *txnengine.DelTableDefResp) (err error) {
	if _, ok := c.sysRelationIDs[req.TableID]; ok {
		defer logReq("catalog", req, meta, resp, &err)()
		return moerr.NewNoSuchTable(req.DatabaseName, req.TableName)
	}
	return c.upstream.HandleDelTableDef(meta, req, resp)
}

func (c *CatalogHandler) HandleDelete(meta txn.TxnMeta, req txnengine.DeleteReq, resp *txnengine.DeleteResp) (err error) {
	if _, ok := c.sysRelationIDs[req.TableID]; ok {
		defer logReq("catalog", req, meta, resp, &err)()
		return moerr.NewNoSuchTable(req.DatabaseName, req.TableName)
	}
	return c.upstream.HandleDelete(meta, req, resp)
}

func (c *CatalogHandler) HandleDeleteDatabase(meta txn.TxnMeta, req txnengine.DeleteDatabaseReq, resp *txnengine.DeleteDatabaseResp) (err error) {

	tx := c.upstream.getTx(meta)
	if err := c.upstream.ensureAccount(tx, req.AccessInfo); err != nil {
		return err
	}

	if req.Name == catalog.MO_CATALOG {
		defer logReq("catalog", req, meta, resp, &err)()
		return moerr.NewBadDB(req.Name)
	}
	return c.upstream.HandleDeleteDatabase(meta, req, resp)
}

func (c *CatalogHandler) HandleDeleteRelation(meta txn.TxnMeta, req txnengine.DeleteRelationReq, resp *txnengine.DeleteRelationResp) (err error) {
	if req.DatabaseID == c.dbID {
		for _, name := range c.sysRelationIDs {
			if req.Name == name {
				defer logReq("catalog", req, meta, resp, &err)()
				return moerr.NewNoSuchTable(req.DatabaseName, req.Name)
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
			func(_ ID, row *AttributeRow) error {
				attrsMap[row.Name] = row
				return nil
			},
		); err != nil {
			return err
		}

		var iter any
		switch name {
		case catalog.MO_DATABASE:
			tableIter := c.upstream.databases.NewIter(tx)
			iter = &DatabaseRowIter{
				TableIter: tableIter,
				AttrsMap:  attrsMap,
				nextFunc:  tableIter.First,
			}
		case catalog.MO_TABLES:
			tableIter := c.upstream.relations.NewIter(tx)
			iter = &RelationRowIter{
				TableIter: tableIter,
				AttrsMap:  attrsMap,
				nextFunc:  tableIter.First,
			}
		case catalog.MO_COLUMNS:
			tableIter := c.upstream.attributes.NewIter(tx)
			iter = &AttributeRowIter{
				TableIter: tableIter,
				AttrsMap:  attrsMap,
				nextFunc:  tableIter.First,
			}
		default:
			panic(fmt.Sprintf("fixme: %s", name))
		}

		id := txnengine.NewID()
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
				c.upstream,
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

		case *DatabaseRowIter:
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

		case *RelationRowIter:
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

		case *AttributeRowIter:
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
				if end, err := handleRow(row); err != nil {
					return err
				} else if end {
					return nil
				}
			}

		default:
			panic(fmt.Sprintf("fixme: %T", v))
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
	if _, ok := c.sysRelationIDs[req.TableID]; ok {
		defer logReq("catalog", req, meta, resp, &err)()
		return moerr.NewNoSuchTable(req.DatabaseName, req.TableName)
	}
	return c.upstream.HandleTruncate(meta, req, resp)
}

func (c *CatalogHandler) HandleUpdate(meta txn.TxnMeta, req txnengine.UpdateReq, resp *txnengine.UpdateResp) (err error) {
	if _, ok := c.sysRelationIDs[req.TableID]; ok {
		defer logReq("catalog", req, meta, resp, &err)()
		return moerr.NewNoSuchTable(req.DatabaseName, req.TableName)
	}
	return c.upstream.HandleUpdate(meta, req, resp)
}

func (c *CatalogHandler) HandleWrite(meta txn.TxnMeta, req txnengine.WriteReq, resp *txnengine.WriteResp) (err error) {
	if _, ok := c.sysRelationIDs[req.TableID]; ok {
		defer logReq("catalog", req, meta, resp, &err)()
		return moerr.NewNoSuchTable(req.DatabaseName, req.TableName)
	}
	err = c.upstream.HandleWrite(meta, req, resp)
	return
}

func (c *CatalogHandler) HandleTableStats(meta txn.TxnMeta, req txnengine.TableStatsReq, resp *txnengine.TableStatsResp) (err error) {
	return c.upstream.HandleTableStats(meta, req, resp)
}
