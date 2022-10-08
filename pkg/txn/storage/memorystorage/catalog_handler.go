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
	"github.com/matrixorigin/matrixone/pkg/txn/storage/memorystorage/memtable"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/memoryengine"
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
		now.Statement = math.MaxInt
		if err := tx.Commit(now); err != nil {
			panic(err)
		}
	}()

	// database
	db := &DatabaseRow{
		ID:        ID(catalog.MO_CATALOG_ID),
		AccountID: 0,
		Name:      []byte(catalog.MO_CATALOG),
	}
	if err := upstream.databases.Insert(tx, db); err != nil {
		panic(err)
	}
	handler.dbID = db.ID

	// relations
	databasesRelRow := &RelationRow{
		ID:         ID(catalog.MO_DATABASE_ID),
		DatabaseID: db.ID,
		Name:       []byte(catalog.MO_DATABASE),
		Type:       memoryengine.RelationTable,
	}
	if err := upstream.relations.Insert(tx, databasesRelRow); err != nil {
		panic(err)
	}
	handler.sysRelationIDs[databasesRelRow.ID] = string(databasesRelRow.Name)

	tablesRelRow := &RelationRow{
		ID:         ID(catalog.MO_TABLES_ID),
		DatabaseID: db.ID,
		Name:       []byte(catalog.MO_TABLES),
		Type:       memoryengine.RelationTable,
	}
	if err := upstream.relations.Insert(tx, tablesRelRow); err != nil {
		panic(err)
	}
	handler.sysRelationIDs[tablesRelRow.ID] = string(tablesRelRow.Name)

	attributesRelRow := &RelationRow{
		ID:         ID(catalog.MO_COLUMNS_ID),
		DatabaseID: db.ID,
		Name:       []byte(catalog.MO_COLUMNS),
		Type:       memoryengine.RelationTable,
	}
	if err := upstream.relations.Insert(tx, attributesRelRow); err != nil {
		panic(err)
	}
	handler.sysRelationIDs[attributesRelRow.ID] = string(attributesRelRow.Name)

	// attributes
	// databases
	for i, def := range catalog.MoDatabaseTableDefs {
		attr, ok := def.(*engine.AttributeDef)
		if !ok {
			continue
		}
		row := &AttributeRow{
			ID:         memoryengine.NewID(),
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
			ID:         memoryengine.NewID(),
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
			ID:         memoryengine.NewID(),
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

func (c *CatalogHandler) HandleAddTableDef(ctx context.Context, meta txn.TxnMeta, req memoryengine.AddTableDefReq, resp *memoryengine.AddTableDefResp) (err error) {
	if _, ok := c.sysRelationIDs[req.TableID]; ok {
		defer logReq("catalog", req, meta, resp, &err)()
		return moerr.NewInternalError(
			"read only, db %v, table %v",
			req.DatabaseName,
			req.TableName,
		)
	}
	return c.upstream.HandleAddTableDef(ctx, meta, req, resp)
}

func (c *CatalogHandler) HandleClose(ctx context.Context) error {
	return c.upstream.HandleClose(ctx)
}

func (c *CatalogHandler) HandleCloseTableIter(ctx context.Context, meta txn.TxnMeta, req memoryengine.CloseTableIterReq, resp *memoryengine.CloseTableIterResp) (err error) {

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

	return c.upstream.HandleCloseTableIter(ctx, meta, req, resp)
}

func (c *CatalogHandler) HandleCommit(ctx context.Context, meta txn.TxnMeta) error {
	err := c.upstream.HandleCommit(ctx, meta)
	return err
}

func (c *CatalogHandler) HandleCommitting(ctx context.Context, meta txn.TxnMeta) error {
	return c.upstream.HandleCommitting(ctx, meta)
}

func (c *CatalogHandler) HandleCreateDatabase(ctx context.Context, meta txn.TxnMeta, req memoryengine.CreateDatabaseReq, resp *memoryengine.CreateDatabaseResp) (err error) {

	tx := c.upstream.getTx(meta)
	if err := c.upstream.ensureAccount(tx, req.AccessInfo); err != nil {
		return err
	}

	if req.Name == catalog.MO_CATALOG {
		defer logReq("catalog", req, meta, resp, &err)()
		return moerr.NewDBAlreadyExists(req.Name)
	}
	return c.upstream.HandleCreateDatabase(ctx, meta, req, resp)
}

func (c *CatalogHandler) HandleCreateRelation(ctx context.Context, meta txn.TxnMeta, req memoryengine.CreateRelationReq, resp *memoryengine.CreateRelationResp) (err error) {
	return c.upstream.HandleCreateRelation(ctx, meta, req, resp)
}

func (c *CatalogHandler) HandleDelTableDef(ctx context.Context, meta txn.TxnMeta, req memoryengine.DelTableDefReq, resp *memoryengine.DelTableDefResp) (err error) {
	if _, ok := c.sysRelationIDs[req.TableID]; ok {
		defer logReq("catalog", req, meta, resp, &err)()
		return moerr.NewInternalError(
			"read only, db %v, table %v",
			req.DatabaseName,
			req.TableName,
		)
	}
	return c.upstream.HandleDelTableDef(ctx, meta, req, resp)
}

func (c *CatalogHandler) HandleDelete(ctx context.Context, meta txn.TxnMeta, req memoryengine.DeleteReq, resp *memoryengine.DeleteResp) (err error) {
	if _, ok := c.sysRelationIDs[req.TableID]; ok {
		defer logReq("catalog", req, meta, resp, &err)()
		return moerr.NewInternalError(
			"read only, db %v, table %v",
			req.DatabaseName,
			req.TableName,
		)
	}
	return c.upstream.HandleDelete(ctx, meta, req, resp)
}

func (c *CatalogHandler) HandleDeleteDatabase(ctx context.Context, meta txn.TxnMeta, req memoryengine.DeleteDatabaseReq, resp *memoryengine.DeleteDatabaseResp) (err error) {

	tx := c.upstream.getTx(meta)
	if err := c.upstream.ensureAccount(tx, req.AccessInfo); err != nil {
		return err
	}

	if req.Name == catalog.MO_CATALOG {
		defer logReq("catalog", req, meta, resp, &err)()
		return moerr.NewBadDB(req.Name)
	}
	return c.upstream.HandleDeleteDatabase(ctx, meta, req, resp)
}

func (c *CatalogHandler) HandleDeleteRelation(ctx context.Context, meta txn.TxnMeta, req memoryengine.DeleteRelationReq, resp *memoryengine.DeleteRelationResp) (err error) {
	if req.DatabaseID == c.dbID {
		for _, name := range c.sysRelationIDs {
			if req.Name == name {
				defer logReq("catalog", req, meta, resp, &err)()
				return moerr.NewInternalError(
					"read only, db %v, table %v",
					req.DatabaseName,
					req.Name,
				)
			}
		}
	}
	return c.upstream.HandleDeleteRelation(ctx, meta, req, resp)
}

func (c *CatalogHandler) HandleDestroy(ctx context.Context) error {
	return c.upstream.HandleDestroy(ctx)
}

func (c *CatalogHandler) HandleGetDatabases(ctx context.Context, meta txn.TxnMeta, req memoryengine.GetDatabasesReq, resp *memoryengine.GetDatabasesResp) error {

	tx := c.upstream.getTx(meta)
	if err := c.upstream.ensureAccount(tx, req.AccessInfo); err != nil {
		return err
	}

	if err := c.upstream.HandleGetDatabases(ctx, meta, req, resp); err != nil {
		return err
	}
	return nil
}

func (c *CatalogHandler) HandleGetPrimaryKeys(ctx context.Context, meta txn.TxnMeta, req memoryengine.GetPrimaryKeysReq, resp *memoryengine.GetPrimaryKeysResp) (err error) {
	return c.upstream.HandleGetPrimaryKeys(ctx, meta, req, resp)
}

func (c *CatalogHandler) HandleGetRelations(ctx context.Context, meta txn.TxnMeta, req memoryengine.GetRelationsReq, resp *memoryengine.GetRelationsResp) (err error) {
	return c.upstream.HandleGetRelations(ctx, meta, req, resp)
}

func (c *CatalogHandler) HandleGetTableDefs(ctx context.Context, meta txn.TxnMeta, req memoryengine.GetTableDefsReq, resp *memoryengine.GetTableDefsResp) (err error) {
	return c.upstream.HandleGetTableDefs(ctx, meta, req, resp)
}

func (c *CatalogHandler) HandleGetHiddenKeys(ctx context.Context, meta txn.TxnMeta, req memoryengine.GetHiddenKeysReq, resp *memoryengine.GetHiddenKeysResp) (err error) {
	return c.upstream.HandleGetHiddenKeys(ctx, meta, req, resp)
}

func (c *CatalogHandler) HandleNewTableIter(ctx context.Context, meta txn.TxnMeta, req memoryengine.NewTableIterReq, resp *memoryengine.NewTableIterResp) (err error) {

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

		id := memoryengine.NewID()
		resp.IterID = id
		c.iterators.Lock()
		c.iterators.Map[id] = iter
		c.iterators.Unlock()

		return nil
	}

	return c.upstream.HandleNewTableIter(ctx, meta, req, resp)
}

func (c *CatalogHandler) HandleOpenDatabase(ctx context.Context, meta txn.TxnMeta, req memoryengine.OpenDatabaseReq, resp *memoryengine.OpenDatabaseResp) (err error) {

	tx := c.upstream.getTx(meta)
	if err := c.upstream.ensureAccount(tx, req.AccessInfo); err != nil {
		return err
	}

	return c.upstream.HandleOpenDatabase(ctx, meta, req, resp)
}

func (c *CatalogHandler) HandleOpenRelation(ctx context.Context, meta txn.TxnMeta, req memoryengine.OpenRelationReq, resp *memoryengine.OpenRelationResp) (err error) {
	return c.upstream.HandleOpenRelation(ctx, meta, req, resp)
}

func (c *CatalogHandler) HandlePrepare(ctx context.Context, meta txn.TxnMeta) (timestamp.Timestamp, error) {
	return c.upstream.HandlePrepare(ctx, meta)
}

func (c *CatalogHandler) HandleRead(ctx context.Context, meta txn.TxnMeta, req memoryengine.ReadReq, resp *memoryengine.ReadResp) (err error) {
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
				0,
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

	return c.upstream.HandleRead(ctx, meta, req, resp)
}

func (c *CatalogHandler) HandleRollback(ctx context.Context, meta txn.TxnMeta) error {
	return c.upstream.HandleRollback(ctx, meta)
}

func (c *CatalogHandler) HandleStartRecovery(ctx context.Context, ch chan txn.TxnMeta) {
	c.upstream.HandleStartRecovery(ctx, ch)
}

func (c *CatalogHandler) HandleTruncate(ctx context.Context, meta txn.TxnMeta, req memoryengine.TruncateReq, resp *memoryengine.TruncateResp) (err error) {
	if _, ok := c.sysRelationIDs[req.TableID]; ok {
		defer logReq("catalog", req, meta, resp, &err)()
		return moerr.NewInternalError(
			"read only, db %v, table %v",
			req.DatabaseName,
			req.TableName,
		)
	}
	return c.upstream.HandleTruncate(ctx, meta, req, resp)
}

func (c *CatalogHandler) HandleUpdate(ctx context.Context, meta txn.TxnMeta, req memoryengine.UpdateReq, resp *memoryengine.UpdateResp) (err error) {
	if _, ok := c.sysRelationIDs[req.TableID]; ok {
		defer logReq("catalog", req, meta, resp, &err)()
		return moerr.NewInternalError(
			"read only, db %v, table %v",
			req.DatabaseName,
			req.TableName,
		)
	}
	return c.upstream.HandleUpdate(ctx, meta, req, resp)
}

func (c *CatalogHandler) HandleWrite(ctx context.Context, meta txn.TxnMeta, req memoryengine.WriteReq, resp *memoryengine.WriteResp) (err error) {
	if _, ok := c.sysRelationIDs[req.TableID]; ok {
		defer logReq("catalog", req, meta, resp, &err)()
		return moerr.NewInternalError(
			"read only, db %v, table %v",
			req.DatabaseName,
			req.TableName,
		)
	}
	err = c.upstream.HandleWrite(ctx, meta, req, resp)
	return
}

func (c *CatalogHandler) HandleTableStats(ctx context.Context, meta txn.TxnMeta, req memoryengine.TableStatsReq, resp *memoryengine.TableStatsResp) (err error) {
	return c.upstream.HandleTableStats(ctx, meta, req, resp)
}
