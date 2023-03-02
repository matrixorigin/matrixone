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

package disttae

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/txn/client"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/cache"
)

func newDB(dnList []DNStore) *DB {
	dnMap := make(map[string]int)
	for i := range dnList {
		dnMap[dnList[i].ServiceID] = i
	}
	db := &DB{
		dnMap:      dnMap,
		metaTables: make(map[string]Partitions),
		partitions: make(map[[2]uint64]Partitions),
	}
	return db
}

// init is used to insert some data that will not be synchronized by logtail.
func (db *DB) init(ctx context.Context, m *mpool.MPool, catalogCache *cache.CatalogCache) error {
	db.Lock()
	defer db.Unlock()
	{
		parts := make(Partitions, len(db.dnMap))
		for i := range parts {
			parts[i] = NewPartition(nil)
		}
		db.partitions[[2]uint64{catalog.MO_CATALOG_ID, catalog.MO_DATABASE_ID}] = parts
	}
	{
		parts := make(Partitions, len(db.dnMap))
		for i := range parts {
			parts[i] = NewPartition(nil)
		}
		db.partitions[[2]uint64{catalog.MO_CATALOG_ID, catalog.MO_TABLES_ID}] = parts
	}
	{
		parts := make(Partitions, len(db.dnMap))
		for i := range parts {
			parts[i] = NewPartition(nil)
		}
		db.partitions[[2]uint64{catalog.MO_CATALOG_ID, catalog.MO_COLUMNS_ID}] = parts
	}
	{ // mo_catalog
		part := db.partitions[[2]uint64{catalog.MO_CATALOG_ID, catalog.MO_DATABASE_ID}][0]
		bat, err := genCreateDatabaseTuple("", 0, 0, 0, catalog.MO_CATALOG, catalog.MO_CATALOG_ID, m)
		if err != nil {
			return err
		}
		ibat, err := genInsertBatch(bat, m)
		if err != nil {
			bat.Clean(m)
			return err
		}
		if err := part.Insert(ctx, MO_PRIMARY_OFF, ibat, false); err != nil {
			bat.Clean(m)
			return err
		}
		catalogCache.InsertDatabase(bat)
		bat.Clean(m)
	}
	{ // mo_database
		part := db.partitions[[2]uint64{catalog.MO_CATALOG_ID, catalog.MO_TABLES_ID}][0]
		cols, err := genColumns(0, catalog.MO_DATABASE, catalog.MO_CATALOG, catalog.MO_DATABASE_ID,
			catalog.MO_CATALOG_ID, catalog.MoDatabaseTableDefs)
		if err != nil {
			return err
		}
		tbl := new(table)
		tbl.relKind = catalog.SystemOrdinaryRel
		bat, err := genCreateTableTuple(tbl, "", 0, 0, 0,
			catalog.MO_DATABASE, catalog.MO_DATABASE_ID,
			catalog.MO_CATALOG_ID, catalog.MO_CATALOG, m)
		if err != nil {
			return err
		}
		ibat, err := genInsertBatch(bat, m)
		if err != nil {
			bat.Clean(m)
			return err
		}
		if err := part.Insert(ctx, MO_PRIMARY_OFF+catalog.MO_TABLES_REL_ID_IDX, ibat, false); err != nil {
			bat.Clean(m)
			return err
		}
		catalogCache.InsertTable(bat)
		bat.Clean(m)
		part = db.partitions[[2]uint64{catalog.MO_CATALOG_ID, catalog.MO_COLUMNS_ID}][0]
		bat = batch.NewWithSize(len(catalog.MoColumnsSchema))
		bat.Attrs = append(bat.Attrs, catalog.MoColumnsSchema...)
		bat.SetZs(len(cols), m)
		for _, col := range cols {
			bat0, err := genCreateColumnTuple(col, m)
			if err != nil {
				return err
			}
			if bat.Vecs[0] == nil {
				for i, vec := range bat0.Vecs {
					bat.Vecs[i] = vector.New(vec.GetType())
				}
			}
			for i, vec := range bat0.Vecs {
				if err := vector.UnionOne(bat.Vecs[i], vec, 0, m); err != nil {
					bat.Clean(m)
					bat0.Clean(m)
					return err
				}
			}
			bat0.Clean(m)
		}
		ibat, err = genInsertBatch(bat, m)
		if err != nil {
			bat.Clean(m)
			return err
		}
		if err := part.Insert(ctx, MO_PRIMARY_OFF+catalog.MO_COLUMNS_ATT_UNIQ_NAME_IDX,
			ibat, false); err != nil {
			bat.Clean(m)
			return err
		}
		catalogCache.InsertColumns(bat)
		bat.Clean(m)
	}
	{ // mo_tables
		part := db.partitions[[2]uint64{catalog.MO_CATALOG_ID, catalog.MO_TABLES_ID}][0]
		cols, err := genColumns(0, catalog.MO_TABLES, catalog.MO_CATALOG, catalog.MO_TABLES_ID,
			catalog.MO_CATALOG_ID, catalog.MoTablesTableDefs)
		if err != nil {
			return err
		}
		tbl := new(table)
		tbl.relKind = catalog.SystemOrdinaryRel
		bat, err := genCreateTableTuple(tbl, "", 0, 0, 0, catalog.MO_TABLES, catalog.MO_TABLES_ID,
			catalog.MO_CATALOG_ID, catalog.MO_CATALOG, m)
		if err != nil {
			return err
		}
		ibat, err := genInsertBatch(bat, m)
		if err != nil {
			bat.Clean(m)
			return err
		}
		if err := part.Insert(ctx, MO_PRIMARY_OFF+catalog.MO_TABLES_REL_ID_IDX, ibat, false); err != nil {
			bat.Clean(m)
			return err
		}
		catalogCache.InsertTable(bat)
		bat.Clean(m)
		part = db.partitions[[2]uint64{catalog.MO_CATALOG_ID, catalog.MO_COLUMNS_ID}][0]
		bat = batch.NewWithSize(len(catalog.MoColumnsSchema))
		bat.Attrs = append(bat.Attrs, catalog.MoColumnsSchema...)
		bat.SetZs(len(cols), m)
		for _, col := range cols {
			bat0, err := genCreateColumnTuple(col, m)
			if err != nil {
				return err
			}
			if bat.Vecs[0] == nil {
				for i, vec := range bat0.Vecs {
					bat.Vecs[i] = vector.New(vec.GetType())
				}
			}
			for i, vec := range bat0.Vecs {
				if err := vector.UnionOne(bat.Vecs[i], vec, 0, m); err != nil {
					bat.Clean(m)
					bat0.Clean(m)
					return err
				}
			}
			bat0.Clean(m)
		}
		ibat, err = genInsertBatch(bat, m)
		if err != nil {
			bat.Clean(m)
			return err
		}
		if err := part.Insert(ctx, MO_PRIMARY_OFF+catalog.MO_COLUMNS_ATT_UNIQ_NAME_IDX,
			ibat, false); err != nil {
			bat.Clean(m)
			return err
		}
		catalogCache.InsertColumns(bat)
		bat.Clean(m)
	}
	{ // mo_columns
		part := db.partitions[[2]uint64{catalog.MO_CATALOG_ID, catalog.MO_TABLES_ID}][0]
		cols, err := genColumns(0, catalog.MO_COLUMNS, catalog.MO_CATALOG, catalog.MO_COLUMNS_ID,
			catalog.MO_CATALOG_ID, catalog.MoColumnsTableDefs)
		if err != nil {
			return err
		}
		tbl := new(table)
		tbl.relKind = catalog.SystemOrdinaryRel
		bat, err := genCreateTableTuple(tbl, "", 0, 0, 0, catalog.MO_COLUMNS, catalog.MO_COLUMNS_ID,
			catalog.MO_CATALOG_ID, catalog.MO_CATALOG, m)
		if err != nil {
			return err
		}
		ibat, err := genInsertBatch(bat, m)
		if err != nil {
			bat.Clean(m)
			return err
		}
		if err := part.Insert(ctx, MO_PRIMARY_OFF+catalog.MO_TABLES_REL_ID_IDX, ibat, false); err != nil {
			bat.Clean(m)
			return err
		}
		catalogCache.InsertTable(bat)
		bat.Clean(m)
		part = db.partitions[[2]uint64{catalog.MO_CATALOG_ID, catalog.MO_COLUMNS_ID}][0]
		bat = batch.NewWithSize(len(catalog.MoColumnsSchema))
		bat.Attrs = append(bat.Attrs, catalog.MoColumnsSchema...)
		bat.SetZs(len(cols), m)
		for _, col := range cols {
			bat0, err := genCreateColumnTuple(col, m)
			if err != nil {
				return err
			}
			if bat.Vecs[0] == nil {
				for i, vec := range bat0.Vecs {
					bat.Vecs[i] = vector.New(vec.GetType())
				}
			}
			for i, vec := range bat0.Vecs {
				if err := vector.UnionOne(bat.Vecs[i], vec, 0, m); err != nil {
					bat.Clean(m)
					bat0.Clean(m)
					return err
				}
			}
			bat0.Clean(m)
		}
		ibat, err = genInsertBatch(bat, m)
		if err != nil {
			bat.Clean(m)
			return err
		}
		if err := part.Insert(ctx, MO_PRIMARY_OFF+catalog.MO_COLUMNS_ATT_UNIQ_NAME_IDX,
			ibat, false); err != nil {
			bat.Clean(m)
			return err
		}
		catalogCache.InsertColumns(bat)
		bat.Clean(m)
	}
	return nil
}

func (db *DB) getMetaPartitions(name string) Partitions {
	db.Lock()
	parts, ok := db.metaTables[name]
	if !ok { // create a new table
		parts = make(Partitions, len(db.dnMap))
		for i := range parts {
			parts[i] = NewPartition(nil)
		}
		db.metaTables[name] = parts
	}
	db.Unlock()
	return parts

}

func (db *DB) getPartitions(databaseId, tableId uint64) Partitions {
	db.Lock()
	parts, ok := db.partitions[[2]uint64{databaseId, tableId}]
	if !ok { // create a new table
		parts = make(Partitions, len(db.dnMap))
		for i := range parts {
			parts[i] = NewPartition(nil)
		}
		db.partitions[[2]uint64{databaseId, tableId}] = parts
	}
	db.Unlock()
	return parts
}

func (db *DB) UpdateOfPush(ctx context.Context, databaseId, tableId uint64, ts timestamp.Timestamp) error {
	return db.cnE.tryToGetTableLogTail(ctx, databaseId, tableId)
}

func (db *DB) UpdateOfPull(ctx context.Context, dnList []DNStore, tbl *table, op client.TxnOperator,
	primaryIdx int, databaseId, tableId uint64, ts timestamp.Timestamp) error {
	db.Lock()
	parts, ok := db.partitions[[2]uint64{databaseId, tableId}]
	if !ok { // create a new table
		parts = make(Partitions, len(db.dnMap))
		for i := range parts {
			parts[i] = NewPartition(nil)
		}
		db.partitions[[2]uint64{databaseId, tableId}] = parts
	}
	db.Unlock()

	for i, dn := range dnList {
		part := parts[db.dnMap[dn.ServiceID]]

		select {
		case <-part.lock:
			if part.ts.Greater(ts) ||
				part.ts.Equal(ts) {
				part.lock <- struct{}{}
				return nil
			}
		case <-ctx.Done():
			return ctx.Err()
		}

		if err := updatePartitionOfPull(
			i, primaryIdx, tbl, ctx, op, db, part, dn,
			genSyncLogTailReq(part.ts, ts, databaseId, tableId),
		); err != nil {
			part.lock <- struct{}{}
			return err
		}

		part.ts = ts
		part.lock <- struct{}{}
	}

	return nil
}
