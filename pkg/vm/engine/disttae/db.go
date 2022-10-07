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

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
)

func newDB(cli client.TxnClient, dnList []DNStore) *DB {
	dnMap := make(map[string]int)
	for i := range dnList {
		dnMap[dnList[i].UUID] = i
	}
	db := &DB{
		cli:    cli,
		dnMap:  dnMap,
		tables: make(map[[2]uint64]Partitions),
	}
	return db
}

// init is used to insert some data that will not be synchronized by logtail.
func (db *DB) init(ctx context.Context, m *mheap.Mheap) error {
	{
		parts := make(Partitions, len(db.dnMap))
		for i := range parts {
			parts[i] = NewPartition()
		}
		db.tables[[2]uint64{catalog.MO_CATALOG_ID, catalog.MO_DATABASE_ID}] = parts
	}
	{
		parts := make(Partitions, len(db.dnMap))
		for i := range parts {
			parts[i] = NewPartition()
		}
		db.tables[[2]uint64{catalog.MO_CATALOG_ID, catalog.MO_TABLES_ID}] = parts
	}
	{
		parts := make(Partitions, len(db.dnMap))
		for i := range parts {
			parts[i] = NewPartition()
		}
		db.tables[[2]uint64{catalog.MO_CATALOG_ID, catalog.MO_COLUMNS_ID}] = parts
	}
	{ // mo_catalog
		part := db.tables[[2]uint64{catalog.MO_CATALOG_ID, catalog.MO_DATABASE_ID}][0]
		bat, err := genCreateDatabaseTuple("", 0, 0, 0, catalog.MO_CATALOG, catalog.MO_CATALOG_ID, m)
		if err != nil {
			return err
		}
		ibat, err := genInsertBatch(bat, m)
		if err != nil {
			bat.Clean(m)
			return err
		}
		if err := part.Insert(ctx, ibat); err != nil {
			bat.Clean(m)
			return err
		}
		bat.Clean(m)
	}
	{ // mo_database
		part := db.tables[[2]uint64{catalog.MO_CATALOG_ID, catalog.MO_TABLES_ID}][0]
		cols, err := genColumns(0, catalog.MO_DATABASE, catalog.MO_CATALOG, catalog.MO_DATABASE_ID,
			catalog.MO_CATALOG_ID, catalog.MoDatabaseTableDefs)
		if err != nil {
			return err
		}
		bat, err := genCreateTableTuple("", 0, 0, 0, catalog.MO_DATABASE, catalog.MO_DATABASE_ID,
			catalog.MO_CATALOG_ID, catalog.MO_CATALOG, "", m)
		if err != nil {
			return err
		}
		ibat, err := genInsertBatch(bat, m)
		if err != nil {
			bat.Clean(m)
			return err
		}
		if err := part.Insert(ctx, ibat); err != nil {
			bat.Clean(m)
			return err
		}
		bat.Clean(m)
		part = db.tables[[2]uint64{catalog.MO_CATALOG_ID, catalog.MO_COLUMNS_ID}][0]
		for _, col := range cols {
			bat, err := genCreateColumnTuple(col, m)
			if err != nil {
				return err
			}
			ibat, err := genInsertBatch(bat, m)
			if err != nil {
				bat.Clean(m)
				return err
			}
			if err := part.Insert(ctx, ibat); err != nil {
				bat.Clean(m)
				return err
			}
			bat.Clean(m)
		}
	}
	{ // mo_tables
		part := db.tables[[2]uint64{catalog.MO_CATALOG_ID, catalog.MO_TABLES_ID}][0]
		cols, err := genColumns(0, catalog.MO_TABLES, catalog.MO_CATALOG, catalog.MO_TABLES_ID,
			catalog.MO_CATALOG_ID, catalog.MoTablesTableDefs)
		if err != nil {
			return err
		}
		bat, err := genCreateTableTuple("", 0, 0, 0, catalog.MO_TABLES, catalog.MO_TABLES_ID,
			catalog.MO_CATALOG_ID, catalog.MO_CATALOG, "", m)
		if err != nil {
			return err
		}
		ibat, err := genInsertBatch(bat, m)
		if err != nil {
			bat.Clean(m)
			return err
		}
		if err := part.Insert(ctx, ibat); err != nil {
			bat.Clean(m)
			return err
		}
		bat.Clean(m)
		part = db.tables[[2]uint64{catalog.MO_CATALOG_ID, catalog.MO_COLUMNS_ID}][0]
		for _, col := range cols {
			bat, err := genCreateColumnTuple(col, m)
			if err != nil {
				return err
			}
			ibat, err := genInsertBatch(bat, m)
			if err != nil {
				bat.Clean(m)
				return err
			}
			if err := part.Insert(ctx, ibat); err != nil {
				bat.Clean(m)
				return err
			}
			bat.Clean(m)
		}
	}
	{ // mo_columns
		part := db.tables[[2]uint64{catalog.MO_CATALOG_ID, catalog.MO_TABLES_ID}][0]
		cols, err := genColumns(0, catalog.MO_COLUMNS, catalog.MO_CATALOG, catalog.MO_COLUMNS_ID,
			catalog.MO_CATALOG_ID, catalog.MoColumnsTableDefs)
		if err != nil {
			return err
		}
		bat, err := genCreateTableTuple("", 0, 0, 0, catalog.MO_COLUMNS, catalog.MO_COLUMNS_ID,
			catalog.MO_CATALOG_ID, catalog.MO_CATALOG, "", m)
		if err != nil {
			return err
		}
		ibat, err := genInsertBatch(bat, m)
		if err != nil {
			bat.Clean(m)
			return err
		}
		if err := part.Insert(ctx, ibat); err != nil {
			bat.Clean(m)
			return err
		}
		bat.Clean(m)
		part = db.tables[[2]uint64{catalog.MO_CATALOG_ID, catalog.MO_COLUMNS_ID}][0]
		for _, col := range cols {
			bat, err := genCreateColumnTuple(col, m)
			if err != nil {
				return err
			}
			ibat, err := genInsertBatch(bat, m)
			if err != nil {
				bat.Clean(m)
				return err
			}
			if err := part.Insert(ctx, ibat); err != nil {
				bat.Clean(m)
				return err
			}
			bat.Clean(m)
		}
	}
	return nil
}

func (db *DB) getPartitions(databaseId, tableId uint64) Partitions {
	db.Lock()
	parts, ok := db.tables[[2]uint64{databaseId, tableId}]
	if !ok { // create a new table
		parts = make(Partitions, len(db.dnMap))
		for i := range parts {
			parts[i] = NewPartition()
		}
		db.tables[[2]uint64{databaseId, tableId}] = parts
	}
	db.Unlock()
	return parts
}

func (db *DB) Update(ctx context.Context, dnList []DNStore,
	databaseId, tableId uint64, ts timestamp.Timestamp) error {
	op, err := db.cli.New()
	if err != nil {
		return err
	}
	db.Lock()
	parts, ok := db.tables[[2]uint64{databaseId, tableId}]
	if !ok { // create a new table
		parts = make(Partitions, len(db.dnMap))
		for i := range parts {
			parts[i] = NewPartition()
		}
		db.tables[[2]uint64{databaseId, tableId}] = parts
	}
	db.Unlock()
	for _, dn := range dnList {
		part := parts[db.dnMap[dn.UUID]]
		part.Lock()
		if part.ts.Less(ts) {
			if err := updatePartition(ctx, op, part, dn,
				genSyncLogTailReq(part.ts, ts, databaseId, tableId)); err != nil {
				part.Unlock()
				return err
			}
			part.ts = ts
		}
		part.Unlock()
	}
	return nil
}
