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

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
)

// init is used to insert some data that will not be synchronized by logtail.
func (e *Engine) init(ctx context.Context, m *mpool.MPool) error {
	e.Lock()
	defer e.Unlock()

	var packer *types.Packer
	put := e.packerPool.Get(&packer)
	defer put.Put()

	{
		parts := make(logtailreplay.Partitions, len(e.dnMap))
		for i := range parts {
			parts[i] = logtailreplay.NewPartition()
		}
		e.partitions[[2]uint64{catalog.MO_CATALOG_ID, catalog.MO_DATABASE_ID}] = parts
	}

	{
		parts := make(logtailreplay.Partitions, len(e.dnMap))
		for i := range parts {
			parts[i] = logtailreplay.NewPartition()
		}
		e.partitions[[2]uint64{catalog.MO_CATALOG_ID, catalog.MO_TABLES_ID}] = parts
	}

	{
		parts := make(logtailreplay.Partitions, len(e.dnMap))
		for i := range parts {
			parts[i] = logtailreplay.NewPartition()
		}
		e.partitions[[2]uint64{catalog.MO_CATALOG_ID, catalog.MO_COLUMNS_ID}] = parts
	}

	{ // mo_catalog
		part := e.partitions[[2]uint64{catalog.MO_CATALOG_ID, catalog.MO_DATABASE_ID}][0]
		bat, err := genCreateDatabaseTuple("", 0, 0, 0, catalog.MO_CATALOG, catalog.MO_CATALOG_ID, "", m)
		if err != nil {
			return err
		}
		ibat, err := genInsertBatch(bat, m)
		if err != nil {
			bat.Clean(m)
			return err
		}
		state, done := part.MutateState()
		state.HandleRowsInsert(ctx, ibat, MO_PRIMARY_OFF, packer)
		done()
		e.catalog.InsertDatabase(bat)
		bat.Clean(m)
	}

	{ // mo_database
		part := e.partitions[[2]uint64{catalog.MO_CATALOG_ID, catalog.MO_TABLES_ID}][0]
		cols, err := genColumns(0, catalog.MO_DATABASE, catalog.MO_CATALOG, catalog.MO_DATABASE_ID,
			catalog.MO_CATALOG_ID, catalog.MoDatabaseTableDefs)
		if err != nil {
			return err
		}
		tbl := new(txnTable)
		tbl.relKind = catalog.SystemOrdinaryRel
		bat, err := genCreateTableTuple(tbl, "", 0, 0, 0,
			catalog.MO_DATABASE, catalog.MO_DATABASE_ID,
			catalog.MO_CATALOG_ID, catalog.MO_CATALOG, types.Rowid{}, false, m)
		if err != nil {
			return err
		}
		ibat, err := genInsertBatch(bat, m)
		if err != nil {
			bat.Clean(m)
			return err
		}
		state, done := part.MutateState()
		state.HandleRowsInsert(ctx, ibat, MO_PRIMARY_OFF+catalog.MO_TABLES_REL_ID_IDX, packer)
		done()
		e.catalog.InsertTable(bat)
		bat.Clean(m)

		part = e.partitions[[2]uint64{catalog.MO_CATALOG_ID, catalog.MO_COLUMNS_ID}][0]
		bat = batch.NewWithSize(len(catalog.MoColumnsSchema))
		bat.Attrs = append(bat.Attrs, catalog.MoColumnsSchema...)
		bat.SetZs(len(cols), m)
		for _, col := range cols {
			bat0, err := genCreateColumnTuple(col, types.Rowid{}, false, m)
			if err != nil {
				return err
			}
			if bat.Vecs[0] == nil {
				for i, vec := range bat0.Vecs {
					bat.Vecs[i] = vector.NewVec(*vec.GetType())
				}
			}
			for i, vec := range bat0.Vecs {
				if err := bat.Vecs[i].UnionOne(vec, 0, m); err != nil {
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
		state, done = part.MutateState()
		state.HandleRowsInsert(ctx, ibat, MO_PRIMARY_OFF+catalog.MO_COLUMNS_ATT_UNIQ_NAME_IDX, packer)
		done()
		e.catalog.InsertColumns(bat)
		bat.Clean(m)
	}

	{ // mo_tables
		part := e.partitions[[2]uint64{catalog.MO_CATALOG_ID, catalog.MO_TABLES_ID}][0]
		cols, err := genColumns(0, catalog.MO_TABLES, catalog.MO_CATALOG, catalog.MO_TABLES_ID,
			catalog.MO_CATALOG_ID, catalog.MoTablesTableDefs)
		if err != nil {
			return err
		}
		tbl := new(txnTable)
		tbl.relKind = catalog.SystemOrdinaryRel
		bat, err := genCreateTableTuple(tbl, "", 0, 0, 0, catalog.MO_TABLES, catalog.MO_TABLES_ID,
			catalog.MO_CATALOG_ID, catalog.MO_CATALOG, types.Rowid{}, false, m)
		if err != nil {
			return err
		}
		ibat, err := genInsertBatch(bat, m)
		if err != nil {
			bat.Clean(m)
			return err
		}
		state, done := part.MutateState()
		state.HandleRowsInsert(ctx, ibat, MO_PRIMARY_OFF+catalog.MO_TABLES_REL_ID_IDX, packer)
		done()
		e.catalog.InsertTable(bat)
		bat.Clean(m)

		part = e.partitions[[2]uint64{catalog.MO_CATALOG_ID, catalog.MO_COLUMNS_ID}][0]
		bat = batch.NewWithSize(len(catalog.MoColumnsSchema))
		bat.Attrs = append(bat.Attrs, catalog.MoColumnsSchema...)
		bat.SetZs(len(cols), m)
		for _, col := range cols {
			bat0, err := genCreateColumnTuple(col, types.Rowid{}, false, m)
			if err != nil {
				return err
			}
			if bat.Vecs[0] == nil {
				for i, vec := range bat0.Vecs {
					bat.Vecs[i] = vector.NewVec(*vec.GetType())
				}
			}
			for i, vec := range bat0.Vecs {
				if err := bat.Vecs[i].UnionOne(vec, 0, m); err != nil {
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
		state, done = part.MutateState()
		state.HandleRowsInsert(ctx, ibat, MO_PRIMARY_OFF+catalog.MO_COLUMNS_ATT_UNIQ_NAME_IDX, packer)
		done()
		e.catalog.InsertColumns(bat)
		bat.Clean(m)
	}

	{ // mo_columns
		part := e.partitions[[2]uint64{catalog.MO_CATALOG_ID, catalog.MO_TABLES_ID}][0]
		cols, err := genColumns(0, catalog.MO_COLUMNS, catalog.MO_CATALOG, catalog.MO_COLUMNS_ID,
			catalog.MO_CATALOG_ID, catalog.MoColumnsTableDefs)
		if err != nil {
			return err
		}
		tbl := new(txnTable)
		tbl.relKind = catalog.SystemOrdinaryRel
		bat, err := genCreateTableTuple(tbl, "", 0, 0, 0, catalog.MO_COLUMNS, catalog.MO_COLUMNS_ID,
			catalog.MO_CATALOG_ID, catalog.MO_CATALOG, types.Rowid{}, false, m)
		if err != nil {
			return err
		}
		ibat, err := genInsertBatch(bat, m)
		if err != nil {
			bat.Clean(m)
			return err
		}
		state, done := part.MutateState()
		state.HandleRowsInsert(ctx, ibat, MO_PRIMARY_OFF+catalog.MO_TABLES_REL_ID_IDX, packer)
		done()
		e.catalog.InsertTable(bat)
		bat.Clean(m)

		part = e.partitions[[2]uint64{catalog.MO_CATALOG_ID, catalog.MO_COLUMNS_ID}][0]
		bat = batch.NewWithSize(len(catalog.MoColumnsSchema))
		bat.Attrs = append(bat.Attrs, catalog.MoColumnsSchema...)
		bat.SetZs(len(cols), m)
		for _, col := range cols {
			bat0, err := genCreateColumnTuple(col, types.Rowid{}, false, m)
			if err != nil {
				return err
			}
			if bat.Vecs[0] == nil {
				for i, vec := range bat0.Vecs {
					bat.Vecs[i] = vector.NewVec(*vec.GetType())
				}
			}
			for i, vec := range bat0.Vecs {
				if err := bat.Vecs[i].UnionOne(vec, 0, m); err != nil {
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
		state, done = part.MutateState()
		state.HandleRowsInsert(ctx, ibat, MO_PRIMARY_OFF+catalog.MO_COLUMNS_ATT_UNIQ_NAME_IDX, packer)
		done()
		e.catalog.InsertColumns(bat)
		bat.Clean(m)
	}

	return nil
}

func (e *Engine) getPartitions(databaseId, tableId uint64) logtailreplay.Partitions {
	e.Lock()
	defer e.Unlock()
	parts, ok := e.partitions[[2]uint64{databaseId, tableId}]
	if !ok { // create a new table
		parts = make(logtailreplay.Partitions, len(e.dnMap))
		for i := range parts {
			parts[i] = logtailreplay.NewPartition()
		}
		e.partitions[[2]uint64{databaseId, tableId}] = parts
	}
	return parts
}

func (e *Engine) lazyLoad(ctx context.Context, tbl *txnTable) error {
	parts := e.getPartitions(tbl.db.databaseId, tbl.tableId)

	for _, part := range parts {

		select {
		case <-part.Lock():
			defer part.Unlock()
		case <-ctx.Done():
			return ctx.Err()
		}

		state, doneMutate := part.MutateState()

		if err := state.ConsumeCheckpoints(func(checkpoint string) error {
			entries, err := logtail.LoadCheckpointEntries(
				ctx,
				checkpoint,
				tbl.tableId,
				tbl.tableName,
				tbl.db.databaseId,
				tbl.db.databaseName,
				tbl.db.txn.engine.fs)
			if err != nil {
				return err
			}
			for _, entry := range entries {
				if err = consumeEntry(ctx, tbl.primarySeqnum, e, state, entry); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			return err
		}

		doneMutate()
	}

	return nil
}

func (e *Engine) UpdateOfPush(ctx context.Context, databaseId, tableId uint64, ts timestamp.Timestamp) error {
	return e.pClient.TryToSubscribeTable(ctx, databaseId, tableId)
}

func (e *Engine) UpdateOfPull(ctx context.Context, dnList []DNStore, tbl *txnTable, op client.TxnOperator,
	primarySeqnum int, databaseId, tableId uint64, ts timestamp.Timestamp) error {
	logDebugf(op.Txn(), "UpdateOfPull")

	parts := e.ensureTableParts(databaseId, tableId)

	for _, dn := range dnList {
		part := parts[e.dnMap[dn.ServiceID]]

		if err := func() error {
			select {
			case <-part.Lock():
				defer part.Unlock()
				if part.TS.Greater(ts) || part.TS.Equal(ts) {
					return nil
				}
			case <-ctx.Done():
				return ctx.Err()
			}

			if err := updatePartitionOfPull(
				primarySeqnum, tbl, ctx, op, e, part, dn,
				genSyncLogTailReq(part.TS, ts, databaseId, tableId),
			); err != nil {
				return err
			}

			part.TS = ts

			return nil
		}(); err != nil {
			return err
		}

	}

	return nil
}

func (e *Engine) ensureTableParts(databaseId uint64, tableId uint64) logtailreplay.Partitions {
	e.Lock()
	defer e.Unlock()
	parts, ok := e.partitions[[2]uint64{databaseId, tableId}]
	if !ok {
		parts = make(logtailreplay.Partitions, len(e.dnMap))
		for i := range parts {
			parts[i] = logtailreplay.NewPartition()
		}
		e.partitions[[2]uint64{databaseId, tableId}] = parts
	}
	return parts
}
