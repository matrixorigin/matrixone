// Copyright 2024 Matrix Origin
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

package test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/cdc"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/idxcdc"
	"github.com/matrixorigin/matrixone/pkg/util/executor"

	// "github.com/matrixorigin/matrixone/pkg/defines"
	// "github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"

	// "github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"

	// "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/test/testutil"
	"github.com/stretchr/testify/assert"

	// ie "github.com/matrixorigin/matrixone/pkg/util/internalExecutor"
	catalog2 "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
)

type idAllocator interface {
	Alloc() uint64
}

func newMoAsyncIndexLogBatch(
	mp *mpool.MPool,
	withRowID bool,
) *batch.Batch {
	bat := batch.New(
		[]string{
			"id",
			"account_id",
			"table_id",
			"index_id",
			"last_sync_txn_ts",
			"err_code",
			"error_msg",
			"info",
			"drop_at",
		},
	)
	bat.Vecs[0] = vector.NewVec(types.T_int32.ToType())   //id
	bat.Vecs[1] = vector.NewVec(types.T_uint64.ToType())  //account_id
	bat.Vecs[2] = vector.NewVec(types.T_int32.ToType())   //table_id
	bat.Vecs[3] = vector.NewVec(types.T_int32.ToType())   //index_id
	bat.Vecs[4] = vector.NewVec(types.T_varchar.ToType()) //last_sync_txn_ts
	bat.Vecs[5] = vector.NewVec(types.T_int32.ToType())   //err_code
	bat.Vecs[6] = vector.NewVec(types.T_varchar.ToType()) //error_msg
	bat.Vecs[7] = vector.NewVec(types.T_varchar.ToType()) //info
	bat.Vecs[8] = vector.NewVec(types.T_varchar.ToType()) //drop_at
	if withRowID {
		bat.Attrs = append(bat.Attrs, objectio.PhysicalAddr_Attr)
		bat.Vecs = append(bat.Vecs, vector.NewVec(types.T_Rowid.ToType())) //row_id
	}
	return bat
}

func getInsertWatermarkFn(
	t *testing.T,
	idAllocator idAllocator,
	de *testutil.TestDisttaeEngine,
	mp *mpool.MPool,
) func(
	ctx context.Context,
	tableID uint64,
	accountID int32,
	indexID int32,
) error {
	return func(
		ctx context.Context,
		tableID uint64,
		accountID int32,
		indexID int32,
	) error {
		_, rel, txn, err := de.GetTable(ctx, "mo_catalog", "mo_async_index_log")
		assert.NoError(t, err)
		bat := batch.New(
			[]string{
				"id",
				"account_id",
				"table_id",
				"index_id",
				"last_sync_txn_ts",
				"err_code",
				"error_msg",
				"info",
				"drop_at",
			},
		)
		bat.Vecs[0] = vector.NewVec(types.T_int32.ToType()) //id
		vector.AppendFixed(bat.Vecs[0], int32(idAllocator.Alloc()), false, mp)
		bat.Vecs[1] = vector.NewVec(types.T_int32.ToType()) //account_id
		vector.AppendFixed(bat.Vecs[1], accountID, false, mp)
		bat.Vecs[2] = vector.NewVec(types.T_int32.ToType()) //table_id
		vector.AppendFixed(bat.Vecs[2], tableID, false, mp)
		bat.Vecs[3] = vector.NewVec(types.T_int32.ToType()) //index_id
		vector.AppendFixed(bat.Vecs[3], indexID, false, mp)
		bat.Vecs[4] = vector.NewVec(types.T_varchar.ToType()) //last_sync_txn_ts
		vector.AppendBytes(bat.Vecs[4], []byte(""), false, mp)
		bat.Vecs[5] = vector.NewVec(types.T_int32.ToType()) //err_code
		vector.AppendFixed(bat.Vecs[5], int32(0), false, mp)
		bat.Vecs[6] = vector.NewVec(types.T_varchar.ToType()) //error_msg
		vector.AppendFixed(bat.Vecs[6], []byte(""), false, mp)
		bat.Vecs[7] = vector.NewVec(types.T_varchar.ToType()) //info
		vector.AppendBytes(bat.Vecs[7], []byte(""), false, mp)
		bat.Vecs[8] = vector.NewVec(types.T_varchar.ToType()) //drop_at
		vector.AppendBytes(bat.Vecs[8], []byte(""), false, mp)
		bat.Vecs[9] = vector.NewVec(types.T_bool.ToType()) //pause
		vector.AppendFixed(bat.Vecs[9], false, false, mp)
		bat.SetRowCount(1)

		assert.NoError(t, err)
		err = rel.Write(ctx, bat)
		assert.NoError(t, err)
		assert.NoError(t, txn.Commit(ctx))
		return nil
	}

}

func getFlushWatermarkFn(
	t *testing.T,
	de *testutil.TestDisttaeEngine,
	mp *mpool.MPool,
) func(
	ctx context.Context,
	tableID uint64,
	watermark types.TS,
	pause bool,
	accountID int32,
	indexID int32,
	errorCode int,
	info string,
	errorMsg string,
) error {
	return func(
		ctx context.Context,
		tableID uint64,
		watermark types.TS,
		pause bool,
		accountID int32,
		indexID int32,
		errorCode int,
		info string,
		errorMsg string,
	) error {
		txn, rel, reader, err := testutil.GetTableTxnReader(ctx, de, "mo_catalog", "mo_async_index_log", nil, mp, t)
		assert.NoError(t, err)
		defer func() {
			assert.NoError(t, reader.Close())
		}()

		bat := newMoAsyncIndexLogBatch(mp, true)

		done := false
		deleteRowIDs := make([]types.Rowid, 0)
		deletePks := make([]int32, 0)
		for !done {
			done, err = reader.Read(ctx, bat.Attrs, nil, mp, bat)
			assert.NoError(t, err)
			tableIDs := vector.MustFixedColNoTypeCheck[uint64](bat.Vecs[2])
			accountIDs := vector.MustFixedColNoTypeCheck[int32](bat.Vecs[1])
			indexIDs := vector.MustFixedColNoTypeCheck[int32](bat.Vecs[3])
			rowIDs := vector.MustFixedColNoTypeCheck[types.Rowid](bat.Vecs[9])
			pks := vector.MustFixedColNoTypeCheck[int32](bat.Vecs[0])
			for i := 0; i < bat.Vecs[0].Length(); i++ {
				if tableIDs[i] == tableID && accountIDs[i] == accountID && indexIDs[i] == indexID {
					deleteRowIDs = append(deleteRowIDs, rowIDs[i])
					deletePks = append(deletePks, pks[i])
				}
			}
		}
		if len(deleteRowIDs) != 1 {
			panic(fmt.Sprintf("logic err: rowCount:%d,tableID:%d,accountID:%d,indexID:%d, ts %v", len(deleteRowIDs), tableID, accountID, indexID, txn.SnapshotTS()))
		}

		deleteBatch := batch.New([]string{catalog.Row_ID, objectio.TombstoneAttr_PK_Attr})
		deleteBatch.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
		deleteBatch.Vecs[1] = vector.NewVec(types.T_int32.ToType())
		vector.AppendFixed(deleteBatch.Vecs[0], deleteRowIDs[0], false, mp)
		vector.AppendFixed(deleteBatch.Vecs[1], deletePks[0], false, mp)
		deleteBatch.SetRowCount(1)

		_, rel, txn, err = de.GetTable(ctx, "mo_catalog", "mo_async_index_log")
		assert.NoError(t, err)
		assert.NoError(t, rel.Delete(ctx, deleteBatch, catalog.Row_ID))

		bat = batch.New(
			[]string{
				"id",
				"account_id",
				"table_id",
				"index_id",
				"last_sync_txn_ts",
				"err_code",
				"error_msg",
				"info",
				"drop_at",
			},
		)
		bat.Vecs[0] = vector.NewVec(types.T_int32.ToType()) //id
		vector.AppendFixed(bat.Vecs[0], deletePks[0], false, mp)
		bat.Vecs[1] = vector.NewVec(types.T_int32.ToType()) //account_id
		vector.AppendFixed(bat.Vecs[1], accountID, false, mp)
		bat.Vecs[2] = vector.NewVec(types.T_int32.ToType()) //table_id
		vector.AppendFixed(bat.Vecs[2], tableID, false, mp)
		bat.Vecs[3] = vector.NewVec(types.T_int32.ToType()) //index_id
		vector.AppendFixed(bat.Vecs[3], indexID, false, mp)
		bat.Vecs[4] = vector.NewVec(types.T_varchar.ToType()) //last_sync_txn_ts
		vector.AppendBytes(bat.Vecs[4], []byte(watermark.ToString()), false, mp)
		bat.Vecs[5] = vector.NewVec(types.T_int32.ToType()) //err_code
		vector.AppendFixed(bat.Vecs[5], int32(errorCode), false, mp)
		bat.Vecs[6] = vector.NewVec(types.T_varchar.ToType()) //error_msg
		vector.AppendFixed(bat.Vecs[6], []byte(errorMsg), false, mp)
		bat.Vecs[7] = vector.NewVec(types.T_varchar.ToType()) //info
		vector.AppendBytes(bat.Vecs[7], []byte(info), false, mp)
		bat.Vecs[8] = vector.NewVec(types.T_varchar.ToType()) //drop_at
		vector.AppendBytes(bat.Vecs[8], []byte(errorMsg), false, mp)
		bat.Vecs[9] = vector.NewVec(types.T_bool.ToType()) //pause
		vector.AppendFixed(bat.Vecs[9], pause, false, mp)
		bat.SetRowCount(1)

		assert.NoError(t, err)
		err = rel.Write(ctx, bat)
		assert.NoError(t, err)
		assert.NoError(t, txn.Commit(ctx))
		return nil
	}
}
func getReplayFn(
	t *testing.T,
	de *testutil.TestDisttaeEngine,
	mp *mpool.MPool,
) func(
	ctx context.Context,
	tableID uint64,
	accountID uint32,
	indexID int32,
) (
	watermark types.TS,
	errorCode int,
	errorMsg string,
	pause bool,
	err error,
) {
	return func(
		ctx context.Context,
		tableID uint64,
		accountID uint32,
		indexID int32,
	) (
		watermark types.TS,
		errorCode int,
		errorMsg string,
		pause bool,
		err error,
	) {
		txn, _, reader, err := testutil.GetTableTxnReader(ctx, de, "mo_catalog", "mo_async_index_log", nil, mp, t)
		assert.NoError(t, err)
		defer func() {
			assert.NoError(t, reader.Close())
		}()

		bat := newMoAsyncIndexLogBatch(mp, true)

		done := false
		rowcount := 0
		for !done {
			done, err = reader.Read(ctx, bat.Attrs, nil, mp, bat)
			assert.NoError(t, err)
			tableIDs := vector.MustFixedColNoTypeCheck[uint64](bat.Vecs[2])
			accountIDs := vector.MustFixedColNoTypeCheck[int32](bat.Vecs[1])
			indexIDs := vector.MustFixedColNoTypeCheck[int32](bat.Vecs[3])
			errors := vector.MustFixedColNoTypeCheck[int32](bat.Vecs[5])
			pauses := vector.MustFixedColNoTypeCheck[bool](bat.Vecs[9])
			for i := 0; i < bat.Vecs[0].Length(); i++ {
				if tableIDs[i] == tableID && accountIDs[i] == int32(accountID) && indexIDs[i] == indexID {
					rowcount++
					watermarkStr := bat.Vecs[4].GetStringAt(i)
					watermark = types.StringToTS(watermarkStr)
					errorCode = int(errors[i])
					errorMsg = bat.Vecs[6].GetStringAt(i)
					pause = pauses[i]
				}
			}
		}
		if rowcount != 1 {
			err = moerr.NewInternalError(ctx, fmt.Sprintf("row count not match, row count %d", rowcount))
			return
		}
		assert.NoError(t, txn.Commit(ctx))
		return watermark, errorCode, errorMsg, pause, err
	}
}

func getDeleteFn(
	t *testing.T,
	de *testutil.TestDisttaeEngine,
	mp *mpool.MPool,
) func(
	ctx context.Context,
	tableID uint64,
	accountID uint32,
	indexID int32,
) (
	err error,
) {
	return func(
		ctx context.Context,
		tableID uint64,
		accountID uint32,
		indexID int32,
	) (
		err error,
	) {
		txn, rel, reader, err := testutil.GetTableTxnReader(ctx, de, "mo_catalog", "mo_async_index_log", nil, mp, t)
		assert.NoError(t, err)
		defer func() {
			assert.NoError(t, reader.Close())
		}()

		bat := newMoAsyncIndexLogBatch(mp, true)

		done := false
		deleteRowIDs := make([]types.Rowid, 0)
		deletePks := make([]int32, 0)
		for !done {
			done, err = reader.Read(ctx, bat.Attrs, nil, mp, bat)
			assert.NoError(t, err)
			tableIDs := vector.MustFixedColNoTypeCheck[uint64](bat.Vecs[2])
			accountIDs := vector.MustFixedColNoTypeCheck[int32](bat.Vecs[1])
			indexIDs := vector.MustFixedColNoTypeCheck[int32](bat.Vecs[3])
			rowIDs := vector.MustFixedColNoTypeCheck[types.Rowid](bat.Vecs[9])
			pks := vector.MustFixedColNoTypeCheck[int32](bat.Vecs[0])
			for i := 0; i < bat.Vecs[0].Length(); i++ {
				if tableIDs[i] == tableID && accountIDs[i] == int32(accountID) && indexIDs[i] == indexID {
					deleteRowIDs = append(deleteRowIDs, rowIDs[i])
					deletePks = append(deletePks, pks[i])
				}
			}
		}
		if len(deleteRowIDs) != 1 {
			panic(fmt.Sprintf("logic err: rowCount:%d,tableID:%d,accountID:%d,indexID:%d, ts %v", len(deleteRowIDs), tableID, accountID, indexID, txn.SnapshotTS()))
		}

		deleteBatch := batch.New([]string{catalog.Row_ID, objectio.TombstoneAttr_PK_Attr})
		deleteBatch.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
		deleteBatch.Vecs[1] = vector.NewVec(types.T_int32.ToType())
		vector.AppendFixed(deleteBatch.Vecs[0], deleteRowIDs[0], false, mp)
		vector.AppendFixed(deleteBatch.Vecs[1], deletePks[0], false, mp)
		deleteBatch.SetRowCount(1)

		assert.NoError(t, rel.Delete(ctx, deleteBatch, catalog.Row_ID))
		assert.NoError(t, txn.Commit(ctx))
		return err
	}
}

// func getCDCExecutor(
// 	ctx context.Context,
// 	t *testing.T,
// 	idAllocator idAllocator,
// 	accountID uint32,
// 	cnTestEngine *testutil.TestDisttaeEngine,
// 	taeHandler *testutil.TestTxnStorage,
// ) *frontend.CDCTaskExecutor {

// 	mockSpec := &task.CreateCdcDetails{
// 		TaskName: "cdc_task",
// 	}

// 	ieFactory := func() ie.InternalExecutor {
// 		return frontend.NewInternalExecutor(cnTestEngine.Engine.GetService())
// 	}

// 	txnFactory := func() (client.TxnOperator, error) {
// 		return cnTestEngine.NewTxnOperator(ctx, cnTestEngine.Now())
// 	}
// 	sinkerFactory := func(dbName, tableName string, tableDefs []engine.TableDef) (cdc.Sinker, error) {
// 		ctx = context.WithValue(ctx, defines.TenantIDKey{}, accountID)
// 		return frontend.MockCNSinker(
// 			ctx,
// 			func(ctx context.Context) (engine.Relation, client.TxnOperator, error) {
// 				_, rel, txn, err := cnTestEngine.GetTable(ctx, dbName, tableName)
// 				return rel, txn, err
// 			},
// 			func(ctx context.Context, dstDefs []engine.TableDef) error {
// 				txn, err := cnTestEngine.NewTxnOperator(ctx, cnTestEngine.Now())
// 				if err != nil {
// 					return err
// 				}
// 				db, err := cnTestEngine.Engine.Database(ctx, dbName, txn)
// 				if err != nil {
// 					return err
// 				}

// 				if _, err = db.Relation(ctx, tableName, nil); err == nil {
// 					return nil
// 				}

// 				err = db.Create(ctx, tableName, dstDefs)
// 				if err != nil {
// 					return err
// 				}
// 				return txn.Commit(ctx)
// 			},
// 			tableDefs,
// 			common.DebugAllocator,
// 		)
// 	}
// 	cdcExecutor := frontend.NewCDCTaskExecutor(
// 		ctx,
// 		uint64(accountID),
// 		mockSpec,
// 		ieFactory,
// 		sinkerFactory,
// 		txnFactory,
// 		cnTestEngine.Engine,
// 		cnTestEngine.Engine.GetService(),
// 		taeHandler.GetRPCHandle().HandleGetChangedTableList,
// 		getInsertWatermarkFn(t, idAllocator, cnTestEngine, common.DebugAllocator),
// 		getFlushWatermarkFn(t, cnTestEngine, common.DebugAllocator),
// 		getReplayFn(t, cnTestEngine, common.DebugAllocator),
// 		getDeleteFn(t, cnTestEngine, common.DebugAllocator),
// 		common.DebugAllocator,
// 	)
// 	return cdcExecutor
// }

/*
CREATE TABLE mo_async_index_log (

	id INT AUTO_INCREMENT PRIMARY KEY,
	account_id INT NOT NULL,
	table_id INT NOT NULL,
	db_id VARCHAR NOT NULL,
	index_name VARCHAR NOT NULL,
	last_sync_txn_ts VARCHAR(32)  NOT NULL,
	err_code INT NOT NULL,
	error_msg VARCHAR(255) NOT NULL,
	info VARCHAR(255) NOT NULL,
	drop_at VARCHAR(32) NULL,
	consumer_config VARCHAR(32) NULL,

);
*/
func mock_mo_async_index_log(
	de *testutil.TestDisttaeEngine,
	ctx context.Context,
) (err error) {
	var defs = make([]engine.TableDef, 0)

	addDefFn := func(name string, typ types.Type, idx int) {
		defs = append(defs, &engine.AttributeDef{
			Attr: engine.Attribute{
				Type:          typ,
				IsRowId:       false,
				Name:          name,
				ID:            uint64(idx),
				Primary:       name == "table_id",
				IsHidden:      false,
				Seqnum:        uint16(idx),
				ClusterBy:     false,
				AutoIncrement: false,
				Default: &plan.Default{
					NullAbility: name == "drop_at",
				},
			},
		})
	}

	addDefFn("account_id", types.T_uint32.ToType(), 0)
	addDefFn("table_id", types.T_uint64.ToType(), 1)
	addDefFn("index_name", types.T_varchar.ToType(), 2)
	addDefFn("last_sync_txn_ts", types.T_varchar.ToType(), 3)
	addDefFn("err_code", types.T_int32.ToType(), 4)
	addDefFn("error_msg", types.T_varchar.ToType(), 5)
	addDefFn("info", types.T_varchar.ToType(), 6)
	addDefFn("drop_at", types.T_varchar.ToType(), 7)
	addDefFn("consumer_config", types.T_varchar.ToType(), 8)

	defs = append(defs,
		&engine.ConstraintDef{
			Cts: []engine.Constraint{
				&engine.PrimaryKeyDef{
					Pkey: &plan.PrimaryKeyDef{
						PkeyColName: "table_id",
						Names:       []string{"table_id"},
					},
				},
			},
		},
	)
	dbName := "mo_catalog"
	tableName := "mo_async_index_log"
	var txn client.TxnOperator
	if txn, err = de.NewTxnOperator(ctx, de.Now()); err != nil {
		return
	}

	var database engine.Database
	if database, err = de.Engine.Database(ctx, dbName, txn); err != nil {
		return
	}
	if err = database.Create(ctx, tableName, defs); err != nil {
		return
	}

	if _, err = database.Relation(ctx, tableName, nil); err != nil {
		return
	}

	if err = txn.Commit(ctx); err != nil {
		return
	}
	return
}

/*
CREATE TABLE mo_async_index_iterations (

	id INT AUTO_INCREMENT PRIMARY KEY,// ignore id in ut, or preinsert will panic
	account_id INT UNSIGNED NOT NULL,
	table_id BIGINT UNSIGNED NOT NULL,// use table_id as primary key instead
	index_names VARCHAR(255),--multiple indexes
	from_ts VARCHAR(32) NOT NULL,
	to_ts VARCHAR(32) NOT NULL,
	error_json VARCHAR(255) NOT NULL,--Multiple errors are stored. Different consumers may have different errors.
	start_at DATETIME NULL,
	end_at DATETIME NULL,

);
*/
func mock_mo_async_index_iterations(
	de *testutil.TestDisttaeEngine,
	ctx context.Context,
) (err error) {

	sql := "CREATE TABLE mo_catalog.mo_async_index_iterations (" +
		"account_id INT UNSIGNED NOT NULL," +
		"table_id BIGINT UNSIGNED NOT NULL," +
		"index_names VARCHAR," +
		"from_ts VARCHAR(32) NOT NULL," +
		"to_ts VARCHAR(32) NOT NULL," +
		"error_json VARCHAR NOT NULL," +
		"start_at DATETIME NULL," +
		"end_at DATETIME NULL," +
		"PRIMARY KEY (table_id, index_names,to_ts)" +
		")"

	v, ok := moruntime.ServiceRuntime("").GetGlobalVariables(moruntime.InternalSQLExecutor)
	if !ok {
		panic("missing lock service")
	}

	exec := v.(executor.SQLExecutor)
	txn, err := de.NewTxnOperator(ctx, de.Now())
	if err != nil {
		return err
	}
	opts := executor.Options{}.
		// All runSql and runSqlWithResult is a part of input sql, can not incr statement.
		// All these sub-sql's need to be rolled back and retried en masse when they conflict in pessimistic mode
		WithDisableIncrStatement().
		WithTxn(txn)

	_, err = exec.Exec(ctx, sql, opts)
	if err != nil {
		return err
	}
	if err = txn.Commit(ctx); err != nil {
		return err
	}
	return err
}

/*
CREATE TABLE `mo_indexes` (

	`id` bigint unsigned NOT NULL,
	`table_id` bigint unsigned NOT NULL,
	`database_id` bigint unsigned NOT NULL,
	`name` varchar(64) NOT NULL,
	`type` varchar(11) NOT NULL,
	`algo` varchar(11) DEFAULT NULL,
	`algo_table_type` varchar(11) DEFAULT NULL,
	`algo_params` varchar(2048) DEFAULT NULL,
	`is_visible` tinyint NOT NULL,
	`hidden` tinyint NOT NULL,
	`comment` varchar(2048) NOT NULL,
	`column_name` varchar(256) NOT NULL,
	`ordinal_position` int unsigned NOT NULL,
	`options` text DEFAULT NULL,
	`index_table_name` varchar(5000) DEFAULT NULL,
	PRIMARY KEY (`id`,`column_name`)

)
*/
func mock_mo_indexes(
	de *testutil.TestDisttaeEngine,
	ctx context.Context,
) (err error) {
	sql := "CREATE TABLE `mo_catalog`.`mo_indexes` ( " +
		"`id` bigint unsigned NOT NULL," +
		"`table_id` bigint unsigned NOT NULL," +
		"`database_id` bigint unsigned NOT NULL," +
		"`name` varchar(64) NOT NULL," +
		"`type` varchar(11) NOT NULL," +
		"`algo` varchar(11) DEFAULT NULL," +
		"`algo_table_type` varchar(11) DEFAULT NULL," +
		"`algo_params` varchar(2048) DEFAULT NULL," +
		"`is_visible` tinyint NOT NULL," +
		"`hidden` tinyint NOT NULL," +
		"`comment` varchar(2048) NOT NULL," +
		"`column_name` varchar(256) NOT NULL," +
		"`ordinal_position` int unsigned NOT NULL," +
		"`options` text DEFAULT NULL," +
		"`index_table_name` varchar(5000) DEFAULT NULL," +
		"PRIMARY KEY (`table_id`,`column_name`)" + // use table_id as primary key instead of id to avoid duplicate
		")"

	v, ok := moruntime.ServiceRuntime("").GetGlobalVariables(moruntime.InternalSQLExecutor)
	if !ok {
		panic("missing lock service")
	}

	exec := v.(executor.SQLExecutor)
	txn, err := de.NewTxnOperator(ctx, de.Now())
	if err != nil {
		return err
	}
	opts := executor.Options{}.
		// All runSql and runSqlWithResult is a part of input sql, can not incr statement.
		// All these sub-sql's need to be rolled back and retried en masse when they conflict in pessimistic mode
		WithDisableIncrStatement().
		WithTxn(txn)

	_, err = exec.Exec(ctx, sql, opts)
	if err != nil {
		return err
	}
	if err = txn.Commit(ctx); err != nil {
		return err
	}
	return err
}
func mock_mo_foreign_keys(
	de *testutil.TestDisttaeEngine,
	ctx context.Context,
) (err error) {
	sql := "CREATE TABLE `mo_catalog`.`mo_foreign_keys` (" +
		"`constraint_name` varchar(5000) NOT NULL," +
		"`constraint_id` bigint unsigned NOT NULL DEFAULT 0," +
		"`db_name` varchar(5000) NOT NULL," +
		"`db_id` bigint unsigned NOT NULL DEFAULT 0," +
		"`table_name` varchar(5000) NOT NULL," +
		"`table_id` bigint unsigned NOT NULL DEFAULT 0," +
		"`column_name` varchar(256) NOT NULL," +
		"`column_id` bigint unsigned NOT NULL DEFAULT 0," +
		"`refer_db_name` varchar(5000) NOT NULL," +
		"`refer_db_id` bigint unsigned NOT NULL DEFAULT 0," +
		"`refer_table_name` varchar(5000) NOT NULL," +
		"`refer_table_id` bigint unsigned NOT NULL DEFAULT 0," +
		"`refer_column_name` varchar(256) NOT NULL," +
		"`refer_column_id` bigint unsigned NOT NULL DEFAULT 0," +
		"`on_delete` varchar(128) NOT NULL," +
		"`on_update` varchar(128) NOT NULL," +
		"PRIMARY KEY (`constraint_name`,`constraint_id`,`db_name`,`db_id`,`table_name`,`table_id`,`column_name`,`column_id`,`refer_db_name`,`refer_db_id`,`refer_table_name`,`refer_table_id`,`refer_column_name`,`refer_column_id`)" +
		")"

	result, err := execSql(de, ctx, sql)
	result.Close()
	return err
}

func execSql(
	de *testutil.TestDisttaeEngine,
	ctx context.Context,
	sql string,
) (result executor.Result,err error) {
	v, ok := moruntime.ServiceRuntime("").GetGlobalVariables(moruntime.InternalSQLExecutor)
	if !ok {
		panic("missing lock service")
	}

	exec := v.(executor.SQLExecutor)
	txn, err := de.NewTxnOperator(ctx, de.Now())
	if err != nil {
		return 
	}
	opts := executor.Options{}.
		// All runSql and runSqlWithResult is a part of input sql, can not incr statement.
		// All these sub-sql's need to be rolled back and retried en masse when they conflict in pessimistic mode
		WithDisableIncrStatement().
		WithTxn(txn)

		result, err = exec.Exec(ctx, sql, opts)
	if err != nil {
		return
	}
	if err = txn.Commit(ctx); err != nil {
		return result, err
	}
	return result, nil
}
func getCDCPitrTablesString(
	srcDB, srcTable string,
	dstDB, dstTable string,
) string {
	table := cdc.PatternTuple{
		Source: cdc.PatternTable{
			Database: srcDB,
			Table:    srcTable,
		},
		Sink: cdc.PatternTable{
			Database: dstDB,
			Table:    dstTable,
		},
	}
	var tablesPatternTuples cdc.PatternTuples
	tablesPatternTuples.Append(&table)
	tableStr, err := cdc.JsonEncode(tablesPatternTuples)
	if err != nil {
		panic(err)
	}
	return tableStr
}

// func addCDCTask(
// 	ctx context.Context,
// 	exec *frontend.CDCTaskExecutor,
// 	srcDB, srcTable string,
// 	dstDB, dstTable string,
// ) error {
// 	err := exec.StartTables(
// 		ctx,
// 		frontend.CDCCreateTaskOptions{
// 			PitrTables: getCDCPitrTablesString(
// 				srcDB,
// 				srcTable,
// 				dstDB,
// 				dstTable,
// 			),
// 		},
// 	)
// 	return err
// }

// func pauseCDCTask(
// 	ctx context.Context,
// 	exec *frontend.CDCTaskExecutor,
// 	srcDB, srcTable string,
// 	dstDB, dstTable string,
// ) error {
// 	err := exec.PauseTables(
// 		ctx,
// 		frontend.CDCCreateTaskOptions{
// 			PitrTables: getCDCPitrTablesString(
// 				srcDB,
// 				srcTable,
// 				dstDB,
// 				dstTable,
// 			),
// 		},
// 	)
// 	return err
// }

// func dropCDCTask(
// 	ctx context.Context,
// 	exec *frontend.CDCTaskExecutor,
// 	srcDB, srcTable string,
// 	dstDB, dstTable string,
// ) error {
// 	err := exec.DropTables(
// 		ctx,
// 		frontend.CDCCreateTaskOptions{
// 			PitrTables: getCDCPitrTablesString(
// 				srcDB,
// 				srcTable,
// 				dstDB,
// 				dstTable,
// 			),
// 		},
// 	)
// 	return err
// }

func CreateDBAndTableForHNSWAndGetAppendData(
	t *testing.T,
	de *testutil.TestDisttaeEngine,
	ctx context.Context,
	databaseName string,
	tableName string,
	rowCount int,
) *containers.Batch {
	// int64 is column 3, array_float32 is column 18
	schema := catalog2.MockSchemaAll(20, 3)
	txn, err := de.NewTxnOperator(ctx, de.Now())
	assert.NoError(t, err)

	err = de.Engine.Create(ctx, databaseName, txn)
	assert.NoError(t, err)

	database, err := de.Engine.Database(ctx, databaseName, txn)
	assert.NoError(t, err)

	engineTblDef, err := testutil.EngineTableDefBySchema(schema)
	assert.NoError(t, err)

	// add index
	indexColName := schema.ColDefs[18].Name
	engineTblDef = testutil.EngineDefAddIndex(engineTblDef, indexColName)

	err = database.Create(ctx, tableName, engineTblDef)
	assert.NoError(t, err)

	_, err = database.Relation(ctx, tableName, nil)
	assert.NoError(t, err)

	err = txn.Commit(ctx)
	assert.NoError(t, err)

	return catalog2.MockBatch(schema, rowCount)
}

func CreateDBAndTableForCNConsumerAndGetAppendData(
	t *testing.T,
	de *testutil.TestDisttaeEngine,
	ctx context.Context,
	databaseName string,
	tableName string,
	rowCount int,
) (*containers.Batch) {
	createDBSql := fmt.Sprintf("create database if not exists %s", databaseName)
	createTableSql := fmt.Sprintf(
		"create table %s.%s (id int primary key, name varchar)", databaseName, tableName)

	v, ok := moruntime.ServiceRuntime("").
		GetGlobalVariables(moruntime.InternalSQLExecutor)
	if !ok {
		panic("missing lock service")
	}

	exec := v.(executor.SQLExecutor)
	_, err := exec.Exec(ctx, createDBSql, executor.Options{})
	assert.NoError(t, err)
	_, err = exec.Exec(ctx, createTableSql, executor.Options{})
	assert.NoError(t, err)

	return containers.MockBatchWithAttrs(
		[]types.Type{types.T_int32.ToType(), types.T_varchar.ToType()},
		[]string{"id", "name"},
		rowCount,
		0,
		nil,
	)
}

func GetTestCDCExecutorOption() *idxcdc.CDCExecutorOption {
	return &idxcdc.CDCExecutorOption{
		GCInterval:             time.Millisecond * 100,
		GCTTL:                  time.Millisecond,
		SyncTaskInterval:       time.Millisecond * 100,
		FlushWatermarkInterval: time.Millisecond * 500,
	}
}

func CheckTableData(
	t *testing.T,
	de *testutil.TestDisttaeEngine,
	ctx context.Context,
	dbName string,
	tableName string,
	tableID uint64,
	indexName string,
){
	asyncIndexDBName := "test_async_index_cdc"
	asyncIndexTableName := fmt.Sprintf("test_table_%d_%v", tableID, indexName)
	sql1:= fmt.Sprintf(
		"SELECT * FROM %v.%v EXCEPT SELECT * FROM %v.%v;",
		dbName, tableName,
		asyncIndexDBName, asyncIndexTableName,
	)
	result1, err := execSql(de, ctx, sql1)
	assert.NoError(t, err)
	defer result1.Close()
	rowCount:=0
	result1.ReadRows(func(rows int, cols []*vector.Vector) bool {
		rowCount+=rows
		return true
	})
	assert.Equal(t, rowCount, 0)

	sql2 := fmt.Sprintf(
		"SELECT * FROM %v.%v EXCEPT SELECT * FROM %v.%v;",
		asyncIndexDBName, asyncIndexTableName,
		dbName, tableName,
	)
	result2, err := execSql(de, ctx, sql2)
	assert.NoError(t, err)
	defer result2.Close()
	rowCount = 0
	result2.ReadRows(func(rows int, cols []*vector.Vector) bool {
		rowCount += rows
		return true
	})
	assert.Equal(t, rowCount, 0)
}