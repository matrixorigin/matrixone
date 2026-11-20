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

package test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/lni/goutils/leaktest"
	catalog2 "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/cmd_util"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	testutil2 "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils/config"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/test/testutil"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zapcore"
)

func TestMoTableStatsMoCtl(t *testing.T) {
	defer leaktest.AfterTest(t)()
	var (
		opts         testutil.TestOptions
		tableName    = "test1"
		databaseName = "db1"
	)

	opts.TaeEngineOptions = config.WithLongScanAndCKPOpts(nil)
	p := testutil.InitEnginePack(opts, t)
	defer p.Close()

	schema := catalog.MockSchemaAll(3, 2)
	schema.Name = tableName

	txnop := p.StartCNTxn()
	_, firstTable := p.CreateDBAndTable(txnop, databaseName, schema)
	require.NoError(t, txnop.Commit(p.Ctx))

	v, ok := runtime.ServiceRuntime("").GetGlobalVariables(runtime.InternalSQLExecutor)
	require.True(t, ok)

	exec := v.(executor.SQLExecutor)

	e := p.D.Engine
	{
		ret := e.HandleMoTableStatsCtl("restore_default_setting:true")
		require.Equal(t, "move_on(true), use_old_impl(false), force_update(false)", ret)
	}

	{
		ret := e.HandleMoTableStatsCtl("move_on: false")
		require.Equal(t, "move on: true to false", ret)
	}

	{
		txnop = p.StartCNTxn()
		_, err := exec.Exec(
			p.Ctx,
			fmt.Sprintf("insert into `%s`.`%s` values(1,1,1),(2,2,2),(3,3,3),(4,4,4);", databaseName, tableName),
			executor.Options{}.WithTxn(txnop))
		require.NoError(t, err)
		require.NoError(t, txnop.Commit(p.Ctx))
	}

	{
		txnop = p.StartCNTxn()
		_, err := exec.Exec(
			p.Ctx,
			fmt.Sprintf("select mo_table_rows('%s','%s'), mo_table_size('%s','%s');",
				databaseName, tableName, databaseName, tableName),
			executor.Options{}.WithTxn(txnop))
		require.NotNil(t, err)
		require.Contains(t, err.Error(), fmt.Sprintf(catalog2.MO_TABLE_STATS))
		require.NoError(t, txnop.Commit(p.Ctx))
	}

	{
		ret := e.HandleMoTableStatsCtl("force_update:true")
		require.Equal(t, "force update: false to true", ret)
	}

	{
		txnop = p.StartCNTxn()
		_, err := exec.Exec(
			p.Ctx,
			fmt.Sprintf("select mo_table_rows('%s','%s'), mo_table_size('%s','%s');",
				databaseName, tableName, databaseName, tableName),
			executor.Options{}.WithTxn(txnop))
		require.NotNil(t, err)
		require.Contains(t, err.Error(), fmt.Sprintf(catalog2.MO_TABLE_STATS))
		require.NoError(t, txnop.Commit(p.Ctx))
	}

	{
		ret := e.HandleMoTableStatsCtl("use_old_impl:true")
		require.Equal(t, "use old impl: false to true", ret)
	}

	{
		txnop = p.StartCNTxn()
		_, err := exec.Exec(
			p.Ctx,
			fmt.Sprintf("select mo_table_rows('%s','%s'), mo_table_size('%s','%s');",
				databaseName, tableName, databaseName, tableName),
			executor.Options{}.WithTxn(txnop))
		require.NoError(t, err)
		require.NoError(t, txnop.Commit(p.Ctx))
	}

	// Reuse this fixture: its server-state checker prevents background MTS
	// tasks from starting, so the seeded statistics cannot be overwritten.
	t.Run("account size aggregation", func(t *testing.T) {
		e.HandleMoTableStatsCtl("use_old_impl:false")
		e.HandleMoTableStatsCtl("force_update:false")
		runSQL := func(sql string) {
			result, err := exec.Exec(p.Ctx, sql, executor.Options{})
			require.NoError(t, err)
			defer result.Close()
		}
		runSQL(frontend.MoCatalogMoIndexesDDL)
		runSQL(frontend.MoCatalogMoTableStatsDDL)
		secondSchema := catalog.MockSchemaAll(3, 2)
		secondSchema.Name = "account_size_peer"
		secondTable := func() engine.Relation {
			op := p.StartCNTxn()
			defer op.Rollback(p.Ctx)
			rel := p.CreateTableInDB(op, databaseName, secondSchema)
			require.NoError(t, op.Commit(p.Ctx))
			return rel
		}()
		dbID, firstID, secondID := firstTable.GetDBID(p.Ctx), firstTable.GetTableID(p.Ctx), secondTable.GetTableID(p.Ctx)
		statsTable := "mo_catalog." + catalog2.MO_TABLE_STATS
		insert := func(account, database, table uint64, value string) {
			runSQL(fmt.Sprintf("insert into %s (account_id,database_id,table_id,table_stats,update_time) values (%d,%d,%d,%s,'2026-01-01 00:00:00')",
				statsTable, account, database, table, value))
		}
		// Each incomplete join key would count one of these unrelated rows.
		insert(42, dbID, firstID, `'{"table_size":9999}'`)
		insert(0, 9223372036854775807, firstID, `'{"table_size":9999}'`)
		insert(0, dbID, 9223372036854775807, `'{"table_size":9999}'`)
		for _, test := range []struct {
			name, first, second string
			want                uint64
		}{
			{"truncate before sum", `'{"table_size":1.8}'`, `'{"table_size":1.8}'`, 2},
			{"negative fraction from older statistics", `'{"table_size":-0.5}'`, `'{"table_size":1.8}'`, 1},
			{"negative fractions near zero and minus one", `'{"table_size":-0.0001}'`, `'{"table_size":-0.9999}'`, 0},
			{"SQL and JSON null", "NULL", `'{"table_size":null}'`, 0},
			{"missing field and row", `'{}'`, "", 0},
			{"exact integer sum", `'{"table_size":9007199254740992}'`, `'{"table_size":1}'`, 9007199254740993},
			{"near uint64 limit", `'{"table_size":18446744073709549568}'`, `'{"table_size":1}'`, 18446744073709549569},
			{"uint64 sum wraps", `'{"table_size":9223372036854775808}'`, `'{"table_size":9223372036854775808}'`, 0},
		} {
			t.Run(test.name, func(t *testing.T) {
				runSQL(fmt.Sprintf("delete from %s where account_id=0 and database_id=%d and table_id in (%d,%d)", statsTable, dbID, firstID, secondID))
				insert(0, dbID, firstID, test.first)
				if test.second != "" {
					insert(0, dbID, secondID, test.second)
				}
				sizes, ok, err := e.QueryAccountTableSizes(p.Ctx, []uint64{0, 404}, false)
				require.NoError(t, err)
				require.True(t, ok)
				require.Equal(t, map[uint64]uint64{0: test.want}, sizes)
				values, accounts, ok, err := e.QueryTableStatsByAccounts(p.Ctx, []int{disttae.TableStatsTableSize}, []uint64{0, 404}, false, false)
				require.NoError(t, err)
				require.True(t, ok)
				legacy := make(map[uint64]uint64)
				for i, account := range accounts {
					legacy[account] += uint64(values[0][i].(float64))
				}
				require.Equal(t, legacy, sizes)
			})
		}
		// Run the destructive case last; the outer fixture owns final cleanup.
		t.Run("dropped table stale statistics", func(t *testing.T) {
			// Seed both rows independently of which sibling subtests ran.
			runSQL(fmt.Sprintf("delete from %s where account_id=0 and database_id=%d and table_id in (%d,%d)", statsTable, dbID, firstID, secondID))
			insert(0, dbID, firstID, `'{"table_size":1}'`)
			insert(0, dbID, secondID, `'{"table_size":9999}'`)
			sizes, ok, err := e.QueryAccountTableSizes(p.Ctx, []uint64{0}, false)
			require.NoError(t, err)
			require.True(t, ok)
			require.Equal(t, map[uint64]uint64{0: 10000}, sizes, "both live tables' statistics must be counted")

			func() {
				op := p.StartCNTxn()
				defer op.Rollback(p.Ctx)
				p.DeleteTableInDB(op, databaseName, secondSchema.Name)
				require.NoError(t, op.Commit(p.Ctx))
			}()
			sizes, ok, err = e.QueryAccountTableSizes(p.Ctx, []uint64{0}, false)
			require.NoError(t, err)
			require.True(t, ok)
			require.Equal(t, map[uint64]uint64{0: 1}, sizes, "a dropped table's stale statistics must be excluded")
		})

		t.Run("cross-product stale identity", func(t *testing.T) {
			otherSchema := catalog.MockSchemaAll(3, 2)
			otherSchema.Name = "account_size_other"
			op := p.StartCNTxn()
			defer op.Rollback(p.Ctx)
			_, otherTable := p.CreateDBAndTable(op, "account_size_other_db", otherSchema)
			require.NoError(t, op.Commit(p.Ctx))
			otherDB, otherID := otherTable.GetDBID(p.Ctx), otherTable.GetTableID(p.Ctx)

			// Every ID in the stale row is live, but this combination is not.
			runSQL(fmt.Sprintf("delete from %s where account_id=0 and database_id=%d and table_id in (%d,%d)", statsTable, dbID, firstID, secondID))
			insert(0, otherDB, otherID, `'{"table_size":4}'`)
			insert(0, otherDB, firstID, `'{"table_size":9999}'`)
			sizes, ok, err := e.QueryAccountTableSizes(p.Ctx, []uint64{0}, false)
			require.NoError(t, err)
			require.True(t, ok)
			require.Equal(t, map[uint64]uint64{0: 4}, sizes)

			values, accounts, ok, err := e.QueryTableStatsByAccounts(p.Ctx, []int{disttae.TableStatsTableSize}, []uint64{0}, false, false)
			require.NoError(t, err)
			require.True(t, ok)
			var legacy uint64
			for i, account := range accounts {
				if account == 0 {
					legacy += uint64(values[0][i].(float64))
				}
			}
			require.Equal(t, uint64(10003), legacy, "legacy independent IN filters admit the stale identity")
		})
	})
}

func TestMoTableStatsMoCtl2(t *testing.T) {
	defer leaktest.AfterTest(t)()
	var opts testutil.TestOptions
	opts.TaeEngineOptions = config.WithLongScanAndCKPOpts(nil)
	p := testutil.InitEnginePack(opts, t)
	defer p.Close()

	e := p.D.Engine
	e.LaunchMTSTasksForUT()

	ret := e.HandleMoTableStatsCtl("recomputing:0")
	fmt.Println(ret)

	e.NotifyCleanDeletes()
	e.NotifyUpdateForgotten()

	schema := catalog.MockSchemaAll(3, 2)
	schema.Name = "test1"

	txnop := p.StartCNTxn()
	_, rel := p.CreateDBAndTable(txnop, "db1", schema)
	require.NoError(t, txnop.Commit(p.Ctx))

	ret = e.HandleMoTableStatsCtl("restore_default_setting:true")
	require.Equal(t, "move_on(true), use_old_impl(false), force_update(false)", ret)

	dbId := rel.GetDBID(p.Ctx)
	tblId := rel.GetTableID(p.Ctx)

	_, err, _ := e.QueryTableStats(context.Background(),
		[]int{disttae.TableStatsTableRows},
		[]uint64{0}, []uint64{dbId}, []uint64{tblId},
		false, false)
	require.NotNil(t, err)

	ret = e.HandleMoTableStatsCtl("use_old_impl:true")
	require.Equal(t, "use old impl: false to true", ret)

	_, err, _ = e.QueryTableStats(context.Background(),
		[]int{disttae.TableStatsTableRows},
		[]uint64{0}, []uint64{dbId}, []uint64{tblId},
		false, false)
	require.NoError(t, err)

	ret = e.HandleMoTableStatsCtl("use_old_impl:false")
	require.Equal(t, "use old impl: true to false", ret)

	ret = e.HandleMoTableStatsCtl("force_update:true")
	require.Equal(t, "force update: false to true", ret)

	_, err, _ = e.QueryTableStats(context.Background(),
		[]int{disttae.TableStatsTableRows},
		[]uint64{0}, []uint64{dbId}, []uint64{tblId},
		true, false)
	require.NotNil(t, err)

	ret = e.HandleMoTableStatsCtl("move_on: false")
	require.Equal(t, "move on: true to false", ret)

	_, err, _ = e.QueryTableStats(context.Background(),
		[]int{disttae.TableStatsTableRows},
		[]uint64{0}, []uint64{dbId}, []uint64{tblId},
		false, true)
	require.NotNil(t, err)

	ret = e.HandleMoTableStatsCtl("echo_current_setting:false")
	require.Equal(t, "noop", ret)

	ret = e.HandleMoTableStatsCtl("echo_current_setting:true")
	require.Equal(t, "move_on(false), use_old_impl(false), force_update(true)", ret)

	{
		ret = e.HandleMoTableStatsCtl("no_such_cmd:true")
		require.Equal(t, "failed, cmd invalid", ret)

		ret = e.HandleMoTableStatsCtl("force_update:yes")
		require.Equal(t, "failed, cmd invalid", ret)

		ret = e.HandleMoTableStatsCtl("force_update:true:false")
		require.Equal(t, "invalid command", ret)
	}

}

func TestHandleGetChangedList(t *testing.T) {
	originalLogger := logutil.GetGlobalLogger()
	t.Cleanup(func() {
		require.Same(t, originalLogger, logutil.GetGlobalLogger(), "test must restore the process-global logger")
	})
	var restoreLogger func() bool
	// Register before fixture cleanup, so those cleanups still run with DEBUG.
	t.Cleanup(func() {
		if restoreLogger != nil && !restoreLogger() {
			t.Error("the process-global logger was reconfigured during the test scope")
		}
	})

	defer leaktest.AfterTest(t)()
	var opts testutil.TestOptions
	opts.TaeEngineOptions = config.WithLongScanAndCKPOpts(nil)
	p := testutil.InitEnginePack(opts, t)
	defer p.Close()

	const dbName = "db1"
	func() {
		op, err := p.D.NewTxnOperator(p.Ctx, p.D.Now())
		require.NoError(t, err)
		committed := false
		defer func() {
			if !committed {
				rollbackWorkspaceTestTxn(t, context.WithoutCancel(p.Ctx), op.Rollback)
			}
		}()
		require.NoError(t, p.D.Engine.New(p.Ctx, op))
		require.NoError(t, p.D.Engine.Create(p.Ctx, dbName, op))
		_, err = p.D.Engine.Database(p.Ctx, dbName, op)
		require.NoError(t, err)
		require.NoError(t, op.Commit(p.Ctx))
		committed = true
	}()

	largeSchema := catalog.MockSchemaAll(3, 2)
	largeBatch := catalog.MockBatch(largeSchema, 8192*2)
	defer largeBatch.Close()
	largeInput := containers.ToCNBatch(largeBatch)
	smallSchema := catalog.MockSchemaAll(3, 2)
	smallBatch := catalog.MockBatch(smallSchema, 2)
	defer smallBatch.Close()
	smallInput := containers.ToCNBatch(smallBatch)
	largeNames := []string{"test1", "test2", "test3"}
	smallNames := []string{"small1", "small2", "small3"}
	var dbID uint64
	var largeIDs, smallIDs []uint64

	// Each checkpoint covers one large CN-created object and, when selected,
	// one independently flushed small TN object. The three pairs retain three
	// separate checkpoint intervals without writing six checkpoints.
	runPairs := func(t *testing.T, includeSmall bool) {
		t.Helper()
		var previousWitness, previousEnd types.TS
		for i := range largeNames {
			largeSchema.Name = largeNames[i]
			currentDB, largeID := writeChangedListTable(t, p, dbName, largeSchema, largeInput)
			if i == 0 {
				dbID = currentDB
			}
			require.Equal(t, dbID, currentDB)
			require.NotContains(t, smallIDs, largeID)
			largeIDs = append(largeIDs, largeID)
			checkLargeChangedListObject(t, p, dbName, largeSchema)
			testutil2.CompactBlocks(t, 0, p.T.GetDB(), dbName, largeSchema, false)
			checkChangedListPersistedObject(t, p, dbName, largeSchema.Name)

			var smallID uint64
			if includeSmall {
				smallSchema.Name = smallNames[i]
				smallDB, id := writeChangedListTable(t, p, dbName, smallSchema, smallInput)
				require.Equal(t, dbID, smallDB)
				require.NotContains(t, largeIDs, id, "small and large tables must be distinct")
				smallID = id
				smallIDs = append(smallIDs, id)
				checkSmallChangedListFlush(t, p, dbName, smallSchema)
			}

			func() {
				checkpointTxn, err := p.T.StartTxn()
				require.NoError(t, err)
				committed := false
				defer func() {
					if !committed {
						rollbackWorkspaceTestTxn(t, context.WithoutCancel(p.Ctx), checkpointTxn.Rollback)
					}
				}()
				witness := checkpointTxn.GetStartTS()
				if i > 0 {
					require.True(t, witness.GT(&previousWitness), "checkpoint witness must advance")
				}
				requested := witness.Next()
				require.NoError(t, p.T.GetDB().ForceCheckpoint(p.Ctx, requested))
				require.NoError(t, checkpointTxn.Commit(p.Ctx))
				committed = true
				checkpoint := p.T.GetDB().BGCheckpointRunner.MaxCheckpoint()
				require.NotNil(t, checkpoint, "forced checkpoint must be recorded")
				end := checkpoint.GetEnd()
				require.True(t, end.GE(&requested), "checkpoint must cover the pair")
				if i > 0 {
					require.True(t, end.GT(&previousEnd), "checkpoint intervals must advance")
				}
				previousWitness, previousEnd = witness, end
			}()

			if includeSmall {
				// Check both IDs immediately after their shared checkpoint. A QA
				// control also removes dirty-tree results to verify checkpoint origin.
				req := changedListRequest(dbID, []uint64{largeID, smallID}, false)
				resp := &cmd_util.GetChangedTableListResp{}
				_, err := p.T.GetRPCHandle().HandleGetChangedTableList(p.Ctx, txn.TxnMeta{}, req, resp)
				require.NoError(t, err)
				require.ElementsMatch(t, []uint64{largeID, smallID}, resp.TableIds)
			}
		}
	}

	ranSmall := false
	if !t.Run("small_appendable_flush", func(t *testing.T) {
		ranSmall = true
		runPairs(t, true)
		req := changedListRequest(dbID, smallIDs, true)
		resp := &cmd_util.GetChangedTableListResp{}
		_, err := p.T.GetRPCHandle().HandleGetChangedTableList(p.Ctx, txn.TxnMeta{}, req, resp)
		require.NoError(t, err)
		require.Equal(t, smallIDs[:len(smallIDs)-1], resp.TableIds)
	}) {
		t.FailNow()
	}
	if !ranSmall {
		// A non-matching child selector must still run the original large case.
		runPairs(t, false)
	}

	req := changedListRequest(dbID, largeIDs, true)
	resp := &cmd_util.GetChangedTableListResp{}
	restoreLogger = logutil.SetupMOLoggerForTest(&logutil.LogConfig{Level: "debug"})
	require.True(t, logutil.GetSkip1Logger().Core().Enabled(zapcore.DebugLevel))
	_, err := p.T.GetRPCHandle().HandleGetChangedTableList(p.Ctx, txn.TxnMeta{}, req, resp)
	require.NoError(t, err)
	require.Equal(t, largeIDs[:len(largeIDs)-1], resp.TableIds)
}

func changedListRequest(dbID uint64, tableIDs []uint64, lastAtMax bool) *cmd_util.GetChangedTableListReq {
	req := &cmd_util.GetChangedTableListReq{Type: cmd_util.CheckChanged}
	for _, id := range tableIDs {
		ts := timestamp.Timestamp{}
		req.TS = append(req.TS, &ts)
		req.AccIds = append(req.AccIds, 0)
		req.DatabaseIds = append(req.DatabaseIds, dbID)
		req.TableIds = append(req.TableIds, id)
	}
	if lastAtMax {
		ts := types.MaxTs().ToTimestamp()
		req.TS[len(req.TS)-1] = &ts
	}
	return req
}

func writeChangedListTable(t *testing.T, p *testutil.EnginePack, dbName string, schema *catalog.Schema, input *batch.Batch) (uint64, uint64) {
	t.Helper()
	op, err := p.D.NewTxnOperator(p.Ctx, p.D.Now())
	require.NoError(t, err)
	committed := false
	defer func() {
		if !committed {
			rollbackWorkspaceTestTxn(t, context.WithoutCancel(p.Ctx), op.Rollback)
		}
	}()
	require.NoError(t, p.D.Engine.New(p.Ctx, op))
	database, err := p.D.Engine.Database(p.Ctx, dbName, op)
	require.NoError(t, err)
	defs, err := catalog.SchemaToDefs(schema)
	require.NoError(t, err)
	require.NoError(t, database.Create(p.Ctx, schema.Name, defs))
	relation, err := database.Relation(p.Ctx, schema.Name, nil)
	require.NoError(t, err)
	require.NoError(t, relation.Write(p.Ctx, input))
	dbID, tableID := relation.GetDBID(p.Ctx), relation.GetTableID(p.Ctx)
	require.NoError(t, op.Commit(p.Ctx))
	committed = true
	return dbID, tableID
}

func rollbackChangedListRead(t *testing.T, ctx context.Context, rollback func(context.Context) error) {
	t.Helper()
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	require.NoError(t, rollback(cleanupCtx))
}

func checkLargeChangedListObject(t *testing.T, p *testutil.EnginePack, dbName string, schema *catalog.Schema) {
	t.Helper()
	readTxn, err := p.T.StartTxn()
	require.NoError(t, err)
	defer rollbackChangedListRead(t, p.Ctx, readTxn.Rollback)
	readTxn.BindAccessInfo(0, 0, 0)
	readDB, err := readTxn.GetDatabase(dbName)
	require.NoError(t, err)
	readRel, err := readDB.GetRelationByName(schema.Name)
	require.NoError(t, err)
	cnObjects := 0
	for _, object := range testutil2.GetAllBlockMetas(readRel, false) {
		if object.HasDropCommitted() {
			continue
		}
		stats := object.GetObjectStats()
		if stats.GetCNCreated() {
			cnObjects++
			require.EqualValues(t, 8192*2, stats.Rows(), "table %s must retain the CN-created rows", schema.Name)
			require.EqualValues(t, 2, stats.BlkCnt(), "table %s must retain the original two-block object", schema.Name)
		}
	}
	require.Equal(t, 1, cnObjects, "table %s must retain one CN-created object", schema.Name)
}

func checkChangedListPersistedObject(t *testing.T, p *testutil.EnginePack, dbName, tableName string) {
	t.Helper()
	readTxn, err := p.T.StartTxn()
	require.NoError(t, err)
	defer rollbackChangedListRead(t, p.Ctx, readTxn.Rollback)
	readTxn.BindAccessInfo(0, 0, 0)
	readDB, err := readTxn.GetDatabase(dbName)
	require.NoError(t, err)
	readRel, err := readDB.GetRelationByName(tableName)
	require.NoError(t, err)
	persisted := false
	for _, object := range testutil2.GetAllBlockMetas(readRel, false) {
		if !object.IsAppendable() && !object.HasDropCommitted() {
			persisted = true
			break
		}
	}
	require.True(t, persisted, "table %s must have a visible non-appendable object", tableName)
}

func checkSmallChangedListFlush(t *testing.T, p *testutil.EnginePack, dbName string, schema *catalog.Schema) {
	t.Helper()
	before := make(map[string]struct{})
	func() {
		readTxn, err := p.T.StartTxn()
		require.NoError(t, err)
		defer rollbackChangedListRead(t, p.Ctx, readTxn.Rollback)
		readTxn.BindAccessInfo(0, 0, 0)
		database, err := readTxn.GetDatabase(dbName)
		require.NoError(t, err)
		relation, err := database.GetRelationByName(schema.Name)
		require.NoError(t, err)
		for _, object := range testutil2.GetAllBlockMetas(relation, false) {
			if object.IsAppendable() && !object.HasDropCommitted() {
				before[object.ID().String()] = struct{}{}
			}
		}
	}()
	require.NotEmpty(t, before, "small table %s must start with an appendable object", schema.Name)
	testutil2.CompactBlocks(t, 0, p.T.GetDB(), dbName, schema, false)
	func() {
		readTxn, err := p.T.StartTxn()
		require.NoError(t, err)
		defer rollbackChangedListRead(t, p.Ctx, readTxn.Rollback)
		readTxn.BindAccessInfo(0, 0, 0)
		database, err := readTxn.GetDatabase(dbName)
		require.NoError(t, err)
		relation, err := database.GetRelationByName(schema.Name)
		require.NoError(t, err)
		var flushedRows uint32
		for _, object := range testutil2.GetAllBlockMetas(relation, false) {
			if _, existed := before[object.ID().String()]; existed {
				require.True(t, !object.IsAppendable() || object.HasDropCommitted(), "the old appendable object must no longer be active")
			}
			stats := object.GetObjectStats()
			if !object.IsAppendable() && !object.HasDropCommitted() && !stats.GetCNCreated() {
				require.NotContains(t, before, object.ID().String(), "flush must create a new persisted object")
				flushedRows += stats.Rows()
			}
		}
		require.EqualValues(t, 2, flushedRows, "small table %s must flush both rows", schema.Name)
	}()
}
