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

	"github.com/lni/goutils/leaktest"
	catalog2 "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
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
	defer leaktest.AfterTest(t)()
	var opts testutil.TestOptions

	opts.TaeEngineOptions = config.WithLongScanAndCKPOpts(nil)

	p := testutil.InitEnginePack(opts, t)
	defer p.Close()

	schema := catalog.MockSchemaAll(3, 2)

	rowsCnt := 8192 * 2

	dbBatch := catalog.MockBatch(schema, rowsCnt)
	defer dbBatch.Close()
	bat := containers.ToCNBatch(dbBatch)

	dbName := "db1"
	tblNames := []string{"test1", "test2", "test3"}

	txnop := p.StartCNTxn()
	p.CreateDB(txnop, dbName)
	require.NoError(t, txnop.Commit(p.Ctx))

	var dbId uint64
	var tblIds = make([]uint64, 0, len(tblNames))

	for i := range tblNames {
		schema.Name = tblNames[i]
		txnop = p.StartCNTxn()
		rel := p.CreateTableInDB(txnop, dbName, schema)
		require.NoError(t, rel.Write(p.Ctx, bat))
		require.NoError(t, txnop.Commit(p.Ctx))

		testutil2.CompactBlocks(t, 0, p.T.GetDB(), dbName, schema, false)
		txn, _ := p.T.StartTxn()
		ts := txn.GetStartTS()
		require.NoError(t, p.T.GetDB().ForceCheckpoint(p.Ctx, ts.Next()))
		require.NoError(t, txn.Commit(p.Ctx))

		dbId = rel.GetDBID(p.Ctx)
		tblIds = append(tblIds, rel.GetTableID(p.Ctx))
	}

	req := &cmd_util.GetChangedTableListReq{
		Type: cmd_util.CheckChanged,
	}
	resp := &cmd_util.GetChangedTableListResp{}

	for i := range tblIds {
		ts := timestamp.Timestamp{}
		req.TS = append(req.TS, &ts)
		req.AccIds = append(req.AccIds, 0)
		req.TableIds = append(req.TableIds, tblIds[i])
		req.DatabaseIds = append(req.DatabaseIds, dbId)
	}

	ts := types.MaxTs().ToTimestamp()
	req.TS[len(tblNames)-1] = &ts

	logutil.SetupMOLogger(&logutil.LogConfig{
		Level: "debug",
	})

	_, err := p.T.GetRPCHandle().HandleGetChangedTableList(p.Ctx, txn.TxnMeta{}, req, resp)
	require.NoError(t, err)

	require.Equal(t, tblIds[:len(tblIds)-1], resp.TableIds)
}
