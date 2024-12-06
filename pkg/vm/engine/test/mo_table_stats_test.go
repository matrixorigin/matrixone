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

	catalog2 "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
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
	_, _ = p.CreateDBAndTable(txnop, databaseName, schema)
	require.NoError(t, txnop.Commit(p.Ctx))

	v, ok := runtime.ServiceRuntime("").GetGlobalVariables(runtime.InternalSQLExecutor)
	require.True(t, ok)

	exec := v.(executor.SQLExecutor)

	{
		ret := disttae.HandleMoTableStatsCtl("restore_default_setting:true")
		require.Equal(t, "move_on(true), use_old_impl(false), force_update(false)", ret)
	}

	{
		ret := disttae.HandleMoTableStatsCtl("move_on: false")
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
		ret := disttae.HandleMoTableStatsCtl("force_update:true")
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
		ret := disttae.HandleMoTableStatsCtl("use_old_impl:true")
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
	}
}

func TestMoTableStatsMoCtl2(t *testing.T) {
	var opts testutil.TestOptions
	opts.TaeEngineOptions = config.WithLongScanAndCKPOpts(nil)
	p := testutil.InitEnginePack(opts, t)
	defer p.Close()

	disttae.NotifyCleanDeletes()
	disttae.NotifyUpdateForgotten()

	schema := catalog.MockSchemaAll(3, 2)
	schema.Name = "test1"

	txnop := p.StartCNTxn()
	_, rel := p.CreateDBAndTable(txnop, "db1", schema)
	require.NoError(t, txnop.Commit(p.Ctx))

	ret := disttae.HandleMoTableStatsCtl("restore_default_setting:true")
	require.Equal(t, "move_on(true), use_old_impl(false), force_update(false)", ret)

	dbId := rel.GetDBID(p.Ctx)
	tblId := rel.GetTableID(p.Ctx)

	_, err := disttae.QueryTableStats(context.Background(),
		[]int{disttae.TableStatsTableRows},
		[]uint64{0}, []uint64{dbId}, []uint64{tblId},
		false, false, nil)
	require.NotNil(t, err)

	ret = disttae.HandleMoTableStatsCtl("use_old_impl:true")
	require.Equal(t, "use old impl: false to true", ret)

	_, err = disttae.QueryTableStats(context.Background(),
		[]int{disttae.TableStatsTableRows},
		[]uint64{0}, []uint64{dbId}, []uint64{tblId},
		false, false, nil)
	require.NoError(t, err)

	ret = disttae.HandleMoTableStatsCtl("use_old_impl:false")
	require.Equal(t, "use old impl: true to false", ret)

	ret = disttae.HandleMoTableStatsCtl("force_update:true")
	require.Equal(t, "force update: false to true", ret)

	_, err = disttae.QueryTableStats(context.Background(),
		[]int{disttae.TableStatsTableRows},
		[]uint64{0}, []uint64{dbId}, []uint64{tblId},
		true, false, nil)
	require.NotNil(t, err)

	ret = disttae.HandleMoTableStatsCtl("move_on: false")
	require.Equal(t, "move on: true to false", ret)

	_, err = disttae.QueryTableStats(context.Background(),
		[]int{disttae.TableStatsTableRows},
		[]uint64{0}, []uint64{dbId}, []uint64{tblId},
		false, true, nil)
	require.NotNil(t, err)

	ret = disttae.HandleMoTableStatsCtl("echo_current_setting:false")
	require.Equal(t, "noop", ret)

	ret = disttae.HandleMoTableStatsCtl("echo_current_setting:true")
	require.Equal(t, "move_on(false), use_old_impl(false), force_update(true)", ret)

	{
		ret = disttae.HandleMoTableStatsCtl("no_such_cmd:true")
		require.Equal(t, "failed, cmd invalid", ret)

		ret = disttae.HandleMoTableStatsCtl("force_update:yes")
		require.Equal(t, "failed, cmd invalid", ret)

		ret = disttae.HandleMoTableStatsCtl("force_update:true:false")
		require.Equal(t, "invalid command", ret)
	}

}

func TestHandleGetChangedList(t *testing.T) {

	opt, err := testutil.GetS3SharedFileServiceOption(
		context.Background(), testutil.GetDefaultTestPath("test", t))
	require.NoError(t, err)

	var opts testutil.TestOptions
	opts.TaeEngineOptions = opt
	p := testutil.InitEnginePack(opts, t)
	defer p.Close()

	schema := catalog.MockSchemaAll(3, 2)

	rowsCnt := 81920

	dbBatch := catalog.MockBatch(schema, rowsCnt)
	defer dbBatch.Close()
	bat := containers.ToCNBatch(dbBatch)

	dbName := "db1"
	tblNames := []string{"test1", "test2", "test3", "test4"}

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
		require.NoError(t, p.T.GetDB().ForceCheckpoint(p.Ctx, ts.Next(), time.Second*10))
		require.NoError(t, txn.Commit(p.Ctx))

		dbId = rel.GetDBID(p.Ctx)
		tblIds = append(tblIds, rel.GetTableID(p.Ctx))
	}

	req := &cmd_util.GetChangedTableListReq{}
	resp := &cmd_util.GetChangedTableListResp{}

	for i := range tblIds {
		ts := timestamp.Timestamp{}
		req.From = append(req.From, &ts)
		req.AccIds = append(req.AccIds, 0)
		req.TableIds = append(req.TableIds, tblIds[i])
		req.DatabaseIds = append(req.DatabaseIds, dbId)
	}

	ts := types.MaxTs().ToTimestamp()
	req.From[len(tblNames)-1] = &ts

	_, err = p.T.GetRPCHandle().HandleGetChangedTableList(p.Ctx, txn.TxnMeta{}, req, resp)
	require.NoError(t, err)

	require.Equal(t, tblIds[:len(tblIds)-1], resp.TableIds)
}
