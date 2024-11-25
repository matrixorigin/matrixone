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

	catalog2 "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
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
			fmt.Sprintf("select mo_table_rows('%s','%s');", databaseName, tableName),
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
			fmt.Sprintf("select mo_table_rows('%s','%s');", databaseName, tableName),
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
			fmt.Sprintf("select mo_table_rows('%s','%s');", databaseName, tableName),
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

	ret := disttae.HandleMoTableStatsCtl("restore_default_setting:true")
	require.Equal(t, "move_on(true), use_old_impl(false), force_update(false)", ret)

	_, err := disttae.QueryTableStats(context.Background(),
		[]int{disttae.TableStatsTableRows},
		[]uint64{0}, []uint64{1}, []uint64{2},
		false, false, nil)
	require.NotNil(t, err)

	ret = disttae.HandleMoTableStatsCtl("use_old_impl:true")
	require.Equal(t, "use old impl: false to true", ret)

	_, err = disttae.QueryTableStats(context.Background(),
		[]int{disttae.TableStatsTableRows},
		[]uint64{0}, []uint64{1}, []uint64{2},
		false, false, nil)
	require.NoError(t, err)

	ret = disttae.HandleMoTableStatsCtl("use_old_impl:false")
	require.Equal(t, "use old impl: true to false", ret)

	ret = disttae.HandleMoTableStatsCtl("force_update:true")
	require.Equal(t, "force update: false to true", ret)

	_, err = disttae.QueryTableStats(context.Background(),
		[]int{disttae.TableStatsTableRows},
		[]uint64{0}, []uint64{1}, []uint64{2},
		true, false, nil)
	require.NotNil(t, err)

	ret = disttae.HandleMoTableStatsCtl("move_on: false")
	require.Equal(t, "move on: true to false", ret)

	_, err = disttae.QueryTableStats(context.Background(),
		[]int{disttae.TableStatsTableRows},
		[]uint64{0}, []uint64{1}, []uint64{2},
		false, true, nil)
	require.NotNil(t, err)
}
