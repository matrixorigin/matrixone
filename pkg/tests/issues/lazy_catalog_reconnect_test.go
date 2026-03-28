// Copyright 2021 - 2024 Matrix Origin
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

package issues

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	"github.com/stretchr/testify/require"
)

func TestLazyCatalogReconnectRestoresReadyAccounts(t *testing.T) {
	c, err := embed.NewCluster(embed.WithCNCount(1))
	require.NoError(t, err)
	require.NoError(t, c.Start())
	defer func() {
		require.NoError(t, c.Close())
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*180)
	defer cancel()

	op, err := c.GetCNService(0)
	require.NoError(t, err)

	cn := op.RawService().(cnservice.Service)
	eng := cn.GetEngine().(*disttae.Engine)
	logtailClient := eng.PushClient()

	reconnectC := make(chan struct{}, 4)
	logtailClient.SetReconnectHandler(func() {
		select {
		case reconnectC <- struct{}{}:
		default:
		}
	})

	accountIDA := testutils.CreateAccount(t, c, "lazyreconnecta", "111")
	accountIDB := testutils.CreateAccount(t, c, "lazyreconnectb", "111")
	accountIDC := testutils.CreateAccount(t, c, "lazyreconnectc", "111")

	dbA := "reconnect_dba"
	dbB := "reconnect_dbb"
	dbC := "reconnect_dbc"

	require.NoError(t, eng.ActivateTenantCatalog(ctx, uint32(accountIDA)))
	require.NoError(t, eng.ActivateTenantCatalog(ctx, uint32(accountIDB)))

	testutils.CreateTestDatabaseWithAccount(t, accountIDA, dbA, op)
	testutils.ExecSQLWithAccount(t, accountIDA, dbA, op,
		"create table ta(a int)",
		"insert into ta values (1), (2)",
	)
	testutils.CreateTestDatabaseWithAccount(t, accountIDB, dbB, op)
	testutils.ExecSQLWithAccount(t, accountIDB, dbB, op,
		"create table tb(b int)",
		"insert into tb values (21), (22), (23)",
	)

	maxTS := types.MaxTs().ToTimestamp()
	require.True(t, eng.PushClient().CanServeAccount(uint32(accountIDA), maxTS))
	require.True(t, eng.PushClient().CanServeAccount(uint32(accountIDB), maxTS))
	require.False(t, eng.PushClient().CanServeAccount(uint32(accountIDC), maxTS))

	require.NoError(t, logtailClient.Disconnect())
	waitReconnectReady(t, cn, reconnectC)

	require.True(t, eng.PushClient().CanServeAccount(uint32(accountIDA), maxTS))
	require.True(t, eng.PushClient().CanServeAccount(uint32(accountIDB), maxTS))
	require.False(t, eng.PushClient().CanServeAccount(uint32(accountIDC), maxTS))

	require.True(t, databaseExistsWithAccount(t, accountIDA, dbA, op))
	require.False(t, databaseExistsWithAccount(t, accountIDA, dbB, op))
	require.False(t, databaseExistsWithAccount(t, accountIDA, dbC, op))
	require.True(t, testutils.TableExistsWithAccount(t, accountIDA, dbA, "ta", op))
	require.Equal(t, int64(2), queryCountWithAccount(t, accountIDA, dbA, op, "select count(*) from ta"))

	require.True(t, databaseExistsWithAccount(t, accountIDB, dbB, op))
	require.False(t, databaseExistsWithAccount(t, accountIDB, dbA, op))
	require.False(t, databaseExistsWithAccount(t, accountIDB, dbC, op))
	require.True(t, testutils.TableExistsWithAccount(t, accountIDB, dbB, "tb", op))
	require.Equal(t, int64(3), queryCountWithAccount(t, accountIDB, dbB, op, "select count(*) from tb"))

	require.NoError(t, eng.ActivateTenantCatalog(ctx, uint32(accountIDC)))
	testutils.CreateTestDatabaseWithAccount(t, accountIDC, dbC, op)
	testutils.ExecSQLWithAccount(t, accountIDC, dbC, op,
		"create table tc(c int)",
		"insert into tc values (31)",
	)
	require.True(t, eng.PushClient().CanServeAccount(uint32(accountIDC), maxTS))

	require.NoError(t, logtailClient.Disconnect())
	waitReconnectReady(t, cn, reconnectC)

	require.True(t, eng.PushClient().CanServeAccount(uint32(accountIDA), maxTS))
	require.True(t, eng.PushClient().CanServeAccount(uint32(accountIDB), maxTS))
	require.True(t, eng.PushClient().CanServeAccount(uint32(accountIDC), maxTS))

	require.True(t, databaseExistsWithAccount(t, accountIDA, dbA, op))
	require.False(t, databaseExistsWithAccount(t, accountIDA, dbB, op))
	require.False(t, databaseExistsWithAccount(t, accountIDA, dbC, op))
	require.True(t, testutils.TableExistsWithAccount(t, accountIDA, dbA, "ta", op))
	require.Equal(t, int64(2), queryCountWithAccount(t, accountIDA, dbA, op, "select count(*) from ta"))

	require.True(t, databaseExistsWithAccount(t, accountIDB, dbB, op))
	require.False(t, databaseExistsWithAccount(t, accountIDB, dbA, op))
	require.False(t, databaseExistsWithAccount(t, accountIDB, dbC, op))
	require.True(t, testutils.TableExistsWithAccount(t, accountIDB, dbB, "tb", op))
	require.Equal(t, int64(3), queryCountWithAccount(t, accountIDB, dbB, op, "select count(*) from tb"))

	require.True(t, databaseExistsWithAccount(t, accountIDC, dbC, op))
	require.False(t, databaseExistsWithAccount(t, accountIDC, dbA, op))
	require.False(t, databaseExistsWithAccount(t, accountIDC, dbB, op))
	require.True(t, testutils.TableExistsWithAccount(t, accountIDC, dbC, "tc", op))
	require.Equal(t, int64(1), queryCountWithAccount(t, accountIDC, dbC, op, "select count(*) from tc"))
}

func TestLazyCatalogReconnectPreservesCheckpointAndInMemoryCatalogMix(t *testing.T) {
	c, err := embed.NewCluster(embed.WithCNCount(1))
	require.NoError(t, err)
	require.NoError(t, c.Start())
	defer func() {
		require.NoError(t, c.Close())
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*180)
	defer cancel()

	op, err := c.GetCNService(0)
	require.NoError(t, err)

	cn := op.RawService().(cnservice.Service)
	eng := cn.GetEngine().(*disttae.Engine)
	logtailClient := eng.PushClient()

	reconnectC := make(chan struct{}, 4)
	logtailClient.SetReconnectHandler(func() {
		select {
		case reconnectC <- struct{}{}:
		default:
		}
	})

	accountIDA := testutils.CreateAccount(t, c, "lazyckpmixa", "111")
	accountIDB := testutils.CreateAccount(t, c, "lazyckpmixb", "111")

	dbA := "mix_ckp_a"
	dbB := "mix_ckp_b"

	require.NoError(t, eng.ActivateTenantCatalog(ctx, uint32(accountIDA)))
	require.NoError(t, eng.ActivateTenantCatalog(ctx, uint32(accountIDB)))

	testutils.CreateTestDatabaseWithAccount(t, accountIDA, dbA, op)
	testutils.ExecSQLWithAccount(t, accountIDA, dbA, op,
		"create table t_flush(a int)",
		"insert into t_flush values (1), (2)",
	)
	testutils.CreateTestDatabaseWithAccount(t, accountIDB, dbB, op)
	testutils.ExecSQLWithAccount(t, accountIDB, dbB, op,
		"create table t_flush(b int)",
		"insert into t_flush values (21), (22), (23)",
	)

	testutils.ExecSQLWithAccount(t, accountIDA, dbA, op,
		fmt.Sprintf("select mo_ctl('dn','flush','%s.t_flush')", dbA),
	)
	testutils.ExecSQLWithAccount(t, accountIDB, dbB, op,
		fmt.Sprintf("select mo_ctl('dn','flush','%s.t_flush')", dbB),
	)
	testutils.ExecSQL(t, "mo_catalog", op, "select mo_ctl('dn','checkpoint','')")

	testutils.ExecSQLWithAccount(t, accountIDA, dbA, op,
		"create table t_mem(a int)",
		"insert into t_mem values (3), (4)",
	)
	testutils.ExecSQLWithAccount(t, accountIDB, dbB, op,
		"create table t_mem(b int)",
		"insert into t_mem values (24), (25)",
	)

	require.NoError(t, logtailClient.Disconnect())
	waitReconnectReady(t, cn, reconnectC)

	maxTS := types.MaxTs().ToTimestamp()
	require.True(t, eng.PushClient().CanServeAccount(uint32(accountIDA), maxTS))
	require.True(t, eng.PushClient().CanServeAccount(uint32(accountIDB), maxTS))

	require.True(t, databaseExistsWithAccount(t, accountIDA, dbA, op))
	require.False(t, databaseExistsWithAccount(t, accountIDA, dbB, op))
	require.True(t, testutils.TableExistsWithAccount(t, accountIDA, dbA, "t_flush", op))
	require.True(t, testutils.TableExistsWithAccount(t, accountIDA, dbA, "t_mem", op))
	require.Equal(t, int64(2), queryCountWithAccount(t, accountIDA, dbA, op, "select count(*) from t_flush"))
	require.Equal(t, int64(2), queryCountWithAccount(t, accountIDA, dbA, op, "select count(*) from t_mem"))

	require.True(t, databaseExistsWithAccount(t, accountIDB, dbB, op))
	require.False(t, databaseExistsWithAccount(t, accountIDB, dbA, op))
	require.True(t, testutils.TableExistsWithAccount(t, accountIDB, dbB, "t_flush", op))
	require.True(t, testutils.TableExistsWithAccount(t, accountIDB, dbB, "t_mem", op))
	require.Equal(t, int64(3), queryCountWithAccount(t, accountIDB, dbB, op, "select count(*) from t_flush"))
	require.Equal(t, int64(2), queryCountWithAccount(t, accountIDB, dbB, op, "select count(*) from t_mem"))
}

func waitReconnectReady(t *testing.T, cn cnservice.Service, reconnectC <-chan struct{}) {
	t.Helper()
	select {
	case <-reconnectC:
	case <-time.After(time.Second * 30):
		t.Fatal("timed out waiting for reconnect signal")
	}
	waitLogtailResume(cn)
}

func databaseExistsWithAccount(
	t *testing.T,
	account int32,
	name string,
	cn embed.ServiceOperator,
) bool {
	t.Helper()
	exists := false
	testutils.ExecSQLWithReadResultAndAccount(
		t,
		account,
		"mo_catalog",
		cn,
		func(i int, s string, r executor.Result) {
			exists = testutils.HasName(name, r)
		},
		fmt.Sprintf("show databases like '%s'", name),
	)
	return exists
}

func queryCountWithAccount(
	t *testing.T,
	account int32,
	db string,
	cn embed.ServiceOperator,
	statement string,
) int64 {
	t.Helper()
	var count int64 = -1
	testutils.ExecSQLWithReadResultAndAccount(
		t,
		account,
		db,
		cn,
		func(i int, s string, r executor.Result) {
			r.ReadRows(func(rows int, cols []*vector.Vector) bool {
				count = executor.GetFixedRows[int64](cols[0])[0]
				return false
			})
		},
		statement,
	)
	require.NotEqual(t, int64(-1), count)
	return count
}
