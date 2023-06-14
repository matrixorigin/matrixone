// Copyright 2021 - 2022 Matrix Origin
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

package txn

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/lni/goutils/leaktest"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/tests/service"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

var (
	testOptionsSet = map[string][]func(opts service.Options) service.Options{
		"tae-cn-tae-dn": {useDistributedTAEEngine, useTAEStorage},
	}
)

func TestBasicSingleShard(t *testing.T) {
	defer leaktest.AfterTest(t)()
	if testing.Short() {
		t.Skip("skipping in short mode.")
		return
	}
	ctx := context.Background()

	// this case will start a mo cluster with 1 CNService, 1 DNService and 3 LogService.
	// A Txn read and write will success.
	for name, options := range testOptionsSet {
		t.Run(name, func(t *testing.T) {
			c, err := NewCluster(ctx, t,
				getBasicClusterOptions(options...))
			require.NoError(t, err)
			defer c.Stop()
			c.Start()

			cli := c.NewClient()

			key := "k"
			value := "v"

			checkRead(t, mustNewTxn(t, cli), key, "", nil, true)
			checkWrite(t, mustNewTxn(t, cli), key, value, nil, true)
			checkRead(t, mustNewTxn(t, cli), key, value, nil, true)
		})
	}
}

func TestBasicSingleShardCannotReadUncommittedValue(t *testing.T) {
	defer leaktest.AfterTest(t)()
	if testing.Short() {
		t.Skip("skipping in short mode.")
		return
	}
	ctx := context.Background()

	// this case will start a mo cluster with 1 CNService, 1 DNService and 3 LogService.
	// 1. start t1
	// 2. start t2
	// 3. t1 write
	// 4. t2 can not read t1's write
	for name, options := range testOptionsSet {
		t.Run(name, func(t *testing.T) {
			c, err := NewCluster(ctx, t,
				getBasicClusterOptions(options...))
			require.NoError(t, err)
			defer c.Stop()
			c.Start()

			cli := c.NewClient()

			key := "k"
			value := "v"

			t1 := mustNewTxn(t, cli)
			t2 := mustNewTxn(t, cli)

			checkWrite(t, t1, key, value, nil, false)
			checkRead(t, t2, key, "", nil, true)

			require.NoError(t, t1.Commit())
		})
	}
}

func TestWriteSkewIsAllowed(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// this case will start a mo cluster with 1 CNService, 1 DNService and 3 LogService.
	// 1. start t1
	// 2. start t2
	// 3. t1 reads x
	// 4. t2 reads y
	// 5. t1 writes x -> y
	// 6. t2 writes y -> x
	// 7. t1 commits
	// 8. t2 commits
	if testing.Short() {
		t.Skip("skipping in short mode.")
		return
	}
	ctx := context.Background()

	for name, options := range testOptionsSet {
		t.Run(name, func(t *testing.T) {
			c, err := NewCluster(ctx, t,
				getBasicClusterOptions(options...))
			require.NoError(t, err)
			defer c.Stop()
			c.Start()

			cli := c.NewClient()

			k1 := "x"
			k2 := "y"

			checkWrite(t, mustNewTxn(t, cli), k1, "a", nil, true)
			checkWrite(t, mustNewTxn(t, cli), k2, "b", nil, true)

			t1 := mustNewTxn(t, cli)
			t2 := mustNewTxn(t, cli)

			x, err := t1.Read(k1)
			require.NoError(t, err)
			err = t1.Write(k2, x)
			require.NoError(t, err)
			y, err := t2.Read(k2)
			require.NoError(t, err)
			err = t2.Write(k1, y)
			require.NoError(t, err)
			err = t1.Commit()
			require.NoError(t, err)
			err = t2.Commit()
			require.NoError(t, err)

			checkRead(t, mustNewTxn(t, cli), k1, "b", nil, true)
			checkRead(t, mustNewTxn(t, cli), k2, "a", nil, true)
		})
	}
}

func TestBasicSingleShardWithInternalSQLExecutor(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode.")
		return
	}
	ctx := context.Background()

	// this case will start a mo cluster with 1 CNService, 1 DNService and 3 LogService.
	// A Txn read and write will success.
	for name, options := range testOptionsSet {
		t.Run(name, func(t *testing.T) {
			c, err := NewCluster(ctx, t,
				getBasicClusterOptions(options...))
			require.NoError(t, err)
			defer c.Stop()
			c.Start()

			v, ok := runtime.ProcessLevelRuntime().GetGlobalVariables(runtime.InternalSQLExecutor)
			if !ok {
				panic("missing internal sql executor")
			}
			exec := v.(executor.SQLExecutor)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()
			exec.ExecTxn(
				ctx,
				func(te executor.TxnExecutor) error {
					res, err := te.Exec("create database zx")
					require.NoError(t, err)
					res.Close()

					res, err = te.Exec("create table t1 (id int primary key, name varchar(255))")
					require.NoError(t, err)
					res.Close()

					res, err = te.Exec("insert into t1 values (1, 'a'),(2, 'b'),(3, 'c')")
					require.NoError(t, err)
					require.Equal(t, uint64(3), res.AffectedRows)
					res.Close()

					res, err = te.Exec("select id,name from t1 order by id")
					require.NoError(t, err)
					var ids []int32
					var names []string
					res.ReadRows(func(cols []*vector.Vector) bool {
						ids = append(ids, executor.GetFixedRows[int32](cols[0])...)
						names = append(names, executor.GetStringRows(cols[1])...)
						return true
					})
					require.Equal(t, []int32{1, 2, 3}, ids)
					require.Equal(t, []string{"a", "b", "c"}, names)
					res.Close()
					return nil
				},
				executor.Options{}.WithDatabase("zx"))
		})
	}
}

func TestSingleShardWithCreateTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	if testing.Short() {
		t.Skip("skipping in short mode.")
		return
	}
	ctx := context.Background()

	c, err := NewCluster(ctx, t,
		getBasicClusterOptions(useTAEStorage, useDistributedTAEEngine))
	require.NoError(t, err)
	defer c.Stop()
	c.Start()

	cli := c.NewClient()

	txn, err := cli.NewTxn()
	require.NoError(t, err)
	sqlTxn := txn.(SQLBasedTxn)

	_, err = sqlTxn.ExecSQL("create database test_db")
	require.NoError(t, err)
	require.NoError(t, sqlTxn.Commit())

	txn, err = cli.NewTxn()
	require.NoError(t, err)
	sqlTxn = txn.(SQLBasedTxn)
	_, err = sqlTxn.ExecSQL("use test_db")
	require.NoError(t, err)
	require.NoError(t, sqlTxn.Commit())
}

// # issue for #7748
// SQL statement here refers to func_aggr_avg.test
func TestAggTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	if testing.Short() {
		t.Skip("skipping in short mode.")
		return
	}
	ctx := context.Background()

	c, err := NewCluster(ctx, t,
		getBasicClusterOptions(useTAEStorage, useDistributedTAEEngine))
	require.NoError(t, err)
	defer c.Stop()
	c.Start()

	cli1 := c.NewClient()
	cli2 := c.NewClient()

	txn1, err := cli1.NewTxn()
	require.NoError(t, err)
	txn2, err := cli2.NewTxn()
	require.NoError(t, err)

	sqlTxn1 := txn1.(SQLBasedTxn)
	sqlTxn2 := txn2.(SQLBasedTxn)
	var txnList []SQLBasedTxn = []SQLBasedTxn{sqlTxn1, sqlTxn2}
	var tblList []string = []string{"t1", "t2"}
	type avgline struct {
		a int
		b float64
	}

	_, err = sqlTxn1.ExecSQL("create database test_avg;")
	require.NoError(t, err)
	_, err = sqlTxn1.ExecSQL("use test_avg;")
	require.NoError(t, err)
	_, err = sqlTxn1.ExecSQL("CREATE TABLE t1 (a INT, b INT);")
	require.NoError(t, err)
	_, err = sqlTxn1.ExecSQL("INSERT INTO t1 VALUES (1,1),(1,2),(1,3),(1,4),(1,5),(1,6),(1,7),(1,8);")
	require.NoError(t, err)
	_, err = sqlTxn2.ExecSQL("CREATE TABLE t2 (a INT, b INT);")
	require.NoError(t, err)
	_, err = sqlTxn2.ExecSQL("INSERT INTO t2 VALUES (1,1),(1,2),(1,3),(1,4),(1,5),(1,6),(1,7);")
	require.NoError(t, err)

	wg := sync.WaitGroup{}
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			insertTable(txnList[i], t, tblList[i])
		}(i)
	}
	wg.Wait()

	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(i int, t *testing.T) {
			defer wg.Done()
			rows, err := txnList[i].ExecSQLQuery(fmt.Sprintf("SELECT DISTINCT a, AVG( b) FROM %s GROUP BY a HAVING AVG( b) > 50;", tblList[i]))
			defer mustCloseRows(t, rows)
			defer func() {
				err := rows.Err()
				require.NoError(t, err)
			}()
			require.NoError(t, err)
			var l avgline
			if !rows.Next() {
				return
			}
			err = rows.Scan(&l.a, &l.b)
			require.NoError(t, err)
			if tblList[i] == "t1" {
				require.Equal(t, 32768.5, l.b)
			} else {
				require.Equal(t, 32774.5, l.b)
			}
		}(i, t)
	}
	wg.Wait()

	require.NoError(t, sqlTxn1.Commit())
	require.NoError(t, sqlTxn2.Commit())
}

func checkRead(t *testing.T, txn Txn, key string, expectValue string, expectError error, commit bool) {
	v, err := txn.Read(key)
	defer func() {
		if commit {
			require.NoError(t, txn.Commit())
		}
	}()
	require.Equal(t, expectError, err)
	require.Equal(t, expectValue, v)
}

func checkWrite(t *testing.T, txn Txn, key, value string, expectError error, commit bool) {
	defer func() {
		if commit {
			require.NoError(t, txn.Commit())
		}
	}()
	require.Equal(t, expectError, txn.Write(key, value))
}

func getBasicClusterOptions(opts ...func(opts service.Options) service.Options) service.Options {
	basic := service.DefaultOptions().
		WithDNShardNum(1).
		WithLogShardNum(1).
		WithDNServiceNum(1).
		WithLogServiceNum(3).
		WithCNShardNum(1).
		WithCNServiceNum(1)
	for _, opt := range opts {
		basic = opt(basic)
	}
	return basic
}

func useTAEStorage(opts service.Options) service.Options {
	return opts.WithDNUseTAEStorage()
}

func useDistributedTAEEngine(opts service.Options) service.Options {
	return opts.WithCNUseDistributedTAEEngine()
}

func mustNewTxn(t *testing.T, cli Client, options ...client.TxnOption) Txn {
	txn, err := cli.NewTxn(options...)
	require.NoError(t, err)
	return txn
}

// helper function for TestAggTable
func insertTable(s SQLBasedTxn, t *testing.T, tbl string) {
	var arr []int = []int{8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768}
	for _, v := range arr {
		if tbl == "t1" {
			_, err := s.ExecSQL(fmt.Sprintf("INSERT INTO %s SELECT a, b+%d       FROM %s;", tbl, v, tbl))
			require.NoError(t, err)
		} else {
			_, err := s.ExecSQL(fmt.Sprintf("INSERT INTO %s SELECT a, b+%d       FROM %s;", tbl, v+1, tbl))
			require.NoError(t, err)
		}
	}
}

// helper function for TestAggTable
