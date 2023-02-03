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
	"testing"

	"github.com/lni/goutils/leaktest"
	"github.com/matrixorigin/matrixone/pkg/tests/service"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
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

	// this case will start a mo cluster with 1 CNService, 1 DNService and 3 LogService.
	// A Txn read and write will success.
	for name, options := range testOptionsSet {
		t.Run(name, func(t *testing.T) {
			c, err := NewCluster(t,
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

	// this case will start a mo cluster with 1 CNService, 1 DNService and 3 LogService.
	// 1. start t1
	// 2. start t2
	// 3. t1 write
	// 4. t2 can not read t1's write
	for name, options := range testOptionsSet {
		t.Run(name, func(t *testing.T) {
			c, err := NewCluster(t,
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
	for name, options := range testOptionsSet {
		t.Run(name, func(t *testing.T) {
			c, err := NewCluster(t,
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

func TestSingleShardWithCreateTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	if testing.Short() {
		t.Skip("skipping in short mode.")
		return
	}

	c, err := NewCluster(t,
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
