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
	"database/sql"
	"fmt"
	"testing"

	"github.com/lni/goutils/leaktest"
	pb "github.com/matrixorigin/matrixone/pkg/pb/ctl"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/util/json"
	"github.com/stretchr/testify/require"
)

func TestMoCtlGetSnapshot(t *testing.T) {
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
			v := mustGetSnapshot(t, cli)
			ts, err := timestamp.ParseTimestamp(v)
			require.NoError(t, err)
			require.NotEqual(t, timestamp.Timestamp{}, ts)
		})
	}
}

func TestMoCtlUseSnapshot(t *testing.T) {
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

			k1 := "k1"
			checkWrite(t, mustNewTxn(t, cli), k1, "1", nil, true)
			checkRead(t, mustNewTxn(t, cli), k1, "1", nil, true)

			v := mustGetSnapshot(t, cli)

			checkWrite(t, mustNewTxn(t, cli), k1, "2", nil, true)
			checkRead(t, mustNewTxn(t, cli), k1, "2", nil, true)

			mustUseSnapshot(t, cli, v)
			checkRead(t, mustNewTxn(t, cli), k1, "1", nil, true)
		})
	}
}

func mustCloseRows(t *testing.T, rows *sql.Rows) {
	require.NoError(t, rows.Close())
}

func mustGetSnapshot(t *testing.T, cli Client) string {
	txn, err := cli.NewTxn()
	require.NoError(t, err)
	sqlTxn := txn.(SQLBasedTxn)
	defer func() {
		require.NoError(t, sqlTxn.Commit())
	}()

	rows, err := sqlTxn.ExecSQLQuery("select mo_ctl('cn', 'GetSnapshot', '')")
	require.NoError(t, err)
	defer func() {
		err := rows.Close()
		require.NoError(t, err)
		err = rows.Err()
		require.NoError(t, err)
	}()

	defer mustCloseRows(t, rows)
	require.True(t, rows.Next())

	var result pb.CtlResult
	value := ""
	require.NoError(t, rows.Scan(&value))
	json.MustUnmarshal([]byte(value), &result)
	return result.Data.(string)
}

func mustUseSnapshot(t *testing.T, cli Client, ts string) {
	txn, err := cli.NewTxn()
	require.NoError(t, err)
	sqlTxn := txn.(SQLBasedTxn)
	defer func() {
		require.NoError(t, sqlTxn.Commit())
	}()

	rows, err := sqlTxn.ExecSQLQuery(fmt.Sprintf("select mo_ctl('cn', 'UseSnapshot', '%s')", ts))
	require.NoError(t, err)
	mustCloseRows(t, rows)
}
