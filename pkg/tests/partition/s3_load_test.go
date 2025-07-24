// Copyright 2021 - 2025 Matrix Origin
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

package partition

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

func TestLoadS3(t *testing.T) {
	runPartitionClusterTest(
		t,
		func(c embed.Cluster) {
			cn, err := c.GetCNService(0)
			require.NoError(t, err)
			exec := testutils.GetSQLExecutor(cn)

			db := testutils.GetDatabaseName(t)
			testutils.CreateTestDatabase(t, db, cn)

			sql := fmt.Sprintf(
				"create table %s (c int primary key, d int) partition by hash(c) partitions 2",
				t.Name(),
			)

			testutils.ExecSQL(
				t,
				db,
				cn,
				sql,
			)

			n := 100000
			sql = fmt.Sprintf("load data infile '%s' into table %s fields terminated by ','", genLoadFile(t, n), t.Name())
			testutils.ExecSQLWithReadResult(
				t,
				db,
				cn,
				func(i int, s string, r executor.Result) {
					require.Equal(t, uint64(n), r.AffectedRows)
				},
				sql,
			)

			loaded := 0
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*20)
			defer cancel()
			exec.ExecTxn(
				ctx,
				func(txn executor.TxnExecutor) error {
					metadata := getMetadata(
						t,
						0,
						db,
						t.Name(),
						cn,
					)

					for _, p := range metadata.Partitions {
						txn.Use(db)
						rs, err := txn.Exec(
							fmt.Sprintf("select count(1) from `%s`", p.PartitionTableName),
							executor.StatementOption{},
						)
						require.NoError(t, err)
						loaded += testutils.ReadCount(rs)
						rs.Close()
					}

					return nil
				},
				executor.Options{},
			)

			require.Equal(t, n, loaded)
		},
	)
}

func TestLoadS3WithIndex(t *testing.T) {
	runPartitionClusterTest(
		t,
		func(c embed.Cluster) {
			cn, err := c.GetCNService(0)
			require.NoError(t, err)
			exec := testutils.GetSQLExecutor(cn)
			eng := cn.RawService().(cnservice.Service).GetEngine()

			db := testutils.GetDatabaseName(t)
			testutils.CreateTestDatabase(t, db, cn)

			sql := fmt.Sprintf(
				"create table %s (c int primary key, d int, unique key(d)) partition by hash(c) partitions 2",
				t.Name(),
			)

			testutils.ExecSQL(
				t,
				db,
				cn,
				sql,
			)

			n := 100000
			sql = fmt.Sprintf("load data infile '%s' into table %s fields terminated by ','", genLoadFile(t, n), t.Name())
			testutils.ExecSQLWithReadResult(
				t,
				db,
				cn,
				func(i int, s string, r executor.Result) {
					require.Equal(t, uint64(n), r.AffectedRows)
				},
				sql,
			)

			loaded := 0
			indexLoaded := 0
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*20)
			defer cancel()
			exec.ExecTxn(
				ctx,
				func(txn executor.TxnExecutor) error {
					metadata := getMetadata(
						t,
						0,
						db,
						t.Name(),
						cn,
					)

					for _, p := range metadata.Partitions {
						txn.Use(db)
						rs, err := txn.Exec(
							fmt.Sprintf("select count(1) from `%s`", p.PartitionTableName),
							executor.StatementOption{},
						)
						require.NoError(t, err)
						loaded += testutils.ReadCount(rs)
						rs.Close()

						_, _, r, err := eng.GetRelationById(
							defines.AttachAccountId(ctx, 0),
							txn.Txn(),
							p.PartitionID,
						)
						require.NoError(t, err)

						_, _, indexR, err := eng.GetRelationById(
							defines.AttachAccountId(ctx, 0),
							txn.Txn(),
							r.GetExtraInfo().IndexTables[0],
						)
						require.NoError(t, err)

						rs, err = txn.Exec(
							fmt.Sprintf("select count(1) from `%s`", indexR.GetTableName()),
							executor.StatementOption{},
						)
						require.NoError(t, err)
						indexLoaded += testutils.ReadCount(rs)
						rs.Close()
					}

					return nil
				},
				executor.Options{},
			)

			require.Equal(t, n, loaded)
			require.Equal(t, n, indexLoaded)
		},
	)
}

func genLoadFile(
	t *testing.T,
	n int,
) string {
	tempDir := t.TempDir()
	csvFilePath := fmt.Sprintf("%s/%d.csv", tempDir, time.Now().UnixNano())

	file, err := os.Create(csvFilePath)
	require.NoError(t, err)

	defer func() {
		require.NoError(t, file.Close())
	}()

	for i := 1; i <= n; i++ {
		line := fmt.Sprintf("%d,%d\n", i, i)
		_, err := file.WriteString(line)
		require.NoError(t, err)
	}

	return csvFilePath
}
