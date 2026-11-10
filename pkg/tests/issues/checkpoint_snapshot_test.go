// Copyright 2026 Matrix Origin
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
	"database/sql"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

func TestCheckpointColdSnapshotSurvivesDatabaseDrop(t *testing.T) {
	runAuthenticatedClusterTest(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 4*time.Minute)
		defer cancel()
		cn0, err := cluster.GetCNService(0)
		require.NoError(t, err)
		cn1, err := cluster.GetCNService(1)
		require.NoError(t, err)
		db0, err := sql.Open("mysql", issue27487DSN(cn0.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db0.Close()
		conn0, err := db0.Conn(ctx)
		require.NoError(t, err)
		defer conn0.Close()
		const database = "issue29852_visibility"
		const snapshot = "issue29852_visibility_before_drop"
		defer func() {
			cleanup, stop := context.WithTimeout(context.Background(), 30*time.Second)
			defer stop()
			_, _ = conn0.ExecContext(cleanup, "rollback")
			_, _ = conn0.ExecContext(cleanup, "drop database if exists "+database)
			_, _ = conn0.ExecContext(cleanup, "drop snapshot if exists "+snapshot)
		}()
		for _, q := range []string{
			"set role moadmin",
			`select mo_feature_registry_upsert('snapshot','Snapshot feature','{"allowed_scope":["account","database","table"]}',true)`,
			"create database " + database,
			"create table " + database + ".baseline(id int primary key,v int unique)",
			"insert into " + database + ".baseline values (1,7),(2,8)",
			"create table " + database + ".unopened(id int primary key)",
			"insert into " + database + ".unopened values(42)",
			"delete from " + database + ".baseline where id=2",
			"create snapshot " + snapshot + " for account sys",
			"update " + database + ".baseline set v=9 where id=1",
			"insert into " + database + ".baseline values(3,10)",
			"select mo_ctl('dn','globalcheckpoint','')",
			"select mo_ctl('dn','checkpoint','')",
		} {
			_, err = conn0.ExecContext(ctx, q)
			require.NoError(t, err, q)
		}
		var dbID uint64
		require.NoError(t, conn0.QueryRowContext(ctx, "select dat_id from mo_catalog.mo_database where datname=? and account_id=0", database).Scan(&dbID))
		var historical int
		require.NoError(t, conn0.QueryRowContext(ctx, "select count(*) from "+database+".baseline{snapshot='"+snapshot+"'} where id=1 and v=7").Scan(&historical))
		require.Equal(t, 1, historical, "snapshot must contain the row before DROP")
		// CN1 has not opened this database: load checkpoint-backed catalog cold.
		db1, err := sql.Open("mysql", issue27487DSN(cn1.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db1.Close()
		var value int
		require.NoError(t, db1.QueryRowContext(ctx, "select v from "+database+".baseline where id=1").Scan(&value))
		require.Equal(t, 9, value)
		// Warm the checkpoint definition, then publish additional DDL via other CN.
		createIncremental := "create table " + database + ".incremental(id int primary key,v int unique)"
		_, err = conn0.ExecContext(ctx, createIncremental)
		require.NoError(t, err)
		_, err = conn0.ExecContext(ctx, "insert into "+database+".incremental values(2,8)")
		require.NoError(t, err)
		require.NoError(t, db1.QueryRowContext(ctx, "select count(*) from "+database+".baseline{snapshot='"+snapshot+"'} where id=1 and v=7").Scan(&historical))
		require.Equal(t, 1, historical, "cold CN history after second checkpoint")
		require.NoError(t, db1.QueryRowContext(ctx, "select count(*) from "+database+".baseline{snapshot='"+snapshot+"'}").Scan(&historical))
		require.Equal(t, 1, historical, "snapshot excludes prior DELETE and later INSERT/UPDATE")
		require.NoError(t, db1.QueryRowContext(ctx, "select count(*) from "+database+".baseline").Scan(&historical))
		require.Equal(t, 2, historical, "latest rows are not duplicated by checkpoint replay")
		_, err = db1.ExecContext(ctx, "drop database "+database)
		require.NoError(t, err)
		var remaining int
		require.NoError(t, db1.QueryRowContext(ctx, "select count(*) from mo_catalog.mo_indexes where database_id=?", dbID).Scan(&remaining))
		require.Zero(t, remaining)
		require.NoError(t, db1.QueryRowContext(ctx, "select count(*) from mo_catalog.mo_tables where reldatabase_id=? and account_id=0", dbID).Scan(&remaining))
		require.Zero(t, remaining)
		require.NoError(t, conn0.QueryRowContext(ctx, "select count(*) from "+database+".baseline{snapshot='"+snapshot+"'} where id=1 and v=7").Scan(&historical))
		require.Equal(t, 1, historical, "DROP must preserve the previously readable snapshot")
		require.NoError(t, db1.QueryRowContext(ctx, "select count(*) from "+database+".unopened{snapshot='"+snapshot+"'} where id=42").Scan(&historical))
		require.Equal(t, 1, historical, "first cold historical open after DROP must retain persisted rows")
		// Exercise own uncommitted definitions through the same DROP owner.
		for _, q := range []string{"begin", "create database " + database, "create table " + database + ".own(id int primary key,v int unique)", "drop database " + database, "commit"} {
			_, err = conn0.ExecContext(ctx, q)
			require.NoError(t, err, q)
		}
		require.NoError(t, conn0.QueryRowContext(ctx, "select count(*) from mo_catalog.mo_database where datname=? and account_id=0", database).Scan(&remaining))
		require.Zero(t, remaining)
	})
}
