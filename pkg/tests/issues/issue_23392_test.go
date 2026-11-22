// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
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
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/stretchr/testify/require"
)

func TestIssue23392RenameIndexInplace(t *testing.T) {
	embed.RunBaseClusterTests(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
		defer cancel()
		writer, err := cluster.GetCNService(0)
		require.NoError(t, err)
		reader, err := cluster.GetCNService(1)
		require.NoError(t, err)
		db := issue30936Open(t, cluster, writer)
		defer db.Close()
		other := issue30936Open(t, cluster, reader)
		defer other.Close()

		database := strings.ToLower(testutils.GetDatabaseName(t))
		table := database + ".target"
		execSQLRequire(t, ctx, db, "create database "+database)
		defer execSQLMaybe(t, context.Background(), db, "drop database if exists "+database)
		execSQLRequire(t, ctx, db, "create table "+table+
			" (id int primary key, v int, w int, body mediumtext, index ix_a(v,w), unique key ix_b(w), unique key spare(id,w))")
		execSQLRequire(t, ctx, db, "insert into "+table+" values (1, 10, 20, repeat('x', 70000))")

		type indexIdentity struct {
			tableID, indexID, hiddenTableID uint64
			position                        uint32
			column                          string
		}
		readIndex := func(name string) []indexIdentity {
			t.Helper()
			query := fmt.Sprintf("select i.table_id, i.id, h.rel_id, i.ordinal_position, i.column_name, i.name "+
				"from mo_catalog.mo_indexes i join mo_catalog.mo_tables p on i.table_id = p.rel_id "+
				"join mo_catalog.mo_tables h on i.index_table_name = h.relname and p.reldatabase_id = h.reldatabase_id "+
				"where p.reldatabase = '%s' and p.relname = 'target' order by i.ordinal_position", database)
			rows, err := other.QueryContext(ctx, query)
			require.NoError(t, err)
			defer rows.Close()
			var identities []indexIdentity
			for rows.Next() {
				var index indexIdentity
				var actualName string
				require.NoError(t, rows.Scan(&index.tableID, &index.indexID, &index.hiddenTableID, &index.position, &index.column, &actualName))
				if actualName == name {
					identities = append(identities, index)
				}
			}
			require.NoError(t, rows.Err())
			return identities
		}
		readShowCreate := func() string {
			t.Helper()
			var name, ddl string
			require.NoError(t, other.QueryRowContext(ctx, "show create table "+table).Scan(&name, &ddl))
			return ddl
		}
		readBodyHash := func() string {
			t.Helper()
			var digest string
			var length int
			require.NoError(t, other.QueryRowContext(ctx, "select length(body), md5(body) from "+table+" where id = 1").Scan(&length, &digest))
			require.Equal(t, 70000, length)
			return digest
		}

		type ownership struct {
			creator, owner uint32
			createdTime    string
			logicalID      uint64
		}
		readOwnership := func() ownership {
			t.Helper()
			query := fmt.Sprintf("select creator, owner, cast(created_time as char), rel_logical_id "+
				"from mo_catalog.mo_tables where reldatabase = '%s' and relname = 'target'", database)
			var result ownership
			require.NoError(t, other.QueryRowContext(ctx, query).Scan(&result.creator, &result.owner, &result.createdTime, &result.logicalID))
			return result
		}
		originalOwner := readOwnership()
		first := readIndex("ix_a")
		second := readIndex("ix_b")
		require.GreaterOrEqual(t, len(first), 2)
		require.Len(t, second, 1)
		originalHash := readBodyHash()
		execSQLRequire(t, ctx, db, "alter table "+table+
			" rename index IX_A to ix_temp, rename key ix_b to Ix_A, rename index ix_temp to Ix_B, algorithm=inplace")
		require.Equal(t, second, readIndex("Ix_A"))
		require.Equal(t, first, readIndex("Ix_B"))
		require.Empty(t, readIndex("ix_a"))
		require.Empty(t, readIndex("ix_temp"))
		require.Equal(t, originalHash, readBodyHash())
		require.Equal(t, originalOwner, readOwnership())
		require.Contains(t, readShowCreate(), "Ix_B")
		require.Contains(t, readShowCreate(), "Ix_A")

		// Exercise a failure after an earlier clause binds, then verify that
		// neither catalog nor engine constraint names changed.
		_, err = db.ExecContext(ctx, "alter table "+table+" rename index Ix_B to free_name, rename index missing to Ix_B")
		issue289RequireMySQLError(t, err, 1091)
		require.Equal(t, first, readIndex("Ix_B"))
		require.Empty(t, readIndex("free_name"))
		require.NotContains(t, readShowCreate(), "free_name")

		tx, err := db.BeginTx(ctx, nil)
		require.NoError(t, err)
		defer tx.Rollback()
		_, err = tx.ExecContext(ctx, "alter table "+table+" rename index Ix_B to rollback_name")
		require.NoError(t, err)
		require.NoError(t, tx.Rollback())
		require.Equal(t, first, readIndex("Ix_B"))
		require.Empty(t, readIndex("rollback_name"))
		require.NotContains(t, readShowCreate(), "rollback_name")

		_, err = other.ExecContext(ctx, "insert into "+table+" values (2, 11, 20, 'duplicate')")
		issue289RequireMySQLError(t, err, 1062)

		publication := database + "_pub"
		defer execSQLMaybe(t, context.Background(), db, "drop publication if exists "+publication)
		for _, source := range []string{database, "*"} {
			execSQLRequire(t, ctx, db, "create publication "+publication+" database "+source+" account all")
			_, err = other.ExecContext(ctx, "alter table "+table+" rename index Ix_B to published_name")
			require.ErrorContains(t, err, "published source database")
			require.Equal(t, first, readIndex("Ix_B"))
			require.Empty(t, readIndex("published_name"))
			execSQLRequire(t, ctx, db, "drop publication "+publication)
		}

		execSQLRequire(t, ctx, db, "create table "+database+".child (id int primary key, v int, "+
			"constraint fk foreign key(v) references "+table+"(w))")
		_, err = db.ExecContext(ctx, "alter table "+table+" rename index Ix_A to new_unique")
		issue289RequireMySQLError(t, err, 1553)
		execSQLRequire(t, ctx, db, "alter table "+table+" rename index spare to spare_renamed")
		require.NotEmpty(t, readIndex("spare_renamed"))
		execSQLRequire(t, ctx, db, "alter table "+table+" rename index Ix_B to allowed")
		require.Equal(t, first, readIndex("allowed"))
		require.Equal(t, second, readIndex("Ix_A"))
	})
}
