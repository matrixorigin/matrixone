// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package isolated

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/stretchr/testify/require"
)

func TestMandatoryRoleRulesAcrossConnections(t *testing.T) {
	dir := t.TempDir()
	socket := filepath.Join(dir, "tcp-only")
	require.NoError(t, os.WriteFile(socket, nil, 0600))
	cluster, err := embed.StartTestCluster(embed.WithDataPath(dir), embed.WithCNCount(2),
		embed.WithPreStart(func(service embed.ServiceOperator) {
			if service.ServiceType() == metadata.ServiceType_CN {
				service.Adjust(func(cfg *embed.ServiceConfig) {
					cfg.CN.Frontend.Host = "127.0.0.1"
					cfg.CN.Frontend.UnixSocketAddress = socket
					cfg.CN.Frontend.SkipCheckUser = false
					cfg.CN.SQLAddress = fmt.Sprintf("127.0.0.1:%d", cfg.CN.Frontend.Port)
				})
			}
		}))
	if cluster != nil {
		t.Cleanup(func() { require.NoError(t, cluster.Close()) })
	}
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()

	connect := func(cnIndex int, user string) *sql.Conn {
		t.Helper()
		cn, err := cluster.GetCNService(cnIndex)
		require.NoError(t, err)
		cfg := mysql.NewConfig()
		cfg.User, cfg.Passwd = user, "111"
		cfg.Net = "tcp"
		cfg.MultiStatements = true
		cfg.Addr = fmt.Sprintf("127.0.0.1:%d", cn.GetServiceConfig().CN.Frontend.Port)
		db, err := sql.Open("mysql", cfg.FormatDSN())
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, db.Close()) })
		require.Eventually(t, func() bool {
			pingCtx, cancel := context.WithTimeout(ctx, time.Second)
			defer cancel()
			return db.PingContext(pingCtx) == nil
		}, 30*time.Second, 100*time.Millisecond)
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, conn.Close()) })
		return conn
	}
	exec := func(conn *sql.Conn, statement string) {
		t.Helper()
		_, err := conn.ExecContext(ctx, statement)
		require.NoError(t, err)
	}
	assertIDs := func(rows *sql.Rows, err error, expected ...int) {
		t.Helper()
		require.NoError(t, err)
		defer rows.Close()
		cols, err := rows.Columns()
		require.NoError(t, err)
		require.Equal(t, []string{"id"}, cols)
		var ids []int
		for rows.Next() {
			var id int
			require.NoError(t, rows.Scan(&id))
			ids = append(ids, id)
		}
		require.NoError(t, rows.Err())
		require.Equal(t, expected, ids)
	}

	admin := connect(0, "root")
	for _, statement := range []string{
		"create database policy_fixture",
		"create table policy_fixture.docs(id int primary key, body varchar(48), embedding vecf32(3))",
		"create table policy_fixture.summary(id int primary key, value int)",
		"insert into policy_fixture.summary values (1,1)",
		"insert into policy_fixture.docs values (1,'orchard apple','[1,0,0]'),(2,'orchard pear','[2,0,0]'),(3,'orchard plum','[3,0,0]')",
		"create fulltext index docs_text on policy_fixture.docs(body)",
		"create index docs_ann using ivfflat on policy_fixture.docs(embedding) lists=1 op_type 'vector_l2_ops'",
		"create role policy_reader, policy_other",
		"alter role policy_other add rule 'select id from policy_fixture.docs where id = 2' on table policy_fixture.docs",
		"create user policy_test_user identified by '111' default role policy_reader",
		"grant connect on account * to policy_reader, policy_other",
		"grant select on table policy_fixture.docs to policy_reader, policy_other",
		"grant select, insert, update on table policy_fixture.summary to policy_reader",
		"grant policy_other to policy_test_user",
	} {
		exec(admin, statement)
	}
	reader := connect(1, "sys#policy_test_user#policy_reader")
	// Warm an empty policy with a connection-only control before publishing
	// the first rule on the other CN.
	for range 2 {
		rows, err := reader.QueryContext(ctx, "select 1 as id")
		assertIDs(rows, err, 1)
	}
	exec(admin, "alter role policy_reader add rule 'select id, body, embedding from policy_fixture.docs where id <= 2' on table policy_fixture.docs")
	const relational = "select id from policy_fixture.docs order by id"
	const fulltext = "select id from policy_fixture.docs where match(body) against('orchard') order by id"
	const vector = "select id from policy_fixture.docs order by l2_distance(embedding,'[0,0,0]') limit 2 by rank with option 'mode=pre'"
	for _, query := range []string{relational, fulltext, vector} {
		rows, err := reader.QueryContext(ctx, query)
		assertIDs(rows, err, 1, 2)
	}
	planContains := func(query, expected string) {
		t.Helper()
		rows, err := reader.QueryContext(ctx, "explain "+query)
		require.NoError(t, err)
		defer rows.Close()
		found := false
		for rows.Next() {
			var line string
			require.NoError(t, rows.Scan(&line))
			found = found || strings.Contains(line, expected)
		}
		require.NoError(t, rows.Err())
		require.True(t, found, "expected plan detail %q", expected)
	}
	planContains(vector, "Table Function on ivf_search")
	const refreshSummary = "insert into policy_fixture.summary values (1,1) on duplicate key update value = (select min(id) from policy_fixture.docs)"
	planContains(refreshSummary, "Filter Cond: (docs.id <= 2)")
	exec(reader, refreshSummary)
	var visibleMinimum int
	require.NoError(t, reader.QueryRowContext(ctx, "select value from policy_fixture.summary").Scan(&visibleMinimum))
	require.Equal(t, 1, visibleMinimum)

	exec(reader, "set @session_message = 'ready'")
	prepared, err := reader.PrepareContext(ctx, relational)
	require.NoError(t, err)
	defer prepared.Close()
	rows, err := prepared.QueryContext(ctx)
	assertIDs(rows, err, 1, 2)
	exec(reader, "prepare text_policy from 'select id from policy_fixture.docs order by id'")
	rows, err = reader.QueryContext(ctx, "execute text_policy")
	assertIDs(rows, err, 1, 2)

	// Update from another CN while the original restricted session stays open.
	exec(admin, "alter role policy_reader add rule 'select id, body, embedding from policy_fixture.docs where id = 1' on table policy_fixture.docs")
	for _, query := range []string{relational, fulltext, vector} {
		rows, err = reader.QueryContext(ctx, query)
		assertIDs(rows, err, 1)
	}
	_, err = prepared.QueryContext(ctx)
	require.ErrorContains(t, err, "role rule policy changed")
	_, err = reader.QueryContext(ctx, "execute text_policy")
	require.ErrorContains(t, err, "role rule policy changed")
	exec(reader, "deallocate prepare text_policy")
	exec(reader, "set role policy_other")
	rows, err = reader.QueryContext(ctx, "select * from policy_fixture.docs")
	assertIDs(rows, err, 2)
	_, err = reader.QueryContext(ctx, vector)
	require.ErrorContains(t, err, "embedding")
	// A committed catalog change also applies to the next statement in
	// the same request. The database belongs only to this cluster fixture.
	batch, err := admin.QueryContext(ctx, "alter role moadmin add rule 'select id from policy_fixture.docs where id = 1' on table policy_fixture.docs; "+relational)
	require.NoError(t, err)
	// go-sql-driver/mysql positions the reader on the first nonempty result.
	assertIDs(batch, nil, 1)
}
