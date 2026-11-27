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
	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	qclient "github.com/matrixorigin/matrixone/pkg/queryservice/client"
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
	assertIDs := func(query func() (*sql.Rows, error), expected ...int) {
		t.Helper()
		rows, err := query()
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

	selectIDs := func(conn *sql.Conn, stmt string, expected ...int) {
		t.Helper()
		assertIDs(func() (*sql.Rows, error) { return conn.QueryContext(ctx, stmt) }, expected...)
	}

	admin := connect(0, "root")
	for _, statement := range []string{
		"create database policy_fixture",
		"create table policy_fixture.docs(id int primary key, body varchar(48), embedding vecf32(3), note varchar(16))",
		"create table policy_fixture.summary(id int primary key, value int)",
		"insert into policy_fixture.summary values (1,1)",
		"insert into policy_fixture.docs values (1,'orchard apple','[1,0,0]','a'),(2,'orchard pear','[2,0,0]','b'),(3,'orchard plum','[0,0,0]','c')",
		"create fulltext index docs_text on policy_fixture.docs(body)",
		"create index docs_ann using ivfflat on policy_fixture.docs(embedding) lists=1 op_type 'vector_l2_ops'",
		"set view_security_type = 'INVOKER'",
		"create view policy_fixture.docs_view as select * from policy_fixture.docs",
		"create view policy_fixture.docs_nested as select * from policy_fixture.docs_view",
		"create role policy_reader, policy_other",
		"alter role policy_other add rule 'select id from policy_fixture.docs where id = 2' on table policy_fixture.docs",
		"create user policy_test_user identified by '111' default role policy_reader",
		"grant connect on account * to policy_reader, policy_other",
		"grant select on table policy_fixture.docs to policy_reader, policy_other",
		"grant select on view policy_fixture.docs_view to policy_reader",
		"grant select on view policy_fixture.docs_nested to policy_reader",
		"grant insert, update, delete on table policy_fixture.docs to policy_reader",
		"grant select, insert, update, delete on table policy_fixture.summary to policy_reader",
		"grant policy_other to policy_test_user",
	} {
		exec(admin, statement)
	}
	exec(admin, "create snapshot policy_history for table policy_fixture docs")
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, err := admin.ExecContext(ctx, "drop snapshot if exists policy_history")
		require.NoError(t, err)
	})
	exec(admin, "insert into policy_fixture.docs values (4,'late row','[8,0,0]','d')")
	const readerLogin = "sys#policy_test_user#policy_reader"
	reader := connect(1, readerLogin)
	observer := connect(1, "root")
	publishPolicy := func(rule string) {
		t.Helper()
		exec(admin, fmt.Sprintf("alter role policy_reader add rule '%s' on table policy_fixture.docs", rule))
		// Wait for the receiving CN to observe the committed catalog row
		// before testing its existing restricted connection against that view.
		require.Eventually(t, func() bool {
			var name, received string
			err := observer.QueryRowContext(ctx, "show rules on role policy_reader").Scan(&name, &received)
			return err == nil && name == "policy_fixture.docs" && received == rule
		}, 10*time.Second, 50*time.Millisecond)
	}
	migrationTarget := connect(0, readerLogin)
	connectionID := func(conn *sql.Conn) uint32 {
		t.Helper()
		var id uint32
		require.NoError(t, conn.QueryRowContext(ctx, "select connection_id()").Scan(&id))
		return id
	}
	sourceCN, err := cluster.GetCNService(1)
	require.NoError(t, err)
	targetCN, err := cluster.GetCNService(0)
	require.NoError(t, err)
	clusterView := clusterservice.GetMOCluster(sourceCN.ServiceID())
	queryAddress := func(cn embed.ServiceOperator) string {
		t.Helper()
		var address string
		require.Eventually(t, func() bool {
			clusterView.GetCNService(clusterservice.NewSelector().SelectByServiceID(cn.ServiceID()), func(service metadata.CNService) bool {
				address = service.QueryAddress
				return false
			})
			return address != ""
		}, 15*time.Second, 100*time.Millisecond)
		return address
	}
	sourceAddress, targetAddress := queryAddress(sourceCN), queryAddress(targetCN)
	rpc, err := qclient.NewQueryClient(sourceCN.ServiceID(), morpc.Config{})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, rpc.Close()) })
	export := rpc.NewRequest(query.CmdMethod_MigrateConnFrom)
	export.MigrateConnFromRequest = &query.MigrateConnFromRequest{ConnID: connectionID(reader)}
	destinationID := connectionID(migrationTarget)

	exec(reader, "prepare move_doc from 'select id from policy_fixture.docs where id = ?'")
	native, err := reader.PrepareContext(ctx, "select id from policy_fixture.docs where id = ?")
	require.NoError(t, err)
	defer native.Close()
	assertIDs(func() (*sql.Rows, error) { return native.QueryContext(ctx, 2) }, 2)
	exported, err := rpc.SendMessage(ctx, sourceAddress, export)
	require.NoError(t, err)
	defer rpc.Release(exported)
	require.Len(t, exported.MigrateConnFromResponse.PrepareStmts, 2)

	replay := func(id uint32) error {
		call := rpc.NewRequest(query.CmdMethod_MigrateConnTo)
		call.MigrateConnToRequest = &query.MigrateConnToRequest{
			ConnID: id, PrepareStmts: exported.MigrateConnFromResponse.PrepareStmts, RoleState: exported.MigrateConnFromResponse.RoleState,
		}
		resp, err := rpc.SendMessage(ctx, targetAddress, call)
		if err == nil {
			ack := resp.GetMigrateConnToResponse()
			require.NotNil(t, ack)
			require.True(t, ack.Success)
		}
		if resp != nil {
			rpc.Release(resp)
		}
		return err
	}
	require.NoError(t, replay(destinationID))
	exec(migrationTarget, "set @move_doc_id = 2")
	for _, stmt := range exported.MigrateConnFromResponse.PrepareStmts {
		selectIDs(migrationTarget, fmt.Sprintf("execute %s using @move_doc_id", stmt.Name), 2)
	}

	// Warm an empty policy with a connection-only control before publishing
	// the first rule on the other CN.
	for range 2 {
		selectIDs(reader, "select 1 as id", 1)
	}
	publishPolicy("select id, body, embedding from policy_fixture.docs where id <= 2")
	// The old source handle and a stale export are both ineligible once
	// the receiving statement would be subject to a new mandatory policy.
	export = rpc.NewRequest(query.CmdMethod_MigrateConnFrom)
	export.MigrateConnFromRequest = &query.MigrateConnFromRequest{ConnID: connectionID(reader)}
	_, err = rpc.SendMessage(ctx, sourceAddress, export)
	require.True(t, moerr.IsMoErrCode(err, moerr.OkExpectedMigrationPolicyChanged), "%v", err)
	err = replay(connectionID(connect(0, readerLogin)))
	require.True(t, moerr.IsMoErrCode(err, moerr.OkExpectedMigrationPolicyChanged), "%v", err)

	const relational = "select id from policy_fixture.docs order by id"
	const fulltext = "select * from policy_fixture.docs where match(body) against('orchard') order by id"
	const vector = "select * from policy_fixture.docs order by l2_distance(embedding,'[0,0,0]') limit 2 by rank with option 'mode=pre'"
	const nestedView = "select * from policy_fixture.docs_nested order by id"
	type doc struct {
		id              int
		body, embedding string
	}
	first := doc{1, "orchard apple", "[1, 0, 0]"}
	second := doc{2, "orchard pear", "[2, 0, 0]"}
	assertDocs := func(query string, expected ...doc) {
		t.Helper()
		rows, err := reader.QueryContext(ctx, query)
		require.NoError(t, err)
		defer rows.Close()
		cols, err := rows.Columns()
		require.NoError(t, err)
		require.Equal(t, []string{"id", "body", "embedding"}, cols)
		var actual []doc
		for rows.Next() {
			var d doc
			require.NoError(t, rows.Scan(&d.id, &d.body, &d.embedding))
			actual = append(actual, d)
		}
		require.NoError(t, rows.Err())
		require.Equal(t, expected, actual)
	}
	assertQueryDenied := func(query func() (*sql.Rows, error), expected string) {
		t.Helper()
		rows, err := query()
		if rows != nil {
			defer rows.Close()
			require.NoError(t, rows.Err())
		}
		require.ErrorContains(t, err, expected)
	}
	assertDenied := func(query, expected string) {
		t.Helper()
		assertQueryDenied(func() (*sql.Rows, error) { return reader.QueryContext(ctx, query) }, expected)
	}
	for _, query := range []string{fulltext, vector, nestedView} {
		assertDocs(query, first, second)
	}
	const historical = "select id from policy_fixture.docs {snapshot = 'policy_history'} order by id"
	selectIDs(admin, historical, 1, 2, 3)
	selectIDs(admin, relational, 1, 2, 3, 4)
	assertDenied(historical, "table history qualifiers are not supported with active role-rule rewrites")
	var table string
	require.NoError(t, reader.QueryRowContext(ctx, "show tables from policy_fixture like 'docs'").Scan(&table))
	require.Equal(t, "docs", table)
	require.ErrorIs(t, reader.QueryRowContext(ctx, "show tables from policy_fixture where 1 = 0").Scan(&table), sql.ErrNoRows)
	assertDenied("show tables from policy_fixture where length('x') = 1", "cannot enforce active role rules")

	// The write is authorized and the result shape depends on the target policy.
	var returned int
	require.NoError(t, reader.QueryRowContext(ctx, "insert into policy_fixture.summary values (10,10) returning id").Scan(&returned))
	require.Equal(t, 10, returned)
	unboundUpdate, err := reader.PrepareContext(ctx, "update policy_fixture.summary set value = ? where id = ?")
	require.NoError(t, err)
	defer unboundUpdate.Close()
	updated, err := unboundUpdate.ExecContext(ctx, 20, 10)
	require.NoError(t, err)
	count, err := updated.RowsAffected()
	require.NoError(t, err)
	require.EqualValues(t, 1, count)
	var value int
	require.NoError(t, admin.QueryRowContext(ctx, "select value from policy_fixture.summary where id = 10").Scan(&value))
	require.Equal(t, 20, value)
	exec(reader, "delete from policy_fixture.summary where id = 10")
	selectIDs(admin, "select id from policy_fixture.summary order by id", 1)
	assertDenied("update policy_fixture.docs set note = 'blocked' where id = 1", "write target cannot enforce active role rules")
	assertDenied("delete from policy_fixture.docs where id = 1", "write target cannot enforce active role rules")
	assertDenied("delete from policy_fixture.summary where id in (select 1)", "cannot enforce active role rules")
	var note string
	require.NoError(t, admin.QueryRowContext(ctx, "select note from policy_fixture.docs where id = 1").Scan(&note))
	require.Equal(t, "a", note)
	exec(reader, "insert into policy_fixture.docs values (10,'other','[8,0,0]','d')")
	assertDenied("insert into policy_fixture.docs values (11,'other','[8,0,0]','e') returning id", "INSERT RETURNING cannot enforce active role rules")
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
	const refreshSummary = "insert into policy_fixture.summary values (1,1) on duplicate key update value = (select count(*) from policy_fixture.docs)"
	planContains(refreshSummary, "Filter Cond: (docs.id <= 2)")
	exec(reader, refreshSummary)
	var visibleCount int
	require.NoError(t, reader.QueryRowContext(ctx, "select value from policy_fixture.summary where id = 1").Scan(&visibleCount))
	require.Equal(t, 2, visibleCount)

	exec(reader, "set @session_message = 'ready'")
	exec(reader, "set @visible_docs = (select count(*) from policy_fixture.docs)")
	var setCount int
	require.NoError(t, reader.QueryRowContext(ctx, "select @visible_docs").Scan(&setCount))
	require.Equal(t, 2, setCount)
	prepared, err := reader.PrepareContext(ctx, relational)
	require.NoError(t, err)
	defer prepared.Close()
	assertIDs(func() (*sql.Rows, error) { return prepared.QueryContext(ctx) }, 1, 2)
	exec(reader, "prepare text_policy from 'select id from policy_fixture.docs order by id'")
	selectIDs(reader, "execute text_policy", 1, 2)

	// Update from another CN while the original restricted session stays open.
	publishPolicy("select id, body, embedding from policy_fixture.docs where id = 1")
	selectIDs(reader, relational, 1)
	for _, query := range []string{fulltext, vector, nestedView} {
		assertDocs(query, first)
	}
	assertQueryDenied(func() (*sql.Rows, error) { return prepared.QueryContext(ctx) }, "role rule policy changed")
	assertDenied("execute text_policy", "role rule policy changed")
	exec(reader, "deallocate prepare text_policy")
	// Object denial is a separate assertion from cross-CN policy publication.
	// Revoke on the reader's CN so this check observes its committed DDL.
	exec(observer, "revoke select on table policy_fixture.docs from policy_reader")
	for _, query := range []string{relational, fulltext, vector, nestedView} {
		assertDenied(query, "privilege")
	}
	exec(reader, "set role policy_other")
	selectIDs(reader, "select * from policy_fixture.docs", 2)
	assertDenied(vector, "embedding")
	// A committed catalog change also applies to the next statement in
	// the same request. The database belongs only to this cluster fixture.
	batch, err := admin.QueryContext(ctx, "alter role moadmin add rule 'select id from policy_fixture.docs where id = 1' on table policy_fixture.docs; "+relational)
	require.NoError(t, err)
	defer batch.Close()
	// go-sql-driver/mysql positions the reader on the first nonempty result.
	assertIDs(func() (*sql.Rows, error) { return batch, nil }, 1)
	require.NoError(t, batch.Err())

	exec(admin, "set global lower_case_table_names = 0")
	caseAdmin := connect(0, "root")
	for _, statement := range []string{
		"create database PolicyCase",
		"create table PolicyCase.Docs(id int primary key)",
		"create table PolicyCase.docs(id int primary key)",
		"insert into PolicyCase.Docs values (11),(12)",
		"insert into PolicyCase.docs values (21),(22)",
		"create role policy_case_role",
		"create user policy_case_user identified by '111' default role policy_case_role",
		"grant connect on account * to policy_case_role",
		"grant select on table PolicyCase.Docs to policy_case_role",
		"grant select on table PolicyCase.docs to policy_case_role",
		"alter role policy_case_role add rule 'select id from PolicyCase.Docs where id = 11' on table PolicyCase.Docs",
		"alter role policy_case_role add rule 'select id from PolicyCase.docs where id = 21' on table PolicyCase.docs",
	} {
		exec(caseAdmin, statement)
	}
	caseReader := connect(0, "sys#policy_case_user#policy_case_role")
	selectIDs(caseReader, "select id from PolicyCase.Docs", 11)
	selectIDs(caseReader, "select id from PolicyCase.docs", 21)
	exec(caseAdmin, "alter role policy_case_role drop rule on table PolicyCase.Docs")
	var ruleName, ruleSQL string
	require.NoError(t, caseAdmin.QueryRowContext(ctx, "show rules on role policy_case_role").Scan(&ruleName, &ruleSQL))
	require.Equal(t, "PolicyCase.docs", ruleName)
	selectIDs(caseReader, "select id from PolicyCase.docs", 21)

	exec(caseAdmin, "alter role policy_case_role add rule 'select id from PolicyCase.Docs where id = 11' on table PolicyCase.Docs")
	selectIDs(caseReader, "select id from PolicyCase.Docs", 11)
	for _, statement := range []string{
		"create database legacy_case",
		"create table legacy_case.docs(id int primary key)",
		"insert into legacy_case.docs values (31),(32)",
		"grant select on table legacy_case.docs to policy_case_role",
		"alter role policy_case_role add rule 'select id from legacy_case.docs where id = 31' on table legacy_case.docs",
	} {
		exec(caseAdmin, statement)
	}
	selectIDs(caseReader, "select id from legacy_case.docs", 31)
	legacyPrepared, err := caseReader.PrepareContext(ctx, "select id from legacy_case.docs")
	require.NoError(t, err)
	defer legacyPrepared.Close()
	assertIDs(func() (*sql.Rows, error) { return legacyPrepared.QueryContext(ctx) }, 31)
	exec(caseAdmin, "create table legacy_case.Docs(id int primary key)")
	exec(caseAdmin, "grant select on table legacy_case.Docs to policy_case_role")
	for _, name := range []string{"legacy_case.docs", "legacy_case.Docs"} {
		assertQueryDenied(func() (*sql.Rows, error) {
			return caseReader.QueryContext(ctx, "select id from "+name)
		}, "ambiguous case spelling")
	}
	assertQueryDenied(func() (*sql.Rows, error) { return legacyPrepared.QueryContext(ctx) }, "role rule policy changed")
	exec(caseAdmin, "alter role policy_case_role drop rule on table legacy_case.docs")

	exec(caseAdmin, "set global lower_case_table_names = 2")
	preservingReader := connect(0, "sys#policy_case_user#policy_case_role")
	selectIDs(preservingReader, "select id from PolicyCase.Docs", 11)
	selectIDs(preservingReader, "select docs.* from PolicyCase.docs", 21)
	selectIDs(preservingReader, "select Docs.id from PolicyCase.Docs", 11)
	exec(caseAdmin, "set global lower_case_table_names = 1")
	selectIDs(caseReader, "select id from PolicyCase.Docs", 11)
	foldingAdmin := connect(0, "root")
	exec(foldingAdmin, "alter role policy_case_role drop rule on table PolicyCase.Docs")
	require.ErrorIs(t, foldingAdmin.QueryRowContext(ctx, "show rules on role policy_case_role").Scan(&ruleName, &ruleSQL), sql.ErrNoRows)

	// An authorized administrator still cannot physically export rows while
	// subject to a mandatory policy. Refusal must precede manifest creation.
	exec(foldingAdmin, "alter role moadmin add rule 'select id from policy_fixture.summary where id = 1' on table policy_fixture.summary")
	defer exec(foldingAdmin, "alter role moadmin drop rule on table policy_fixture.summary")
	destination := filepath.Join(t.TempDir(), "export")
	_, err = foldingAdmin.ExecContext(ctx, fmt.Sprintf("dump table policy_fixture.summary to '%s'", destination))
	require.ErrorContains(t, err, "cannot enforce active role rules")
	require.NoFileExists(t, filepath.Join(destination, "manifest.json"))
}
