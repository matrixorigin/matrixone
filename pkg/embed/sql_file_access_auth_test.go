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

package embed

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSQLFileAccessAuthorization(t *testing.T) {
	RunSingleCNBaseClusterTests(t, func(c Cluster) {
		admin := openIcebergTestDB(t, c)
		defer admin.Close()

		const username = "sql_file_access_public"
		const password = "sql-file-access-test"
		mustExec(t, admin, fmt.Sprintf("create user %s identified by '%s' default role public", username, password))
		defer func() {
			_, _ = admin.Exec("drop user if exists " + username)
		}()

		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		publicDB, err := sql.Open("mysql", fmt.Sprintf("sys#%s#public:%s@tcp(127.0.0.1:%d)/", username, password, cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer publicDB.Close()

		dir := t.TempDir()
		input := filepath.Join(dir, "input.txt")
		output := filepath.Join(dir, "output.txt")
		missing := filepath.Join(dir, "denied", "missing.txt")
		require.NoError(t, os.WriteFile(input, []byte("owned test input"), 0o600))
		const dbName = "sql_file_access_auth_db"
		mustExec(t, admin, "create database "+dbName)
		defer func() { _, _ = admin.Exec("drop database if exists " + dbName) }()
		mustExec(t, admin, fmt.Sprintf("create table %s.file_paths (id int primary key, fpath datalink)", dbName))
		mustExec(t, admin, fmt.Sprintf("insert into %s.file_paths values (1, cast('file://%s' as datalink))", dbName, input))
		mustExec(t, admin, fmt.Sprintf("grant select on table %s.file_paths to public", dbName))

		var content string
		err = publicDB.QueryRow(fmt.Sprintf("select load_file('%s')", input)).Scan(&content)
		require.ErrorContains(t, err, "only the sys account moadmin role")
		err = publicDB.QueryRow(fmt.Sprintf("select load_file('%s')", missing)).Scan(&content)
		require.ErrorContains(t, err, "only the sys account moadmin role")
		err = publicDB.QueryRow(fmt.Sprintf("select load_file(fpath) from %s.file_paths", dbName)).Scan(&content)
		require.ErrorContains(t, err, "only the sys account moadmin role")
		_, err = os.Stat(filepath.Dir(missing))
		require.ErrorIs(t, err, os.ErrNotExist, "authorization must precede LocalETLFS directory creation")
		_, err = publicDB.Exec(fmt.Sprintf("select save_file(cast('file://%s' as datalink), 'owned test output')", output))
		require.ErrorContains(t, err, "only the sys account moadmin role")
		_, err = os.Stat(output)
		require.ErrorIs(t, err, os.ErrNotExist)

		var adminContent string
		require.NoError(t, admin.QueryRow(fmt.Sprintf("select load_file('%s')", input)).Scan(&adminContent))
		require.Equal(t, "owned test input", adminContent)
		require.NoError(t, admin.QueryRow(fmt.Sprintf("select load_file(fpath) from %s.file_paths", dbName)).Scan(&adminContent))
		require.Equal(t, "owned test input", adminContent)
		var written int64
		require.NoError(t, admin.QueryRow(fmt.Sprintf("select save_file(cast('file://%s' as datalink), 'owned test output')", output)).Scan(&written))
		require.Equal(t, int64(len("owned test output")), written)
		data, err := os.ReadFile(output)
		require.NoError(t, err)
		require.Equal(t, "owned test output", string(data))

		// The process is reused between statements. Verify that a role change
		// cannot leave the previous moadmin identity cached, including through a
		// prepared statement, and that switching back restores the same access.
		adminConn, err := admin.Conn(t.Context())
		require.NoError(t, err)
		defer adminConn.Close()
		stmt, err := adminConn.PrepareContext(t.Context(), "select load_file(?)")
		require.NoError(t, err)
		defer stmt.Close()
		_, err = adminConn.ExecContext(t.Context(), "set role public")
		require.NoError(t, err)
		err = stmt.QueryRowContext(t.Context(), input).Scan(&content)
		require.ErrorContains(t, err, "only the sys account moadmin role")
		_, err = adminConn.ExecContext(t.Context(), "set role moadmin")
		require.NoError(t, err)
		require.NoError(t, stmt.QueryRowContext(t.Context(), input).Scan(&content))
		require.Equal(t, "owned test input", content)
	})
}

func TestSQLFileAccessTenantStageCDC(t *testing.T) {
	RunSingleCNBaseClusterTests(t, func(c Cluster) {
		admin := openIcebergTestDB(t, c)
		defer admin.Close()
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		const account = "sql_file_access_stage_account"
		const dbName = "sql_file_access_stage_db"
		mustExec(t, admin, "create account "+account+" admin_name 'admin' identified by 'sql-file-access-stage-test'")
		defer func() { _, _ = admin.Exec("drop account if exists " + account) }()

		dir := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(dir, "content.txt"), []byte("matrixone stage content"), 0o600))
		require.NoError(t, os.WriteFile(filepath.Join(dir, "json.txt"), []byte("[1]"), 0o600))
		tenant, err := sql.Open("mysql", fmt.Sprintf("%s#admin#accountadmin:sql-file-access-stage-test@tcp(127.0.0.1:%d)/", account, cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer tenant.Close()

		mustExec(t, tenant, "set experimental_fulltext2_index = 1")
		mustExec(t, tenant, "create database "+dbName)
		defer func() { _, _ = tenant.Exec("drop database if exists " + dbName) }()
		mustExec(t, tenant, "use "+dbName)
		mustExec(t, tenant, fmt.Sprintf("create stage sql_file_access_stage URL='file://%s'", dir))
		defer func() { _, _ = tenant.Exec("drop stage if exists sql_file_access_stage") }()
		var stageContent string
		err = tenant.QueryRow("select load_file(cast('stage://sql_file_access_stage/content.txt' as datalink))").Scan(&stageContent)
		require.ErrorContains(t, err, "only the sys account moadmin role", "foreground tenant stage access must remain denied")
		var jsonValue int64
		err = tenant.QueryRow("select jt.n from json_table(cast('stage://sql_file_access_stage/json.txt' as datalink), '$[*]' columns(n int path '$')) jt").Scan(&jsonValue)
		require.ErrorContains(t, err, "only the sys account moadmin role", "indirect SQL Datalink consumers must enforce the same policy")
		mustExec(t, tenant, "create table documents (id bigint primary key, fpath datalink, fulltext2 ftidx(fpath))")
		mustExec(t, tenant, "insert into documents values (1, 'stage://sql_file_access_stage/content.txt')")

		// This is the real tenant CDC path: the background writer must resolve the
		// tenant-owned stage and use the narrow trusted stage-origin exception.
		require.Eventually(t, func() bool {
			var id int64
			return tenant.QueryRow("select id from documents where match(fpath) against('matrixone')").Scan(&id) == nil && id == 1
		}, 30*time.Second, 250*time.Millisecond)

		// Exercise the server-owned spill-file path with a non-empty CREATE and a
		// later compaction. Both statements run as the tenant; their generated
		// load_file INSERTs must use the scoped storage identity, while the user's
		// session identity remains unchanged.
		mustExec(t, tenant, "create table build_documents (id bigint primary key, body text)")
		defer func() { _, _ = tenant.Exec("drop table if exists build_documents") }()
		mustExec(t, tenant, "insert into build_documents values (1, 'initial storage build')")
		mustExec(t, tenant, "create fulltext2 index build_ftidx on build_documents(body)")
		var id int64
		require.NoError(t, tenant.QueryRow("select id from build_documents where match(body) against('initial')").Scan(&id))
		require.Equal(t, int64(1), id)

		mustExec(t, tenant, "insert into build_documents values (2, 'compaction storage build')")
		var store, meta string
		require.NoError(t, tenant.QueryRow("select index_table_name from mo_catalog.mo_indexes where name = 'build_ftidx' and algo_table_type = 'ftv2_index' and table_id in (select rel_id from mo_catalog.mo_tables where reldatabase = database() and relname = 'build_documents') limit 1").Scan(&store))
		require.NoError(t, tenant.QueryRow("select index_table_name from mo_catalog.mo_indexes where name = 'build_ftidx' and algo_table_type = 'ftv2_meta' and table_id in (select rel_id from mo_catalog.mo_tables where reldatabase = database() and relname = 'build_documents') limit 1").Scan(&meta))
		require.Eventually(t, func() bool {
			var n int
			return tenant.QueryRow(fmt.Sprintf("select count(*) from `%s`.`%s` where index_id = 'cdc_tail' and tag = 1", dbName, store)).Scan(&n) == nil && n > 0
		}, 30*time.Second, 250*time.Millisecond)
		mustExec(t, tenant, fmt.Sprintf("select * from fulltext2_compact('%s', '%s', '%s', '1') as f", dbName, store, meta))
		require.NoError(t, tenant.QueryRow("select id from build_documents where match(body) against('compaction')").Scan(&id))
		require.Equal(t, int64(2), id)

		// HNSW uses the same server-owned spill-file INSERT boundary. Keep this
		// tiny and synchronous so it remains a focused authorization oracle.
		mustExec(t, tenant, "set experimental_hnsw_index = 1")
		mustExec(t, tenant, "create table build_vectors (id bigint primary key, v vecf32(3))")
		defer func() { _, _ = tenant.Exec("drop table if exists build_vectors") }()
		mustExec(t, tenant, "insert into build_vectors values (1, '[1,0,0]'), (2, '[0,1,0]')")
		mustExec(t, tenant, "create index build_hidx using hnsw on build_vectors(v) op_type 'vector_l2_ops' M 16 EF_CONSTRUCTION 32 EF_SEARCH 32 FORCE_SYNC")
		var hnswMeta string
		require.NoError(t, tenant.QueryRow("select index_table_name from mo_catalog.mo_indexes where name = 'build_hidx' and algo = 'hnsw' and algo_table_type = 'hnsw_meta' and table_id in (select rel_id from mo_catalog.mo_tables where reldatabase = database() and relname = 'build_vectors') limit 1").Scan(&hnswMeta))
		var filesz int64
		require.NoError(t, tenant.QueryRow(fmt.Sprintf("select filesize from `%s`.`%s` limit 1", dbName, hnswMeta)).Scan(&filesz))
		require.Greater(t, filesz, int64(0))
	})
}
