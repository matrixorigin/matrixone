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
	mysql "github.com/go-sql-driver/mysql"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
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

		// OUTFILE and server-side LOAD DATA must enforce the same file contract.
		exportPath := filepath.Join(dir, "denied-export", "output.csv")
		_, err = publicDB.Exec(fmt.Sprintf("select 1 into outfile '%s'", exportPath))
		require.ErrorContains(t, err, "only the sys account moadmin role")
		_, err = os.Stat(filepath.Dir(exportPath))
		require.ErrorIs(t, err, os.ErrNotExist)
		mustExec(t, admin, fmt.Sprintf("create table %s.import_rows (id int)", dbName))
		mustExec(t, admin, fmt.Sprintf("grant insert on table %s.import_rows to public", dbName))
		csvPath := filepath.Join(dir, "input.csv")
		require.NoError(t, os.WriteFile(csvPath, []byte("7\n"), 0o600))
		_, err = publicDB.Exec(fmt.Sprintf("load data infile '%s' into table %s.import_rows", csvPath, dbName))
		require.ErrorContains(t, err, "only the sys account moadmin role")
		var rowCount int
		require.NoError(t, admin.QueryRow(fmt.Sprintf("select count(*) from %s.import_rows", dbName)).Scan(&rowCount))
		require.Zero(t, rowCount)

		// LOCAL is data sent by the client, not an operation on a CN file.
		const handlerName = "sql_file_access_client"
		mysql.RegisterReaderHandler(handlerName, func() io.Reader { return strings.NewReader("8\n") })
		defer mysql.DeregisterReaderHandler(handlerName)
		mustExec(t, publicDB, fmt.Sprintf("load data local infile 'Reader::%s' into table %s.import_rows", handlerName, dbName))
		var imported int
		require.NoError(t, admin.QueryRow(fmt.Sprintf("select id from %s.import_rows", dbName)).Scan(&imported))
		require.Equal(t, 8, imported)

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
		roleExport := filepath.Join(dir, "role-export.csv")
		_, err = adminConn.ExecContext(t.Context(), fmt.Sprintf("select 7 into outfile '%s' header 'false'", roleExport))
		require.ErrorContains(t, err, "only the sys account moadmin role")

		_, err = adminConn.ExecContext(t.Context(), "set role moadmin")
		require.NoError(t, err)
		require.NoError(t, stmt.QueryRowContext(t.Context(), input).Scan(&content))
		require.Equal(t, "owned test input", content)
		_, err = adminConn.ExecContext(t.Context(), fmt.Sprintf("select 7 into outfile '%s' header 'false'", roleExport))
		require.NoError(t, err)
		exported, err := os.ReadFile(roleExport)
		require.NoError(t, err)
		require.Equal(t, "7\n", string(exported))

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
		mustExec(t, tenant, fmt.Sprintf("create stage sql_file_access_local_stage URL='file://%s'", dir))
		defer func() { _, _ = tenant.Exec("drop stage if exists sql_file_access_local_stage") }()
		remoteObjects := map[string][]byte{"/bucket/content.txt": []byte("matrixone stage content"), "/bucket/json.txt": []byte("[1]")}
		for _, name := range []string{"mo.pdf", "chinese.pdf", "file-sample_100kB.docx"} {
			data, err := os.ReadFile(filepath.Join("..", "..", "test", "distributed", "resources", "fulltext", name))
			require.NoError(t, err)
			remoteObjects["/bucket/"+name] = data
		}
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			data, ok := remoteObjects[r.URL.Path]
			if !ok {
				http.NotFound(w, r)
				return
			}
			w.Header().Set("Content-Length", strconv.Itoa(len(data)))
			if r.Method != http.MethodHead {
				_, _ = w.Write(data)
			}
		}))
		defer server.Close()
		mustExec(t, tenant, fmt.Sprintf("create stage sql_file_access_stage URL='s3://bucket/' CREDENTIALS={'AWS_KEY_ID'='id','AWS_SECRET_KEY'='secret','AWS_REGION'='us-east-1','PROVIDER'='amazon','ENDPOINT'='%s'}", server.URL))
		defer func() { _, _ = tenant.Exec("drop stage if exists sql_file_access_stage") }()
		var stageContent string
		err = tenant.QueryRow("select load_file(cast('stage://sql_file_access_local_stage/content.txt' as datalink))").Scan(&stageContent)
		require.ErrorContains(t, err, "only the sys account moadmin role", "foreground tenant stage access must remain denied")
		var jsonValue int64
		err = tenant.QueryRow("select jt.n from json_table(cast('stage://sql_file_access_local_stage/json.txt' as datalink), '$[*]' columns(n int path '$')) jt").Scan(&jsonValue)
		require.ErrorContains(t, err, "only the sys account moadmin role", "indirect SQL Datalink consumers must enforce the same policy")
		// A tenant stage owner may manage remote objects, but owning stage
		// metadata does not grant access to server-local files.
		_, err = tenant.Exec(fmt.Sprintf("select 1 into outfile 'stage://sql_file_access_local_stage/export.csv'"))
		require.ErrorContains(t, err, "only the sys account moadmin role")
		_, err = tenant.Exec("remove files from stage 'stage://sql_file_access_local_stage/content.txt'")
		require.ErrorContains(t, err, "only the sys account moadmin role")
		localContent, err := os.ReadFile(filepath.Join(dir, "content.txt"))
		require.NoError(t, err)
		require.Equal(t, "matrixone stage content", string(localContent))
		require.NoError(t, tenant.QueryRow("select load_file(cast('stage://sql_file_access_stage/content.txt' as datalink))").Scan(&stageContent))
		require.Equal(t, "matrixone stage content", stageContent)
		require.NoError(t, tenant.QueryRow("select jt.n from json_table(cast('stage://sql_file_access_stage/json.txt' as datalink), '$[*]' columns(n int path '$')) jt").Scan(&jsonValue))
		require.Equal(t, int64(1), jsonValue)

		mustExec(t, tenant, "create table documents (id bigint primary key, fpath datalink, fulltext2 ftidx(fpath))")
		mustExec(t, tenant, "insert into documents values (1, 'stage://sql_file_access_stage/content.txt'), (2, 'stage://sql_file_access_stage/mo.pdf'), (3, 'stage://sql_file_access_stage/chinese.pdf')")

		// This is the real tenant CDC path: foreground and background execution
		// both use the same remote-stage contract, without a local-file exception.
		require.Eventually(t, func() bool {
			var id int64
			return tenant.QueryRow("select id from documents where id=1 and match(fpath) against('matrixone')").Scan(&id) == nil && id == 1
		}, 30*time.Second, 250*time.Millisecond)

		var documentID int64
		require.NoError(t, tenant.QueryRow("select id from documents where id=2 and match(fpath) against('matrixone')").Scan(&documentID))
		require.Equal(t, int64(2), documentID)
		require.NoError(t, tenant.QueryRow("select id from documents where id=3 and match(fpath) against('慢慢地' in natural language mode)").Scan(&documentID))
		require.Equal(t, int64(3), documentID)

		// Preserve the async content-update oracle on a fresh search object:
		// wait on its durable tail before its first MATCH can cache a snapshot.
		mustExec(t, tenant, "create table mutation_documents (id bigint primary key, fpath datalink, fulltext2 mutation_ftidx(fpath))")
		defer func() { _, _ = tenant.Exec("drop table if exists mutation_documents") }()
		mustExec(t, tenant, "insert into mutation_documents values (4, 'stage://sql_file_access_stage/mo.pdf')")
		var mutationStore string
		require.NoError(t, tenant.QueryRow("select index_table_name from mo_catalog.mo_indexes where name='mutation_ftidx' and algo_table_type='ftv2_index' and table_id in (select rel_id from mo_catalog.mo_tables where reldatabase=database() and relname='mutation_documents') limit 1").Scan(&mutationStore))
		tailSQL := fmt.Sprintf("select coalesce(max(chunk_id),-1) from `%s`.`%s` where index_id='cdc_tail' and tag=1", dbName, mutationStore)
		var beforeTail int64
		require.Eventually(t, func() bool { return tenant.QueryRow(tailSQL).Scan(&beforeTail) == nil && beforeTail >= 0 }, 30*time.Second, 250*time.Millisecond)
		mustExec(t, tenant, "update mutation_documents set fpath='stage://sql_file_access_stage/file-sample_100kB.docx' where id=4")
		require.Eventually(t, func() bool { var now int64; return tenant.QueryRow(tailSQL).Scan(&now) == nil && now > beforeTail }, 30*time.Second, 250*time.Millisecond)
		require.NoError(t, tenant.QueryRow("select id from mutation_documents where match(fpath) against('Nulla facilisi' in natural language mode)").Scan(&documentID))
		require.Equal(t, int64(4), documentID)
		require.ErrorIs(t, tenant.QueryRow("select id from mutation_documents where match(fpath) against('matrixone')").Scan(&documentID), sql.ErrNoRows)

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
