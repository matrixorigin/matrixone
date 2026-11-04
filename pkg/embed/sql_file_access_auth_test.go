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
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	mysql "github.com/go-sql-driver/mysql"

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/fulltext2"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/buffer"
	"go.uber.org/zap/zapcore"
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

		t.Run("file TVF admission", func(t *testing.T) {
			var line string
			err := publicDB.QueryRow("select * from parse_jsonl_file(" + sqlquote.String(input) + ") t").Scan(&line)
			require.ErrorContains(t, err, "only the sys account moadmin role")
			require.NoError(t, admin.QueryRow("select * from parse_jsonl_file("+sqlquote.String(input)+") t").Scan(&line))
			require.Equal(t, "owned test input", line)
			require.NoError(t, publicDB.QueryRow("select * from parse_jsonl_data('owned inline data') t").Scan(&line))
			require.Equal(t, "owned inline data", line)
		})

		t.Run("dynamic provider resource dependencies", func(t *testing.T) {
			base := "s3-opts,endpoint=https://owned.invalid,region=us-east-1,bucket=owned,key=owned-key,secret=owned-secret"
			for _, db := range []*sql.DB{publicDB, admin} {
				for _, tc := range []struct{ path, expected string }{
					{base + ",cert-files=" + input + ":object", "server-local auxiliary files"},
					{base + ",kerberos-keytab-path=" + input + ":object", "server-local auxiliary files"},
					{base + ",shared-config-profile=owned:object", "server credential discovery"},
					{"s3-opts,endpoint=https://owned.invalid,region=us-east-1,bucket=owned:object", "requires explicit credentials"},
					{"s3-opts,endpoint=https://owned.invalid,region=us-east-1,bucket=owned,role-arn=owned-role:object", "requires explicit credentials"},
				} {
					var content string
					err := db.QueryRow("select load_file(" + sqlquote.String(tc.path) + ")").Scan(&content)
					require.ErrorContains(t, err, tc.expected)
				}
			}
		})

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
		for _, name := range []string{"part_a1.txt", "part_b2.txt", "part_c3.txt"} {
			remoteObjects["/bucket/"+name] = []byte(name)
		}
		var objectsMu sync.Mutex
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			objectsMu.Lock()
			defer objectsMu.Unlock()
			if r.Method == http.MethodDelete {
				delete(remoteObjects, r.URL.Path)
				w.WriteHeader(http.StatusNoContent)
				return
			}
			if r.Method == http.MethodPost && r.URL.Query().Has("delete") {
				var req struct {
					Objects []struct {
						Key string `xml:"Key"`
					} `xml:"Object"`
				}
				if err := xml.NewDecoder(r.Body).Decode(&req); err != nil {
					http.Error(w, err.Error(), 400)
					return
				}
				w.Header().Set("Content-Type", "application/xml")
				fmt.Fprint(w, `<DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`)
				for _, obj := range req.Objects {
					delete(remoteObjects, "/bucket/"+strings.TrimLeft(obj.Key, "/"))
					fmt.Fprintf(w, "<Deleted><Key>%s</Key></Deleted>", obj.Key)
				}
				fmt.Fprint(w, "</DeleteResult>")
				return
			}
			if r.Method == http.MethodGet && (r.URL.Query().Has("prefix") || r.URL.Query().Has("delimiter")) {
				prefix := r.URL.Query().Get("prefix")
				w.Header().Set("Content-Type", "application/xml")
				if strings.Trim(prefix, "/") == "" {
					fmt.Fprint(w, `<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><IsTruncated>false</IsTruncated>`)
					for key, data := range remoteObjects {
						name := strings.TrimPrefix(key, "/bucket/")
						if !strings.Contains(name, "/") {
							fmt.Fprintf(w, "<Contents><Key>%s</Key><Size>%d</Size></Contents>", name, len(data))
						}
					}
					fmt.Fprint(w, "</ListBucketResult>")
				} else if strings.Trim(prefix, "/") == "hive" {
					fmt.Fprintf(w, `<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><IsTruncated>false</IsTruncated><CommonPrefixes><Prefix>%syear=2026/</Prefix></CommonPrefixes></ListBucketResult>`, prefix)
				} else {
					fmt.Fprintf(w, `<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><IsTruncated>false</IsTruncated><Contents><Key>%sdata.parquet</Key><Size>1</Size></Contents></ListBucketResult>`, prefix)
				}
				return
			}
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
		t.Run("async source admission and local TEXT CDC", func(t *testing.T) { testAsyncDatalinkSourceAdmission(t, c, admin, server.URL, dir) })

		t.Run("file TVF and Hive planning admission", func(t *testing.T) {
			remote := fileservice.JoinPath("s3-opts,endpoint="+server.URL+",bucket=bucket,region=us-east-1,key=id,secret=secret", "content.txt")
			var line string
			require.NoError(t, tenant.QueryRow("select * from parse_jsonl_file("+sqlquote.String(remote)+") t").Scan(&line))
			require.Equal(t, "matrixone stage content", line)
			parent := filepath.Join(dir, "uncreated-hive")
			_, err := tenant.Exec("create external table refused_hive(id int,year int) infile{'filepath'=" + sqlquote.String(filepath.Join(parent, "partition")) + ",'format'='parquet','hive_partitioning'='true'}")
			require.ErrorContains(t, err, "only the sys account moadmin role")
			_, err = os.Stat(parent)
			require.ErrorIs(t, err, os.ErrNotExist)
			// Administrator inference still lists an owned local directory.
			require.NoError(t, os.MkdirAll(filepath.Join(dir, "hive", "year=2026"), 0700))
			require.NoError(t, os.WriteFile(filepath.Join(dir, "hive", "year=2026", "data.parquet"), []byte("owned placeholder"), 0600))
			const hiveDB = "sql_file_access_hive_admin"
			mustExec(t, admin, "create database "+hiveDB)
			defer admin.Exec("drop database " + hiveDB)
			mustExec(t, admin, "create external table "+hiveDB+".local_hive(id int,year int) infile{'filepath'="+sqlquote.String(filepath.Join(dir, "hive"))+",'format'='parquet','hive_partitioning'='true'}")
			mustExec(t, tenant, "create external table remote_hive(id int,year int) url s3option {'endpoint'="+sqlquote.String(server.URL)+",'bucket'='bucket','region'='us-east-1','access_key_id'='id','secret_access_key'='secret','filepath'='hive/','format'='parquet','hive_partitioning'='true'}")
			defer tenant.Exec("drop table remote_hive")
			var matches int
			require.NoError(t, tenant.QueryRow("select count(*) from stage_list('stage://sql_file_access_stage/part_[ab]*.txt') t").Scan(&matches))
			require.Equal(t, 2, matches)
			mustExec(t, tenant, "remove files from stage 'stage://sql_file_access_stage/part_[ab]%3F.txt'")
			require.NoError(t, tenant.QueryRow("select count(*) from stage_list('stage://sql_file_access_stage/part_[ab]*.txt') t").Scan(&matches))
			require.Zero(t, matches)
			objectsMu.Lock()
			_, kept := remoteObjects["/bucket/part_c3.txt"]
			objectsMu.Unlock()
			require.True(t, kept)

		})

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

		t.Run("binary persistence and rollback", func(t *testing.T) {
			var accountID uint32
			require.NoError(t, admin.QueryRow("select account_id from mo_catalog.mo_account where account_name='"+account+"'").Scan(&accountID))
			exec := cn.RawService().(cnservice.Service).GetSQLExecutor()
			ctx, cancel := context.WithTimeout(defines.AttachAccountId(t.Context(), accountID), 30*time.Second)
			defer cancel()
			stmt := executor.StatementOption{}.WithAccountID(accountID).WithSessionIdentity(account, "accountadmin")
			opts := executor.Options{}.WithAccountID(accountID).WithDatabase(dbName).WithStatementOption(stmt)
			mustExec(t, tenant, "create table binary_store (index_id varchar(128), chunk_id bigint, data longblob, tag int, extra varchar(32) default 'ordinary default')")
			t.Cleanup(func() { _, _ = tenant.Exec("drop table if exists binary_store") })
			for _, size := range []int{1, 65535, 65536, 65537} {
				payload := make([]byte, size)
				for i := range payload {
					payload[i] = byte(i)
				}
				id := fmt.Sprintf("quoted'%d", size)
				err := exec.ExecTxn(ctx, func(tx executor.TxnExecutor) error {
					w, err := sqlexec.NewChunkInserter(ctx, tx, stmt, dbName, "binary_store", 500)
					if err != nil {
						return err
					}
					if _, err = w.WriteRange(bytes.NewReader(payload), 0, int64(size), id, 0, 1); err != nil {
						return err
					}
					return w.Flush()
				}, opts)
				require.NoError(t, err)
				func() {
					rows, err := tenant.Query("select chunk_id,data,extra from binary_store where index_id=? order by chunk_id", id)
					require.NoError(t, err)
					defer rows.Close()
					var got []byte
					var next int64
					for rows.Next() {
						var chunkID int64
						var chunk []byte
						var extra string
						require.NoError(t, rows.Scan(&chunkID, &chunk, &extra))
						require.Equal(t, next, chunkID)
						require.Equal(t, "ordinary default", extra)
						got = append(got, chunk...)
						next++
					}
					require.NoError(t, rows.Err())
					require.Equal(t, payload, got)
				}()
			}

			// A later failure must roll back an already-executed chunk batch;
			// the same artifact can then be retried without losing or duplicating data.
			payload := bytes.Repeat([]byte{0, 255}, 65537)
			persist := func(tx executor.TxnExecutor, fail bool) error {
				w, err := sqlexec.NewChunkInserter(ctx, tx, stmt, dbName, "binary_store", 1)
				if err != nil {
					return err
				}
				if _, err = w.WriteRange(bytes.NewReader(payload), 0, int64(len(payload)), "retry", 0, 1); err != nil {
					return err
				}
				if fail {
					return errors.New("owned transaction failure after earlier batches")
				}
				return w.Flush()
			}
			err := exec.ExecTxn(ctx, func(tx executor.TxnExecutor) error { return persist(tx, true) }, opts)
			require.ErrorContains(t, err, "owned transaction failure")
			var retryRows int
			require.NoError(t, tenant.QueryRow("select count(*) from binary_store where index_id='retry'").Scan(&retryRows))
			require.Zero(t, retryRows)
			require.NoError(t, exec.ExecTxn(ctx, func(tx executor.TxnExecutor) error { return persist(tx, false) }, opts))
			require.NoError(t, tenant.QueryRow("select count(*) from binary_store where index_id='retry'").Scan(&retryRows))
			require.Equal(t, 3, retryRows)

			marker := filepath.Join(dir, "default-marker")
			mustExec(t, tenant, "create table refused_store (index_id varchar(128), chunk_id bigint, data longblob, tag int, extra bigint default (save_file(cast("+sqlquote.String("file://"+marker)+" as datalink),'owned marker')))")
			t.Cleanup(func() { _, _ = tenant.Exec("drop table if exists refused_store") })
			err = exec.ExecTxn(ctx, func(tx executor.TxnExecutor) error {
				w, err := sqlexec.NewChunkInserter(ctx, tx, stmt, dbName, "refused_store", 1)
				if err != nil {
					return err
				}
				if _, err = w.WriteRange(bytes.NewReader([]byte("owned")), 0, 5, "id", 0, 1); err != nil {
					return err
				}
				return w.Flush()
			}, opts)
			require.ErrorContains(t, err, "only the sys account moadmin role")
			_, err = os.Stat(marker)
			require.ErrorIs(t, err, os.ErrNotExist)
			var count int
			require.NoError(t, tenant.QueryRow("select count(*) from refused_store").Scan(&count))
			require.Zero(t, count)

			// Exercise the public TVF, including its earlier metadata write, so
			// denied omitted defaults prove rollback of the complete replacement.
			mustExec(t, tenant, "create table authority_source (id bigint primary key, body text)")
			mustExec(t, tenant, "insert into authority_source values (1,'owned source document')")
			mustExec(t, tenant, "create table authority_metadata (index_id varchar(128), timestamp bigint, checksum varchar(32), filesize bigint, recency bigint, nrow bigint, build_ts bigint)")
			t.Cleanup(func() {
				_, _ = tenant.Exec("drop table if exists authority_source")
				_, _ = tenant.Exec("drop table if exists authority_metadata")
			})
			cfg, err := json.Marshal(fulltext2.TableConfig{DbName: dbName, SrcTable: "authority_source", IndexTable: "refused_store", MetadataTable: "authority_metadata", PKey: "id"})
			require.NoError(t, err)
			_, err = tenant.Exec("select f.* from authority_source s cross apply fulltext2_create('{}'," + sqlquote.String(string(cfg)) + ",s.id,s.body) f")
			require.ErrorContains(t, err, "only the sys account moadmin role")
			require.NoError(t, tenant.QueryRow("select count(*) from authority_metadata").Scan(&count))
			require.Zero(t, count)
			_, err = os.Stat(marker)
			require.ErrorIs(t, err, os.ErrNotExist)

		})

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
		// later compaction. Owned artifacts are persisted as binary parameters
		// while generated SQL retains the tenant session identity.
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

		t.Run("HNSW loaded IDs remain data during persistence", func(t *testing.T) {
			var hnswStore string
			require.NoError(t, tenant.QueryRow("select index_table_name from mo_catalog.mo_indexes where name = 'build_hidx' and algo_table_type = 'hnsw_index' and table_id in (select rel_id from mo_catalog.mo_tables where reldatabase = database() and relname = 'build_vectors') limit 1").Scan(&hnswStore))
			conn, err := tenant.Conn(t.Context())
			require.NoError(t, err)
			t.Cleanup(func() { _ = conn.Close() })
			metaTable := sqlquote.QualifiedIdent(dbName, hnswMeta)
			storeTable := sqlquote.QualifiedIdent(dbName, hnswStore)
			var oldID, oldChecksum string
			require.NoError(t, conn.QueryRowContext(t.Context(), "select index_id, checksum from "+metaTable).Scan(&oldID, &oldChecksum))
			// The marker belongs to this fixture. Expression-looking metadata must
			// remain literal data when a background storage writer persists it.
			const literalID = "0' or save_file(cast('stage://sql_file_access_local_stage/hnsw-marker' as datalink),'x') or '0"
			require.LessOrEqual(t, len(literalID), 128)
			_, err = conn.ExecContext(t.Context(), "select save_file(cast('stage://sql_file_access_local_stage/hnsw-marker' as datalink),'x')")
			require.ErrorContains(t, err, "only the sys account moadmin role")
			_, err = conn.ExecContext(t.Context(), "begin")
			require.NoError(t, err)
			t.Cleanup(func() {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				_, _ = conn.ExecContext(ctx, "rollback")
			})
			for _, table := range []string{metaTable, storeTable} {
				_, err = conn.ExecContext(t.Context(), "update "+table+" set index_id="+sqlquote.String(literalID)+" where index_id="+sqlquote.String(oldID))
				require.NoError(t, err)
			}
			_, err = conn.ExecContext(t.Context(), "commit")
			require.NoError(t, err)
			var storedID string
			require.NoError(t, conn.QueryRowContext(t.Context(), "select index_id from "+metaTable).Scan(&storedID))
			require.Equal(t, literalID, storedID)
			_, err = conn.ExecContext(t.Context(), "insert into "+sqlquote.QualifiedIdent(dbName, "build_vectors")+" values (3, '[0,0,1]')")
			require.NoError(t, err)
			require.Eventually(t, func() bool {
				var checksum string
				return conn.QueryRowContext(t.Context(), "select checksum from "+metaTable+" where index_id="+sqlquote.String(literalID)).Scan(&checksum) == nil && checksum != oldChecksum
			}, 30*time.Second, 250*time.Millisecond, "the loaded model must be modified and persisted by CDC")
			var n int
			require.NoError(t, conn.QueryRowContext(t.Context(), "select count(*) from "+storeTable+" where index_id="+sqlquote.String(literalID)).Scan(&n))
			require.Positive(t, n)
			_, err = os.Stat(filepath.Join(dir, "hnsw-marker"))
			require.ErrorIs(t, err, os.ErrNotExist)
			_, err = conn.ExecContext(t.Context(), "set experimental_hnsw_index = 1")
			require.NoError(t, err)
			require.NoError(t, conn.QueryRowContext(t.Context(), "select id from "+sqlquote.QualifiedIdent(dbName, "build_vectors")+" force index (build_hidx) order by l2_distance(v, '[0,0,1]') limit 1").Scan(&id))
			require.Equal(t, int64(3), id)
		})

		t.Run("compaction metadata retains caller authority", func(t *testing.T) {
			marker := filepath.Join(dir, "metadata-marker.txt")
			mustExec(t, tenant, "create table authority_paths (fpath datalink)")
			t.Cleanup(func() { _, _ = tenant.Exec("drop table if exists authority_paths") })
			mustExec(t, tenant, fmt.Sprintf("insert into authority_paths values ('file://%s')", marker))
			// Every metadata column is valid: a missing-column error must not
			// conceal whether the view expression receives storage authority.
			mustExec(t, tenant, "create view authority_meta as select cast(save_file(fpath, 'owned test marker') as varchar(128)) as index_id, cast(0 as bigint) as timestamp, cast('unused' as varchar(32)) as checksum, cast(0 as bigint) as filesize, cast(0 as bigint) as recency, cast(0 as bigint) as nrow, cast(0 as bigint) as build_ts from authority_paths")
			t.Cleanup(func() { _, _ = tenant.Exec("drop view if exists authority_meta") })
			counts := func() [2]int {
				var n [2]int
				require.NoError(t, tenant.QueryRow(fmt.Sprintf("select count(*) from `%s`.`%s`", dbName, store)).Scan(&n[0]))
				require.NoError(t, tenant.QueryRow(fmt.Sprintf("select count(*) from `%s`.`%s`", dbName, meta)).Scan(&n[1]))
				return n
			}
			before := counts()
			_, err := tenant.Exec("select index_id from authority_meta")
			require.ErrorContains(t, err, "only the sys account moadmin role")
			_, err = tenant.Exec(fmt.Sprintf("select * from fulltext2_compact('%s', '%s', 'authority_meta', '1') as f", dbName, store))
			require.ErrorContains(t, err, "only the sys account moadmin role")
			_, err = os.Stat(marker)
			require.ErrorIs(t, err, os.ErrNotExist)
			require.Equal(t, before, counts(), "refused compaction must leave genuine index storage unchanged")
			require.NoError(t, tenant.QueryRow("select id from build_documents where match(body) against('compaction')").Scan(&id))
			require.Equal(t, int64(2), id)
		})

		t.Run("dump and load use local provider authority", func(t *testing.T) {
			const dumpDB = "sql_file_access_dump_db"
			mustExec(t, admin, "create database "+dumpDB)
			t.Cleanup(func() { _, _ = admin.Exec("drop database if exists " + dumpDB) })
			mustExec(t, admin, "create table "+dumpDB+".source (id int primary key, value varchar(32))")
			mustExec(t, admin, "insert into "+dumpDB+".source values (7, 'owned dump data')")
			mustExec(t, admin, "select mo_ctl('dn', 'flush', '"+dumpDB+".source')")
			// Preserve the spelling until the fixed-local owner normalizes it.
			alternate := dir + "/unused,remote:x/../dump-fixture"
			mustExec(t, admin, fmt.Sprintf("dump table %s.source to '%s'", dumpDB, alternate))
			fixture := filepath.Join(dir, "dump-fixture")
			_, err := os.Stat(filepath.Join(fixture, "manifest.json"))
			require.NoError(t, err)
			mustExec(t, admin, "create table "+dumpDB+".target like "+dumpDB+".source")
			mustExec(t, admin, fmt.Sprintf("load table %s.target from 'file://%s'", dumpDB, alternate))
			var gotID int
			var value string
			require.NoError(t, admin.QueryRow("select id, value from "+dumpDB+".target").Scan(&gotID, &value))
			require.Equal(t, 7, gotID)
			require.Equal(t, "owned dump data", value)

			mustExec(t, tenant, "create table dump_target (id int primary key, value varchar(32))")
			t.Cleanup(func() { _, _ = tenant.Exec("drop table if exists dump_target") })
			mustExec(t, tenant, "insert into dump_target values (9, 'tenant original')")
			for _, tc := range []struct{ name, prefix, suffix string }{
				{"absolute", "", ""},
				{"file URL", "file://", ""},
				{"service syntax in local name", "", "/unused,remote:x/.."},
			} {
				t.Run(tc.name, func(t *testing.T) {
					target := tc.prefix + dir + tc.suffix + "/denied-dump"
					_, err := tenant.Exec(fmt.Sprintf("dump table dump_target to '%s'", target))
					require.ErrorContains(t, err, "only the sys account moadmin role")
					_, err = os.Stat(filepath.Join(dir, "denied-dump"))
					require.ErrorIs(t, err, os.ErrNotExist)
					source := tc.prefix + dir + tc.suffix + "/dump-fixture"
					_, err = tenant.Exec(fmt.Sprintf("load table dump_target from '%s'", source))
					require.ErrorContains(t, err, "only the sys account moadmin role")
					require.NoError(t, tenant.QueryRow("select id, value from dump_target").Scan(&gotID, &value))
					require.Equal(t, 9, gotID)
					require.Equal(t, "tenant original", value)
				})
			}
		})
	})
}

// A separate topology proof keeps the larger CDC fixture single-CN. Releasing
// the prior fixture uses the existing admission owner, not a second cluster lease.
func TestSQLFileAccessPeerPersistence(t *testing.T) {
	require.NoError(t, CloseSingleCNBaseClusterTests())
	defer func() { require.NoError(t, CloseBaseClusterTests()) }()
	RunBaseClusterTests(t, func(c Cluster) {
		admin := openIcebergTestDB(t, c)
		defer admin.Close()
		const account = "sql_file_access_peer"
		mustExec(t, admin, "create account "+account+" admin_name 'admin' identified by 'peer-test'")
		defer admin.Exec("drop account if exists " + account)
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		tenant, err := sql.Open("mysql", fmt.Sprintf("%s#admin#accountadmin:peer-test@tcp(127.0.0.1:%d)/", account, cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer tenant.Close()
		mustExec(t, tenant, "create database peer_db")
		mustExec(t, tenant, "create table peer_db.store(index_id varchar(128),chunk_id bigint,data longblob,tag int)")
		var accountID uint32
		require.NoError(t, admin.QueryRow("select account_id from mo_catalog.mo_account where account_name='"+account+"'").Scan(&accountID))
		ctx, cancel := context.WithTimeout(defines.AttachAccountId(t.Context(), accountID), 30*time.Second)
		defer cancel()
		stmt := executor.StatementOption{}.WithAccountID(accountID).WithSessionIdentity(account, "accountadmin")
		payload := make([]byte, 65537)
		for i := range payload {
			payload[i] = byte(i)
		}
		err = cn.RawService().(cnservice.Service).GetSQLExecutor().ExecTxn(ctx, func(tx executor.TxnExecutor) error {
			w, err := sqlexec.NewChunkInserter(ctx, tx, stmt, "peer_db", "store", 500)
			if err != nil {
				return err
			}
			if _, err = w.WriteRange(bytes.NewReader(payload), 0, int64(len(payload)), "owned", 0, 1); err != nil {
				return err
			}
			return w.Flush()
		}, executor.Options{}.WithAccountID(accountID).WithDatabase("peer_db").WithStatementOption(stmt))
		require.NoError(t, err)
		second, err := c.GetCNService(1)
		require.NoError(t, err)
		peer, err := sql.Open("mysql", fmt.Sprintf("%s#admin#accountadmin:peer-test@tcp(127.0.0.1:%d)/peer_db", account, second.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer peer.Close()
		func() {
			rows, err := peer.Query("select data from store order by chunk_id")
			require.NoError(t, err)
			defer rows.Close()
			var got []byte
			for rows.Next() {
				var data []byte
				require.NoError(t, rows.Scan(&data))
				got = append(got, data...)
			}
			require.NoError(t, rows.Err())
			require.Equal(t, payload, got)
		}()
		_, err = peer.Exec("select load_file(" + sqlquote.String(filepath.Join(t.TempDir(), "missing-owned-input")) + ")")
		require.ErrorContains(t, err, "only the sys account moadmin role")
		// Force the existing distributed scan route and inspect its physical
		// execution, so this proves role transport rather than only peer reads.
		const remoteDB = "sql_file_access_remote_scope"
		mustExec(t, admin, "create database "+remoteDB)
		defer admin.Exec("drop database " + remoteDB)
		mustExec(t, admin, "create table "+remoteDB+".paths(id int primary key,p varchar(2048))")
		owned := filepath.Join(t.TempDir(), "remote-owned.txt")
		require.NoError(t, os.WriteFile(owned, []byte("remote owned bytes"), 0600))
		for i := 0; i < 2; i++ {
			mustExec(t, admin, fmt.Sprintf("insert into %s.paths values(%d,%s)", remoteDB, i, sqlquote.String(owned)))
			mustExec(t, admin, "select mo_ctl('dn','flush','"+remoteDB+".paths')")
		}

		var peerAddress string
		require.Eventually(t, func() bool {
			clusterservice.GetMOCluster(cn.RawService().(cnservice.Service).ID()).ForceRefresh(true)
			nodes, err := cn.RawService().(cnservice.Service).GetEngine().Nodes(false, "sys", "root", nil)
			for _, node := range nodes {
				if node.Id == second.RawService().(cnservice.Service).ID() {
					peerAddress = node.Addr
				}
			}
			return err == nil && len(nodes) == 2 && peerAddress != ""
		}, 15*time.Second, 100*time.Millisecond, "both workers must be discoverable before forcing the remote route")

		oldForce := plan2.GetForceScanOnMultiCN()
		plan2.SetForceScanOnMultiCN(true)
		defer plan2.SetForceScanOnMultiCN(oldForce)
		func() {
			rows, err := admin.Query("explain phyplan analyze select load_file(p) from " + remoteDB + ".paths")
			require.NoError(t, err)
			defer rows.Close()
			cols, err := rows.Columns()
			require.NoError(t, err)
			var planText strings.Builder
			for rows.Next() {
				values := make([]sql.RawBytes, len(cols))
				args := make([]any, len(cols))
				for i := range args {
					args[i] = &values[i]
				}
				require.NoError(t, rows.Scan(args...))
				for _, v := range values {
					planText.Write(v)
					planText.WriteByte('\n')
				}
			}
			require.NoError(t, rows.Err())
			t.Log("forced physical plan", planText.String())
			require.Contains(t, planText.String(), "addr:"+peerAddress)
		}()
		// The EXPLAIN renderer includes scheduled sender scopes even without
		// remote execution statistics. Observe the existing diagnostic receipt
		// emitted by the pipeline server for this query, then verify its rows.
		var remoteReceipts atomic.Int64
		previousReport := logutil.GetReportZapFunc()
		logutil.SetLogReporter(&logutil.TraceReporter{ReportZap: func(enc zapcore.Encoder, entry zapcore.Entry, fields []zapcore.Field) (*buffer.Buffer, error) {
			if strings.HasPrefix(entry.Message, "inject_send_pipeline: client2server") {
				remoteReceipts.Add(1)
			}
			return previousReport(enc, entry, fields)
		}})
		defer logutil.SetLogReporter(&logutil.TraceReporter{ReportZap: previousReport})
		if fault.Enable() {
			defer fault.Disable()
		}
		require.NoError(t, fault.AddFaultPoint(t.Context(), "inject_send_pipeline", ":::", "return", 0, remoteDB+".paths where id", false))
		defer func() {
			_, err := fault.RemoveFaultPoint(t.Context(), "inject_send_pipeline")
			require.NoError(t, err)
		}()
		func() {
			stmt, err := admin.Prepare("select concat(load_file(p),cast(? as longblob)) from " + remoteDB + ".paths where id>=?")
			require.NoError(t, err)
			defer stmt.Close()
			rows, err := stmt.Query(payload, 0)
			require.NoError(t, err)
			defer rows.Close()
			var count int
			for rows.Next() {
				var content []byte
				require.NoError(t, rows.Scan(&content))
				require.Equal(t, append([]byte("remote owned bytes"), payload...), content)
				count++
			}
			require.NoError(t, rows.Err())
			require.Equal(t, 2, count)
		}()
		require.Positive(t, remoteReceipts.Load(), "the prepared query must reach the other CN pipeline server")

	})
}
