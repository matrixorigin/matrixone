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
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/stretchr/testify/require"
)

// Reuse the tenant/remote-stage fixture; this adds no cluster or external service.
func testAsyncDatalinkSourceAdmission(t *testing.T, c Cluster, admin *sql.DB, endpoint, dir string) {
	conn, err := admin.Conn(t.Context())
	require.NoError(t, err)
	defer conn.Close()
	exec := func(q string) { t.Helper(); _, e := conn.ExecContext(t.Context(), q); require.NoError(t, e, q) }
	const database = "sql_file_access_async_source"
	exec("create database " + database)
	defer admin.Exec("drop database if exists " + database)
	exec("use " + database)
	exec("set experimental_fulltext2_index=1")
	exec("create stage async_source_local url=" + sqlquote.String("file://"+dir+"/"))
	defer admin.Exec("drop stage if exists async_source_local")
	exec(fmt.Sprintf("create stage async_source_remote URL='s3://bucket/' CREDENTIALS={'AWS_KEY_ID'='id','AWS_SECRET_KEY'='secret','AWS_REGION'='us-east-1','PROVIDER'='amazon','ENDPOINT'=%s}", sqlquote.String(endpoint)))
	defer admin.Exec("drop stage if exists async_source_remote")
	local := "stage://async_source_local/content.txt"
	remote := "stage://async_source_remote/content.txt"
	exec("create table existing_local(id bigint primary key,path datalink)")
	exec("insert into existing_local values(1," + sqlquote.String(local) + ")")
	_, err = conn.ExecContext(t.Context(), "create fulltext2 index bad on existing_local(path) force_sync")
	require.ErrorContains(t, err, "asynchronous indexes require remote Datalink sources")
	_, err = conn.ExecContext(t.Context(), "create fulltext2 index bad_json on existing_local(path) with parser json")
	require.ErrorContains(t, err, "asynchronous indexes require remote Datalink sources")
	var count int
	require.NoError(t, conn.QueryRowContext(t.Context(), "select count(*) from mo_catalog.mo_indexes where name='bad' and table_id in(select rel_id from mo_catalog.mo_tables where reldatabase=database() and relname='existing_local')").Scan(&count))
	require.Zero(t, count)
	exec("create table docs(id bigint primary key,path datalink default " + sqlquote.String(local) + ",fulltext2 ft(path))")
	exec("insert into docs values(1,NULL)")
	denied := []string{
		"insert into docs values(2," + sqlquote.String(local) + ")",
		"insert into docs(id) values(2)",
		"insert into docs select 2,path from existing_local",
		"update docs set path=" + sqlquote.String(local) + " where id=1",
		"replace into docs values(1," + sqlquote.String(local) + ")",
		"insert into docs values(1,NULL) on duplicate key update path=" + sqlquote.String(local),
		"insert into docs values(2," + sqlquote.String(remote) + "),(3," + sqlquote.String(local) + ")",
		"load data inline format='csv', data=" + sqlquote.String("2,"+local+"\n") + " into table docs fields terminated by ','",
	}
	for _, q := range denied {
		_, err = conn.ExecContext(t.Context(), q)
		require.ErrorContains(t, err, "asynchronous indexes require remote Datalink sources", q)
		require.NoError(t, conn.QueryRowContext(t.Context(), "select count(*) from docs where id=1 and path is null").Scan(&count))
		require.Equal(t, 1, count)
		require.NoError(t, conn.QueryRowContext(t.Context(), "select count(*) from docs").Scan(&count))
		require.Equal(t, 1, count)
	}
	prepared, err := conn.PrepareContext(t.Context(), "insert into docs values(?,?)")
	require.NoError(t, err)
	defer prepared.Close()
	_, err = prepared.ExecContext(t.Context(), 2, local)
	require.ErrorContains(t, err, "asynchronous indexes require remote Datalink sources")

	// Both async algorithms share final-value admission; synchronous fulltext
	// still reads authorized local content within the foreground statement.
	var tablesBefore int
	require.NoError(t, conn.QueryRowContext(t.Context(), "select count(*) from mo_catalog.mo_tables where reldatabase=database()").Scan(&tablesBefore))
	_, err = conn.ExecContext(t.Context(), "create fulltext index bad_async on existing_local(path) async")
	require.ErrorContains(t, err, "asynchronous indexes require remote Datalink sources")
	require.NoError(t, conn.QueryRowContext(t.Context(), "select count(*) from mo_catalog.mo_tables where reldatabase=database()").Scan(&count))
	require.Equal(t, tablesBefore, count, "failed admission must roll back its hidden table")
	require.NoError(t, conn.QueryRowContext(t.Context(), "select count(*) from mo_catalog.mo_indexes where name='bad_async' and table_id in(select rel_id from mo_catalog.mo_tables where reldatabase=database() and relname='existing_local')").Scan(&count))
	require.Zero(t, count)
	require.NoError(t, conn.QueryRowContext(t.Context(), "select count(*) from mo_catalog.mo_iscp_log where account_id=0 and job_name='index_bad_async' and table_id in(select rel_id from mo_catalog.mo_tables where reldatabase=database() and relname='existing_local')").Scan(&count))
	require.Zero(t, count)
	exec("create fulltext index local_sync on existing_local(path)")
	exec("create table classic_async(id bigint primary key,path datalink,fulltext ft(path) async)")
	_, err = conn.ExecContext(t.Context(), "insert into classic_async values(1,"+sqlquote.String(local)+")")
	require.ErrorContains(t, err, "asynchronous indexes require remote Datalink sources")
	// The incoming conflict candidate is not the final stored row.
	exec("insert into docs values(1," + sqlquote.String(local) + ") on duplicate key update path=NULL")
	exec("insert ignore into docs values(1," + sqlquote.String(local) + ")")
	exec("create table empty_target(id bigint primary key,path datalink,fulltext2 eft(path))")
	exec("update docs d left join empty_target e on d.id=e.id set e.path=" + sqlquote.String(local))
	// The unchanged role-less resolver remains denied even for source account zero.
	cn, err := c.GetCNService(0)
	require.NoError(t, err)
	service := cn.RawService().(cnservice.Service)
	t.Run("bounded classic async admission", func(t *testing.T) {
		testClassicAsyncAdmissionResults(t, conn, service, database, local, remote)
	})
	exec("create user async_source_public identified by 'owned-async-source' default role public")
	defer admin.Exec("drop user if exists async_source_public")
	exec("grant insert on table " + database + ".docs to public")
	publicDB, e := sql.Open("mysql", fmt.Sprintf("sys#async_source_public#public:owned-async-source@tcp(127.0.0.1:%d)/%s", cn.GetServiceConfig().CN.Frontend.Port, database))
	require.NoError(t, e)
	defer publicDB.Close()
	_, err = publicDB.Exec("insert into docs values(2," + sqlquote.String("file://"+filepath.Join(dir, "content.txt")) + ")")
	require.ErrorContains(t, err, "asynchronous indexes require remote Datalink sources")

	var resolvedText string
	backgroundRead := func(raw string) error {
		resolvedText = ""
		return sqlexec.RunTxnWithSqlContext(context.Background(), service.GetEngine(), service.GetTxnClient(), service.ID(), 0, time.Minute, nil, nil, func(sp *sqlexec.SqlProcess, _ any) error {
			result, e := sqlexec.RunSql(sp, "select load_text(cast("+sqlquote.String(raw)+" as datalink))")
			defer result.Close()
			if e == nil {
				for _, b := range result.Batches {
					if b != nil && b.RowCount() > 0 {
						resolvedText = string(b.Vecs[0].GetBytesAt(0))
						break
					}
				}
			}
			return e
		})
	}
	require.ErrorContains(t, backgroundRead(local), "only the sys account moadmin role")
	// Deterministically change the provider between admission and the same
	// background acquisition boundary; restoring the source permits normal retry.
	var admitted bool
	require.NoError(t, conn.QueryRowContext(t.Context(), "select _check_async_datalink_source(cast("+sqlquote.String(remote)+" as datalink),true)").Scan(&admitted))
	require.True(t, admitted)
	exec(fmt.Sprintf("create stage async_source_stable URL='s3://bucket/' CREDENTIALS={'AWS_KEY_ID'='id','AWS_SECRET_KEY'='secret','AWS_REGION'='us-east-1','PROVIDER'='amazon','ENDPOINT'=%s}", sqlquote.String(endpoint)))
	defer admin.Exec("drop stage if exists async_source_stable")
	stable := "stage://async_source_stable/content.txt"
	exec("create table changed_sources(id bigint primary key,p1 datalink,p2 datalink,status int,fulltext2 changed_ft(p1,p2))")
	exec("insert into changed_sources values(1," + sqlquote.String(stable) + "," + sqlquote.String(remote) + ",0),(2," + sqlquote.String(stable) + "," + sqlquote.String(remote) + ",0)")
	var changedStorage string
	require.NoError(t, conn.QueryRowContext(t.Context(), "select index_table_name from mo_catalog.mo_indexes where name='changed_ft' and algo_table_type='ftv2_index' and table_id in(select rel_id from mo_catalog.mo_tables where reldatabase=database() and relname='changed_sources') limit 1").Scan(&changedStorage))
	changedTailSQL := "select coalesce(max(chunk_id),-1) from " + sqlquote.QualifiedIdent(database, changedStorage) + " where index_id='cdc_tail' and tag=1"
	var changedBefore int64
	require.Eventually(t, func() bool {
		return conn.QueryRowContext(t.Context(), changedTailSQL).Scan(&changedBefore) == nil && changedBefore >= 0
	}, 30*time.Second, 100*time.Millisecond)
	exec("alter stage async_source_remote set url=" + sqlquote.String("file://"+dir+"/"))
	// Keeping an OLD source does not introduce a source or authorize a read.
	exec("update changed_sources set status=1")
	for _, q := range []string{
		"update changed_sources set p2=p2,status=8 where id=1",
		"update changed_sources set p1=" + sqlquote.String(local) + ",status=8 where id=1",
		"update changed_sources a join changed_sources b on a.id=b.id set a.status=3,b.status=4 where a.id=1",
	} {
		_, err = conn.ExecContext(t.Context(), q)
		require.ErrorContains(t, err, "asynchronous indexes require remote Datalink sources", q)
		require.NoError(t, conn.QueryRowContext(t.Context(), "select count(*) from changed_sources where status=1").Scan(&count))
		require.Equal(t, 2, count, "rejected assignments roll back all database effects")
	}
	// Validate the assigned source independently of a carried, invalidated peer.
	exec("update changed_sources set p1=" + sqlquote.String(stable) + ",status=2 where id=1")
	var storedP1, storedP2 string
	var status int
	require.NoError(t, conn.QueryRowContext(t.Context(), "select p1,p2,status from changed_sources where id=1").Scan(&storedP1, &storedP2, &status))
	require.Equal(t, stable, storedP1)
	require.Equal(t, remote, storedP2)
	require.Equal(t, 2, status)
	require.ErrorContains(t, backgroundRead(remote), "only the sys account moadmin role")
	exec("alter stage async_source_remote set url='s3://bucket/'")
	require.NoError(t, backgroundRead(remote))
	require.Equal(t, "matrixone stage content", resolvedText)
	exec("update changed_sources set p1='stage://async_source_stable/file-sample_100kB.docx',p2=NULL,status=3 where id=1")
	require.Eventually(t, func() bool {
		var now int64
		return conn.QueryRowContext(t.Context(), changedTailSQL).Scan(&now) == nil && now > changedBefore
	}, 30*time.Second, 100*time.Millisecond)
	require.Eventually(t, func() bool {
		return conn.QueryRowContext(t.Context(), "select count(*) from changed_sources where id=1 and match(p1,p2) against('Nulla facilisi' in natural language mode)").Scan(&count) == nil && count == 1
	}, 30*time.Second, 100*time.Millisecond)
	require.NoError(t, conn.QueryRowContext(t.Context(), "select count(*) from changed_sources where id=1 and match(p1,p2) against('matrixone')").Scan(&count))
	require.Zero(t, count)

	// Actual supported local lifecycle: acquire text with the foreground authority,
	// then maintain committed TEXT through CDC, without synchronous rebuilding.
	require.NoError(t, os.WriteFile(filepath.Join(dir, "async-initial.txt"), []byte("initiallocaltoken document"), 0600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "async-update.txt"), []byte("updatedlocaltoken document"), 0600))
	exec("create table local_text(id bigint primary key,body text,fulltext2 text_ft(body))")
	exec("insert into local_text values(1,load_text('stage://async_source_local/async-initial.txt'))")
	var storage string
	require.NoError(t, conn.QueryRowContext(t.Context(), "select index_table_name from mo_catalog.mo_indexes where name='text_ft' and algo_table_type='ftv2_index' and table_id in(select rel_id from mo_catalog.mo_tables where reldatabase=database() and relname='local_text') limit 1").Scan(&storage))
	tailSQL := "select coalesce(max(chunk_id),-1) from " + sqlquote.QualifiedIdent(database, storage) + " where index_id='cdc_tail' and tag=1"
	var before int64
	require.Eventually(t, func() bool { return conn.QueryRowContext(t.Context(), tailSQL).Scan(&before) == nil && before >= 0 }, 30*time.Second, 100*time.Millisecond)
	exec("update local_text set body=load_text('stage://async_source_local/async-update.txt') where id=1")
	require.Eventually(t, func() bool {
		var now int64
		return conn.QueryRowContext(t.Context(), tailSQL).Scan(&now) == nil && now > before
	}, 30*time.Second, 100*time.Millisecond)
	require.NoError(t, conn.QueryRowContext(t.Context(), "select count(*) from local_text where match(body) against('updatedlocaltoken')").Scan(&count))
	require.Equal(t, 1, count)
	require.NoError(t, conn.QueryRowContext(t.Context(), "select count(*) from local_text where match(body) against('initiallocaltoken')").Scan(&count))
	require.Zero(t, count)
}
