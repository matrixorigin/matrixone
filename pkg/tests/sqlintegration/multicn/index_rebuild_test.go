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

package multicn

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/stretchr/testify/require"
)

// A mirror transaction on a remote scan worker cannot commit the builder's
// internal index INSERTs. Two CNs distinguish a remote source from its writer.
func TestIssue29566IndexRebuildCoordinator(t *testing.T) {
	cluster, err := embed.StartTestCluster(embed.WithCNCount(2))
	if cluster != nil {
		t.Cleanup(func() { require.NoError(t, cluster.Close()) })
	}
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	cn, err := cluster.GetCNService(0)
	require.NoError(t, err)
	inventory := clusterservice.GetMOCluster(cn.ServiceID())
	refresher, ok := inventory.(clusterservice.AuthoritativeRefresher)
	require.True(t, ok)
	var peer string
	waitWorkers := func(ctx context.Context, want int) {
		t.Helper()
		require.Eventually(t, func() bool {
			if refresher.Refresh(ctx) != nil {
				return false
			}
			count := 0
			inventory.GetCNService(clusterservice.NewSelector(), func(service metadata.CNService) bool {
				count++
				if service.ServiceID != cn.ServiceID() {
					peer = service.PipelineServiceAddress
				}
				return true
			})
			return count == want
		}, 30*time.Second, 100*time.Millisecond)
	}
	waitWorkers(ctx, 2)
	oldForce := plan.GetForceScanOnMultiCN()
	t.Cleanup(func() { plan.SetForceScanOnMultiCN(oldForce) })
	conns := make([]*sql.Conn, 0, 2)
	for i := 0; i < 2; i++ {
		cn, err := cluster.GetCNService(i)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, db.Close()) })
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, conn.Close()) })
		conns = append(conns, conn)
	}
	conn := conns[0]
	const schema = "index_rebuild_coordinator"
	// Cluster.Close owns the isolated database and its index jobs. Roll back
	// any unfinished subtest transaction before closing the connections.
	t.Cleanup(func() {
		plan.SetForceScanOnMultiCN(oldForce)
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		for _, conn := range conns {
			_, err := conn.ExecContext(cleanupCtx, "rollback")
			require.NoError(t, err)
		}
	})
	exec := func(t *testing.T, statement string) {
		t.Helper()
		_, err := conn.ExecContext(ctx, statement)
		require.NoError(t, err, statement)
	}
	query := func(t *testing.T, statement string) []string {
		t.Helper()
		rows, err := conn.QueryContext(ctx, statement)
		require.NoError(t, err, statement)
		defer rows.Close()
		cols, err := rows.Columns()
		require.NoError(t, err)
		out := make([]string, 0)
		for rows.Next() {
			values := make([]sql.NullString, len(cols))
			args := make([]any, len(cols))
			for i := range values {
				args[i] = &values[i]
			}
			require.NoError(t, rows.Scan(args...))
			texts := make([]string, len(cols))
			for i, value := range values {
				texts[i] = "NULL"
				if value.Valid {
					texts[i] = value.String
				}
			}
			out = append(out, strings.Join(texts, "\t"))
		}
		require.NoError(t, rows.Err())
		return out
	}
	for _, statement := range []string{
		"create database " + schema, "use " + schema,
		"set experimental_fulltext2_index=1", "set experimental_hnsw_index=1",
		"create table docs(id int primary key,body varchar(200))",
		"insert into docs values(1,'bbbbbbbbbbbbbbbbbbbbbbbbbb'),(2,'aaaaaaaaaaaaaaaaaaaaaaa'),(3,'hello bbbbbbbbbbbbbbbbbbbbbbbbbb')",
		"create fulltext2 index ft on docs(body) with parser ngram",
		"create table vectors(id bigint primary key,v vecf64(2))",
		"insert into vectors values(1,'[1,0]'),(2,'[2,0]'),(3,'[3,0]')",
		"create index hx using hnsw on vectors(v) op_type 'vector_l2_ops' m=8 ef_construction=64 ef_search=64",
	} {
		exec(t, statement)
	}
	for _, table := range []string{"docs", "vectors"} {
		exec(t, "select mo_ctl('dn','flush','"+schema+"."+table+"')")
	}
	for _, statement := range []string{"use " + schema, "set experimental_fulltext2_index=1", "set experimental_hnsw_index=1"} {
		_, err = conns[1].ExecContext(ctx, statement)
		require.NoError(t, err)
	}

	for _, tc := range []struct {
		name, table, index, algo, match, want, search, metaType string
	}{
		{"fulltext2", "docs", "ft", "fulltext2", "select id from docs where match(body) against('bbbbbbbbbbbbbbbbbbbbbbbbbb') order by id", "1\n3", "fulltext2_search", catalog.FullText2Index_TblType_Metadata},
		{"hnsw", "vectors", "hx", "hnsw", "select id from vectors order by l2_distance(v,'[0,0]') limit 3 by rank with option 'mode=post'", "1\n2\n3", "hnsw_search", catalog.Hnsw_TblType_Metadata},
	} {
		t.Run(tc.name, func(t *testing.T) {
			conn = conns[0]
			require.Equal(t, tc.want, strings.Join(query(t, tc.match), "\n"))
			require.Contains(t, strings.Join(query(t, "explain "+tc.match), "\n"), tc.search)
			metadataTable := query(t, "select index_table_name from mo_catalog.mo_indexes where table_id=(select rel_id from mo_catalog.mo_tables where reldatabase='"+schema+"' and relname='"+tc.table+"') and algo_table_type='"+tc.metaType+"'")
			require.Len(t, metadataTable, 1)
			meta := "`" + metadataTable[0] + "`"
			rebuild := func() {
				plan.SetForceScanOnMultiCN(true)
				defer plan.SetForceScanOnMultiCN(oldForce)
				exec(t, "alter table "+tc.table+" alter reindex "+tc.index+" "+tc.algo+" force_sync")
			}
			// Rebuild from both coordinators: a fixed physical object owner must
			// be remote to one of them. No hash-dependent retries are needed.
			for i, coordinator := range conns {
				conn = coordinator
				before := query(t, "select max(build_ts) from "+meta)
				require.NotEqual(t, []string{"NULL"}, before)
				if i == 0 {
					physical := func() string {
						plan.SetForceScanOnMultiCN(true)
						defer plan.SetForceScanOnMultiCN(oldForce)
						return strings.Join(query(t, "explain phyplan analyze select id from "+tc.table), "\n")
					}()
					require.NotEmpty(t, peer)
					require.Contains(t, physical, peer, "source scans must use both CNs")
				}
				rebuild()
				require.Equal(t, tc.want, strings.Join(query(t, tc.match), "\n"), "successful rebuild on CN%d must commit replacement index writes", i)
				after := query(t, "select max(build_ts) from "+meta)
				require.NotEqual(t, []string{"NULL"}, after)
				require.NotEqual(t, before, after, "successful rebuild must publish a new generation")
				require.Equal(t, []string{"3"}, query(t, "select count(*) from "+tc.table), "source rows remain intact")
				if i == 0 {
					// Empty input still clears the old model, but both source and
					// index changes must remain owned by this transaction.
					exec(t, "begin")
					exec(t, "delete from "+tc.table+" where id>=0")
					rebuild()
					require.Empty(t, query(t, tc.match))
					require.Equal(t, []string{"NULL"}, query(t, "select max(build_ts) from "+meta))
					exec(t, "rollback")
					require.Equal(t, tc.want, strings.Join(query(t, tc.match), "\n"))
					require.Equal(t, after, query(t, "select max(build_ts) from "+meta), "rollback must preserve the prior committed generation")
				}
			}
		})
	}
}
