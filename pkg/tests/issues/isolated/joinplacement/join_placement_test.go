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

package joinplacement

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSingleRemoteWorkerJoinPlacement(t *testing.T) {
	// Separate process: the embedded cluster and forced AP planner flag are
	// private to these placement tests. Ports use the embed package's lease.
	var ingressID string
	cluster, err := embed.StartTestCluster(embed.WithDataPath(t.TempDir()), embed.WithCNCount(2),
		embed.WithPreStart(func(s embed.ServiceOperator) {
			if s.ServiceType() == metadata.ServiceType_CN {
				s.Adjust(func(cfg *embed.ServiceConfig) {
					// The first CN accepts direct SQL connections but is not an AP
					// worker. Keep that topology for this private cluster's lifetime.
					if ingressID == "" {
						ingressID = s.ServiceID()
						cfg.CN.InitWorkState = metadata.WorkState_Draining.String()
					}
					cfg.CN.SQLAddress = fmt.Sprintf("127.0.0.1:%d", cfg.CN.Frontend.Port)
					// The private cluster is not testing authorization. Do
					// not subject its internal privilege queries to forced AP.
					cfg.CN.Frontend.SkipCheckPrivilege = true
				})
			}
		}))
	if cluster != nil {
		t.Cleanup(func() { require.NoError(t, cluster.Close()) })
	}
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	coordinator, err := cluster.GetCNService(0)
	require.NoError(t, err)
	require.Equal(t, ingressID, coordinator.ServiceID())
	peer, err := cluster.GetCNService(1)
	require.NoError(t, err)
	open := func(cn embed.ServiceOperator) *sql.DB {
		t.Helper()
		port := cn.GetServiceConfig().CN.Frontend.Port
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", port))
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, db.Close()) })
		return db
	}
	db, peerDB := open(coordinator), open(peer)
	const schema = "join_placement_33150"
	execSQL := func(t *testing.T, q string) {
		t.Helper()
		_, err := db.ExecContext(ctx, q)
		require.NoError(t, err, q)
	}
	execSQL(t, "create database "+schema)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		_, err := db.ExecContext(cleanupCtx, "drop database "+schema)
		require.NoError(t, err)
	})
	for _, q := range []string{
		"create table " + schema + ".a(id int,k int)",
		"create table " + schema + ".c(rid int,k int,v int)",
		"insert into " + schema + ".a values(1,1),(2,1),(3,2),(4,NULL)",
		"insert into " + schema + ".c values(11,1,1),(12,1,3),(13,2,NULL),(14,3,7)",
		"create table " + schema + ".append_only(k int primary key)",
		"insert into " + schema + ".append_only values(42)",
		"create table " + schema + ".docs(id int primary key, body text, emb vecf32(4))",
		"insert into " + schema + ".docs values(1,'alpha','[1,0,0,0]'),(2,'alpha','[1,0,0,0]')",
		"create fulltext index ft on " + schema + ".docs(body)",
		"create index ivf using ivfflat on " + schema + ".docs(emb) lists=1 op_type 'vector_l2_ops'",
	} {
		execSQL(t, q)
	}
	var ivfEntries, ftEntries string
	indexTable := `select index_table_name from mo_catalog.mo_indexes where table_id=(select rel_id from mo_catalog.mo_tables where reldatabase=? and relname='docs') and name=?`
	require.NoError(t, db.QueryRowContext(ctx, indexTable+" and algo_table_type='entries'", schema, "ivf").Scan(&ivfEntries))
	require.NoError(t, db.QueryRowContext(ctx, indexTable, schema, "ft").Scan(&ftEntries))
	hiddenName := regexp.MustCompile(`^__mo_index_secondary_[a-f0-9-]+$`)
	require.Regexp(t, hiddenName, ivfEntries)
	require.Regexp(t, hiddenName, ftEntries)
	// Leave IVF metadata and centroids unflushed: the remote UPDATE must
	// find the current version and centroid even when they are appendable.
	for _, table := range []string{"a", "c", "docs", ftEntries, ivfEntries} {
		execSQL(t, "select mo_ctl('dn','flush','"+schema+"."+table+"')")
	}
	// USE checks the catalog via an internal background query. Complete all
	// session admission before forcing AP, which targets the statements below.
	var connections [4]*sql.Conn
	for i := range connections {
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, conn.Close()) })
		for _, q := range []string{"use " + schema, "set session query_max_workers=1", "set session enable_explain_scheduling=1"} {
			_, err := conn.ExecContext(ctx, q)
			require.NoError(t, err, q)
		}
		connections[i] = conn
	}
	checkIndexes := func(t *testing.T, id, body, vec string) {
		t.Helper()
		newest := coordinator.RawService().(cnservice.Service).GetTxnClient().GetLatestCommitTS()
		require.False(t, newest.IsEmpty())
		for _, cn := range []embed.ServiceOperator{coordinator, peer} {
			svc := cn.RawService().(cnservice.Service)
			_, err := svc.GetTimestampWaiter().GetTimestamp(ctx, newest)
			require.NoError(t, err, "fence %s at %s", cn.ServiceID(), newest.DebugString())
			svc.GetTxnClient().SyncLatestCommitTS(newest)
		}
		for slot, reader := range []*sql.DB{db, peerDB} {
			var got string
			var distance float64
			var ivfCount, ivfExact, ftOld, ftNew int
			q := "select body, l2_distance(emb, '" + vec + "'), " +
				"(select count(*) from " + schema + ".`" + ivfEntries + "` where __mo_index_pri_col=" + id + "), " +
				"(select count(*) from " + schema + ".`" + ivfEntries + "` where __mo_index_pri_col=" + id +
				" and l2_distance(__mo_index_centroid_fk_entry, '" + vec + "')=0), " +
				"(select count(*) from " + schema + ".`" + ftEntries + "` where doc_id=" + id + " and word='alpha'), " +
				"(select count(*) from " + schema + ".`" + ftEntries + "` where doc_id=" + id + " and word='omega') " +
				"from " + schema + ".docs where id=" + id
			require.NoError(t, reader.QueryRowContext(ctx, q).Scan(&got, &distance, &ivfCount, &ivfExact, &ftOld, &ftNew))
			t.Logf("CN%d pk=%s base=%s distance=%v ivf=%d/%d fulltext-alpha/omega=%d/%d fence=%s",
				slot, id, got, distance, ivfCount, ivfExact, ftOld, ftNew, newest.DebugString())
			assert.Equal(t, body, got)
			assert.Zero(t, distance)
			assert.Equal(t, 1, ivfCount)
			assert.Equal(t, 1, ivfExact)
			if body == "alpha" {
				assert.Positive(t, ftOld)
				assert.Zero(t, ftNew)
			} else {
				assert.Zero(t, ftOld)
				assert.Positive(t, ftNew)
			}
		}
	}
	const update = "update docs set body='omega',emb='[0,0,1,0]' where id="
	t.Run("coordinator_control", func(t *testing.T) {
		r, err := connections[3].ExecContext(ctx, update+"2")
		require.NoError(t, err)
		rows, err := r.RowsAffected()
		require.NoError(t, err)
		require.Equal(t, int64(1), rows)
		checkIndexes(t, "2", "omega", "[0,0,1,0]")
	})
	inv := clusterservice.GetMOCluster(coordinator.ServiceID())
	refresher, ok := inv.(clusterservice.AuthoritativeRefresher)
	require.True(t, ok)
	require.Eventually(t, func() bool {
		if refresher.Refresh(ctx) != nil {
			return false
		}
		states := make(map[string]metadata.WorkState)
		inv.GetCNServiceWithoutWorkingState(clusterservice.NewSelector(), func(s metadata.CNService) bool {
			states[s.ServiceID] = s.WorkState
			return true
		})
		return len(states) == 2 && states[ingressID] == metadata.WorkState_Draining &&
			states[peer.ServiceID()] == metadata.WorkState_Working
	}, 20*time.Second, 100*time.Millisecond, "expected draining SQL ingress and one working peer")
	t.Run("read_own_appendable_changes", func(t *testing.T) {
		// Row 42 is committed but still appendable. A remote worker with a
		// fresh workspace would see 42 and miss the replacement row 43.
		for _, tc := range []struct {
			name     string
			db       *sql.DB
			eligible bool
		}{{"draining_owner", db, false}, {"working_owner", peerDB, true}} {
			t.Run(tc.name, func(t *testing.T) {
				conn, err := tc.db.Conn(ctx)
				require.NoError(t, err)
				defer conn.Close()
				_, err = conn.ExecContext(ctx, "set session query_max_workers=1")
				require.NoError(t, err)
				tx, err := conn.BeginTx(ctx, nil)
				require.NoError(t, err)
				defer tx.Rollback()
				r, err := tx.ExecContext(ctx, "delete from "+schema+".append_only where k=42")
				require.NoError(t, err)
				deleted, err := r.RowsAffected()
				require.NoError(t, err)
				require.EqualValues(t, 1, deleted)
				_, err = tx.ExecContext(ctx, "insert into "+schema+".append_only values(43)")
				require.NoError(t, err)

				oldForce := plan.GetForceScanOnMultiCN()
				plan.SetForceScanOnMultiCN(true)
				defer plan.SetForceScanOnMultiCN(oldForce)
				var key, count int
				err = tx.QueryRowContext(ctx, "select k,count(*) over() from "+schema+".append_only order by k").Scan(&key, &count)
				if !tc.eligible {
					require.ErrorContains(t, err, "current-cn-draining", "ineligible workspace owner must not yield a stale row (got %d)", key)
					return
				}
				require.NoError(t, err)
				require.Equal(t, 43, key)
				require.Equal(t, 1, count)
			})
		}
	})
	t.Run("remote_appendable_range", func(t *testing.T) {
		conn := connections[2]
		const q = "select k,row_number() over(order by k) rn from append_only"
		const want = "42\t1\n"
		require.Equal(t, want, queryText(t, ctx, conn, q))
		remoteRead := func() string {
			oldForce := plan.GetForceScanOnMultiCN()
			plan.SetForceScanOnMultiCN(true)
			defer plan.SetForceScanOnMultiCN(oldForce)
			phy := queryText(t, ctx, conn, "explain phyplan analyze "+q)
			require.Contains(t, phy, peer.ServiceID(), "route must be selected in execution")
			return queryText(t, ctx, conn, q)
		}
		before := remoteRead()
		t.Logf("remote peer=%s appendable rows=%q", peer.ServiceID(), before)
		assert.Equal(t, want, before)
		execSQL(t, "select mo_ctl('dn','flush','"+schema+".append_only')")
		after := remoteRead()
		t.Logf("remote peer=%s persisted rows=%q", peer.ServiceID(), after)
		require.Equal(t, want, after)
	})
	for i, tc := range []struct{ name, q, want string }{
		{"window_probe", "select d.id,d.rn,c.v from (select id,k,row_number() over(order by id) as rn from a) d left join c on d.k=c.k order by d.id,c.v", "1\t1\t1\n1\t1\t3\n2\t2\t1\n2\t2\t3\n3\t3\tNULL\n4\t4\tNULL\n"},
		{"window_build", "select a.id,d.v from a left join (select k,v,row_number() over(partition by k order by rid desc) as rn from c) d on a.k=d.k and d.rn=1 order by a.id", "1\t3\n2\t3\n3\tNULL\n4\tNULL\n"},
		{"window_empty_local_control", "select d.id from (select id,k,row_number() over(order by id) rn from a where id<0) d left join c on d.k=c.k", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			conn := connections[i]
			oldForce := plan.GetForceScanOnMultiCN()
			if tc.want != "" {
				plan.SetForceScanOnMultiCN(true)
			}
			defer plan.SetForceScanOnMultiCN(oldForce)
			qctx, done := context.WithTimeout(ctx, 10*time.Second)
			defer done()
			require.Equal(t, tc.want, queryText(t, qctx, conn, tc.q))
			if tc.want != "" {
				physical := queryText(t, qctx, conn, "explain phyplan analyze "+tc.q)
				require.Contains(t, physical, peer.ServiceID(), "real execution must select the peer")
			}
		})
	}
	t.Run("indexed_update", func(t *testing.T) {
		conn := connections[3]
		describe := func() string {
			oldForce := plan.GetForceScanOnMultiCN()
			plan.SetForceScanOnMultiCN(true)
			defer plan.SetForceScanOnMultiCN(oldForce)
			return queryText(t, ctx, conn, "explain verbose "+update+"1")
		}
		desc := describe()
		t.Log(desc)
		require.Contains(t, desc, "exec=ap-multi-cn")
		require.Regexp(t, `Selected workers: [^\n]*`+regexp.QuoteMeta(peer.ServiceID())+`[^\n]*remote`, desc)
		require.Contains(t, desc, ivfEntries)
		require.Contains(t, desc, ftEntries)
		require.Contains(t, strings.ToLower(desc), "sink scan")
		for _, tc := range []struct {
			name, id string
			rows     int64
			commit   bool
		}{
			{"empty", "99", 0, true}, {"rollback", "1", 1, false}, {"commit", "1", 1, true},
		} {
			t.Run(tc.name, func(t *testing.T) {
				qctx, done := context.WithTimeout(ctx, 10*time.Second)
				defer done()
				tx, err := conn.BeginTx(qctx, nil)
				require.NoError(t, err)
				defer tx.Rollback()
				apply := func() (sql.Result, error) {
					oldForce := plan.GetForceScanOnMultiCN()
					plan.SetForceScanOnMultiCN(true)
					defer plan.SetForceScanOnMultiCN(oldForce)
					return tx.ExecContext(qctx, update+tc.id)
				}
				r, err := apply()
				require.NoError(t, err)
				rows, err := r.RowsAffected()
				require.NoError(t, err)
				require.Equal(t, tc.rows, rows)
				if tc.commit {
					require.NoError(t, tx.Commit())
				} else {
					require.NoError(t, tx.Rollback())
				}
				body, vec := "alpha", "[1,0,0,0]"
				if tc.name == "commit" {
					body, vec = "omega", "[0,0,1,0]"
				}
				checkIndexes(t, "1", body, vec)
			})
			if t.Failed() {
				break // a failed statement may have closed this connection or left an unknown result
			}
		}
	})
}

func queryText(t *testing.T, ctx context.Context, conn *sql.Conn, q string) string {
	t.Helper()
	rows, err := conn.QueryContext(ctx, q)
	require.NoError(t, err, q)
	defer rows.Close()
	cols, err := rows.Columns()
	require.NoError(t, err)
	var out strings.Builder
	for rows.Next() {
		values := make([]sql.NullString, len(cols))
		pointers := make([]any, len(cols))
		for i := range values {
			pointers[i] = &values[i]
		}
		require.NoError(t, rows.Scan(pointers...))
		for i, v := range values {
			if i != 0 {
				out.WriteByte('\t')
			}
			if v.Valid {
				out.WriteString(v.String)
			} else {
				out.WriteString("NULL")
			}
		}
		out.WriteByte('\n')
	}
	require.NoError(t, rows.Err())
	return out.String()
}
