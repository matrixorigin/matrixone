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

package embed

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/stretchr/testify/require"
)

// This tests existing Clone transaction retention, not a Rebase executor.
// A dropped, unprotected object's physical deletion proves disk GC actually ran.
func TestRebaseRetainedBaseGC(t *testing.T) {
	require.NoError(t, CloseSingleCNBaseClusterTests())
	c, err := NewCluster(WithTesting(), WithCNCount(1), WithTestAdmissionContext(t.Context()), WithPreStart(func(svc ServiceOperator) {
		adjustBasicClusterService(svc)
		if svc.ServiceType() == metadata.ServiceType_TN {
			svc.Adjust(func(cfg *ServiceConfig) {
				tn := cfg.TN_please_use_getTNServiceConfig
				if tn == nil {
					tn = cfg.TNCompatible
				}
				if tn != nil {
					tn.GCCfg.CatalogGCInterval.Duration = time.Second
					tn.GCCfg.ScanGCInterval.Duration = 2 * time.Second
					tn.GCCfg.GCTTL.Duration = time.Millisecond
					tn.GCCfg.GCInMemoryTTL.Duration = time.Millisecond
					tn.Ckp.IncrementalInterval.Duration = 2 * time.Second
				}
			})
		}
	}))
	if c != nil {
		t.Cleanup(func() {
			require.NoError(t, c.Close())
			if !t.Failed() {
				require.NoError(t, os.RemoveAll(c.(*cluster).options.dataPath))
			}
		})
	}
	require.NoError(t, err)
	require.NoError(t, c.Start())
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Minute)
	defer cancel()
	cn, err := c.GetCNService(0)
	require.NoError(t, err)
	db := recoverySQLClient(t, cn)
	defer func() { require.NoError(t, db.Close()) }()
	exec := func(q string) { t.Helper(); _, err := db.ExecContext(ctx, q); require.NoError(t, err, q) }
	count := func(q string) int {
		t.Helper()
		var n int
		require.NoError(t, db.QueryRowContext(ctx, q).Scan(&n), q)
		return n
	}
	exec("create database rebase_gc_source")
	for _, table := range []string{"a", "b"} {
		exec("create table rebase_gc_source." + table + "(id int primary key,v int)")
		exec("insert into rebase_gc_source." + table + " values(1,10),(2,20)")
		exec("select mo_ctl('dn','flush','rebase_gc_source." + table + "')")
	}
	exec("create snapshot rebase_gc_u for database rebase_gc_source")
	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	defer conn.Close()
	for _, q := range []string{"set session transaction isolation level read committed", "begin", "data branch create database rebase_gc_target from rebase_gc_source{snapshot='rebase_gc_u'}", "update rebase_gc_target.a set v=11 where id=1", "delete from rebase_gc_target.b where id=2", "commit"} {
		_, err := conn.ExecContext(ctx, q)
		require.NoError(t, err, q)
	}
	require.NoError(t, conn.Close())
	var base int64
	require.NoError(t, db.QueryRowContext(ctx, "select min(clone_ts) from mo_catalog.mo_branch_metadata where table_id in (select rel_id from mo_catalog.mo_tables where reldatabase='rebase_gc_target')").Scan(&base))
	require.Positive(t, base)
	exec("drop snapshot rebase_gc_u")
	exec("drop database rebase_gc_source")
	check := func() {
		t.Helper()
		require.Equal(t, 11, count("select v from rebase_gc_target.a where id=1"))
		require.Equal(t, 1, count("select count(*) from rebase_gc_target.b"))
		for _, table := range []string{"a", "b"} {
			require.Equal(t, 30, count(fmt.Sprintf("select sum(v) from rebase_gc_source.%s{MO_TS=%d}", table, base)))
		}
		require.Equal(t, 2, count("select count(*) from mo_catalog.mo_snapshots where kind='branch' and database_name='rebase_gc_source'"))
		require.Equal(t, 2, count(fmt.Sprintf("select count(*) from mo_catalog.mo_branch_metadata where clone_ts=%d and table_id in (select rel_id from mo_catalog.mo_tables where reldatabase='rebase_gc_target')", base)))
	}
	check()
	assertDiskGCReclaimsProbe(t, ctx, db, c, branchRestartScenario{database: "rebase_gc_target", reclaimProbeTable: "unprotected_gc_probe"})
	check()
	require.NoError(t, db.Close())
	require.NoError(t, c.Stop())
	require.NoError(t, c.Start())
	cn, err = c.GetCNService(0)
	require.NoError(t, err)
	db = recoverySQLClient(t, cn)
	check()
}
