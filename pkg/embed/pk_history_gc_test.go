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
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/incrservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	"github.com/stretchr/testify/require"
)

func TestAutoIncrementAfterHistoryGC(t *testing.T) {
	c, err := NewCluster(WithTesting(), WithTestAdmissionContext(t.Context()), WithCNCount(2), WithPreStart(func(svc ServiceOperator) {
		adjustBasicClusterService(svc)
		if svc.ServiceType() != metadata.ServiceType_TN {
			return
		}
		svc.Adjust(func(cfg *ServiceConfig) {
			tn := cfg.TN_please_use_getTNServiceConfig
			if tn == nil {
				tn = cfg.TNCompatible
			}
			if tn != nil {
				tn.GCCfg.CatalogGCInterval.Duration = time.Second
				tn.GCCfg.ScanGCInterval.Duration = 2 * time.Second
				tn.Ckp.IncrementalInterval.Duration = 2 * time.Second
			}
		})
	}))
	if c != nil {
		t.Cleanup(func() { require.NoError(t, c.Close()); require.NoError(t, os.RemoveAll(c.(*cluster).options.dataPath)) })
	}
	require.NoError(t, err)
	require.NoError(t, c.Start())
	ctx, cancel := context.WithTimeout(defines.AttachAccountId(t.Context(), 0), 5*time.Minute)
	defer cancel()
	cn0, err := c.GetCNService(0)
	require.NoError(t, err)
	cn1, err := c.GetCNService(1)
	require.NoError(t, err)
	db0 := recoverySQLClient(t, cn0)
	t.Cleanup(func() { _ = db0.Close() })
	db1 := recoverySQLClient(t, cn1)
	t.Cleanup(func() { _ = db1.Close() })
	exec := func(db *sql.DB, q string) { t.Helper(); _, e := db.ExecContext(ctx, q); require.NoError(t, e, q) }
	syncCN := func() {
		t.Helper()
		ts := cn1.RawService().(cnservice.Service).GetTxnClient().GetLatestCommitTS()
		_, e := cn0.RawService().(cnservice.Service).GetTimestampWaiter().GetTimestamp(ctx, ts)
		require.NoError(t, e)
	}
	exec(db1, "create database pk_history_probe")
	exec(db1, "create table pk_history_probe.t(id bigint auto_increment primary key, v int)")
	exec(db1, "insert into pk_history_probe.t values(1,1)")
	syncCN()
	exec(db0, "insert into pk_history_probe.t(v) values(2)")
	var generated int64
	require.NoError(t, db0.QueryRowContext(ctx, "select id from pk_history_probe.t where v=2").Scan(&generated))
	require.Less(t, generated, int64(1000000))
	var dbID, tableID uint64
	require.NoError(t, db0.QueryRowContext(ctx, "select reldatabase_id,rel_id from mo_catalog.mo_tables where reldatabase='pk_history_probe' and relname='t'").Scan(&dbID, &tableID))
	allocator := incrservice.GetAutoIncrementService(cn0.ServiceID())
	require.NotNil(t, allocator)
	allocated, err := allocator.GetLastAllocateTS(ctx, tableID, 0, nil, "id")
	require.NoError(t, err)
	from := types.TimestampToTS(allocated)
	t.Logf("reserved generated=%d from=%s table=%d", generated, from.ToString(), tableID)
	exec(db1, "insert into pk_history_probe.t values(1000000,3)")
	exec(db1, "select mo_ctl('dn','flush','pk_history_probe.t')")
	names := func() []string {
		t.Helper()
		rows, e := db1.QueryContext(ctx, "select distinct object_name from metadata_scan('pk_history_probe.t','id') m")
		require.NoError(t, e)
		defer rows.Close()
		var out []string
		for rows.Next() {
			var n string
			require.NoError(t, rows.Scan(&n))
			out = append(out, n)
		}
		require.NoError(t, rows.Err())
		return out
	}
	old := names()
	require.NotEmpty(t, old)
	shared := filepath.Join(c.(*cluster).options.dataPath, "s3")
	for _, n := range old {
		_, e := os.Stat(filepath.Join(shared, n))
		require.NoError(t, e)
	}
	syncCN()
	eng := cn0.RawService().(cnservice.Service).GetEngine().(*disttae.Engine)
	snap := eng.GetOrCreateLatestPart(ctx, 0, dbID, tableID).Snapshot()
	deleted, created := snap.GetChangedObjsBetween(from, types.MaxTs())
	obsolete := make([]string, 0, len(deleted))
	for n := range deleted {
		o, ok := snap.GetObject(n)
		require.True(t, ok)
		require.True(t, o.CreateTime.LE(&from))
		require.True(t, o.DeleteTime.GT(&from))
		obsolete = append(obsolete, o.ObjectName().String())
		t.Logf("deleted object=%s create=%s delete=%s from=%s", o.ObjectName().String(), o.CreateTime.ToString(), o.DeleteTime.ToString(), from.ToString())
	}
	t.Logf("candidate metadata deleted=%d created=%d", len(deleted), len(created))
	require.NotEmpty(t, deleted, "must select old objects across the allocator timestamp")
	// The exact historical source must exist before explicit GC. Normal GC
	// keeps its production TTL so construction cannot race reclamation.
	for _, name := range obsolete {
		_, err := os.Stat(filepath.Join(shared, name))
		require.NoError(t, err, "persisted source before GC")
	}
	// ForceGC samples active transaction minima. Start controls on both CNs
	// after retirement, so neither pins the obsolete source nor leaves the
	// debug RPC with an empty minimum on whichever CN it samples first.
	controls := make([]*sql.Tx, 0, 2)
	for _, db := range []*sql.DB{db0, db1} {
		tx, err := db.BeginTx(ctx, nil)
		require.NoError(t, err)
		t.Cleanup(func() { _ = tx.Rollback() })
		var rows int
		require.NoError(t, tx.QueryRowContext(ctx, "select count(*) from pk_history_probe.t").Scan(&rows))
		require.Equal(t, 3, rows)
		controls = append(controls, tx)
	}
	deadline := time.Now().Add(90 * time.Second)
	for {
		reclaimed := true
		for _, n := range obsolete {
			_, e := os.Stat(filepath.Join(shared, n))
			if e == nil {
				reclaimed = false
			} else {
				require.ErrorIs(t, e, os.ErrNotExist)
			}
		}
		if reclaimed {
			break
		}
		require.True(t, time.Now().Before(deadline), "GC must physically reclaim %v", obsolete)
		exec(db1, "select mo_ctl('dn','globalcheckpoint','')")
		exec(db1, "select mo_ctl('dn','diskcleaner','force_gc')")
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(2 * time.Second):
		}
	}
	for _, tx := range controls {
		require.NoError(t, tx.Rollback())
	}
	before, err := allocator.GetLastAllocateTS(ctx, tableID, 0, nil, "id")
	require.NoError(t, err)
	require.Equal(t, allocated, before)
	_, err = db0.ExecContext(ctx, "insert into pk_history_probe.t(v) values(5)")
	require.NoError(t, err, "automatic insert after reclaiming %v", obsolete)
	after, err := allocator.GetLastAllocateTS(ctx, tableID, 0, nil, "id")
	require.NoError(t, err)
	fresh := types.TimestampToTS(after)
	require.True(t, fresh.GT(&from), "retry must replace the stale allocation")
	var count, unique, manual, id int64
	require.NoError(t, db0.QueryRowContext(ctx, "select count(*),count(distinct id),sum(id=1000000),max(case when v=5 then id else 0 end) from pk_history_probe.t").Scan(&count, &unique, &manual, &id))
	require.Equal(t, int64(4), count)
	require.Equal(t, count, unique)
	require.Equal(t, int64(1), manual)
	require.Greater(t, id, int64(1000000), "retry uses a fresh reservation beyond the remote manual key")
}
