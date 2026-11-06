//go:build mo_m0

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

package sqlintegration

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/stretchr/testify/require"
)

func TestM0ModeGeneratedColumnUsesCopyAndRestartSQL(t *testing.T) {
	runSQLIntegration(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
		defer cancel()
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		var db *sql.DB
		running := true
		defer func() {
			if db != nil {
				_ = db.Close()
			}
			if !running {
				if err := cluster.Start(); err != nil {
					t.Errorf("restart cluster for cleanup: %v", err)
					return
				}
			}
			cleanupSQLIntegration(t, cn, "drop database if exists m0_generated_recovery")
		}()
		db = openM0SQL(t, cn)
		assertOrdinary := func() {
			t.Helper()
			var raw []byte
			require.NoError(t, db.QueryRowContext(ctx, "select extra_info from mo_catalog.mo_tables where reldatabase='m0_generated_recovery' and relname='t'").Scan(&raw))
			var extra api.SchemaExtra
			require.NoError(t, extra.Unmarshal(raw))
			require.Nil(t, extra.M0Evolution, "generated-column tables cannot acquire an M0 root")
		}
		m0Exec(t, ctx, db, "create database m0_generated_recovery")
		m0Exec(t, ctx, db, "create table m0_generated_recovery.t(a int not null,b int not null,g int generated always as (a+b) stored,primary key(a,b))")
		m0Exec(t, ctx, db, "insert into m0_generated_recovery.t(a,b) values(2,3)")
		m0Exec(t, ctx, db, "select mo_ctl('dn','flush','m0_generated_recovery.t')")
		m0Exec(t, ctx, db, "select mo_ctl('dn','globalcheckpoint','')")
		assertOrdinary()
		id, _ := m0TableGeneration(t, ctx, db, "m0_generated_recovery", "t")
		m0Exec(t, ctx, db, "alter table m0_generated_recovery.t add column x int")
		newID, newVersion := m0TableGeneration(t, ctx, db, "m0_generated_recovery", "t")
		require.NotEqual(t, id, newID, "generated-column tables remain ordinary COPY, not M0")
		require.Zero(t, newVersion)
		assertOrdinary()
		require.Equal(t, 1, m0Count(t, ctx, db, "select count(*) from m0_generated_recovery.t where g=5 and x is null"))
		require.NoError(t, db.Close())
		db = nil
		require.NoError(t, cluster.Stop())
		running = false
		require.NoError(t, cluster.Start())
		running = true
		cn, err = cluster.GetCNService(0)
		require.NoError(t, err)
		db = openM0SQL(t, cn)
		require.Equal(t, 1, m0Count(t, ctx, db, "select count(*) from m0_generated_recovery.t where g=5 and x is null"))
		m0Exec(t, ctx, db, "insert into m0_generated_recovery.t(a,b,x) values(4,5,7)")
		require.Equal(t, 1, m0Count(t, ctx, db, "select count(*) from m0_generated_recovery.t where g=9 and x=7"))
		assertOrdinary()
		m0Exec(t, ctx, db, "select mo_ctl('dn','flush','m0_generated_recovery.t')")
		// COPY may already have produced a higher-level object. Two distinct
		// post-restart flushes supply real ordinary L0 merge inputs.
		m0Exec(t, ctx, db, "insert into m0_generated_recovery.t(a,b,x) values(6,7,9)")
		m0Exec(t, ctx, db, "select mo_ctl('dn','flush','m0_generated_recovery.t')")
		require.Equal(t, 1, m0Count(t, ctx, db, "select count(*) from m0_generated_recovery.t where g=13 and x=9"))
		before, err := m0GeneratedObjectNames(ctx, db)
		require.NoError(t, err)
		require.NotEmpty(t, before)
		m0RunL0Merge(t, ctx, db, "m0_generated_recovery.t")
		require.Eventually(t, func() bool {
			after, scanErr := m0GeneratedObjectNames(ctx, db)
			if scanErr != nil || len(after) == 0 {
				return false
			}
			old, current := make(map[string]bool), make(map[string]bool)
			for _, name := range before {
				old[name] = true
			}
			for _, name := range after {
				current[name] = true
			}
			removed, added := false, false
			for name := range old {
				removed = removed || !current[name]
			}
			for name := range current {
				added = added || !old[name]
			}
			return removed && added
		}, 30*time.Second, 100*time.Millisecond, "data merge must publish a new object and retire an input")
		require.Equal(t, 1, m0Count(t, ctx, db, "select count(*) from m0_generated_recovery.t where g=5 and x is null"))
		require.Equal(t, 1, m0Count(t, ctx, db, "select count(*) from m0_generated_recovery.t where g=9 and x=7"))
	})
}

func m0GeneratedObjectNames(ctx context.Context, db *sql.DB) ([]string, error) {
	rows, err := db.QueryContext(ctx, "select distinct object_name from metadata_scan(?, 'a') g order by object_name", "m0_generated_recovery.t")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var names []string
	for rows.Next() {
		var name string
		if err = rows.Scan(&name); err != nil {
			return nil, err
		}
		names = append(names, name)
	}
	return names, rows.Err()
}
