// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
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
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

func TestClusterReplayPublicAdmission(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		defer cancel()
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		exec := func(query string) { _, err := db.ExecContext(ctx, query); require.NoError(t, err, query) }
		exec("create database cluster_replay_public")
		defer cleanupSQLIntegration(t, cn, "drop database if exists cluster_replay_public")
		exec("create table cluster_replay_public.metrics(tenant_id int,device_id int,metric_id int,event_ts datetime,value double) cluster by(tenant_id,device_id,metric_id,event_ts)")
		exec("insert into cluster_replay_public.metrics values(0,0,0,'2026-01-01 00:03:00',19),(0,0,0,'2026-01-01 00:03:10',20),(0,1,0,'2026-01-01 00:03:00',1),(0,null,0,'2026-01-01 00:03:00',2),(0,0,0,'2026-01-01 00:00:00',3),(0,0,0,'2026-01-01 00:01:00',4)")
		query := "select count(*),sum(value) from cluster_replay_public.metrics where tenant_id=0 and device_id=0 and metric_id=0 and event_ts>='2026-01-01 00:03:00'"
		check := func(query string, wantRows int, wantSum float64, scanned int) {
			var count int
			var sum float64
			require.NoError(t, db.QueryRowContext(ctx, query).Scan(&count, &sum))
			require.Equal(t, wantRows, count)
			require.Equal(t, wantSum, sum)
			rows, err := db.QueryContext(ctx, "explain analyze "+query)
			require.NoError(t, err)
			defer rows.Close()
			scanNext, checked := false, false
			for rows.Next() {
				var line string
				require.NoError(t, rows.Scan(&line))
				if strings.Contains(line, "Table Scan on cluster_replay_public.metrics") {
					scanNext = true
				} else if scanNext && strings.Contains(line, "Analyze:") {
					require.Contains(t, line, fmt.Sprintf("inputRows=%d outputRows=%d ", scanned, wantRows), "verify index pruning or exhaustive fallback with real residual filtering")
					require.Contains(t, line, "ReadSize=0 bytes|0 bytes|0 bytes", "this exercises unflushed replay, not object pruning")
					checked = true
					scanNext = false
				}
			}
			require.NoError(t, rows.Err())
			require.True(t, checked, "EXPLAIN must include the real scan analysis")
		}
		check(query, 2, 39, 2)
		// Four matches reach the bounded probe budget: the same reader must
		// scan all six candidates, then reject the two nonmatching rows.
		check(strings.Replace(query, "00:03:00", "00:00:00", 1), 4, 46, 6)
	})
}
