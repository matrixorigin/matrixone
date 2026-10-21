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

package isolated

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/stretchr/testify/require"
)

func TestMoTableStatsAfterOtherClusterCloses(t *testing.T) {
	// Independent clusters avoid a stopped peer changing the surviving CN's
	// membership/routing. One CN each is enough to prove statistics ownership.
	start := func() embed.Cluster {
		c, err := embed.StartTestCluster(embed.WithCNCount(1), embed.WithPreStart(func(s embed.ServiceOperator) {
			if s.ServiceType() == metadata.ServiceType_CN {
				s.Adjust(func(cfg *embed.ServiceConfig) {
					cfg.CN.Frontend.SkipCheckUser = false
					cfg.CN.SQLAddress = fmt.Sprintf("127.0.0.1:%d", cfg.CN.Frontend.Port)
				})
			}
		}))
		if c != nil {
			var dataDir string
			c.ForeachServices(func(s embed.ServiceOperator) bool {
				dataDir = s.GetServiceConfig().DataDir
				return false
			})
			t.Cleanup(func() {
				if err := c.Close(); err != nil {
					t.Errorf("close owned cluster: %v", err)
					return
				}
				if dataDir != "" {
					require.NoError(t, os.RemoveAll(dataDir))
					require.NoDirExists(t, dataDir)
				}
			})
		}
		require.NoError(t, err)
		return c
	}
	first := start()
	cn, err := first.GetCNService(0)
	require.NoError(t, err)
	control := cn.RawService().(cnservice.Service).GetEngine().(interface{ HandleMoTableStatsCtl(string) string })
	control.HandleMoTableStatsCtl("use_old_impl:false")
	control.HandleMoTableStatsCtl("force_update:false")
	db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
	require.NoError(t, err)
	defer db.Close()
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	defer conn.Close()
	for _, statement := range []string{
		"create database stats_owner",
		"use stats_owner",
		"create table t(id bigint primary key auto_increment, v int, key ix(v)) auto_increment=10",
		"set mo_table_stats.use_old_impl='no'",
	} {
		_, err = conn.ExecContext(ctx, statement)
		require.NoError(t, err)
	}
	cancel()
	check := func() {
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()
		var count, size int64
		require.NoError(t, conn.QueryRowContext(ctx, "select mo_table_rows(database(),'t'), mo_table_size(database(),'t')").Scan(&count, &size))
		require.Zero(t, count)
		require.Zero(t, size)
		rows, err := conn.QueryContext(ctx, "show table status")
		require.NoError(t, err)
		defer rows.Close()
		columns, err := rows.Columns()
		require.NoError(t, err)
		require.True(t, rows.Next())
		var name string
		var next int64
		dest := make([]any, len(columns))
		for i, col := range columns {
			dest[i] = new(any)
			switch strings.ToLower(col) {
			case "name":
				dest[i] = &name
			case "auto_increment":
				dest[i] = &next
			}
		}
		require.NoError(t, rows.Scan(dest...))
		require.Equal(t, "t", name)
		require.Equal(t, int64(10), next)
		require.False(t, rows.Next())
		require.NoError(t, rows.Err())
	}
	check()
	second := start()
	require.NoError(t, second.Close())
	check()
}
