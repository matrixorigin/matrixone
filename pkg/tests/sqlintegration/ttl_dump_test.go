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

package sqlintegration

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/stretchr/testify/require"
)

// Reuse the canonical one-CN fixture. Each case replays only the dump's own
// CREATE statement, then LOADs it through MySQL; SHOW CREATE is an oracle,
// never a replacement for the manifest being tested.
func TestTTLDumpReplayAfterAlter(t *testing.T) {
	runSQLIntegration(t, func(cluster embed.Cluster) {
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
		defer cancel()
		client, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer client.Close()
		conn, err := client.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		db := strings.ToLower(testutils.GetDatabaseName(t))
		_, err = conn.ExecContext(ctx, "create database "+sqlquote.Ident(db))
		require.NoError(t, err)
		defer cleanupSQLIntegration(t, cn, "drop database if exists "+sqlquote.Ident(db))
		_, err = conn.ExecContext(ctx, "use "+sqlquote.Ident(db))
		require.NoError(t, err)
		for _, metadataOnly := range []bool{true, false} {
			for _, tc := range []struct {
				name, initial, alter string
				seconds              uint64
			}{
				{"unchanged", " ttl ts + interval 1 day", "", 86400},
				{"add", "", "modify ttl ts + interval 1 day", 86400},
				{"modify", " ttl ts + interval 1 day", "modify ttl ts + interval 2 day", 172800},
				{"remove", " ttl ts + interval 1 day", "remove ttl", 0},
			} {
				t.Run(fmt.Sprintf("%s/metadata_only=%t", tc.name, metadataOnly), func(t *testing.T) {
					exec := func(q string) { t.Helper(); _, err := conn.ExecContext(ctx, q); require.NoError(t, err, q) }
					table := fmt.Sprintf("replay_%s_%t", tc.name, metadataOnly)
					exec("create table " + table + "(id int primary key, ts datetime(6), payload varchar(20) default 'kept')" + tc.initial)
					if tc.alter != "" {
						exec("alter table " + table + " " + tc.alter)
					}
					if !metadataOnly {
						exec("insert into " + table + " values(1,'2027-01-01 00:00:00.123456','original')")
						var flushed string
						require.NoError(t, conn.QueryRowContext(ctx, "select mo_ctl('dn','flush','"+db+"."+table+"')").Scan(&flushed))
					}
					path := t.TempDir()
					dump := "dump table " + table + " to '" + path + "'"
					if metadataOnly {
						dump += " metadata only"
					}
					exec(dump)
					data, err := os.ReadFile(filepath.Join(path, "manifest.json"))
					require.NoError(t, err)
					var manifest struct {
						CreateSQL    string `json:"create_sql"`
						MetadataOnly bool   `json:"metadata_only"`
					}
					require.NoError(t, json.Unmarshal(data, &manifest))
					require.NotEmpty(t, manifest.CreateSQL)
					require.Equal(t, metadataOnly, manifest.MetadataOnly)
					exec("drop table " + table)
					exec(manifest.CreateSQL)
					exec("load table " + table + " from '" + path + "'")
					var name, shown string
					require.NoError(t, conn.QueryRowContext(ctx, "show create table "+table).Scan(&name, &shown))
					if tc.seconds == 0 {
						require.NotContains(t, strings.ToUpper(shown), "TTL ")
					} else {
						require.Contains(t, strings.ToUpper(shown), fmt.Sprintf("TTL `TS` + INTERVAL %d SECOND", tc.seconds))
					}
					var count int
					require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from "+table).Scan(&count))
					if metadataOnly {
						require.Zero(t, count)
					} else {
						require.Equal(t, 1, count)
						var payload, ts string
						require.NoError(t, conn.QueryRowContext(ctx, "select payload,cast(ts as char) from "+table+" where id=1").Scan(&payload, &ts))
						require.Equal(t, "original", payload)
						require.Equal(t, "2027-01-01 00:00:00.123456", ts)
					}
					exec("insert into " + table + "(id,ts) values(2,'2027-01-02')")
					var defaultValue string
					require.NoError(t, conn.QueryRowContext(ctx, "select payload from "+table+" where id=2").Scan(&defaultValue))
					require.Equal(t, "kept", defaultValue)
				})
			}
		}
	})
}
