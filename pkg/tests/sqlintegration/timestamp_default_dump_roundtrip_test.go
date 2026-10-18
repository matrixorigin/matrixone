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
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/embed"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestTimestampDefaultDumpRoundtrip(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
		defer cancel()
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		const schema = "timestamp_default_dump_contract"
		defer cleanupSQLIntegration(t, cn, "drop database if exists "+schema)
		exec := func(q string) { t.Helper(); _, err := conn.ExecContext(ctx, q); require.NoError(t, err, q) }
		exec("create database " + schema)
		exec("use " + schema)
		for _, tc := range []struct {
			name, declaration, createZone, dumpZone string
			want, updated                           any
			legacy, raw                             bool
		}{
			{"implicit_6", "timestamp(6)", "+00:00", "+00:00", "2024-01-01 00:00:00.500000", "2024-01-01 00:00:01.500000", true, false},
			// The released precision-0 owner rounds half seconds upward.
			{"implicit_0", "timestamp", "+00:00", "+08:00", "2024-01-01 00:00:01.000000", "2024-01-01 00:00:02.000000", true, false},
			{"literal_same_zone", "timestamp(6) default '2024-01-01 00:00:00.123456'", "+00:00", "+00:00", "2024-01-01 00:00:00.123456", "", false, false},
			{"literal_changed_zone", "timestamp(6) default '2024-01-01 00:00:00.123456'", "+00:00", "+08:00", "2024-01-01 00:00:00.123456", "", false, false},
			{"datetime_expression", "datetime(6) default (from_unixtime(1704067200))", "+00:00", "+08:00", "2024-01-01 00:00:00.000000", "", false, false},
			{"varchar_expression", "varchar(64) default (from_unixtime(1704067200,'%Y-%m-%d %H:%i:%s'))", "+00:00", "+08:00", "2024-01-01 00:00:00", "", false, true},
			{"named_timezone", "timestamp default '1971-01-01 00:00:00'", "Africa/Monrovia", "+08:00", "1971-01-01 00:44:30.000000", "", false, false},
			{"dst_expression", "varchar(64) default (concat(from_unixtime(1704067200,'%H'),':',from_unixtime(1719792000,'%H')))", "America/New_York", "+08:00", "19:20", "", false, true},
			{"nullable_default", "int default (if(hour(from_unixtime(1704067200))=0,null,1))", "+00:00", "+08:00", nil, "", false, true},
			{"backup_overflow", "tinyint default (hour(from_unixtime(1704067200))+120)", "+00:00", "+08:00", "120", "", false, true},
		} {
			t.Run(tc.name, func(t *testing.T) {
				exec := func(q string) { t.Helper(); _, err := conn.ExecContext(ctx, q); require.NoError(t, err, q) }
				t.Cleanup(func() {
					cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
					defer cancel()
					_, err := conn.ExecContext(cleanupCtx, "drop table if exists "+tc.name+"_source,"+tc.name+"_target")
					require.NoError(t, err)
				})
				exec("set time_zone='" + tc.createZone + "'")
				policy := 1
				if tc.legacy {
					policy = 0
				}
				exec(fmt.Sprintf("set explicit_defaults_for_timestamp=%d", policy))
				exec("create table " + tc.name + "_source(id int primary key,a " + tc.declaration + ")")
				exec("dump table " + tc.name + "_source to '" + t.TempDir() + "' metadata only")
				exec("set time_zone='" + tc.dumpZone + "'")
				path := t.TempDir()
				exec("dump table " + tc.name + "_source to '" + path + "' metadata only")
				data, err := os.ReadFile(filepath.Join(path, "manifest.json"))
				require.NoError(t, err)
				var manifest struct {
					Version int    `json:"version"`
					Bound   []byte `json:"bound_expressions"`
				}
				require.NoError(t, json.Unmarshal(data, &manifest))
				require.Equal(t, 3, manifest.Version, "binding witnesses must survive a different LOAD time zone")
				require.NotEmpty(t, manifest.Bound)
				var source planpb.TableDef
				require.NoError(t, proto.Unmarshal(manifest.Bound, &source))
				exec("set time_zone='+05:45'")
				exec("create table " + tc.name + "_target(id int primary key,a " + tc.declaration + ")")
				exec("load table " + tc.name + "_target from '" + path + "'")
				// Read back the installed trees through a second real DUMP before
				// inserting in-memory rows, which require a flush before DUMP.
				restoredPath := t.TempDir()
				exec("dump table " + tc.name + "_target to '" + restoredPath + "' metadata only")
				data, err = os.ReadFile(filepath.Join(restoredPath, "manifest.json"))
				require.NoError(t, err)
				require.NoError(t, json.Unmarshal(data, &manifest))
				var restored planpb.TableDef
				require.NoError(t, proto.Unmarshal(manifest.Bound, &restored))
				require.Equal(t, len(source.Cols), len(restored.Cols))
				require.True(t, proto.Equal(source.Cols[1].Default, restored.Cols[1].Default))
				require.True(t, proto.Equal(source.Cols[1].OnUpdate, restored.Cols[1].OnUpdate))
				exec("set timestamp=1704067200.5")
				exec("insert into " + tc.name + "_target(id) values(1)")
				exec("set time_zone='+00:00'")
				assertValue := func(want any) {
					t.Helper()
					var value sql.NullString
					expression := "date_format(a,'%Y-%m-%d %H:%i:%s.%f')"
					if tc.raw {
						expression = "a"
					}
					require.NoError(t, conn.QueryRowContext(ctx, "select "+expression+" from "+tc.name+"_target").Scan(&value))
					if want == nil {
						require.False(t, value.Valid)
					} else {
						require.True(t, value.Valid)
						require.Equal(t, want, value.String)
					}
				}
				assertValue(tc.want)
				if tc.legacy {
					exec("set timestamp=1704067201.5")
					exec("update " + tc.name + "_target set id=2")
					assertValue(tc.updated)
				}
			})
		}
		var remaining int
		require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from mo_catalog.mo_tables where reldatabase=?", schema).Scan(&remaining))
		require.Zero(t, remaining)
	})
}
