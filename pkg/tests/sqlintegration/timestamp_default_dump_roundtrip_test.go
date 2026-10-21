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
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"
	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/embed"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestTimestampDefaultDumpRoundtrip(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		const schema = "timestamp_default_dump_contract"
		defer cleanupSQLIntegration(t, cn, "drop database if exists "+schema)
		func() {
			ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
			defer cancel()
			_, err := db.ExecContext(ctx, "create database "+schema)
			require.NoError(t, err)
		}()
		run := func(name string, scenario func(*testing.T, context.Context, *sql.Conn)) {
			t.Run(name, func(t *testing.T) {
				withTimestampDefaultScenarioSession(t, db, func(ctx context.Context, conn *sql.Conn) {
					for _, query := range []string{
						"use " + schema,
						"set sql_mode=DEFAULT,time_zone='+00:00',lc_time_names='en_US',timestamp=0,div_precision_increment=4,explicit_defaults_for_timestamp=1",
					} {
						_, err := conn.ExecContext(ctx, query)
						require.NoError(t, err, query)
					}
					scenario(t, ctx, conn)
				})
			})
		}
		for _, tc := range []struct {
			name, declaration, createZone, dumpZone string
			want, updated                           any
			legacy, raw                             bool
			locale                                  string
		}{
			{"implicit_6", "timestamp(6)", "+00:00", "+00:00", "2024-01-01 00:00:00.500000", "2024-01-01 00:00:01.500000", true, false, ""},
			// The released precision-0 owner rounds half seconds upward.
			{"implicit_0", "timestamp", "+00:00", "+08:00", "2024-01-01 00:00:01.000000", "2024-01-01 00:00:02.000000", true, false, ""},
			{"literal_same_zone", "timestamp(6) default '2024-01-01 00:00:00.123456'", "+00:00", "+00:00", "2024-01-01 00:00:00.123456", "", false, false, ""},
			{"literal_changed_zone", "timestamp(6) default '2024-01-01 00:00:00.123456'", "+00:00", "+08:00", "2024-01-01 00:00:00.123456", "", false, false, ""},
			{"datetime_expression", "datetime(6) default (from_unixtime(1704067200))", "+00:00", "+08:00", "2024-01-01 00:00:00.000000", "", false, false, ""},
			{"varchar_expression", "varchar(64) default (from_unixtime(1704067200,'%Y-%m-%d %H:%i:%s'))", "+00:00", "+08:00", "2024-01-01 00:00:00", "", false, true, ""},
			{"named_timezone", "timestamp default '1971-01-01 00:00:00'", "Africa/Monrovia", "+08:00", "1971-01-01 00:44:30.000000", "", false, false, ""},
			{"dst_expression", "varchar(64) default (concat(from_unixtime(1704067200,'%H'),':',from_unixtime(1719792000,'%H')))", "America/New_York", "+08:00", "19:20", "", false, true, ""},
			{"nullable_default", "int default (if(hour(from_unixtime(1704067200))=0,null,1))", "+00:00", "+08:00", nil, "", false, true, ""},
			{"backup_overflow", "tinyint default (hour(from_unixtime(1704067200))+120)", "+00:00", "+08:00", "120", "", false, true, ""},
			{"locale_monthname", "varchar(64) default (monthname('2024-01-01'))", "+00:00", "+08:00", "January", "", false, true, "en_US"},
			{"locale_date_format", "varchar(64) default (date_format('2024-01-01','%M'))", "+00:00", "+08:00", "janvier", "", false, true, "fr_FR"},
			{"locale_dayname", "varchar(64) default (dayname('2024-01-01'))", "+00:00", "+08:00", "月曜日", "", false, true, "ja_JP"},
			{"locale_mixed", "varchar(64) default (date_format(from_unixtime(1704067200),'%M %H'))", "America/New_York", "+08:00", "décembre 19", "", false, true, "fr_FR"},
		} {
			run(tc.name, func(t *testing.T, ctx context.Context, conn *sql.Conn) {
				exec := func(q string) { t.Helper(); _, err := conn.ExecContext(ctx, q); require.NoError(t, err, q) }
				t.Cleanup(func() {
					cleanupSQLIntegration(t, cn, "drop table if exists "+schema+"."+tc.name+"_source,"+schema+"."+tc.name+"_target")
				})
				creationLocale := tc.locale
				if creationLocale == "" {
					creationLocale = "en_US"
				}
				exec("set lc_time_names='" + creationLocale + "'")
				exec("set time_zone='" + tc.createZone + "'")
				policy := 1
				if tc.legacy {
					policy = 0
				}
				exec(fmt.Sprintf("set explicit_defaults_for_timestamp=%d", policy))
				exec("create table " + tc.name + "_source(id int primary key,a " + tc.declaration + ")")
				exec("dump table " + tc.name + "_source to '" + t.TempDir() + "' metadata only")
				if tc.locale != "" {
					exec("set lc_time_names='de_DE'")
				}
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
				if tc.locale != "" {
					exec("set lc_time_names='ja_JP'")
				}
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

		// The matching precision must be tried before unrelated session tuples.
		const precisionExpression = "concat(cast(cast(1 as decimal(10,0))/cast(3 as decimal(10,0)) as char), ':', date_format(from_unixtime(1704067200),'%M'))"
		for _, tc := range []struct {
			name               string
			precision, columns int
			want               string
		}{
			{"precision10", 10, 1, "0.3333333333:January"},
			{"three_precision4", 4, 3, "0.3333:January"},
		} {
			run(tc.name, func(t *testing.T, ctx context.Context, conn *sql.Conn) {
				exec := func(q string) { t.Helper(); _, err := conn.ExecContext(ctx, q); require.NoError(t, err, q) }
				t.Cleanup(func() {
					cleanupSQLIntegration(t, cn, "drop table if exists "+schema+"."+tc.name+"_source,"+schema+"."+tc.name+"_target")
				})
				exec("set time_zone='+00:00'")
				exec("set lc_time_names='en_US'")
				exec(fmt.Sprintf("set div_precision_increment=%d", tc.precision))
				cols := []string{"id int primary key"}
				names := []string{}
				for i := 0; i < tc.columns; i++ {
					name := fmt.Sprintf("a%d", i)
					names = append(names, name)
					cols = append(cols, name+" varchar(200) default ("+precisionExpression+")")
				}
				ddl := "(" + strings.Join(cols, ",") + ")"
				exec("create table " + tc.name + "_source" + ddl)
				exec("insert into " + tc.name + "_source(id) values(1)")
				var flush string
				require.NoError(t, conn.QueryRowContext(ctx, "select mo_ctl('dn','flush','"+schema+"."+tc.name+"_source')").Scan(&flush))
				path := t.TempDir()
				exec("dump table " + tc.name + "_source to '" + path + "' metadata only")
				exec("set div_precision_increment=0")
				exec("set lc_time_names='de_DE'")
				exec("set time_zone='+08:00'")
				exec("create table " + tc.name + "_target" + ddl)
				exec("load table " + tc.name + "_target from '" + path + "'")
				exec("dump table " + tc.name + "_target to '" + t.TempDir() + "' metadata only")
				exec("insert into " + tc.name + "_target(id) values(2)")
				for _, name := range names {
					var got string
					require.NoError(t, conn.QueryRowContext(ctx, "select "+name+" from "+tc.name+"_target where id=2").Scan(&got))
					require.Equal(t, tc.want, got)
				}
			})
		}

		run("zero_temporal_cross_mode", func(t *testing.T, ctx context.Context, conn *sql.Conn) {
			exec := func(q string) { t.Helper(); _, err := conn.ExecContext(ctx, q); require.NoError(t, err, q) }
			t.Cleanup(func() {
				cleanupSQLIntegration(t, cn, "drop table if exists "+schema+".zero_mode_source,"+schema+".zero_mode_target,"+schema+".zero_mode_rejected")
			})
			exec("set sql_mode=''")
			const zeroDDL = "(a timestamp default '0000-00-00 00:00:00',b date default '0000-00-00',c datetime default '0000-00-00 00:00:00',d timestamp default 0,e date default 0,f datetime default 0)"
			exec("create table zero_mode_source" + zeroDDL)
			exec("create table zero_mode_target" + zeroDDL)
			seed := t.TempDir()
			exec("dump table zero_mode_source to '" + seed + "' metadata only")
			exec("set sql_mode=DEFAULT")
			var mode string
			require.NoError(t, conn.QueryRowContext(ctx, "select @@sql_mode").Scan(&mode))
			require.Contains(t, mode, "NO_ZERO_DATE")
			for _, declaration := range []string{"timestamp default '0000-00-00 00:00:00'", "date default '0000-00-00'", "datetime default '0000-00-00 00:00:00'", "timestamp default 0", "date default 0", "datetime default 0"} {
				_, err := conn.ExecContext(ctx, "create table zero_mode_rejected(a "+declaration+")")
				var invalidDefault *mysql.MySQLError
				require.ErrorAs(t, err, &invalidDefault)
				require.Equal(t, uint16(1067), invalidDefault.Number)
			}
			exec("dump table zero_mode_source to '" + t.TempDir() + "' metadata only")
			exec("load table zero_mode_target from '" + seed + "'")
			exec("dump table zero_mode_target to '" + t.TempDir() + "' metadata only")
			var after string
			require.NoError(t, conn.QueryRowContext(ctx, "select @@sql_mode").Scan(&after))
			require.Equal(t, mode, after)
			// Ordinary writes retain their own session policy. Use the original
			// admitting policy to observe the restored default's actual value.
			exec("set sql_mode=''")
			exec("insert into zero_mode_target values(default,default,default,default,default,default)")
			var value string
			require.NoError(t, conn.QueryRowContext(ctx, "select concat_ws(',',cast(a as char),cast(b as char),cast(c as char),cast(d as char),cast(e as char),cast(f as char)) from zero_mode_target").Scan(&value))
			require.Equal(t, "0000-00-00 00:00:00,0000-00-00,0000-00-00 00:00:00,0000-00-00 00:00:00,0000-00-00,0000-00-00 00:00:00", value)
		})
		run("pre_zero_numeric_timestamp", func(t *testing.T, ctx context.Context, conn *sql.Conn) {
			exec := func(q string) { t.Helper(); _, err := conn.ExecContext(ctx, q); require.NoError(t, err, q) }
			t.Cleanup(func() {
				cleanupSQLIntegration(t, cn, "drop table if exists "+schema+".pre_zero_source,"+schema+".pre_zero_target")
			})
			exec("set sql_mode=''")
			exec("set time_zone='+00:00'")
			exec("create table pre_zero_source(a timestamp default 0)")
			exec("create table pre_zero_target(a timestamp default 0)")
			seedPath := t.TempDir()
			exec("dump table pre_zero_source to '" + seedPath + "' metadata only")
			data, err := os.ReadFile(filepath.Join(seedPath, "manifest.json"))
			require.NoError(t, err)
			var manifest map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(data, &manifest))
			var payload []byte
			require.NoError(t, json.Unmarshal(manifest["bound_expressions"], &payload))
			var transport planpb.TableDef
			require.NoError(t, proto.Unmarshal(payload, &transport))
			raw, err := os.ReadFile(filepath.Join("..", "..", "sql", "plan", "testdata", "temporal_zero_pre_b233_baseline.json"))
			require.NoError(t, err)
			var frozen map[string][]byte
			require.NoError(t, json.Unmarshal(raw, &frozen))
			var old planpb.TableDef
			require.NoError(t, proto.Unmarshal(frozen["0"], &old))
			require.False(t, proto.Equal(transport.Cols[0].Default.Expr, old.Cols[0].Default.Expr))
			require.Equal(t, transport.Cols[0].Default.OriginString, old.Cols[0].Default.OriginString)
			transport.Cols[0].Default = old.Cols[0].Default
			payload, err = proto.Marshal(&transport)
			require.NoError(t, err)
			manifest["bound_expressions"], err = json.Marshal(payload)
			require.NoError(t, err)
			manifest["expression_hash"], err = json.Marshal(fmt.Sprintf("%x", sha256.Sum256(payload)))
			require.NoError(t, err)
			data, err = json.Marshal(manifest)
			require.NoError(t, err)
			require.NoError(t, os.WriteFile(filepath.Join(seedPath, "manifest.json"), data, 0600))
			exec("set sql_mode=DEFAULT")
			exec("load table pre_zero_source from '" + seedPath + "'")
			path := t.TempDir()
			exec("dump table pre_zero_source to '" + path + "' metadata only")
			exec("load table pre_zero_target from '" + path + "'")
			path = t.TempDir()
			exec("dump table pre_zero_target to '" + path + "' metadata only")
			data, err = os.ReadFile(filepath.Join(path, "manifest.json"))
			require.NoError(t, err)
			require.NoError(t, json.Unmarshal(data, &manifest))
			require.NoError(t, json.Unmarshal(manifest["bound_expressions"], &payload))
			var restored planpb.TableDef
			require.NoError(t, proto.Unmarshal(payload, &restored))
			require.True(t, proto.Equal(old.Cols[0].Default, restored.Cols[0].Default))
			// The historical epoch-zero value is below TIMESTAMP's legal lower
			// bound. The original binder rejects later default assignments in
			// both modes; DUMP/LOAD must preserve that binding and writer policy.
			for _, mode := range []string{"DEFAULT", "''"} {
				exec("set sql_mode=" + mode)
				_, err = conn.ExecContext(ctx, "insert into pre_zero_target values(default)")
				var invalidTimestamp *mysql.MySQLError
				require.ErrorAs(t, err, &invalidTimestamp)
				require.Equal(t, uint16(1366), invalidTimestamp.Number)
			}
			var count int
			require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from pre_zero_target").Scan(&count))
			require.Zero(t, count)
		})

		ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
		defer cancel()
		var remaining int
		require.NoError(t, db.QueryRowContext(ctx, "select count(*) from mo_catalog.mo_tables where reldatabase=?", schema).Scan(&remaining))
		require.Zero(t, remaining)
	})
}

// Each public scenario owns its deadline and connection. A slow or cancelled
// scenario cannot consume the next scenario's allowance or driver connection.
func withTimestampDefaultScenarioSession(t *testing.T, db *sql.DB, scenario func(context.Context, *sql.Conn)) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()
	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	defer conn.Close()
	scenario(ctx, conn)
}

func TestTimestampDefaultScenarioSessionDeadlines(t *testing.T) {
	for _, firstDelay := range []time.Duration{60 * time.Second, 91 * time.Second} {
		t.Run(firstDelay.String(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				db, mock, err := sqlmock.New()
				require.NoError(t, err)
				defer db.Close()
				mock.ExpectExec("first").WillDelayFor(firstDelay).WillReturnResult(sqlmock.NewResult(0, 1))
				mock.ExpectExec("second").WillDelayFor(60 * time.Second).WillReturnResult(sqlmock.NewResult(0, 1))
				withTimestampDefaultScenarioSession(t, db, func(ctx context.Context, conn *sql.Conn) {
					_, err := conn.ExecContext(ctx, "first")
					if firstDelay > 90*time.Second {
						require.Error(t, err)
						require.ErrorIs(t, ctx.Err(), context.DeadlineExceeded)
					} else {
						require.NoError(t, err)
					}
				})
				withTimestampDefaultScenarioSession(t, db, func(ctx context.Context, conn *sql.Conn) {
					_, err := conn.ExecContext(ctx, "second")
					require.NoError(t, err)
					require.NoError(t, ctx.Err())
				})
				require.NoError(t, mock.ExpectationsWereMet())
			})
		})
	}
}
