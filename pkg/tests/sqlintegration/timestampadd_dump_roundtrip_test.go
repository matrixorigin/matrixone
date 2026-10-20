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
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/gogo/protobuf/proto"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/embed"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

// The historical binding comes from exact published main, not this branch's
// compatibility binder. A valid public metadata manifest installs that frozen
// definition; subsequent real DUMP/LOAD and DML exercise the production paths.
// This is current execution around historical metadata, not a historical-server
// deployment or a replacement for independent old-binary rollout validation.
func TestTimestampAddHistoricalDumpRoundtrip(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		const schema = "timestampadd_dump_contract"
		defer cleanupSQLIntegration(t, cn, "drop database if exists "+schema)
		exec := func(q string) { t.Helper(); _, err := conn.ExecContext(ctx, q); require.NoError(t, err, q) }
		scalar := func(q, want string) {
			t.Helper()
			var got string
			require.NoError(t, conn.QueryRowContext(ctx, q).Scan(&got), q)
			require.Equal(t, want, got, q)
		}
		exec("create database " + schema)
		const historicalSchema = schema + "_historical"
		const restoredSchema = schema + "_restored"
		exec("create database " + historicalSchema)
		defer cleanupSQLIntegration(t, cn, "drop database if exists "+historicalSchema)
		exec("create database " + restoredSchema)
		defer cleanupSQLIntegration(t, cn, "drop database if exists "+restoredSchema)
		exec("use " + schema)
		const seedCurrent = schema + ".t"
		const historicalSource = historicalSchema + ".t"
		const restored = restoredSchema + ".t"
		exec("set time_zone='+00:00'")
		create := func(name string) {
			exec("create table " + name + "(a decimal(10,2),g datetime(6) generated always as(timestampadd(second,a,cast('2024-01-01' as datetime(6)))) stored,check(timestampadd(second,a,cast('2024-01-01' as datetime(6))) < cast('2024-01-01 00:01:00' as datetime(6))))")
		}
		for _, name := range []string{seedCurrent, historicalSource, restored, "fresh_control"} {
			create(name)
		}
		exec("insert into fresh_control(a) values(1.50)")
		scalar("select cast(g as char) from fresh_control", "2024-01-01 00:00:01.500000")
		root := t.TempDir()
		seedPath := filepath.Join(root, "seed")
		dataPath := filepath.Join(root, "data")
		exec("dump table " + seedCurrent + " to '" + seedPath + "' metadata only")
		readManifest := func(t *testing.T, path string) map[string]json.RawMessage {
			t.Helper()
			data, err := os.ReadFile(filepath.Join(path, "manifest.json"))
			require.NoError(t, err)
			var m map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(data, &m))
			var version int
			require.NoError(t, json.Unmarshal(m["version"], &version))
			require.Equal(t, 3, version, "old readers must reject the new binding-dependent format")
			return m
		}
		manifest := readManifest(t, seedPath)
		raw, err := os.ReadFile(filepath.Join("..", "..", "sql", "plan", "testdata", "timestampadd_direct_baseline.json"))
		require.NoError(t, err)
		var frozen struct {
			Source     string `json:"source"`
			Definition []byte `json:"definition"`
		}
		require.NoError(t, json.Unmarshal(raw, &frozen))
		require.Equal(t, "3fc00709eccc277c175c7a09eed2605ca1aaee29", frozen.Source)
		var def planpb.TableDef
		require.NoError(t, proto.Unmarshal(frozen.Definition, &def))
		// Native transport also has the engine-owned rowid column, absent from
		// the logical CREATE capture. Keep that skeleton and replace only the
		// independently frozen declarations, without rebinding any expression.
		var currentPayload []byte
		require.NoError(t, json.Unmarshal(manifest["bound_expressions"], &currentPayload))
		var transport planpb.TableDef
		require.NoError(t, proto.Unmarshal(currentPayload, &transport))
		copied := 0
		for _, source := range def.Cols {
			if source.GeneratedCol == nil {
				continue
			}
			for _, dest := range transport.Cols {
				if dest.Name == source.Name {
					require.Equal(t, source.GeneratedCol.OriginString, dest.GeneratedCol.OriginString)
					dest.GeneratedCol = source.GeneratedCol
					copied++
				}
			}
		}
		require.Len(t, transport.Checks, len(def.Checks))
		checkNames := make([]string, len(transport.Checks))
		for i, source := range def.Checks {
			dest := transport.Checks[i]
			checkNames[i] = dest.Name
			require.Equal(t, source.OriginSql, dest.OriginSql)
			require.False(t, proto.Equal(source.Check, dest.Check), "fresh and historical CHECKs must differ")
			dest.Check = source.Check
			copied++
		}
		require.Equal(t, 2, copied)
		payload, err := proto.Marshal(&transport)
		require.NoError(t, err)
		digest := fmt.Sprintf("%x", sha256.Sum256(payload))
		manifest["bound_expressions"], err = json.Marshal(payload)
		require.NoError(t, err)
		manifest["expression_hash"], err = json.Marshal(digest)
		require.NoError(t, err)
		seed, err := json.Marshal(manifest)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(seedPath, "manifest.json"), seed, 0600))
		exec("load table " + historicalSource + " from '" + seedPath + "'")
		exec("insert into " + historicalSource + "(a) values(1.50)")
		scalar("select cast(g as char) from "+historicalSource, "2024-01-01 00:00:02.000000")
		var flush string
		require.NoError(t, conn.QueryRowContext(ctx, "select mo_ctl('dn','flush','"+historicalSource+"')").Scan(&flush))
		t.Logf("synchronous flush: %s", flush)
		// A rolling deployment may still author at the previous floor. Failure
		// to bind the current era must preserve the historical binding witness.
		rt := moruntime.ServiceRuntime(cn.ServiceID())
		originalFloor, hadFloor := rt.GetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor)
		defer func() {
			if hadFloor {
				rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor, originalFloor)
			} else if floor, ok := rt.GetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor); ok {
				rt.CompareAndDeleteGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor, floor)
			}
		}()
		rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor, defines.MORPCVersion118)
		exec("dump table " + historicalSource + " to '" + dataPath + "'")
		exported := readManifest(t, dataPath)
		assertHistoricalChecks := func(manifest map[string]json.RawMessage) {
			t.Helper()
			var payload []byte
			require.NoError(t, json.Unmarshal(manifest["bound_expressions"], &payload))
			var published planpb.TableDef
			require.NoError(t, proto.Unmarshal(payload, &published))
			require.Equal(t, len(def.Checks), len(published.Checks))
			for i, check := range def.Checks {
				require.Equal(t, checkNames[i], published.Checks[i].Name, "trusted current CHECK identity must survive")
				require.Equal(t, check.OriginSql, published.Checks[i].OriginSql)
				require.True(t, proto.Equal(check.Check, published.Checks[i].Check), "catalog must retain the exact historical expression")
			}
		}
		assertHistoricalChecks(exported)
		var metadataOnly bool
		require.NoError(t, json.Unmarshal(exported["metadata_only"], &metadataOnly))
		require.False(t, metadataOnly)
		rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor, defines.MORPCLatestVersion)
		exec("load table " + restored + " from '" + dataPath + "'")
		restoredMetadata := filepath.Join(root, "restored-metadata")
		exec("dump table " + restored + " to '" + restoredMetadata + "' metadata only")
		assertHistoricalChecks(readManifest(t, restoredMetadata))
		scalar("select cast(g as char) from "+restored, "2024-01-01 00:00:02.000000")
		func() {
			rows, err := conn.QueryContext(ctx, "select g from "+restored)
			require.NoError(t, err)
			defer rows.Close()
			columns, err := rows.ColumnTypes()
			require.NoError(t, err)
			require.Len(t, columns, 1)
			require.Equal(t, "DATETIME", columns[0].DatabaseTypeName())
			require.True(t, rows.Next())
			var stored string
			require.NoError(t, rows.Scan(&stored))
			require.Equal(t, "2024-01-01 00:00:02.000000", stored)
			require.False(t, rows.Next())
			require.NoError(t, rows.Err())
		}()
		// A legitimate old v1 manifest has no binding witness. Rejection must
		// leave the target empty and its freshly authored future-row semantics.
		// Reuse the empty seed so its CHECK identity matches the manifest.
		manifest["version"] = json.RawMessage("1")
		delete(manifest, "bound_expressions")
		delete(manifest, "expression_hash")
		legacy, err := json.Marshal(manifest)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(seedPath, "manifest.json"), legacy, 0600))
		_, err = conn.ExecContext(ctx, "load table "+seedCurrent+" from '"+seedPath+"'")
		require.ErrorContains(t, err, "legacy table dump cannot verify persisted expression bindings")
		scalar("select count(*) from "+seedCurrent, "0")
		exec("insert into " + seedCurrent + "(a) values(1.50)")
		scalar("select cast(g as char) from "+seedCurrent, "2024-01-01 00:00:01.500000")

		// 59.50 is valid under fractional semantics (<60s), but rounds to 60s
		// under the preserved CHECK. Observe enforcement, not only metadata.
		_, err = conn.ExecContext(ctx, "insert into "+restored+"(a) values(59.50)")
		var checkErr *mysql.MySQLError
		require.ErrorAs(t, err, &checkErr)
		require.Equal(t, uint16(3819), checkErr.Number)
		scalar("select count(*) from "+restored+" where a=59.50", "0")
		exec("insert into fresh_control(a) values(59.50)")
		scalar("select cast(g as char) from fresh_control where a=59.50", "2024-01-01 00:00:59.500000")
		exec("insert into " + restored + "(a) values(2.50)")
		scalar("select cast(g as char) from "+restored+" where a=2.50", "2024-01-01 00:00:03.000000")
		exec("update " + restored + " set a=3.50 where a=1.50")
		scalar("select cast(g as char) from "+restored+" where a=3.50", "2024-01-01 00:00:04.000000")
		exec("create index idx_g on " + restored + "(g)")
		scalar("select count(*) from "+restored+" force index(idx_g) where g=cast('2024-01-01 00:00:04' as datetime(6))", "1")
		exec("update " + restored + " set a=4.50 where a=3.50")
		scalar("select count(*) from "+restored+" force index(idx_g) where g=cast('2024-01-01 00:00:04' as datetime(6))", "0")
		scalar("select count(*) from "+restored+" force index(idx_g) where g=cast('2024-01-01 00:00:05' as datetime(6))", "1")
		rows, err := conn.QueryContext(ctx, "explain select a from "+restored+" force index(idx_g) where g=cast('2024-01-01 00:00:05' as datetime(6))")
		require.NoError(t, err)
		defer rows.Close()
		var text strings.Builder
		for rows.Next() {
			var line string
			require.NoError(t, rows.Scan(&line))
			text.WriteString(line)
		}
		require.NoError(t, rows.Err())
		require.Contains(t, text.String(), "__mo_index_", "the result oracle must exercise an actual physical index")
		require.NoError(t, rows.Close())
		// Reuse this cluster to prove that historical DIV bindings survive the
		// same public transport owner after main changes their result domains.
		rawDiv, err := os.ReadFile(filepath.Join("..", "..", "sql", "plan", "testdata", "decimal_div_pre121_baseline.json"))
		require.NoError(t, err)
		var divFixtures map[string][]byte
		require.NoError(t, json.Unmarshal(rawDiv, &divFixtures))
		for _, tc := range []struct{ name, divisor string }{{"signed", "decimal(10,2)"}, {"unsigned", "bigint unsigned"}} {
			t.Run("historical_div_"+tc.name, func(t *testing.T) {

				exec := func(q string) { t.Helper(); _, err := conn.ExecContext(ctx, q); require.NoError(t, err, q) }
				scalar := func(q, want string) {
					t.Helper()
					var got string
					require.NoError(t, conn.QueryRowContext(ctx, q).Scan(&got), q)
					require.Equal(t, want, got, q)
				}
				seedName, sourceName, restoredName := schema+".div_"+tc.name, historicalSchema+".div_"+tc.name, restoredSchema+".div_"+tc.name
				for _, name := range []string{seedName, sourceName, restoredName} {
					exec("create table " + name + "(a decimal(10,2),b " + tc.divisor + ",g bigint generated always as(a div b) stored,check(a div b > 0))")
				}
				seedDir := filepath.Join(root, "div-seed-"+tc.name)
				exec("dump table " + seedName + " to '" + seedDir + "' metadata only")
				m := readManifest(t, seedDir)
				var payload []byte
				require.NoError(t, json.Unmarshal(m["bound_expressions"], &payload))
				var transport, frozen planpb.TableDef
				require.NoError(t, proto.Unmarshal(payload, &transport))
				require.NoError(t, proto.Unmarshal(divFixtures[tc.name], &frozen))
				copied := 0
				for _, old := range frozen.Cols {
					if old.GeneratedCol == nil {
						continue
					}
					for _, current := range transport.Cols {
						if current.Name == old.Name {
							require.Equal(t, old.GeneratedCol.OriginString, current.GeneratedCol.OriginString)
							current.GeneratedCol = old.GeneratedCol
							copied++
						}
					}
				}
				require.Len(t, transport.Checks, 1)
				require.Len(t, frozen.Checks, 1)
				require.Equal(t, frozen.Checks[0].OriginSql, transport.Checks[0].OriginSql)
				trustedCheckName := transport.Checks[0].Name
				transport.Checks[0].Check = frozen.Checks[0].Check
				require.Equal(t, 1, copied)
				payload, err = proto.Marshal(&transport)
				require.NoError(t, err)
				m["bound_expressions"], err = json.Marshal(payload)
				require.NoError(t, err)
				m["expression_hash"], err = json.Marshal(fmt.Sprintf("%x", sha256.Sum256(payload)))
				require.NoError(t, err)
				encoded, err := json.Marshal(m)
				require.NoError(t, err)
				require.NoError(t, os.WriteFile(filepath.Join(seedDir, "manifest.json"), encoded, 0600))
				exec("load table " + sourceName + " from '" + seedDir + "'")
				exec("insert into " + sourceName + "(a,b) values(5.50,2)")
				var flush string
				require.NoError(t, conn.QueryRowContext(ctx, "select mo_ctl('dn','flush','"+sourceName+"')").Scan(&flush))
				dataDir := filepath.Join(root, "div-data-"+tc.name)
				exec("dump table " + sourceName + " to '" + dataDir + "'")
				exec("load table " + restoredName + " from '" + dataDir + "'")
				metadataDir := filepath.Join(root, "div-restored-"+tc.name)
				exec("dump table " + restoredName + " to '" + metadataDir + "' metadata only")
				for _, dir := range []string{dataDir, metadataDir} {
					m := readManifest(t, dir)
					var payload []byte
					require.NoError(t, json.Unmarshal(m["bound_expressions"], &payload))
					var published planpb.TableDef
					require.NoError(t, proto.Unmarshal(payload, &published))
					require.Equal(t, trustedCheckName, published.Checks[0].Name)
					require.True(t, proto.Equal(frozen.Cols[2].GeneratedCol.Expr, published.Cols[2].GeneratedCol.Expr))
					require.True(t, proto.Equal(frozen.Checks[0].Check, published.Checks[0].Check))
				}
				scalar("select g from "+restoredName, "2")
				exec("insert into " + restoredName + "(a,b) values(7.50,2)")
				scalar("select g from "+restoredName+" where a=7.50", "3")
				exec("update " + restoredName + " set a=9.50 where a=5.50")
				scalar("select g from "+restoredName+" where a=9.50", "4")
				_, err = conn.ExecContext(ctx, "insert into "+restoredName+"(a,b) values(0,2)")
				var checkErr *mysql.MySQLError
				require.ErrorAs(t, err, &checkErr)
				require.Equal(t, uint16(3819), checkErr.Number)
				scalar("select count(*) from "+restoredName, "2")
			})
		}

		// Reuse this cluster and the same three schemas for pre-8dd nullable
		// metadata. Frozen expressions are independent of the compatibility
		// branch; destination CHECK identities remain current and unchanged.
		temporalRaw, err := os.ReadFile(filepath.Join("..", "..", "sql", "plan", "testdata", "temporal_nullability_pre8dd_baseline.json"))
		require.NoError(t, err)
		var temporalFixtures map[string][]byte
		require.NoError(t, json.Unmarshal(temporalRaw, &temporalFixtures))
		for _, tc := range []struct{ name, typ, expr, want string }{
			{"date_constant", "date", "date('2024-01-01')", "2024-01-01"},
			{"implicit_constant", "varchar(64)", "date_format('2024-01-01','%Y-%m-%d')", "2024-01-01"},
			{"fractional_constant", "datetime", "timestampadd(second,1.5,cast('2024-01-01' as datetime))", "2024-01-01 00:00:02"},
		} {
			t.Run("historical_temporal_"+tc.name, func(t *testing.T) {
				// These additions own their own allowance; they cannot consume
				// the earlier historical TIMESTAMPADD/DIV scenario's deadline.
				ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
				defer cancel()
				conn, err := db.Conn(ctx)
				require.NoError(t, err)
				defer conn.Close()
				exec := func(q string) { t.Helper(); _, err := conn.ExecContext(ctx, q); require.NoError(t, err, q) }
				exec("use " + schema)
				exec("set time_zone='+00:00',sql_mode=DEFAULT,explicit_defaults_for_timestamp=1")
				logical := "temporal_" + tc.name
				seedName, sourceName, restoredName := schema+"."+logical, historicalSchema+"."+logical, restoredSchema+"."+logical
				for _, name := range []string{seedName, sourceName, restoredName} {
					exec("create table " + name + "(a varchar(32) not null,g " + tc.typ + " generated always as (" + tc.expr + ") stored,check(" + tc.expr + " is not null))")
				}
				seedDir := t.TempDir()
				exec("dump table " + seedName + " to '" + seedDir + "' metadata only")
				m := readManifest(t, seedDir)
				var payload []byte
				require.NoError(t, json.Unmarshal(m["bound_expressions"], &payload))
				var transport, old planpb.TableDef
				require.NoError(t, proto.Unmarshal(payload, &transport))
				require.NoError(t, proto.Unmarshal(temporalFixtures[tc.name], &old))
				require.Len(t, transport.Checks, 1)
				require.Len(t, old.Checks, 1)
				trustedName := transport.Checks[0].Name
				require.Equal(t, old.Cols[1].GeneratedCol.OriginString, transport.Cols[1].GeneratedCol.OriginString)
				require.Equal(t, old.Checks[0].OriginSql, transport.Checks[0].OriginSql)
				require.False(t, proto.Equal(old.Cols[1].GeneratedCol.Expr, transport.Cols[1].GeneratedCol.Expr))
				transport.Cols[1].GeneratedCol.Expr = old.Cols[1].GeneratedCol.Expr
				transport.Checks[0].Check = old.Checks[0].Check
				payload, err = proto.Marshal(&transport)
				require.NoError(t, err)
				m["bound_expressions"], err = json.Marshal(payload)
				require.NoError(t, err)
				m["expression_hash"], err = json.Marshal(fmt.Sprintf("%x", sha256.Sum256(payload)))
				require.NoError(t, err)
				data, err := json.Marshal(m)
				require.NoError(t, err)
				require.NoError(t, os.WriteFile(filepath.Join(seedDir, "manifest.json"), data, 0600))
				exec("load table " + sourceName + " from '" + seedDir + "'")
				dir := t.TempDir()
				exec("dump table " + sourceName + " to '" + dir + "' metadata only")
				exec("load table " + restoredName + " from '" + dir + "'")
				dir = t.TempDir()
				exec("dump table " + restoredName + " to '" + dir + "' metadata only")
				m = readManifest(t, dir)
				require.NoError(t, json.Unmarshal(m["bound_expressions"], &payload))
				var published planpb.TableDef
				require.NoError(t, proto.Unmarshal(payload, &published))
				require.Equal(t, trustedName, published.Checks[0].Name)
				require.True(t, proto.Equal(old.Cols[1].GeneratedCol.Expr, published.Cols[1].GeneratedCol.Expr))
				require.True(t, proto.Equal(old.Checks[0].Check, published.Checks[0].Check))
				exec("insert into " + restoredName + "(a) values('2024-03-04')")
				var value string
				require.NoError(t, conn.QueryRowContext(ctx, "select cast(g as char) from "+restoredName).Scan(&value))
				require.Equal(t, tc.want, value)
			})
		}

	})
}
