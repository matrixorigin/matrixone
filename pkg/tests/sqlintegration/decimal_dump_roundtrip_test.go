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

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/embed"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

// Install genuine base-authored expressions through a valid metadata manifest,
// then exercise real SQL DUMP/LOAD, physical rows and future DML. This is not a
// claim of deploying or restarting a historical server binary.
func TestDecimalHistoricalDumpRoundtrip(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "sql", "plan", "testdata", "legacy_decimal_dump_63a964a82f.json"))
	require.NoError(t, err)
	var fixtures map[string]struct {
		SQL      string `json:"sql"`
		TableDef []byte `json:"table_def"`
	}
	require.NoError(t, json.Unmarshal(data, &fixtures))
	runSQLIntegration(t, func(c embed.Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		const schema = "decimal_dump_contract"
		for _, name := range []string{"check", "generated"} {
			t.Run(name, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(t.Context(), 180*time.Second)
				defer cancel()
				conn, err := db.Conn(ctx)
				require.NoError(t, err)
				defer conn.Close()
				exec := func(query string) { t.Helper(); _, err := conn.ExecContext(ctx, query); require.NoError(t, err, query) }
				count := func(query string, want int) {
					t.Helper()
					var got int
					require.NoError(t, conn.QueryRowContext(ctx, query).Scan(&got))
					require.Equal(t, want, got, query)
				}
				exec("set lc_time_names='en_US'")
				fixture, ok := fixtures[name]
				require.True(t, ok)
				var old pb.TableDef
				require.NoError(t, old.Unmarshal(fixture.TableDef))
				table := func(suffix string) string { return schema + "_" + name + "_" + suffix + ".legacy_dump" }
				for _, suffix := range []string{"seed", "source", "target", "reject"} {
					database := schema + "_" + name + "_" + suffix
					exec("create database " + database)
					defer cleanupSQLIntegration(t, cn, "drop database if exists "+database)
					exec(strings.Replace(fixture.SQL, "legacy_dump", table(suffix), 1))
				}
				root := t.TempDir()
				seedPath, dumpPath := filepath.Join(root, "seed"), filepath.Join(root, "dump")
				exec("dump table " + table("seed") + " to '" + seedPath + "' metadata only")
				readManifest := func(path string) map[string]json.RawMessage {
					t.Helper()
					data, err := os.ReadFile(filepath.Join(path, "manifest.json"))
					require.NoError(t, err)
					var manifest map[string]json.RawMessage
					require.NoError(t, json.Unmarshal(data, &manifest))
					return manifest
				}
				manifest := readManifest(seedPath)
				var payload []byte
				require.NoError(t, json.Unmarshal(manifest["bound_expressions"], &payload))
				var transport pb.TableDef
				require.NoError(t, transport.Unmarshal(payload))
				if name == "check" {
					require.Equal(t, old.Checks[0].OriginSql, transport.Checks[0].OriginSql)
					transport.Checks[0].Check = old.Checks[0].Check
				} else {
					require.Equal(t, old.Cols[2].GeneratedCol.OriginString, transport.Cols[2].GeneratedCol.OriginString)
					transport.Cols[2].GeneratedCol.Expr = old.Cols[2].GeneratedCol.Expr
				}
				writePayload := func(path string, def *pb.TableDef) {
					t.Helper()
					payload, err := def.Marshal()
					require.NoError(t, err)
					manifest["bound_expressions"], err = json.Marshal(payload)
					require.NoError(t, err)
					manifest["expression_hash"], err = json.Marshal(fmt.Sprintf("%x", sha256.Sum256(payload)))
					require.NoError(t, err)
					data, err := json.Marshal(manifest)
					require.NoError(t, err)
					require.NoError(t, os.WriteFile(filepath.Join(path, "manifest.json"), data, 0600))
				}
				writePayload(seedPath, &transport)
				exec("load table " + table("source") + " from '" + seedPath + "'")
				exec("insert into " + table("source") + "(i,tm) values(1000000000000000000,'00:00:00')")
				var flush string
				require.NoError(t, conn.QueryRowContext(ctx, "select mo_ctl('dn','flush','"+table("source")+"')").Scan(&flush))
				exec("dump table " + table("source") + " to '" + dumpPath + "'")
				exported := readManifest(dumpPath)
				require.NoError(t, json.Unmarshal(exported["bound_expressions"], &payload))
				var published pb.TableDef
				require.NoError(t, published.Unmarshal(payload))
				if name == "check" {
					require.True(t, proto.Equal(old.Checks[0].Check, published.Checks[0].Check))
				} else {
					require.True(t, proto.Equal(old.Cols[2].GeneratedCol.Expr, published.Cols[2].GeneratedCol.Expr))
				}
				exec("load table " + table("target") + " from '" + dumpPath + "'")
				exec("insert into " + table("target") + "(i,tm) values(1000000000000000001,'00:00:00')")
				exec("update " + table("target") + " set i=i+1")
				predicate := " where i>=1000000000000000001 and m='January'"
				if name == "generated" {
					predicate += " and g=true"
				}
				count("select count(*) from "+table("target")+predicate, 2)
				narrow := schema + "_" + name + "_target.narrow"
				exec("create table " + narrow + "(d decimal(18,0))")
				_, err = conn.ExecContext(ctx, "insert into "+narrow+" select i from "+table("target"))
				require.ErrorContains(t, err, "beyond the range")
				require.ErrorContains(t, err, "(18,0)")
				count("select count(*) from "+narrow, 0)
				if name == "check" {
					transport.Checks[0].Check = &pb.Expr{Typ: transport.Checks[0].Check.Typ, Expr: &pb.Expr_Lit{Lit: &pb.Literal{Value: &pb.Literal_Bval{Bval: true}}}}
				} else {
					transport.Cols[2].GeneratedCol.Expr = &pb.Expr{Typ: transport.Cols[2].Typ, Expr: &pb.Expr_Lit{Lit: &pb.Literal{Value: &pb.Literal_Bval{Bval: true}}}}
				}
				writePayload(seedPath, &transport)
				_, err = conn.ExecContext(ctx, "load table "+table("reject")+" from '"+seedPath+"'")
				require.ErrorContains(t, err, "cannot verify bound")
				count("select count(*) from "+table("reject"), 0)
				if name == "check" {
					_, err = conn.ExecContext(ctx, "insert into "+table("reject")+"(i,tm) values(-1,'00:00:00')")
					require.ErrorContains(t, err, "violated", "failed LOAD must retain the original CHECK")
					count("select count(*) from "+table("reject"), 0)
				} else {
					exec("insert into " + table("reject") + "(i,tm) values(-1,'00:00:00')")
					count("select count(*) from "+table("reject")+" where g=false", 1)
				}
			})
		}
		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		defer cancel()
		var remaining int
		require.NoError(t, db.QueryRowContext(ctx, "select count(*) from mo_catalog.mo_database where datname like 'decimal_dump_contract_%'").Scan(&remaining))
		require.Zero(t, remaining, "each scenario must complete schema teardown")
	})
}
