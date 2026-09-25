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

package plan

import (
	"errors"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

func databaseDefaultsProtocol(t *testing.T, version int64) {
	t.Helper()
	rt := moruntime.ServiceRuntime("")
	old, had := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, version)
	t.Cleanup(func() {
		if had {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, old)
		} else {
			rt.CompareAndDeleteGlobalVariables(moruntime.MOProtocolVersion, version)
		}
	})
}

func TestNormalizeDatabaseDefaults(t *testing.T) {
	for _, tc := range []struct{ sql, fallback, want, fail string }{
		{"create database d", "utf8mb4_bin", "utf8mb4_bin", ""},
		{"create database d character set UTF8MB4", "utf8mb4_bin", "utf8mb4_general_ci", ""},
		{"create database d collate UTF8MB4_BIN", "utf8mb4_general_ci", "utf8mb4_bin", ""},
		{"create database d default collate=utf8mb4_bin default character set=utf8mb4", "", "utf8mb4_bin", ""},
		{"create database d collate utf8mb4_bin collate UTF8MB4_BIN", "", "utf8mb4_bin", ""},
		{"create database d character set utf8mb4 character set latin1", "", "", "conflicting"},
		{"create database d collate utf8mb4_bin collate utf8mb4_general_ci", "", "", "conflicting"},
		{"create database d character set latin1", "", "", "unsupported database character set"},
		{"create database d collate utf8mb4_0900_ai_ci", "", "", "unsupported database collation"},
		{"create database d encryption='Y'", "", "", "ENCRYPTION"},
		{"create database d", "binary", "", "unsupported database collation"},
	} {
		t.Run(tc.sql+tc.fallback, func(t *testing.T) {
			stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, tc.sql, 1)
			require.NoError(t, err)
			defer stmt.Free()
			got, err := NormalizeDatabaseDefaults(t.Context(), stmt.(*tree.CreateDatabase).CreateOptions, tc.fallback)
			if tc.fail != "" {
				require.ErrorContains(t, err, tc.fail)
				return
			}
			require.NoError(t, err)
			require.Equal(t, &planpb.DatabaseDefaults{CharacterSet: "utf8mb4", Collation: tc.want, Version: 1}, got)
		})
	}
}

func defaultsResult(t *testing.T, mp *mpool.MPool, charset, collation []string, versions []uint64) executor.Result {
	t.Helper()
	r := executor.NewMemResult([]types.Type{types.T_varchar.ToType(), types.T_varchar.ToType(), types.T_uint64.ToType()}, mp)
	r.NewBatchWithRowCount(len(versions))
	require.NoError(t, executor.AppendStringRows(r, 0, charset))
	require.NoError(t, executor.AppendStringRows(r, 1, collation))
	require.NoError(t, executor.AppendFixedRows(r, 2, versions))
	return r.GetResult()
}

func TestDecodeDatabaseDefaults(t *testing.T) {
	for _, tc := range []struct {
		name, charset, collation string
		version                  uint64
		rows                     int
		null                     bool
		fail                     bool
	}{
		{name: "legacy absence"},
		{name: "persisted", charset: "utf8mb4", collation: "utf8mb4_bin", version: 9, rows: 1},
		{name: "duplicate", charset: "utf8mb4", collation: "utf8mb4_bin", version: 1, rows: 2, fail: true},
		{name: "zero version", charset: "utf8mb4", collation: "utf8mb4_bin", rows: 1, fail: true},
		{name: "invalid charset", charset: "binary", collation: "utf8mb4_bin", version: 1, rows: 1, fail: true},
		{name: "invalid collation", charset: "utf8mb4", collation: "bogus", version: 1, rows: 1, fail: true},
		{name: "null", charset: "utf8mb4", collation: "utf8mb4_bin", version: 1, rows: 1, null: true, fail: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mp := mpool.MustNewZeroNoFixed()
			var r executor.Result
			if tc.rows > 0 {
				cs, co, vs := make([]string, tc.rows), make([]string, tc.rows), make([]uint64, tc.rows)
				for i := range cs {
					cs[i] = tc.charset
					co[i] = tc.collation
					vs[i] = tc.version
				}
				r = defaultsResult(t, mp, cs, co, vs)
				if tc.null {
					nulls.Add(r.Batches[0].Vecs[0].GetNulls(), 0)
				}
			}
			got, err := DecodeDatabaseDefaults(t.Context(), r, 42)
			r.Close()
			require.Zero(t, mp.CurrNB())
			if tc.fail {
				require.ErrorContains(t, err, "invalid database default metadata")
				return
			}
			require.NoError(t, err)
			require.Equal(t, uint64(42), got.DatabaseId)
			require.Equal(t, tc.version, got.Version)
		})
	}
}

func TestDatabaseDefaultsPublicDDLPlans(t *testing.T) {
	databaseDefaultsProtocol(t, defines.MORPCVersion96)
	for _, tc := range []struct{ sql, db, collation string }{
		{"alter database target character set utf8mb4 collate utf8mb4_bin", "target", "utf8mb4_bin"},
		{"alter schema collate utf8mb4_bin", "tpch", "utf8mb4_bin"},
		{"alter database target character set utf8mb4", "target", "utf8mb4_general_ci"},
		{"create database target collate utf8mb4_bin", "target", "utf8mb4_bin"},
		{"create database target", "target", "utf8mb4_bin"},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			ctx := NewMockCompilerContext(false)
			ctx.ResolveVariableFunc = func(string, bool, bool) (any, error) { return "utf8mb4_bin", nil }
			stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, tc.sql, 1)
			require.NoError(t, err)
			defer stmt.Free()
			p, err := BuildPlan(ctx, stmt, false)
			require.NoError(t, err)
			copy := DeepCopyPlan(p)
			var defaults *planpb.DatabaseDefaults
			if ddl := p.GetDdl().GetAlterDatabase(); ddl != nil {
				require.Equal(t, tc.db, ddl.Database)
				defaults = ddl.Defaults
				require.Equal(t, defaults, copy.GetDdl().GetAlterDatabase().Defaults)
			} else {
				ddl := p.GetDdl().GetCreateDatabase()
				require.Equal(t, tc.db, ddl.Database)
				defaults = ddl.Defaults
				require.Equal(t, defaults, copy.GetDdl().GetCreateDatabase().Defaults)
			}
			require.Equal(t, tc.collation, defaults.Collation)
		})
	}
	for _, sql := range []string{"alter database mo_catalog collate utf8mb4_bin", "alter database target collate unknown"} {
		stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, sql, 1)
		require.NoError(t, err)
		_, err = BuildPlan(NewMockCompilerContext(false), stmt, false)
		stmt.Free()
		require.Error(t, err)
	}
	t.Run("old cluster rejects", func(t *testing.T) {
		databaseDefaultsProtocol(t, defines.MORPCVersion94)
		for _, sql := range []string{"alter database target collate utf8mb4_bin", "create database target collate utf8mb4_bin"} {
			stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, sql, 1)
			require.NoError(t, err)
			_, err = BuildPlan(NewMockCompilerContext(false), stmt, false)
			stmt.Free()
			require.ErrorContains(t, err, "protocol version 96")
		}
	})
}

func TestCreateTableInheritsDatabaseDefaults(t *testing.T) {
	databaseDefaultsProtocol(t, defines.MORPCVersion96)
	for _, tc := range []struct {
		sql       string
		want      uint32
		read      bool
		legacy    bool
		readError bool
	}{
		{sql: "create table target.t(v varchar(8))", want: uint32(types.CharsetUTF8MB4Bin), read: true},
		{sql: "create table target.t(v varchar(8) collate utf8mb4_general_ci)", want: uint32(types.CharsetUTF8), read: true},
		{sql: "create table target.t(v varchar(8)) collate utf8mb4_general_ci", want: uint32(types.CharsetUTF8)},
		{sql: "create table target.t(v varchar(8)) character set utf8mb4", want: uint32(types.CharsetUTF8)},
		{sql: "create table target.legacy(v varchar(8))", want: uint32(types.CharsetUTF8), read: true, legacy: true},
		{sql: "create table target.failure(v varchar(8))", read: true, readError: true},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			ctx := NewMockCompilerContext(false)
			proc := ctx.GetProcess()
			reads := 0
			ctx.GetDatabaseIdFunc = func(name string, _ *Snapshot) (uint64, error) { require.Equal(t, "target", name); return 42, nil }
			ctx.ResolveVariableFunc = func(string, bool, bool) (any, error) { return "utf8mb4_general_ci", nil }
			injected := errors.New("catalog unavailable")
			ctx.processHolder.internalSQLExecutor = executor.NewMemExecutor(func(sql string) (executor.Result, error) {
				if strings.Contains(sql, "`mo_foreign_keys`") {
					return executor.Result{}, nil
				}
				reads++
				require.Equal(t, DatabaseDefaultsSelectSQL(0, 42), sql)
				if tc.readError {
					return executor.Result{}, injected
				}
				if tc.legacy {
					return executor.Result{}, nil
				}
				return defaultsResult(t, proc.Mp(), []string{"utf8mb4"}, []string{"utf8mb4_bin"}, []uint64{7}), nil
			})
			before := proc.Mp().CurrNB()
			stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, tc.sql, 1)
			require.NoError(t, err)
			defer stmt.Free()
			p, err := BuildPlan(ctx, stmt, false)
			if tc.readError {
				require.ErrorIs(t, err, injected)
			} else {
				require.NoError(t, err)
				create := p.GetDdl().GetCreateTable()
				require.Equal(t, tc.want, create.TableDef.Cols[0].Typ.Charset)
				if tc.read {
					require.Equal(t, uint64(42), create.DatabaseDefaults.DatabaseId)
					if tc.legacy {
						require.Zero(t, create.DatabaseDefaults.Version)
					} else {
						require.Equal(t, uint64(7), create.DatabaseDefaults.Version)
					}
				} else {
					require.Nil(t, create.DatabaseDefaults)
				}
			}
			if tc.read {
				require.Equal(t, 1, reads)
			} else {
				require.Zero(t, reads)
			}
			require.Equal(t, before, proc.Mp().CurrNB())
		})
	}
}

func TestDatabaseDefaultsPreserveReplayedTableCollation(t *testing.T) {
	databaseDefaultsProtocol(t, defines.MORPCVersion96)
	for _, replay := range []bool{false, true} {
		t.Run(map[bool]string{false: "show after alter database", true: "like into different database"}[replay], func(t *testing.T) {
			mock := NewMockOptimizer(false)
			def, err := buildTestCreateTableStmt(mock, "create table source(v varchar(8)) collate utf8mb4_general_ci")
			require.NoError(t, err)
			proc := mock.ctxt.GetProcess()
			mock.ctxt.processHolder.internalSQLExecutor = executor.NewMemExecutor(func(sql string) (executor.Result, error) {
				if strings.Contains(sql, "mo_foreign_keys") {
					return executor.Result{}, nil
				}
				require.Contains(t, sql, "mo_database_defaults")
				return defaultsResult(t, proc.Mp(), []string{"utf8mb4"}, []string{"utf8mb4_bin"}, []uint64{2}), nil
			})
			sql, stmt, err := ConstructCreateTableSQL(&mock.ctxt, def, nil, replay, nil)
			require.NoError(t, err)
			require.Contains(t, sql, "COLLATE=utf8mb4_general_ci")
			rebound, err := BuildPlan(&mock.ctxt, stmt, false)
			require.NoError(t, err)
			require.Equal(t, uint32(types.CharsetUTF8), rebound.GetDdl().GetCreateTable().TableDef.Cols[0].Typ.Charset)
		})
	}
}
