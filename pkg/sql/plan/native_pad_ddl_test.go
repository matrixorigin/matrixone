// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"context"
	"strings"
	"testing"

	"github.com/gogo/protobuf/proto"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

func nativePADDDLContext(t *testing.T) (*MockCompilerContext, func(string) (*Plan, error)) {
	t.Helper()
	ctx := NewMockCompilerContext(false, newPlanTestProcess(t))
	ctx.GetAccountIdFunc = func() (uint32, error) { return 42, nil }
	ctx.SetContext(context.WithValue(t.Context(), collationBindingRevisionKey{}, uint32(1)))
	rt := moruntime.ServiceRuntime(ctx.GetProcess().GetService())
	for _, key := range []string{moruntime.MOProtocolVersion, moruntime.PersistedExpressionProtocolFloor, moruntime.PersistedExpressionProtocolAuthoringFloor} {
		old, exists := rt.GetGlobalVariables(key)
		t.Cleanup(func() {
			if exists {
				rt.SetGlobalVariables(key, old)
			} else if value, ok := rt.GetGlobalVariables(key); ok {
				rt.CompareAndDeleteGlobalVariables(key, value)
			}
		})
		rt.SetGlobalVariables(key, defines.MORPCVersion124)
	}
	return ctx, func(sql string) (*Plan, error) {
		stmt, err := parsers.ParseOne(ctx.GetContext(), dialect.MYSQL, sql, 1)
		if err != nil {
			return nil, err
		}
		defer stmt.Free()
		return BuildPlan(&rootSQLCompilerContext{MockCompilerContext: ctx, rootSQL: sql}, stmt, false)
	}
}

func TestNativePADColumnAndTableAuthoring(t *testing.T) {
	ctx, build := nativePADDDLContext(t)
	for _, tc := range []struct {
		sql                                              string
		columnID, columnRevision, tableID, tableRevision uint32
	}{
		{"create table n (s varchar(8))", 3, 0, 3, 0},
		{"create table n (s varchar(8) collate utf8mb4_0900_ai_ci)", 3, 0, 3, 0},
		{"create table n (s varchar(8)) collate utf32_bin", 2, 0, 2, 0},
		{"create table n (s varchar(8)) collate utf8mb4_0900_ai_ci collate utf8mb4_bin", 2, 1, 2, 1},
		{"create table n (s varchar(8)) collate utf8mb4_bin collate utf8mb4_0900_ai_ci", 3, 0, 3, 0},
		{"create table n (s varchar(8)) character set utf8mb4", 3, 0, 3, 0},
		{"create table n (s varchar(8) collate utf8mb4_general_ci)", 3, 1, 3, 0},
		{"create table n (s varchar(8)) collate utf8mb4_general_ci", 3, 1, 3, 1},
		{"create table n (s varchar(8)) collate utf8mb3_bin", 2, 1, 2, 1},
		{"create table n (s varchar(8) character set utf8mb4) collate utf8mb4_bin", 3, 0, 2, 1},
		{"create table n (s varchar(8) collate utf8mb4_general_ci) collate utf8mb4_bin", 3, 1, 2, 1},
		{"create table n (s varchar(8) character set binary) collate utf8mb4_bin", 1, 0, 2, 1},
		{"create table n (s varchar(8) collate utf8mb4_unicode_ci)", 10, 1, 3, 0},
		{"create table n (id int primary key, s varchar(8) collate utf8mb4_bin, key k(id))", 2, 1, 3, 0},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			p, err := build(tc.sql)
			require.NoError(t, err)
			table := p.GetDdl().GetCreateTable().TableDef
			s := FindColumn(table.Cols, "s")
			require.NotNil(t, s)
			require.Equal(t, tc.columnID, s.Typ.Charset)
			require.Equal(t, tc.columnRevision, s.Typ.CollationVersion)
			require.Equal(t, tc.tableID, table.DefaultCharset)
			require.Equal(t, tc.tableRevision, table.CollationVersion)
		})
	}
	rt := moruntime.ServiceRuntime(ctx.GetProcess().GetService())
	rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor, defines.MORPCVersion120)
	p, err := build("create table n (s varchar(8) collate utf8mb4_bin)")
	require.ErrorContains(t, err, "protocol version 124")
	require.Nil(t, p)
}

func TestNativePADCTASRetainsPreFoldProtocolAndBinding(t *testing.T) {
	ctx, build := nativePADDDLContext(t)
	for _, sql := range []string{"create table n as select 'A' collate utf8mb4_general_ci = 'a' as b", "create table n as select 'a ' collate utf8mb4_bin as s"} {
		t.Run(sql, func(t *testing.T) {
			p, err := build(sql)
			require.NoError(t, err)
			create := p.GetDdl().GetCreateTable()
			require.Equal(t, uint32(1), create.CollationBindingRevision)
			require.Equal(t, defines.MORPCVersion124, create.RequiredProtocolVersion)
			wire, err := p.Marshal()
			require.NoError(t, err)
			var decoded Plan
			require.NoError(t, decoded.Unmarshal(wire))
			require.True(t, proto.Equal(create, decoded.GetDdl().GetCreateTable()))
			require.True(t, proto.Equal(create, DeepCopyCreateTable(create)))
			rt := moruntime.ServiceRuntime(ctx.GetProcess().GetService())
			rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor, defines.MORPCVersion120)
			require.ErrorContains(t, RequireCreateTableCollationsForAuthoring(ctx.GetContext(), ctx.GetProcess(), create), "protocol version 124")
			p, err = build(sql)
			require.ErrorContains(t, err, "protocol version 124")
			require.Nil(t, p)
			rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor, defines.MORPCVersion124)
		})
	}
}

func TestNativePADDDLReplayRetainsGenerations(t *testing.T) {
	ctx, build := nativePADDDLContext(t)
	for _, tc := range []struct {
		sql      string
		revision uint32
	}{
		{"create table original (s varchar(8) collate utf8mb4_general_ci)", 0},
		{"create table original (s varchar(8) collate utf8mb4_bin, u varchar(8) collate utf8mb4_unicode_ci unique)", 0},
		{"create table original (s varchar(8) character set utf8mb4, n varchar(8)) collate utf8mb4_general_ci", 1},
		{"create table original (s varchar(8), n varchar(8) collate utf8mb4_general_ci)", 1},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			ctx.SetContext(context.WithValue(t.Context(), collationBindingRevisionKey{}, tc.revision))
			p, err := build(tc.sql)
			require.NoError(t, err)
			original := p.GetDdl().GetCreateTable().TableDef
			for _, index := range original.Indexes {
				catalog.SetIndexVisibility(index, true)
			}
			if len(original.Indexes) > 0 {
				previous := ctx.internalSQLExecutor
				t.Cleanup(func() { ctx.internalSQLExecutor = previous })
				ctx.internalSQLExecutor = executor.NewMemExecutor(func(sql string) (executor.Result, error) {
					if !strings.Contains(sql, "SELECT name, is_visible FROM mo_catalog.mo_indexes") {
						return executor.Result{}, nil
					}
					result := executor.NewMemResult([]types.Type{types.T_varchar.ToType(), types.T_int8.ToType()}, ctx.GetProcess().Mp())
					result.NewBatchWithRowCount(1)
					require.NoError(t, executor.AppendStringRows(result, 0, []string{"u"}))
					require.NoError(t, executor.AppendFixedRows(result, 1, []int8{1}))
					return result.GetResult(), nil
				})
			}
			before := proto.Clone(original).(*TableDef)
			ctx.tables["original"] = original
			ctx.objects["original"] = &ObjectRef{Obj: 1001, SchemaName: "tpch", ObjName: "original"}
			parent := context.WithValue(t.Context(), collationBindingRevisionKey{}, uint32(1))
			ctx.SetContext(parent)
			copied, err := build("create table copied like original")
			require.NoError(t, err)
			table := copied.GetDdl().GetCreateTable().TableDef
			for _, name := range []string{"s", "n", "u"} {
				if old := FindColumn(original.Cols, name); old != nil {
					got := FindColumn(table.Cols, name)
					require.NotNil(t, got)
					require.Equal(t, old.Typ, got.Typ)
				}
			}
			require.Equal(t, before, original)
			require.Equal(t, original.DefaultCharset, table.DefaultCharset)
			require.Equal(t, original.CollationVersion, table.CollationVersion)
			require.Same(t, parent, ctx.GetContext())
		})
	}
}

func TestNativePADAlterPreservesDomains(t *testing.T) {
	for _, tc := range []struct{ sql, failure string }{
		{"alter table n add column extra varchar(8) collate utf8mb4_bin", ""},
		{"alter table n rename column s to renamed", ""},
		{"alter table n modify s varchar(16) collate utf8mb4_general_ci, algorithm=copy", ""},
		{"alter table n modify s varchar(8) comment 'new', algorithm=inplace", ""},
		{"alter table n collate utf8mb4_bin", ""},
		{"alter table n add index k(id)", ""},
		{"alter table n add unique index k(id)", ""},
		{"alter table n drop column raw_s", ""},
		{"alter table n drop column s, add column s varchar(8) collate utf8mb4_bin", ""},
		{"alter table n add index k(s)", "native PAD"},
		{"alter table n add primary key(s)", "native PAD"},
		{"alter table n modify raw_s varchar(8) collate utf8mb4_general_ci", "native PAD"},
		{"alter table n convert to character set utf8mb4 collate utf8mb4_bin", "native PAD"},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			ctx, build := nativePADDDLContext(t)
			p, err := build("create table n (id int, raw_s varchar(8) character set utf8mb4, s varchar(8)) collate utf8mb4_general_ci")
			require.NoError(t, err)
			source := p.GetDdl().GetCreateTable().TableDef
			source.TblId = 1001
			for i, col := range source.Cols {
				col.ColId = uint64(i + 1)
			}
			ctx.tables["n"] = source
			ctx.objects["n"] = &ObjectRef{Obj: 1001, SchemaName: "tpch", ObjName: "n"}
			before := proto.Clone(source)
			parent := ctx.GetContext()
			p, err = build(tc.sql)
			if tc.failure != "" {
				require.ErrorContains(t, err, tc.failure)
				require.Nil(t, p)
			} else {
				require.NoError(t, err)
				alter := p.GetDdl().GetAlterTable()
				target := alter.CopyTableDef
				if target == nil {
					target = alter.TableDef
				}
				for _, old := range source.Cols {
					for _, next := range target.Cols {
						if old.ColId == next.ColId {
							require.Equal(t, old.Typ.Charset, next.Typ.Charset)
							require.Equal(t, old.Typ.CollationVersion, next.Typ.CollationVersion)
						}
					}
				}
				rt := moruntime.ServiceRuntime(ctx.GetProcess().GetService())
				rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor, defines.MORPCVersion120)
				_, err = build(tc.sql)
				require.ErrorContains(t, err, "protocol version 124")
			}
			require.True(t, proto.Equal(before, source), "ALTER不得修改共享来源schema")
			require.Same(t, parent, ctx.GetContext())
		})
	}
}

func TestNativePADAlterRenameZeroIDCannotRelabel(t *testing.T) {
	ctx, build := nativePADDDLContext(t)
	p, err := build("create table n (s varchar(8)) collate utf8mb4_general_ci")
	require.NoError(t, err)
	source := p.GetDdl().GetCreateTable().TableDef
	for i, col := range source.Cols {
		col.ColId = uint64(i)
	}
	require.Zero(t, FindColumn(source.Cols, "s").ColId)
	ctx.tables["n"] = source
	ctx.objects["n"] = &ObjectRef{Obj: 1001, SchemaName: "tpch", ObjName: "n"}
	_, err = build("alter table n rename column s to renamed, algorithm=copy")
	require.NoError(t, err)
	_, err = build("alter table n rename column s to renamed, convert to character set utf8mb4 collate utf8mb4_bin")
	require.ErrorContains(t, err, "online native PAD collation conversion")
	// SQL绑定已有MODIFY保护；接收的typed ALTER也必须独立校验零值列ID。
	target := DeepCopyTableDef(source, true)
	col := FindColumn(target.Cols, "s")
	col.Name, col.OriginName, col.Typ.Charset = "renamed", "renamed", 2
	err = RequireAlterTableCollationsForAuthoring(ctx.GetContext(), ctx.GetProcess(), &planpb.AlterTable{TableDef: source, CopyTableDef: target})
	require.ErrorContains(t, err, "online native PAD collation conversion")
}

func TestNativePADDDLRejectsPhysicalKeys(t *testing.T) {
	_, build := nativePADDDLContext(t)
	for _, sql := range []string{
		"create table n (s varchar(8) collate utf8mb4_bin primary key)",
		"create table n (s varchar(8) collate utf8mb4_general_ci, key k(s))",
		"create table n (s varchar(8) collate utf8mb4_bin unique)",
		"create table n (i int, s varchar(8) collate utf8mb4_bin, primary key(i,s))",
		"create table n (s varchar(8) collate utf8mb4_general_ci) cluster by s",
		"create table n (s varchar(8) collate utf8mb4_general_ci, key k((lower(s))))",
		"create table n (p varchar(8) primary key, s varchar(8) collate utf8mb4_general_ci, foreign key(s) references n(p))",
		"create table n (u varchar(8) collate utf8mb4_unicode_ci unique) collate utf8mb4_general_ci",
	} {
		t.Run(sql, func(t *testing.T) {
			p, err := build(sql)
			require.ErrorContains(t, err, "native PAD")
			require.Nil(t, p)
		})
	}
}
