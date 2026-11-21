// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/stretchr/testify/require"
)

// Issue #31688: a snapshot-qualified table is a read-only historical version;
// using it as a write or DDL target used to modify the live table silently.
func TestRejectSnapshotQualifiedWriteTarget(t *testing.T) {
	ctx := NewMockCompilerContext(false, nil)
	check := func(sql string) error {
		stmts, err := parsers.Parse(context.TODO(), dialect.MYSQL, sql, 1)
		require.NoError(t, err, sql)
		return rejectSnapshotQualifiedWriteTarget(ctx, stmts[0])
	}
	for _, sql := range []string{
		"insert into t{snapshot='s'} values (1)",
		"replace into t{snapshot='s'} values (1)",
		"update t{snapshot='s'} set v = 1 where id = 1",
		"update t{snapshot='s'} as x join o on o.id = x.id set x.v = 1",
		"update o join t{snapshot='s'} on o.id = t.id set t.v = 1",
		"delete from t{snapshot='s'} where id = 1",
		"delete x from t{snapshot='s'} as x join o on o.id = x.id",
		"alter table t{snapshot='s'} add column c int",
		"truncate table t{snapshot='s'}",
		"drop table t{snapshot='s'}",
		"drop table o, t{snapshot='s'}",
		"insert into t{timestamp='2024-01-01 00:00:00'} values (1)",
	} {
		err := check(sql)
		require.Error(t, err, sql)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrNotSupported), "%s: %v", sql, err)
	}
	for _, sql := range []string{
		"insert into t select * from t{snapshot='s'}",
		"update o join t{snapshot='s'} as x on o.id = x.id set o.v = x.v",
		"delete o from o join t{snapshot='s'} as x on o.id = x.id",
		"select * from t{snapshot='s'}",
		"insert into t values (1)",
		"drop table t",
	} {
		require.NoError(t, check(sql), sql)
	}
}

type caseSensitiveNamesContext struct{ *MockCompilerContext }

func (c caseSensitiveNamesContext) GetLowerCaseTableNames() int64 { return 0 }

// Review follow-ups for #31688: unqualified SET columns resolved to a
// snapshot-qualified table, the remaining write/DDL statements, and
// case-sensitive alias matching under lower_case_table_names=0.
func TestRejectSnapshotQualifiedWriteTargetFollowUps(t *testing.T) {
	ctx := NewMockCompilerContext(false, nil)
	intCol := func(name string) *planpb.ColDef {
		return &planpb.ColDef{Name: name, OriginName: name, Typ: planpb.Type{Id: int32(types.T_int32)}}
	}
	ctx.tables["t"] = &planpb.TableDef{Name: "t", DbName: "tpch", Cols: []*planpb.ColDef{intCol("id"), intCol("v")}}
	ctx.objects["t"] = &ObjectRef{ObjName: "t", SchemaName: "tpch"}
	ctx.tables["o"] = &planpb.TableDef{Name: "o", DbName: "tpch", Cols: []*planpb.ColDef{intCol("id"), intCol("w")}}
	ctx.objects["o"] = &ObjectRef{ObjName: "o", SchemaName: "tpch"}

	check := func(c CompilerContext, sql string) error {
		stmts, err := parsers.Parse(context.TODO(), dialect.MYSQL, sql, c.GetLowerCaseTableNames())
		require.NoError(t, err, sql)
		return rejectSnapshotQualifiedWriteTarget(c, stmts[0])
	}
	rejected := func(c CompilerContext, sql string) {
		t.Helper()
		err := check(c, sql)
		require.Error(t, err, sql)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrNotSupported), "%s: %v", sql, err)
	}
	for _, sql := range []string{
		// v resolves to the snapshot-qualified t.
		"update t{snapshot='s'} as x join o on x.id = o.id set v = 9",
		"update o join t{snapshot='s'} on o.id = t.id set v = o.w",
		"load data infile '/tmp/x.csv' into table t{snapshot='s'}",
		"insert all into t{snapshot='s'} values (1, 1) select 1",
		"create index ix on t{snapshot='s'}(v)",
		"drop index ix on t{snapshot='s'}",
		"rename table t{snapshot='s'} to t2",
	} {
		rejected(ctx, sql)
	}
	for _, sql := range []string{
		// w only exists in the live, unqualified o.
		"update t{snapshot='s'} as x join o on x.id = o.id set w = 9",
		"create index ix on t(v)",
		"drop index ix on t",
		"rename table t to t2",
	} {
		require.NoError(t, check(ctx, sql), sql)
	}

	// Under lower_case_table_names=0 the aliases x and X are distinct: only X
	// names the snapshot-qualified table.
	cs := caseSensitiveNamesContext{ctx}
	require.NoError(t, check(cs, "update t as x join o{snapshot='s'} as X on x.id = X.id set x.v = X.id"))
	rejected(cs, "update t as x join o{snapshot='s'} as X on x.id = X.id set X.w = x.v")
	// With case-insensitive names x and X are the same alias, so the qualified
	// assignment is rejected as before.
	rejected(ctx, "update t{snapshot='s'} as X join o on X.id = o.id set x.v = 1")
}
