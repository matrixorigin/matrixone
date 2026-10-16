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
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/ttl"
	"github.com/stretchr/testify/require"
)

func TestTTLCreatePlanRoundTrip(t *testing.T) {
	for _, typ := range []string{"date", "datetime", "datetime(6)", "timestamp", "timestamp(6)"} {
		for _, unit := range []string{"second", "minute", "hour", "day", "week"} {
			t.Run(typ+"_"+unit, func(t *testing.T) {
				ctx := NewMockCompilerContext(false, newPlanTestProcess(t))
				sql := fmt.Sprintf("create table t(id int primary key, event_time %s, payload varchar(20)) ttl event_time + interval 7 %s delete", typ, unit)
				stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, sql, 1)
				require.NoError(t, err)
				defer stmt.Free()
				require.Contains(t, tree.String(stmt, dialect.MYSQL), "ttl `event_time`")
				built, err := BuildPlan(ctx, stmt, false)
				require.NoError(t, err)
				def := built.GetDdl().GetCreateTable().TableDef
				policy, err := ttl.Get(def)
				require.NoError(t, err)
				require.Equal(t, uint32(1), policy.Seqnum)
				shown, _, err := ConstructCreateTableSQL(ctx, def, nil, false, nil)
				require.NoError(t, err)
				require.Contains(t, shown, "TTL `event_time`")
				require.NotContains(t, shown, "mo_ttl")
				restoredStmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, shown, 1)
				require.NoError(t, err)
				defer restoredStmt.Free()
				restored, err := BuildPlan(ctx, restoredStmt, false)
				require.NoError(t, err)
				actual, err := ttl.Get(restored.GetDdl().GetCreateTable().TableDef)
				require.NoError(t, err)
				require.Equal(t, policy, actual)
				copyPolicy, err := ttl.Get(DeepCopyTableDef(def, true))
				require.NoError(t, err)
				require.Equal(t, policy, copyPolicy)
			})
		}
	}
}
func TestTTLCreatePlanRejectsInvalid(t *testing.T) {
	for _, sql := range []string{
		"create table t(ts datetime) ttl ts + interval 0 second",
		"create table t(ts datetime) ttl ts + interval 18446744073709551615 second",
		"create table t(ts datetime) ttl ts + interval 1 month",
		"create table t(ts datetime) ttl ts + interval 1 year",
		"create table t(ts datetime) ttl ts + interval 1 microsecond",
		"create table t(ts int) ttl ts + interval 1 day",
		"create table t(ts time) ttl ts + interval 1 day",
		"create table t(ts varchar(20)) ttl ts + interval 1 day",
		"create table t(ts datetime) ttl missing + interval 1 day",
		"create table t(ts datetime) ttl ts + interval 1 day ttl ts + interval 2 day",
		"create temporary table t(ts datetime) ttl ts + interval 1 day",
		"create cluster table t(ts datetime) ttl ts + interval 1 day",
		"create table t(ts datetime) properties('mo_ttl'='{}')",
		"create table mo_catalog.mo_ttl_invalid(ts datetime) ttl ts + interval 1 day",
	} {
		t.Run(sql, func(t *testing.T) {
			stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, sql, 1)
			require.NoError(t, err)
			defer stmt.Free()
			_, err = BuildPlan(NewMockCompilerContext(false, newPlanTestProcess(t)), stmt, false)
			require.Error(t, err)
		})
	}
}
func TestTTLAlterPolicyAndDependencies(t *testing.T) {
	ctx := NewMockCompilerContext(true, newPlanTestProcess(t))
	var source *TableDef
	// Obtain exact temporal type and ordinary table metadata from the CREATE binder.
	stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, "create table ttl_t(id int primary key, ts datetime, payload int) ttl ts + interval 1 day", 1)
	require.NoError(t, err)
	defer stmt.Free()
	built, err := BuildPlan(ctx, stmt, false)
	require.NoError(t, err)
	source = built.GetDdl().GetCreateTable().TableDef
	source.TblId = 123
	source.DbName = ctx.DefaultDatabase()
	ctx.tables["ttl_t"] = source
	ctx.objects["ttl_t"] = &ObjectRef{Obj: 123, ObjName: "ttl_t", SchemaName: source.DbName}
	for _, sql := range []string{"alter table ttl_t modify ttl ts + interval 2 hour", "alter table ttl_t ttl ts + interval 2 hour", "alter table ttl_t remove ttl"} {
		t.Run(sql, func(t *testing.T) {
			stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, sql, 1)
			require.NoError(t, err)
			defer stmt.Free()
			p, err := BuildPlan(ctx, stmt, false)
			require.NoError(t, err)
			original, err := ttl.Get(source)
			require.NoError(t, err)
			require.Equal(t, uint64(86400), original.Seconds)
			policy, err := ttl.Get(p.GetDdl().GetAlterTable().CopyTableDef)
			require.NoError(t, err)
			if sql == "alter table ttl_t remove ttl" {
				require.Nil(t, policy)
			} else {
				require.Equal(t, uint64(7200), policy.Seconds)
			}
		})
	}
	for _, sql := range []string{"alter table ttl_t drop column ts", "alter table ttl_t rename column ts to renamed", "alter table ttl_t modify ts datetime(6)", "alter table ttl_t change ts renamed datetime", "alter table ttl_t remove ttl, drop ts", "alter table ttl_t modify ttl ts + interval 1 day, modify ttl ts + interval 2 day"} {
		stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, sql, 1)
		require.NoError(t, err)
		defer stmt.Free()
		_, err = BuildPlan(ctx, stmt, false)
		require.Error(t, err, sql)
	}
}
