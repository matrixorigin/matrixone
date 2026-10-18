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

package plan

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestTableDumpTimestampDefaults(t *testing.T) {
	for _, tc := range []struct {
		name, sql string
		legacy    bool
		changeTZ  bool
		createTZ  int
		capture   bool
	}{
		{name: "implicit_precision_6", sql: "create table t(a timestamp(6))", legacy: true},
		{name: "implicit_precision_0", sql: "create table t(a timestamp)", legacy: true},
		{name: "explicit_current", sql: "create table t(a timestamp(6) default current_timestamp(6))"},
		{name: "literal_same_zone", sql: "create table t(a timestamp default '2024-01-01 00:00:00')", capture: true},
		{name: "literal_changed_zone", sql: "create table t(a timestamp default '2024-01-01 00:00:00')", changeTZ: true, capture: true},
		{name: "fractional_quarter_hour", sql: "create table t(a timestamp(6) default '2024-01-01 00:00:00.123456')", createTZ: 5*3600 + 45*60, capture: true},
		{name: "positive_limit", sql: "create table t(a timestamp default '2024-01-01 00:00:00')", createTZ: 14 * 3600, capture: true},
		{name: "negative_limit", sql: "create table t(a timestamp default '2024-01-01 00:00:00')", createTZ: -(13*3600 + 59*60), capture: true},
		{name: "epoch_boundary", sql: "create table t(a timestamp default '1970-01-01 00:00:01')", changeTZ: true, capture: true},
		{name: "utc_probe_below_epoch", sql: "create table t(a timestamp default '1969-12-31 23:00:01')", createTZ: -3600, capture: true},
		{name: "null", sql: "create table t(a timestamp default null)", changeTZ: true},
		{name: "zero", sql: "create table t(a timestamp default '0000-00-00 00:00:00')", changeTZ: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := newPlanTestProcess(t)
			proc.GetSessionInfo().TimeZone = time.UTC
			if tc.createTZ != 0 {
				proc.GetSessionInfo().TimeZone = time.FixedZone("creation", tc.createTZ)
			}
			compiler := NewMockCompilerContext(false, proc)
			compiler.ResolveVariableFunc = func(name string, _, _ bool) (any, error) {
				switch name {
				case "explicit_defaults_for_timestamp":
					return !tc.legacy, nil
				case "div_precision_increment":
					return int64(4), nil
				default:
					return "", nil
				}
			}
			stmt, err := mysql.ParseOne(t.Context(), tc.sql, 1)
			require.NoError(t, err)
			defer stmt.Free()
			built, err := BuildPlan(compiler, stmt, false)
			require.NoError(t, err)
			def := built.GetDdl().GetCreateTable().TableDef
			before := proto.Clone(def)
			if tc.changeTZ || tc.createTZ != 0 {
				proc.GetSessionInfo().TimeZone = time.FixedZone("backup", 8*3600)
			}
			backupTZ := proc.GetSessionInfo().TimeZone
			warnings := &process.WarningProbe{}
			proc.WarningSink = warnings
			// The reading session's explicit-default policy must not prevent
			// reproduction of catalog expressions authored under legacy policy.
			compiler.ResolveVariableFunc = nil
			sensitive, err := AnalyzeTableDumpBindings(compiler, def, def)
			require.NoError(t, err, fmt.Sprintf("definition: %s", def.Cols[0]))
			if tc.capture {
				require.True(t, sensitive, "same-zone DUMP must retain a witness for cross-zone LOAD")
			}
			require.True(t, proto.Equal(before, def), "verification must not rewrite catalog trees")
			require.Same(t, backupTZ, proc.GetSessionInfo().TimeZone, "verification must not change the session time zone")
			require.Same(t, proc, compiler.GetProcess())
			require.Same(t, warnings, proc.WarningSink)
			require.False(t, warnings.Warned())
			require.NotNil(t, def.Cols[0].Default.Expr)
			if tc.legacy {
				require.NotNil(t, def.Cols[0].OnUpdate.Expr)
				for _, kind := range []string{"default", "on_update"} {
					forged := proto.Clone(def).(*planpb.TableDef)
					expr := forged.Cols[0].Default.Expr
					if kind == "on_update" {
						expr = forged.Cols[0].OnUpdate.Expr
					}
					for expr.GetF().Func.ObjName != "current_timestamp" {
						expr = expr.GetF().Args[0]
					}
					require.Equal(t, int64(def.Cols[0].Typ.Scale), expr.GetF().Args[0].GetLit().GetI64Val())
					expr.GetF().Args[0].GetLit().Value = &planpb.Literal_I64Val{I64Val: 3}
					_, err = AnalyzeTableDumpBindings(compiler, def, forged)
					require.ErrorContains(t, err, "cannot verify bound "+kind)
				}
			}
			if tc.capture {
				for _, mutation := range []string{"microsecond", "impossible_offset", "overflow", "type"} {
					t.Run("reject_"+mutation, func(t *testing.T) {
						forged := proto.Clone(def).(*planpb.TableDef)
						expr := forged.Cols[0].Default.Expr
						stamp := expr.GetLit().GetTimestampval()
						switch mutation {
						case "microsecond":
							stamp++
						case "impossible_offset":
							stamp += 48 * 3600 * 1_000_000
						case "overflow":
							stamp = math.MinInt64
						case "type":
							expr.Typ.Scale++
						}
						expr.GetLit().Value = &planpb.Literal_Timestampval{Timestampval: stamp}
						_, err = AnalyzeTableDumpBindings(compiler, def, forged)
						require.ErrorContains(t, err, "cannot verify bound default")
						require.Same(t, backupTZ, proc.GetSessionInfo().TimeZone)
						require.False(t, warnings.Warned())
					})
				}
			}
		})
	}
}

func TestTableDumpRejectsExpressionsForEmptyTarget(t *testing.T) {
	compiler := NewMockCompilerContext(false, newPlanTestProcess(t))
	target := &planpb.TableDef{Cols: []*planpb.ColDef{{Name: "a"}}}
	supplied := proto.Clone(target).(*planpb.TableDef)
	supplied.Cols[0].Default = &planpb.Default{OriginString: "1", Expr: makePlan2Int64ConstExprWithType(1)}
	_, err := AnalyzeTableDumpBindings(compiler, target, supplied)
	require.ErrorContains(t, err, "expression count does not match")
}
