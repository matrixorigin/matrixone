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
	"context"
	"fmt"
	"math"
	"sync/atomic"
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

func TestTableDumpDefaultTimeZones(t *testing.T) {
	for _, tc := range []struct {
		name, declaration, creationZone string
		capture                         bool
	}{
		{"datetime_expression", "datetime default (from_unixtime(1704067200))", "UTC", true},
		{"varchar_expression", "varchar(64) default (from_unixtime(1704067200,'%Y-%m-%d %H:%i:%s'))", "UTC", true},
		{"named_timezone", "timestamp default '1971-01-01 00:00:00'", "Africa/Monrovia", true},
		{"numeric_expression", "int default (hour(from_unixtime(1704067200)))", "Pacific/Chatham", true},
		{"unfolded_unix_timestamp", "bigint default (unix_timestamp('1971-01-01 00:00:00'))", "Africa/Monrovia", false},
		{"dst_expression", "varchar(64) default (concat(from_unixtime(1704067200,'%H'),':',from_unixtime(1719792000,'%H')))", "America/New_York", true},
		{"uuid_datetime", "uuid default (uuid(cast('2024-01-01 00:00:00' as datetime)))", "Africa/Monrovia", true},
		{"datetime_literal", "datetime default '2024-01-01 00:00:00'", "UTC", false},
		{"ordinary_arithmetic", "int default (1+1)", "UTC", false},
		{"ordinary_text", "varchar(64) default 'literal'", "UTC", false},
		{"ordinary_string_function", "varchar(64) default (concat('a','b'))", "UTC", false},
		{"literal_null", "timestamp default null", "UTC", false},
		{"literal_zero", "timestamp default '0000-00-00 00:00:00'", "UTC", false},
		{"nullable_utc", "int default (if(hour(from_unixtime(1704067200))=0,null,1))", "UTC", true},
		{"nullable_east", "int default (if(hour(from_unixtime(1704067200))=0,null,1))", "Asia/Shanghai", true},
		{"zero_result", "timestamp default (if(hour(from_unixtime(1704067200))=0,'0000-00-00 00:00:00','2024-01-01 00:00:00'))", "UTC", true},
		{"backup_overflow", "tinyint default (hour(from_unixtime(1704067200))+120)", "UTC", true},
		{"budget_control", "varchar(64) default (from_unixtime(cast(1704067200 as decimal(20,6))+cast(1 as decimal(20,6))/cast(3 as decimal(20,6)),'%Y'))", "UTC", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := newPlanTestProcess(t)
			location, err := time.LoadLocation(tc.creationZone)
			require.NoError(t, err)
			proc.GetSessionInfo().TimeZone = location
			compiler := NewMockCompilerContext(false, proc)
			compiler.ResolveVariableFunc = func(name string, _, _ bool) (any, error) {
				if name == "div_precision_increment" {
					return int64(4), nil
				}
				if name == "explicit_defaults_for_timestamp" {
					return true, nil
				}
				return "", nil
			}
			stmt, err := mysql.ParseOne(t.Context(), "create table t(a "+tc.declaration+")", 1)
			require.NoError(t, err)
			defer stmt.Free()
			built, err := BuildPlan(compiler, stmt, false)
			require.NoError(t, err)
			def := built.GetDdl().GetCreateTable().TableDef
			before := proto.Clone(def)
			backupZones := []*time.Location{time.FixedZone("backup", 8*3600), location}
			if location != time.UTC {
				backupZones = append(backupZones, time.UTC)
			}
			for _, backupZone := range backupZones {
				proc.GetSessionInfo().TimeZone = backupZone
				warnings := &process.WarningProbe{}
				proc.WarningSink = warnings
				sensitive, err := AnalyzeTableDumpBindings(compiler, def, def)
				require.NoError(t, err)
				require.Equal(t, tc.capture, sensitive, "same-zone capture must protect a subsequent cross-zone LOAD")
				require.True(t, proto.Equal(before, def))
				require.Same(t, backupZone, proc.GetSessionInfo().TimeZone)
				require.False(t, warnings.Warned())
			}
			if tc.name == "budget_control" {
				forged := proto.Clone(def).(*planpb.TableDef)
				require.True(t, tamperTableDumpDefaultValue(forged.Cols[0].Default.Expr))
				started := time.Now()
				_, err := AnalyzeTableDumpBindings(compiler, def, forged)
				require.ErrorContains(t, err, "expression binding exceeds limit")
				t.Logf("unmatched division/timezone search stopped at shared budget after %s", time.Since(started))
				return
			}
			if tc.capture {
				for _, mutation := range []string{"value", "type"} {
					t.Run("reject_"+mutation, func(t *testing.T) {
						forged := proto.Clone(def).(*planpb.TableDef)
						expr := forged.Cols[0].Default.Expr
						if mutation == "type" {
							expr.Typ.Scale++
						} else {
							require.True(t, tamperTableDumpDefaultValue(expr))
						}
						zone := proc.GetSessionInfo().TimeZone
						_, err := AnalyzeTableDumpBindings(compiler, def, forged)
						require.ErrorContains(t, err, "cannot verify bound default")
						require.Same(t, zone, proc.GetSessionInfo().TimeZone)
						require.True(t, proto.Equal(before, def))
					})
				}
			}
			if tc.name == "dst_expression" {
				// Cancel while an unmatched origin is being rebound, rather than
				// cancelling before Analyze enters its timezone witness search.
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				bindingCtx := &tableDumpCancellingContext{Context: ctx, cancel: cancel}
				compiler.SetContext(bindingCtx)
				proc.GetSessionInfo().TimeZone = time.UTC
				// A valid default may find its witness before cancellation.
				// Force the same impossible value already rejected above so the
				// search reaches its cancellation checkpoint on every host.
				forged := proto.Clone(def).(*planpb.TableDef)
				require.True(t, tamperTableDumpDefaultValue(forged.Cols[0].Default.Expr))
				forgedBefore := proto.Clone(forged)
				_, err := AnalyzeTableDumpBindings(compiler, def, forged)
				require.ErrorIs(t, err, context.Canceled)
				require.True(t, proto.Equal(before, def))
				require.True(t, proto.Equal(forgedBefore, forged))
				require.GreaterOrEqual(t, bindingCtx.calls.Load(), int32(8))
				require.Same(t, time.UTC, proc.GetSessionInfo().TimeZone)
			}
		})
	}
}

func tamperTableDumpDefaultValue(expr *planpb.Expr) bool {
	if literal := expr.GetLit(); literal != nil {
		if literal.Isnull {
			literal.Isnull = false
			literal.Value = &planpb.Literal_I32Val{I32Val: 777}
			return true
		}
		switch value := literal.Value.(type) {
		case *planpb.Literal_Datetimeval:
			value.Datetimeval++
		case *planpb.Literal_Timestampval:
			value.Timestampval++
		case *planpb.Literal_Sval:
			value.Sval = "unreachable formatted default"
		case *planpb.Literal_I8Val:
			value.I8Val = -128
		case *planpb.Literal_I32Val:
			value.I32Val = 777
		case *planpb.Literal_I64Val:
			value.I64Val = 777
		default:
			return false
		}
		return true
	}
	if fn := expr.GetF(); fn != nil {
		for _, arg := range fn.Args {
			if tamperTableDumpDefaultValue(arg) {
				return true
			}
		}
	}
	return false
}

type tableDumpCancellingContext struct {
	context.Context
	cancel context.CancelFunc
	calls  atomic.Int32
}

func (c *tableDumpCancellingContext) Err() error {
	if c.calls.Add(1) == 8 {
		c.cancel()
	}
	return c.Context.Err()
}

func TestTableDumpDefaultLocales(t *testing.T) {
	for _, tc := range []struct {
		name, declaration, locale, zone string
		capture                         bool
	}{
		{"monthname", "varchar(64) default (monthname('2024-01-01'))", "en_US", "UTC", true},
		{"date_format", "varchar(64) default (date_format('2024-01-01','%M'))", "en_US", "UTC", true},
		{"dayname", "varchar(64) default (dayname('2024-01-01'))", "ja_JP", "UTC", true},
		{"mixed", "varchar(64) default (date_format(from_unixtime(1704067200),'%M %H'))", "fr_FR", "America/New_York", true},
		{"german", "varchar(64) default (monthname('2024-01-01'))", "de_DE", "UTC", true},
		{"localized_verbs", "varchar(64) default (date_format('2024-02-05','%M/%b/%W/%a'))", "fr_FR", "UTC", true},
		{"budget", "varchar(64) default (date_format(from_unixtime(cast(1704067200 as decimal(20,6))+cast(1 as decimal(20,6))/cast(3 as decimal(20,6))),'%M'))", "fr_FR", "America/New_York", true},
		{"nullable", "int default (if(monthname('2024-01-01')='January',null,1))", "en_US", "UTC", true},
		{"overflow", "tinyint default (if(monthname('2024-01-01')='January',120,128))", "en_US", "UTC", true},
		{"literal", "varchar(64) default 'January'", "en_US", "UTC", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := newPlanTestProcess(t)
			zone, err := time.LoadLocation(tc.zone)
			require.NoError(t, err)
			proc.GetSessionInfo().TimeZone = zone
			locale := tc.locale
			// The resolver takes precedence over a conflicting session snapshot.
			proc.GetSessionInfo().LCTimeNames = "ja_JP"
			proc.SetResolveVariableFunc(func(name string, _, _ bool) (any, error) {
				if name == "lc_time_names" {
					return locale, nil
				}
				return "", nil
			})
			compiler := NewMockCompilerContext(false, proc)
			compiler.ResolveVariableFunc = func(name string, _, _ bool) (any, error) {
				switch name {
				case "div_precision_increment":
					return int64(4), nil
				case "explicit_defaults_for_timestamp":
					return true, nil
				case "lc_time_names":
					return locale, nil
				}
				return "", nil
			}
			stmt, err := mysql.ParseOne(t.Context(), "create table t(a "+tc.declaration+")", 1)
			require.NoError(t, err)
			defer stmt.Free()
			built, err := BuildPlan(compiler, stmt, false)
			require.NoError(t, err)
			def := built.GetDdl().GetCreateTable().TableDef
			before := proto.Clone(def)
			for _, readingLocale := range []string{tc.locale, "de_DE"} {
				locale = readingLocale
				// The resolver takes precedence over a conflicting session snapshot.
				proc.GetSessionInfo().LCTimeNames = "ja_JP"
				proc.GetSessionInfo().TimeZone = time.UTC
				warnings := &process.WarningProbe{}
				proc.WarningSink = warnings
				capture, err := AnalyzeTableDumpBindings(compiler, def, def)
				require.NoError(t, err)
				require.Equal(t, tc.capture, capture)
				require.Equal(t, "ja_JP", proc.GetSessionInfo().LCTimeNames)
				resolved, err := proc.GetResolveVariableFunc()("lc_time_names", true, false)
				require.NoError(t, err)
				require.Equal(t, locale, resolved)
				require.Same(t, time.UTC, proc.GetSessionInfo().TimeZone)
				require.False(t, warnings.Warned())
				require.True(t, proto.Equal(before, def))
			}
			if tc.capture {
				forged := proto.Clone(def).(*planpb.TableDef)
				require.True(t, tamperTableDumpDefaultValue(forged.Cols[0].Default.Expr))
				forgedBefore := proto.Clone(forged)
				if tc.name == "mixed" {
					ctx, cancel := context.WithCancel(t.Context())
					defer cancel()
					bindingCtx := &tableDumpCancellingContext{Context: ctx, cancel: cancel}
					compiler.SetContext(bindingCtx)
					_, err := AnalyzeTableDumpBindings(compiler, def, forged)
					require.ErrorIs(t, err, context.Canceled)
					require.GreaterOrEqual(t, bindingCtx.calls.Load(), int32(8))
					compiler.SetContext(t.Context())
				}
				_, err := AnalyzeTableDumpBindings(compiler, def, forged)
				if tc.name == "budget" {
					require.ErrorContains(t, err, "expression binding exceeds limit")
				} else {
					require.ErrorContains(t, err, "cannot verify bound default")
				}
				require.True(t, proto.Equal(forgedBefore, forged))
				require.True(t, proto.Equal(before, def))
				require.Equal(t, "ja_JP", proc.GetSessionInfo().LCTimeNames)
				require.Same(t, time.UTC, proc.GetSessionInfo().TimeZone)
			}
		})
	}
}
