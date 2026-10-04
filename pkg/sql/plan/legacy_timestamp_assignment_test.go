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
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/stretchr/testify/require"
)

func TestLegacyTimestampRuntimeNullAssignment(t *testing.T) {
	for _, policy := range []struct {
		name     string
		legacy   bool
		nullable bool
	}{
		{name: "legacy NOT NULL", legacy: true},
		{name: "legacy nullable", legacy: true, nullable: true},
		{name: "explicit defaults enabled"},
	} {
		t.Run(policy.name, func(t *testing.T) {
			mock := NewMockOptimizer(false)
			mock.ctxt.ResolveVariableFunc = func(name string, _, _ bool) (interface{}, error) {
				if name == "explicit_defaults_for_timestamp" {
					return !policy.legacy, nil
				}
				return "", nil
			}
			proc := mock.ctxt.GetProcess()
			proc.GetSessionInfo().TimeZone = time.UTC
			proc.Base.UnixTime = time.Date(2024, 1, 2, 3, 4, 5, 123456000, time.UTC).UnixNano()
			frozen := types.UnixNanoToTimestamp(proc.Base.UnixTime)
			literal, err := types.ParseTimestamp(time.UTC, "2000-01-01 00:00:00", 6)
			require.NoError(t, err)
			col := &planpb.ColDef{Typ: planpb.Type{Id: int32(types.T_timestamp), Scale: 6},
				Default: &planpb.Default{NullAbility: policy.nullable, Expr: makePlan2TimestampConstExprWithType(int64(literal))}}
			builder := NewQueryBuilder(planpb.Query_UPDATE, &mock.ctxt, false, false)
			source := &planpb.Expr{Typ: col.Typ, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}}}
			expr, err := builder.wrapLegacyTimestampAssignment(col, source)
			require.NoError(t, err)
			executor, err := colexec.NewExpressionExecutor(proc, expr)
			require.NoError(t, err)
			defer executor.Free()
			// Reuse one executor across NULL -> value -> NULL, as cached
			// prepared executions do; no wall-clock sleep or RNG oracle.
			for _, null := range []bool{true, false, true} {
				b := batch.NewWithSize(1)
				b.Vecs[0] = vector.NewVec(types.T_timestamp.ToTypeWithScale(6))
				require.NoError(t, vector.AppendFixed(b.Vecs[0], literal, null, proc.Mp()))
				b.SetRowCount(1)
				executor.ResetForNextQuery()
				out, err := executor.Eval(proc, []*batch.Batch{b}, nil)
				require.NoError(t, err)
				adjust := policy.legacy && !policy.nullable
				require.Equal(t, null && !adjust, out.IsNull(0))
				if !out.IsNull(0) {
					want := literal
					if null {
						want = frozen
					}
					require.Equal(t, want, vector.GetFixedAtWithTypeCheck[types.Timestamp](out, 0), "NULL uses statement time, never the literal DEFAULT")
				}
				b.Clean(proc.Mp())
			}
			nullAssignment, err := buildLegacyTimestampNullAssignment(&mock.ctxt, col)
			require.NoError(t, err)
			require.Equal(t, policy.legacy && !policy.nullable, nullAssignment != nil)
		})
	}
}

func TestLegacyTimestampVolatileWrapperSharesMemoOwner(t *testing.T) {
	mock := NewMockOptimizer(false)
	mock.ctxt.ResolveVariableFunc = func(string, bool, bool) (interface{}, error) { return int8(0), nil }
	builder := NewQueryBuilder(planpb.Query_UPDATE, &mock.ctxt, false, false)
	col := &planpb.ColDef{Typ: planpb.Type{Id: int32(types.T_timestamp)}, Default: &planpb.Default{}}
	rand := expressionDefaultRand(t)
	expr, err := builder.wrapLegacyTimestampAssignment(col, rand)
	require.NoError(t, err)
	var ids []int32
	require.NoError(t, planpb.VisitExprTree(expr, func(e *planpb.Expr) error {
		if f := e.GetF(); f != nil && f.Func.GetObjName() == "rand" {
			ids = append(ids, e.AuxId)
		}
		return nil
	}))
	require.Len(t, ids, 2)
	require.Negative(t, ids[0], "both execution positions must share an expression-local memo")
	require.Equal(t, ids[0], ids[1])
	require.Zero(t, rand.AuxId, "do not mutate the caller's expression")
}

func TestLegacyFirstTimestampDefinitionPolicy(t *testing.T) {
	for _, tc := range []struct {
		name       string
		definition string
		legacy     bool
		implicit   bool
		onUpdate   bool
		nullable   bool
	}{
		{"implicit first", "first_ts TIMESTAMP(6)", true, true, true, false},
		{"explicit nullable consumes first", "first_ts TIMESTAMP(6) NULL", true, false, false, true},
		{"explicit default consumes first", "first_ts TIMESTAMP(6) DEFAULT '2000-01-01 00:00:00'", true, false, false, false},
		{"explicit on update consumes first", "first_ts TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6)", true, false, true, false},
		{"explicit defaults enabled", "first_ts TIMESTAMP(6)", false, false, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := NewMockOptimizer(false)
			mock.ctxt.ResolveVariableFunc = func(name string, _, _ bool) (interface{}, error) {
				if name == "explicit_defaults_for_timestamp" {
					return !tc.legacy, nil
				}
				return "", nil
			}
			stmts, err := mysql.Parse(mock.ctxt.GetContext(), "CREATE TABLE timestamp_policy (id INT, "+tc.definition+", later_ts TIMESTAMP(6))", 1)
			require.NoError(t, err)
			defer stmts[0].Free()
			p, err := BuildPlan(&mock.ctxt, stmts[0], false)
			require.NoError(t, err)
			cols := p.GetDdl().GetCreateTable().GetTableDef().Cols
			require.Equal(t, tc.nullable, cols[1].Default.NullAbility)
			require.Equal(t, !tc.legacy, cols[2].Default.NullAbility, "implicit NOT NULL applies to later legacy columns too")
			if tc.legacy {
				proc := mock.ctxt.GetProcess()
				proc.GetSessionInfo().TimeZone = time.UTC
				executor, err := colexec.NewExpressionExecutor(proc, cols[2].Default.Expr)
				require.NoError(t, err)
				defer executor.Free()
				out, err := executor.Eval(proc, []*batch.Batch{batch.EmptyForConstFoldBatch}, nil)
				require.NoError(t, err)
				require.False(t, out.IsNull(0))
				require.Equal(t, types.ZeroTimestamp, vector.GetFixedAtWithTypeCheck[types.Timestamp](out, 0))
			}
			require.Equal(t, tc.implicit, exprContainsFunc(cols[1].Default.Expr, "current_timestamp"))
			require.Equal(t, tc.onUpdate, cols[1].OnUpdate != nil)
			require.Nil(t, cols[2].OnUpdate, "the exception belongs to the first definition, not the first eligible synthesis")
			if tc.implicit {
				proc := mock.ctxt.GetProcess()
				proc.Base.UnixTime = time.Date(2024, 1, 2, 3, 4, 5, 123456000, time.UTC).UnixNano()
				executor, err := colexec.NewExpressionExecutor(proc, cols[1].Default.Expr)
				require.NoError(t, err)
				defer executor.Free()
				out, err := executor.Eval(proc, []*batch.Batch{batch.EmptyForConstFoldBatch}, nil)
				require.NoError(t, err)
				require.Equal(t, types.UnixNanoToTimestamp(proc.Base.UnixTime), vector.GetFixedAtWithTypeCheck[types.Timestamp](out, 0))
			}
		})
	}
}

func TestLegacyTimestampZeroAdmissionAndReplay(t *testing.T) {
	mock := NewMockOptimizer(false)
	legacy, mode := true, "STRICT_TRANS_TABLES,NO_ZERO_DATE"
	resolve := func(name string, _, _ bool) (interface{}, error) {
		if name == "explicit_defaults_for_timestamp" {
			return !legacy, nil
		}
		if name == "sql_mode" {
			return mode, nil
		}
		return "", nil
	}
	mock.ctxt.ResolveVariableFunc = resolve
	mock.ctxt.GetProcess().SetResolveVariableFunc(resolve)
	build := func(definition string) (*planpb.Plan, error) {
		stmts, err := mysql.Parse(mock.ctxt.GetContext(), "CREATE TABLE timestamp_policy ("+definition+")", 1)
		require.NoError(t, err)
		defer stmts[0].Free()
		return BuildPlan(&mock.ctxt, stmts[0], false)
	}
	for _, definition := range []string{
		"ts TIMESTAMP DEFAULT '0000-00-00 00:00:00'",
		"ts TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
		"first_ts TIMESTAMP, later_ts TIMESTAMP",
		"ts TIMESTAMP NOT NULL DEFAULT NULL",
	} {
		_, err := build(definition)
		require.Error(t, err, definition)
	}
	for _, definition := range []string{"ts TIMESTAMP", "ts TIMESTAMP NULL"} {
		_, err := build(definition)
		require.NoError(t, err, definition)
	}
	mode = "NO_ZERO_DATE" // strictness and zero-date membership are both required
	_, err := build("ts TIMESTAMP ON UPDATE CURRENT_TIMESTAMP")
	require.NoError(t, err)

	// An unchanged persisted absence of a default is a contract too. Replaying
	// a modern NOT NULL declaration under legacy mode must not synthesize one.
	legacy, mode = false, ""
	originalPlan, err := build("ts TIMESTAMP(6) NOT NULL")
	require.NoError(t, err)
	original := originalPlan.GetDdl().GetCreateTable().GetTableDef()
	require.Nil(t, original.Cols[0].Default.Expr)
	legacy = true
	mock.ctxt.SetContext(WithPersistedDDLReplay(mock.ctxt.GetContext(), original, original))
	replayedPlan, err := build("ts TIMESTAMP(6) NOT NULL")
	require.NoError(t, err)
	replayed := replayedPlan.GetDdl().GetCreateTable().GetTableDef().Cols[0]
	require.Nil(t, replayed.Default.Expr)
	require.Nil(t, replayed.OnUpdate)
	require.False(t, replayed.Default.NullAbility)
}
