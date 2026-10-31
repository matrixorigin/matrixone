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

package frontend

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/pb/statsinfo"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"testing"

	pbplan "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/stretchr/testify/require"
)

func TestTemporalNativeFramesPreserveCompletedScopeAndShadowNesting(t *testing.T) {
	compiler := &TxnCompilerContext{}
	outer := plan.NewTemporalPlanningScope(&tree.Select{}, plan.TemporalPlanningIntent{})
	defer outer.Close()
	popOuter := compiler.PushTemporalPlanningScope(outer)
	outerFrame := compiler.temporal.frame
	popNil := compiler.PushTemporalPlanningScope(nil)
	require.Nil(t, compiler.temporal.frame.scope)
	require.True(t, outer.Usable())
	popNil()
	require.Same(t, outerFrame, compiler.temporal.frame)
	query := &plan.Query{}
	outer.SelectQuery(query)
	popOuter()
	require.Nil(t, compiler.temporal.frame)
	require.True(t, outerFrame.completed)
	require.True(t, outer.MatchesQuery(query), "normal pop preserves the completed root until lexical cleanup")
	popOuter()
	require.Zero(t, compiler.temporal.depth)
	outer.Invalidate()
	require.False(t, outer.Usable())
}

func TestTemporalNativeFramesRejectResolveReentryAndNonLIFOPop(t *testing.T) {
	for _, active := range []bool{false, true} {
		t.Run(map[bool]string{false: "non_lifo", true: "active_resolve"}[active], func(t *testing.T) {
			compiler := &TxnCompilerContext{}
			outer := plan.NewTemporalPlanningScope(&tree.Select{}, plan.TemporalPlanningIntent{})
			defer outer.Close()
			incoming := plan.NewTemporalPlanningScope(&tree.Select{}, plan.TemporalPlanningIntent{})
			defer incoming.Close()
			popOuter := compiler.PushTemporalPlanningScope(outer)
			var capture *temporalNativeCapture
			if active {
				capture = compiler.beginTemporalNativeResolve(true)
			}
			popInner := compiler.PushTemporalPlanningScope(incoming)
			if active {
				require.False(t, outer.Usable())
				require.False(t, incoming.Usable())
				popInner()
				capture.finish()
				popOuter()
			} else {
				popOuter()
				popInner()
				require.False(t, outer.Usable())
				require.False(t, incoming.Usable())
			}
			require.Nil(t, compiler.temporal.frame)
			require.Zero(t, compiler.temporal.depth)
		})
	}
}

func TestTemporalNativeContentionRetiresOwnersAndRecovers(t *testing.T) {
	for _, during := range []string{"pop", "finish"} {
		t.Run(during, func(t *testing.T) {
			compiler := &TxnCompilerContext{execCtx: &ExecCtx{}}
			scope := plan.NewTemporalPlanningScope(&tree.Select{}, plan.TemporalPlanningIntent{})
			defer scope.Close()
			pop := compiler.PushTemporalPlanningScope(scope)
			old := compiler.temporal.frame
			capture := compiler.beginTemporalNativeResolve(true)
			if during == "pop" {
				capture.finish()
			}
			compiler.temporal.mu.Lock()
			if during == "pop" {
				pop()
			} else {
				capture.finish()
			}
			compiler.temporal.mu.Unlock()
			require.False(t, scope.Usable())
			require.Zero(t, old.active.Load())
			if during == "finish" {
				pop()
			}
			require.Nil(t, old.binding.Load(), "failed cleanup cannot retain a transaction/ExecCtx")
			scope.Close()
			fresh := plan.NewTemporalPlanningScope(&tree.Select{}, plan.TemporalPlanningIntent{})
			defer fresh.Close()
			restore := compiler.PushTemporalPlanningScope(fresh)
			require.True(t, fresh.Usable(), "contention must not disable every future statement")
			require.Equal(t, 1, compiler.temporal.depth)
			require.NotSame(t, old, compiler.temporal.frame)
			active := compiler.temporal.frame
			pop()
			capture.finish()
			require.Same(t, active, compiler.temporal.frame, "retired callbacks cannot mutate a fresh frame")
			restore()
			require.Nil(t, compiler.temporal.frame)
			require.Zero(t, compiler.temporal.depth)
		})
	}
}

func TestTemporalNativeTextExecuteRetainsSelectedScopeAcrossASTReplacement(t *testing.T) {
	ses, prepared, cw, execCtx := newPreparedExecuteEnv(t, 130)
	defer prepared.Close()
	defer cw.Free()
	prepared.needsRebuild = true
	outer := tree.NewExecute(tree.Identifier(prepared.Name))
	cw.stmt, cw.stmtBorrowed = outer, false
	execCtx.input.isBinaryProtExecute = false
	_, runtimePlan, selected, _, owned, err := initExecuteStmtParam(execCtx, ses, cw, &pbplan.Execute{Name: prepared.Name}, prepared.Name)
	require.NoError(t, err)
	scope := cw.temporalScope
	require.NotNil(t, scope)
	require.True(t, scope.MatchesQuery(runtimePlan.GetQuery()))
	cw.installPreparedExecutionStatement(selected, owned)
	require.Same(t, scope, cw.temporalScope)
	require.True(t, scope.MatchesQuery(runtimePlan.GetQuery()), "the text Compile AST replacement keeps its selected native scope")
	cw.replaceTemporalScope(nil)
	require.False(t, scope.Usable())
}

func TestTemporalNativeSnapshotOverridePreservesCurrentReads(t *testing.T) {
	for _, snapshot := range []*pbplan.Snapshot{nil, {}, {TS: &timestamp.Timestamp{}}} {
		require.False(t, temporalNativeSnapshotOverride(snapshot))
	}
	for _, snapshot := range []*pbplan.Snapshot{
		{TS: &timestamp.Timestamp{PhysicalTime: 1}},
		{Tenant: &pbplan.SnapshotTenant{}},
		{ExtraInfo: &pbplan.SnapshotExtraInfo{}},
	} {
		require.True(t, temporalNativeSnapshotOverride(snapshot))
	}
}

type temporalCaptureRelation struct{ engine.Relation }

func (*temporalCaptureRelation) ValidateTemporalRead(context.Context, engine.TemporalRead) bool {
	return true
}

func (*temporalCaptureRelation) ReadTemporalCostStats(context.Context, engine.TemporalRead, *statsinfo.StatsInfo, string) (engine.TemporalStats, bool, error) {
	return engine.TemporalStats{}, false, nil
}
func (*temporalCaptureRelation) VerifyTemporalIndexPair(context.Context, engine.TemporalRead, engine.TemporalPairCheck, *int) (bool, error) {
	return false, nil
}

func TestTemporalNativeCaptureBindsOrdinaryTransactionAndReleases(t *testing.T) {
	for _, scenario := range []string{"current", "snapshot changed", "snapshot ABA", "cancelled", "excluded", "nested resolve"} {
		t.Run(scenario, func(t *testing.T) {
			f := newViewSchemaOwnedFixture(t)
			f.op.SetSnapshotTS(timestamp.Timestamp{PhysicalTime: 3})
			scope := plan.NewTemporalPlanningScope(&tree.Select{}, plan.TemporalPlanningIntent{})
			defer scope.Close()
			pop := f.parent.PushTemporalPlanningScope(scope)
			defer pop()
			frame := f.parent.temporal.frame
			capture := f.parent.beginTemporalNativeResolve(scenario == "excluded")
			require.NotNil(t, capture)
			if scenario != "excluded" {
				require.True(t, capture.captureOK)
			}
			ctx, cancel := context.WithCancel(f.ctx)
			defer cancel()
			switch scenario {
			case "snapshot changed":
				f.op.SetSnapshotTS(timestamp.Timestamp{PhysicalTime: 4})
			case "snapshot ABA":
				f.op.SetSnapshotTS(timestamp.Timestamp{PhysicalTime: 4})
				f.op.SetSnapshotTS(timestamp.Timestamp{PhysicalTime: 3})
			case "cancelled":
				cancel()
			case "nested resolve":
				nested := f.parent.beginTemporalNativeResolve(false)
				require.NotNil(t, nested)
				nested.finish()
			}
			relation := &temporalCaptureRelation{}
			ref := &pbplan.ObjectRef{Obj: 42, SchemaName: "db", ObjName: "t"}
			full := &pbplan.TableDef{TblId: 42, Name: "t"}
			capture.capture(ctx, relation, ref, full)
			capture.finish()
			capture.finish()
			got, definition, read, ok := scope.NativeRelation(ref, full)
			require.Equal(t, scenario == "current", ok)
			if ok {
				require.Same(t, relation, got)
				require.Same(t, full, definition)
				require.Same(t, f.op, read.Op)
				require.Same(t, f.workspace, read.Stamp.Workspace)
			}
			pop()
			require.Zero(t, frame.active.Load())
			scope.Close()
			require.Nil(t, frame.binding.Load())
		})
	}
}
