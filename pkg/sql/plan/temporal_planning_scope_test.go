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
	"sync"
	"sync/atomic"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestTemporalPlanningScopeIsSingleUseAndRootOwned(t *testing.T) {
	root := &tree.Select{}
	other := &tree.Select{}
	intent := TemporalPlanningIntent{ParameterFree: true}
	scope := NewTemporalPlanningScope(root, intent)
	_, ok := scope.claim(other)
	require.False(t, ok)
	got, ok := scope.claim(root)
	require.True(t, ok)
	require.True(t, got.ParameterFree)
	_, ok = scope.claim(root)
	require.False(t, ok)
	var absent *TemporalPlanningScope
	_, ok = absent.claim(root)
	require.False(t, ok)

	// Two optimization attempts can share an immutable borrowed AST without
	// sharing a claim; concurrent claims within one attempt still have one owner.
	fresh := NewTemporalPlanningScope(root, intent)
	var successes atomic.Int32
	var wg sync.WaitGroup
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, claimed := fresh.claim(root); claimed {
				successes.Add(1)
			}
		}()
	}
	wg.Wait()
	require.EqualValues(t, 1, successes.Load())
	_, ok = NewTemporalPlanningScope(root, intent).claim(root)
	require.True(t, ok)
}

func TestTemporalIntentRejectsSessionLimitDrift(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	intent := TemporalPlanningIntent{TimeZone: proc.GetSessionInfo().TimeZone, UnixTime: proc.Base.UnixTime}
	proc.Base.SessionInfo.ApplySQLSelectLimit = true
	proc.SetResolveVariableFunc(nil)
	require.False(t, intent.MatchesProcess(proc), "unknown session limits cannot prove a full scan")
	limit := ^uint64(0)
	proc.SetResolveVariableFunc(func(name string, _, _ bool) (any, error) {
		require.Equal(t, SQLSelectLimitVariable, name)
		return limit, nil
	})
	require.True(t, intent.MatchesProcess(proc))
	limit = 1
	require.False(t, intent.MatchesProcess(proc), "a limit installed after planning invalidates the execution proof")
}

func TestTemporalPlanningScopeFollowsRootStatementWrappers(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	ctx := NewMockCompilerContext(true, proc)
	ctx.SetContext(proc.Ctx)
	for _, sql := range []string{"select 1", "(select 1)", "explain select 1", "explain analyze select 1"} {
		t.Run(sql, func(t *testing.T) {
			stmt, err := parsers.ParseOne(proc.Ctx, dialect.MYSQL, sql, 1)
			require.NoError(t, err)
			defer stmt.Free()
			var root *tree.Select
			switch s := stmt.(type) {
			case *tree.Select:
				root = s
			case *tree.ParenSelect:
				root = s.Select
			case *tree.ExplainStmt:
				root, _ = s.Statement.(*tree.Select)
			case *tree.ExplainAnalyze:
				root, _ = s.Statement.(*tree.Select)
			}
			require.NotNil(t, root)
			scope := NewTemporalPlanningScope(root, TemporalPlanningIntent{ParameterFree: true})
			_, err = NewBaseOptimizer(ctx).OptimizeWithTemporalScope(stmt, false, scope)
			require.NoError(t, err)
			_, claimed := scope.claim(root)
			require.False(t, claimed, "the actual root builder must consume the one-shot scope")
			other := &tree.Select{}
			mismatch := NewTemporalPlanningScope(other, TemporalPlanningIntent{ParameterFree: true})
			_, err = BuildPlanWithTemporalScope(ctx, stmt, false, mismatch)
			require.NoError(t, err)
			_, claimed = mismatch.claim(other)
			require.True(t, claimed, "a different root must not consume the scope")
			if sql == "select 1" {
				prepared := &tree.PrepareStmt{Name: "temporal_scope", Stmt: root}
				preparedScope := NewTemporalPlanningScope(root, TemporalPlanningIntent{ParameterFree: true})
				_, err = BuildPlanWithTemporalScope(ctx, prepared, false, preparedScope)
				require.NoError(t, err)
				_, claimed = preparedScope.claim(root)
				require.False(t, claimed, "the explicit prepared AST must reach its own optimizer attempt")
				stringScope := NewTemporalPlanningScope(root, TemporalPlanningIntent{ParameterFree: true})
				_, err = BuildPlanWithTemporalScope(ctx, tree.NewPrepareString("temporal_text", "select 1"), false, stringScope)
				require.NoError(t, err)
				_, claimed = stringScope.claim(root)
				require.True(t, claimed, "a separately parsed PREPARE string must not consume another AST's scope")
			}
		})
	}
}

// A relation is borrowed from ordinary resolution; closing the scope releases
// its references without invoking any method on the relation itself.
func TestTemporalPlanningScopeBorrowsCanonicalRelation(t *testing.T) {
	builder, relation, _ := temporalCBOTraversalFixture(t)
	scope := builder.temporalPlanningScope
	scan := builder.qry.Nodes[0]
	got, full, read, ok := scope.NativeRelation(scan.ObjRef, scan.TableDef)
	require.True(t, ok)
	require.Same(t, relation, got)
	require.Same(t, scan.TableDef, full)
	require.Same(t, builder.compCtx.GetProcess().GetTxnOperator(), read.Op)
	wrong := DeepCopyObjectRef(scan.ObjRef)
	wrong.SchemaName = "another_database"
	_, _, _, ok = scope.NativeRelation(wrong, scan.TableDef)
	require.False(t, ok)
	scope.Close()
	require.Zero(t, scope.sourceCount)
	for _, record := range scope.sources {
		require.Nil(t, record.relation)
		require.Nil(t, record.full)
		require.Nil(t, record.read.Op)
	}
	_, _, _, ok = scope.NativeRelation(scan.ObjRef, scan.TableDef)
	require.False(t, ok)
	scope.Close()
}

func TestTemporalPlanningScopeDuplicateRelationDeclines(t *testing.T) {
	builder, relation, _ := temporalCBOTraversalFixture(t)
	scope := builder.temporalPlanningScope
	scan := builder.qry.Nodes[0]
	read := engine.TemporalRead{Op: builder.compCtx.GetProcess().GetTxnOperator(), Stamp: client.CatalogReadStamp{Snapshot: timestamp.Timestamp{PhysicalTime: 1}, Workspace: &temporalCBOTestWorkspace{}}}
	require.False(t, scope.AddNativeRelation(scan.ObjRef, scan.TableDef, relation, read))
	require.False(t, scope.Usable())
}
