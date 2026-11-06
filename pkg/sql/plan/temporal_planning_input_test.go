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
	"sync"
	"sync/atomic"
	"testing"

	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestTemporalPlanningInputIsSingleUseAndRootOwned(t *testing.T) {
	root := &tree.Select{}
	other := &tree.Select{}
	intent := TemporalPlanningIntent{ParameterFree: true}
	scope := NewTemporalPlanningInput(root, intent, nil)
	_, _, ok := scope.claim(other)
	require.False(t, ok)
	got, _, ok := scope.claim(root)
	require.True(t, ok)
	require.True(t, got.ParameterFree)
	_, _, ok = scope.claim(root)
	require.False(t, ok)
	var absent *TemporalPlanningInput
	_, _, ok = absent.claim(root)
	require.False(t, ok)

	// Two optimization attempts can share an immutable borrowed AST without
	// sharing a claim; concurrent claims within one attempt still have one owner.
	fresh := NewTemporalPlanningInput(root, intent, nil)
	var successes atomic.Int32
	var wg sync.WaitGroup
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, _, claimed := fresh.claim(root); claimed {
				successes.Add(1)
			}
		}()
	}
	wg.Wait()
	require.EqualValues(t, 1, successes.Load())
	_, _, ok = NewTemporalPlanningInput(root, intent, nil).claim(root)
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

func TestTemporalPlanningInputFollowsRootStatementWrappers(t *testing.T) {
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
			scope := NewTemporalPlanningInput(root, TemporalPlanningIntent{ParameterFree: true}, nil)
			_, err = NewBaseOptimizer(ctx).OptimizeWithTemporalInput(stmt, false, scope)
			require.NoError(t, err)
			_, _, claimed := scope.claim(root)
			require.False(t, claimed, "the actual root builder must consume the one-shot scope")
			other := &tree.Select{}
			mismatch := NewTemporalPlanningInput(other, TemporalPlanningIntent{ParameterFree: true}, nil)
			_, err = BuildPlanWithTemporalInput(ctx, stmt, false, mismatch)
			require.NoError(t, err)
			_, _, claimed = mismatch.claim(other)
			require.True(t, claimed, "a different root must not consume the scope")
			if sql == "select 1" {
				prepared := &tree.PrepareStmt{Name: "temporal_scope", Stmt: root}
				preparedScope := NewTemporalPlanningInput(root, TemporalPlanningIntent{ParameterFree: true}, nil)
				_, err = BuildPlanWithTemporalInput(ctx, prepared, false, preparedScope)
				require.NoError(t, err)
				_, _, claimed = preparedScope.claim(root)
				require.False(t, claimed, "the explicit prepared AST must reach its own optimizer attempt")
				stringScope := NewTemporalPlanningInput(root, TemporalPlanningIntent{ParameterFree: true}, nil)
				_, err = BuildPlanWithTemporalInput(ctx, tree.NewPrepareString("temporal_text", "select 1"), false, stringScope)
				require.NoError(t, err)
				_, _, claimed = stringScope.claim(root)
				require.True(t, claimed, "a separately parsed PREPARE string must not consume another AST's scope")
			}
		})
	}
}

// A resolver's borrowed table belongs to the scan produced from that same
// result, not a coincidentally equal node in a later rewrite or attempt.
func TestTemporalScanBorrowUsesOriginalBinding(t *testing.T) {
	for _, change := range []string{"none", "table", "version", "tag", "replacement"} {
		t.Run(change, func(t *testing.T) {
			builder, source, _ := temporalCBOTraversalFixture(t)
			scan := builder.qry.Nodes[0]
			switch change {
			case "table":
				scan.ObjRef.SchemaName = "different"
			case "version":
				def := *scan.TableDef
				def.Version++
				scan.TableDef = &def
			case "tag":
				scan.BindingTags[0]++
			case "replacement":
				copy := *scan
				builder.qry.Nodes[0] = &copy
			}
			full, borrow, ok := builder.temporalScanBorrow(0)
			require.Equal(t, change == "none", ok)
			if ok {
				require.Same(t, source, borrow.Relation)
				require.Equal(t, uint64(42), full.TblId)
			}
			builder.clearTemporalBorrows()
			require.Empty(t, builder.temporalScanBorrows)
		})
	}
}

func TestTemporalScanBorrowDeclinesDuplicatesAndOverflow(t *testing.T) {
	for _, duplicate := range []bool{true, false} {
		t.Run(map[bool]string{true: "duplicate", false: "overflow"}[duplicate], func(t *testing.T) {
			builder, _, _ := temporalCBOTraversalFixture(t)
			borrow := builder.temporalScanBorrows[0].borrow
			for i := 1; i <= maxTemporalNativeRelations; i++ {
				id := uint64(42 + i)
				if duplicate {
					id = 42
				}
				ref := &ObjectRef{Obj: int64(id), SchemaName: "test", ObjName: fmt.Sprintf("table_%d", i)}
				def := &TableDef{TblId: id}
				scan := &Node{NodeType: planpb.Node_TABLE_SCAN, ObjRef: ref, TableDef: def, BindingTags: []int32{int32(i + 1)}}
				builder.qry.Nodes = append(builder.qry.Nodes, scan)
				builder.rememberTemporalScan(int32(i), ref, def, borrow)
				if duplicate {
					break
				}
				if i == maxTemporalNativeRelations-1 {
					require.Len(t, builder.temporalScanBorrows, maxTemporalNativeRelations)
				}
			}
			require.True(t, builder.temporalBorrowDisabled)
			require.Empty(t, builder.temporalScanBorrows)
			_, _, ok := builder.temporalScanBorrow(0)
			require.False(t, ok)
		})
	}
}
