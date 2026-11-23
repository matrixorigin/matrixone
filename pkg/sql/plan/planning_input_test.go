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

func TestPlanningInputIsSingleUseAndRootOwned(t *testing.T) {
	stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, "select 1 from nation", 1)
	require.NoError(t, err)
	defer stmt.Free()
	root := stmt.(*tree.Select)
	other := &tree.Select{Select: root.Select}
	intent := ExecutionIntent{}
	scope := NewPlanningInput(root, intent, nil)
	require.False(t, scope.claim(other))
	require.True(t, scope.claim(root))
	require.False(t, scope.claim(root))
	var absent *PlanningInput
	require.False(t, absent.claim(root))

	// Two optimization attempts can share an immutable borrowed AST without
	// sharing a claim; concurrent claims within one attempt still have one owner.
	fresh := NewPlanningInput(root, intent, nil)
	var successes atomic.Int32
	var wg sync.WaitGroup
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if fresh.claim(root) {
				successes.Add(1)
			}
		}()
	}
	wg.Wait()
	require.EqualValues(t, 1, successes.Load())
	require.True(t, NewPlanningInput(root, intent, nil).claim(root))
	for _, sql := range []string{"select ?", "select now(?)", "select 1 where exists (select ?)"} {
		stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, sql, 1)
		require.NoError(t, err)
		func() {
			defer stmt.Free()
			require.Nil(t, NewPlanningInput(stmt.(*tree.Select), intent, nil), sql)
		}()
	}
}

func TestTemporalIntentRejectsSessionLimitDrift(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	intent := ExecutionIntent{TimeZone: proc.GetSessionInfo().TimeZone, UnixTime: proc.Base.UnixTime}
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

func TestPlanningInputFollowsRootStatementWrappers(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	ctx := NewMockCompilerContext(true, proc)
	ctx.SetContext(proc.Ctx)
	for _, sql := range []string{"select 1 from nation", "(select 1 from nation)", "explain select 1 from nation", "explain analyze select 1 from nation"} {
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
			scope := NewPlanningInput(root, ExecutionIntent{}, nil)
			_, err = NewBaseOptimizer(ctx).Optimize(stmt, false, scope)
			require.NoError(t, err)
			require.False(t, scope.claim(root), "the actual root builder must consume the one-shot scope")
			other := &tree.Select{Select: root.Select}
			mismatch := NewPlanningInput(other, ExecutionIntent{}, nil)
			_, err = BuildPlan(ctx, stmt, false, mismatch)
			require.NoError(t, err)
			require.True(t, mismatch.claim(other), "a different root must not consume the scope")
		})
	}
}

// A resolver's borrowed table belongs to the scan produced from that same
// result, not a coincidentally equal node in a later rewrite or attempt.
func TestTemporalScanBorrowUsesOriginalBinding(t *testing.T) {
	for _, change := range []string{"none", "table", "version", "tag", "replacement"} {
		t.Run(change, func(t *testing.T) {
			builder, source, _ := newIndexCostFixture(t)
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
			full, borrow, ok := builder.scanBorrow(0)
			require.Equal(t, change == "none", ok)
			if ok {
				require.Same(t, source, borrow.Relation)
				require.Equal(t, uint64(42), full.TblId)
			}
			builder.clearScanBorrows()
			require.Empty(t, builder.scanBorrows)
		})
	}
}

func TestTemporalScanBorrowDeclinesDuplicatesAndOverflow(t *testing.T) {
	for _, duplicate := range []bool{true, false} {
		t.Run(map[bool]string{true: "duplicate", false: "overflow"}[duplicate], func(t *testing.T) {
			builder, _, _ := newIndexCostFixture(t)
			input := builder.planningInput
			borrow := builder.scanBorrows[0].borrow
			for i := 1; i <= maxBorrowedRelations; i++ {
				id := uint64(42 + i)
				if duplicate {
					id = 42
				}
				ref := &ObjectRef{Obj: int64(id), SchemaName: "test", ObjName: fmt.Sprintf("table_%d", i)}
				def := &TableDef{TblId: id}
				scan := &Node{NodeType: planpb.Node_TABLE_SCAN, ObjRef: ref, TableDef: def, BindingTags: []int32{int32(i + 1)}}
				builder.qry.Nodes = append(builder.qry.Nodes, scan)
				builder.rememberScanBorrow(int32(i), ref, def, borrow)
				if duplicate {
					break
				}
				if i == maxBorrowedRelations-1 {
					require.Len(t, builder.scanBorrows, maxBorrowedRelations)
				}
			}
			require.True(t, builder.borrowResolutionDisabled)
			require.Empty(t, builder.scanBorrows)
			require.Same(t, input, builder.planningInput)
			require.Same(t, builder.compCtx, builder.borrowingCompiler())
			_, _, ok := builder.scanBorrow(0)
			require.False(t, ok)
			builder.qry.Nodes = builder.qry.Nodes[:1]
			builder.qry.Steps = nil
			builder.qry.ExecutionDependentAccess = false
			builder.collectOriginalIndexOwner(0)
			require.True(t, builder.qry.ExecutionDependentAccess)
			require.True(t, builder.originalIndexScan.safe)
		})
	}
}

func TestPlanningInputSkipsOnlyProvedScanFreeRoots(t *testing.T) {
	for _, tc := range []struct {
		sql      string
		possible bool
	}{
		{"select 1", false}, {"select 'literal',null", false}, {"select 1 from nation", true},
		{"select 1 where exists(select 1 from nation)", true}, {"select (select 1 from nation)", true},
		{"with n as (select 1 from nation) select 1", true}, {"select ?", true},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, tc.sql, 1)
			require.NoError(t, err)
			defer stmt.Free()
			require.Equal(t, tc.possible, PlanningInputMayUseIndex(stmt.(*tree.Select)))
			if !tc.possible {
				require.Nil(t, NewPlanningInput(stmt.(*tree.Select), ExecutionIntent{}, nil))
			}
		})
	}
}
