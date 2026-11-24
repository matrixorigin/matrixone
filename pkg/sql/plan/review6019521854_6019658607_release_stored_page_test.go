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
	"encoding/json"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

// Reviews 6019521854/6019658607: a release-authored (marker-free) stored view
// rebinds with the released page interpretation - every enclosing page of the
// stored body is discarded, so the plan carries no applied outer stage - while
// a definition recorded with the current generation applies its pages inside
// out and the optimized plan carries the stage. The OFFSET clause belongs to
// the enclosing stage alone in this witness, which makes its presence in the
// optimized plan a faithful discriminator of the applied stage.
func TestReleaseStoredPageGeneration(t *testing.T) {
	viewBody := "create view v_page as with q as ((select 1 as a union all select 2 as a order by a limit 2) limit 1 offset 1) select a from q"
	for _, tc := range []struct {
		name       string
		generation *int64
	}{
		{"release-authored marker-free definition discards the enclosing page", nil},
		{"current-generation recorded definition applies the enclosing page", func() *int64 { v := int64(1); return &v }()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			compiler := NewMockCompilerContext(true, newPlanTestProcess(t))
			t.Cleanup(compiler.GetProcess().Free)
			data, err := json.Marshal(ViewData{Stmt: viewBody, DefaultDatabase: "db", SecurityType: "DEFINER", CTEScopeProtocol: tc.generation})
			require.NoError(t, err)
			compiler.tables["v_page"] = &TableDef{
				Name: "v_page", DbName: "db", TableType: catalog.SystemViewRel,
				ViewSql: &planpb.ViewDef{View: string(data)},
				Cols:    []*ColDef{{Name: "a", Typ: planpb.Type{Id: 24, Width: 64}}},
			}
			compiler.objects["v_page"] = &ObjectRef{SchemaName: "db", ObjName: "v_page", Obj: int64(701)}
			optimizer := &MockOptimizer{ctxt: *compiler}
			logicPlan, err := runOneStmt(optimizer, t, fmt.Sprintf("select a from v_page order by a"))
			require.NoError(t, err)
			t.Logf("nodes: %s", logicPlan.GetQuery().String())
			var stage *planpb.Node
			for _, node := range logicPlan.GetQuery().Nodes {
				if node == nil || node.Offset == nil {
					continue
				}
				stage = node
			}
			if tc.generation == nil {
				require.Nil(t, stage, "the release interpretation discards the enclosing page of a marker-free definition")
			} else {
				require.NotNil(t, stage, "the recorded generation keeps the enclosing page applied inside out")
			}
		})
	}

}

// TestWrapperGuardDeclinesAuthoredLockState proves at the planner level what
// the SQL-level FOR UPDATE shape cannot reach past the parser: a constructed
// parenthesized wrapper that carries authored lock state never enters the
// page reconstruction - the whole expression returns to the normal binder,
// so nothing may silently strip an authored clause.
func TestWrapperGuardDeclinesAuthoredLockState(t *testing.T) {
	// The two real page stages: the content owns an ORDER BY page and the
	// wrapper carries its own enclosing ORDER BY page, so the unspecialized
	// control is definitely REWITTEN by the wrap - only the authored lock
	// state distinguishes the declining branch.
	sortExpr := tree.NewUnresolvedName(tree.NewCStr("v", 1))
	buildWrapper := func(lock *tree.SelectLockInfo) *tree.Select {
		inner := &tree.Select{
			Select:  &tree.SelectClause{},
			OrderBy: tree.OrderBy{&tree.Order{Expr: tree.NewUnresolvedName(tree.NewCStr("inner", 1))}},
		}
		_ = sortExpr
		return &tree.Select{
			Select:         &tree.ParenSelect{Select: inner},
			OrderBy:        tree.OrderBy{&tree.Order{Expr: sortExpr}},
			SelectLockInfo: lock,
		}
	}
	rewritten, _, _, ok := ordinaryPaginationWrap(buildWrapper(nil), false)
	require.True(t, ok, "the unspecialized wrapper with real pages must be rewritten")
	require.NotNil(t, rewritten, "the rewritten control must synthesize its page tree")

	locked := buildWrapper(&tree.SelectLockInfo{})
	rewrittenLocked, _, _, okLocked := ordinaryPaginationWrap(locked, false)
	require.False(t, okLocked, "an authored lock state on a wrapper must decline the page reconstruction")
	require.Nil(t, rewrittenLocked, "the declined rewrite must not synthesize a replacement tree")
	require.NotNil(t, locked.SelectLockInfo, "the authored lock state must stay on the original wrapper")
}
