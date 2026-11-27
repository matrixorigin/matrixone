// Copyright 2021 MatrixOrigin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	pbplan "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

// Reviews 6068118621/6068411871, finding 1: a marker-free released view whose
// body keeps nested parenthesized time windows infers like the released
// CREATE boundary - the immediate parenthesized child owns the effective
// window, and the deeper peeled window is neither adopted nor rejected as a
// duplicate; a marked current-generation definition keeps the whole-statement
// validation instead.
func TestReleasedMarkerFreeNestedWindowInferenceBindsMiddleOwner(t *testing.T) {
	createSQL := "create view pg189.v_triple as ((select count(*) as a from pg189.tw_src interval(ts, 1, minute)) interval(ts, 2, minute) order by a) interval(ts, 3, minute) limit 5"
	def := `{"Stmt":"` + createSQL + `","DefaultDatabase":"pg189","sql_mode":"","security_type":"DEFINER"}`
	ctx := NewMockCompilerContext(false, newPlanTestProcess(t))
	ctx.SetContext(context.Background())
	seedReleasePg189TwSrc(t, ctx)
	viewDef := &TableDef{
		Name: "v_triple", DbName: "pg189", DbId: 7, TblId: 4022, LogicalId: 4022, Version: 5,
		TableType: catalog.SystemViewRel,
		ViewSql:   &pbplan.ViewDef{View: def},
		Cols:      []*ColDef{{Name: "stale", Typ: pbplan.Type{Id: int32(types.T_int8)}}},
	}
	q := mockQualifiedTableName("pg189", "v_triple")
	ctx.tablesByQualifiedName[q] = viewDef
	ctx.objectsByQualifiedName[q] = &ObjectRef{DbName: "pg189", ObjName: "v_triple", Obj: 4022, SchemaName: "pg189"}
	ctx.SetContext(WithRestoredViewDefinition(context.Background(), "pg189", "v_triple", def, createSQL))
	var data ViewData
	require.NoError(t, json.Unmarshal([]byte(def), &data))
	stmt, perr := mysql.ParseOne(context.Background(), data.Stmt, 1)
	require.NoErrorf(t, perr, "the reviewers' triple-window CTAS body must parse")
	cv, okCv := stmt.(*tree.CreateView)
	require.Truef(t, okCv, "the persisted statement is the CREATE VIEW")
	inner := restoredLegacyInnerWindowInferenceInput(ctx, "pg189", "v_triple", cv.AsSource)
	require.Samef(t, cv.AsSource.Select.(*tree.ParenSelect).Select, inner,
		"the inference input is the immediate parenthesized child, matching the released boundary")
	// Semantic ownership beyond pointer identity: the selected inference
	// input's OWN time window is the released two-minute middle ownership,
	// not the peeled inner one-minute or the outer three-minute wrapper.
	require.NotNilf(t, inner.TimeWindow, "the selected inference input carries the immediate middle time window")
	windowCtx := tree.NewFmtCtx(dialect.MYSQL)
	inner.TimeWindow.Format(windowCtx)
	require.Equalf(t, "interval(ts, 2, minute)", strings.ToLower(windowCtx.String()),
		"the immediate middle SELECT owns the two-minute window in the actual inference boundary")
	cols, derr := DescribeViewColumns(ctx, def)
	require.NoErrorf(t, derr, "the marker-free describe must bind like the released CREATE inference")
	require.Lenf(t, cols, 1, "one visible output column")
	require.Truef(t, colNameEqualFold(cols[0], "a"), "the visible column is the count alias a")
	regen, rerr := RegenerateViewDefinition(ctx, def)
	require.NoErrorf(t, rerr, "the marker-free regeneration succeeds without the duplicate-window rejection")
	require.NotNilf(t, regen, "the regenerated definition is real")
	// A marked current-generation definition on the same shape keeps the
	// complete-statement validation surface.
	marked := markedReleaseViewData(t, def)
	requireRecognizedMarker(t, marked)
	markedCtx := NewMockCompilerContext(false, newPlanTestProcess(t))
	markedCtx.SetContext(context.Background())
	_, mderr := DescribeViewColumns(markedCtx, marked)
	require.ErrorContainsf(t, mderr, "multiple time window clauses",
		"a marked definition keeps the whole-statement duplicate-window validation")
}

func colNameEqualFold(col *plan.ColDef, name string) bool {
	if col == nil {
		return false
	}
	return (col.Name == name)
}

// Reviews 6068118621/6068411871, finding 2: the merge-base child-only CTAS
// source without an enclosing WITH stays accepted on both the regular and the
// prepared inference route, while a WITH-bearing source keeps its complete
// lexical scope and resolves the declared CTE.
func TestChildOnlyCtasSourceWithoutWithStaysAcceptedOnPrepareAndRegularRoutes(t *testing.T) {
	ctx := NewMockCompilerContext(false, newPlanTestProcess(t))
	ctx.SetContext(context.Background())
	sql := "create table pg189.ctas_page as (select 7 as a limit 1) limit 1"
	stmt, perr := mysql.ParseOne(context.Background(), sql, 1)
	require.NoErrorf(t, perr, "the child-only CTAS syntax parses")
	ct := stmt.(*tree.CreateTable)
	// Production selects the merge-base child-only boundary exactly ONCE in
	// buildCreateTable before inference (and once more on the frontend's own
	// privilege copy). Mirror that single selection here.
	for _, prepared := range []bool{false, true} {
		cols, _, _, gerr := genAsSelectCols(ctx, ReleasedChildOnlyCtasSource(ct.AsSource), prepared, nil)
		require.NoErrorf(t, gerr, "child-only CTAS source inference route prepared=%v", prepared)
		require.Lenf(t, cols, 1, "one projected column route prepared=%v", prepared)
		require.Truef(t, colNameEqualFold(cols[0], "a"), "the projected column is a (prepared=%v)", prepared)
	}
	// The enclosing-WITH shape keeps the complete source: the declared CTE
	// resolves, so a same-name physical table can never answer the projection.
	seedReleasePg189TwSrc(t, ctx)
	withSQL := "create table pg189.ctas_with as with w as (select 99 as a) (select a from w) limit 1"
	wstmt, werr := mysql.ParseOne(context.Background(), withSQL, 1)
	require.NoErrorf(t, werr, "the enclosing-WITH CTAS syntax parses")
	wct := wstmt.(*tree.CreateTable)
	wcols, _, _, wgerr := genAsSelectCols(ctx, ReleasedChildOnlyCtasSource(wct.AsSource), false, nil)
	require.NoErrorf(t, wgerr, "the enclosing-WITH CTAS source keeps its whole lexical scope")
	require.Lenf(t, wcols, 1, "the WITH projection has one column")
	require.Truef(t, colNameEqualFold(wcols[0], "a"), "the WITH projection column is a")
}

// Reviews 6068118621/6068411871, double-selection regression: a nested
// no-WITH parenthesized CTAS source keeps exactly ONE shared merge-base
// boundary. The conflicting-limits shape discriminates one peel from two:
// binding the immediate middle wrapper surfaces the genuine inner/middle
// duplicate at the real plan build, while a re-peel would hide it behind the
// deepest single-limit child. The no-conflict shape stays accepted exactly as
// fresh main selects it.
func TestReleasedChildOnlyCtasNestedSourceSelectsOneImmediateChild(t *testing.T) {
	ctx := NewMockCompilerContext(false, newPlanTestProcess(t))
	ctx.SetContext(context.Background())
	// Production selects the shared child-only boundary exactly once before
	// the inference bind (buildCreateTable for the plan; the same boundary on
	// the frontend privilege copy). Mirror that single selection and drive the
	// real inference builder. The from-less bodies keep exactly the peel
	// semantics under test; the physical-source variants of the same shapes
	// run in the embedded public PREPARE regression.
	conflictStmt, perr := mysql.ParseOne(context.Background(),
		"create table pg189.nested_conflict as ((select 7 as a limit 5) limit 3)", 1)
	require.NoErrorf(t, perr, "the conflicting nested CTAS source parses")
	conflictCt := conflictStmt.(*tree.CreateTable)
	_, _, _, cerr := genAsSelectCols(ctx, ReleasedChildOnlyCtasSource(conflictCt.AsSource), false, nil)
	require.ErrorContainsf(t, cerr, "multiple LIMIT clauses not allowed",
		"one shared selection binds the immediate middle wrapper and surfaces the genuine inner/middle duplicate; a second peel would hide it behind the deepest child")
	acceptedStmt, aerr := mysql.ParseOne(context.Background(),
		"create table pg189.nested_ok as ((select 7 as a) limit 3)", 1)
	require.NoErrorf(t, aerr, "the accepted nested control parses")
	acceptedCt := acceptedStmt.(*tree.CreateTable)
	acceptedCols, _, _, abindErr := genAsSelectCols(ctx, ReleasedChildOnlyCtasSource(acceptedCt.AsSource), false, nil)
	require.NoErrorf(t, abindErr, "the no-conflict nested source binds through the one immediate child")
	require.Lenf(t, acceptedCols, 1, "the nested accepted control projects one column")
	require.Truef(t, colNameEqualFold(acceptedCols[0], "a"), "the nested accepted control projects the single literal column")
}
