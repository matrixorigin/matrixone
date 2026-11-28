// Copyright 2021 - 2026 MatrixOne
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

	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

// The EXACT release-written v4.2.9 pg189 ViewDef bytes captured from the real
// release service (marker-free, never transformed). Regeneration and the
// metadata consumer must keep the released one-level boundary: the complete
// authored shape (the inner WITH, both window levels, the trailing LIMIT and
// the authored ORDER where present) and the actual pg189.tw_src dependency.
const (
	releasePg189VInnerWith = `{"Stmt":"create view pg189.v_innerwith as (with c as (select ts from pg189.tw_src) select count(*) as a from c interval(ts, 1, minute)) interval(ts, 2, minute) limit 5","DefaultDatabase":"","sql_mode":"ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION,NO_ZERO_DATE,NO_ZERO_IN_DATE,ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES","security_type":"DEFINER"}`
	releasePg189VInnerLim  = `{"Stmt":"create view pg189.v_innerlim as (select count(*) as a from pg189.tw_src interval(ts, 1, minute) order by a limit 3) interval(ts, 2, minute) order by a limit 5","DefaultDatabase":"","sql_mode":"ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION,NO_ZERO_DATE,NO_ZERO_IN_DATE,ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES","security_type":"DEFINER"}`
	releasePg189VPlain     = `{"Stmt":"create view pg189.v_plain as (select count(*) as a from pg189.tw_src interval(ts, 1, minute)) interval(ts, 2, minute) limit 5","DefaultDatabase":"","sql_mode":"ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION,NO_ZERO_DATE,NO_ZERO_IN_DATE,ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES","security_type":"DEFINER"}`

	releasePg189VNested = `{"Stmt":"create view pg200.v_nested as ((select count(*) as a from pg200.tw_src interval(ts, 1, minute))) interval(ts, 2, minute) limit 5","DefaultDatabase":"","sql_mode":"ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION,NO_ZERO_DATE,NO_ZERO_IN_DATE,ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES","security_type":"DEFINER"}`
)

func seedReleasePg189TwSrc(t *testing.T, ctx CompilerContext) {
	mc := ctx.(*MockCompilerContext)
	tsType := types.T_timestamp.ToType()
	srcDef := &planpb.TableDef{
		Name:      "tw_src",
		DbName:    "pg189",
		TblId:     4201,
		LogicalId: 4201,
		Cols:      []*planpb.ColDef{{Name: "ts", Typ: makePlan2Type(&tsType)}},
	}
	objRef := &planpb.ObjectRef{DbName: "pg189", ObjName: "tw_src", Obj: 4201}
	qualified := mockQualifiedTableName("pg189", "tw_src")
	mc.objectsByQualifiedName[qualified] = objRef
	mc.tablesByQualifiedName[qualified] = srcDef
	mc.objects["tw_src"] = objRef
	mc.tables["tw_src"] = srcDef
}

func seedReleasePg200TwSrc(t *testing.T, ctx CompilerContext) {
	t.Helper()
	mc := ctx.(*MockCompilerContext)
	tsType := types.T_timestamp.ToType()
	srcDef := &planpb.TableDef{
		Name:      "tw_src",
		DbName:    "pg200",
		TblId:     4202,
		LogicalId: 4202,
		Cols:      []*planpb.ColDef{{Name: "ts", Typ: makePlan2Type(&tsType)}},
	}
	objRef := &planpb.ObjectRef{DbName: "pg200", ObjName: "tw_src", Obj: 4202}
	qualified := mockQualifiedTableName("pg200", "tw_src")
	mc.objectsByQualifiedName[qualified] = objRef
	mc.tablesByQualifiedName[qualified] = srcDef
}

// markedReleaseViewData builds a known-valid marked control from the authentic
// release bytes: the marshaled field set always decodes, and the caller also
// asserts the recognized non-nil marker before relying on it.
func markedReleaseViewData(t testing.TB, source string) string {
	t.Helper()
	var fields map[string]json.RawMessage
	require.NoErrorf(t, json.Unmarshal([]byte(source), &fields), "the marked control source must decode")
	fields["cte_scope_protocol"], _ = json.Marshal(1)
	out, err := json.Marshal(fields)
	require.NoErrorf(t, err, "the marked control must marshal to valid JSON")
	return string(out)
}

func requireRecognizedMarker(t testing.TB, marked string) {
	t.Helper()
	var data ViewData
	require.NoErrorf(t, json.Unmarshal([]byte(marked), &data), "the marked control must decode to view metadata")
	require.NotNilf(t, data.CTEScopeProtocol, "the marked control must carry the recognized non-nil generation marker")
	require.Equalf(t, int64(1), *data.CTEScopeProtocol, "the recognized marker value must decode exactly")
}

func TestRegenerateViewDefinitionPreservesReleaseAuthoredMarkerFreeDefinitions(t *testing.T) {
	ctx := NewMockCompilerContext(false, newPlanTestProcess(t))
	ctx.GetAccountIdFunc = func() (uint32, error) { return 42, nil }
	seedReleasePg189TwSrc(t, ctx)
	seedReleasePg200TwSrc(t, ctx)
	for name, vd := range map[string]string{
		"v_innerwith": releasePg189VInnerWith,
		"v_innerlim":  releasePg189VInnerLim,
		"v_plain":     releasePg189VPlain,
		"v_nested":    releasePg189VNested,
	} {
		regenerated, err := RegenerateViewDefinition(ctx, vd)
		require.NoErrorf(t, err, "case %s: the release-authored marker-free definition must regenerate without the legacy-context rejection", name)
		var inData ViewData
		require.NoErrorf(t, json.Unmarshal([]byte(vd), &inData), "case %s: the captured release ViewDef must decode", name)
		var outData ViewData
		require.NoErrorf(t, json.Unmarshal([]byte(regenerated.TableDef.GetViewSql().GetView()), &outData), "case %s: the regenerated view must decode as ViewData", name)
		// the complete canonical persisted statement equals the authentic release
		// input statement: direct equality over both full CREATE VIEW texts, so
		// any dropped clause, reshaped window, renamed qualifier or scope change
		// fails the oracle; the marker-free input must also stay marker-free.
		require.Equalf(t, inData.Stmt, outData.Stmt, "case %s: the regenerated Stmt must preserve the complete authentic statement", name)
		require.Nilf(t, outData.CTEScopeProtocol, "case %s: the marker-free input must stay marker-free", name)
		// the physical pg189.tw_src dependency must bind on the fixture's own
		// stable object identity, not any generic nonempty dependency list.
		depDB, depID := "pg189", uint64(4201)
		if name == "v_nested" {
			depDB, depID = "pg200", uint64(4202)
		}
		var sawSrcTwSrc bool
		for _, dep := range regenerated.Dependencies {
			if dep.DatabaseName == depDB && dep.RelationName == "tw_src" {
				require.Equalf(t, depID, dep.RelationID, "case %s: the dependency RelationID must resolve from the seeded physical identity", name)
				require.Equalf(t, depID, dep.LogicalID, "case %s: the dependency LogicalID must carry the seeded physical identity", name)
				sawSrcTwSrc = true
			}
		}
		require.Truef(t, sawSrcTwSrc, "case %s: the actual %s.tw_src dependency must bind on the seeded physical identity", name, depDB)
		// the derived output column a is BIGINT, matching the v4.2.9 SHOW
		// COLUMNS receipts of these very release-authored views.
		// the exact visible schema per the v4.2.9 SHOW COLUMNS readbacks: these
		// release views expose EXACTLY one visible output column, named a, of
		// BIGINT. Hidden entries are qualified out first, so a hidden column
		// named a can never satisfy this oracle, and any second visible column
		// (whatever its name) is a hard failure.
		var visibleCt int
		var colA *planpb.ColDef
		for _, c := range regenerated.TableDef.Cols {
			if c.Hidden {
				continue
			}
			visibleCt++
			colA = c
		}
		require.Equalf(t, 1, visibleCt, "case %s: the regenerated view must expose EXACTLY one visible output column (per the v4.2.9 SHOW COLUMNS readback), got %d", name, visibleCt)
		require.NotNilf(t, colA, "case %s: the visible output column must be present", name)
		require.Falsef(t, colA.Hidden, "case %s: the visible output column must not be hidden", name)
		require.Truef(t, strings.EqualFold(colA.Name, "a"), "case %s: the visible output column must be exactly a, got %q", name, colA.Name)
		require.Equalf(t, int32(types.T_int64), colA.Typ.Id, "case %s: the output column a must be BIGINT per the v4.2.9 SHOW COLUMNS readback", name)
		// the persisted catalog record (the stored ViewData written separately by
		// view_regeneration.go's patchPersistedViewMetadata) must carry the same
		// dependency agreement as the returned slice, with the exact seeded
		// physical identity - a missing or wrong stored dependency fails.
		require.Equalf(t, len(regenerated.Dependencies), len(outData.Dependencies), "case %s: the stored Dependencies must agree with the returned slice", name)
		var storedSrcTwSrc bool
		for _, dep := range outData.Dependencies {
			if dep.DatabaseName == depDB && dep.RelationName == "tw_src" {
				require.Equalf(t, depID, dep.RelationID, "case %s: the stored dependency RelationID must also carry the seeded identity", name)
				require.Equalf(t, depID, dep.LogicalID, "case %s: the stored dependency LogicalID must also carry the seeded identity", name)
				storedSrcTwSrc = true
			}
		}
		require.Truef(t, storedSrcTwSrc, "case %s: the stored catalog record must also bind the actual %s.tw_src dependency", name, depDB)
	}
}

// DescribeViewColumns reaches the metadata description consumer (the SHOW
// COLUMNS / view_columns route) through the same released inference boundary:
// the authentic release-authored extra-grouping fixture must describe its
// single BIGINT output column, not fail the duplicate-window validation, and
// the caller owns the returned descriptioncopy alone. A marked definition of
// the same shape keeps the complete binding and surfaces the real error.
func TestDescribeViewColumnsConsumesReleasedExtraGroupingShape(t *testing.T) {
	ctx := NewMockCompilerContext(false, newPlanTestProcess(t))
	ctx.GetAccountIdFunc = func() (uint32, error) { return 42, nil }
	seedReleasePg189TwSrc(t, ctx)
	seedReleasePg200TwSrc(t, ctx)
	createSQL := "create view pg200.v_nested as ((select count(*) as a from pg200.tw_src interval(ts, 1, minute))) interval(ts, 2, minute) limit 5"
	bind := func(def string) {
		ctx.SetContext(WithRestoredViewDefinition(context.Background(), "pg200", "v_nested", def, createSQL))
	}
	// the authentic marker-free capture
	bind(releasePg189VNested)
	desc, err := DescribeViewColumns(ctx, releasePg189VNested)
	require.NoErrorf(t, err, "the released extra-grouping shape must describe without the duplicate-window rejection")
	require.Lenf(t, desc, 1, "exactly one visible output column per the release SHOW COLUMNS readback")
	require.Truef(t, strings.EqualFold(desc[0].Name, "a"), "the single visible output column is a")
	require.Equalf(t, int32(types.T_int64), desc[0].Typ.Id, "the output column a is BIGINT per the release readback")
	// the caller owns the description copy: mutating it must not leak into the
	// source catalog metadata nor the next description.
	desc[0].Typ.Id = int32(types.T_int32)
	again, err := DescribeViewColumns(ctx, releasePg189VNested)
	require.NoError(t, err)
	require.Equalf(t, int32(types.T_int64), again[0].Typ.Id, "the caller's mutation must not corrupt the stored view metadata")
	// the marked/new-view full-binding control: the same shape with the
	// generation marker keeps the complete defining select and surfaces the
	// real duplicate-window error at the description consumer.
	marked := markedReleaseViewData(t, releasePg189VNested)
	requireRecognizedMarker(t, marked)
	bind(marked)
	_, err = DescribeViewColumns(ctx, marked)
	require.ErrorContainsf(t, err, "multiple time window clauses", "a marked definition keeps the complete binding and must genuinely surface the duplicate-window validation")
}

// The frontend restore route (buildViewCreatePlanForRestore) replays the
// stored CREATE VIEW through plan.BuildPlan with the restored marker-free
// definition installed - this witness drives that exact replay context on
// the authentic release-authored extra-grouping fixture: the CREATE plan must
// build without the duplicate-window rejection and bind the seeded physical
// pg200.tw_src identity inside the built plan, while the marked definition
// keeps the complete binding and surfaces the real validation error.
func TestBuildPlanRestoreReplayContextAcceptsReleasedExtraGroupingShape(t *testing.T) {
	ctx := NewMockCompilerContext(false, newPlanTestProcess(t))
	ctx.GetAccountIdFunc = func() (uint32, error) { return 42, nil }
	seedReleasePg189TwSrc(t, ctx)
	seedReleasePg200TwSrc(t, ctx)
	createSQL := "create view pg200.v_nested as ((select count(*) as a from pg200.tw_src interval(ts, 1, minute))) interval(ts, 2, minute) limit 5"
	ctx.SetContext(WithRestoredViewDefinition(context.Background(), "pg200", "v_nested", releasePg189VNested, createSQL))
	stmt := p132ParseOne(t, createSQL)

	p, err := BuildPlan(ctx, stmt, false, nil)
	require.NoErrorf(t, err, "the restore replay context must plan the released extra-grouping shape")
	require.NotNilf(t, p.GetDdl(), "the restore replay must build the CREATE VIEW DDL plan")
	require.NotNilf(t, p.GetDdl().GetCreateView(), "the plan must carry the create-view payload")
	tdef := p.GetDdl().GetCreateView().GetTableDef()
	require.NotNilf(t, tdef, "the built view table definition must exist on the replay route")
	require.Lenf(t, tdef.Cols, 1, "the replay-bound view definition has exactly one visible output column")
	require.Truef(t, strings.EqualFold(tdef.Cols[0].Name, "a"), "the replay-bound column is a")
	require.Equalf(t, int32(types.T_int64), tdef.Cols[0].Typ.Id, "the replay-bound column a is BIGINT per the release readback")
	// the marked full-binding control on the same replay route.
	marked := markedReleaseViewData(t, releasePg189VNested)
	requireRecognizedMarker(t, marked)
	ctx.SetContext(WithRestoredViewDefinition(context.Background(), "pg200", "v_nested", marked, createSQL))
	_, err = BuildPlan(ctx, stmt, false, nil)
	require.ErrorContainsf(t, err, "multiple time window clauses", "a marked definition keeps the complete binding and must genuinely surface the duplicate-window validation")
}

// The CLUSTER BY bare-END materialization: both unquoted forms plan through
// the DDL binder and materialize the cluster key on the built table
// definition exactly like the quoted spelling and the ordinary column. The
// AST assertion alone is parse coverage; this drives the real planner.
func TestClusterByBareEndMaterializesThroughThePlanner(t *testing.T) {
	for name, sql := range map[string]string{
		"bare_end_parenthesized": "CREATE TABLE t (id INT, end INT) CLUSTER BY (end)",
		"bare_end_single":        "CREATE TABLE t (id INT, end INT) CLUSTER BY end",
		"quoted_end_control":     "CREATE TABLE t (id INT, `end` INT) CLUSTER BY (`end`)",
		"ordinary_control":       "CREATE TABLE t (id INT, end INT) CLUSTER BY (id)",
	} {
		t.Run(name, func(t *testing.T) {
			ctx := NewMockCompilerContext(false, newPlanTestProcess(t))
			ctx.GetAccountIdFunc = func() (uint32, error) { return 42, nil }
			stmt := p132ParseOne(t, sql)

			p, err := BuildPlan(ctx, stmt, false, nil)
			require.NoErrorf(t, err, "expected %q to plan through the DDL binder", sql)
			require.NotNilf(t, p, "a built plan must exist for %q", sql)
			require.NotNilf(t, p.GetDdl(), "the plan must be a DDL plan for %q", sql)
			require.NotNilf(t, p.GetDdl().GetCreateTable(), "the plan must carry the create-table payload for %q", sql)
			tdef := p.GetDdl().GetCreateTable().GetTableDef()
			require.NotNilf(t, tdef, "the built table definition must exist for %q", sql)
			require.NotNilf(t, tdef.ClusterBy, "the planner must materialize the cluster keys for %q", sql)
			want := "end"
			if name == "ordinary_control" {
				want = "id"
			}
			require.Equalf(t, want, tdef.ClusterBy.GetName(), "the ClusterByDef name is the materialized owner for %q", sql)
			var sawFlag bool
			for _, col := range tdef.Cols {
				if strings.EqualFold(col.Name, want) {
					require.Truef(t, col.ClusterBy, "the expected column must carry the materialized ClusterBy flag for %q", sql)
					sawFlag = true
				}
			}
			require.Truef(t, sawFlag, "the declared %s column must be present and flagged as the cluster owner for %q", want, sql)
		})
	}
}
