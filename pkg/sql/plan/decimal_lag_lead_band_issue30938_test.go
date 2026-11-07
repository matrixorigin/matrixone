// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/stretchr/testify/require"
)

// The LAG/LEAD expansion band (issue #30938, design decision log 82,
// F-LAGLEAD-PARENT-GATE-ADMISSION) admits lag/lead decimal default merges
// whose parent-domain derivation the pre-change engine rejects at planning
// (derived width 66-76). Under the supported shutdown-only compatibility
// model (decision log 86 (ii), F-SHUTDOWN-UPGRADE-COMPATIBILITY) this change
// mints NO issue-specific ViewData.RequiredProtocolVersion marker, floor or
// fence, and every authored record carries exactly the requirement the
// unchanged pre-existing channel computes for the same statement — the v89
// wide-literal requirement included — with absence holding only where no
// existing feature requires a marker (decision log 87 (i),
// F-PRESERVE-EXISTING-MARKERS). Parent-supported shapes — an absent, NULL or
// parent-matching default, a no-default form, or a genuine wide match —
// persist parent-identical records.

// buildLagLeadBandView binds one CREATE VIEW statement against the mock
// compiler context and returns the DDL plan.
func buildLagLeadBandView(t *testing.T, ctx *MockCompilerContext, sql string) (*Plan, error) {
	t.Helper()
	stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, sql, 1)
	require.NoError(t, err)
	defer stmt.Free()
	return BuildPlan(&rootSQLCompilerContext{MockCompilerContext: ctx, rootSQL: sql}, stmt, false)
}

// buildLagLeadBandSelect binds one ordinary SELECT against the mock context.
func buildLagLeadBandSelect(t *testing.T, ctx *MockCompilerContext, sql string) (*Plan, error) {
	t.Helper()
	stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, sql, 1)
	require.NoError(t, err)
	defer stmt.Free()
	return BuildPlan(ctx, stmt, false)
}

// lagLeadBandViewMarker decodes the persisted view record's protocol marker.
func lagLeadBandViewMarker(t *testing.T, p *Plan) *int64 {
	t.Helper()
	var data ViewData
	require.NoError(t, json.Unmarshal(
		[]byte(p.GetDdl().GetCreateView().GetTableDef().GetViewSql().GetView()), &data))
	return data.RequiredProtocolVersion
}

// registerLagLeadBandView installs a built view record in the mock catalog
// so a later statement resolves and expands it through bindView.
func registerLagLeadBandView(ctx *MockCompilerContext, name string, tableDef *plan.TableDef) {
	def := DeepCopyTableDef(tableDef, true)
	// The built CREATE VIEW record does not carry the catalog rel kind, and
	// the binder routes a definition into bindView only for SystemViewRel
	// (query_builder.go buildTable); every suite view registration sets it.
	def.TableType = catalog.SystemViewRel
	ctx.tables[name] = def
	ctx.objects[name] = &plan.ObjectRef{SchemaName: "tpch", ObjName: name}
}

// lagLeadBandShapes are the witness expressions shared by the band suites.
var (
	lagLeadBandMul = "cast(n_nationkey as decimal(38,0)) * cast(n_nationkey as decimal(38,0))"
	lagLeadNines76 = strings.Repeat("9", 76)
	lagLeadNines66 = strings.Repeat("9", 66)
	// A 70-digit fractional literal: 0.000...1 with 69 zeros.
	lagLeadFraction70 = "0." + strings.Repeat("0", 69) + "1"
)

func TestLagLeadExpansionBandMarkerPreservation(t *testing.T) {
	mul := lagLeadBandMul
	nines76 := lagLeadNines76
	nines66 := lagLeadNines66
	fraction70 := lagLeadFraction70
	// A plain decimal division fires the pre-existing DecimalDivisionSemantics
	// feature whose channel floor is v97 (base string_literal_form.go:1118 and
	// persisted_ip.go:176-177), above the wide literal's v89.
	decimalDivision := "cast(n_nationkey as decimal(38,10)) / cast(1 as decimal(38,10))"

	for _, tc := range []struct {
		name        string
		selectExpr  string
		extraSelect string
		wantMarker  int64
		wantWidth   int32
		wantScale   int32
		wantErr     string
	}{
		{
			// (a) INHERITED-NONZERO-MARKER cell: GATE-DISAPPEARANCE — the
			// widened publication (76,0) equals the default's type, so the
			// surface gate never runs; the parent's own gate over (65,0) vs
			// (76,0) rejects the derivation at planning, which makes the
			// shape newly admitted band behavior. The record persists (76,0)
			// WITH the v89 requirement the unchanged channel computes for the
			// N76 literal (decimalLiteralRequiresV82, base make.go:309-311).
			name:       "gate disappearance inherits the literal v89 requirement",
			selectExpr: "lag(" + mul + ", 0, " + nines76 + ") over ()",
			wantMarker: defines.MORPCVersion89,
			wantWidth:  76,
			wantScale:  0,
		},
		{
			// (c) ABSENCE cell: the retry witness's surface reconcile over
			// (76,0) and (65,1) needs 77 digits and rejects; the retry over
			// the parent domains binds the parent's own (66,1), a derivation
			// the pre-change engine rejects at planning. No existing feature
			// requires a marker, so the field keeps its base default.
			name:       "retry witness 66 1 persists a plain record",
			selectExpr: "lag(" + mul + ", 1, cast(0 as decimal(65,1))) over ()",
			wantWidth:  66,
			wantScale:  1,
		},
		{
			// (c) ABSENCE cell: the surface reconcile over (76,0) and (30,10)
			// needs 86 digits and rejects; the retry binds the parent's
			// (75,10).
			name:       "retry witness 75 10 persists a plain record",
			selectExpr: "lead(" + mul + ", 1, cast(0 as decimal(30,10))) over (order by n_nationkey)",
			wantWidth:  75,
			wantScale:  10,
		},
		{
			// Provenance-free band: no multiplication anywhere; the parent's
			// gate over (65,65) vs the (70,70) literal derives (70,70), which
			// the pre-change engine rejects at planning. The 70-digit
			// fraction's normalized width exceeds the DECIMAL128 width 38, so
			// the unchanged channel computes the pre-existing v89 literal
			// requirement for it and the record inherits exactly that — no
			// issue-specific marker.
			name:       "provenance free scale 70 merge inherits the literal v89 requirement",
			selectExpr: "lag(cast(0.1 as decimal(65,65)), 0, " + fraction70 + ") over ()",
			wantMarker: defines.MORPCVersion89,
			wantWidth:  70,
			wantScale:  70,
		},
		{
			// (a) second cell: a genuine wide match is pre-change-supported
			// (reconciliation skipped for matching types, base
			// base_binder.go:9084) and persists parent-identically WITH the
			// base v89 exact-decimal-literal requirement its 66-digit
			// literals raise — the band adds nothing.
			name:       "genuine wide match keeps its base v89 requirement",
			selectExpr: "lag(" + nines66 + ", 0, " + nines66 + ") over ()",
			wantMarker: defines.MORPCVersion89,
			wantWidth:  66,
			wantScale:  0,
		},
		{
			// (b) MIXED-FEATURE cell: the band merge beside a decimal
			// division (pre-existing v97 feature) persists the unchanged
			// channel's computed maximum max(v89, v97) = v97; the band adds
			// nothing.
			name:        "mixed feature view keeps the channel computed maximum",
			selectExpr:  "lag(" + mul + ", 0, " + nines76 + ") over ()",
			extraSelect: ", " + decimalDivision + " as w",
			wantMarker:  defines.MORPCVersion97,
			wantWidth:   76,
			wantScale:   0,
		},
		{
			// GATE-APPEARANCE mirror: the parent's gate is skipped because
			// its published (65,0) equals the default, so the parent binds
			// (65,0) with no rejection and the record stays parent-identical
			// with no marker even though the changed engine's gate runs.
			name:       "gate appearance mirror stays parent identical",
			selectExpr: "lag(" + mul + ", 0, cast(0 as decimal(65,0))) over ()",
			wantWidth:  65,
			wantScale:  0,
		},
		{
			// No default: the value argument's own pre-existing domain
			// publishes; nothing is newly admitted.
			name:       "no default stays parent supported",
			selectExpr: "lag(" + mul + ", 1) over ()",
			wantWidth:  65,
			wantScale:  0,
		},
		{
			// A NULL default adds no value domain at all, so the value
			// domain survives exactly on both engines.
			name:       "null default stays parent supported",
			selectExpr: "lag(" + mul + ", 1, null) over ()",
			wantWidth:  65,
			wantScale:  0,
		},
		{
			// An integer default reconciles into the declarable value
			// domain on both engines.
			name:       "integer default stays parent supported",
			selectExpr: "lag(" + mul + ", 1, 0) over ()",
			wantWidth:  65,
			wantScale:  0,
		},
		{
			// ABOVE-BOUND control: a decimal×decimal common whose required
			// width overflows the carrier (65 + 20 = 85) fails INSIDE
			// coalesce overload resolution and surfaces through the
			// reconcile's lookup-failure fallback; both engines emit the
			// identical base text for this shape.
			name:       "derived width above 76 rejects with the base text",
			selectExpr: "lag(cast(n_nationkey as decimal(65,0)), 0, cast(0 as decimal(20,20))) over ()",
			wantErr:    "LAG/LEAD common precision exceeds SQL limit 65",
		},
		{
			// Provenance present but both engines reject: the surface
			// reconcile over (76,0) and (65,20) overflows the carrier
			// (96 > 76), and the parent-domain retry derives (85,20), also
			// above 76, so the surface rejection re-arms untouched.
			name:       "provenance with both engines rejecting re-arms the surface error",
			selectExpr: "lag(" + mul + ", 1, cast(0 as decimal(65,20))) over ()",
			wantErr:    "LAG/LEAD common precision exceeds SQL limit 65",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := NewMockCompilerContext(false, newPlanTestProcess(t))
			p, err := buildLagLeadBandView(t, ctx,
				"create view v_laglead_band as select "+tc.selectExpr+" as v"+tc.extraSelect+" from nation")
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			marker := lagLeadBandViewMarker(t, p)
			if tc.wantMarker > 0 {
				require.NotNil(t, marker,
					"the unchanged channel's computed requirement must persist on the record")
				require.Equal(t, tc.wantMarker, *marker)
			} else {
				require.Nil(t, marker,
					"no existing feature requires a marker: the field keeps its base default")
			}
			cols := p.GetDdl().GetCreateView().GetTableDef().GetCols()
			require.NotEmpty(t, cols)
			require.Equal(t, int32(types.T_decimal256), cols[0].Typ.Id)
			require.Equal(t, tc.wantWidth, cols[0].Typ.Width)
			require.Equal(t, tc.wantScale, cols[0].Typ.Scale)
		})
	}
}

func TestLagLeadExpansionBandNestedViewMarkers(t *testing.T) {
	mul := lagLeadBandMul
	nines76 := lagLeadNines76

	ctx := NewMockCompilerContext(false, newPlanTestProcess(t))

	// Level 1: the band inner view over the gate-disappearance shape carries
	// the channel's v89 requirement for its N76 literal.
	inner, err := buildLagLeadBandView(t, ctx,
		"create view band_inner_v as select lag("+mul+", 0, "+nines76+") over () as v from nation")
	require.NoError(t, err)
	innerMarker := lagLeadBandViewMarker(t, inner)
	require.NotNil(t, innerMarker)
	require.Equal(t, defines.MORPCVersion89, *innerMarker)
	registerLagLeadBandView(ctx, "band_inner_v", inner.GetDdl().GetCreateView().GetTableDef())

	// Level 2: an outer view over the band inner is itself band. The channel
	// computes EACH LEVEL's own requirement over that level's expanded plan
	// (design section 9 I9 marker-preservation matrix): the expansion
	// re-binds the inner definition with its N76 literal, so the outer
	// record carries v89 as well — never an absence assertion.
	outer, err := buildLagLeadBandView(t, ctx,
		"create view band_outer_v as select v from band_inner_v")
	require.NoError(t, err)
	outerMarker := lagLeadBandViewMarker(t, outer)
	require.NotNil(t, outerMarker,
		"the outer level carries the requirement its own expanded plan computes")
	require.Equal(t, defines.MORPCVersion89, *outerMarker)
	outerCols := outer.GetDdl().GetCreateView().GetTableDef().GetCols()
	require.Len(t, outerCols, 1)
	require.Equal(t, int32(types.T_decimal256), outerCols[0].Typ.Id)
	require.Equal(t, int32(76), outerCols[0].Typ.Width)
	require.Equal(t, int32(0), outerCols[0].Typ.Scale)
	registerLagLeadBandView(ctx, "band_outer_v", outer.GetDdl().GetCreateView().GetTableDef())

	// Level 3: the per-level computation holds transitively across nesting
	// depth.
	third, err := buildLagLeadBandView(t, ctx,
		"create view band_third_v as select v from band_outer_v")
	require.NoError(t, err)
	thirdMarker := lagLeadBandViewMarker(t, third)
	require.NotNil(t, thirdMarker)
	require.Equal(t, defines.MORPCVersion89, *thirdMarker)

	// Control: an outer view over a NON-band inner whose expanded plan needs
	// no marker keeps the field at its base default at both levels.
	plain, err := buildLagLeadBandView(t, ctx,
		"create view plain_inner_v as select lag("+mul+", 0, cast(0 as decimal(65,0))) over () as v from nation")
	require.NoError(t, err)
	require.Nil(t, lagLeadBandViewMarker(t, plain))
	registerLagLeadBandView(ctx, "plain_inner_v", plain.GetDdl().GetCreateView().GetTableDef())
	plainOuter, err := buildLagLeadBandView(t, ctx,
		"create view plain_outer_v as select v from plain_inner_v")
	require.NoError(t, err)
	require.Nil(t, lagLeadBandViewMarker(t, plainOuter),
		"outer view over a non-band inner keeps the base default")

	// An ordinary SELECT from a band view binds without any authoring gate.
	_, err = buildLagLeadBandSelect(t, ctx, "select v from band_outer_v")
	require.NoError(t, err)
}

func TestLagLeadExpansionBandStatementRequirements(t *testing.T) {
	mul := lagLeadBandMul
	nines76 := lagLeadNines76

	ctx := NewMockCompilerContext(false, newPlanTestProcess(t))
	// The band carries no per-expression persisted requirement of its own:
	// the retry-witness shape holds no wide literals and no other marked
	// feature, so the unchanged channel's walk over the bound statement is
	// exactly zero.
	sel, err := buildLagLeadBandSelect(t, ctx,
		"select lag("+mul+", 1, cast(0 as decimal(65,1))) over () as v from nation")
	require.NoError(t, err)
	required, err := RequiredPersistedExpressionProtocolVersion(sel.GetQuery())
	require.NoError(t, err)
	require.Equal(t, int64(0), required,
		"a band statement's plan carries no per-expression persisted requirement")

	// The gate-disappearance shape's 76-digit literal carries the
	// PRE-EXISTING v89 exact-decimal-literal floor the parent also walks;
	// the band adds nothing beyond it.
	lit, err := buildLagLeadBandSelect(t, ctx,
		"select lag("+mul+", 0, "+nines76+") over () as v from nation")
	require.NoError(t, err)
	required, err = RequiredPersistedExpressionProtocolVersion(lit.GetQuery())
	require.NoError(t, err)
	require.Equal(t, defines.MORPCVersion89, required,
		"a band statement with a wide literal walks the pre-existing literal floor alone")
}

func TestLagLeadExpansionBandNoNewRemoteFeature(t *testing.T) {
	mul := lagLeadBandMul
	nines76 := lagLeadNines76

	ctx := NewMockCompilerContext(false, newPlanTestProcess(t))
	// Differential guard: the band trigger adds exactly the wide literal's
	// PRE-EXISTING feature bit over the parent-supported mirror and nothing
	// else — no new RequiredRemoteExpressionFeatures bit exists on any
	// widened-multiplication or band pipeline (design section 9 I9).
	band, err := buildLagLeadBandSelect(t, ctx,
		"select lag("+mul+", 0, "+nines76+") over () as v from nation")
	require.NoError(t, err)
	bandFeatures, err := plan.RequiredRemoteExpressionFeatures(band.GetQuery())
	require.NoError(t, err)
	mirror, err := buildLagLeadBandSelect(t, ctx,
		"select lag("+mul+", 0, cast(0 as decimal(65,0))) over () as v from nation")
	require.NoError(t, err)
	mirrorFeatures, err := plan.RequiredRemoteExpressionFeatures(mirror.GetQuery())
	require.NoError(t, err)
	expected := mirrorFeatures
	expected.DecimalLiteralSemantics = true
	require.Equal(t, expected, bandFeatures,
		"the band trigger adds only the N76 literal's pre-existing feature bit")
	require.True(t, bandFeatures.DecimalLiteralSemantics)

	// The retry witness holds no wide literal: its remote-feature identity
	// equals the parent-supported mirror's exactly.
	retry, err := buildLagLeadBandSelect(t, ctx,
		"select lag("+mul+", 1, cast(0 as decimal(65,1))) over () as v from nation")
	require.NoError(t, err)
	retryFeatures, err := plan.RequiredRemoteExpressionFeatures(retry.GetQuery())
	require.NoError(t, err)
	require.Equal(t, mirrorFeatures, retryFeatures,
		"the retry witness carries no feature bit beyond the parent-supported shape")

	// Untouched-gate pins (design section 9 I9): pkg/defines/const.go gains
	// no MORPCVersion135 — the constant's absence is a compile-level pin —
	// and MORPCLatestVersion keeps its base value.
	require.Equal(t, defines.MORPCVersion134, defines.MORPCLatestVersion,
		"the protocol gate stays untouched: latest keeps its base value")
}
