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
	"context"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func decimalArithmeticTestColumn(typ types.Type, position int32) *planpb.Expr {
	return GetColExpr(makePlan2Type(&typ), 0, position)
}

func TestDecimalArithmeticExplicitCastPreservesDeclaredDomain(t *testing.T) {
	ctx := context.Background()
	leftType := types.New(types.T_decimal128, 38, 0)
	rightType := types.New(types.T_decimal128, 38, 18)
	right, err := appendExplicitCastBeforeExpr(
		ctx, makePlan2StringConstExprWithType("0.5"), makePlan2Type(&rightType))
	require.NoError(t, err)

	result, err := BindFuncExprImplByPlanExpr(ctx, "+", []*planpb.Expr{
		decimalArithmeticTestColumn(leftType, 0), right,
	})
	require.NoError(t, err)
	require.Equal(t, int32(types.T_decimal256), result.Typ.Id)
	require.Equal(t, int32(57), result.Typ.Width)
	require.Equal(t, int32(18), result.Typ.Scale)
	require.Equal(t, int32(38), result.GetF().Args[1].Typ.Width)
	require.Equal(t, int32(18), result.GetF().Args[1].Typ.Scale)
}

func TestDecimalArithmeticWeakStringLiteralUsesCanonicalPrecision(t *testing.T) {
	ctx := context.Background()
	peerType := types.New(types.T_decimal128, 20, 0)
	for _, test := range []struct {
		name      string
		value     string
		scale     int32
		wantWidth int32
	}{
		{name: "positive exponent", value: "1e2", wantWidth: 3},
		{name: "negative exponent", value: "1e-2", scale: 4, wantWidth: 4},
		{name: "leading and trailing zeroes", value: "0001.20", scale: 2, wantWidth: 3},
	} {
		t.Run(test.name, func(t *testing.T) {
			literalType := types.New(types.T_decimal128, 38, test.scale)
			literal, err := appendCastBeforeExpr(
				ctx, makePlan2StringConstExprWithType(test.value), makePlan2Type(&literalType))
			require.NoError(t, err)
			lookup := refineDecimalArithmeticLiteralLookupTypes(
				"+",
				[]*planpb.Expr{decimalArithmeticTestColumn(peerType, 0), literal},
				[]types.Type{peerType, literalType},
			)
			require.Equal(t, test.wantWidth, lookup[1].Width)
			require.Equal(t, test.scale, lookup[1].Scale)
		})
	}
}

func TestDecimalArithmeticSmallIntegerLiteralKeepsDecimal128(t *testing.T) {
	ctx := context.Background()
	decimalType := types.New(types.T_decimal128, 20, 0)
	result, err := BindFuncExprImplByPlanExpr(ctx, "*", []*planpb.Expr{
		decimalArithmeticTestColumn(decimalType, 0), makePlan2Int64ConstExprWithType(2),
	})
	require.NoError(t, err)
	require.Equal(t, int32(types.T_decimal128), result.Typ.Id)
	require.Equal(t, int32(21), result.Typ.Width)
}

func TestDecimalMultiplyIntegerLiteralUsesRefinedCast(t *testing.T) {
	ctx := context.Background()
	decimalType := types.New(types.T_decimal64, 10, 2)
	result, err := BindFuncExprImplByPlanExpr(ctx, "*", []*planpb.Expr{
		decimalArithmeticTestColumn(decimalType, 0), makePlan2Int64ConstExprWithType(8),
	})
	require.NoError(t, err)
	require.Equal(t, int32(types.T_decimal128), result.Typ.Id)
	require.Equal(t, int32(2), result.Typ.Scale)

	args := result.GetF().Args
	require.Len(t, args, 2)
	require.Equal(t, int32(types.T_decimal64), args[0].Typ.Id)
	require.Equal(t, int32(types.T_decimal64), args[1].Typ.Id)
	require.Equal(t, int32(0), args[1].Typ.Scale)
	require.NotNil(t, args[1].GetF(), "the integer literal must be physically cast")
}

func TestDecimalAddIntegerLiteralRetainsDecimal128Coercion(t *testing.T) {
	ctx := context.Background()
	decimalType := types.New(types.T_decimal64, 10, 2)
	result, err := BindFuncExprImplByPlanExpr(ctx, "+", []*planpb.Expr{
		decimalArithmeticTestColumn(decimalType, 0), makePlan2Int64ConstExprWithType(8),
	})
	require.NoError(t, err)
	require.Equal(t, int32(types.T_decimal128), result.Typ.Id)
	require.Equal(t, int32(2), result.Typ.Scale)

	args := result.GetF().Args
	require.Len(t, args, 2)
	require.Equal(t, int32(types.T_decimal128), args[0].Typ.Id)
	require.Equal(t, int32(types.T_decimal128), args[1].Typ.Id)
}

func TestWideDecimalProductRemainsWideInComparison(t *testing.T) {
	ctx := context.Background()
	leftType := types.New(types.T_decimal128, 38, 0)
	rightType := types.New(types.T_decimal128, 28, 0)
	product, err := BindFuncExprImplByPlanExpr(ctx, "*", []*planpb.Expr{
		decimalArithmeticTestColumn(leftType, 0),
		decimalArithmeticTestColumn(rightType, 1),
	})
	require.NoError(t, err)
	require.Equal(t, int32(types.T_decimal256), product.Typ.Id)
	// Issue #30938: the product publishes its physical precision instead of
	// clamping to the 65-digit declaration limit. A width-38 DECIMAL128
	// operand counts 39 coefficient digits (decimalMultiplyCoefficientWidth:
	// the released carrier envelope retains a full signed coefficient), so
	// 39 + 28 = 67.
	require.Equal(t, int32(67), product.Typ.Width)

	comparison, err := BindFuncExprImplByPlanExpr(ctx, ">", []*planpb.Expr{
		product, decimalArithmeticTestColumn(leftType, 0),
	})
	require.NoError(t, err)
	require.Equal(t, int32(types.T_decimal256), comparison.GetF().Args[0].Typ.Id)
	require.Equal(t, int32(types.T_decimal256), comparison.GetF().Args[1].Typ.Id)
}

func TestCTASPublishesDecimal256ProductBoundary(t *testing.T) {
	logicPlan, err := buildSingleStmt(NewMockOptimizer(false, newPlanTestProcess(t)), t,
		"create table decimal_product_boundary as select "+
			"cast(n_nationkey as decimal(38,0)) * "+
			"cast(n_nationkey as decimal(27,0)) as product from nation")
	require.NoError(t, err)

	columns := logicPlan.GetDdl().GetCreateTable().GetTableDef().GetCols()
	require.NotEmpty(t, columns)
	require.Equal(t, int32(types.T_decimal256), columns[0].Typ.Id)
	require.Equal(t, int32(65), columns[0].Typ.Width)
	require.Equal(t, int32(0), columns[0].Typ.Scale)
}

// TestCTASClampsPhysicalCarrierProductToDeclaredDomain pins the storage side
// of issue #30938: a multiplication expression may publish the 76-digit
// Decimal256 carrier width, but a persisted CTAS column stays inside the
// declared 65-digit SQL domain so the insert admission check keeps authority
// over wider values.
func TestCTASClampsPhysicalCarrierProductToDeclaredDomain(t *testing.T) {
	for _, tc := range []struct {
		name      string
		expr      string
		wantWidth int32
		wantScale int32
	}{
		{
			name:      "76 digit integer product clamps to the declared domain",
			expr:      "cast(n_nationkey as decimal(38,0)) * cast(n_nationkey as decimal(38,0))",
			wantWidth: 65,
			wantScale: 0,
		},
		{
			name:      "66 digit product clamps to the declared domain",
			expr:      "cast(n_nationkey as decimal(38,0)) * cast(n_nationkey as decimal(28,0))",
			wantWidth: 65,
			wantScale: 0,
		},
		{
			name:      "scaled carrier product clamps width and keeps scale",
			expr:      "cast(n_nationkey as decimal(38,2)) * cast(n_nationkey as decimal(38,2))",
			wantWidth: 65,
			wantScale: 4,
		},
		{
			name:      "65 digit product was already inside the declared domain",
			expr:      "cast(n_nationkey as decimal(38,0)) * cast(n_nationkey as decimal(27,0))",
			wantWidth: 65,
			wantScale: 0,
		},
		{
			name: "coalesce over a carrier product stays in the declared domain",
			expr: "coalesce(cast(n_nationkey as decimal(38,0)) * " +
				"cast(n_nationkey as decimal(38,0)), cast(n_nationkey as decimal(38,0)))",
			wantWidth: 65,
			wantScale: 0,
		},
		{
			name: "case over a carrier product stays in the declared domain",
			expr: "case when n_nationkey > 0 then cast(n_nationkey as decimal(38,0)) * " +
				"cast(n_nationkey as decimal(38,0)) else cast(n_nationkey as decimal(38,0)) end",
			wantWidth: 65,
			wantScale: 0,
		},
		{
			name: "unary minus over a carrier product stays in the declared domain",
			expr: "-(cast(n_nationkey as decimal(38,0)) * " +
				"cast(n_nationkey as decimal(38,0)))",
			wantWidth: 65,
			wantScale: 0,
		},
		{
			// refineDecimalRoundingReturnType re-derives round metadata as
			// integerDigits+digits+1 precision with scale=digits: the (76,4)
			// carrier product publishes (75,2), and the storage boundary then
			// persists the parent's envelope-derived declaration (64,2)
			// (F-BOUNDARY-FLOOR, design decision log).
			name: "rounding over a carrier product stays in the declared domain",
			expr: "round(cast(n_nationkey as decimal(38,2)) * " +
				"cast(n_nationkey as decimal(38,2)), 2)",
			wantWidth: 64,
			wantScale: 2,
		},
		{
			name: "nested carrier product with fractional operand keeps declared width and scale",
			expr: "cast(n_nationkey as decimal(38,0)) * " +
				"cast(n_nationkey as decimal(38,0)) * 1.0",
			wantWidth: 65,
			wantScale: 1,
		},
		{
			// Review round 3 (issue #30938): an implicit integer literal is
			// coalesce-aligned to the common (76,0) type with a binder-generated
			// cast; its contribution must derive from the literal's own value
			// domain, never from the aligned carrier width.
			name: "coalesce of a carrier product and an integer literal clamps to the declared domain",
			expr: "coalesce(cast(n_nationkey as decimal(38,0)) * " +
				"cast(n_nationkey as decimal(38,0)), 0)",
			wantWidth: 65,
			wantScale: 0,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logicPlan, err := buildSingleStmt(NewMockOptimizer(false, newPlanTestProcess(t)), t,
				"create table decimal_ctas_carrier as select "+tc.expr+" as product from nation")
			require.NoError(t, err)
			columns := logicPlan.GetDdl().GetCreateTable().GetTableDef().GetCols()
			require.NotEmpty(t, columns)
			require.Equal(t, int32(types.T_decimal256), columns[0].Typ.Id)
			require.Equal(t, tc.wantWidth, columns[0].Typ.Width)
			require.Equal(t, tc.wantScale, columns[0].Typ.Scale)
		})
	}
}

// TestCTASPreservesNonMultiplicationDecimalWidths pins the other half of the
// issue #30938 storage boundary: only multiplication-derived carrier widths
// narrow to the declared 65-digit domain on persistence. Pre-existing wide
// inference, such as plain literals bound through DECIMAL256 (up to 76
// digits, including the 76-digit fractional domain), or a conditional whose
// widest non-multiplication branch dominates, keeps its exact width.
func TestCTASPreservesNonMultiplicationDecimalWidths(t *testing.T) {
	lit66 := strings.Repeat("9", 66)
	lit70 := strings.Repeat("9", 70)
	lit7676 := "0." + strings.Repeat("0", 75) + "1"
	for _, tc := range []struct {
		name      string
		sql       string
		wantWidth int32
		wantScale int32
	}{
		{
			name:      "66 digit integer literal keeps its bound precision",
			sql:       "create table decimal_ctas_lit66 as select " + lit66 + " as product",
			wantWidth: 66,
			wantScale: 0,
		},
		{
			name:      "76 digit fractional literal keeps its bound domain",
			sql:       "create table decimal_ctas_lit7676 as select " + lit7676 + " as product",
			wantWidth: 76,
			wantScale: 76,
		},
		{
			name: "conditional dominated by a wide literal keeps that literal domain",
			sql: "create table decimal_ctas_lit_dominant as select coalesce(" +
				"cast(n_nationkey as decimal(38,0)) * cast(n_nationkey as decimal(38,0)), " +
				lit70 + ") as product from nation",
			wantWidth: 70,
			wantScale: 0,
		},
		{
			// Issue #30938 review round 7: a binder alignment CAST whose
			// target exceeds the declared domain is looked through onto its
			// operand; an INTEGER operand still carries its exact digit
			// domain (19 for BIGINT), so the merged base inference (69,50)
			// is preserved instead of discarding the integer branch.
			name:      "coalesce with a BIGINT operand keeps the merged integer domain",
			sql:       "create table decimal_ctas_coalesce_int as select coalesce(cast(1.5 as decimal(65,50)), o_orderkey) as v from orders",
			wantWidth: 69,
			wantScale: 50,
		},
		{
			// Guard: the integer domain must not override a decimal branch
			// whose own integral capacity dominates; the merge stays
			// exactly what base inference publishes.
			name:      "coalesce with a narrower INT operand keeps the decimal branch domain",
			sql:       "create table decimal_ctas_coalesce_intguard as select coalesce(cast(1.5 as decimal(65,50)), n_nationkey) as v from nation",
			wantWidth: 65,
			wantScale: 50,
		},
		{
			// Review round 3 (issue #30938): combining domains must preserve the
			// integer-capacity/scale envelope. coalesceCheck derives (66 ints,
			// scale 4) = (70,4) for a DECIMAL(65,4) column against a 66-digit
			// literal; narrowing to the literal's raw precision (66,4) would
			// shrink the scale envelope and reject the NULL-branch value on
			// write admission. Base publishes (70,4) here.
			name: "mixed scale coalesce keeps the integer capacity and scale envelope",
			sql: "create table decimal_ctas_mixed_scale as select coalesce(" +
				"cast(n_nationkey as decimal(65,4)), " + lit66 + ") as product from nation",
			wantWidth: 70,
			wantScale: 4,
		},
		{
			// Review round 3 (issue #30938): a scaled multiplication under a
			// wider literal keeps the base envelope of (40 ints, scale 30) =
			// (70,30), exactly what the 65-capped base published.
			// decimalMultiplyReturnType publishes scale 15 here (not 15+15), so
			// the carrier product carries {61 integral, 15 scale}; the 60-digit
			// literal dominates the integral capacity and the combined envelope
			// is {60, 15}. Base (multiplication capped at 65) published exactly
			// (75,15), so the storage boundary must preserve it.
			name: "scaled product dominated by a 60 digit literal keeps the base envelope",
			sql: "create table decimal_ctas_scaled_dominant as select coalesce(" +
				"cast(n_nationkey as decimal(38,15)) * cast(n_nationkey as decimal(38,15)), " +
				strings.Repeat("9", 60) + ") as product from nation",
			wantWidth: 75,
			wantScale: 15,
		},
		{
			// Guard (issue #30938 review round 8): a negative wide literal is
			// a genuine literal domain, not a folded multiplication carrier;
			// it keeps its bound precision like the positive form.
			name:      "negative 66 digit integer literal keeps its bound precision",
			sql:       "create table decimal_ctas_neg_lit66 as select -" + lit66 + " as product",
			wantWidth: 66,
			wantScale: 0,
		},
		{
			// Issue #30938 review round 8: when the LAG/LEAD default already
			// shares the value domain, base reconciliation skips widening and
			// the parent publishes the matching domain. The storage boundary
			// must preserve that pre-existing wide domain instead of capping
			// to the declared 65 digits.
			name: "lag with a matching wide literal default keeps the parent domain",
			sql: "create table decimal_ctas_lag_wide_match as select lag(" + lit66 + ", 0, " +
				lit66 + ") over () as product",
			wantWidth: 66,
			wantScale: 0,
		},
		{
			// Issue #30938 review round 8: a NULL default adds no value
			// domain at all, so the wide literal value domain survives
			// exactly.
			name: "lag with a NULL default keeps the wide literal domain",
			sql: "create table decimal_ctas_lag_null_def as select lag(" + lit66 + ", 0, null) " +
				"over () as product",
			wantWidth: 66,
			wantScale: 0,
		},
		{
			// Guard: matching max-scale DECIMAL value and default domains
			// skip widening reconciliation; the bound stays the declarable
			// value domain on both sides of the round-8 refinement.
			name: "lag with a matching high scale default keeps the declarable domain",
			sql: "create table decimal_ctas_lag_hs_match as select " +
				"lag(cast(0.1 as decimal(65,65)), 0, cast(0.2 as decimal(65,65))) over () as product",
			wantWidth: 65,
			wantScale: 65,
		},
		{
			// Issue #30938 review round 9: constant folding materializes a
			// folded selector over wide scalar vectors as a single Expr_Vec.
			// This selector contains no multiplication at all; the 66-digit
			// literal is a genuine wide domain the parent supports. The
			// storage boundary must not infer multiplication provenance from
			// the folded width.
			name: "folded selector returning a genuine wide literal keeps its domain",
			sql: "create table decimal_ctas_fold_sel66 as select +if(false, cast(1 as decimal(65,0)), " +
				lit66 + ") as product",
			wantWidth: 66,
			wantScale: 0,
		},
		{
			// Issue #30938 review round 9: folded multiplication beneath a
			// dominating wide branch. The genuine 66-digit domain dominates
			// the carrier branch; the folded vector width cannot tell the
			// two apart, so the boundary keeps the unfolded tree's envelope.
			name: "folded multiplication beneath a dominating wide literal keeps the wide domain",
			sql: "create table decimal_ctas_fold_dom66 as select +if(true, " + lit66 +
				", cast(2 as decimal(65,0)) * cast(3 as decimal(65,0))) as product",
			wantWidth: 66,
			wantScale: 0,
		},
		{
			// Same dominance with the branches exchanged: the selected value
			// is the fitting product, but the accepted domain includes the
			// genuine 66-digit branch, so (66,0) is the parent-published
			// storage domain.
			name: "folded multiplication selected under a wide literal branch keeps the wide domain",
			sql: "create table decimal_ctas_fold_mul_sel as select +if(true, " +
				"cast(2 as decimal(65,0)) * cast(3 as decimal(65,0)), " + lit66 + ") as product",
			wantWidth: 66,
			wantScale: 0,
		},
		{
			// Guard: the un-folded form of the dominance shape binds the
			// wide domain through the branch envelope on both sides of the
			// round-9 refinement.
			name: "unfolded dominating wide literal keeps the wide domain",
			sql: "create table decimal_ctas_if_dom66 as select if(true, " + lit66 +
				", cast(2 as decimal(65,0)) * cast(3 as decimal(65,0))) as product",
			wantWidth: 66,
			wantScale: 0,
		},
		{
			// Issue #30938 review round 9: a recursive CTE anchor of genuine
			// wide literals keeps its non-carrier domain at the CTAS
			// boundary; the recursive member contributes nothing beyond the
			// anchor's envelope (bounded source-step cycle resolution).
			name: "recursive CTE over a genuine wide literal keeps the wide domain",
			sql: "create table decimal_ctas_rcte_lit66 as with recursive r(n,p) as " +
				"(select 1 as n, " + lit66 + " as p union all select n+1, p from r where n < 2) " +
				"select p as product from r",
			wantWidth: 66,
			wantScale: 0,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logicPlan, err := buildSingleStmt(NewMockOptimizer(false, newPlanTestProcess(t)), t, tc.sql)
			require.NoError(t, err)
			columns := logicPlan.GetDdl().GetCreateTable().GetTableDef().GetCols()
			require.NotEmpty(t, columns)
			require.Equal(t, int32(types.T_decimal256), columns[0].Typ.Id)
			require.Equal(t, tc.wantWidth, columns[0].Typ.Width)
			require.Equal(t, tc.wantScale, columns[0].Typ.Scale)
		})
	}
}

// TestCTASMultiplicationStorageBoundaryThroughConstantFolding pins issue
// #30938 review rounds 8-9: statement-constant arithmetic would fold before
// CTAS inference, and materializing a wide decimal256 scalar as a
// single-value vector (rule.MaterializeDecimal256Constant) destroys the
// multiplication expression shape the storage envelope keys on. Round 9
// preserves that provenance at the source: a constant tree carried by a
// decimal multiplication published wider than the declared 65-digit domain
// is not folded (CarriesWideDecimalMultiplicationProvenance), so the
// boundary sees the executable tree and applies the multiplication storage
// domain (65 digits, published scale): fitting carrier products persist a
// declarable, replayable type, and oversized carrier products hit the same
// insert-admission rejection as un-folded ones. Folds with no multiplication
// provenance still materialize, so genuine wide literals keep their own
// bound precision.
func TestCTASMultiplicationStorageBoundaryThroughConstantFolding(t *testing.T) {
	lit38 := strings.Repeat("9", 38)
	for _, tc := range []struct {
		name      string
		sql       string
		wantWidth int32
		wantScale int32
	}{
		{
			name: "folded fitting carrier product keeps the multiplication storage domain",
			sql: "create table decimal_ctas_fold_fit as select " +
				"cast(2 as decimal(65,0)) * cast(3 as decimal(65,0)) as product",
			wantWidth: 65,
			wantScale: 0,
		},
		{
			// The 76-digit folded product exceeds the declared storage
			// domain; persisting (65,0) makes insert admission reject it,
			// exactly like the un-folded multiplication over the same casts.
			name: "folded oversized carrier product keeps the multiplication storage domain",
			sql: "create table decimal_ctas_fold_over as select " +
				"cast(" + lit38 + " as decimal(65,0)) * cast(" + lit38 + " as decimal(65,0)) as product",
			wantWidth: 65,
			wantScale: 0,
		},
		{
			name: "folded scaled carrier product keeps the declared width and scale",
			sql: "create table decimal_ctas_fold_scaled as select " +
				"cast(1.5 as decimal(38,2)) * cast(2 as decimal(38,2)) as product",
			wantWidth: 65,
			wantScale: 4,
		},
		{
			// Guard: a folded product that never exceeds the declared carrier
			// width needs no narrowing at all.
			name: "folded narrow product keeps its published type",
			sql: "create table decimal_ctas_fold_narrow as select " +
				"cast(2 as decimal(30,0)) * cast(3 as decimal(30,0)) as product",
			wantWidth: 60,
			wantScale: 0,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logicPlan, err := buildSingleStmt(NewMockOptimizer(false, newPlanTestProcess(t)), t, tc.sql)
			require.NoError(t, err)
			columns := logicPlan.GetDdl().GetCreateTable().GetTableDef().GetCols()
			require.NotEmpty(t, columns)
			require.Equal(t, int32(types.T_decimal256), columns[0].Typ.Id)
			require.Equal(t, tc.wantWidth, columns[0].Typ.Width)
			require.Equal(t, tc.wantScale, columns[0].Typ.Scale)
		})
	}
}

// TestCTASAdmitsWindowMergeExpansionBand pins the repaired storage envelope
// (issue #30938, design §4.1, decision log 82): a LAG/LEAD value/default
// merge whose parent-domain derivation the pre-change engine rejects at
// planning (derived width 66-76, here the provenance-free scale-70 merge) is
// the expansion band — the changed engine binds the merge and persists the
// reconcile-derived tag exactly, whether or not the offending default is
// selected. The band's cross-version fence is the binding-locus protocol
// raise on view records (decision log 82), never a truncated or rejected
// schema.
func TestCTASAdmitsWindowMergeExpansionBand(t *testing.T) {
	fraction70 := "0." + strings.Repeat("0", 69) + "1"
	for _, tc := range []struct {
		name string
		sql  string
	}{
		{
			name: "lag merging into the band scale persists the reconcile-derived tag",
			sql: "create table decimal_ctas_lag_hs as select " +
				"lag(cast(0.1 as decimal(65,65)), 0, " + fraction70 + ") over () as product",
		},
		{
			name: "lead with the band default selected out of frame persists the reconcile-derived tag",
			sql: "create table decimal_ctas_lead_hs as select " +
				"lead(cast(0.1 as decimal(65,65)), 1, " + fraction70 + ") over (order by n_nationkey) as product from nation",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logicPlan, err := buildSingleStmt(NewMockOptimizer(false, newPlanTestProcess(t)), t, tc.sql)
			require.NoError(t, err)
			columns := logicPlan.GetDdl().GetCreateTable().GetTableDef().GetCols()
			require.NotEmpty(t, columns)
			require.Equal(t, int32(types.T_decimal256), columns[0].Typ.Id)
			require.Equal(t, int32(70), columns[0].Typ.Width)
			require.Equal(t, int32(70), columns[0].Typ.Scale)
		})
	}
}

// TestCTASMultiplicationProvenanceThroughRelationalOutputs pins review rounds
// 3-7 of issue #30938: the multiplication storage boundary must follow output
// references through aggregate, distinct, set-operation, sort, window, join
// and filter nodes. MAX keeps its argument's type, and DISTINCT/UNION expose
// values through typed output columns, so a bare ColRef at the CTAS
// projection must resolve through the plan instead of keeping its published
// carrier width. Round 7's LAG/LEAD value/default merge cases stand with
// their repaired envelopes (design §4.1, decision log 82): band merges
// persist the reconcile-derived tag above 65, parent-supported merges keep
// the parent's own domain, and admission checks out-of-frame defaults.
func TestCTASMultiplicationProvenanceThroughRelationalOutputs(t *testing.T) {
	mul := "cast(n_nationkey as decimal(38,0)) * cast(n_nationkey as decimal(38,0))"
	for _, tc := range []struct {
		name      string
		sql       string
		wantWidth int32
		wantScale int32
	}{
		{
			name:      "aggregate max of a carrier product clamps to the declared domain",
			sql:       "create table decimal_ctas_agg as select max(" + mul + ") as product from nation",
			wantWidth: 65,
			wantScale: 0,
		},
		{
			name:      "distinct over a carrier product clamps to the declared domain",
			sql:       "create table decimal_ctas_distinct as select distinct " + mul + " as product from nation",
			wantWidth: 65,
			wantScale: 0,
		},
		{
			name: "union of a carrier product and an integer branch clamps to the declared domain",
			sql: "create table decimal_ctas_union as select " + mul + " as product from nation " +
				"union all select " + strings.Repeat("9", 20) + " from nation",
			wantWidth: 65,
			wantScale: 0,
		},
		{
			name: "aggregate max over a narrow literal keeps that literal domain",
			sql: "create table decimal_ctas_agg_lit as select max(" + strings.Repeat("9", 66) +
				") as product from nation",
			wantWidth: 66,
			wantScale: 0,
		},
		{
			// query_builder omits unselected grouping keys from AGG.ProjectList,
			// so parent output slot 0 maps through the projection ColRef
			// (RelPos -2) to AggList[0], not to GroupBy[0] (review round 4).
			name: "grouped aggregate over a wide literal keeps its bound precision",
			sql: "create table decimal_ctas_agg_group_lit as select max(" + strings.Repeat("9", 66) +
				") as product from nation group by n_nationkey",
			wantWidth: 66,
			wantScale: 0,
		},
		{
			name: "grouped aggregate over a carrier product clamps to the declared domain",
			sql: "create table decimal_ctas_agg_group as select max(" + mul +
				") as product from nation group by n_nationkey",
			wantWidth: 65,
			wantScale: 0,
		},
		{
			// The aggregate surfaces inside the scalar wrapper as a column
			// reference; the envelope must resolve it relationally too.
			name:      "scalar wrapper over an aggregate of a carrier product clamps to the declared domain",
			sql:       "create table decimal_ctas_abs_agg as select abs(max(" + mul + ")) as product from nation",
			wantWidth: 65,
			wantScale: 0,
		},
		{
			name:      "coalesce over an aggregate of a carrier product clamps to the declared domain",
			sql:       "create table decimal_ctas_coalesce_agg as select coalesce(max(" + mul + "), 0) as product from nation",
			wantWidth: 65,
			wantScale: 0,
		},
		{
			name: "scalar wrapper over an aggregate of a wide literal keeps that literal domain",
			sql: "create table decimal_ctas_abs_agg_lit as select abs(max(" + strings.Repeat("9", 66) +
				")) as product from nation",
			wantWidth: 66,
			wantScale: 0,
		},
		{
			// ORDER BY keeps a result PROJECT above SORT, and SORT mirrors
			// its child outputs (query_builder SORT/PARTITION pull-up), so
			// the resolver must follow projection through SORT too.
			name:      "ordered carrier product clamps to the declared domain",
			sql:       "create table decimal_ctas_order as select " + mul + " as product from nation order by product",
			wantWidth: 65,
			wantScale: 0,
		},
		{
			name: "ordered wide literal keeps its bound precision",
			sql: "create table decimal_ctas_order_lit as select " + strings.Repeat("9", 66) +
				" as product from nation order by product",
			wantWidth: 66,
			wantScale: 0,
		},
		{
			// A window result is projected as a self-reference (RelPos -1)
			// alongside passthrough mirrors of the WINDOW child; the producer
			// is the (single) WinSpecList function itself.
			name:      "window max of a carrier product clamps to the declared domain",
			sql:       "create table decimal_ctas_win as select max(" + mul + ") over () as product from nation",
			wantWidth: 65,
			wantScale: 0,
		},
		{
			name: "partitioned window max of a carrier product clamps to the declared domain",
			sql: "create table decimal_ctas_win_part as select max(" + mul +
				") over (partition by n_regionkey) as product from nation",
			wantWidth: 65,
			wantScale: 0,
		},
		{
			name: "window max over a wide literal keeps that literal domain",
			sql: "create table decimal_ctas_win_lit as select max(" + strings.Repeat("9", 66) +
				") over () as product from nation",
			wantWidth: 66,
			wantScale: 0,
		},
		{
			// any_value/max_by/max_by_non_null return values of their first
			// argument (ReturnFirstArgType), exactly like max/min.
			name:      "any_value of a carrier product clamps to the declared domain",
			sql:       "create table decimal_ctas_anyval as select any_value(" + mul + ") as product from nation",
			wantWidth: 65,
			wantScale: 0,
		},
		{
			name: "any_value over a wide literal keeps that literal domain",
			sql: "create table decimal_ctas_anyval_lit as select any_value(" + strings.Repeat("9", 66) +
				") as product from nation",
			wantWidth: 66,
			wantScale: 0,
		},
		{
			name: "max_by of a carrier product clamps to the declared domain",
			sql: "create table decimal_ctas_maxby as select max_by(" + mul +
				", n_nationkey, n_nationkey) as product from nation",
			wantWidth: 65,
			wantScale: 0,
		},
		{
			name: "max_by_non_null of a carrier product clamps to the declared domain",
			sql: "create table decimal_ctas_maxbynn as select max_by_non_null(" + mul +
				", n_nationkey, n_nationkey) as product from nation",
			wantWidth: 65,
			wantScale: 0,
		},
		{
			// Only the value argument (arg 0) contributes: the envelope must
			// not combine the wider ordering argument's scale domain.
			name: "max_by follows its value argument, not the ordering arguments",
			sql: "create table decimal_ctas_maxby_scaled as select max_by(" +
				"cast(n_nationkey as decimal(38,15)) * cast(n_nationkey as decimal(38,15)), " +
				"cast(n_nationkey as decimal(65,4)), n_nationkey) as product from nation",
			wantWidth: 65,
			wantScale: 15,
		},
		{
			// refineDecimalRoundingReturnType compares digits in int64;
			// narrowing to int32 first wraps 2^32 to 0 and re-derives width.
			name: "round with digits beyond int32 keeps the input domain",
			sql: "create table decimal_ctas_round_2p32 as select round(coalesce(cast(n_nationkey as decimal(65,4)), " +
				strings.Repeat("9", 66) + "), 4294967296) as product from nation",
			wantWidth: 70,
			wantScale: 4,
		},
		{
			name: "round with digits at the int32 sign boundary keeps the input domain",
			sql: "create table decimal_ctas_round_2p31 as select round(coalesce(cast(n_nationkey as decimal(65,4)), " +
				strings.Repeat("9", 66) + "), 2147483648) as product from nation",
			wantWidth: 70,
			wantScale: 4,
		},
		{
			name: "truncate with digits beyond int32 keeps the input domain",
			sql: "create table decimal_ctas_trunc_2p32 as select truncate(coalesce(cast(n_nationkey as decimal(65,4)), " +
				strings.Repeat("9", 66) + "), 4294967296) as product from nation",
			wantWidth: 70,
			wantScale: 4,
		},
		{
			name:      "first_value of a carrier product clamps to the declared domain",
			sql:       "create table decimal_ctas_fv as select first_value(" + mul + ") over (order by n_nationkey) as product from nation",
			wantWidth: 65,
			wantScale: 0,
		},
		{
			name: "last_value of a carrier product clamps to the declared domain",
			sql: "create table decimal_ctas_lv as select last_value(" + mul +
				") over (order by n_nationkey rows between unbounded preceding and unbounded following) as product from nation",
			wantWidth: 65,
			wantScale: 0,
		},
		{
			name:      "nth_value of a carrier product clamps to the declared domain",
			sql:       "create table decimal_ctas_nv as select nth_value(" + mul + ", 1) over (order by n_nationkey) as product from nation",
			wantWidth: 65,
			wantScale: 0,
		},
		{
			name:      "lag of a carrier product clamps to the declared domain",
			sql:       "create table decimal_ctas_lag as select lag(" + mul + ") over (order by n_nationkey) as product from nation",
			wantWidth: 65,
			wantScale: 0,
		},
		{
			name:      "lead of a carrier product clamps to the declared domain",
			sql:       "create table decimal_ctas_lead as select lead(" + mul + ") over (order by n_nationkey) as product from nation",
			wantWidth: 65,
			wantScale: 0,
		},
		{
			name:      "lag with an integer default over a carrier product clamps to the declared domain",
			sql:       "create table decimal_ctas_lag_def as select lag(" + mul + ", 1, 0) over (order by n_nationkey) as product from nation",
			wantWidth: 65,
			wantScale: 0,
		},
		{
			name:      "lead with an integer default over a carrier product clamps to the declared domain",
			sql:       "create table decimal_ctas_lead_def as select lead(" + mul + ", 1, 0) over (order by n_nationkey) as product from nation",
			wantWidth: 65,
			wantScale: 0,
		},
		{
			name:      "first_value over a wide literal keeps that literal domain",
			sql:       "create table decimal_ctas_fv_lit as select first_value(" + strings.Repeat("9", 66) + ") over (order by n_nationkey) as product from nation",
			wantWidth: 66,
			wantScale: 0,
		},
		{
			// Issue #30938 review round 7, repaired envelope (design §4.1,
			// decision log 82): the widened LAG/LEAD reconciliation over a
			// 66-digit literal default is band — the parent-domain
			// derivation (66,0) is the parent's planning rejection region —
			// so the merge persists its reconcile-derived tag exactly;
			// admission checks the selected values, including out-of-frame
			// defaults.
			name: "lag with a wide literal default persists the reconcile-derived band tag",
			sql: "create table decimal_ctas_lag_def_lit as select lag(" + mul + ", 1, " +
				strings.Repeat("9", 66) + ") over (order by n_nationkey) as product from nation",
			wantWidth: 66,
			wantScale: 0,
		},
		{
			name: "join of a derived carrier product clamps to the declared domain",
			sql: "create table decimal_ctas_join as select d.product from (select max(" + mul +
				") as product from nation group by n_regionkey) d cross join nation r",
			wantWidth: 65,
			wantScale: 0,
		},
		{
			name: "join of a derived wide literal keeps that literal domain",
			sql: "create table decimal_ctas_join_lit as select d.product from (select max(" +
				strings.Repeat("9", 66) + ") as product from nation group by n_regionkey) d cross join nation r",
			wantWidth: 66,
			wantScale: 0,
		},
		{
			// Issue #30938 review round 7: a predicate that cannot cross
			// LIMIT leaves a FILTER node between the projection and the
			// subquery; its identity-mirror outputs resolve exactly like a
			// project's.
			name:      "filtered carrier product crossing a LIMIT clamps to the declared domain",
			sql:       "create table decimal_ctas_filter as select p from (select " + mul + " as p from nation limit 1) d where p > 0",
			wantWidth: 65,
			wantScale: 0,
		},
		{
			name:      "filtered wide literal crossing a LIMIT keeps that literal domain",
			sql:       "create table decimal_ctas_filter_lit as select p from (select " + strings.Repeat("9", 66) + " as p from nation limit 1) d where p > 0",
			wantWidth: 66,
			wantScale: 0,
		},
		{
			// Issue #30938 review round 7, repaired envelope (design §4.1,
			// decision log 82): a LAG/LEAD value/default merge whose
			// parent-domain derivation lands in the 66-76 band persists
			// the reconcile-derived tag exactly — the integer default
			// contributes one integral digit above the (65,65) value
			// domain, deriving (66,65); oversized selected values fail
			// admission at insert.
			name:      "lag over a max-scale decimal with an integer default persists the band tag",
			sql:       "create table decimal_ctas_lag_wide as select lag(cast(0.1 as decimal(65,65)), 0, 1) over (order by n_nationkey) as v from nation",
			wantWidth: 66,
			wantScale: 65,
		},
		{
			// The decimal(3,1) default contributes two integral digits
			// above the (65,64) value domain's one, deriving the band tag
			// (66,64) — the parent-domain reconcile's own derivation,
			// persisted untruncated by the repaired envelope.
			name:      "lead with a decimal default persists the reconcile-derived band tag",
			sql:       "create table decimal_ctas_lead_wide as select lead(cast(0.1 as decimal(65,64)), 0, cast(10 as decimal(3,1))) over (order by n_nationkey) as v from nation",
			wantWidth: 66,
			wantScale: 64,
		},
		{
			// Guard: a default whose own digits fit the value scale adds
			// no integral capacity, so the bound already matches base
			// inference and stays declarable on both sides of the fix.
			name:      "lag with a scale-fitting default keeps the value domain",
			sql:       "create table decimal_ctas_lag_fit as select lag(cast(0.1 as decimal(65,65)), 1, 0.5) over (order by n_nationkey) as v from nation",
			wantWidth: 65,
			wantScale: 65,
		},
		{
			// Issue #30938 review round 8: conditional control arguments
			// (the WHEN condition) never produce result values; only the
			// result-producing branches may contribute to the storage
			// envelope. The boolean condition contributed one integral
			// digit and pushed the persisted width to an undeclarable 66.
			name: "case over a high scale carrier product clamps to the declared domain",
			sql: "create table decimal_ctas_case_hs as select case when n_nationkey > 0 then " +
				"cast(n_nationkey as decimal(65,65)) * cast(n_nationkey as decimal(65,65)) " +
				"else cast(n_nationkey as decimal(65,65)) end as product from nation",
			wantWidth: 65,
			wantScale: 65,
		},
		{
			name: "if over a high scale carrier product clamps to the declared domain",
			sql: "create table decimal_ctas_if_hs as select if(n_nationkey > 0, " +
				"cast(n_nationkey as decimal(65,65)) * cast(n_nationkey as decimal(65,65)), " +
				"cast(n_nationkey as decimal(65,65))) as product from nation",
			wantWidth: 65,
			wantScale: 65,
		},
		{
			// Issue #30938 review round 8: SAMPLE keeps sampled expressions
			// in AggList and surfaces them through self-referencing output
			// slots; the envelope must resolve the sampled producer through
			// that slot mapping.
			name:      "sample of a carrier product clamps to the declared domain",
			sql:       "create table decimal_ctas_sample as select sample(" + mul + ", 100 percent) as product from nation",
			wantWidth: 65,
			wantScale: 0,
		},
		{
			name: "sample over a wide literal keeps that literal domain",
			sql: "create table decimal_ctas_sample_lit as select sample(" + strings.Repeat("9", 66) +
				", 100 percent) as product from nation",
			wantWidth: 66,
			wantScale: 0,
		},
		{
			// Issue #30938 review round 9: TIME_WINDOW exposes aggregate
			// outputs as self-referencing slots addressed through
			// BuildTimeWindowLayout; the envelope must resolve the slot to
			// its aggregate producer instead of keeping the authored
			// carrier width.
			name: "time window aggregate of a carrier product clamps to the declared domain",
			sql: "create table decimal_ctas_twin as select max(" +
				"cast(o_orderkey as decimal(38,0)) * cast(o_custkey as decimal(38,0))" +
				") as product from orders interval(o_orderdate, 1, minute)",
			wantWidth: 65,
			wantScale: 0,
		},
		{
			// FILL mirrors the TIME_WINDOW child projection positionally
			// and forwards it unchanged; the same carrier envelope must
			// flow through.
			name: "filled time window aggregate of a carrier product clamps to the declared domain",
			sql: "create table decimal_ctas_twin_fill as select max(" +
				"cast(o_orderkey as decimal(38,0)) * cast(o_custkey as decimal(38,0))" +
				") as product from orders where o_orderdate > '1992-01-01' and " +
				"o_orderdate < '1998-12-31' interval(o_orderdate, 2, minute) sliding(1, minute) fill(prev)",
			wantWidth: 65,
			wantScale: 0,
		},
		{
			// Guard: a non-multiplication aggregate keeps its own author
			// domain through TIME_WINDOW on both sides of the fix.
			name: "time window aggregate of a decimal column keeps its domain",
			sql: "create table decimal_ctas_twin_col as select max(cast(o_orderkey as decimal(65,0))" +
				") as product from orders interval(o_orderdate, 1, minute)",
			wantWidth: 65,
			wantScale: 0,
		},
		{
			// Guard: the window-partition key is not part of the selected
			// projection but must not confuse the slot mapping.
			name: "time window with a partition key of a carrier product clamps to the declared domain",
			sql: "create table decimal_ctas_twin_part as select max(" +
				"cast(o_orderkey as decimal(38,0)) * cast(o_custkey as decimal(38,0))" +
				") as product from orders group by o_custkey interval(o_orderdate, 1, minute)",
			wantWidth: 65,
			wantScale: 0,
		},
		{
			// Issue #30938 review round 9: a recursive CTE reads its
			// producer through source steps (anchor SINK plus recursive
			// member SINKs); the envelope follows them with bounded cycle
			// handling and retains the anchor's non-carrier envelope.
			name: "recursive CTE anchor of a carrier product clamps to the declared domain",
			sql: "create table decimal_ctas_rcte as with recursive r(n,p) as " +
				"(select 1 as n, cast(o_orderkey as decimal(38,0)) * cast(o_custkey as decimal(38,0))" +
				" as p from orders union all select n+1, p from r where n < 2) " +
				"select p as product from r",
			wantWidth: 65,
			wantScale: 0,
		},
		{
			// The union is shared: two outer readers of the same
			// recursive CTE resolve the same source steps independently.
			name: "shared recursive CTE source of a carrier product clamps to the declared domain",
			sql: "create table decimal_ctas_rcte_shared as with recursive r(n,p) as " +
				"(select 1 as n, cast(o_orderkey as decimal(38,0)) * cast(o_custkey as decimal(38,0))" +
				" as p from orders union all select n+1, p from r where n < 2) " +
				"select a.p as product from r a join r b on a.n = b.n",
			wantWidth: 65,
			wantScale: 0,
		},
		{
			// Issue #30938 review round 10: a recursive member that wraps
			// the recursive column re-enters the same producer expression
			// while resolving that column; expression-level cycle
			// detection closes the path so planning terminates and the
			// anchor's carrier envelope decides the stored domain.
			name: "recursive CTE member abs expression of a carrier product terminates and clamps to the declared domain",
			sql: "create table decimal_ctas_rcte_abs as with recursive r(n,p) as " +
				"(select 1 as n, cast(o_orderkey as decimal(38,0)) * cast(o_custkey as decimal(38,0))" +
				" as p from orders union all select n+1, abs(p) from r where n < 2) " +
				"select p as product from r",
			wantWidth: 65,
			wantScale: 0,
		},
		{
			name: "recursive CTE member coalesce expression of a carrier product terminates and clamps to the declared domain",
			sql: "create table decimal_ctas_rcte_coalesce as with recursive r(n,p) as " +
				"(select 1 as n, cast(o_orderkey as decimal(38,0)) * cast(o_custkey as decimal(38,0))" +
				" as p from orders union all select n+1, coalesce(p, 0) from r where n < 2) " +
				"select p as product from r",
			wantWidth: 65,
			wantScale: 0,
		},
		{
			// Guard: an ordinary (inlined) non-recursive CTE resolves
			// through its project chain exactly like the pre-round-9
			// behavior.
			name: "inline non-recursive CTE of a carrier product clamps to the declared domain",
			sql: "create table decimal_ctas_cte_inline as with c as " +
				"(select cast(o_orderkey as decimal(38,0)) * cast(o_custkey as decimal(38,0))" +
				" as p from orders) select p as product from c",
			wantWidth: 65,
			wantScale: 0,
		},
		{
			// Issue #30938 review round 10: AVG over a widened
			// multiplication carrier keeps the integer capacity the
			// declared domain always had; the metadata widening must not
			// turn a previously answerable 64-integer-digit product into
			// a rejected fractional average. The same published type
			// feeds the CTAS column, the ordinary aggregate and the
			// window aggregate.
			name: "aggregate average of a carrier product keeps the declared capacity",
			sql: "create table decimal_ctas_avg as select avg(" +
				"cast(o_orderkey as decimal(38,0)) * cast(o_custkey as decimal(38,0))" +
				") as product from orders",
			wantWidth: 65,
			wantScale: 0,
		},
		{
			name: "window average of a carrier product keeps the declared capacity",
			sql: "create table decimal_ctas_avg_win as select avg(" +
				"cast(o_orderkey as decimal(38,0)) * cast(o_custkey as decimal(38,0))" +
				") over () as product from orders",
			wantWidth: 65,
			wantScale: 0,
		},
		{
			name: "aggregate average of a scaled carrier product keeps its product scale",
			sql: "create table decimal_ctas_avg_scaled as select avg(" +
				"cast(o_orderkey as decimal(38,4)) * cast(o_custkey as decimal(38,4))" +
				") as product from orders",
			wantWidth: 65,
			wantScale: 8,
		},
		{
			name: "window average of a scaled carrier product keeps its product scale",
			sql: "create table decimal_ctas_avg_win_scaled as select avg(" +
				"cast(o_orderkey as decimal(38,4)) * cast(o_custkey as decimal(38,4))" +
				") over () as product from orders",
			wantWidth: 65,
			wantScale: 8,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logicPlan, err := buildSingleStmt(NewMockOptimizer(false, newPlanTestProcess(t)), t, tc.sql)
			require.NoError(t, err)
			columns := logicPlan.GetDdl().GetCreateTable().GetTableDef().GetCols()
			require.NotEmpty(t, columns)
			require.Equal(t, int32(types.T_decimal256), columns[0].Typ.Id)
			require.Equal(t, tc.wantWidth, columns[0].Typ.Width)
			require.Equal(t, tc.wantScale, columns[0].Typ.Scale)
		})
	}
}

// TestLagLeadDefaultReconcileWidenedDomain pins issue #30938 review round 6:
// LAG/LEAD with an explicit default must reconcile into the widened
// expression domain (the Decimal256 physical carrier) instead of rejecting a
// multiplication that now legitimately publishes 66..76 digits. Persisted
// admission remains a separate concern of the CTAS storage boundary. A merge
// that would exceed the carrier still errors.
func TestLagLeadDefaultReconcileWidenedDomain(t *testing.T) {
	mul := "cast(n_nationkey as decimal(38,0)) * cast(n_nationkey as decimal(38,0))"
	for _, tc := range []struct {
		name      string
		sql       string
		wantWidth int32
		wantScale int32
	}{
		{
			name:      "lag with integer default keeps the carrier width",
			sql:       "select lag(" + mul + ", 1, 0) over (order by n_nationkey) as product from nation",
			wantWidth: 76,
			wantScale: 0,
		},
		{
			name:      "lead with integer default keeps the carrier width",
			sql:       "select lead(" + mul + ", 1, 0) over (order by n_nationkey) as product from nation",
			wantWidth: 76,
			wantScale: 0,
		},
		{
			name: "lag with a decimal default merges to the carrier width",
			sql: "select lag(" + mul + ", 1, cast(" + strings.Repeat("9", 38) +
				" as decimal(38,0))) over (order by n_nationkey) as product from nation",
			wantWidth: 76,
			wantScale: 0,
		},
		{
			name: "lead with a scaled decimal default merges scale within the carrier",
			sql: "select lead(cast(n_nationkey as decimal(38,4)) * cast(n_nationkey as decimal(27,2)), 1, " +
				"cast(1 as decimal(10,8))) over (order by n_nationkey) as product from nation",
			// The product publishes (66,6): the width-38 operand counts 39
			// coefficient digits, so 39 + 27 = 66 with scale min(4+2, 12) = 6.
			// Merged with the (10,8) default: integral max(60,2) + scale 8 = 68.
			wantWidth: 68,
			wantScale: 8,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := runOneExprStmt(NewMockOptimizer(false, newPlanTestProcess(t)), t, tc.sql)
			require.NoError(t, err)
			var expr *planpb.Expr
			for _, node := range stmt.GetQuery().Nodes {
				if node.NodeType == planpb.Node_PROJECT && len(node.ProjectList) == 1 {
					expr = node.ProjectList[0]
				}
			}
			require.NotNil(t, expr)
			require.Equal(t, int32(types.T_decimal256), expr.Typ.Id)
			require.Equal(t, tc.wantWidth, expr.Typ.Width)
			require.Equal(t, tc.wantScale, expr.Typ.Scale)
		})
	}

	t.Run("merge beyond the carrier still errors", func(t *testing.T) {
		// integral max(76,0)+scale 65 = 141 exceeds the Decimal256 carrier.
		_, err := runOneExprStmt(NewMockOptimizer(false, newPlanTestProcess(t)), t,
			"select lag("+mul+", 1, cast(0.1 as decimal(65,65))) over (order by n_nationkey) as product from nation")
		require.Error(t, err)
	})
}

// TestPreparedCTASDecimalMultiplicationStorageBoundary pins issue #30938 for
// execute-time specialization: a DECIMAL(38,0) parameter widens the
// multiplication result to the 76-digit physical carrier, but the refreshed
// persisted column must follow the same declared 65-digit boundary as the
// initial CTAS binding, not the raw specialized expression type.
func TestPreparedCTASDecimalMultiplicationStorageBoundary(t *testing.T) {
	for _, tc := range []struct {
		name        string
		input       string
		source      types.Type
		wantWidth   int32
		wantScale   int32
		runtimeWide int32
	}{
		{
			name:        "scale zero parameter matches the provisional result",
			input:       strings.Repeat("9", 38),
			source:      types.New(types.T_decimal128, 38, 0),
			wantWidth:   65,
			wantScale:   0,
			runtimeWide: 76,
		},
		{
			name:        "scaled parameter specializes the provisional result",
			input:       strings.Repeat("9", 28) + "." + strings.Repeat("9", 10),
			source:      types.New(types.T_decimal128, 38, 10),
			wantWidth:   65,
			wantScale:   10,
			runtimeWide: 76,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := NewMockOptimizer(false, newPlanTestProcess(t))
			proc := mock.ctxt.GetProcess()
			proc.SetPrepareParams(nil)
			sql := "create table decimal_ctas_prepared as select ? * " +
				"cast(n_nationkey as decimal(38,0)) as product from nation"
			stmt, err := mysql.ParseOne(context.Background(), sql, 1)
			require.NoError(t, err)
			defer stmt.Free()
			prepared, err := runOneStmt(mock, t,
				"prepare decimal_ctas_prepared_stmt from '"+strings.ReplaceAll(sql, "'", "''")+"'")
			require.NoError(t, err)
			original := prepared.GetDcl().GetPrepare().Plan
			params := vector.NewVec(types.T_text.ToType())
			defer func() {
				proc.SetPrepareParams(nil)
				params.Free(proc.Mp())
			}()
			require.NoError(t, vector.AppendBytes(params,
				[]byte(tc.input), false, proc.Mp()))
			proc.SetPrepareParams(params)
			value := ParamValue{
				Value:            tc.input,
				SourceType:       tc.source,
				HasSourceType:    true,
				PrepareParamKind: vector.PrepareParamDecimal,
			}
			filled, _, err := FillValuesOfParamsInPlanWithSpecialization(
				context.Background(), original, []any{value})
			require.NoError(t, err)
			// Specialization must have widened the runtime multiplication
			// result to the physical carrier width; the persisted column
			// still narrows through the same boundary as initial binding.
			query := &Plan{Plan: &planpb.Plan_Query{Query: filled.GetDdl().Query}}
			root := query.GetQuery().Nodes[query.GetQuery().Steps[query.GetQuery().MainOutputStep()]]
			require.NotEmpty(t, root.ProjectList)
			require.Equal(t, int32(types.T_decimal256), root.ProjectList[0].Typ.Id)
			require.Equal(t, tc.runtimeWide, root.ProjectList[0].Typ.Width)
			// Forward the published retained-evidence carrier exactly as the
			// production execute path does after PREPARE (read-once claim).
			retained, _ := proc.ClaimDecimalBoundaryEvidence()
			require.NoError(t, RefreshPreparedCTASInferredColumns(context.Background(), filled, original, stmt, retained, function.DefaultDivPrecisionIncrement))
			column := filled.GetDdl().GetCreateTable().TableDef.Cols[0]
			require.Equal(t, int32(types.T_decimal256), column.Typ.Id)
			require.Equal(t, tc.wantWidth, column.Typ.Width)
			require.Equal(t, tc.wantScale, column.Typ.Scale)
		})
	}
}

func TestWideFractionalLiteralMultiplicationPreservesHighBits(t *testing.T) {
	for _, test := range []struct {
		name     string
		literal  string
		expected string
	}{
		{
			name:     "positive literal",
			literal:  "2733892455124775.7851878942123454",
			expected: "3377636369505588272411432821735887.9920303797133694",
		},
		{
			name:     "negative literal",
			literal:  "-2733892455124775.7851878942123454",
			expected: "-3377636369505588272411432821735887.9920303797133694",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			stmt, err := runOneExprStmt(NewMockOptimizer(false, newPlanTestProcess(t)), t,
				"select cast(1235467899687894561 as decimal(19,0)) * "+test.literal)
			require.NoError(t, err)
			expr := stmt.GetQuery().Nodes[1].ProjectList[0]
			require.Equal(t, int32(types.T_decimal256), expr.Typ.Id)
			require.Equal(t, int32(51), expr.Typ.Width)
			require.Equal(t, int32(16), expr.Typ.Scale)

			proc := testutil.NewProc(t)
			defer proc.Free()
			executor, err := colexec.NewExpressionExecutor(proc, expr)
			require.NoError(t, err)
			defer executor.Free()
			result, err := executor.Eval(proc, []*batch.Batch{batch.EmptyForConstFoldBatch}, nil)
			require.NoError(t, err)
			require.False(t, result.IsNull(0))
			actual := vector.GetFixedAtWithTypeCheck[types.Decimal256](result, 0)
			expected, err := types.ParseDecimal256(test.expected, 51, 16)
			require.NoError(t, err)
			require.Equal(t, expected, actual)
		})
	}
}

// TestWideMultiplicationConstantFoldKeepsStorageProvenance pins issue #30938
// review round 9: a statement-constant decimal multiplication published
// wider than the declared 65-digit domain is NOT folded into a scalar
// vector, because the vector representation erases exactly the multiplication
// provenance the CTAS storage boundary keys on (a folded 76-digit product
// and a folded genuine 66-digit literal are indistinguishable). The
// executable expression tree is preserved instead; evaluation results are
// identical. Folds without multiplication provenance still materialize.
func TestWideMultiplicationConstantFoldKeepsStorageProvenance(t *testing.T) {
	lit66 := strings.Repeat("9", 66)
	for _, tc := range []struct {
		name     string
		sql      string
		wantFold bool
	}{
		{
			name:     "wide multiplication carrier keeps the executable tree",
			sql:      "select cast(2 as decimal(65,0)) * cast(3 as decimal(65,0)) as product from nation",
			wantFold: false,
		},
		{
			name:     "wide multiplication under a selector keeps the executable tree",
			sql:      "select +if(true, " + lit66 + ", cast(2 as decimal(65,0)) * cast(3 as decimal(65,0))) as product from nation",
			wantFold: false,
		},
		{
			// Guard: a folded selector without any multiplication still
			// materializes as a scalar vector and binds the genuine wide
			// literal domain.
			name:     "selector over a genuine wide literal still folds",
			sql:      "select +if(false, cast(1 as decimal(65,0)), " + lit66 + ") as product from nation",
			wantFold: true,
		},
		{
			// Guard: narrow products never exceed the declared carrier
			// width, so their constant fold is retained (a narrow
			// Decimal128 product folds to a literal).
			name:     "narrow multiplication still folds",
			sql:      "select cast(2 as decimal(10,0)) * cast(3 as decimal(10,0)) as product from nation",
			wantFold: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			qry, err := buildSingleStmt(NewMockOptimizer(false, newPlanTestProcess(t)), t, tc.sql)
			require.NoError(t, err)
			nodes := qry.GetQuery().GetNodes()
			var root *Node = nil
			for _, n := range nodes {
				if n != nil {
					root = n
				}
			}
			require.NotNil(t, root)
			require.NotEmpty(t, root.ProjectList)
			expr := root.ProjectList[0]
			if tc.wantFold {
				require.True(t, expr.GetVec() != nil || expr.GetLit() != nil,
					"expected a folded constant, got %T", expr.Expr)
			} else {
				require.Nil(t, expr.GetVec(), "expected an executable expression tree, got a folded vector")
				require.NotNil(t, expr.GetF(), "expected a function expression tree, got %T", expr.Expr)
			}
		})
	}
}
