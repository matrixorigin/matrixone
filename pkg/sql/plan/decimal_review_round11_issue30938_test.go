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
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

// This file pins the four planner regressions of issue #30938 review round
// 11 (pre-pr-review-1793625715469.json, all P2) against approved design
// revision 41 (sha256 ac4827f137577d937bce9ef8ff8740958b19cbbe9a26a3836bb5301617ba1780):
//
//	F1: the storage-boundary rounding envelope re-invokes the parent's own
//	    refineDecimalRoundingReturnType over the resolved parent-side operand
//	    types, so the omitted-digits, negative-digits position capacity,
//	    runtime-digits widening and legacy-precision variants persist exactly
//	    the parent's publication - including statements without
//	    multiplication, where the retired hand mirror regressed base parity.
//	F2: the aggregate-domain reconciliation consumes the statement's session
//	    div_precision_increment, so AVG((a*b)/d) at increments 0/4/10/30
//	    keeps the parent's scale-0/4/10/30 quotient and average instead of
//	    narrowing everything through the default increment 4.
//	F3: source admission reaches below grouped selectors: the aggregate
//	    output self-references (RelPos -1/-2) resolve through their
//	    authoritative producer mappings and MAX/MIN calls admit their
//	    interior, so a violating product is checked before the selector can
//	    discard its row.
//	F4: the dependent-call and capture-domain edges of revision 8
//	    F-DEPENDENT-ENVELOPE (design lines 1442-1466): a scalar dependent
//	    call's envelope resolves through the callee program's root producer
//	    slot, memoized in the statement-local (NodeId, slot) space, so an
//	    outer AVG over a depth-two scalar subquery admits against the parent
//	    product domain and dependent CTAS/view outputs persist the parent's
//	    reconstructed tag.
//
// Every expected tag below is derived from the shared typing owners
// (refineDecimalRoundingReturnType, decimalDivisionReturnType,
// AvgReturnType/SumReturnType and the parent product publication), never
// from observed candidate behavior.

// buildReviewRound11Plan registers the round-11 fixtures (the MOD-boundary
// trio plus m30938r carrying DECIMAL(10,2)/DECIMAL(10,0) rounding operands)
// and runs one statement through the public planner.
func buildReviewRound11Plan(t *testing.T, sql string) *pb.Plan {
	t.Helper()
	proc := newPlanTestProcess(t)
	t.Cleanup(proc.Free)
	mock := NewMockOptimizer(false, proc)
	registerReviewRound11Tables(t, mock)
	built, err := runOneStmt(mock, t, sql)
	require.NoError(t, err)
	return built
}

func registerReviewRound11Tables(t *testing.T, mock *MockOptimizer) {
	t.Helper()
	for _, ddl := range []string{
		"create table m30938s(a decimal(38,10), b decimal(38,10), d decimal(39,0))",
		"create table m30938w(a decimal(38,0), b decimal(38,0), d decimal(39,0))",
		"create table m30938i(c decimal(8,2), mi mediumint, i int, bt bit)",
		"create table m30938r(c decimal(10,2), c0 decimal(10,0), i int)",
	} {
		registerModBoundaryTable(t, mock, ddl)
	}
}

// reviewRound11Optimizer exposes a fixed CompilerContext through the
// Optimizer interface so a statement binds against one session setting
// (the div_precision_increment wrapper of div_precision_increment_test.go).
type reviewRound11Optimizer struct{ ctx CompilerContext }

func (o reviewRound11Optimizer) Optimize(stmt tree.Statement) (*Query, error) {
	built, err := BuildPlan(o.ctx, stmt, false)
	if err != nil {
		return nil, err
	}
	return built.GetQuery(), nil
}

func (o reviewRound11Optimizer) CurrentContext() CompilerContext { return o.ctx }

// findReviewRound11Agg returns the first aggregate call named objName in the
// query's AggList positions.
func findReviewRound11Agg(query *pb.Query, objName string) *pb.Expr {
	for _, node := range query.Nodes {
		if node == nil || node.NodeType != pb.Node_AGG {
			continue
		}
		for _, call := range node.AggList {
			if fn := call.GetF(); fn != nil && fn.Func != nil && fn.Func.GetObjName() == objName {
				return call
			}
		}
	}
	return nil
}

// findReviewRound11Call returns the first function call named objName
// anywhere in the query's expression trees, including window functions.
func findReviewRound11Call(query *pb.Query, objName string) *pb.Expr {
	var found *pb.Expr
	for _, node := range query.Nodes {
		if node == nil || found != nil {
			continue
		}
		_ = pb.VisitExpressionsInOwner(node, func(root *pb.Expr) error {
			return pb.VisitExprTree(root, func(e *pb.Expr) error {
				if found == nil {
					if fn := e.GetF(); fn != nil && fn.Func != nil && fn.Func.GetObjName() == objName {
						found = e
					}
				}
				return nil
			})
		})
	}
	return found
}

// requireEngineCastTo pins an engine-owned (non SQL-authored) cast to the
// given DECIMAL256 target and returns the cast's operand.
func requireEngineCastTo(t *testing.T, expr *pb.Expr, width, scale int32, msgAndArgs ...any) *pb.Expr {
	t.Helper()
	message := append([]any{expr.Typ.String()}, msgAndArgs...)
	fn := expr.GetF()
	require.NotNil(t, fn, message...)
	require.Equal(t, "cast", fn.Func.GetObjName(), message...)
	require.False(t, fn.GetSyntaxExplicitCast(), message...)
	requireDecimalTyp(t, expr.Typ, width, scale, message...)
	return fn.Args[0]
}

// TestDecimalRoundingStorageBoundaryReviewRound11Issue30938 is F1: CTAS and
// view records persist the parent's own rounding publication, recomputed by
// the boundary through refineDecimalRoundingReturnType over the resolved
// parent-side operand types. The non-multiplication cells are base-parity
// shapes the retired hand mirror regressed (it returned the input envelope
// for omitted digits and reconstructed the wrong width for negative and
// runtime digits); the widened-product cells pin the parent-side tag through
// the widening seam.
func TestDecimalRoundingStorageBoundaryReviewRound11Issue30938(t *testing.T) {
	for _, cell := range []struct {
		name  string
		expr  string
		from  string
		id    types.T
		width int32
		scale int32
	}{
		// Omitted digits: digits defaults to 0 and refines whenever the
		// input carries a scale (integerDigits + carry, scale 0).
		{"round omitted digits", "round(c)", "m30938r", types.T_decimal64, 9, 0},
		{"ceiling omitted digits", "ceiling(c)", "m30938r", types.T_decimal64, 9, 0},
		{"floor omitted digits", "floor(c)", "m30938r", types.T_decimal64, 9, 0},
		{"truncate omitted digits", "truncate(c)", "m30938r", types.T_decimal64, 8, 0},
		// Negative digits: the floor/ceil family reserves the position
		// capacity max(integerDigits+1, 1-position); round reserves the
		// non-legacy carry min(integerDigits+1, family cap) while truncate
		// keeps the input width.
		{"ceil negative digits", "ceil(c, -12)", "m30938r", types.T_decimal64, 13, 0},
		{"floor negative digits", "floor(c, -12)", "m30938r", types.T_decimal64, 13, 0},
		{"round negative digits", "round(c0, -12)", "m30938r", types.T_decimal64, 11, 0},
		{"truncate negative digits", "truncate(c0, -12)", "m30938r", types.T_decimal64, 10, 0},
		// Runtime digits: round reserves one carry digit
		// (min(input.Width+1, cap)) and ceil/floor the full family bound,
		// both retaining the input scale.
		{"round runtime digits", "round(c, i)", "m30938r", types.T_decimal64, 11, 2},
		{"ceil runtime digits", "ceil(c, i)", "m30938r", types.T_decimal64, 18, 2},
		// Widened products over DECIMAL(38,10) operands: the parent-side
		// product domain is (65,12), so the rounding tags derive from 53
		// integral digits, never from the widened (76,12) publication.
		{"round widened product", "round(a*b)", "m30938s", types.T_decimal256, 54, 0},
		{"truncate widened product", "truncate(a*b)", "m30938s", types.T_decimal256, 53, 0},
		{"ceil widened product negative digits", "ceil(a*b, -12)", "m30938s", types.T_decimal256, 54, 0},
	} {
		t.Run(cell.name, func(t *testing.T) {
			ctas := buildReviewRound11Plan(t,
				"create table r11f1 as select "+cell.expr+" as v from "+cell.from)
			requireModBoundaryTyp(t, ctas.GetDdl().GetCreateTable().GetTableDef().GetCols()[0].GetTyp(),
				cell.id, cell.width, cell.scale,
				"F1: the CTAS record persists the parent's own rounding tag")
			view := buildReviewRound11Plan(t,
				"create view r11f1v as select "+cell.expr+" as v from "+cell.from)
			requireModBoundaryTyp(t, view.GetDdl().GetCreateView().GetTableDef().GetCols()[0].GetTyp(),
				cell.id, cell.width, cell.scale,
				"F1: the view record persists the parent's own rounding tag")
		})
	}
}

// TestDecimalRoundingEnvelopeLegacyReviewRound11Issue30938 is F1's
// direct-domain cell: the boundary envelope threads the domain's
// legacyPrecision flag into the parent's rule, so the legacy variants keep
// their released metadata exactly. The legacy flag is confined to DUMP/LOAD
// origin binding (function.WithLegacyDecimalIntermediatePrecision), outside
// the plan-test compiler context, so this family drives the statement-scoped
// evaluator directly.
func TestDecimalRoundingEnvelopeLegacyReviewRound11Issue30938(t *testing.T) {
	decimalCol := func(width, scale int32) *pb.Expr {
		typ := types.New(types.T_decimal64, width, scale)
		return &pb.Expr{Typ: makePlan2Type(&typ), Expr: &pb.Expr_Col{Col: &pb.ColRef{RelPos: 0, ColPos: 0}}}
	}
	digitsLiteral := func(digits int64) *pb.Expr {
		typ := types.New(types.T_int64, 20, 0)
		return &pb.Expr{Typ: makePlan2Type(&typ), Expr: &pb.Expr_Lit{Lit: &pb.Literal{
			Value: &pb.Literal_I64Val{I64Val: digits}}}}
	}
	runtimeDigits := func() *pb.Expr {
		typ := types.New(types.T_int64, 20, 0)
		return &pb.Expr{Typ: makePlan2Type(&typ), Expr: &pb.Expr_Col{Col: &pb.ColRef{RelPos: 0, ColPos: 1}}}
	}
	envelopeOf := func(legacy bool, name string, input *pb.Expr, digits *pb.Expr) decimalStorageEnvelope {
		args := []*pb.Expr{input}
		if digits != nil {
			args = append(args, digits)
		}
		d := &ctasStorageDomain{legacyPrecision: legacy}
		return d.roundingEnvelope(name, &pb.Function{Args: args}, nil)
	}
	for _, cell := range []struct {
		name   string
		legacy bool
		fn     string
		input  *pb.Expr
		digits *pb.Expr
		want   decimalStorageEnvelope
	}{
		{"legacy ceil negative digits", true, "ceil", decimalCol(10, 2), digitsLiteral(-12), decimalStorageEnvelope{integral: 9, scale: 0}},
		{"legacy floor negative digits", true, "floor", decimalCol(10, 2), digitsLiteral(-12), decimalStorageEnvelope{integral: 9, scale: 0}},
		{"legacy round negative digits", true, "round", decimalCol(10, 0), digitsLiteral(-12), decimalStorageEnvelope{integral: 10, scale: 0}},
		{"non-legacy ceil negative digits", false, "ceil", decimalCol(10, 2), digitsLiteral(-12), decimalStorageEnvelope{integral: 13, scale: 0}},
		{"non-legacy floor negative digits", false, "floor", decimalCol(10, 2), digitsLiteral(-12), decimalStorageEnvelope{integral: 13, scale: 0}},
		{"non-legacy round negative digits", false, "round", decimalCol(10, 0), digitsLiteral(-12), decimalStorageEnvelope{integral: 11, scale: 0}},
		{"non-legacy round runtime digits", false, "round", decimalCol(10, 2), runtimeDigits(), decimalStorageEnvelope{integral: 9, scale: 2}},
		{"non-legacy ceil runtime digits", false, "ceil", decimalCol(10, 2), runtimeDigits(), decimalStorageEnvelope{integral: 16, scale: 2}},
		{"round omitted digits", false, "round", decimalCol(10, 2), nil, decimalStorageEnvelope{integral: 9, scale: 0}},
		{"truncate omitted digits", false, "truncate", decimalCol(10, 2), nil, decimalStorageEnvelope{integral: 8, scale: 0}},
	} {
		t.Run(cell.name, func(t *testing.T) {
			require.Equal(t, cell.want, envelopeOf(cell.legacy, cell.fn, cell.input, cell.digits),
				"F1: the boundary envelope equals the parent's rounding rule output")
		})
	}
}

// TestDecimalAggregateDivPrecisionReviewRound11Issue30938 is F2: the
// aggregate reconciliation consumes the session div_precision_increment, so
// the division envelope equals the publication the binder computed with the
// same setting and no narrowing cast lands on the quotient. Expectations
// derive from decimalDivisionReturnType (scale = left scale + increment,
// precision capped at 65) and avgDecimal256ReturnType (the +4 fractional
// reserve capped by the input's own integer digits): the average keeps
// exactly the session increment's scale.
func TestDecimalAggregateDivPrecisionReviewRound11Issue30938(t *testing.T) {
	for _, cell := range []struct {
		increment int64
		scale     int32
	}{
		{0, 0},
		{4, 4},
		{10, 10},
		{30, 30},
	} {
		t.Run(fmt.Sprintf("increment %d", cell.increment), func(t *testing.T) {
			proc := newPlanTestProcess(t)
			t.Cleanup(proc.Free)
			mock := NewMockOptimizer(false, proc)
			registerReviewRound11Tables(t, mock)
			cc := divPrecisionCompilerContext{CompilerContext: mock.CurrentContext(), increment: cell.increment}
			built, err := runOneStmt(reviewRound11Optimizer{cc}, t,
				"select avg((a*b)/d) as v from m30938w")
			require.NoError(t, err)
			avg := findReviewRound11Agg(built.GetQuery(), "avg")
			require.NotNil(t, avg, "the statement carries one AVG call")
			requireDecimalTyp(t, avg.Typ, 65, cell.scale,
				"F2: the average keeps the session increment's scale")
			quotient := avg.GetF().Args[0]
			division := quotient.GetF()
			require.NotNil(t, division,
				"F2: the quotient admits without a narrowing cast (actual %s)", quotient.Typ.String())
			require.Equal(t, "/", division.Func.GetObjName(),
				"F2: the AVG argument is the quotient itself, not a reconciliation cast")
			requireDecimalTyp(t, quotient.Typ, 65, cell.scale,
				"F2: the quotient publication keeps the session increment's scale")
			// The widened product inside the quotient carries its own source
			// admission against the parent's (65,0) product domain,
			// independent of the increment.
			product := requireEngineCastTo(t, division.Args[0], 65, 0,
				"F2: the product admits against the parent's (65,0) domain")
			require.Equal(t, "*", product.GetF().Func.GetObjName())
		})
	}
}

// TestDecimalAggregateInteriorAdmissionReviewRound11Issue30938 is F3: a
// grouped selector's widened product is checked at the original source seam
// once an AVG/SUM reader resolves across the row-changing operator, before
// MAX/MIN can discard the violating row. The negative-outlier value family
// runs in the live fixture (a SUM over a group holding -10^65 and 1 must
// error, never silently return 1); here the plan shape pins the interior
// admission cast and the narrow control proves provenance-free products
// stay uncast. The same test pins the boundary of that admission: a bare
// value-selecting aggregate or window call returns its input rows
// carrier-exact, so its argument admits no cast even where the envelope
// diverges from the publication (design §5 §9 value-selecting row; the
// challenge round 8 MIN/MAX/ANY_VALUE and window-MAX cases, which the
// unconditional interior admission of the first F3 repair rejected with
// ERROR 20301 over a carrier-fitting 76-digit product).
func TestDecimalAggregateInteriorAdmissionReviewRound11Issue30938(t *testing.T) {
	for _, name := range []string{"max", "min"} {
		t.Run(name, func(t *testing.T) {
			built := buildReviewRound11Plan(t,
				"select sum(v) from (select "+name+"(a*b) as v from m30938w group by d) q")
			call := findReviewRound11Agg(built.GetQuery(), name)
			require.NotNil(t, call, "the statement carries one %s call", name)
			product := requireEngineCastTo(t, call.GetF().Args[0], 65, 0,
				"F3: the grouped selector's product admits at the source seam")
			require.Equal(t, "*", product.GetF().Func.GetObjName(),
				"F3: the admission cast wraps the product itself")
			sum := findReviewRound11Agg(built.GetQuery(), "sum")
			require.NotNil(t, sum, "the statement carries one SUM call")
			requireDecimalTyp(t, sum.Typ, 65, 0,
				"F3: the outer SUM rebinds over the admitted parent domain")
		})
	}
	t.Run("window selector", func(t *testing.T) {
		built := buildReviewRound11Plan(t,
			"select sum(w) from (select max(a*b) over (partition by d) as w from m30938w) q")
		call := findReviewRound11Call(built.GetQuery(), "max")
		require.NotNil(t, call, "the statement carries one window MAX call")
		product := requireEngineCastTo(t, call.GetF().Args[0], 65, 0,
			"F3: the window selector's product admits at the source seam")
		require.Equal(t, "*", product.GetF().Func.GetObjName())
	})
	t.Run("narrow control", func(t *testing.T) {
		built := buildReviewRound11Plan(t,
			"select sum(v) from (select max(c*mi) as v from m30938i group by i) q")
		call := findReviewRound11Agg(built.GetQuery(), "max")
		require.NotNil(t, call, "the control carries one MAX call")
		head := call.GetF().Args[0].GetF()
		require.NotNil(t, head)
		require.Equal(t, "*", head.Func.GetObjName(),
			"F3 control: a product both engines publish identically stays uncast")
	})
	for _, name := range []string{"max", "min", "any_value"} {
		t.Run("bare "+name+" admits nothing", func(t *testing.T) {
			built := buildReviewRound11Plan(t,
				"select "+name+"(a*b) as v from m30938w group by d")
			call := findReviewRound11Agg(built.GetQuery(), name)
			require.NotNil(t, call, "the statement carries one %s call", name)
			head := call.GetF().Args[0].GetF()
			require.NotNil(t, head,
				"F3 boundary: the bare selector's argument stays the published product, no admission cast")
			require.Equal(t, "*", head.Func.GetObjName(),
				"F3 boundary: the bare selector's argument stays the published product, no admission cast")
			requireDecimalTyp(t, call.Typ, 76, 0,
				"F3 boundary: the bare selector publishes the widened product type carrier-exact")
		})
	}
	t.Run("bare window selector admits nothing", func(t *testing.T) {
		built := buildReviewRound11Plan(t,
			"select max(a*b) over (partition by d) as w from m30938w")
		call := findReviewRound11Call(built.GetQuery(), "max")
		require.NotNil(t, call, "the statement carries one window MAX call")
		head := call.GetF().Args[0].GetF()
		require.NotNil(t, head,
			"F3 boundary: the bare window selector's argument stays the published product, no admission cast")
		require.Equal(t, "*", head.Func.GetObjName(),
			"F3 boundary: the bare window selector's argument stays the published product, no admission cast")
		requireDecimalTyp(t, call.Typ, 76, 0,
			"F3 boundary: the bare window selector publishes the widened product type carrier-exact")
	})
}

// TestDecimalDependentEnvelopeReviewRound11Issue30938 is F4: the outer AVG's
// scalar dependent argument admits against the parent product domain the
// callee program's root producer reconstructs, so the average rebinds to the
// parent's accepted (65,0) instead of AvgReturnType's (65,4) over the
// widened (76,0) publication, and dependent CTAS/view outputs persist the
// parent's reconstructed tag. The depth-two shape mirrors the dependent
// execution route of TestWrappedCorrelatedScalarProjectionAdmission.
func TestDecimalDependentEnvelopeReviewRound11Issue30938(t *testing.T) {
	// The depth-two scalar subquery returns the widened product (76,0) to
	// the outer query; the parent product domain is (65,0).
	depthTwo := "(select (select a*b from m30938w where m30938w.d > m30938i.i limit 1) " +
		"from m30938w as w2 limit 1)"
	t.Run("outer avg", func(t *testing.T) {
		built := buildReviewRound11Plan(t,
			"select avg("+depthTwo+") as v from m30938i")
		require.NoError(t, built.GetQuery().ValidateCorrelation(),
			"the finalized plan keeps the correlation type equalities")
		avg := findReviewRound11Agg(built.GetQuery(), "avg")
		require.NotNil(t, avg, "the statement carries one AVG call")
		requireDecimalTyp(t, avg.Typ, 65, 0,
			"F4: the average rebinds to the parent's accepted (65,0)")
		call := requireEngineCastTo(t, avg.GetF().Args[0], 65, 0,
			"F4: the dependent argument admits against the parent product domain")
		require.NotNil(t, call.GetDependent(),
			"F4: the admitted expression is the scalar dependent call itself")
	})
	t.Run("ctas dependent output", func(t *testing.T) {
		ctas := buildReviewRound11Plan(t,
			"create table r11f4 as select "+depthTwo+" as v from m30938i")
		requireModBoundaryTyp(t, ctas.GetDdl().GetCreateTable().GetTableDef().GetCols()[0].GetTyp(),
			types.T_decimal256, 65, 0,
			"F4: the dependent CTAS output persists the parent's reconstructed tag")
	})
	t.Run("view dependent output", func(t *testing.T) {
		view := buildReviewRound11Plan(t,
			"create view r11f4v as select "+depthTwo+" as v from m30938i")
		requireModBoundaryTyp(t, view.GetDdl().GetCreateView().GetTableDef().GetCols()[0].GetTyp(),
			types.T_decimal256, 65, 0,
			"F4: the dependent view output persists the parent's reconstructed tag")
	})
}
