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
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// This file pins the decline and boundary arms of the issue #30938
// source-domain parent resolver in base_binder.go against approved design
// revision 41 (sha256
// ac4827f137577d937bce9ef8ff8740958b19cbbe9a26a3836bb5301617ba1780).
// Every expectation below derives from the parent's own registered rules
// (decimalMultiplyDomain, decimalDivisionReturnType, DecimalModReturnType,
// setSafeDecimalWidthAndScaleFromSource), never from observed candidate
// behavior: an unresolvable operand or an out-of-domain combination must
// decline (ok=false) so the owning funnel re-arms the family's original
// outcome untouched, and a mirror rule must reproduce the parent's exact
// publication.

func guardArmTyp(oid types.T, width, scale int32) pb.Type {
	return pb.Type{Id: int32(oid), Width: width, Scale: scale}
}

func guardArmColType(oid types.T, width, scale int32, relPos, colPos int32) *pb.Expr {
	return &pb.Expr{
		Typ:  guardArmTyp(oid, width, scale),
		Expr: &pb.Expr_Col{Col: &pb.ColRef{RelPos: relPos, ColPos: colPos}},
	}
}

func guardArmIntLit(value int64) *pb.Expr {
	return &pb.Expr{
		Typ:  guardArmTyp(types.T_int64, 19, 0),
		Expr: &pb.Expr_Lit{Lit: &pb.Literal{Value: &pb.Literal_I64Val{I64Val: value}}},
	}
}

func guardArmWideProduct() *pb.Expr {
	// The design's canonical witness: DECIMAL(38,0) x DECIMAL(38,0)
	// publishes DECIMAL256(76,0) where the parent caps at DECIMAL256(65,0).
	return &pb.Expr{
		Typ: guardArmTyp(types.T_decimal256, 76, 0),
		Expr: &pb.Expr_F{F: &pb.Function{
			Func: &pb.ObjectRef{ObjName: "*"},
			Args: []*pb.Expr{
				guardArmColType(types.T_decimal128, 38, 0, 0, 0),
				guardArmColType(types.T_decimal128, 38, 0, 0, 1),
			},
		}},
	}
}

func guardArmScanNode(nodeID int32) *pb.Node {
	return &pb.Node{NodeType: pb.Node_TABLE_SCAN, NodeId: nodeID}
}

func TestDecimalBoundaryScopeHelpersIssue30938(t *testing.T) {
	t.Run("bind scope attaches nothing without a context or binder", func(t *testing.T) {
		require.Nil(t, withDecimalBoundaryBindScope(nil, &baseBinder{}))
		ctx := context.Background()
		require.Equal(t, ctx, withDecimalBoundaryBindScope(ctx, nil))
	})
	t.Run("bind scope lookup misses on a nil or bare context", func(t *testing.T) {
		require.Nil(t, decimalBoundaryBindScope(nil))
		require.Nil(t, decimalBoundaryBindScope(context.Background()))
	})
	t.Run("div precision increment prefers the live builder", func(t *testing.T) {
		scope := &decimalBoundaryScope{builder: &QueryBuilder{divPrecisionIncrement: 7},
			divPrecisionIncrement: 3, hasDivPrecisionIncrement: true}
		require.Equal(t, int32(7), scope.parentDivPrecisionIncrement())
	})
	t.Run("div precision increment falls back to the engine default", func(t *testing.T) {
		scope := &decimalBoundaryScope{}
		require.Equal(t, function.DefaultDivPrecisionIncrement, scope.parentDivPrecisionIncrement())
	})
	t.Run("lineage scope leaves a nil context or query untouched", func(t *testing.T) {
		require.Nil(t, withDecimalBoundaryLineageScope(nil, &Query{}, 3))
		ctx := context.Background()
		require.Equal(t, ctx, withDecimalBoundaryLineageScope(ctx, nil, 3))
	})
	t.Run("lineage scope preserves an existing binder scope", func(t *testing.T) {
		bctx := &BindContext{}
		existing := &decimalBoundaryScope{bctx: bctx,
			divPrecisionIncrement: 5, hasDivPrecisionIncrement: true}
		base := context.WithValue(context.Background(), decimalBoundaryScopeContextKey{}, existing)
		out := withDecimalBoundaryLineageScope(base, &Query{}, 9)
		scope := decimalBoundaryBindScope(out)
		require.NotNil(t, scope)
		require.Same(t, bctx, scope.bctx, "the binder inventory survives the lineage attach")
		require.NotNil(t, scope.lineage)
		require.Equal(t, int32(5), scope.divPrecisionIncrement,
			"an installed increment is never overwritten by the execute route's value")
		require.True(t, scope.hasDivPrecisionIncrement)
	})
	t.Run("lineage scope installs the execute increment on a scope without one", func(t *testing.T) {
		existing := &decimalBoundaryScope{bctx: &BindContext{}}
		base := context.WithValue(context.Background(), decimalBoundaryScopeContextKey{}, existing)
		out := withDecimalBoundaryLineageScope(base, &Query{}, 9)
		scope := decimalBoundaryBindScope(out)
		require.NotNil(t, scope)
		require.Equal(t, int32(9), scope.divPrecisionIncrement)
		require.True(t, scope.hasDivPrecisionIncrement)
		require.Equal(t, int32(9), scope.parentDivPrecisionIncrement(),
			"the builder-less lineage scope resolves the installed increment")
	})
}

func TestDecimalBoundaryWidenedMultiplyIssue30938(t *testing.T) {
	wide := guardArmTyp(types.T_decimal256, 76, 0)
	t.Run("unknown operand domain declines the provenance test", func(t *testing.T) {
		require.False(t, decimalBoundaryWidenedMultiply("*",
			[]types.Type{types.New(types.T_float64, 0, 0), types.New(types.T_decimal128, 38, 0)}, &wide),
			"a float operand has no exact domain, so the sum cannot convict the publication")
		require.False(t, decimalBoundaryWidenedMultiply("*",
			[]types.Type{types.New(types.T_decimal128, 38, 0), types.New(types.T_varchar, 10, 0)}, &wide))
	})
	t.Run("coefficient-adjusted pair convicts past the cap", func(t *testing.T) {
		// DECIMAL(38,0) x DECIMAL(27,0): domains 39+27=66 > 65.
		require.True(t, decimalBoundaryWidenedMultiply("*",
			[]types.Type{types.New(types.T_decimal128, 38, 0), types.New(types.T_decimal128, 27, 0)}, &wide))
	})
}

func TestDecimalBoundaryRecordDecisionGuardsIssue30938(t *testing.T) {
	proc := newPlanTestProcess(t)
	t.Cleanup(proc.Free)
	release := process.EnterDecimalBoundaryGeneration(proc)
	defer release()

	t.Run("metadata wrapper with a mismatched overload records nothing", func(t *testing.T) {
		bound := &pb.Expr{
			Typ: guardArmTyp(types.T_decimal256, 76, 0),
			Expr: &pb.Expr_F{F: &pb.Function{
				Func: &pb.ObjectRef{ObjName: "*", Obj: 111},
				Args: []*pb.Expr{guardArmIntLit(1), guardArmIntLit(2)},
			}},
		}
		recordDecimalBoundaryDecision(context.Background(), proc, "*", nil, nil, 222, false, bound, nil)
		require.Empty(t, proc.DecimalBoundaryDecisions())
	})
	t.Run("argument-count mismatch records nothing", func(t *testing.T) {
		bound := &pb.Expr{
			Typ: guardArmTyp(types.T_decimal256, 76, 0),
			Expr: &pb.Expr_F{F: &pb.Function{
				Func: &pb.ObjectRef{ObjName: "*", Obj: 111},
				Args: []*pb.Expr{guardArmIntLit(1), guardArmIntLit(2)},
			}},
		}
		recordDecimalBoundaryDecision(context.Background(), proc, "*", nil,
			[]types.Type{types.New(types.T_decimal128, 38, 0)}, 111, false, bound, nil)
		require.Empty(t, proc.DecimalBoundaryDecisions())
	})
}

func TestDecimalBoundaryRetryFunnelGuardsIssue30938(t *testing.T) {
	proc := newPlanTestProcess(t)
	t.Cleanup(proc.Free)
	release := process.EnterDecimalBoundaryGeneration(proc)
	defer release()
	firstErr := errors.New("coalesce: DECIMAL precision 86 exceeds the maximum 76")

	t.Run("operand vector mismatch re-arms the first outcome", func(t *testing.T) {
		got, err, retried := retryDecimalBoundaryFamilyLookup(
			context.Background(), proc, nil, "coalesce",
			[]*Expr{guardArmIntLit(1)}, nil, nil, function.FuncGetResult{}, firstErr)
		require.False(t, retried)
		require.Same(t, firstErr, err)
		require.Equal(t, function.FuncGetResult{}, got)
	})
	t.Run("an unresolvable operand re-arms the first outcome", func(t *testing.T) {
		_, err, retried := retryDecimalBoundaryFamilyLookup(
			context.Background(), proc, nil, "coalesce",
			[]*Expr{nil}, []types.Type{types.New(types.T_decimal128, 38, 0)}, nil,
			function.FuncGetResult{}, firstErr)
		require.False(t, retried)
		require.Same(t, firstErr, err)
	})
	t.Run("string domain modes route the retried lookup", func(t *testing.T) {
		got, err, retried := retryDecimalBoundaryFamilyLookup(
			context.Background(), proc, nil, "coalesce",
			[]*Expr{guardArmWideProduct()},
			[]types.Type{types.New(types.T_decimal128, 38, 0)},
			[]function.StringDomainCheckMode{function.StringDomainCheckKnown},
			function.FuncGetResult{}, firstErr)
		require.NoError(t, err)
		require.True(t, retried, "the widened product carries provenance, so the retry binds")
		require.NotEqual(t, function.FuncGetResult{}, got)
		require.Equal(t, types.T_decimal256, got.GetReturnType().Oid)
		require.Equal(t, int32(65), got.GetReturnType().Width,
			"the parent publication of DECIMAL(38,0) x DECIMAL(38,0) caps at (65,0)")
	})
}

func TestDecimalBoundaryRecordSynthesizedGuardsIssue30938(t *testing.T) {
	t.Run("missing inputs record nothing", func(t *testing.T) {
		recordSynthesizedSetOperationRetryDecision(nil, 1,
			[]types.Type{types.New(types.T_decimal128, 38, 0)},
			[]types.Type{types.New(types.T_decimal128, 38, 0)},
			types.New(types.T_decimal256, 65, 0))
		recordSynthesizedSetOperationRetryDecision(&process.Process{}, 1,
			[]types.Type{types.New(types.T_decimal128, 38, 0)}, nil,
			types.New(types.T_decimal256, 65, 0))
	})
	t.Run("a non-decimal result records nothing", func(t *testing.T) {
		proc := &process.Process{}
		recordSynthesizedSetOperationRetryDecision(proc, 1,
			[]types.Type{types.New(types.T_int64, 19, 0)},
			[]types.Type{types.New(types.T_int64, 19, 0)},
			types.New(types.T_int64, 19, 0))
		require.Empty(t, proc.DecimalBoundaryDecisions())
	})
}

func TestDecimalBoundarySetOperationParentDomainsGuardsIssue30938(t *testing.T) {
	scan := guardArmScanNode(0)
	t.Run("a live process outside a generation declines", func(t *testing.T) {
		parents, ok := decimalBoundarySetOperationParentDomains(
			&process.Process{}, &Query{Nodes: []*pb.Node{scan}}, nil,
			function.DefaultDivPrecisionIncrement,
			[]*pb.Node{scan}, nil, []*Expr{guardArmWideProduct()})
		require.False(t, ok)
		require.Nil(t, parents)
	})
	t.Run("missing query or empty branches decline", func(t *testing.T) {
		_, ok := decimalBoundarySetOperationParentDomains(
			nil, nil, nil, function.DefaultDivPrecisionIncrement,
			[]*pb.Node{scan}, nil, []*Expr{guardArmWideProduct()})
		require.False(t, ok)
		_, ok = decimalBoundarySetOperationParentDomains(
			nil, &Query{Nodes: []*pb.Node{scan}}, nil, function.DefaultDivPrecisionIncrement,
			nil, nil, nil)
		require.False(t, ok)
	})
	t.Run("a nil branch expression declines", func(t *testing.T) {
		_, ok := decimalBoundarySetOperationParentDomains(
			nil, &Query{Nodes: []*pb.Node{scan}}, nil, function.DefaultDivPrecisionIncrement,
			[]*pb.Node{scan}, nil, []*Expr{nil})
		require.False(t, ok)
	})
	t.Run("scan-leaf branches resolve through the lineage view", func(t *testing.T) {
		parents, ok := decimalBoundarySetOperationParentDomains(
			nil, &Query{Nodes: []*pb.Node{scan}}, nil, function.DefaultDivPrecisionIncrement,
			[]*pb.Node{scan}, nil, []*Expr{guardArmWideProduct()})
		require.True(t, ok)
		require.Len(t, parents, 1)
		require.Equal(t, types.T_decimal256, parents[0].Oid)
		require.Equal(t, int32(65), parents[0].Width,
			"the parent publication of DECIMAL(38,0) x DECIMAL(38,0) caps at (65,0)")
	})
}

func TestDecimalBoundaryOperandParentDomainGuardsIssue30938(t *testing.T) {
	t.Run("nil expression declines", func(t *testing.T) {
		_, _, ok := decimalBoundaryOperandParentDomain(nil, nil, nil)
		require.False(t, ok)
	})
	t.Run("column without a payload declines", func(t *testing.T) {
		_, _, ok := decimalBoundaryOperandParentDomain(nil, nil, &pb.Expr{
			Typ:  guardArmTyp(types.T_decimal128, 38, 0),
			Expr: &pb.Expr_Col{Col: nil},
		})
		require.False(t, ok)
	})

	bctx := &BindContext{
		aggregateTag: 1, windowTag: 2, groupTag: 3,
		projectTag: 4, sampleTag: 5, timeTag: 6,
	}
	scope := &decimalBoundaryScope{bctx: bctx}
	tagCases := []struct {
		name   string
		relPos int32
	}{
		{"aggregate inventory out of range", 1},
		{"window inventory out of range", 2},
		{"group inventory out of range", 3},
		{"project inventory out of range", 4},
		{"sample inventory out of range", 5},
		{"time inventory out of range", 6},
	}
	for _, tc := range tagCases {
		t.Run(tc.name, func(t *testing.T) {
			_, _, ok := decimalBoundaryOperandParentDomain(nil, scope,
				guardArmColType(types.T_decimal128, 38, 0, tc.relPos, 0))
			require.False(t, ok, "an empty pending inventory cannot resolve the reference")
		})
	}
	t.Run("window slot without a window function declines", func(t *testing.T) {
		bctx := &BindContext{windowTag: 2, windows: []*pb.Expr{guardArmIntLit(1)}}
		_, _, ok := decimalBoundaryOperandParentDomain(nil, &decimalBoundaryScope{bctx: bctx},
			guardArmColType(types.T_decimal128, 38, 0, 2, 0))
		require.False(t, ok)
	})
	t.Run("time boundary column publishes its identity", func(t *testing.T) {
		bctx := &BindContext{timeTag: 6, times: []*pb.Expr{guardArmIntLit(1)}}
		parent, prov, ok := decimalBoundaryOperandParentDomain(nil, &decimalBoundaryScope{bctx: bctx},
			guardArmColType(types.T_decimal128, 38, 0, 6, 0))
		require.True(t, ok)
		require.False(t, prov)
		require.Equal(t, types.T_decimal128, parent.Oid)
		require.Equal(t, int32(38), parent.Width)
	})
	t.Run("scalar subquery without a builder declines", func(t *testing.T) {
		_, _, ok := decimalBoundaryOperandParentDomain(nil, nil, &pb.Expr{
			Typ:  guardArmTyp(types.T_decimal128, 38, 0),
			Expr: &pb.Expr_Sub{Sub: &pb.SubqueryRef{NodeId: 0}},
		})
		require.False(t, ok)
	})
	t.Run("scalar subquery outside the builder inventory declines", func(t *testing.T) {
		_, _, ok := decimalBoundaryOperandParentDomain(nil,
			&decimalBoundaryScope{builder: &QueryBuilder{}}, &pb.Expr{
				Typ:  guardArmTyp(types.T_decimal128, 38, 0),
				Expr: &pb.Expr_Sub{Sub: &pb.SubqueryRef{NodeId: 0}},
			})
		require.False(t, ok)
	})
	t.Run("window expression without a window function declines", func(t *testing.T) {
		_, _, ok := decimalBoundaryOperandParentDomain(nil, nil, &pb.Expr{
			Typ:  guardArmTyp(types.T_decimal128, 38, 0),
			Expr: &pb.Expr_W{W: &pb.WindowSpec{}},
		})
		require.False(t, ok)
	})
	t.Run("call without a function payload declines", func(t *testing.T) {
		_, _, ok := decimalBoundaryOperandParentDomain(nil, nil, &pb.Expr{
			Typ:  guardArmTyp(types.T_decimal128, 38, 0),
			Expr: &pb.Expr_F{F: &pb.Function{}},
		})
		require.False(t, ok)
	})
	t.Run("admission seam cast passes provenance at the identical domain", func(t *testing.T) {
		// The overload-0 engine-inserted cast whose target equals the
		// operand's parent-published domain passes the operand's
		// provenance through (design §5 fact 16, F-ROW-CHANGING-BOUNDARY).
		seam := &pb.Expr{
			Typ: guardArmTyp(types.T_decimal256, 65, 0),
			Expr: &pb.Expr_F{F: &pb.Function{
				Func: &pb.ObjectRef{ObjName: "cast", Obj: function.EncodeOverloadID(function.CAST, 0)},
				Args: []*pb.Expr{
					guardArmWideProduct(),
					{Expr: &pb.Expr_T{T: &pb.TargetType{}}},
				},
			}},
		}
		parent, prov, ok := decimalBoundaryOperandParentDomain(nil, nil, seam)
		require.True(t, ok)
		require.True(t, prov, "the seam cast forwards the widened product's provenance")
		require.Equal(t, types.T_decimal256, parent.Oid)
		require.Equal(t, int32(65), parent.Width)
	})
}

func TestDecimalBoundaryAggregateParentDomainGuardsIssue30938(t *testing.T) {
	t.Run("non-call expression declines", func(t *testing.T) {
		_, _, ok := decimalBoundaryAggregateParentDomain(nil, nil, guardArmIntLit(1), false)
		require.False(t, ok)
	})
	t.Run("unresolvable value argument declines", func(t *testing.T) {
		fn := &pb.Expr{
			Typ: guardArmTyp(types.T_decimal128, 38, 0),
			Expr: &pb.Expr_F{F: &pb.Function{
				Func: &pb.ObjectRef{ObjName: "sum"},
				Args: []*pb.Expr{nil},
			}},
		}
		_, _, ok := decimalBoundaryAggregateParentDomain(nil, nil, fn, false)
		require.False(t, ok)
	})
	t.Run("sum over a string domain declines", func(t *testing.T) {
		fn := &pb.Expr{
			Typ: guardArmTyp(types.T_decimal128, 38, 0),
			Expr: &pb.Expr_F{F: &pb.Function{
				Func: &pb.ObjectRef{ObjName: "sum"},
				Args: []*pb.Expr{guardArmColType(types.T_varchar, 10, 0, 0, 0)},
			}},
		}
		_, _, ok := decimalBoundaryAggregateParentDomain(nil, nil, fn, false)
		require.False(t, ok, "the parent's SumReturnType admits no VARCHAR domain")
	})
	t.Run("lag with an unresolvable default declines", func(t *testing.T) {
		fn := &pb.Expr{
			Typ: guardArmTyp(types.T_decimal128, 38, 0),
			Expr: &pb.Expr_F{F: &pb.Function{
				Func: &pb.ObjectRef{ObjName: "lag"},
				Args: []*pb.Expr{
					guardArmColType(types.T_decimal128, 38, 0, 0, 0),
					guardArmIntLit(1),
					nil,
				},
			}},
		}
		_, _, ok := decimalBoundaryAggregateParentDomain(nil, nil, fn, false)
		require.False(t, ok)
	})
	t.Run("lag with a domainless default declines the reconcile", func(t *testing.T) {
		anyDefault := &pb.Expr{
			Typ:  guardArmTyp(types.T_any, 0, 0),
			Expr: &pb.Expr_Lit{Lit: &pb.Literal{Isnull: true}},
		}
		fn := &pb.Expr{
			Typ: guardArmTyp(types.T_decimal128, 38, 0),
			Expr: &pb.Expr_F{F: &pb.Function{
				Func: &pb.ObjectRef{ObjName: "lag"},
				Args: []*pb.Expr{
					guardArmColType(types.T_decimal128, 38, 0, 0, 0),
					guardArmIntLit(1),
					anyDefault,
				},
			}},
		}
		_, _, ok := decimalBoundaryAggregateParentDomain(nil, nil, fn, false)
		require.False(t, ok, "a domainless ANY default has no common type to reconcile")
	})
	t.Run("unknown aggregate name declines", func(t *testing.T) {
		fn := &pb.Expr{
			Typ: guardArmTyp(types.T_decimal128, 38, 0),
			Expr: &pb.Expr_F{F: &pb.Function{
				Func: &pb.ObjectRef{ObjName: "foobar"},
				Args: []*pb.Expr{guardArmColType(types.T_decimal128, 38, 0, 0, 0)},
			}},
		}
		_, _, ok := decimalBoundaryAggregateParentDomain(nil, nil, fn, false)
		require.False(t, ok)
	})
}

func TestDecimalBoundaryCallParentDomainGuardsIssue30938(t *testing.T) {
	decimalCol := func() *pb.Expr { return guardArmColType(types.T_decimal128, 38, 0, 0, 0) }
	floatCol := func() *pb.Expr { return guardArmColType(types.T_float64, 0, 0, 0, 0) }
	intCol := func() *pb.Expr { return guardArmColType(types.T_int64, 19, 0, 0, 0) }
	surface := guardArmTyp(types.T_decimal256, 76, 0)
	call := func(name string, args ...*pb.Expr) (types.Type, bool, bool) {
		return decimalBoundaryCallParentDomain(nil, nil, name, &pb.Function{
			Func: &pb.ObjectRef{ObjName: name},
			Args: args,
		}, &surface)
	}

	t.Run("empty argument list declines", func(t *testing.T) {
		_, _, ok := call("+")
		require.False(t, ok)
	})
	t.Run("name_const without a value argument declines", func(t *testing.T) {
		_, _, ok := call("name_const", decimalCol())
		require.False(t, ok)
	})
	t.Run("multiplication over an inexact operand declines", func(t *testing.T) {
		_, _, ok := call("*", floatCol(), decimalCol())
		require.False(t, ok, "a FLOAT operand takes no part in the exact-numeric publication")
	})
	t.Run("addition with one operand declines", func(t *testing.T) {
		_, _, ok := call("+", decimalCol())
		require.False(t, ok)
	})
	t.Run("division over non-decimal operands declines", func(t *testing.T) {
		_, _, ok := call("/", floatCol(), floatCol())
		require.False(t, ok)
	})
	t.Run("mod with one operand declines", func(t *testing.T) {
		_, _, ok := call("mod", decimalCol())
		require.False(t, ok)
	})
	t.Run("mod with an unresolvable left operand declines", func(t *testing.T) {
		_, _, ok := call("mod", nil, decimalCol())
		require.False(t, ok)
	})
	t.Run("mod with an unresolvable right operand declines", func(t *testing.T) {
		_, _, ok := call("mod", decimalCol(), nil)
		require.False(t, ok)
	})
	t.Run("mod over two integer columns declines", func(t *testing.T) {
		_, _, ok := call("mod", intCol(), intCol())
		require.False(t, ok, "the mirror only mirrors a decimal-family MOD publication")
	})
	t.Run("mod with an out-of-domain operand declines", func(t *testing.T) {
		_, _, ok := call("mod", decimalCol(), floatCol())
		require.False(t, ok, "DecimalModReturnType keeps the selected type for FLOAT operands")
	})
	t.Run("rounding over a non-decimal operand declines", func(t *testing.T) {
		_, _, ok := call("round", floatCol())
		require.False(t, ok)
	})
	t.Run("merge over integer-only operands declines", func(t *testing.T) {
		_, _, ok := call("coalesce", intCol(), intCol())
		require.False(t, ok, "no decimal seed exists for an integer-only merge")
	})
	t.Run("legacy precision without builder or process is disabled", func(t *testing.T) {
		require.False(t, decimalBoundaryLegacyPrecision(nil, &decimalBoundaryScope{}))
	})
}

func TestDecimalBoundaryParentMultiplyGuardsIssue30938(t *testing.T) {
	t.Run("unknown operand domain retains the carrier envelope", func(t *testing.T) {
		parent := decimalBoundaryParentMultiply(
			types.New(types.T_float64, 0, 0), types.New(types.T_decimal128, 38, 0))
		require.Equal(t, types.T_decimal128, parent.Oid)
		require.Equal(t, int32(38), parent.Width,
			"the parent's retType keeps the carrier capacity for an unknown domain")
	})
	t.Run("domain width table mirrors decimalMultiplyDomain", func(t *testing.T) {
		cases := []struct {
			typ            types.Type
			width, nominal int32
			ok             bool
		}{
			{types.New(types.T_decimal64, 0, 0), 19, 18, true},
			{types.New(types.T_decimal128, 38, 0), 39, 38, true},
			{types.New(types.T_decimal256, 76, 0), 76, 76, true},
			{types.New(types.T_int8, 3, 0), 3, 3, true},
			{types.New(types.T_bit, 1, 0), 20, 20, true},
			{types.New(types.T_bool, 1, 0), 1, 1, true},
			{types.New(types.T_float64, 0, 0), 0, 0, false},
		}
		for _, tc := range cases {
			width, nominal, ok := decimalMultiplyParentDomainWidth(tc.typ)
			require.Equal(t, tc.width, width, tc.typ.String())
			require.Equal(t, tc.nominal, nominal, tc.typ.String())
			require.Equal(t, tc.ok, ok, tc.typ.String())
		}
	})
}

func TestDecimalBoundaryParentDivisionGuardsIssue30938(t *testing.T) {
	scopeWithIncrement := func(increment int32) *decimalBoundaryScope {
		return &decimalBoundaryScope{divPrecisionIncrement: increment, hasDivPrecisionIncrement: true}
	}
	t.Run("a negative increment clamps to zero", func(t *testing.T) {
		parent := decimalBoundaryParentDivision(scopeWithIncrement(-5),
			types.New(types.T_decimal128, 10, 2), types.New(types.T_decimal128, 5, 1))
		require.Equal(t, types.T_decimal128, parent.Oid)
		require.Equal(t, int32(11), parent.Width)
		require.Equal(t, int32(2), parent.Scale)
	})
	t.Run("an increment beyond thirty clamps to thirty", func(t *testing.T) {
		parent := decimalBoundaryParentDivision(scopeWithIncrement(40),
			types.New(types.T_decimal128, 10, 25), types.New(types.T_decimal128, 5, 5))
		require.Equal(t, types.T_decimal256, parent.Oid)
		require.Equal(t, int32(45), parent.Width)
		require.Equal(t, int32(30), parent.Scale,
			"the scale cap min(s1+increment, 30) fires at 25+30")
	})
	t.Run("the precision floor never drops below the scale", func(t *testing.T) {
		parent := decimalBoundaryParentDivision(scopeWithIncrement(0),
			types.New(types.T_decimal128, 5, 10), types.New(types.T_decimal128, 5, 0))
		require.Equal(t, int32(10), parent.Width)
		require.Equal(t, int32(10), parent.Scale)
	})
}

func TestDecimalBoundaryParentModGuardsIssue30938(t *testing.T) {
	d64 := func(w, s int32) types.Type { return types.New(types.T_decimal64, w, s) }
	d128 := func(w, s int32) types.Type { return types.New(types.T_decimal128, w, s) }
	d256 := func(w, s int32) types.Type { return types.New(types.T_decimal256, w, s) }

	t.Run("undeclared decimal widths fall back to carrier capacity", func(t *testing.T) {
		parent, ok := decimalBoundaryParentMod(d64(0, 0), d64(5, 1))
		require.True(t, ok)
		require.Equal(t, types.T_decimal64, parent.Oid)
		require.Equal(t, int32(18), parent.Width)
		require.Equal(t, int32(1), parent.Scale)

		parent, ok = decimalBoundaryParentMod(d128(0, 0), d64(5, 0))
		require.True(t, ok)
		require.Equal(t, types.T_decimal128, parent.Oid)
		require.Equal(t, int32(38), parent.Width)

		parent, ok = decimalBoundaryParentMod(d256(0, 0), d64(5, 0))
		require.True(t, ok)
		require.Equal(t, types.T_decimal256, parent.Oid)
		require.Equal(t, int32(76), parent.Width)
	})
	t.Run("integer operands contribute their exact digit domains", func(t *testing.T) {
		cases := []struct {
			left, right types.Type
			width       int32
			family      types.T
		}{
			{types.New(types.T_int8, 3, 0), d64(5, 0), 5, types.T_decimal64},
			{types.New(types.T_int16, 5, 0), d64(5, 0), 5, types.T_decimal64},
			{types.New(types.T_int32, 10, 0), d64(5, 0), 10, types.T_decimal64},
			{types.New(types.T_int64, 19, 0), d64(5, 0), 19, types.T_decimal128},
			{types.New(types.T_uint64, 20, 0), d64(5, 0), 20, types.T_decimal128},
			{types.New(types.T_int32, 24, 0), d64(5, 0), 7, types.T_decimal64},
			{types.New(types.T_uint32, 24, 0), d64(5, 0), 8, types.T_decimal64},
			{types.New(types.T_int32, 10, 0), types.New(types.T_int64, 19, 0), 19, types.T_decimal128},
		}
		for _, tc := range cases {
			parent, ok := decimalBoundaryParentMod(tc.left, tc.right)
			require.True(t, ok, tc.left.String())
			require.Equal(t, tc.family, parent.Oid, tc.left.String())
			require.Equal(t, tc.width, parent.Width, tc.left.String())
		}
	})
	t.Run("out-of-domain operands decline", func(t *testing.T) {
		_, ok := decimalBoundaryParentMod(types.New(types.T_float64, 0, 0), d64(5, 0))
		require.False(t, ok)
		// BIT is outside IsInteger, so the mirror declines it exactly as the
		// executor publication's default arm keeps the selected type.
		_, ok = decimalBoundaryParentMod(types.New(types.T_bit, 1, 0), d64(5, 0))
		require.False(t, ok)
	})
	t.Run("a wide precision promotes the family", func(t *testing.T) {
		parent, ok := decimalBoundaryParentMod(d128(39, 0), d64(5, 0))
		require.True(t, ok)
		require.Equal(t, types.T_decimal256, parent.Oid)
		require.Equal(t, int32(39), parent.Width)
	})
	t.Run("mod operand channels", func(t *testing.T) {
		_, _, ok := decimalBoundaryModOperandParentDomain(nil, nil, nil)
		require.False(t, ok)

		big := &pb.Expr{
			Typ:  guardArmTyp(types.T_int64, 19, 0),
			Expr: &pb.Expr_Lit{Lit: &pb.Literal{Value: &pb.Literal_I64Val{I64Val: 9223372036854775807}}},
		}
		parent, _, ok := decimalBoundaryModOperandParentDomain(nil, nil, big)
		require.True(t, ok)
		require.Equal(t, types.T_decimal128, parent.Oid)
		require.Equal(t, int32(19), parent.Width,
			"a nineteen-digit integer literal recovers through the DECIMAL128 channel")
	})
	t.Run("mod literal width guards", func(t *testing.T) {
		explicit := &pb.Expr{
			Typ: guardArmTyp(types.T_decimal128, 38, 0),
			Expr: &pb.Expr_F{F: &pb.Function{
				Func:               &pb.ObjectRef{ObjName: "cast", Obj: function.EncodeOverloadID(function.CAST, 0)},
				SyntaxExplicitCast: true,
				Args:               []*pb.Expr{guardArmIntLit(5), {Expr: &pb.Expr_T{T: &pb.TargetType{}}}},
			}},
		}
		_, ok := decimalBoundaryModIntegerLiteralWidth(explicit)
		require.False(t, ok, "a syntax-explicit cast stops the literal descent")

		nullLit := &pb.Expr{
			Typ:  guardArmTyp(types.T_int64, 19, 0),
			Expr: &pb.Expr_Lit{Lit: &pb.Literal{Isnull: true}},
		}
		_, ok = decimalBoundaryModIntegerLiteralWidth(nullLit)
		require.False(t, ok)

		dangling := &pb.Expr{
			Typ: guardArmTyp(types.T_decimal128, 38, 0),
			Expr: &pb.Expr_F{F: &pb.Function{
				Func: &pb.ObjectRef{ObjName: "cast", Obj: function.EncodeOverloadID(function.CAST, 0)},
				Args: []*pb.Expr{nil},
			}},
		}
		_, ok = decimalBoundaryModIntegerLiteralWidth(dangling)
		require.False(t, ok)
	})
	t.Run("implicit cast source guards", func(t *testing.T) {
		funcless := &pb.Expr{
			Typ:  guardArmTyp(types.T_decimal256, 76, 0),
			Expr: &pb.Expr_F{F: &pb.Function{}},
		}
		require.Nil(t, decimalBoundaryImplicitDecimalCastSource(funcless))

		notCast := &pb.Expr{
			Typ: guardArmTyp(types.T_decimal256, 76, 0),
			Expr: &pb.Expr_F{F: &pb.Function{
				Func: &pb.ObjectRef{ObjName: "abs"},
				Args: []*pb.Expr{guardArmColType(types.T_decimal128, 38, 0, 0, 0)},
			}},
		}
		require.Nil(t, decimalBoundaryImplicitDecimalCastSource(notCast))
	})
	t.Run("mod operand through an implicit alignment cast", func(t *testing.T) {
		castOver := func(source *pb.Expr) *pb.Expr {
			return &pb.Expr{
				Typ: guardArmTyp(types.T_decimal256, 76, 0),
				Expr: &pb.Expr_F{F: &pb.Function{
					Func: &pb.ObjectRef{ObjName: "cast", Obj: function.EncodeOverloadID(function.CAST, 0)},
					Args: []*pb.Expr{source, {Expr: &pb.Expr_T{T: &pb.TargetType{}}}},
				}},
			}
		}
		parent, _, ok := decimalBoundaryModOperandParentDomain(nil, nil,
			castOver(guardArmColType(types.T_int64, 19, 0, 0, 0)))
		require.True(t, ok)
		require.Equal(t, types.T_int64, parent.Oid,
			"the integer source carries the operand's SQL precision, not the storage target")

		_, _, ok = decimalBoundaryModOperandParentDomain(nil, nil,
			castOver(guardArmColType(types.T_float64, 0, 0, 0, 0)))
		require.False(t, ok, "a non-decimal non-integer source declines the mirror")

		parent, _, ok = decimalBoundaryModOperandParentDomain(nil, nil,
			castOver(guardArmColType(types.T_decimal128, 38, 0, 0, 0)))
		require.True(t, ok)
		require.Equal(t, types.T_decimal128, parent.Oid)
		require.Equal(t, int32(38), parent.Width)
	})
}

func TestDecimalBoundaryParentMergeGuardsIssue30938(t *testing.T) {
	d64 := func(w, s int32) types.Type { return types.New(types.T_decimal64, w, s) }
	d128 := func(w, s int32) types.Type { return types.New(types.T_decimal128, w, s) }
	d256 := func(w, s int32) types.Type { return types.New(types.T_decimal256, w, s) }

	t.Run("integer arms contribute their integral widths", func(t *testing.T) {
		cases := []struct {
			integer types.Type
			width   int32
		}{
			{types.New(types.T_int8, 3, 0), 18},
			{types.New(types.T_int16, 5, 0), 18},
			{types.New(types.T_int32, 10, 0), 18},
			{types.New(types.T_uint64, 20, 0), 20},
		}
		for _, tc := range cases {
			merged, ok := decimalBoundaryParentMerge([]types.Type{d64(18, 0), tc.integer})
			require.True(t, ok, tc.integer.String())
			require.Equal(t, tc.width, merged.Width, tc.integer.String())
			require.Equal(t, int32(0), merged.Scale)
		}
	})
	t.Run("merge without a decimal seed declines", func(t *testing.T) {
		_, ok := decimalBoundaryParentMerge([]types.Type{types.New(types.T_varchar, 10, 0)})
		require.False(t, ok)
		_, ok = decimalBoundaryParentMerge([]types.Type{types.New(types.T_int64, 19, 0)})
		require.False(t, ok, "integers alone admit no decimal family seed")
	})
	t.Run("decimal64 seed climbs the family ladder", func(t *testing.T) {
		merged, ok := decimalBoundaryParentMerge([]types.Type{d64(18, 0), d64(18, 30)})
		require.True(t, ok)
		require.Equal(t, types.T_decimal256, merged.Oid)
		require.Equal(t, int32(48), merged.Width)
		require.Equal(t, int32(30), merged.Scale)
	})
	t.Run("decimal128 seed climbs past 38", func(t *testing.T) {
		merged, ok := decimalBoundaryParentMerge([]types.Type{d128(38, 0), d64(18, 30)})
		require.True(t, ok)
		require.Equal(t, types.T_decimal256, merged.Oid)
		require.Equal(t, int32(68), merged.Width)
	})
	t.Run("a requirement beyond the carrier declines", func(t *testing.T) {
		_, ok := decimalBoundaryParentMerge([]types.Type{d256(76, 0), d64(18, 38)})
		require.False(t, ok, "integral 76 plus scale 38 exceeds the 76-digit carrier")
	})
}

func TestDecimalBoundaryRetryLagLeadGuardsIssue30938(t *testing.T) {
	proc := newPlanTestProcess(t)
	t.Cleanup(proc.Free)
	release := process.EnterDecimalBoundaryGeneration(proc)
	defer release()

	t.Run("unresolvable value argument declines", func(t *testing.T) {
		_, ok := retryLagLeadReconcileOverParentDomains(context.Background(), proc, nil,
			[]*Expr{
				{Typ: guardArmTyp(types.T_decimal128, 38, 0), Expr: &pb.Expr_Col{Col: nil}},
				guardArmIntLit(1),
				guardArmIntLit(0),
			})
		require.False(t, ok)
	})
	t.Run("unresolvable default argument declines", func(t *testing.T) {
		_, ok := retryLagLeadReconcileOverParentDomains(context.Background(), proc, nil,
			[]*Expr{
				guardArmColType(types.T_decimal128, 38, 0, 0, 0),
				guardArmIntLit(1),
				{Typ: guardArmTyp(types.T_decimal128, 38, 0), Expr: &pb.Expr_Col{Col: nil}},
			})
		require.False(t, ok)
	})
}
