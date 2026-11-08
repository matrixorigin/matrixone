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

package rule

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// A constant tree carrying widened decimal multiplication provenance keeps
// its executable expression shape across this engine's destructive folds:
// folding it to a literal or a scalar vector would erase exactly the
// multiplication provenance the CTAS storage boundary's envelope
// recognition reads (issue #30938, design
// docs/design/20261029-decimal-multiplication-storage-boundary.md,
// revision 41, decision log 54). Constant trees without provenance fold
// unchanged.
func TestDecimalBoundaryFoldGuardPreservesWideProductIssue30938(t *testing.T) {
	proc := testutil.NewProcess(t)
	narrow := types.New(types.T_decimal128, 38, 0)
	mul, err := function.GetFunctionByName(proc.Ctx, "*", []types.Type{narrow, narrow})
	require.NoError(t, err)
	wide := mul.GetReturnType()
	require.Equal(t, types.T_decimal256, wide.Oid)
	require.Greater(t, wide.Width, int32(65),
		"the widened publication past the parent's 65-digit cap is the provenance marker")

	literalOf := func(a int64) *plan.Expr {
		return &plan.Expr{
			Typ: plan.Type{Id: int32(narrow.Oid), Width: narrow.Width, Scale: narrow.Scale},
			Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Decimal128Val{
				Decimal128Val: &plan.Decimal128{A: a},
			}}},
		}
	}
	star := &plan.Expr{
		Typ: plan.Type{Id: int32(wide.Oid), Width: wide.Width, Scale: wide.Scale},
		Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{ObjName: "*", Obj: mul.GetEncodedOverloadID()},
			Args: []*plan.Expr{literalOf(3), literalOf(4)},
		}},
	}

	folded := NewConstantFold(false).constantFold(star, proc)
	require.Same(t, star, folded,
		"the provenance guard returns the executable wide-product tree untouched")
	require.NotNil(t, folded.GetF(), "no literal or scalar vector replaces the product")

	// The identical tree without the widened publication folds base-identically.
	narrowMul, err := function.GetFunctionByName(proc.Ctx, "*",
		[]types.Type{types.New(types.T_decimal64, 18, 0), types.New(types.T_decimal64, 18, 0)})
	require.NoError(t, err)
	narrowType := narrowMul.GetReturnType()
	narrowLit := func() *plan.Expr {
		return &plan.Expr{
			Typ: plan.Type{Id: int32(types.T_decimal64), Width: 18},
			Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Decimal64Val{
				Decimal64Val: &plan.Decimal64{A: 3},
			}}},
		}
	}
	narrowStar := &plan.Expr{
		Typ: plan.Type{Id: int32(narrowType.Oid), Width: narrowType.Width, Scale: narrowType.Scale},
		Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{ObjName: "*", Obj: narrowMul.GetEncodedOverloadID()},
			Args: []*plan.Expr{narrowLit(), narrowLit()},
		}},
	}
	require.False(t, CarriesWideDecimalMultiplicationProvenance(narrowStar))
	require.Nil(t, NewConstantFold(false).constantFold(narrowStar, proc).GetF(),
		"a narrow product keeps the base engine's folded outcome")
}

// MaterializeDecimal256Constant declines a wide scalar whose constant tree
// carries multiplication provenance: materializing it as a scalar vector
// erases the same envelope provenance, and the folded width alone cannot
// distinguish a carrier product from a genuine wide literal (issue #30938
// review round 9, design revision 41 decision log 54). Evaluation is
// unaffected: the un-folded expression computes the same constant at
// runtime.
func TestDecimalBoundaryMaterializeGuardDeclinesWideProductCastIssue30938(t *testing.T) {
	proc := testutil.NewProcess(t)
	narrow := types.New(types.T_decimal128, 38, 0)
	mul, err := function.GetFunctionByName(proc.Ctx, "*", []types.Type{narrow, narrow})
	require.NoError(t, err)
	wide := mul.GetReturnType()
	require.Equal(t, types.T_decimal256, wide.Oid)

	literalOf := func(a int64) *plan.Expr {
		return &plan.Expr{
			Typ: plan.Type{Id: int32(narrow.Oid), Width: narrow.Width, Scale: narrow.Scale},
			Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Decimal128Val{
				Decimal128Val: &plan.Decimal128{A: a},
			}}},
		}
	}
	star := &plan.Expr{
		Typ: plan.Type{Id: int32(wide.Oid), Width: wide.Width, Scale: wide.Scale},
		Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{ObjName: "*", Obj: mul.GetEncodedOverloadID()},
			Args: []*plan.Expr{literalOf(3), literalOf(4)},
		}},
	}
	cast := &plan.Expr{
		Typ: plan.Type{Id: int32(wide.Oid), Width: wide.Width, Scale: wide.Scale},
		Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{ObjName: "cast", Obj: function.EncodeOverloadID(function.CAST, 1)},
			Args: []*plan.Expr{star},
		}},
	}
	require.True(t, function.IsExplicitDecimalCast(cast))
	require.True(t, CanMaterializeConstantVector(cast))
	require.True(t, CarriesWideDecimalMultiplicationProvenance(cast))

	vec, err := vector.NewConstFixed(types.T_decimal256.ToType(), types.Decimal256{B0_63: 12}, 1, proc.Mp())
	require.NoError(t, err)
	defer vec.Free(proc.Mp())
	materialized, err := MaterializeDecimal256Constant(proc, cast, vec)
	require.NoError(t, err)
	require.Nil(t, materialized, "the provenance-bearing tree keeps its executable shape")
}

// The statement-registry key encoders and the fold barrier answer their
// guarded misses for non-call expressions, so provenance-free statements
// fold base-identically at one O(1) membership check (issue #30938, design
// revision 41 decision log entries 52 and 55).
func TestDecimalBoundaryKeyEncodersGuardedMissesIssue30938(t *testing.T) {
	literal := &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_decimal64), Width: 18},
		Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Decimal64Val{
			Decimal64Val: &plan.Decimal64{A: 3},
		}}},
	}
	require.Empty(t, DecimalBoundaryFoldSignature(literal),
		"a non-call expression carries no fold signature")
	require.Empty(t, DecimalBoundaryDecisionLedgerKey(literal),
		"a non-call expression carries no decision ledger key")

	proc := testutil.NewProcess(t)
	release := process.EnterDecimalBoundaryGeneration(proc)
	defer release()
	proc.RecordDecimalBoundaryDecision(
		"ledger-key", &DecimalBoundaryDecisionEntry{Name: "coalesce"}, "barrier-sig", true)
	require.True(t, proc.DecimalBoundaryBarrierActive())
	require.False(t, DecimalBoundaryBarrierHit(proc, literal),
		"a literal node never matches the recorded provenance barrier")
}
