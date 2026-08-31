// Copyright 2025 Matrix Origin
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

package ivfflat

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func litF64(v float64) *plan.Expr {
	return &plan.Expr{Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Dval{Dval: v}}}}
}

func litNull() *plan.Expr {
	return &plan.Expr{Expr: &plan.Expr_Lit{Lit: &plan.Literal{Isnull: true}}}
}

// A prepared distance bound may legally bind NULL. Once the predicate is peeled out of
// the filter list the range is its ONLY consumer, so the range has to reproduce what
// the residual filter used to do: `distance < NULL` is UNKNOWN for every row, which
// selects nothing. Reporting "did not fold to a numeric literal" instead turns a valid
// query into an error.
func TestVectorDistanceBoundNull(t *testing.T) {
	t.Run("a NULL bound is reported as null, not as a failure", func(t *testing.T) {
		for _, bt := range []plan.BoundType{plan.BoundType_INCLUSIVE, plan.BoundType_EXCLUSIVE} {
			v, has, isNull, err := vectorDistanceBound(bt, litNull())
			require.NoError(t, err, "a NULL bound is a value, not a malformed plan")
			require.True(t, isNull)
			require.False(t, has)
			require.Zero(t, v)
		}
	})

	t.Run("an ordinary bound is unchanged", func(t *testing.T) {
		v, has, isNull, err := vectorDistanceBound(plan.BoundType_EXCLUSIVE, litF64(2.5))
		require.NoError(t, err)
		require.True(t, has)
		require.False(t, isNull)
		require.InDelta(t, 2.5, v, 1e-9)
	})

	t.Run("unbounded stays unbounded", func(t *testing.T) {
		_, has, isNull, err := vectorDistanceBound(plan.BoundType_UNBOUNDED, nil)
		require.NoError(t, err)
		require.False(t, has)
		require.False(t, isNull)
	})

	t.Run("a genuinely unfoldable bound is still an error", func(t *testing.T) {
		// Distinct from NULL: this one means the plan handed the reader something it
		// cannot evaluate, which is a defect rather than a value.
		col := &plan.Expr{Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 0, ColPos: 1}}}
		_, _, isNull, err := vectorDistanceBound(plan.BoundType_EXCLUSIVE, col)
		require.Error(t, err)
		require.False(t, isNull, "a non-literal is not the same as a NULL literal")
	})
}
