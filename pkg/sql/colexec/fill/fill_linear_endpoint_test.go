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

package fill

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func linearTestCol() *plan.Expr {
	return &plan.Expr{Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 7, ColPos: 3}}}
}

func linearTestLit() *plan.Expr {
	return &plan.Expr{Expr: &plan.Expr_Lit{Lit: &plan.Literal{}}}
}

func linearTestFn(name string, args ...*plan.Expr) *plan.Expr {
	return &plan.Expr{Expr: &plan.Expr_F{F: &plan.Function{
		Func: &plan.ObjectRef{ObjName: name},
		Args: args,
	}}}
}

func collectLinearColPos(expr *plan.Expr, out *[]int32) {
	switch e := expr.Expr.(type) {
	case *plan.Expr_Col:
		if e.Col.RelPos != -1 {
			panic("relpos not reset")
		}
		*out = append(*out, e.Col.ColPos)
	case *plan.Expr_F:
		for _, arg := range e.F.Args {
			collectLinearColPos(arg, out)
		}
	}
}

// TestResetColRefKeepsLinearEndpointsThroughCasts covers #31661: the LINEAR
// midpoint (left + right) / 2 must read the left endpoint from column 0 and the
// right endpoint from column 1 whatever implicit casts wrap the operands.
func TestResetColRefKeepsLinearEndpointsThroughCasts(t *testing.T) {
	cast := func(e *plan.Expr) *plan.Expr { return linearTestFn("cast", e) }
	midpoint := func(left, right *plan.Expr) *plan.Expr {
		return cast(linearTestFn("/", linearTestFn("+", left, right), linearTestLit()))
	}
	cases := []struct {
		name string
		expr *plan.Expr
	}{
		{"left + right", midpoint(linearTestCol(), linearTestCol())},
		{"cast(left + right)", cast(linearTestFn("+", linearTestCol(), linearTestCol()))},
		{"cast(left) + right", midpoint(cast(linearTestCol()), linearTestCol())},
		{"left + cast(right)", midpoint(linearTestCol(), cast(linearTestCol()))},
		{"cast(left) + cast(right)", midpoint(cast(linearTestCol()), cast(linearTestCol()))},
		{"left + cast(cast(right))", midpoint(linearTestCol(), cast(cast(linearTestCol())))},
		{"cast(cast(left)) + cast(right)", midpoint(cast(cast(linearTestCol())), cast(linearTestCol()))},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			resetColRef(c.expr, 0)
			var got []int32
			collectLinearColPos(c.expr, &got)
			require.Equal(t, []int32{0, 1}, got)
		})
	}
}
