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

package odkueval

import (
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/stretchr/testify/require"
)

func TestDirectNotNullRequiresExactShape(t *testing.T) {
	typ := types.T_int64.ToType()
	col := &plan.Expr{Typ: plan.Type{Id: int32(typ.Oid)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 0, ColPos: 0}}}
	notNull := &plan.Expr{Typ: plan.Type{Id: int32(types.T_bool)}, Expr: &plan.Expr_F{F: &plan.Function{
		Func: &plan.ObjectRef{Obj: int64(function.ISNOTNULL) << 32, ObjName: "isnotnull"}, Args: []*plan.Expr{col},
	}}}
	expr := &plan.Expr{Typ: plan.Type{Id: int32(types.T_bool)}, Expr: &plan.Expr_F{F: &plan.Function{
		Func: &plan.ObjectRef{Obj: int64(function.CHECK_CONSTRAINT_ASSERT) << 32, ObjName: "_check_constraint_assert"},
		Args: []*plan.Expr{notNull, {Typ: plan.Type{Id: int32(types.T_varchar)}, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Sval{Sval: "required"}}}}},
	}}}
	_, ok := directNotNullCheck(expr, []types.Type{typ})
	require.True(t, ok)
	for _, change := range []func(*plan.Expr){
		func(e *plan.Expr) { e.GetF().Args[0].GetF().Args[0].GetCol().RelPos = 1 },
		func(e *plan.Expr) {
			e.GetF().Args[1] = &plan.Expr{Typ: plan.Type{Id: int32(types.T_int64)}, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I64Val{I64Val: 0}}}}
		},
		func(e *plan.Expr) { e.GetF().Args[1].GetLit().Isnull = true },
		func(e *plan.Expr) { e.GetF().Args[0].GetF().Args[0].Typ.Width++ },
	} {
		modified := proto.Clone(expr).(*plan.Expr)
		change(modified)
		_, ok := directNotNullCheck(modified, []types.Type{typ})
		require.False(t, ok)
	}
}
