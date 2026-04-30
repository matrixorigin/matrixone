// Copyright 2024 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

// TestExprStructuralHashDistinguishesObjectRef guards against a regression
// where Expr_F was hashed only by ObjName. Two function exprs with identical
// names but different ObjectRef identities (e.g. different overload ids,
// schemas, or databases) must hash and compare as distinct; otherwise
// applyDistributivity can factor them as a common subexpression.
func TestExprStructuralHashDistinguishesObjectRef(t *testing.T) {
	colExpr := &planpb.Expr{
		Typ: planpb.Type{Id: int32(types.T_int64)},
		Expr: &planpb.Expr_Col{
			Col: &planpb.ColRef{RelPos: 0, ColPos: 0, Name: "a"},
		},
	}
	litExpr := MakePlan2Int64ConstExprWithType(1)

	mkFn := func(schema string, obj int64) *planpb.Expr {
		return &planpb.Expr{
			Typ: planpb.Type{Id: int32(types.T_bool)},
			Expr: &planpb.Expr_F{
				F: &planpb.Function{
					Func: &planpb.ObjectRef{
						ObjName:    "foo",
						SchemaName: schema,
						Obj:        obj,
					},
					Args: []*planpb.Expr{DeepCopyExpr(colExpr), DeepCopyExpr(litExpr)},
				},
			},
		}
	}

	a := mkFn("db_a", 100)
	b := mkFn("db_b", 200)

	require.NotEqual(t, exprStructuralHash(a), exprStructuralHash(b),
		"hash must differ for same ObjName but different ObjectRef identity")
	require.False(t, exprStructuralEqual(a, b),
		"equal must return false for same ObjName but different ObjectRef identity")

	// Sanity check: an identical copy must hash + compare equal.
	c := mkFn("db_a", 100)
	require.Equal(t, exprStructuralHash(a), exprStructuralHash(c))
	require.True(t, exprStructuralEqual(a, c))
}
