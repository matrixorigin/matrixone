// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package loopjoin

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestMarkRandomBytesRetainsPairwiseEvaluation(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	t.Cleanup(func() { proc.Free(); require.Zero(t, proc.Mp().CurrNB()) })
	for _, side := range []int32{0, 1} {
		random, err := function.GetFunctionByName(context.Background(), "random_bytes", []types.Type{types.T_int64.ToType()})
		require.NoError(t, err)
		// This shape arises from random_bytes(lhs.n) = rhs.bytes, not just a literal.
		key := &plan.Expr{Typ: plan.Type{Id: int32(types.T_blob)}, Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{Obj: random.GetEncodedOverloadID(), ObjName: "random_bytes"},
			Args: []*plan.Expr{{Typ: plan.Type{Id: int32(types.T_int64)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: side, ColPos: 0}}}},
		}}}
		cond := markEquality(t, types.T_blob, "=")
		cond.GetF().Args[side] = key
		require.Nil(t, MarkHashKeys(cond), "volatile call count must remain pairwise")
		join := &LoopJoin{JoinType: plan.Node_MARK, NonEqCond: cond}
		require.NoError(t, join.Prepare(proc))
		require.Nil(t, join.ctr.markHash)
		require.NotNil(t, join.ctr.expr, "general evaluator remains installed")
		join.Free(proc, false, nil)
		// A constant argument must also keep an executor rather than a folded vector.
		key.GetF().Args[0] = &plan.Expr{Typ: plan.Type{Id: int32(types.T_int64), NotNullable: true}, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I64Val{I64Val: 1}}}}
		executor, err := colexec.NewExpressionExecutor(proc, key)
		require.NoError(t, err)
		func() {
			defer executor.Free()
			input := batch.NewWithSize(0)
			input.SetRowCount(2)
			vec, err := executor.Eval(proc, []*batch.Batch{input}, nil)
			require.NoError(t, err)
			require.Equal(t, 2, vec.Length())
			require.False(t, vec.IsConst(), "constant arguments must not fold RANDOM_BYTES to one shared draw")
		}()
	}
}
