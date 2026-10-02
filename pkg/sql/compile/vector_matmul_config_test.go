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

package compile

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/gpumode"
	"github.com/stretchr/testify/require"
)

func TestConstructVectorMatmulConfig(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	id := &plan.Expr{Typ: plan.Type{Id: int32(types.T_int64)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}}}
	vec := &plan.Expr{Typ: plan.Type{Id: int32(types.T_array_float8), Width: 4}, Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 1}}}
	topk := &plan.Expr{Typ: plan.Type{Id: int32(types.T_int64)}, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I64Val{I64Val: 3}}}}
	queries := plan2.MakePlan2StringConstExprWithType(`[[1,0,0,0]]`)
	options := plan2.MakePlan2StringConstExprWithType(`{"mode":"cpu"}`)
	call := func(args ...*plan.Expr) ([]*plan.Expr, []byte, error) {
		return constructAggregateConfigWithError(&plan.Function{
			Func: &plan.ObjectRef{ObjName: plan2.NameVectorMatmul},
			Args: args,
		}, proc)
	}

	args, config, err := call(topk, id, vec, queries)
	require.NoError(t, err)
	require.Equal(t, []*plan.Expr{id, vec}, args)
	require.Equal(t, aggexec.EncodeVectorMatmulConfig(3, `[[1,0,0,0]]`, "", gpumode.GpuMode), config)

	args, config, err = call(topk, id, vec, queries, options)
	require.NoError(t, err)
	require.Equal(t, []*plan.Expr{id, vec}, args)
	require.Equal(t, aggexec.EncodeVectorMatmulConfig(3, `[[1,0,0,0]]`, `{"mode":"cpu"}`, gpumode.GpuMode), config)

	_, _, err = call(topk, id, vec)
	require.ErrorContains(t, err, "requires 4 or 5 arguments")
	for _, bad := range [][]*plan.Expr{
		{id, id, vec, queries},
		{topk, id, vec, vec},
		{topk, id, vec, queries, vec},
		{queries, id, vec, queries},
	} {
		_, _, err = call(bad...)
		require.Error(t, err)
	}
	nullTopk := &plan.Expr{Typ: plan.Type{Id: int32(types.T_int64)}, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Isnull: true}}}
	_, _, err = call(nullTopk, id, vec, queries)
	require.ErrorContains(t, err, "must not be NULL")
}
