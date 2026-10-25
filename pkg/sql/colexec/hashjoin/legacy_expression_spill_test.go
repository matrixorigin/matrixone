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

package hashjoin

import (
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestLegacyExpressionBucketDoesNotRepartitionWithoutPressure(t *testing.T) {
	for _, depth := range []int{0, spillMaxPass - 1, spillMaxPass} {
		t.Run(fmt.Sprintf("depth-%d", depth), func(t *testing.T) {
			proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
			defer proc.Free()
			require.Nil(t, proc.Base.ImportMemory)
			spillfs, err := proc.GetSpillFileService()
			require.NoError(t, err)
			input := batch.NewOffHeapWithSize(1)
			input.Vecs[0] = testutil.MakeInt64Vector([]int64{-7}, nil, proc.Mp())
			input.SetRowCount(1)
			defer input.Clean(proc.Mp())
			fd, err := spillfs.CreateAndRemoveFile(proc.Ctx, "legacy_expr_"+uuid.NewString())
			require.NoError(t, err)
			writer := &spillBucketWriter{file: fd}
			analyzer := process.NewAnalyzer(0, false, false, "legacy expression")
			fn, err := function.GetFunctionByName(proc.Ctx, "abs", []types.Type{types.T_int64.ToType()})
			require.NoError(t, err)
			key := &plan.Expr{Typ: plan.Type{Id: int32(types.T_int64)}, Expr: &plan.Expr_F{F: &plan.Function{
				Func: &plan.ObjectRef{Obj: fn.GetEncodedOverloadID(), ObjName: "abs"},
				Args: []*plan.Expr{{Typ: plan.Type{Id: int32(types.T_int64)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}}}},
			}}}
			join := &HashJoin{EqConds: [][]*plan.Expr{{key}, {key}}, RightTypes: []types.Type{types.T_int64.ToType()}}
			defer join.Free(proc, false, nil)
			join.ctr.spillThreshold = 1 << 30
			_, err = join.ctr.flushBucketBuffer(proc, input, writer, analyzer)
			require.NoError(t, err)
			jm, repartitioned, err := join.rebuildHashmapForBucket(proc, spillBucket{buildFd: writer.handOffFd(), depth: depth}, analyzer)
			require.NoError(t, err)
			require.False(t, repartitioned, "an unknown import expression bound is not pressure on an ordinary query")
			require.NotNil(t, jm)
			require.Equal(t, int64(1), jm.GetRowCount())
			require.Empty(t, join.ctr.spillQueue)
			jm.Free()
		})
	}
}
