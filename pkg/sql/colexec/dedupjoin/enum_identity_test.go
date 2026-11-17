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

package dedupjoin

import (
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashbuild"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestDedupJoinCopiesEnumIdentity(t *testing.T) {
	proc, ctrl := newCaptureTestProc(t)
	defer ctrl.Finish()
	defer proc.Free()
	intType := types.T_int32.ToType()
	enumType := types.T_enum.ToType()
	enumType.SetEnumEncoding(types.EnumEncodingFramed)
	enumType.SetEnumCollation(types.EnumCollationGeneralCI)
	build := batch.NewWithSize(2)
	build.Attrs = []string{"id", "e"}
	build.Vecs[0], build.Vecs[1] = vector.NewVec(intType), vector.NewVec(enumType)
	for i, value := range []types.Enum{0, 1, 2, 0} {
		require.NoError(t, vector.AppendFixed(build.Vecs[0], int32(i+1), false, proc.Mp()))
		require.NoError(t, vector.AppendFixed(build.Vecs[1], value, i == 3, proc.Mp()))
	}
	build.SetRowCount(4)
	probe := makeInt32Batch(proc.Mp(), [][]int32{{99}}, nil)
	tag++
	conditions := [][]*plan.Expr{{newExpr(0, intType)}, {newExpr(0, intType)}}
	join := &DedupJoin{LeftTypes: []types.Type{intType}, RightTypes: []types.Type{intType, enumType}, Conditions: conditions,
		Result: []colexec.ResultPos{colexec.NewResultPos(1, 1), colexec.NewResultPos(1, 1)}, OnDuplicateAction: plan.Node_FAIL, JoinMapTag: tag,
		OperatorBase: vm.OperatorBase{OperatorInfo: vm.OperatorInfo{Idx: 0}}}
	buildOp := &hashbuild.HashBuild{NeedHashMap: true, NeedBatches: true, Conditions: conditions[1], IsDedup: true, DelColIdx: -1, JoinMapTag: tag, JoinMapRefCnt: 1,
		OperatorBase: vm.OperatorBase{OperatorInfo: vm.OperatorInfo{Idx: 0}}}
	defer join.Free(proc, false, nil)
	defer buildOp.Free(proc, false, nil)
	output := runFinalizeFixture(t, join, buildOp, proc, build, probe)
	require.Len(t, output, 1)
	require.Equal(t, 4, output[0].RowCount())
	for _, v := range output[0].Vecs {
		require.Equal(t, types.EnumEncodingFramed, v.GetType().GetEnumEncoding())
		require.Equal(t, types.EnumCollationGeneralCI, v.GetType().GetEnumCollation())
		require.Equal(t, []types.Enum{0, 1, 2, 0}, vector.MustFixedColNoTypeCheck[types.Enum](v))
		require.True(t, v.IsNull(3))
	}
}
