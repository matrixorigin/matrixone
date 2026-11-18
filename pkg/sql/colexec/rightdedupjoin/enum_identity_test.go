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

package rightdedupjoin

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

func TestRightDedupJoinCopiesEnumIdentity(t *testing.T) {
	proc, ctrl := newRightDedupTestProcess(t, true)
	defer ctrl.Finish()
	defer proc.Free()
	intType := types.T_int32.ToType()
	enumType := types.T_enum.ToType()
	enumType.SetEnumEncoding(types.EnumEncodingFramed)
	enumType.SetEnumCollation(types.EnumCollationGeneralCI)
	probe := batch.NewWithSize(2)
	probe.Attrs = []string{"id", "e"}
	probe.Vecs[0], probe.Vecs[1] = vector.NewVec(intType), vector.NewVec(enumType)
	for i, value := range []types.Enum{0, 1, 2, 0} {
		require.NoError(t, vector.AppendFixed(probe.Vecs[0], int32(i+1), false, proc.Mp()))
		require.NoError(t, vector.AppendFixed(probe.Vecs[1], value, i == 3, proc.Mp()))
	}
	probe.SetRowCount(4)
	build := batch.NewWithSize(1)
	build.Attrs = []string{"id"}
	build.Vecs[0] = vector.NewVec(intType)
	require.NoError(t, vector.AppendFixed(build.Vecs[0], int32(99), false, proc.Mp()))
	build.SetRowCount(1)
	tag++
	conditions := [][]*plan.Expr{{newExpr(0, intType)}, {newExpr(0, intType)}}
	join := &RightDedupJoin{LeftTypes: []types.Type{intType, enumType}, RightTypes: []types.Type{intType}, Conditions: conditions,
		Result: []colexec.ResultPos{colexec.NewResultPos(0, 1)}, OnDuplicateAction: plan.Node_FAIL, JoinMapTag: tag}
	join.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{probe}))
	buildOp := &hashbuild.HashBuild{NeedHashMap: true, NeedBatches: false, Conditions: conditions[1], DelColIdx: -1, JoinMapTag: tag, JoinMapRefCnt: 1}
	buildOp.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{build}))
	defer join.Free(proc, false, nil)
	defer buildOp.Free(proc, false, nil)
	installTestAllocation(t, join, buildOp)
	require.NoError(t, buildOp.Prepare(proc))
	require.NoError(t, join.Prepare(proc))
	_, err := vm.Exec(buildOp, proc)
	require.NoError(t, err)
	res, err := vm.Exec(join, proc)
	require.NoError(t, err)
	require.NotNil(t, res.Batch)
	require.Equal(t, 4, res.Batch.RowCount())
	v := res.Batch.Vecs[0]
	require.Equal(t, types.EnumEncodingFramed, v.GetType().GetEnumEncoding())
	require.Equal(t, types.EnumCollationGeneralCI, v.GetType().GetEnumCollation())
	require.Equal(t, []types.Enum{0, 1, 2, 0}, vector.MustFixedColNoTypeCheck[types.Enum](v))
	require.True(t, v.IsNull(3))
}
