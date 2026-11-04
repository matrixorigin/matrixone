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

package compile

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashbuild"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestOptionalIndexPipelineRoundtrip(t *testing.T) {
	ctx := &scopeContext{id: 1, root: &scopeContext{}, parent: &scopeContext{}}
	proc := &process.Process{Base: &process.BaseProcess{}}
	for _, optional := range []bool{false, true} {
		build := &hashbuild.HashBuild{OptionalIndex: optional, NeedHashMap: true, NeedBatches: true, JoinMapTag: 7, JoinMapRefCnt: 3}
		dup := dupOperator(build, 0, 3).(*hashbuild.HashBuild)
		require.Equal(t, optional, dup.OptionalIndex)
		dup.Release()
		_, inst, err := convertToPipelineInstruction(build, proc, ctx, 1)
		require.NoError(t, err)
		data, err := inst.Marshal()
		require.NoError(t, err)
		decoded := &pipeline.Instruction{}
		require.NoError(t, decoded.Unmarshal(data))
		op, err := convertToVmOperator(decoded, ctx, nil)
		require.NoError(t, err)
		got := op.(*hashbuild.HashBuild)
		require.Equal(t, optional, got.OptionalIndex)
		require.True(t, got.NeedHashMap)
		require.True(t, got.NeedBatches)
		require.Equal(t, int32(7), got.JoinMapTag)
		require.Equal(t, int32(3), got.JoinMapRefCnt)
		got.Release()
	}
}
