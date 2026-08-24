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

package productl2

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashbuild"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/stretchr/testify/require"
)

func TestProductL2ReleasesProducerAccountedJoinMap(t *testing.T) {
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", mp)
	proc.SetMessageBoard(message.NewMessageBoard())
	registry, err := mpool.NewAllocationAccountRegistry(1, 1<<12)
	require.NoError(t, err)
	account, err := registry.Open(1 << 30)
	require.NoError(t, err)

	arrayType := types.T_array_float32.ToType()
	arrayType.Width = 2
	build := batch.NewWithSize(2)
	build.Vecs[0] = vector.NewVec(arrayType)
	require.NoError(t, vector.AppendArrayList(
		build.Vecs[0], [][]float32{{0, 0}, {10, 10}}, nil, mp,
	))
	build.Vecs[1] = vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixedList(
		build.Vecs[1], []int64{10, 20}, nil, mp,
	))
	build.SetRowCount(2)

	probe := batch.NewWithSize(1)
	probe.Vecs[0] = vector.NewVec(arrayType)
	require.NoError(t, vector.AppendArrayList(
		probe.Vecs[0], [][]float32{{1, 1}}, nil, mp,
	))
	probe.SetRowCount(1)

	const tag = int32(7001)
	producer := &hashbuild.HashBuild{
		NeedBatches:   true,
		JoinMapTag:    tag,
		JoinMapRefCnt: 1,
	}
	producer.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{build}))
	consumer := &Productl2{
		Result:       []colexec.ResultPos{colexec.NewResultPos(1, 1)},
		OnExpr:       onExprWithProbeCol(0),
		JoinMapTag:   tag,
		VectorOpType: metric.OpType_L2Distance,
	}
	consumer.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{probe}))
	require.NoError(t, producer.SetAllocationAccount(account))
	require.NoError(t, producer.Prepare(proc))
	require.NoError(t, consumer.Prepare(proc))

	_, err = vm.Exec(producer, proc)
	require.NoError(t, err)
	result, err := vm.Exec(consumer, proc)
	require.NoError(t, err)
	require.NotNil(t, result.Batch)
	require.Equal(t, []int64{10}, vector.MustFixedColNoTypeCheck[int64](result.Batch.Vecs[0]))

	consumer.Reset(proc, false, nil)
	producer.Reset(proc, false, nil)
	require.Zero(t, account.Snapshot().Used)
	require.NoError(t, producer.ClearAllocationAccount(account))
	_, _, err = registry.CompleteTerminal(account)
	require.NoError(t, err)
	consumer.Free(proc, false, nil)
	producer.Free(proc, false, nil)
	proc.Free()
	require.Zero(t, mp.CurrNB())
}
