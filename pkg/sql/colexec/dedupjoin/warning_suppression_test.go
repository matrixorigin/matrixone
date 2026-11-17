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
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashbuild"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/stretchr/testify/require"
)

func TestDuplicateWarningSuppressedUsesRetainedBatch(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
	proc.Base.Lim.SpillSize = 64 << 20
	op := mock_frontend.NewMockTxnOperator(ctrl)
	op.EXPECT().Txn().Return(txn.TxnMeta{Mode: txn.TxnMode_Pessimistic}).AnyTimes()
	proc.Base.TxnOperator = op
	proc.SetMessageBoard(message.NewMessageBoard())

	keyType := types.T_int32.ToType()
	first := batch.NewWithSize(2)
	first.Vecs[0] = vector.NewVec(keyType)
	first.Vecs[1] = vector.NewVec(keyType)
	second := batch.NewWithSize(2)
	second.Vecs[0] = vector.NewVec(keyType)
	second.Vecs[1] = vector.NewVec(keyType)
	probeBat := batch.NewWithSize(1)
	probeBat.Vecs[0] = vector.NewVec(keyType)
	var join *DedupJoin
	var build *hashbuild.HashBuild
	var allocation *mpool.AllocationAccount
	defer func() {
		if join != nil {
			join.Free(proc, false, nil)
		}
		if build != nil {
			build.Free(proc, false, nil)
		}
		first.Clean(proc.Mp())
		second.Clean(proc.Mp())
		probeBat.Clean(proc.Mp())
		proc.Free()
		require.Zero(t, proc.Mp().CurrNB())
		if allocation != nil {
			require.Zero(t, allocation.Snapshot().Used)
		}
	}()

	for i := 0; i < colexec.DefaultBatchSize; i++ {
		// The NULL key keeps this a compact map with fewer groups than rows.
		require.NoError(t, vector.AppendFixed(first.Vecs[0], int32(i), i == 0, proc.Mp()))
		require.NoError(t, vector.AppendFixed(first.Vecs[1], int32(1), false, proc.Mp()))
	}
	first.SetRowCount(colexec.DefaultBatchSize)
	require.NoError(t, vector.AppendFixed(second.Vecs[0], int32(colexec.DefaultBatchSize), false, proc.Mp()))
	// This NULL marker belongs to the second retained batch. A global-row lookup
	// incorrectly inspects the non-NULL marker at the same offset in batch zero.
	require.NoError(t, vector.AppendFixed(second.Vecs[1], int32(0), true, proc.Mp()))
	second.SetRowCount(1)
	require.NoError(t, vector.AppendFixed(probeBat.Vecs[0], int32(-1), false, proc.Mp()))
	probeBat.SetRowCount(1)

	condition := newExpr(0, keyType)
	condition.GetCol().RelPos = 1
	mapTag := int32(918273)
	join = &DedupJoin{
		LeftTypes: []types.Type{keyType}, RightTypes: []types.Type{keyType, keyType},
		Conditions:        [][]*plan.Expr{{newExpr(0, keyType)}, {condition}},
		Result:            []colexec.ResultPos{colexec.NewResultPos(1, 0)},
		OnDuplicateAction: plan.Node_IGNORE, DedupColName: "uk_named",
		DedupColTypes: []plan.Type{condition.Typ}, JoinMapTag: mapTag,
		WarningSuppressNullColIdx: 2,
		NumCPU:                    1, IsMerger: true,
		DelColIdx: -1,
	}
	build = &hashbuild.HashBuild{
		NeedHashMap: true, NeedBatches: true, Conditions: join.Conditions[1],
		IsDedup: true, OnDuplicateAction: plan.Node_IGNORE, DelColIdx: -1,
		DedupColName: join.DedupColName, DedupColTypes: join.DedupColTypes,
		JoinMapTag: mapTag, JoinMapRefCnt: 1,
	}
	join.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{probeBat}))
	build.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{first, second}))
	allocation = installTestAllocation(t, join, build)
	require.NoError(t, build.Prepare(proc))
	require.NoError(t, join.Prepare(proc))
	_, err := vm.Exec(build, proc)
	require.NoError(t, err)
	res, err := vm.Exec(join, proc)
	require.NoError(t, err)
	if res.Batch != nil {
		res.Batch.Clean(proc.Mp())
	}
	require.NotNil(t, join.ctr.mp)
	mapBatches := join.ctr.batches
	require.NotEmpty(t, mapBatches)
	// HashBuild may coalesce its inputs. Install an explicit two-batch view of
	// the same rows to exercise the compact-map batch-local lookup directly.
	join.ctr.batches = []*batch.Batch{first, second}
	defer func() { join.ctr.batches = mapBatches }()
	require.EqualValues(t, colexec.DefaultBatchSize+1, join.ctr.batchRowCount)
	// IGNORE maps omit row selections. The NULL key makes the group count one
	// less than the row count and therefore selects the compact-map lookup.
	require.NotEqual(t, uint64(join.ctr.batchRowCount), join.ctr.mp.GetGroupCount())

	key := vector.NewVec(keyType)
	defer key.Free(proc.Mp())
	require.NoError(t, vector.AppendFixed(key, int32(colexec.DefaultBatchSize), false, proc.Mp()))
	itr := join.ctr.mp.NewIterator()
	groups, nonNull, err := itr.Find(0, 1, []*vector.Vector{key})
	require.NoError(t, err)
	require.NotZero(t, nonNull[0])
	group := groups[0]
	hashmap.IteratorClearOwner(itr)
	require.NotZero(t, group)
	require.Empty(t, join.ctr.mp.GetSels(group))

	suppressed, err := join.ctr.duplicateWarningSuppressed(join, proc, group)
	require.NoError(t, err)
	require.True(t, suppressed, "the NULL marker must come from the second retained batch")
}
