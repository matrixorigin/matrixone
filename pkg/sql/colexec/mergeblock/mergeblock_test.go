// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package mergeblock

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/process"

	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

type mockRelation struct {
	engine.Relation
	result *batch.Batch
	attrs  []string
}

func (e *mockRelation) Write(_ context.Context, b *batch.Batch, attrs []string) error {
	e.result = b
	e.attrs = attrs
	return nil
}

func TestMergeBlock(t *testing.T) {
	proc := testutil.NewProc(t)
	proc.Ctx = context.TODO()
	segmentid := objectio.NewSegmentid()
	name1 := objectio.BuildObjectName(segmentid, 0)
	name2 := objectio.BuildObjectName(segmentid, 1)
	name3 := objectio.BuildObjectName(segmentid, 2)
	loc1 := blockio.EncodeLocation(name1, objectio.Extent{}, 15, 0)
	loc2 := blockio.EncodeLocation(name2, objectio.Extent{}, 15, 0)
	loc3 := blockio.EncodeLocation(name3, objectio.Extent{}, 15, 0)

	sid1 := loc1.Name().SegmentId()
	blkInfo1 := objectio.BlockInfo{
		BlockID: *objectio.NewBlockid(
			&sid1,
			loc1.Name().Num(),
			loc1.ID()),
	}
	blkInfo1.SetMetaLocation(loc1)

	sid2 := loc2.Name().SegmentId()
	blkInfo2 := objectio.BlockInfo{
		BlockID: *objectio.NewBlockid(
			&sid2,
			loc2.Name().Num(),
			loc2.ID()),
	}
	blkInfo2.SetMetaLocation(loc2)

	sid3 := loc3.Name().SegmentId()
	blkInfo3 := objectio.BlockInfo{
		BlockID: *objectio.NewBlockid(
			&sid3,
			loc3.Name().Num(),
			loc3.ID()),
	}
	blkInfo3.SetMetaLocation(loc3)

	batch1 := &batch.Batch{
		Vecs: []*vector.Vector{
			testutil.MakeTextVector([]string{
				string(objectio.EncodeBlockInfo(&blkInfo1)),
				string(objectio.EncodeBlockInfo(&blkInfo2)),
				string(objectio.EncodeBlockInfo(&blkInfo3))},
				nil, proc.Mp()),
			makeBinaryVector([]string{
				string(objectio.ZeroObjectStats[:]),
				string(objectio.ZeroObjectStats[:]),
				string(objectio.ZeroObjectStats[:])},
				nil, proc.Mp()),
		},
	}
	batch1.SetRowCount(3)

	argument1 := newTestArgument()
	resetChildren(argument1, batch1)

	argument1.OpAnalyzer = process.NewAnalyzer(0, false, false, "mergeblock")
	_, err := vm.Exec(argument1, proc)
	require.NoError(t, err)
	require.Equal(t, uint64(15*3), argument1.container.affectedRows)
	// Check Tbl
	{
		rel := argument1.container.source.(*mockRelation)
		result := rel.result
		// check attr names
		require.True(t, reflect.DeepEqual(
			[]string{catalog.BlockMeta_BlockInfo, catalog.ObjectMeta_ObjectStats},
			rel.attrs,
		))
		// check vector
		require.Equal(t, 2, len(result.Vecs))
		require.Equal(t, 3, result.Vecs[0].Length(), fmt.Sprintf("column number: %d", 0))
		require.Equal(t, 3, result.Vecs[1].Length(), fmt.Sprintf("column number: %d", 1))
	}
	argument1.Free(proc, false, nil)
	argument1.GetChildren(0).Free(proc, false, nil)
	proc.Free()
	require.Equal(t, int64(0), proc.GetMPool().CurrNB())
}

func makeBinaryVector(values []string, _ []uint64, mp *mpool.MPool) *vector.Vector {
	vec := vector.NewVec(types.T_binary.ToType(), mp)
	for _, value := range values {
		if err := vector.AppendBytes(vec, []byte(value), false, mp); err != nil {
			panic(err)
		}
	}
	return vec
}

func resetChildren(arg *MergeBlock, bat *batch.Batch) {
	op := colexec.NewMockOperator().WithBatchs([]*batch.Batch{bat})
	arg.Children = nil
	arg.AppendChild(op)
}

// makeBlockInfoBat builds the insert S3 writer's result: one block of rows
// rows, and its object's stats.
func makeBlockInfoBat(
	t *testing.T,
	proc *process.Process,
	rows uint32,
) *batch.Batch {
	t.Helper()

	segmentID := objectio.NewSegmentid()
	name := objectio.BuildObjectName(segmentID, 0)
	loc := blockio.EncodeLocation(name, objectio.Extent{}, rows, 0)
	blockIDSegment := loc.Name().SegmentId()
	blockInfo := objectio.BlockInfo{
		BlockID: *objectio.NewBlockid(
			&blockIDSegment,
			loc.Name().Num(),
			loc.ID(),
		),
	}
	blockInfo.SetMetaLocation(loc)

	bat := batch.NewWithSize(2)
	bat.Vecs[0] = vector.NewVec(types.T_text.ToType(), proc.Mp())
	bat.Vecs[1] = vector.NewVec(types.T_binary.ToType(), proc.Mp())
	require.NoError(t, vector.AppendBytes(
		bat.Vecs[0], objectio.EncodeBlockInfo(&blockInfo), false, proc.GetMPool(),
	))
	require.NoError(t, vector.AppendBytes(
		bat.Vecs[1], objectio.ZeroObjectStats.Marshal(), false, proc.GetMPool(),
	))
	bat.SetRowCount(1)
	return bat
}

func newTestArgument() *MergeBlock {
	return &MergeBlock{
		container: Container{
			source: &mockRelation{},
			mp:     make(map[int]*batch.Batch),
		},
		AddAffectedRows: true,
	}
}

// Any other layout, such as an older CN's leading table-index column, is
// rejected before anything is appended.
func TestMergeBlockSplitRejectsOtherLayouts(t *testing.T) {
	proc := testutil.NewProc(t)
	arg := newTestArgument()
	bat := batch.NewWithSize(3)
	bat.Vecs[0] = testutil.MakeInt16Vector([]int16{0}, nil, proc.Mp())
	bat.Vecs[1] = testutil.MakeTextVector([]string{"blk"}, nil, proc.Mp())
	bat.Vecs[2] = makeBinaryVector([]string{"stats"}, nil, proc.Mp())
	bat.SetRowCount(1)
	t.Cleanup(func() {
		bat.Clean(proc.GetMPool())
		arg.Free(proc, false, nil)
		proc.Free()
		require.Zero(t, proc.GetMPool().CurrNB())
	})

	require.Error(t, arg.Split(proc, bat, nil))
	require.Nil(t, arg.container.mp[0])
	require.Zero(t, arg.container.affectedRows)
}

func TestMergeBlockSplitReturnsAppendOOM(t *testing.T) {
	for _, preallocateBlockInfo := range []bool{false, true} {
		name := "block-info"
		if preallocateBlockInfo {
			name = "object-stats"
		}
		t.Run(name, func(t *testing.T) {
			procMP, err := mpool.NewMPool(t.Name(), 1<<20, mpool.NoFixed)
			require.NoError(t, err)
			proc := testutil.NewProcessWithMPool(t, "", procMP)
			arg := newTestArgument()
			bat := makeBlockInfoBat(t, proc, 15)
			var filler []byte
			t.Cleanup(func() {
				if filler != nil {
					procMP.Free(filler)
				}
				bat.Clean(procMP)
				arg.Free(proc, false, nil)
				proc.Free()
				currNB := procMP.CurrNB()
				mpool.DeleteMPool(procMP)
				require.Zero(t, currNB)
			})

			if preallocateBlockInfo {
				// Keep capacity in the block-info destination so Split reaches
				// the object-stats append before exhausting the mpool.
				arg.metaLocBat(proc)
				require.NoError(t, vector.AppendBytes(
					arg.container.mp[0].Vecs[0],
					bat.Vecs[0].GetBytesAt(0),
					false,
					procMP,
				))
			}

			remaining := procMP.Cap() - procMP.CurrNB()
			require.Greater(t, remaining, int64(1))
			filler, err = procMP.Alloc(int(remaining-1), true)
			require.NoError(t, err)

			err = arg.Split(proc, bat, nil)
			require.Error(t, err)
			require.Zero(t, arg.container.mp[0].RowCount())
			require.Zero(t, arg.container.affectedRows)

			procMP.Free(filler)
			filler = nil
			require.NoError(t, arg.Split(proc, bat, nil))
			require.Equal(t, 1, arg.container.mp[0].RowCount())
			require.Equal(t, uint64(15), arg.container.affectedRows)
		})
	}
}

// mp[0] is created on first use and emptied, not replaced, afterwards.
func TestMergeBlockMetaLocBatReuse(t *testing.T) {
	arg := newTestArgument()
	proc := testutil.NewProc(t)
	bat := makeBlockInfoBat(t, proc, 15)

	require.NoError(t, arg.Split(proc, bat, nil))
	first := arg.container.mp[0]
	require.Equal(t, 2, len(first.Vecs))
	require.Equal(t, 1, first.RowCount())

	require.NoError(t, arg.Split(proc, bat, nil))
	require.Same(t, first, arg.container.mp[0])
	require.Equal(t, 1, first.RowCount())

	arg.Free(proc, false, nil)
	bat.Clean(proc.GetMPool())
	proc.Free()
	require.Equal(t, int64(0), proc.GetMPool().CurrNB())
}
