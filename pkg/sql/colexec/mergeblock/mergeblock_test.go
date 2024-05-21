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

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/process"

	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/value_scan"
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
}

func (e *mockRelation) Write(_ context.Context, b *batch.Batch) error {
	e.result = b
	return nil
}

func TestMergeBlock(t *testing.T) {
	proc := testutil.NewProc()
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
		SegmentID: sid1,
		//non-appendable block
		EntryState: false,
	}
	blkInfo1.SetMetaLocation(loc1)

	sid2 := loc2.Name().SegmentId()
	blkInfo2 := objectio.BlockInfo{
		BlockID: *objectio.NewBlockid(
			&sid2,
			loc2.Name().Num(),
			loc2.ID()),
		SegmentID: sid2,
		//non-appendable block
		EntryState: false,
	}
	blkInfo2.SetMetaLocation(loc2)

	sid3 := loc3.Name().SegmentId()
	blkInfo3 := objectio.BlockInfo{
		BlockID: *objectio.NewBlockid(
			&sid3,
			loc3.Name().Num(),
			loc3.ID()),
		SegmentID: sid3,
		//non-appendable block
		EntryState: false,
	}
	blkInfo3.SetMetaLocation(loc3)

	batch1 := &batch.Batch{
		Attrs: []string{catalog.BlockMeta_TableIdx_Insert, catalog.BlockMeta_BlockInfo, catalog.ObjectMeta_ObjectStats},
		Vecs: []*vector.Vector{
			testutil.MakeInt16Vector([]int16{0, 0, 0}, nil),
			testutil.MakeTextVector([]string{
				string(objectio.EncodeBlockInfo(blkInfo1)),
				string(objectio.EncodeBlockInfo(blkInfo2)),
				string(objectio.EncodeBlockInfo(blkInfo3))},
				nil),
			testutil.MakeTextVector([]string{
				string(objectio.ZeroObjectStats[:]),
				string(objectio.ZeroObjectStats[:]),
				string(objectio.ZeroObjectStats[:])},
				nil),
		},
		Cnt: 1,
	}
	batch1.SetRowCount(3)

	argument1 := Argument{
		container: &Container{
			source: &mockRelation{},
			mp:     make(map[int]*batch.Batch),
			mp2:    make(map[int][]*batch.Batch),
		},
		//Unique_tbls:  []engine.Relation{&mockRelation{}, &mockRelation{}},
		affectedRows: 0,
		OperatorBase: vm.OperatorBase{
			OperatorInfo: vm.OperatorInfo{
				Idx:     0,
				IsFirst: false,
				IsLast:  false,
			},
		},
		AddAffectedRows: true,
	}
	resetChildren(&argument1, batch1)

	// argument1.Prepare(proc)
	_, err := argument1.Call(proc)
	require.NoError(t, err)
	require.Equal(t, uint64(15*3), argument1.affectedRows)
	// Check Tbl
	{
		result := argument1.container.source.(*mockRelation).result
		// check attr names
		require.True(t, reflect.DeepEqual(
			[]string{catalog.BlockMeta_BlockInfo, catalog.ObjectMeta_ObjectStats},
			result.Attrs,
		))
		// check vector
		require.Equal(t, 2, len(result.Vecs))
		//for i, vec := range result.Vecs {
		require.Equal(t, 3, result.Vecs[0].Length(), fmt.Sprintf("column number: %d", 0))
		require.Equal(t, 3, result.Vecs[1].Length(), fmt.Sprintf("column number: %d", 1))
		//}
	}
	// Check UniqueTables
	//for j := range argument1.Unique_tbls {
	//	tbl := argument1.Unique_tbls[j]
	//	result := tbl.(*mockRelation).result
	//	// check attr names
	//	require.True(t, reflect.DeepEqual(
	//		[]string{catalog.BlockMeta_MetaLoc},
	//		result.Attrs,
	//	))
	//	// check vector
	//	require.Equal(t, 1, len(result.Vecs))
	//	for i, vec := range result.Vecs {
	//		require.Equal(t, 1, vec.Length(), fmt.Sprintf("column number: %d", i))
	//	}
	//}
	argument1.Free(proc, false, nil)
	for k := range argument1.container.mp {
		argument1.container.mp[k].Clean(proc.GetMPool())
	}
	argument1.GetChildren(0).Free(proc, false, nil)
	proc.FreeVectors()
	require.Equal(t, int64(0), proc.GetMPool().CurrNB())
}

func resetChildren(arg *Argument, bat *batch.Batch) {
	arg.SetChildren(
		[]vm.Operator{
			&value_scan.Argument{
				Batchs: []*batch.Batch{bat},
			},
		})
}

func mockBlockInfoBat(proc *process.Process, withStats bool) *batch.Batch {
	var attrs []string
	if withStats {
		attrs = []string{catalog.BlockMeta_TableIdx_Insert, catalog.BlockMeta_BlockInfo, catalog.ObjectMeta_ObjectStats}
	} else {
		attrs = []string{catalog.BlockMeta_TableIdx_Insert, catalog.BlockMeta_BlockInfo}
	}

	blockInfoBat := batch.NewWithSize(len(attrs))
	blockInfoBat.Attrs = attrs
	blockInfoBat.Vecs[0] = proc.GetVector(types.T_int16.ToType())
	blockInfoBat.Vecs[1] = proc.GetVector(types.T_text.ToType())

	if withStats {
		blockInfoBat.Vecs[2], _ = vector.NewConstBytes(types.T_binary.ToType(),
			objectio.ZeroObjectStats.Marshal(), 1, proc.GetMPool())
	}

	return blockInfoBat
}

func TestArgument_GetMetaLocBat(t *testing.T) {
	arg := Argument{
		container: &Container{
			source: &mockRelation{},
			mp:     make(map[int]*batch.Batch),
			mp2:    make(map[int][]*batch.Batch),
		},
		//Unique_tbls:  []engine.Relation{&mockRelation{}, &mockRelation{}},
		affectedRows: 0,
		OperatorBase: vm.OperatorBase{
			OperatorInfo: vm.OperatorInfo{
				Idx:     0,
				IsFirst: false,
				IsLast:  false,
			},
		},
		AddAffectedRows: true,
	}

	proc := testutil.NewProc()
	proc.Ctx = context.TODO()

	// arg.Prepare(proc)

	bat := mockBlockInfoBat(proc, true)
	arg.GetMetaLocBat(bat, proc)

	require.Equal(t, 2, len(arg.container.mp[0].Vecs))

	arg.container.mp[0].Clean(proc.GetMPool())
	bat.Clean(proc.GetMPool())

	bat = mockBlockInfoBat(proc, false)
	arg.GetMetaLocBat(bat, proc)

	require.Equal(t, 2, len(arg.container.mp[0].Vecs))

	arg.Free(proc, false, nil)
	for k := range arg.container.mp {
		arg.container.mp[k].Clean(proc.GetMPool())
	}

	proc.FreeVectors()
	bat.Clean(proc.GetMPool())
	require.Equal(t, int64(0), proc.GetMPool().CurrNB())
}
