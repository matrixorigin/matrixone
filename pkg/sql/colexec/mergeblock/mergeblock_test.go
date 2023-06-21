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

	"github.com/matrixorigin/matrixone/pkg/objectio"
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
	blkInfo1 := catalog.BlockInfo{
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
	blkInfo2 := catalog.BlockInfo{
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
	blkInfo3 := catalog.BlockInfo{
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
		Attrs: []string{catalog.BlockMeta_TableIdx_Insert, catalog.BlockMeta_BlockInfo},
		Vecs: []*vector.Vector{
			testutil.MakeInt16Vector([]int16{0, 0, 0}, nil),
			testutil.MakeTextVector([]string{
				string(catalog.EncodeBlockInfo(blkInfo1)),
				string(catalog.EncodeBlockInfo(blkInfo2)),
				string(catalog.EncodeBlockInfo(blkInfo3))},
				nil),
		},
		Zs:  []int64{1, 1, 1},
		Cnt: 1,
	}
	argument1 := Argument{
		Tbl: &mockRelation{},
		//Unique_tbls:  []engine.Relation{&mockRelation{}, &mockRelation{}},
		affectedRows: 0,
		notFreeBatch: true,
	}
	proc.Reg.InputBatch = batch1
	Prepare(proc, &argument1)
	_, err := Call(0, proc, &argument1, false, false)
	require.NoError(t, err)
	require.Equal(t, uint64(15*3), argument1.affectedRows)
	// Check Tbl
	{
		result := argument1.Tbl.(*mockRelation).result
		// check attr names
		require.True(t, reflect.DeepEqual(
			[]string{catalog.BlockMeta_BlockInfo},
			result.Attrs,
		))
		// check vector
		require.Equal(t, 1, len(result.Vecs))
		for i, vec := range result.Vecs {
			require.Equal(t, 3, vec.Length(), fmt.Sprintf("column number: %d", i))
		}
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
	argument1.Free(proc, false)
	//require.Equal(t, int64(0), proc.GetMPool().CurrNB())
}
