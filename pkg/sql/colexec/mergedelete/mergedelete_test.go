// Copyright 2022 Matrix Origin
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

package mergedelete

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/deletion"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/value_scan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

type mockRelation struct {
	engine.Relation
	result *batch.Batch
}

func (e *mockRelation) Delete(ctx context.Context, b *batch.Batch, attrName string) error {
	e.result = b
	return nil
}

func TestMergeDelete(t *testing.T) {
	proc := testutil.NewProc()
	proc.Ctx = context.TODO()
	metaLocBat0 := &batch.Batch{
		Attrs: []string{
			catalog.BlockMetaOffset,
		},
		Vecs: []*vector.Vector{
			testutil.MakeInt64Vector([]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14}, nil),
		},
	}
	bytes, err := metaLocBat0.MarshalBinary()
	require.Nil(t, err)

	vcu32, _ := vector.NewConstFixed(types.T_uint32.ToType(), uint32(15), 1, proc.GetMPool())
	batch1 := &batch.Batch{
		Attrs: []string{
			catalog.BlockMeta_Delete_ID,
			catalog.BlockMeta_DeltaLoc,
			catalog.BlockMeta_Type,
			catalog.BlockMeta_Partition,
			catalog.BlockMeta_Deletes_Length,
		},
		Vecs: []*vector.Vector{
			testutil.MakeTextVector([]string{"mock_block_id0"}, nil),
			testutil.MakeTextVector([]string{string(bytes)}, nil),
			testutil.MakeInt8Vector([]int8{deletion.RawBatchOffset}, nil),
			testutil.MakeInt32Vector([]int32{0}, nil),
			vcu32,
		},
	}
	batch1.SetRowCount(1)
	uuid1 := objectio.NewSegmentid()
	blkId1 := objectio.NewBlockid(uuid1, 0, 0)
	metaLocBat1 := &batch.Batch{
		Attrs: []string{
			catalog.Row_ID,
		},
		Vecs: []*vector.Vector{
			testutil.MakeRowIdVector([]types.Rowid{
				*objectio.NewRowid(blkId1, 0),
				*objectio.NewRowid(blkId1, 1),
				*objectio.NewRowid(blkId1, 2),
				*objectio.NewRowid(blkId1, 3),
				*objectio.NewRowid(blkId1, 4),
				*objectio.NewRowid(blkId1, 5),
				*objectio.NewRowid(blkId1, 6),
				*objectio.NewRowid(blkId1, 7),
				*objectio.NewRowid(blkId1, 8),
				*objectio.NewRowid(blkId1, 9),
				*objectio.NewRowid(blkId1, 10),
				*objectio.NewRowid(blkId1, 11),
				*objectio.NewRowid(blkId1, 12),
				*objectio.NewRowid(blkId1, 13),
				*objectio.NewRowid(blkId1, 14),
			}, nil),
		},
	}
	bytes1, err := metaLocBat1.MarshalBinary()
	require.Nil(t, err)

	metaLocBat2 := &batch.Batch{
		Attrs: []string{
			catalog.BlockMetaOffset,
		},
		Vecs: []*vector.Vector{
			testutil.MakeInt64Vector([]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14}, nil),
		},
	}
	bytes2, err := metaLocBat2.MarshalBinary()
	require.Nil(t, err)

	metaLocBat3 := &batch.Batch{
		Attrs: []string{
			catalog.BlockMeta_DeltaLoc,
		},
		Vecs: []*vector.Vector{
			testutil.MakeTextVector([]string{"d:magic:15"}, nil),
		},
	}
	bytes3, err := metaLocBat3.MarshalBinary()
	require.Nil(t, err)

	vcu32_2, _ := vector.NewConstFixed(types.T_uint32.ToType(), uint32(45), 3, proc.GetMPool())
	batch2 := &batch.Batch{
		Attrs: []string{
			catalog.BlockMeta_Delete_ID,
			catalog.BlockMeta_DeltaLoc,
			catalog.BlockMeta_Type,
			catalog.BlockMeta_Partition,
			catalog.BlockMeta_Deletes_Length,
		},
		Vecs: []*vector.Vector{
			testutil.MakeTextVector([]string{"mock_block_id1", "mock_block_id2", "mock_block_id3"}, nil),
			testutil.MakeTextVector([]string{string(bytes1), string(bytes2), string(bytes3)}, nil),
			testutil.MakeInt8Vector([]int8{deletion.RawRowIdBatch, deletion.CNBlockOffset, deletion.FlushDeltaLoc}, nil),
			testutil.MakeInt32Vector([]int32{0, 0, 0}, nil),
			vcu32_2,
		},
	}
	batch2.SetRowCount(3)

	argument1 := Argument{
		delSource:    &mockRelation{},
		AffectedRows: 0,
		OperatorBase: vm.OperatorBase{
			OperatorInfo: vm.OperatorInfo{
				Idx:     0,
				IsFirst: false,
				IsLast:  false,
			},
		},
	}

	// require.NoError(t, argument1.Prepare(proc))
	resetChildren(&argument1, batch1)
	_, err = argument1.Call(proc)
	require.NoError(t, err)
	require.Equal(t, uint64(15), argument1.AffectedRows)

	// Check DelSource
	result0 := argument1.delSource.(*mockRelation).result
	// check attr names
	require.True(t, reflect.DeepEqual(
		[]string{
			catalog.BlockMetaOffset,
		},
		result0.Attrs,
	))
	// check vector
	require.Equal(t, 1, len(result0.Vecs))
	for i, vec := range result0.Vecs {
		require.Equal(t, 15, vec.Length(), fmt.Sprintf("column number: %d", i))
	}

	resetChildren(&argument1, batch2)
	_, err = argument1.Call(proc)
	require.NoError(t, err)
	require.Equal(t, uint64(60), argument1.AffectedRows)

	// Check DelSource
	result1 := argument1.delSource.(*mockRelation).result
	// check attr names
	require.True(t, reflect.DeepEqual(
		[]string{
			catalog.BlockMeta_DeltaLoc,
		},
		result1.Attrs,
	))
	// check vector
	require.Equal(t, 1, len(result1.Vecs))
	for i, vec := range result1.Vecs {
		require.Equal(t, 1, vec.Length(), fmt.Sprintf("column number: %d", i))
	}

	// free resource
	argument1.Free(proc, false, nil)
	metaLocBat0.Clean(proc.GetMPool())
	metaLocBat1.Clean(proc.GetMPool())
	metaLocBat2.Clean(proc.GetMPool())
	metaLocBat3.Clean(proc.GetMPool())
	batch1.Clean(proc.GetMPool())
	batch2.Clean(proc.GetMPool())
	// constVector can't free
	// 2 * 16 is 2 header of const vector.
	require.Equal(t, int64(16+2*16), proc.GetMPool().CurrNB())
}

func resetChildren(arg *Argument, bat *batch.Batch) {
	arg.SetChildren(
		[]vm.Operator{
			&value_scan.Argument{
				Batchs: []*batch.Batch{bat},
			},
		})
}
