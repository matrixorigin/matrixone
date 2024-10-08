// Copyright 2021-2024 Matrix Origin
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

package engine_util

import (
	"context"
	"math/rand"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRemoteDataSource_ApplyTombstones(t *testing.T) {
	var rowIds []types.Rowid
	var pks []int32
	var committs []types.TS
	for i := 0; i < 100; i++ {
		row := types.RandomRowid()
		rowIds = append(rowIds, row)
		pks = append(pks, rand.Int31())
		committs = append(committs, types.TimestampToTS(timestamp.Timestamp{
			PhysicalTime: rand.Int63(),
			LogicalTime:  rand.Uint32(),
		}))
	}

	proc := testutil.NewProc()
	proc.Base.FileService = testutil.NewSharedFS()

	bat := batch.NewWithSize(3)
	bat.Attrs = objectio.TombstoneAttrs_TN_Created
	bat.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	bat.Vecs[1] = vector.NewVec(types.T_int32.ToType())
	bat.Vecs[2] = vector.NewVec(types.T_TS.ToType())

	for i := 0; i < len(rowIds)/2; i++ {
		require.NoError(t, vector.AppendFixed[types.Rowid](bat.Vecs[0], rowIds[i], false, proc.Mp()))
		require.NoError(t, vector.AppendFixed[int32](bat.Vecs[1], pks[i], false, proc.Mp()))
		require.NoError(t, vector.AppendFixed[types.TS](bat.Vecs[2], committs[i], false, proc.Mp()))
	}

	bat.SetRowCount(bat.Vecs[0].Length())

	writer, err := colexec.NewS3TombstoneWriter()
	assert.Nil(t, err)

	writer.StashBatch(proc, bat)
	_, ss, err := writer.SortAndSync(proc)
	assert.Nil(t, err)

	require.Equal(t, len(rowIds)/2, int(ss.Rows()))

	tData := NewEmptyTombstoneData()
	for i := len(rowIds) / 2; i < len(rowIds)-1; i++ {
		require.NoError(t, tData.AppendInMemory(rowIds[i]))
	}

	require.NoError(t, tData.AppendFiles(ss))

	relData := NewBlockListRelationData(0)
	require.NoError(t, relData.AttachTombstones(tData))

	ts := types.MaxTs()
	ds := NewRemoteDataSource(context.Background(), proc.GetFileService(), ts.ToTimestamp(), relData)

	bid, offset := rowIds[0].Decode()
	left, err := ds.ApplyTombstones(context.Background(), bid, []int64{int64(offset)}, engine.Policy_CheckAll)
	assert.Nil(t, err)

	require.Equal(t, 0, len(left))

	bid, offset = rowIds[len(rowIds)/2+1].Decode()
	left, err = ds.ApplyTombstones(context.Background(), bid, []int64{int64(offset)}, engine.Policy_CheckAll)
	require.Nil(t, err)
	require.Equal(t, 0, len(left))

	bid, offset = rowIds[len(rowIds)-1].Decode()
	left, err = ds.ApplyTombstones(context.Background(), bid, []int64{int64(offset)}, engine.Policy_CheckAll)
	require.Nil(t, err)
	require.Equal(t, 1, len(left))
}
