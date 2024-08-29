// Copyright 2021 - 2024 Matrix Origin
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

package disttae

import (
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func Test_GetUncommittedS3Tombstone(t *testing.T) {
	proc := testutil.NewProc()

	var locs []objectio.Location = make([]objectio.Location, 3)
	var objs = make(map[objectio.ObjectId]objectio.Location)

	locs[0] = objectio.NewRandomLocation(uint16(0), uint32(10))
	locs[1] = objectio.NewRandomLocation(uint16(1), uint32(20))
	locs[2] = objectio.NewRandomLocation(uint16(2), uint32(30))

	objs[locs[0].ObjectId()] = locs[0]
	objs[locs[1].ObjectId()] = locs[1]
	objs[locs[2].ObjectId()] = locs[2]

	txn := &Transaction{
		blockId_tn_delete_metaLoc_batch: struct {
			sync.RWMutex
			data map[types.Blockid][]*batch.Batch
		}{data: make(map[types.Blockid][]*batch.Batch)},
	}

	for i := range locs {
		oid := locs[i].ObjectId()
		bid := types.NewBlockidWithObjectID(&oid, locs[i].ID())
		bat := batch.NewWithSize(1)
		bat.Vecs[0] = vector.NewVec(types.T_text.ToType())
		vector.AppendBytes(bat.Vecs[0], []byte(locs[i].String()), false, proc.GetMPool())

		txn.blockId_tn_delete_metaLoc_batch.data[*bid] = append(
			txn.blockId_tn_delete_metaLoc_batch.data[*bid], bat)
	}

	objectSlice := objectio.ObjectStatsSlice{}

	require.NoError(t, txn.getUncommittedS3Tombstone(&objectSlice))
	require.Equal(t, len(locs), objectSlice.Len())

	for i := range objectSlice.Len() {
		loc := objs[*objectSlice.Get(i).ObjectName().ObjectId()]
		require.Equal(t, loc.Rows(), objectSlice.Get(i).Rows())
	}

}
