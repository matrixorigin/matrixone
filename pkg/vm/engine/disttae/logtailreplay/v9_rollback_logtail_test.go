// Copyright 2021 Matrix Origin
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

package logtailreplay_test

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"github.com/stretchr/testify/require"
)

func TestV9RollbackSentinelDoesNotReachPartitionState(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	src := &containers.BatchWithVersion{
		Batch:      containers.NewBatchWithCapacity(3),
		Seqnums:    []uint16{0, objectio.SEQNUM_ROWID, objectio.SEQNUM_COMMITTS},
		NextSeqnum: 1,
	}
	defer src.Close()

	primaryKeys := containers.MakeVector(types.T_int64.ToType(), mp)
	rowIDs := containers.MakeVector(types.T_Rowid.ToType(), mp)
	commitTSs := containers.MakeVector(types.T_TS.ToType(), mp)
	var blockID types.Blockid
	for row, key := range []int64{42, 7} {
		primaryKeys.Append(key, false)
		rowIDs.Append(types.NewRowid(&blockID, uint32(row)), false)
		if row == 0 {
			commitTSs.Append(txnif.UncommitTS, false)
		} else {
			commitTSs.Append(types.BuildTS(5, 0), false)
		}
	}
	src.AddVector("pk", primaryKeys)
	src.AddVector(catalog.PhyAddrColumnName, rowIDs)
	src.AddVector(objectio.DefaultCommitTS_Attr, commitTSs)

	logtailBatch := logtail.DataChangeToLogtailBatch(src)
	require.Equal(t, 1, logtailBatch.Length())
	protoBatch, err := batch.BatchToProtoBatch(containers.ToCNBatch(logtailBatch))
	require.NoError(t, err)

	state := logtailreplay.NewPartitionState("test", false, 42, false)
	packer := types.NewPacker()
	defer packer.Close()
	state.HandleRowsInsert(context.Background(), protoBatch, 0, packer, mp)

	packer.Reset()
	abortedKey := readutil.EncodePrimaryKey(int64(42), packer)
	modified, _ := state.PKExistInMemBetween(types.TS{}, txnif.UncommitTS, [][]byte{abortedKey})
	require.False(t, modified)

	packer.Reset()
	liveKey := readutil.EncodePrimaryKey(int64(7), packer)
	modified, _ = state.PKExistInMemBetween(types.TS{}, txnif.UncommitTS, [][]byte{liveKey})
	require.True(t, modified)
}
