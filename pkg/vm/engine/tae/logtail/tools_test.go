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

package logtail

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/stretchr/testify/require"
)

func TestV9RollbackSentinelDoesNotReachTombstoneLogtail(t *testing.T) {
	src := &containers.BatchWithVersion{
		Batch: containers.NewBatchWithCapacity(4),
		Seqnums: []uint16{
			objectio.TombstoneAttr_Rowid_SeqNum,
			objectio.TombstoneAttr_PK_SeqNum,
			objectio.SEQNUM_ROWID,
			objectio.SEQNUM_COMMITTS,
		},
		NextSeqnum: 2,
	}
	defer src.Close()

	deletedRowIDs := containers.MakeVector(types.T_Rowid.ToType(), common.DefaultAllocator)
	primaryKeys := containers.MakeVector(types.T_int64.ToType(), common.DefaultAllocator)
	physicalRowIDs := containers.MakeVector(types.T_Rowid.ToType(), common.DefaultAllocator)
	commitTSs := containers.MakeVector(types.T_TS.ToType(), common.DefaultAllocator)
	var blockID types.Blockid
	for row, key := range []int64{42, 7} {
		deletedRowIDs.Append(types.NewRowid(&blockID, uint32(row+10)), false)
		primaryKeys.Append(key, false)
		physicalRowIDs.Append(types.NewRowid(&blockID, uint32(row)), false)
		if row == 0 {
			commitTSs.Append(txnif.UncommitTS, false)
		} else {
			commitTSs.Append(types.BuildTS(5, 0), false)
		}
	}
	src.AddVector(objectio.TombstoneAttr_Rowid_Attr, deletedRowIDs)
	src.AddVector(objectio.TombstoneAttr_PK_Attr, primaryKeys)
	src.AddVector(catalog.PhyAddrColumnName, physicalRowIDs)
	src.AddVector(objectio.TombstoneAttr_CommitTs_Attr, commitTSs)

	output := TombstoneChangeToLogtailBatch(src)
	require.Equal(t, 1, output.Length())
	require.Equal(t, int64(7), output.GetVectorByName(objectio.TombstoneAttr_PK_Attr).Get(0))
}
