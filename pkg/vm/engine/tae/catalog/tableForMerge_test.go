// Copyright 2025 Matrix Origin
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

package catalog

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/stretchr/testify/require"
)

func newTestObjectStats(t testing.TB,
	v1, v2 int32, size, row uint32, lv int8, segid *types.Segmentid, num uint16,
) *objectio.ObjectStats {
	zm := index.NewZM(types.T_int32, 0)
	index.UpdateZM(zm, types.EncodeInt32(&v1))
	index.UpdateZM(zm, types.EncodeInt32(&v2))
	stats := objectio.NewObjectStats()
	var objName objectio.ObjectName
	if segid != nil {
		objName = objectio.BuildObjectName(segid, num)
	} else {
		nobjid := objectio.NewObjectid()
		objName = objectio.BuildObjectNameWithObjectID(&nobjid)
	}
	require.NoError(t, objectio.SetObjectStatsObjectName(stats, objName))
	require.NoError(t, objectio.SetObjectStatsSortKeyZoneMap(stats, zm))
	require.NoError(t, objectio.SetObjectStatsOriginSize(stats, size))
	require.NoError(t, objectio.SetObjectStatsRowCnt(stats, row))
	stats.SetLevel(lv)
	return stats
}

func TestLogInputTombstoneObjectWithExistingBatches(t *testing.T) {

	table := MockStaloneTableEntry(12, nil)
	table.db = MockDBEntryWithAccInfo(0, 11)

	segid := objectio.NewSegmentid()

	table.AddEntryLocked(&ObjectEntry{
		ObjectMVCCNode: ObjectMVCCNode{
			ObjectStats: *newTestObjectStats(t, 1, 2, 100, 100, 0, segid, 0),
		},
		table: table,
	})
	table.AddEntryLocked(&ObjectEntry{
		ObjectMVCCNode: ObjectMVCCNode{
			ObjectStats: *newTestObjectStats(t, 4, 5, 100, 100, 0, segid, 1),
		},
		table: table,
	})

	tombstone := newTestObjectStats(t, 1, 2, 100, 100, 0, objectio.NewSegmentid(), 0)
	ts := types.BuildTS(10000001, 1)
	vec := vector.NewVec(types.T_Rowid.ToType())
	mp := mpool.MustNew("test")
	for i := 0; i < 50; i++ {
		blk := objectio.NewBlockid(segid, 0, 0)
		vector.AppendFixed(vec, types.NewRowid(blk, uint32(i)), false, mp)
	}

	for i := 0; i < 20; i++ {
		blk := objectio.NewBlockid(segid, 1, 0)
		vector.AppendFixed(vec, types.NewRowid(blk, uint32(i)), false, mp)
	}

	bat := &batch.Batch{
		Vecs: []*vector.Vector{vec},
	}
	defer bat.Clean(mp)
	LogInputTombstoneObjectWithExistingBatches(
		table, tombstone, ts, []*batch.Batch{bat},
	)
}
