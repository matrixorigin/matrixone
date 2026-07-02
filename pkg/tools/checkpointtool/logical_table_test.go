// Copyright 2021 Matrix Origin
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

package checkpointtool

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildLogicalTableViewEmpty(t *testing.T) {
	reader := &CheckpointReader{}
	view, err := reader.BuildLogicalTableView(context.Background(), types.TS{}, nil, nil)
	require.NoError(t, err)
	assert.Equal(t, []string{"object", "block", "row"}, view.Headers)
	assert.Empty(t, view.Rows)
	assert.Equal(t, 0, view.PhysicalRows)
	assert.Equal(t, 0, view.DeletedRows)
	assert.Equal(t, 0, view.VisibleRows)
}

func TestVisibleObjectEntriesFiltersAndDedupesBySnapshot(t *testing.T) {
	snapshot := types.BuildTS(15, 0)
	entries := []*ObjectEntryInfo{
		newTestObjectEntryInfo(1, 10, 0),
		newTestObjectEntryInfo(1, 12, 20),
		newTestObjectEntryInfo(2, 16, 0), // created after snapshot
		newTestObjectEntryInfo(3, 1, 14), // deleted before snapshot
	}

	visible := visibleObjectEntries(entries, snapshot)
	require.Len(t, visible, 1)
	assert.Equal(t, uint32(1), visible[0].ObjectStats.Rows())
	assert.Equal(t, types.BuildTS(10, 0), visible[0].CreateTime)
	assert.Equal(t, types.BuildTS(20, 0), visible[0].DeleteTime)
}

func TestVisibleObjectEntriesDeletedAfterSnapshotBecomesInvisibleLater(t *testing.T) {
	entries := []*ObjectEntryInfo{
		newTestObjectEntryInfo(7, 5, 0),
		newTestObjectEntryInfo(7, 8, 30),
	}

	visibleAt20 := visibleObjectEntries(entries, types.BuildTS(20, 0))
	require.Len(t, visibleAt20, 1)

	visibleAt30 := visibleObjectEntries(entries, types.BuildTS(30, 0))
	assert.Empty(t, visibleAt30)
}

func TestDedupeObjectStats(t *testing.T) {
	entries := []*ObjectEntryInfo{
		newTestObjectEntryInfo(1, 1, 0),
		newTestObjectEntryInfo(1, 2, 0),
		newTestObjectEntryInfo(2, 3, 0),
	}

	stats := dedupeObjectStats(entries)
	require.Len(t, stats, 2)
	assert.NotEqual(t, stats[0].ObjectName().String(), stats[1].ObjectName().String())
}

func newTestObjectEntryInfo(idByte byte, createPhysical int64, deletePhysical int64) *ObjectEntryInfo {
	var objectID objectio.ObjectId
	objectID[0] = idByte
	stats := objectio.NewObjectStats()
	_ = objectio.SetObjectStatsObjectName(stats, objectio.BuildObjectNameWithObjectID(&objectID))
	_ = objectio.SetObjectStatsRowCnt(stats, uint32(idByte))
	_ = objectio.SetObjectStatsBlkCnt(stats, 1)

	entry := &ObjectEntryInfo{
		ObjectStats: *stats,
		CreateTime:  types.BuildTS(createPhysical, 0),
	}
	if deletePhysical > 0 {
		entry.DeleteTime = types.BuildTS(deletePhysical, 0)
	}
	return entry
}
