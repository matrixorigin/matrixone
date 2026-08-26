// Copyright 2026 Matrix Origin
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
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

func TestSnapshotInfoAccessMatrix(t *testing.T) {
	var nilInfo *SnapshotInfo
	require.True(t, nilInfo.IsEmpty())
	require.Zero(t, nilInfo.GetTS(1, 2, 3))
	require.Nil(t, nilInfo.GetSnapshotsByLevel(PitrLevelCluster, 0))
	require.Zero(t, nilInfo.MinTS())
	require.Nil(t, nilInfo.ToTsList())

	pitr := NewPitrInfo()
	require.True(t, pitr.IsEmpty())
	require.Len(t, pitr.cluster, 1)
	snapshots := NewSnapshotInfo()
	require.True(t, snapshots.IsEmpty())
	require.Empty(t, snapshots.cluster)

	clusterTS := types.BuildTS(40, 0)
	accountTS := types.BuildTS(30, 0)
	databaseTS := types.BuildTS(20, 0)
	tableTS := types.BuildTS(10, 0)
	snapshots.cluster = []types.TS{clusterTS}
	snapshots.account[1] = []types.TS{accountTS}
	snapshots.database[2] = []types.TS{databaseTS}
	snapshots.tables[3] = []types.TS{tableTS}
	require.False(t, snapshots.IsEmpty())
	require.Equal(t, tableTS, snapshots.GetTS(1, 2, 3))
	require.Equal(t, tableTS, snapshots.MinTS())
	require.ElementsMatch(t, []types.TS{clusterTS, accountTS, databaseTS, tableTS}, snapshots.ToTsList())

	require.Equal(t, []types.TS{clusterTS}, snapshots.GetSnapshotsByLevel(PitrLevelCluster, 0))
	require.Equal(t, []types.TS{accountTS}, snapshots.GetSnapshotsByLevel(PitrLevelAccount, 1))
	require.Nil(t, snapshots.GetSnapshotsByLevel(PitrLevelAccount, uint64(math.MaxUint32)+1))
	require.Equal(t, []types.TS{databaseTS}, snapshots.GetSnapshotsByLevel(PitrLevelDatabase, 2))
	require.Equal(t, []types.TS{tableTS}, snapshots.GetSnapshotsByLevel(PitrLevelTable, 3))
	require.Nil(t, snapshots.GetSnapshotsByLevel("invalid", 0))
}

func TestSnapshotInfoEmptyLevels(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*SnapshotInfo)
	}{
		{name: "account", mutate: func(info *SnapshotInfo) {
			info.account[1] = []types.TS{types.BuildTS(1, 0)}
		}},
		{name: "database", mutate: func(info *SnapshotInfo) {
			info.database[1] = []types.TS{types.BuildTS(1, 0)}
		}},
		{name: "table", mutate: func(info *SnapshotInfo) {
			info.tables[1] = []types.TS{types.BuildTS(1, 0)}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			info := NewSnapshotInfo()
			test.mutate(info)
			require.False(t, info.IsEmpty())
		})
	}
}

func TestAddDateClampsMonthEnd(t *testing.T) {
	base := time.Date(2024, time.January, 31, 12, 0, 0, 0, time.UTC)
	require.Equal(t, time.Date(2024, time.February, 29, 12, 0, 0, 0, time.UTC), AddDate(base, 0, 1, 0))
	require.Equal(t, time.Date(2024, time.March, 2, 12, 0, 0, 0, time.UTC), AddDate(base, 0, 1, 2))
}

func TestSnapshotMetaAccessorMatrix(t *testing.T) {
	meta := NewSnapshotMeta()
	require.NotNil(t, meta)
	require.True(t, IsMoTable(2))
	require.False(t, IsMoTable(999))
	require.Contains(t, meta.String(), "account count: 0")
	require.Empty(t, meta.GetAllTableIDs())
	_, ok := meta.GetTableDropAt(7)
	require.False(t, ok)
	_, ok = meta.GetAccountId(7)
	require.False(t, ok)
	require.Empty(t, meta.GetTablePK(7))
	require.Nil(t, meta.GetSnapshotListLocked(NewSnapshotInfo(), 7))

	info := &tableInfo{
		accountID: 1, dbID: 2, tid: 7,
		createAt: types.BuildTS(10, 0), deleteAt: types.BuildTS(30, 0), pk: "pk7",
	}
	meta.tables[1] = map[uint64]*tableInfo{7: info, 8: nil}
	meta.tableIDIndex[7] = info
	snapshots := NewSnapshotInfo()
	snapshots.account[1] = []types.TS{types.BuildTS(20, 0)}
	require.Equal(t, snapshots.account[1], meta.GetSnapshotListLocked(snapshots, 7))
	require.Contains(t, meta.TableInfoString(), "metadata: <nil>")
	dropAt, ok := meta.GetTableDropAt(7)
	require.True(t, ok)
	require.Equal(t, info.deleteAt, dropAt)
	accountID, ok := meta.GetAccountId(7)
	require.True(t, ok)
	require.Equal(t, uint32(1), accountID)
	require.Equal(t, map[uint64]bool{7: true}, meta.GetAllTableIDs())
	require.Equal(t, "pk7", meta.GetTablePK(7))

	pitr := NewPitrInfo()
	pitr.cluster[0] = types.BuildTS(5, 0)
	require.Equal(t, types.BuildTS(5, 0), *meta.GetPitrByTable(pitr, 2, 7))
	var nilMeta *SnapshotMeta
	require.Zero(t, *nilMeta.GetPitrByTable(pitr, 2, 7))
	tableSnapshots, tablePitrs := nilMeta.AccountToTableSnapshots(nil, nil)
	require.Empty(t, tableSnapshots)
	require.Empty(t, tablePitrs)
	tableSnapshots, tablePitrs = meta.AccountToTableSnapshots(snapshots, pitr)
	require.Contains(t, tableSnapshots, uint64(7))
	require.Contains(t, tablePitrs, uint64(7))
	require.Error(t, nilMeta.MergeTableInfo(snapshots, pitr))
	require.Error(t, meta.MergeTableInfo(nil, pitr))
	require.NoError(t, NewSnapshotMeta().MergeTableInfo(NewSnapshotInfo(), NewPitrInfo()))

	empty := types.TS{}
	require.False(t, isSnapshotRefers(info, nil, &empty))
	pitrTS := types.BuildTS(20, 0)
	require.True(t, isSnapshotRefers(info, nil, &pitrTS))
	require.True(t, isSnapshotRefers(info, []types.TS{types.BuildTS(20, 0)}, &empty))
}
