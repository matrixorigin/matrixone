// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package backup

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	gc "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/gc/v3"
	"github.com/stretchr/testify/require"
)

func TestBackupRetentionUsesPersistedGCRemainingObjects(t *testing.T) {
	ctx := t.Context()
	fs := newBackupMemoryFS(t, "retention-window")
	create, drop := types.BuildTS(5, 0), types.BuildTS(10, 0)
	start, end, restore := types.BuildTS(1, 0), types.BuildTS(20, 0), types.BuildTS(30, 0)
	id := objectio.NewObjectid()
	stats := objectio.NewObjectStatsWithObjectID(&id, false, false, false)
	name := objectio.BuildObjectNameWithObjectID(&id)
	require.NoError(t, objectio.SetObjectStatsLocation(stats,
		objectio.BuildLocation(name, objectio.NewExtent(0, 0, 1, 1), 1, 0)))
	data := batch.NewWithSchema(false, gc.ObjectTableAttrs, gc.ObjectTableTypes)
	defer data.Clean(common.DebugAllocator)
	require.NoError(t, vector.AppendBytes(data.Vecs[0], stats[:], false, common.DebugAllocator))
	require.NoError(t, vector.AppendFixed(data.Vecs[1], create, false, common.DebugAllocator))
	require.NoError(t, vector.AppendFixed(data.Vecs[2], drop, false, common.DebugAllocator))
	require.NoError(t, vector.AppendFixed(data.Vecs[3], uint64(1), false, common.DebugAllocator))
	require.NoError(t, vector.AppendFixed(data.Vecs[4], uint64(2), false, common.DebugAllocator))
	data.SetRowCount(1)
	gcID := objectio.NewObjectid()
	writer, err := ioutil.NewBlockWriterNew(fs,
		objectio.BuildObjectNameWithObjectID(&gcID), 0, gc.ObjectTableSeqnums, false)
	require.NoError(t, err)
	_, err = writer.WriteBatch(data)
	require.NoError(t, err)
	_, _, err = writer.Sync(ctx)
	require.NoError(t, err)
	writeGCMetadata(t, ctx, fs, start, end, writer.GetObjectStats())
	// A newer partial scan must not displace the cumulative GC window.
	writeGCMetadata(t, ctx, fs, types.BuildTS(25, 0), types.BuildTS(29, 0))
	// A newer window beyond the backup point cannot authorize omissions.
	writeGCMetadata(t, ctx, fs, start, types.BuildTS(40, 0))
	retention, err := loadBackupObjectRetention(ctx, "retention-test", fs, restore, nil)
	require.NoError(t, err)
	require.Equal(t, start, retention.start)
	require.Equal(t, end, retention.end)
	require.Contains(t, retention.retained, *name.Short())
	retained := &objectio.BackupObject{Location: stats.ObjectLocation(), TableID: 2,
		CrateTS: create, DropTS: drop, NeedCopy: true}
	require.Contains(t, selectBackupObjects([]*objectio.BackupObject{retained}, restore, nil, nil, retention), name.String())
	for _, test := range []struct {
		name         string
		create, drop types.TS
		keep         bool
	}{
		{"unretained covered lifecycle", create, drop, false},
		{"creation before window", types.BuildTS(0, 1), drop, true},
		{"creation at window boundary", start, drop, true},
		{"deletion after window", create, types.BuildTS(21, 0), true},
		{"deletion at restore", create, restore, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			id := objectio.NewObjectid()
			name := objectio.BuildObjectNameWithObjectID(&id)
			obj := &objectio.BackupObject{Location: objectio.BuildLocation(name,
				objectio.NewExtent(0, 0, 1, 1), 1, 0), TableID: 2,
				CrateTS: test.create, DropTS: test.drop, NeedCopy: true}
			_, found := selectBackupObjects([]*objectio.BackupObject{obj}, restore, nil, nil, retention)[name.String()]
			require.Equal(t, test.keep, found)
		})
	}
}

func TestBackupRetentionRejectsUnreadableGCWindow(t *testing.T) {
	fs := newBackupMemoryFS(t, "missing-retention-window")
	id := objectio.NewObjectid()
	name := objectio.BuildObjectNameWithObjectID(&id)
	stats := objectio.NewObjectStatsWithObjectID(&id, false, false, false)
	require.NoError(t, objectio.SetObjectStatsLocation(stats,
		objectio.BuildLocation(name, objectio.NewExtent(0, 0, 1, 1), 1, 0)))
	require.NoError(t, objectio.SetObjectStatsRowCnt(stats, 1))
	require.NoError(t, objectio.SetObjectStatsBlkCnt(stats, 1))
	writeGCMetadata(t, t.Context(), fs, types.BuildTS(1, 0), types.BuildTS(20, 0), *stats)
	retention, err := loadBackupObjectRetention(t.Context(), "retention-test", fs, types.BuildTS(30, 0), nil)
	require.Error(t, err)
	require.Nil(t, retention)
}
