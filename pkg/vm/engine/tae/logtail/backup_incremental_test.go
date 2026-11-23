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
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/ckputil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/stretchr/testify/require"
)

// Exercise the real checkpoint writer/reader, not a duplicate of the timestamp
// predicate. A drop changes immutable object visibility, not its file contents.
func TestBackupCheckpointIncrementalCopyDecision(t *testing.T) {
	ctx := t.Context()
	fs, err := fileservice.NewMemoryFS("shared", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	defer fs.Close(ctx)
	data := NewCheckpointData_V2(common.CheckpointAllocator, DefaultCheckpointSize, fs)
	defer data.Close()
	packer := types.NewPacker()
	defer packer.Close()
	base := types.BuildTS(10, 1)
	old := types.BuildTS(5, 0)
	later := types.BuildTS(20, 0)

	type expectedObject struct {
		name                  string
		create, drop          types.TS
		incrementalCopy, skip bool
	}
	expected := make(map[string]expectedObject)
	for _, tombstone := range []bool{false, true} {
		for _, appendable := range []bool{false, true} {
			for _, tc := range []struct {
				name         string
				create, drop types.TS
				copy         bool
				noExtent     bool
			}{
				{name: "old live", create: old},
				{name: "old drop before base", create: old, drop: types.BuildTS(10, 0)},
				{name: "old drop at base", create: old, drop: base, copy: appendable},
				{name: "old drop after base", create: old, drop: later, copy: appendable},
				{name: "create before base logical tick", create: types.BuildTS(10, 0), drop: later, copy: appendable},
				{name: "create at base", create: base, drop: later, copy: true},
				{name: "create after base logical tick", create: types.BuildTS(10, 2), drop: later, copy: true},
				{name: "new live", create: later, copy: true},
				{name: "no persisted extent", create: old, drop: later, noExtent: true},
			} {
				id := objectio.NewObjectid()
				stats := objectio.NewObjectStatsWithObjectID(&id, appendable, false, false)
				if !tc.noExtent {
					require.NoError(t, objectio.SetObjectStatsLocation(stats, objectio.BuildLocation(
						stats.ObjectName(), objectio.NewExtent(0, 0, 1, 1), 1, 0)))
				}
				objectType := ckputil.ObjectType_Data
				if tombstone {
					objectType = ckputil.ObjectType_Tombstone
				}
				require.NoError(t, appendValToBatch(0, 1, 1001, objectType,
					*stats, tc.create, tc.drop, packer, data.batch, common.CheckpointAllocator))
				expected[stats.ObjectName().String()] = expectedObject{
					name:   fmt.Sprintf("tombstone=%t/appendable=%t/%s", tombstone, appendable, tc.name),
					create: tc.create, drop: tc.drop, incrementalCopy: tc.copy,
					skip: tc.noExtent || (appendable && tc.drop.IsEmpty()),
				}
			}
		}
	}
	location, _, err := data.Sync(ctx, fs)
	require.NoError(t, err)
	for _, mode := range []struct {
		name string
		base types.TS
	}{
		{"full", types.TS{}},
		{"incremental", base},
		{"next incremental", types.BuildTS(40, 0)},
	} {
		t.Run(mode.name, func(t *testing.T) {
			objects, reader, err := LoadCheckpointEntriesFromKey(ctx, "backup-test", fs,
				location, CheckpointCurrentVersion, nil, &mode.base)
			require.NoError(t, err)
			checkpointFiles := map[string]bool{location.Name().String(): true}
			for _, loc := range reader.GetLocations() {
				checkpointFiles[loc.Name().String()] = true
			}
			seen := make(map[string]bool)
			for _, obj := range objects {
				name := obj.Location.Name().String()
				if checkpointFiles[name] {
					require.True(t, obj.NeedCopy, "checkpoint files must always be copied")
					delete(checkpointFiles, name)
					continue
				}
				want, ok := expected[name]
				require.True(t, ok, name)
				require.False(t, want.skip, want.name)
				require.False(t, seen[name], want.name)
				seen[name] = true
				wantCopy := mode.name == "full" || (mode.name == "incremental" && want.incrementalCopy)
				require.Equal(t, wantCopy, obj.NeedCopy, want.name)
				require.Equal(t, want.create, obj.CrateTS, want.name)
				require.Equal(t, want.drop, obj.DropTS, "visibility metadata must be unchanged: %s", want.name)
			}
			require.Empty(t, checkpointFiles)
			for name, want := range expected {
				require.Equal(t, !want.skip, seen[name], want.name)
			}
		})
	}
}
