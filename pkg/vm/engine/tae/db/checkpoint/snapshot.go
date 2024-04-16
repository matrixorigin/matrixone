// Copyright 2021 Matrix Origin
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

package checkpoint

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"sort"
)

func ListSnapshotCheckpoint(ctx context.Context, fs fileservice.FileService, snapshot types.TS, tid uint64) ([]*CheckpointEntry, error) {
	files, idx, err := listMeta(ctx, fs, snapshot)
	if err != nil {
		return nil, err
	}
	if len(files) == 0 {
		return nil, nil
	}
	reader, err := blockio.NewFileReader(fs, CheckpointDir+files[idx].name)
	if err != nil {
		return nil, nil
	}
	bats, closeCB, err := reader.LoadAllColumns(ctx, nil, common.DebugAllocator)
	if err != nil {
		return nil, nil
	}
	defer func() {
		if closeCB != nil {
			closeCB()
		}
	}()
	bat := containers.NewBatch()
	defer bat.Close()
	colNames := CheckpointSchema.Attrs()
	colTypes := CheckpointSchema.Types()
	for i := range bats[0].Vecs {
		if len(bats) == 0 {
			continue
		}
		var vec containers.Vector
		if bats[0].Vecs[i].Length() == 0 {
			vec = containers.MakeVector(colTypes[i], common.DebugAllocator)
		} else {
			vec = containers.ToTNVector(bats[0].Vecs[i], common.DebugAllocator)
		}
		bat.AddVector(colNames[i], vec)
	}
	entries, maxGlobalEnd := replayCheckpointEntries(bat, 3)
	for i := range entries {
		if entries[i].end.Equal(&maxGlobalEnd) && entries[i].entryType == ET_Global {
			return entries[i:], nil
		}
	}
	return entries, nil
}

func listMeta(ctx context.Context, fs fileservice.FileService, snapshot types.TS) ([]*metaFile, int, error) {
	dirs, err := fs.List(ctx, CheckpointDir)
	if err != nil {
		return nil, 0, err
	}
	if len(dirs) == 0 {
		return nil, 0, nil
	}
	metaFiles := make([]*metaFile, 0)
	for i, dir := range dirs {
		start, end := blockio.DecodeCheckpointMetadataFileName(dir.Name)
		metaFiles = append(metaFiles, &metaFile{
			start: start,
			end:   end,
			index: i,
			name:  dir.Name,
		})
	}
	sort.Slice(metaFiles, func(i, j int) bool {
		return metaFiles[i].end.Less(&metaFiles[j].end)
	})

	for i, file := range metaFiles {
		if snapshot.LessEq(&file.end) && file.start.IsEmpty() {
			return metaFiles, i, nil
		}
	}
	return metaFiles, len(metaFiles) - 1, nil
}
