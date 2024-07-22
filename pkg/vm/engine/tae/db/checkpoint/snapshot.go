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
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"sort"
)

type GetCheckpointRange = func(snapshot types.TS, files []*MetaFile) ([]*MetaFile, int, error)

func SpecifiedCheckpoint(snapshot types.TS, files []*MetaFile) ([]*MetaFile, int, error) {
	for i, file := range files {
		if snapshot.LessEq(&file.end) {
			return files, i, nil
		}
	}
	return files, len(files) - 1, nil
}

func AllAfterAndGCheckpoint(snapshot types.TS, files []*MetaFile) ([]*MetaFile, int, error) {
	prev := &MetaFile{}
	for i, file := range files {
		if snapshot.LessEq(&file.end) &&
			snapshot.Less(&prev.end) &&
			file.start.IsEmpty() {
			return files, i - 1, nil
		}
		prev = file
	}
	return files, len(files) - 1, nil
}

func ListSnapshotCheckpoint(
	ctx context.Context,
	fs fileservice.FileService,
	snapshot types.TS,
	tid uint64,
	listFunc GetCheckpointRange,
) ([]*CheckpointEntry, error) {
	files, idx, err := ListSnapshotMeta(ctx, fs, snapshot, listFunc)
	if err != nil {
		return nil, err
	}
	if len(files) == 0 {
		return nil, nil
	}
	return ListSnapshotCheckpointWithMeta(ctx, fs, files, idx, types.TS{}, false)
}

func ListSnapshotMeta(
	ctx context.Context,
	fs fileservice.FileService,
	snapshot types.TS,
	listFunc GetCheckpointRange,
) ([]*MetaFile, int, error) {
	dirs, err := fs.List(ctx, CheckpointDir)
	if err != nil {
		return nil, 0, err
	}
	if len(dirs) == 0 {
		return nil, 0, nil
	}
	metaFiles := make([]*MetaFile, 0)
	for i, dir := range dirs {
		start, end := blockio.DecodeCheckpointMetadataFileName(dir.Name)
		metaFiles = append(metaFiles, &MetaFile{
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
		// TODO: remove log
		logutil.Infof("metaFiles[%d]: %v", i, file.String())
	}

	if listFunc == nil {
		listFunc = AllAfterAndGCheckpoint
	}
	return listFunc(snapshot, metaFiles)
}

func ListSnapshotMetaWithDiskCleaner(
	snapshot types.TS,
	listFunc GetCheckpointRange,
	metas map[string]struct{},
) ([]*MetaFile, int, error) {
	if len(metas) == 0 {
		return nil, 0, nil
	}
	metaFiles := make([]*MetaFile, 0)
	idx := 0
	for meta := range metas {
		start, end := blockio.DecodeCheckpointMetadataFileName(meta)
		metaFiles = append(metaFiles, &MetaFile{
			start: start,
			end:   end,
			index: idx,
			name:  meta,
		})
		idx++
	}
	sort.Slice(metaFiles, func(i, j int) bool {
		return metaFiles[i].end.Less(&metaFiles[j].end)
	})

	for i, file := range metaFiles {
		// TODO: remove log
		logutil.Infof("metaFiles[%d]: %v", i, file.String())
	}

	if listFunc == nil {
		listFunc = AllAfterAndGCheckpoint
	}
	return listFunc(snapshot, metaFiles)
}

func ListSnapshotCheckpointWithMeta(
	ctx context.Context,
	fs fileservice.FileService,
	files []*MetaFile,
	idx int,
	gcStage types.TS,
	isAll bool,
) ([]*CheckpointEntry, error) {
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

	var checkpointVersion int
	// in version 1, checkpoint metadata doesn't contain 'version'.
	vecLen := len(bats[0].Vecs)
	if vecLen < CheckpointSchemaColumnCountV1 {
		checkpointVersion = 1
	} else if vecLen < CheckpointSchemaColumnCountV2 {
		checkpointVersion = 2
	} else {
		checkpointVersion = 3
	}

	entries, maxGlobalEnd := replayCheckpointEntries(bat, checkpointVersion)
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].end.Less(&entries[j].end)
	})
	if isAll && gcStage.IsEmpty() {
		return entries, nil
	}
	for i := range entries {
		if !gcStage.IsEmpty() {
			if entries[i].end.Less(&gcStage) {
				continue
			}
			return entries[i:], nil
		}

		if entries[i].end.Equal(&maxGlobalEnd) &&
			entries[i].entryType == ET_Global {
			return entries[i:], nil
		}

	}
	return entries, nil
}
