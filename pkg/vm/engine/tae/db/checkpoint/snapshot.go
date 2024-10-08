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
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"sort"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
)

// `files` should be sorted by the end-ts in the asc order
// Ex.1
//
//	    files  :  [0,100],[100,200],[200,300],[0,300],[300,400],[400,500]
//		ts     :  250
//		return :  [0,100],[100,200],[200,300]
//
// Ex.2
//
//	    files  :  [0,100],[100,200],[200,300],[0,300],[300,400],[400,500]
//		ts     :  300
//		return :  [0,100],[100,200],[200,300],[0,300]
//
// Ex.3
//
//	    files  :  [0,100],[100,200],[200,300],[0,300],[300,400],[400,500],[500,600]
//		ts     :  450
//      return :  [0,100],[100,200],[200,300],[0,300],[300,400],[400,500],[500,600]

func FilterSortedMetaFilesByTimestamp(
	ts *types.TS,
	files []*MetaFile,
) ([]*MetaFile, bool) {
	if len(files) == 0 {
		return nil, false
	}

	prev := files[0]

	// start.IsEmpty() means the file is a global checkpoint
	// ts.LE(&prev.end) means the ts is in the range of the checkpoint
	// it means the ts is in the range of the global checkpoint
	// ts is within GCKP[0, end]
	if prev.start.IsEmpty() && ts.LE(&prev.end) {
		return files[:1], true
	}

	for i := 1; i < len(files); i++ {
		curr := files[i]
		// curr.start.IsEmpty() means the file is a global checkpoint
		// ts.LE(&curr.end) means the ts is in the range of the checkpoint
		// ts.LT(&prev.end) means the ts is not in the range of the previous checkpoint
		if curr.start.IsEmpty() && ts.LE(&curr.end) {
			return files[:i], true
		}
		prev = curr
	}

	return files, false
}

func ListSnapshotCheckpoint(
	ctx context.Context,
	sid string,
	fs fileservice.FileService,
	snapshot types.TS,
	_ uint64,
) ([]*CheckpointEntry, error) {
	metaFiles, err := ListSnapshotMeta(ctx, snapshot, fs)
	if err != nil {
		return nil, err
	}
	if len(metaFiles) == 0 {
		return nil, nil
	}
	bat, version, closeCBs, err := loadCheckpointMeta(ctx, sid, fs, metaFiles)
	defer func() {
		for _, cb := range closeCBs {
			cb()
		}
	}()
	if err != nil {
		return nil, err
	}
	return ListSnapshotCheckpointWithMeta(bat, version)
}

func ListSnapshotMeta(
	ctx context.Context,
	snapshot types.TS,
	fs fileservice.FileService,
) ([]*MetaFile, error) {
	dirs, err := fs.List(ctx, CheckpointDir)
	if err != nil {
		return nil, err
	}
	if len(dirs) == 0 {
		return nil, nil
	}
	metaFiles := make([]*MetaFile, 0)
	compactedFiles := make([]*MetaFile, 0)
	for i, dir := range dirs {
		start, end, ext := blockio.DecodeCheckpointMetadataFileName(dir.Name)
		file := &MetaFile{
			start: start,
			end:   end,
			index: i,
			name:  dir.Name,
		}
		if ext == blockio.CompactedExt {
			compactedFiles = append(compactedFiles, file)
		} else {
			metaFiles = append(metaFiles, file)
		}
	}

	sort.Slice(compactedFiles, func(i, j int) bool {
		return compactedFiles[i].end.LT(&compactedFiles[j].end)
	})

	sort.Slice(metaFiles, func(i, j int) bool {
		return metaFiles[i].end.LT(&metaFiles[j].end)
	})

	for i, file := range metaFiles {
		// TODO: remove debug log
		logutil.Infof("metaFiles[%d]: %v", i, file.String())
	}
	for i, file := range compactedFiles {
		// TODO: remove debug log
		logutil.Infof("compactedFiles[%d]: %v", i, file.String())
	}

	// isRangeHit is a flag, which represents whether the metaFiles passed in meet
	// the range of this snapshot. If the 'compacted' does not meet the requirements,
	// then the normal checkpoint must be returned, that is 'metaFiles'.
	isRangeHit := false
	var files []*MetaFile
	var oFiles []*MetaFile
	if len(compactedFiles) > 0 {
		files, isRangeHit = FilterSortedMetaFilesByTimestamp(&snapshot, compactedFiles)
	}

	if isRangeHit {
		logutil.Infof(
			"ListSnapshotMeta: snapshot=%v files=%v-%v",
			snapshot.ToString(), files[0].end.ToString(), files[len(files)-1].end.ToString())
		// The compacted checkpoint meta only contains one checkpoint record,
		// so you need to read all the meta files
		return files, nil
	}

	// The normal checkpoint meta file records a checkpoint interval,
	// so you only need to read the last meta file
	oFiles, _ = FilterSortedMetaFilesByTimestamp(&snapshot, metaFiles)

	if len(oFiles) == 1 {
		return oFiles, nil
	}
	return oFiles[len(oFiles)-1:], nil
}

func loadCheckpointMeta(
	ctx context.Context,
	sid string,
	fs fileservice.FileService,
	metaFiles []*MetaFile,
) (bat *containers.Batch, checkpointVersion int, releases []func(), err error) {
	colNames := CheckpointSchema.Attrs()
	colTypes := CheckpointSchema.Types()
	bat = containers.NewBatch()
	releases = make([]func(), 0)
	var (
		bats    []*batch.Batch
		closeCB func()
		reader  *blockio.BlockReader
	)

	loader := func(name string) error {
		reader, err = blockio.NewFileReader(sid, fs, name)
		if err != nil {
			return err
		}
		bats, closeCB, err = reader.LoadAllColumns(ctx, nil, common.DebugAllocator)
		if err != nil {
			return err
		}
		if closeCB != nil {
			releases = append(releases, closeCB)
		}

		if len(bats) > 1 {
			panic("unexpected multiple batches in a checkpoint file")
		}
		if len(bats) == 0 {
			return nil
		}
		b := bats[0]
		if len(bat.Vecs) > 0 {
			bat.Append(containers.ToTNBatch(b, common.DebugAllocator))
			return nil
		}
		for i := range b.Vecs {
			var vec containers.Vector
			if bats[0].Vecs[i].Length() == 0 {
				vec = containers.MakeVector(colTypes[i], common.DebugAllocator)
			} else {
				vec = containers.ToTNVector(bats[0].Vecs[i], common.DebugAllocator)
			}
			bat.AddVector(colNames[i], vec)
		}
		return nil
	}

	for _, metaFile := range metaFiles {
		err = loader(CheckpointDir + metaFile.name)
		if err != nil {
			return
		}
	}

	// in version 1, checkpoint metadata doesn't contain 'version'.
	vecLen := len(bat.Vecs)
	if vecLen < CheckpointSchemaColumnCountV1 {
		checkpointVersion = 1
	} else if vecLen < CheckpointSchemaColumnCountV2 {
		checkpointVersion = 2
	} else {
		checkpointVersion = 3
	}
	return
}

func ListSnapshotCheckpointWithMeta(
	bat *containers.Batch,
	version int,
) ([]*CheckpointEntry, error) {

	entries, maxGlobalEnd := ReplayCheckpointEntries(bat, version)
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].end.LT(&entries[j].end)
	})
	for i := range entries {
		p := maxGlobalEnd.Prev()
		if entries[i].end.Equal(&p) || (entries[i].end.Equal(&maxGlobalEnd) &&
			entries[i].entryType == ET_Global) {
			return entries[i:], nil
		}

	}
	return entries, nil
}
