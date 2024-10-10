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
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
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
) []*MetaFile {
	if len(files) == 0 {
		return nil
	}

	prev := files[0]

	// start.IsEmpty() means the file is a global checkpoint
	// ts.LE(&prev.end) means the ts is in the range of the checkpoint
	// it means the ts is in the range of the global checkpoint
	// ts is within GCKP[0, end]
	if prev.start.IsEmpty() && ts.LE(&prev.end) {
		return files[:1]
	}

	for i := 1; i < len(files); i++ {
		curr := files[i]
		// curr.start.IsEmpty() means the file is a global checkpoint
		// ts.LE(&curr.end) means the ts is in the range of the checkpoint
		// ts.LT(&prev.end) means the ts is not in the range of the previous checkpoint
		if curr.start.IsEmpty() && ts.LE(&curr.end) {
			return files[:i]
		}
		prev = curr
	}

	return files
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
	return loadCheckpointMeta(ctx, sid, fs, metaFiles)
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

	retFiles := make([]*MetaFile, 0)
	if len(compactedFiles) > 0 {
		file := compactedFiles[len(compactedFiles)-1]
		retFiles = append(retFiles, file)
		if snapshot.LE(&compactedFiles[len(compactedFiles)-1].end) {
			// If this condition is met, you can return the compacted checkpoint + one normal checkpoint,
			// otherwise you need to call FilterSortedMetaFilesByTimestamp to handle it normally.

			// The compacted checkpoint already contains the range of the snapshot, but in
			// order to avoid data loss, an additional checkpoint is still needed, because
			// the object flushed by the snapshot may be in the next checkpoint
			for _, f := range metaFiles {
				if !f.start.IsEmpty() && f.start.GE(&file.end) {
					retFiles = append(retFiles, f)
					break
				}
			}
			return retFiles, nil
		}
	}

	// The normal checkpoint meta file records a checkpoint interval,
	// so you only need to read the last meta file
	ickpFiles := FilterSortedMetaFilesByTimestamp(&snapshot, metaFiles)

	retFiles = append(retFiles, ickpFiles...)

	return retFiles, nil
}

func loadCheckpointMeta(
	ctx context.Context,
	sid string,
	fs fileservice.FileService,
	metaFiles []*MetaFile,
) (entries []*CheckpointEntry, err error) {
	colNames := CheckpointSchema.Attrs()
	colTypes := CheckpointSchema.Types()
	bat := containers.NewBatch()
	var (
		tmpBat *batch.Batch
	)
	loader := func(name string, start, end types.TS) (err error) {
		var reader *blockio.BlockReader
		var bats []*batch.Batch
		var closeCB func()
		reader, err = blockio.NewFileReader(sid, fs, name)
		if err != nil {
			return err
		}
		bats, closeCB, err = reader.LoadAllColumns(ctx, nil, common.DebugAllocator)
		if err != nil {
			return
		}
		defer func() {
			if closeCB != nil {
				closeCB()
			}
		}()

		if len(bats) > 1 {
			panic("unexpected multiple batches in a checkpoint file")
		}
		if len(bats) == 0 {
			return
		}
		bats[0].SetAttributes(colNames)
		if tmpBat == nil {
			tmpBat, err = bats[0].Dup(common.DebugAllocator)
			if err != nil {
				return
			}
		} else {
			// The incremental checkpoint meta records an interval,
			// and you need to add the specified checkpoint information to tmpBat
			// according to start and end.
			// start is file name start, end is file name end
			appendCheckpointToBatch(tmpBat, bats[0], start, end, common.DebugAllocator)
		}
		return
	}

	for _, metaFile := range metaFiles {
		err = loader(CheckpointDir+metaFile.name, metaFile.start, metaFile.end)
		if err != nil {
			return
		}
	}

	for i := range tmpBat.Vecs {
		var vec containers.Vector
		if tmpBat.Vecs[i].Length() == 0 {
			vec = containers.MakeVector(colTypes[i], common.DebugAllocator)
		} else {
			vec = containers.ToTNVector(tmpBat.Vecs[i], common.DebugAllocator)
		}
		bat.AddVector(colNames[i], vec)
	}
	defer tmpBat.Clean(common.DebugAllocator)
	// in version 1, checkpoint metadata doesn't contain 'version'.
	vecLen := len(bat.Vecs)
	var checkpointVersion int
	if vecLen < CheckpointSchemaColumnCountV1 {
		checkpointVersion = 1
	} else if vecLen < CheckpointSchemaColumnCountV2 {
		checkpointVersion = 2
	} else {
		checkpointVersion = 3
	}
	return ListSnapshotCheckpointWithMeta(bat, checkpointVersion)
}

func ListSnapshotCheckpointWithMeta(
	bat *containers.Batch,
	version int,
) ([]*CheckpointEntry, error) {
	defer bat.Close()
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

func appendCheckpointToBatch(dst, src *batch.Batch, start, end types.TS, mp *mpool.MPool) {
	tSrc := containers.ToTNBatch(src, mp)
	tDst := containers.ToTNBatch(dst, mp)
	length := tSrc.Vecs[0].Length() - 1
	startTs := vector.MustFixedColWithTypeCheck[types.TS](tSrc.Vecs[0].GetDownstreamVector())
	endTs := vector.MustFixedColWithTypeCheck[types.TS](tSrc.Vecs[1].GetDownstreamVector())
	for i := length; i >= 0; i-- {
		if !startTs[i].EQ(&start) || !endTs[i].EQ(&end) {
			continue
		}
		for v, vec := range tSrc.Vecs {
			val := vec.Get(i)
			if val == nil {
				tDst.Vecs[v].Append(val, true)
			} else {
				tDst.Vecs[v].Append(val, false)
			}
		}
		return
	}
	panic("don't find the value in the batch")
}
