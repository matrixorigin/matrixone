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
	"sort"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
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
	files []ioutil.TSRangeFile,
) []ioutil.TSRangeFile {
	if len(files) == 0 {
		return nil
	}

	prevFile := files[0]

	// start.IsEmpty() means the file is a global checkpoint
	// ts.LE(&prevFile.end) means the ts is in the range of the checkpoint
	// it means the ts is in the range of the global checkpoint
	// ts is within GCKP[0, end]
	if prevFile.GetStart().IsEmpty() && ts.LE(prevFile.GetEnd()) {
		return files[:1]
	}

	for i := 1; i < len(files); i++ {
		curr := files[i]
		// curr.start.IsEmpty() means the file is a global checkpoint
		// ts.LE(&curr.end) means the ts is in the range of the checkpoint
		// ts.LT(&prevFile.end) means the ts is not in the range of the previous checkpoint
		if curr.GetStart().IsEmpty() && ts.LE(curr.GetEnd()) {
			if ts.Equal(curr.GetEnd()) {
				return files[:i+1]
			}
			return files[:i]
		}
	}

	return files
}

func getSnapshotMetaFiles(
	metaFiles, compactedFiles []ioutil.TSRangeFile,
	snapshot *types.TS,
) []ioutil.TSRangeFile {
	sort.Slice(compactedFiles, func(i, j int) bool {
		return compactedFiles[i].GetEnd().LT(compactedFiles[j].GetEnd())
	})

	sort.Slice(metaFiles, func(i, j int) bool {
		return metaFiles[i].GetEnd().LT(metaFiles[j].GetEnd())
	})

	retFiles := make([]ioutil.TSRangeFile, 0)
	if len(compactedFiles) > 0 {
		file := compactedFiles[len(compactedFiles)-1]
		retFiles = append(retFiles, file)
		if snapshot.LE(compactedFiles[len(compactedFiles)-1].GetEnd()) {
			// If this condition is met, you can return the compacted checkpoint + one normal checkpoint,
			// otherwise you need to call FilterSortedMetaFilesByTimestamp to handle it normally.

			// The compacted checkpoint already contains the range of the snapshot, but in
			// order to avoid data loss, an additional checkpoint is still needed, because
			// the object flushed by the snapshot may be in the next checkpoint
			for _, f := range metaFiles {
				if !f.GetStart().IsEmpty() && f.GetStart().GE(file.GetEnd()) {
					retFiles = append(retFiles, f)
					break
				}
			}
			return retFiles
		}
	}

	// The normal checkpoint meta file records a checkpoint interval,
	// so you only need to read the last meta file
	iCkpFiles := FilterSortedMetaFilesByTimestamp(snapshot, metaFiles)

	retFiles = append(retFiles, iCkpFiles...)

	return retFiles
}

func ListSnapshotCheckpoint(
	ctx context.Context,
	sid string,
	fs fileservice.FileService,
	snapshot types.TS,
	allFiles map[string]struct{},
) ([]*CheckpointEntry, error) {
	if len(allFiles) == 0 {
		return nil, nil
	}
	metaFiles := make([]ioutil.TSRangeFile, 0)
	compactedFiles := make([]ioutil.TSRangeFile, 0)
	for name := range allFiles {
		meta := ioutil.DecodeTSRangeFile(name)
		if meta.IsCompactExt() {
			compactedFiles = append(compactedFiles, meta)
		} else {
			// PXU FIXME: we should filter out the unexpected meta files
			metaFiles = append(metaFiles, meta)
		}
	}
	return loadCheckpointMeta(
		ctx, sid, getSnapshotMetaFiles(metaFiles, compactedFiles, &snapshot), fs,
		&snapshot,
	)
}

func loadCheckpointMeta(
	ctx context.Context,
	sid string,
	metaFiles []ioutil.TSRangeFile,
	fs fileservice.FileService,
	snapshot *types.TS,
) (entries []*CheckpointEntry, err error) {
	allEntries := make([]*CheckpointEntry, 0)

	// Process each meta file individually to apply the filtering logic
	for _, metaFile := range metaFiles {

		// Create reader for this specific file
		reader := NewCKPMetaReader(sid, "", []string{metaFile.GetCKPFullName()}, 0, fs)
		getter := MetadataEntryGetter{reader: reader}

		// Read entries from this file
		fileEntries := make([]*CheckpointEntry, 0)
		for {
			batchEntries, err := getter.NextBatch(ctx, nil, common.DebugAllocator)
			if err != nil {
				if moerr.IsMoErrCode(err, moerr.OkStopCurrRecur) {
					break
				}
				getter.Close() // Close immediately on error
				return nil, err
			}
			fileEntries = append(fileEntries, batchEntries...)
		}
		getter.Close() // Close after successful reading

		// Filter entries that match the file's start and end timestamps
		// This replicates the logic from appendCheckpointToBatch
		fileStart := metaFile.GetStart()
		fileEnd := metaFile.GetEnd()
		filteredEntries := filterEntriesByTimestamp(fileEntries, fileStart, fileEnd)
		allEntries = append(allEntries, filteredEntries...)
	}

	// Apply the same logic as ListSnapshotCheckpointWithMeta
	return filterSnapshotEntries(allEntries, snapshot), nil
}

// filterEntriesByTimestamp filters checkpoint entries that match the given start and end timestamps
// This function replicates the filtering logic from the original appendCheckpointToBatch function
func filterEntriesByTimestamp(entries []*CheckpointEntry, fileStart, fileEnd *types.TS) []*CheckpointEntry {
	filteredEntries := make([]*CheckpointEntry, 0)
	for _, entry := range entries {
		if entry != nil && fileStart != nil && fileEnd != nil &&
			entry.start.EQ(fileStart) && entry.end.EQ(fileEnd) {
			filteredEntries = append(filteredEntries, entry)
		}
	}
	return filteredEntries
}

// filterSnapshotEntries implements the same logic as ListSnapshotCheckpointWithMeta
func filterSnapshotEntries(entries []*CheckpointEntry, snapshot *types.TS) []*CheckpointEntry {
	if len(entries) == 0 {
		return entries
	}

	// Find the maximum global end timestamp
	var maxGlobalEnd types.TS
	for _, entry := range entries {
		if entry != nil && entry.entryType == ET_Global {
			if entry.end.GT(&maxGlobalEnd) {
				maxGlobalEnd = entry.end
			}
		}
	}

	// Sort by end timestamp
	sort.Slice(entries, func(i, j int) bool {
		if entries[i] == nil || entries[j] == nil {
			return false
		}
		return entries[i].end.LT(&entries[j].end)
	})

	if snapshot != nil && snapshot.Equal(&maxGlobalEnd) {
		// Find the global checkpoint with end == maxGlobalEnd
		for i := range entries {
			if entries[i].end.Equal(&maxGlobalEnd) &&
				entries[i].entryType == ET_Global {
				// Return only the global checkpoint, since snapshot ts == gckp.end
				return []*CheckpointEntry{entries[i]}
			}
		}
	}
	// Find the appropriate truncation point
	for i := range entries {
		if entries[i] == nil {
			continue
		}
		p := maxGlobalEnd.Prev()
		if entries[i].end.Equal(&p) || (entries[i].end.Equal(&maxGlobalEnd) &&
			entries[i].entryType == ET_Global) {
			return entries[i:]
		}
	}

	return entries
}
