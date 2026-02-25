// Copyright 2024 Matrix Origin
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

package backup

import (
	"bytes"
	"context"
	"encoding/csv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
)

const (
	// FileIndexPrefix is the prefix for file index files
	FileIndexPrefix = "file_index_"
	// FileIndexSuffix is the suffix for file index files
	FileIndexSuffix = ".idx"
)

// GlobalFileIndex represents the global file index for backup
// It maps filename to whether the file has been backed up
type GlobalFileIndex struct {
	files map[string]bool
}

// NewGlobalFileIndex creates a new empty global file index
func NewGlobalFileIndex() *GlobalFileIndex {
	return &GlobalFileIndex{
		files: make(map[string]bool),
	}
}

// Has checks if a file exists in the index
func (idx *GlobalFileIndex) Has(fileName string) bool {
	if idx == nil || idx.files == nil {
		return false
	}
	return idx.files[fileName]
}

// Add adds a file to the index
func (idx *GlobalFileIndex) Add(fileName string) {
	if idx == nil || idx.files == nil {
		return
	}
	idx.files[fileName] = true
}

// Size returns the number of files in the index
func (idx *GlobalFileIndex) Size() int {
	if idx == nil || idx.files == nil {
		return 0
	}
	return len(idx.files)
}

// isFileIndexName checks if a file name is a file index file
func isFileIndexName(name string) bool {
	return strings.HasPrefix(name, FileIndexPrefix) && strings.HasSuffix(name, FileIndexSuffix)
}

// parseFileIndexTS extracts the backup timestamp from an index file name
func parseFileIndexTS(name string) string {
	if !isFileIndexName(name) {
		return ""
	}
	ts := strings.TrimPrefix(name, FileIndexPrefix)
	ts = strings.TrimSuffix(ts, FileIndexSuffix)
	return ts
}

// LoadGlobalFileIndex loads the latest global file index from the backup root directory
// rootFs should be the file service pointing to the backup root directory (parent of current backup)
func LoadGlobalFileIndex(ctx context.Context, rootFs fileservice.FileService) (*GlobalFileIndex, error) {
	if rootFs == nil {
		return nil, nil
	}

	// List all files in the backup root
	entries, err := fileservice.SortedList(rootFs.List(ctx, ""))
	if err != nil {
		logutil.Warnf("backup: failed to list backup root for index: %v", err)
		return nil, nil
	}

	// Find all index files and get the latest one
	var latestIndexName string
	var latestTS string
	for _, entry := range entries {
		if entry.IsDir {
			continue
		}
		if isFileIndexName(entry.Name) {
			ts := parseFileIndexTS(entry.Name)
			if ts > latestTS { // String comparison works for timestamp format
				latestTS = ts
				latestIndexName = entry.Name
			}
		}
	}

	if latestIndexName == "" {
		logutil.Info("backup: no global file index found")
		return nil, nil
	}

	// Read the index file
	ioVec := &fileservice.IOVector{
		FilePath: latestIndexName,
		Entries: []fileservice.IOEntry{
			{
				Offset: 0,
				Size:   -1,
			},
		},
	}
	err = rootFs.Read(ctx, ioVec)
	if err != nil {
		logutil.Warnf("backup: failed to read index file %s: %v", latestIndexName, err)
		return nil, nil
	}

	data := ioVec.Entries[0].Data

	// Parse CSV
	reader := csv.NewReader(bytes.NewReader(data))
	lines, err := reader.ReadAll()
	if err != nil {
		logutil.Warnf("backup: failed to parse index file %s: %v", latestIndexName, err)
		return nil, nil
	}

	// Build index
	idx := NewGlobalFileIndex()
	for _, line := range lines {
		if len(line) >= 1 {
			idx.Add(line[0]) // First column is filename
		}
	}

	logutil.Infof("backup: loaded global file index %s with %d entries", latestIndexName, idx.Size())
	return idx, nil
}
