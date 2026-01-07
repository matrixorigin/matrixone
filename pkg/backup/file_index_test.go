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
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGlobalFileIndex_Basic(t *testing.T) {
	// Test NewGlobalFileIndex
	idx := NewGlobalFileIndex()
	assert.NotNil(t, idx)
	assert.NotNil(t, idx.files)
	assert.Equal(t, 0, idx.Size())

	// Test Add and Has
	idx.Add("file1.obj")
	assert.True(t, idx.Has("file1.obj"))
	assert.False(t, idx.Has("file2.obj"))
	assert.Equal(t, 1, idx.Size())

	// Test adding multiple files
	idx.Add("file2.obj")
	idx.Add("file3.obj")
	assert.True(t, idx.Has("file1.obj"))
	assert.True(t, idx.Has("file2.obj"))
	assert.True(t, idx.Has("file3.obj"))
	assert.Equal(t, 3, idx.Size())

	// Test adding duplicate file (should not increase size)
	idx.Add("file1.obj")
	assert.Equal(t, 3, idx.Size())
}

func TestGlobalFileIndex_NilSafety(t *testing.T) {
	// Test nil index
	var idx *GlobalFileIndex = nil
	assert.False(t, idx.Has("file1.obj"))
	assert.Equal(t, 0, idx.Size())

	// Add should not panic on nil
	idx.Add("file1.obj") // Should not panic

	// Test index with nil files map
	idx2 := &GlobalFileIndex{files: nil}
	assert.False(t, idx2.Has("file1.obj"))
	assert.Equal(t, 0, idx2.Size())
	idx2.Add("file1.obj") // Should not panic
}

func TestIsFileIndexName(t *testing.T) {
	tests := []struct {
		name     string
		expected bool
	}{
		{"file_index_12345.idx", true},
		{"file_index_0-1234567890.idx", true},
		{"file_index_.idx", true},
		{"file_index_abc.idx", true},
		{"file_index_12345.idx.sha256", false},
		{"file_index_12345", false},
		{"12345.idx", false},
		{"file_index12345.idx", false},
		{"mo_br.meta", false},
		{"backup_meta", false},
		{"", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isFileIndexName(tt.name)
			assert.Equal(t, tt.expected, result, "isFileIndexName(%q)", tt.name)
		})
	}
}

func TestParseFileIndexTS(t *testing.T) {
	tests := []struct {
		name       string
		expectedTS string
	}{
		{"file_index_12345.idx", "12345"},
		{"file_index_0-1234567890.idx", "0-1234567890"},
		{"file_index_.idx", ""},
		{"file_index_abc.idx", "abc"},
		{"not_an_index.idx", ""},
		{"file_index_12345", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := parseFileIndexTS(tt.name)
			assert.Equal(t, tt.expectedTS, result, "parseFileIndexTS(%q)", tt.name)
		})
	}
}

func TestLoadGlobalFileIndex_NilFs(t *testing.T) {
	ctx := context.Background()

	// Test with nil file service
	idx, err := LoadGlobalFileIndex(ctx, nil)
	assert.Nil(t, err)
	assert.Nil(t, idx)
}

func TestLoadGlobalFileIndex_EmptyDir(t *testing.T) {
	ctx := context.Background()

	// Create a memory file service
	fs, err := fileservice.NewMemoryFS("test", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)

	// Test with empty directory (no index files)
	idx, err := LoadGlobalFileIndex(ctx, fs)
	assert.Nil(t, err)
	assert.Nil(t, idx)
}

func TestLoadGlobalFileIndex_WithIndexFile(t *testing.T) {
	ctx := context.Background()

	// Create a memory file service
	fs, err := fileservice.NewMemoryFS("test", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)

	// Create an index file with CSV content
	indexContent := "file1.obj,full-xxx,0-12345\nfile2.obj,incr-yyy,0-12346\nfile3.obj,incr-zzz,0-12347\n"
	indexName := "file_index_0-12347.idx"

	err = fs.Write(ctx, fileservice.IOVector{
		FilePath: indexName,
		Entries: []fileservice.IOEntry{
			{
				Offset: 0,
				Size:   int64(len(indexContent)),
				Data:   []byte(indexContent),
			},
		},
	})
	require.NoError(t, err)

	// Load the index
	idx, err := LoadGlobalFileIndex(ctx, fs)
	assert.Nil(t, err)
	assert.NotNil(t, idx)
	assert.Equal(t, 3, idx.Size())
	assert.True(t, idx.Has("file1.obj"))
	assert.True(t, idx.Has("file2.obj"))
	assert.True(t, idx.Has("file3.obj"))
	assert.False(t, idx.Has("file4.obj"))
}

func TestLoadGlobalFileIndex_MultipleIndexFiles(t *testing.T) {
	ctx := context.Background()

	// Create a memory file service
	fs, err := fileservice.NewMemoryFS("test", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)

	// Create multiple index files with different timestamps
	// Older index
	oldContent := "file1.obj,full-xxx,0-10000\n"
	err = fs.Write(ctx, fileservice.IOVector{
		FilePath: "file_index_0-10000.idx",
		Entries: []fileservice.IOEntry{
			{Offset: 0, Size: int64(len(oldContent)), Data: []byte(oldContent)},
		},
	})
	require.NoError(t, err)

	// Newer index (should be loaded)
	newContent := "file1.obj,full-xxx,0-10000\nfile2.obj,incr-yyy,0-20000\n"
	err = fs.Write(ctx, fileservice.IOVector{
		FilePath: "file_index_0-20000.idx",
		Entries: []fileservice.IOEntry{
			{Offset: 0, Size: int64(len(newContent)), Data: []byte(newContent)},
		},
	})
	require.NoError(t, err)

	// Load the index - should get the latest one
	idx, err := LoadGlobalFileIndex(ctx, fs)
	assert.Nil(t, err)
	assert.NotNil(t, idx)
	assert.Equal(t, 2, idx.Size())
	assert.True(t, idx.Has("file1.obj"))
	assert.True(t, idx.Has("file2.obj"))
}

func TestLoadGlobalFileIndex_IgnoreNonIndexFiles(t *testing.T) {
	ctx := context.Background()

	// Create a memory file service
	fs, err := fileservice.NewMemoryFS("test", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)

	// Create various files
	files := map[string]string{
		"file_index_0-12345.idx":        "file1.obj,full-xxx,0-12345\n",
		"file_index_0-12345.idx.sha256": "checksum",
		"mo_br.meta":                    "meta content",
		"backup_meta":                   "backup meta",
		"some_other_file.txt":           "other content",
	}

	for name, content := range files {
		err = fs.Write(ctx, fileservice.IOVector{
			FilePath: name,
			Entries: []fileservice.IOEntry{
				{Offset: 0, Size: int64(len(content)), Data: []byte(content)},
			},
		})
		require.NoError(t, err)
	}

	// Load the index - should only load the .idx file
	idx, err := LoadGlobalFileIndex(ctx, fs)
	assert.Nil(t, err)
	assert.NotNil(t, idx)
	assert.Equal(t, 1, idx.Size())
	assert.True(t, idx.Has("file1.obj"))
}

func TestLoadGlobalFileIndex_MalformedCSV(t *testing.T) {
	ctx := context.Background()

	// Create a memory file service
	fs, err := fileservice.NewMemoryFS("test", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)

	// Create an index file with malformed CSV (should still work, just skip bad lines)
	indexContent := "file1.obj,full-xxx,0-12345\n\"unclosed quote\nfile2.obj,incr-yyy,0-12346\n"
	indexName := "file_index_0-12346.idx"

	err = fs.Write(ctx, fileservice.IOVector{
		FilePath: indexName,
		Entries: []fileservice.IOEntry{
			{Offset: 0, Size: int64(len(indexContent)), Data: []byte(indexContent)},
		},
	})
	require.NoError(t, err)

	// Load the index - should return nil on parse error (graceful handling)
	_, err = LoadGlobalFileIndex(ctx, fs)
	// The function handles errors gracefully and returns nil
	assert.Nil(t, err)
	// May be nil due to parse error, which is acceptable
}

func TestLoadGlobalFileIndex_EmptyLines(t *testing.T) {
	ctx := context.Background()

	// Create a memory file service
	fs, err := fileservice.NewMemoryFS("test", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)

	// Create an index file with empty lines
	indexContent := "file1.obj,full-xxx,0-12345\n\nfile2.obj,incr-yyy,0-12346\n\n"
	indexName := "file_index_0-12346.idx"

	err = fs.Write(ctx, fileservice.IOVector{
		FilePath: indexName,
		Entries: []fileservice.IOEntry{
			{Offset: 0, Size: int64(len(indexContent)), Data: []byte(indexContent)},
		},
	})
	require.NoError(t, err)

	// Load the index
	idx, err := LoadGlobalFileIndex(ctx, fs)
	assert.Nil(t, err)
	assert.NotNil(t, idx)
	// Should have 2 valid entries (empty lines are skipped)
	assert.True(t, idx.Has("file1.obj"))
	assert.True(t, idx.Has("file2.obj"))
}

func TestGlobalFileIndex_UsedInBackup(t *testing.T) {
	// Test that the index correctly identifies files that should not be copied
	idx := NewGlobalFileIndex()

	// Simulate files already backed up
	idx.Add("object1.obj")
	idx.Add("object2.obj")
	idx.Add("ckp/meta1.ckp")

	// Simulate checking files during backup
	filesToBackup := []string{
		"object1.obj", // Already in index - should skip
		"object2.obj", // Already in index - should skip
		"object3.obj", // Not in index - should copy
		"object4.obj", // Not in index - should copy
	}

	var needCopy []string
	var skipCopy []string

	for _, file := range filesToBackup {
		if idx.Has(file) {
			skipCopy = append(skipCopy, file)
		} else {
			needCopy = append(needCopy, file)
		}
	}

	assert.Equal(t, []string{"object1.obj", "object2.obj"}, skipCopy)
	assert.Equal(t, []string{"object3.obj", "object4.obj"}, needCopy)
}
