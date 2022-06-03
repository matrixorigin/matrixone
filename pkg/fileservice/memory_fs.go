// Copyright 2022 Matrix Origin
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

package fileservice

import (
	"bytes"
	"context"
	"io"
	"sort"
	"strings"
	"sync"

	"github.com/google/btree"
)

// MemoryFS is an in-memory FileService implementation
type MemoryFS struct {
	sync.RWMutex
	tree *btree.BTree
}

var _ FileService = new(MemoryFS)

//TODO
//var _ MutableFileService = new(MemoryFS)

func NewMemoryFS() (*MemoryFS, error) {
	return &MemoryFS{
		tree: btree.New(2),
	}, nil
}

func (m *MemoryFS) List(ctx context.Context, dirPath string) (entries []DirEntry, err error) {
	m.RLock()
	defer m.RUnlock()

	pivot := &_MemIOEntry{
		FilePath: dirPath,
	}
	m.tree.AscendGreaterOrEqual(pivot, func(v btree.Item) bool {
		item := v.(*_MemIOEntry)
		if !strings.HasPrefix(item.FilePath, dirPath) {
			return false
		}

		relPath := strings.TrimPrefix(item.FilePath, dirPath)
		relPath = strings.Trim(relPath, "/")
		parts := strings.Split(relPath, "/")
		isDir := len(parts) > 1
		name := parts[0]

		if len(entries) == 0 || entries[len(entries)-1].Name != name {
			entries = append(entries, DirEntry{
				IsDir: isDir,
				Name:  name,
			})
		}

		return true
	})

	return
}

func (m *MemoryFS) Write(ctx context.Context, vector IOVector) error {
	m.Lock()
	defer m.Unlock()

	pivot := &_MemIOEntry{
		FilePath: vector.FilePath,
	}
	existed := false
	m.tree.AscendGreaterOrEqual(pivot, func(item btree.Item) bool {
		entry := item.(*_MemIOEntry)
		if entry.FilePath == vector.FilePath {
			existed = true
		}
		return false
	})
	if existed {
		return ErrFileExisted
	}

	if len(vector.Entries) == 0 {
		vector.Entries = []IOEntry{
			{
				Offset: 0,
				Size:   0,
				Data:   nil,
			},
		}
	}

	sort.Slice(vector.Entries, func(i, j int) bool {
		return vector.Entries[i].Offset < vector.Entries[j].Offset
	})
	for _, entry := range vector.Entries {
		var data []byte
		if entry.ReaderForWrite != nil {
			var err error
			data, err = io.ReadAll(entry.ReaderForWrite)
			if err != nil {
				return err
			}
		} else {
			data = make([]byte, len(entry.Data))
			copy(data, entry.Data)
		}
		if len(data) != entry.Size {
			return ErrSizeNotMatch
		}
		item := &_MemIOEntry{
			FilePath: vector.FilePath,
			Offset:   entry.Offset,
			Size:     len(data),
			Data:     data,
		}
		m.tree.ReplaceOrInsert(item)
	}

	return nil
}

func (m *MemoryFS) Read(ctx context.Context, vector *IOVector) error {
	m.RLock()
	defer m.RUnlock()

	pivot := &_MemIOEntry{
		FilePath: vector.FilePath,
	}

	fileFound := false
	for i, entry := range vector.Entries {
		pivot.Offset = entry.Offset
		pivot.Size = entry.Size
		pivot.Data = make([]byte, 0, entry.Size)

		m.tree.DescendLessOrEqual(pivot, func(item btree.Item) bool {
			src := item.(*_MemIOEntry)
			if src.FilePath != pivot.FilePath {
				return false
			}
			fileFound = true
			if pivot.Size <= 0 {
				return false
			}
			if pivot.Offset > src.Offset+src.Size {
				return false
			}
			startPos := pivot.Offset - src.Offset
			endPos := startPos + pivot.Size
			if endPos > src.Size {
				endPos = src.Size
			}
			pivot.Data = append(pivot.Data, src.Data[startPos:endPos]...)
			n := endPos - startPos
			pivot.Offset += n
			pivot.Size -= n
			return true
		})

		m.tree.AscendGreaterOrEqual(pivot, func(item btree.Item) bool {
			src := item.(*_MemIOEntry)
			if src.FilePath != pivot.FilePath {
				return false
			}
			fileFound = true
			if pivot.Size <= 0 {
				return false
			}
			if pivot.Offset+pivot.Size < src.Offset {
				return false
			}
			startPos := pivot.Offset - src.Offset
			endPos := startPos + pivot.Size
			if endPos > src.Size {
				endPos = src.Size
			}
			pivot.Data = append(pivot.Data, src.Data[startPos:endPos]...)
			n := endPos - startPos
			pivot.Offset += n
			pivot.Size -= n
			return true
		})

		if pivot.Size > 0 {
			return ErrUnexpectedEOF
		}
		if len(pivot.Data) == 0 {
			return ErrEmptyRange
		}

		setData := true
		if w := vector.Entries[i].WriterForRead; w != nil {
			setData = false
			_, err := w.Write(pivot.Data)
			if err != nil {
				return err
			}
		}
		if ptr := vector.Entries[i].ReadCloserForRead; ptr != nil {
			setData = false
			*ptr = io.NopCloser(bytes.NewReader(pivot.Data))
		}
		if setData {
			vector.Entries[i].Data = pivot.Data
		}
	}

	if !fileFound {
		return ErrFileNotFound
	}

	return nil
}

func (m *MemoryFS) Delete(ctx context.Context, filePath string) error {
	m.Lock()
	defer m.Unlock()

	pivot := &_MemIOEntry{
		FilePath: filePath,
	}
	var toDelete []btree.Item
	m.tree.AscendGreaterOrEqual(pivot, func(v btree.Item) bool {
		item := v.(*_MemIOEntry)
		if item.FilePath != filePath {
			return false
		}
		toDelete = append(toDelete, v)
		return true
	})
	for _, v := range toDelete {
		m.tree.Delete(v)
	}

	return nil
}

type _MemIOEntry struct {
	FilePath string
	Offset   int
	Size     int
	Data     []byte
}

var _ btree.Item = new(_MemIOEntry)

func (m *_MemIOEntry) Less(than btree.Item) bool {
	m2 := than.(*_MemIOEntry)
	if m.FilePath != m2.FilePath {
		return m.FilePath < m2.FilePath
	}
	return m.Offset < m2.Offset
}
