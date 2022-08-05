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

	"github.com/tidwall/btree"
)

// MemoryFS is an in-memory FileService implementation
type MemoryFS struct {
	sync.RWMutex
	tree *btree.Generic[*_MemFSEntry]
}

var _ FileService = new(MemoryFS)

func NewMemoryFS() (*MemoryFS, error) {
	return &MemoryFS{
		tree: btree.NewGeneric(func(a, b *_MemFSEntry) bool {
			return a.FilePath < b.FilePath
		}),
	}, nil
}

func (m *MemoryFS) List(ctx context.Context, dirPath string) (entries []DirEntry, err error) {
	m.RLock()
	defer m.RUnlock()

	iter := m.tree.Iter()
	defer iter.Release()

	pivot := &_MemFSEntry{
		FilePath: dirPath,
	}
	for ok := iter.Seek(pivot); ok; ok = iter.Next() {
		item := iter.Item()
		if !strings.HasPrefix(item.FilePath, dirPath) {
			break
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
				Size:  len(item.Data),
			})
		}
	}

	return
}

func (m *MemoryFS) Write(ctx context.Context, vector IOVector) error {
	m.Lock()
	defer m.Unlock()

	pivot := &_MemFSEntry{
		FilePath: vector.FilePath,
	}
	_, ok := m.tree.Get(pivot)
	if ok {
		return ErrFileExisted
	}

	return m.write(ctx, vector)
}

func (m *MemoryFS) write(ctx context.Context, vector IOVector) error {

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

	r := newIOEntriesReader(vector.Entries)
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	entry := &_MemFSEntry{
		FilePath: vector.FilePath,
		Data:     data,
	}
	m.tree.Set(entry)

	return nil
}

func (m *MemoryFS) Read(ctx context.Context, vector *IOVector) error {

	if len(vector.Entries) == 0 {
		return ErrEmptyVector
	}

	m.RLock()
	defer m.RUnlock()

	pivot := &_MemFSEntry{
		FilePath: vector.FilePath,
	}

	fsEntry, ok := m.tree.Get(pivot)
	if !ok {
		return ErrFileNotFound
	}

	for i, entry := range vector.Entries {
		if entry.ignore {
			continue
		}

		if entry.Size == 0 {
			return ErrEmptyRange
		}
		if entry.Size < 0 {
			entry.Size = len(fsEntry.Data) - entry.Offset
		}
		if entry.Size > len(fsEntry.Data) {
			return ErrUnexpectedEOF
		}
		data := fsEntry.Data[entry.Offset : entry.Offset+entry.Size]

		setData := true

		if w := vector.Entries[i].WriterForRead; w != nil {
			setData = false
			_, err := w.Write(data)
			if err != nil {
				return err
			}
		}

		if ptr := vector.Entries[i].ReadCloserForRead; ptr != nil {
			setData = false
			*ptr = io.NopCloser(bytes.NewReader(data))
		}

		if setData {
			if len(entry.Data) < entry.Size {
				entry.Data = data
			} else {
				copy(entry.Data, data)
			}
		}

		if err := entry.setObjectFromData(); err != nil {
			return err
		}

		vector.Entries[i] = entry
	}

	return nil
}

func (m *MemoryFS) Delete(ctx context.Context, filePath string) error {
	m.Lock()
	defer m.Unlock()

	pivot := &_MemFSEntry{
		FilePath: filePath,
	}
	m.tree.Delete(pivot)

	return nil
}

type _MemFSEntry struct {
	FilePath string
	Data     []byte
}

var _ ReplaceableFileService = new(MemoryFS)

func (m *MemoryFS) Replace(ctx context.Context, vector IOVector) error {
	m.Lock()
	defer m.Unlock()
	return m.write(ctx, vector)
}
