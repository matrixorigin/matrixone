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
	pathpkg "path"
	"sort"
	"strings"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/tidwall/btree"
)

// MemoryFS is an in-memory FileService implementation
type MemoryFS struct {
	name string
	sync.RWMutex
	memCache        *MemCache
	tree            *btree.BTreeG[*_MemFSEntry]
	caches          []IOVectorCache
	perfCounterSets []*perfcounter.CounterSet
	asyncUpdate     bool
}

var _ FileService = new(MemoryFS)

func NewMemoryFS(
	name string,
	cacheConfig CacheConfig,
	perfCounterSets []*perfcounter.CounterSet,
) (*MemoryFS, error) {

	fs := &MemoryFS{
		name:     name,
		memCache: NewMemCache(NewMemoryCache(1<<20, true, nil), nil),
		tree: btree.NewBTreeG(func(a, b *_MemFSEntry) bool {
			return a.FilePath < b.FilePath
		}),
		perfCounterSets: perfCounterSets,
	}

	return fs, nil
}

func (m *MemoryFS) Name() string {
	return m.name
}

func (m *MemoryFS) Close() {
	m.memCache.Flush()
}

func (m *MemoryFS) List(ctx context.Context, dirPath string) (entries []DirEntry, err error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	m.RLock()
	defer m.RUnlock()

	path, err := ParsePathAtService(dirPath, m.name)
	if err != nil {
		return nil, err
	}

	iter := m.tree.Iter()
	defer iter.Release()

	pivot := &_MemFSEntry{
		FilePath: path.File,
	}
	for ok := iter.Seek(pivot); ok; ok = iter.Next() {
		item := iter.Item()
		if !strings.HasPrefix(item.FilePath, path.File) {
			break
		}

		relPath := strings.TrimPrefix(item.FilePath, path.File)
		relPath = strings.Trim(relPath, "/")
		parts := strings.Split(relPath, "/")
		isDir := len(parts) > 1
		name := parts[0]

		if len(entries) == 0 || entries[len(entries)-1].Name != name {
			entries = append(entries, DirEntry{
				IsDir: isDir,
				Name:  name,
				Size:  int64(len(item.Data)),
			})
		}
	}

	return
}

func (m *MemoryFS) Write(ctx context.Context, vector IOVector) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	m.Lock()
	defer m.Unlock()

	path, err := ParsePathAtService(vector.FilePath, m.name)
	if err != nil {
		return err
	}

	pivot := &_MemFSEntry{
		FilePath: path.File,
	}
	_, ok := m.tree.Get(pivot)
	if ok {
		return moerr.NewFileAlreadyExistsNoCtx(path.File)
	}

	return m.write(ctx, vector)
}

func (m *MemoryFS) write(ctx context.Context, vector IOVector) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	path, err := ParsePathAtService(vector.FilePath, m.name)
	if err != nil {
		return err
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

	r := newIOEntriesReader(ctx, vector.Entries)

	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	entry := &_MemFSEntry{
		FilePath: path.File,
		Data:     data,
	}
	m.tree.Set(entry)

	return nil
}

func (m *MemoryFS) Read(ctx context.Context, vector *IOVector) (err error) {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	for _, cache := range m.caches {
		cache := cache
		if err := cache.Read(ctx, vector); err != nil {
			return err
		}
		defer func() {
			if err != nil {
				return
			}
			err = cache.Update(ctx, vector, m.asyncUpdate)
		}()
	}

	path, err := ParsePathAtService(vector.FilePath, m.name)
	if err != nil {
		return err
	}

	if len(vector.Entries) == 0 {
		return moerr.NewEmptyVectorNoCtx()
	}

	m.RLock()
	defer m.RUnlock()

	pivot := &_MemFSEntry{
		FilePath: path.File,
	}

	fsEntry, ok := m.tree.Get(pivot)
	if !ok {
		return moerr.NewFileNotFoundNoCtx(path.File)
	}

	allocator := m.memCache

	for i, entry := range vector.Entries {
		if entry.done {
			continue
		}

		if entry.Size == 0 {
			return moerr.NewEmptyRangeNoCtx(path.File)
		}
		if entry.Size < 0 {
			entry.Size = int64(len(fsEntry.Data)) - entry.Offset
		}
		if entry.Size > int64(len(fsEntry.Data)) {
			return moerr.NewUnexpectedEOFNoCtx(path.File)
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
			if int64(len(entry.Data)) < entry.Size || entry.Size < 0 {
				entry.Data = data
				if entry.Size < 0 {
					entry.Size = int64(len(data))
				}
			} else {
				copy(entry.Data, data)
			}
		}

		if err := setCachedData(&entry, allocator); err != nil {
			return err
		}

		vector.Entries[i] = entry
	}

	return nil
}

func (m *MemoryFS) ReadCache(ctx context.Context, vector *IOVector) (err error) {
	return nil
}

func (m *MemoryFS) StatFile(ctx context.Context, filePath string) (*DirEntry, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	path, err := ParsePathAtService(filePath, m.name)
	if err != nil {
		return nil, err
	}

	m.RLock()
	defer m.RUnlock()

	pivot := &_MemFSEntry{
		FilePath: path.File,
	}

	fsEntry, ok := m.tree.Get(pivot)
	if !ok {
		return nil, moerr.NewFileNotFoundNoCtx(path.File)
	}

	return &DirEntry{
		Name:  pathpkg.Base(filePath),
		IsDir: false,
		Size:  int64(len(fsEntry.Data)),
	}, nil
}

func (m *MemoryFS) PrefetchFile(ctx context.Context, filePath string) error {
	return nil
}

func (m *MemoryFS) Delete(ctx context.Context, filePaths ...string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	m.Lock()
	defer m.Unlock()
	for _, filePath := range filePaths {
		if err := m.deleteSingle(ctx, filePath); err != nil {
			return err
		}
	}
	return nil
}

func (m *MemoryFS) deleteSingle(ctx context.Context, filePath string) error {

	path, err := ParsePathAtService(filePath, m.name)
	if err != nil {
		return err
	}

	pivot := &_MemFSEntry{
		FilePath: path.File,
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

// mark MemoryFS as ETL-compatible to use it as ETL fs in testing
var _ ETLFileService = new(MemoryFS)

func (m *MemoryFS) ETLCompatible() {}
