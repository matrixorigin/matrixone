// Copyright 2021 Matrix Origin
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

package toolfs

import (
	"context"
	"io"
	"iter"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
)

type lazyCacheFS struct {
	name   string
	remote fileservice.FileService
	local  *fileservice.LocalFS
	root   string

	mu      sync.Mutex
	caching map[string]*sync.Once
}

func newLazyCacheFS(ctx context.Context, remote fileservice.FileService) (fileservice.FileService, string, error) {
	root, err := os.MkdirTemp("", "mo-tool-remote-cache-*")
	if err != nil {
		return nil, "", err
	}
	local, err := fileservice.NewLocalFS(ctx, remote.Name(), root, fileservice.DisabledCacheConfig, nil)
	if err != nil {
		_ = os.RemoveAll(root)
		return nil, "", err
	}
	return &lazyCacheFS{
		name:    remote.Name(),
		remote:  remote,
		local:   local,
		root:    root,
		caching: make(map[string]*sync.Once),
	}, root, nil
}

func (l *lazyCacheFS) Name() string { return l.name }

func (l *lazyCacheFS) Write(ctx context.Context, vector fileservice.IOVector) error {
	if err := l.remote.Write(ctx, vector); err != nil {
		return err
	}
	normalized := stripServiceName(vector.FilePath, l.name)
	_ = os.Remove(filepath.Join(l.root, filepath.FromSlash(normalized)))
	l.mu.Lock()
	delete(l.caching, normalized)
	l.mu.Unlock()
	return nil
}

func (l *lazyCacheFS) Read(ctx context.Context, vector *fileservice.IOVector) error {
	if err := l.ensureCached(ctx, vector.FilePath); err != nil {
		return err
	}
	return l.readCachedFile(ctx, vector)
}

func (l *lazyCacheFS) ReadCache(ctx context.Context, vector *fileservice.IOVector) error {
	if err := l.ensureCached(ctx, vector.FilePath); err != nil {
		return err
	}
	return l.readCachedFile(ctx, vector)
}

func (l *lazyCacheFS) List(ctx context.Context, dirPath string) iter.Seq2[*fileservice.DirEntry, error] {
	return l.remote.List(ctx, dirPath)
}

func (l *lazyCacheFS) Delete(ctx context.Context, filePaths ...string) error {
	_ = l.local.Delete(ctx, filePaths...)
	return l.remote.Delete(ctx, filePaths...)
}

func (l *lazyCacheFS) StatFile(ctx context.Context, filePath string) (*fileservice.DirEntry, error) {
	return l.remote.StatFile(ctx, filePath)
}

func (l *lazyCacheFS) PrefetchFile(ctx context.Context, filePath string) error {
	return l.ensureCached(ctx, filePath)
}

func (l *lazyCacheFS) Cost() *fileservice.CostAttr {
	return l.remote.Cost()
}

func (l *lazyCacheFS) Close(ctx context.Context) {
	l.local.Close(ctx)
	l.remote.Close(ctx)
	_ = os.RemoveAll(l.root)
}

func (l *lazyCacheFS) ensureCached(ctx context.Context, filePath string) error {
	normalized := stripServiceName(filePath, l.name)
	localPath := filepath.Join(l.root, filepath.FromSlash(normalized))
	if _, err := os.Stat(localPath); err == nil {
		return nil
	}

	once := l.getOnce(normalized)
	var cacheErr error
	once.Do(func() {
		cacheErr = l.cacheFile(ctx, normalized, localPath)
		if cacheErr != nil {
			l.mu.Lock()
			delete(l.caching, normalized)
			l.mu.Unlock()
		}
	})
	return cacheErr
}

func (l *lazyCacheFS) getOnce(path string) *sync.Once {
	l.mu.Lock()
	defer l.mu.Unlock()
	if once, ok := l.caching[path]; ok {
		return once
	}
	once := &sync.Once{}
	l.caching[path] = once
	return once
}

func (l *lazyCacheFS) cacheFile(ctx context.Context, normalized string, localPath string) error {
	if err := os.MkdirAll(filepath.Dir(localPath), 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(localPath), ".cache-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer func() {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
	}()

	vec := &fileservice.IOVector{
		FilePath: normalized,
		Entries: []fileservice.IOEntry{{
			Offset:        0,
			Size:          -1,
			WriterForRead: tmp,
		}},
	}
	if err := l.remote.Read(ctx, vec); err != nil {
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpName, localPath); err != nil {
		return err
	}
	return nil
}

func (l *lazyCacheFS) readCachedFile(ctx context.Context, vector *fileservice.IOVector) error {
	normalized := stripServiceName(vector.FilePath, l.name)
	localPath := filepath.Join(l.root, filepath.FromSlash(normalized))

	file, err := os.Open(localPath)
	if os.IsNotExist(err) {
		return moerr.NewFileNotFoundNoCtx(normalized)
	}
	if err != nil {
		return err
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		return err
	}
	fileSize := stat.Size()

	for i, entry := range vector.Entries {
		if entry.Size == 0 {
			return moerr.NewEmptyRangeNoCtx(normalized)
		}
		if entry.Offset < 0 {
			return moerr.NewUnexpectedEOFNoCtx(normalized)
		}
		if entry.Size < 0 {
			entry.Size = fileSize - entry.Offset
			if entry.Size < 0 {
				return moerr.NewUnexpectedEOFNoCtx(normalized)
			}
		}
		if _, err := file.Seek(entry.Offset, io.SeekStart); err != nil {
			return err
		}
		if err := entry.ReadFromOSFile(ctx, file, l.local); err != nil {
			if err == io.ErrUnexpectedEOF {
				return moerr.NewUnexpectedEOFNoCtx(normalized)
			}
			return err
		}
		vector.Entries[i] = entry
	}
	return nil
}

func stripServiceName(path string, service string) string {
	prefix := strings.ToUpper(service) + ":"
	if strings.HasPrefix(strings.ToUpper(path), prefix) {
		return path[len(service)+1:]
	}
	if idx := strings.Index(path, ":"); idx >= 0 {
		return path[idx+1:]
	}
	return path
}

var _ fileservice.FileService = (*lazyCacheFS)(nil)
