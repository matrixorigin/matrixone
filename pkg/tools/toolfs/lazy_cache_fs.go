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
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
)

const defaultLazyCacheMaxBytes int64 = 8 << 30

type lazyCacheEntry struct {
	path       string
	size       int64
	lastAccess uint64
}

type lazyCacheFS struct {
	name   string
	remote fileservice.FileService
	local  *fileservice.LocalFS
	root   string

	mu            sync.Mutex
	caching       map[string]*sync.Once
	entries       map[string]lazyCacheEntry
	reservations  map[string]int64
	usedBytes     int64
	maxBytes      int64
	accessCounter uint64
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
		name:         remote.Name(),
		remote:       remote,
		local:        local,
		root:         root,
		caching:      make(map[string]*sync.Once),
		entries:      make(map[string]lazyCacheEntry),
		reservations: make(map[string]int64),
		maxBytes:     lazyCacheMaxBytesFromEnv(),
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
	l.removeCacheEntryLocked(normalized)
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
	l.mu.Lock()
	for _, filePath := range filePaths {
		normalized := stripServiceName(filePath, l.name)
		delete(l.caching, normalized)
		l.removeCacheEntryLocked(normalized)
	}
	l.mu.Unlock()
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
	if stat, err := os.Stat(localPath); err == nil {
		l.touchCacheEntry(normalized, localPath, stat.Size())
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
	committed := false
	defer func() {
		if !committed {
			l.releaseCacheReservation(normalized)
		}
	}()
	if stat, err := l.remote.StatFile(ctx, normalized); err == nil && stat != nil && stat.Size > 0 {
		l.reserveCacheSpace(stat.Size, normalized)
	}
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
	stat, err := os.Stat(tmpName)
	if err != nil {
		return err
	}
	l.reserveCacheSpace(stat.Size(), normalized)
	if err := os.Rename(tmpName, localPath); err != nil {
		return err
	}
	l.touchCacheEntry(normalized, localPath, stat.Size())
	committed = true
	return nil
}

func (l *lazyCacheFS) readCachedFile(ctx context.Context, vector *fileservice.IOVector) error {
	normalized := stripServiceName(vector.FilePath, l.name)
	localPath := filepath.Join(l.root, filepath.FromSlash(normalized))

	file, err := os.Open(localPath)
	if os.IsNotExist(err) {
		if err := l.ensureCached(ctx, normalized); err != nil {
			return err
		}
		file, err = os.Open(localPath)
		if os.IsNotExist(err) {
			return moerr.NewFileNotFoundNoCtx(normalized)
		}
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
	l.touchCacheEntry(normalized, localPath, fileSize)

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

func (l *lazyCacheFS) touchCacheEntry(normalized string, localPath string, size int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.accessCounter++
	if reservation, ok := l.reservations[normalized]; ok {
		l.usedBytes -= reservation
		delete(l.reservations, normalized)
	}
	if old, ok := l.entries[normalized]; ok {
		l.usedBytes -= old.size
	}
	l.entries[normalized] = lazyCacheEntry{
		path:       localPath,
		size:       size,
		lastAccess: l.accessCounter,
	}
	l.usedBytes += size
}

func (l *lazyCacheFS) reserveCacheSpace(needed int64, protected string) {
	if l.maxBytes <= 0 || needed <= 0 {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	reserved := l.reservations[protected]
	if needed <= reserved {
		return
	}
	delta := needed - reserved
	l.evictCacheLocked(delta, protected)
	l.reservations[protected] = needed
	l.usedBytes += delta
}

func (l *lazyCacheFS) evictCacheLocked(needed int64, protected string) {
	if l.usedBytes+needed <= l.maxBytes {
		return
	}
	entries := make([]lazyCacheEntry, 0, len(l.entries))
	for key, entry := range l.entries {
		if key == protected {
			continue
		}
		entries = append(entries, entry)
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].lastAccess < entries[j].lastAccess
	})
	for _, entry := range entries {
		if l.usedBytes+needed <= l.maxBytes {
			return
		}
		_ = os.Remove(entry.path)
		for key, cached := range l.entries {
			if cached.path == entry.path {
				delete(l.entries, key)
				delete(l.caching, key)
				l.usedBytes -= cached.size
				break
			}
		}
	}
}

func (l *lazyCacheFS) removeCacheEntryLocked(normalized string) {
	if reservation, ok := l.reservations[normalized]; ok {
		l.usedBytes -= reservation
		delete(l.reservations, normalized)
	}
	if entry, ok := l.entries[normalized]; ok {
		l.usedBytes -= entry.size
		delete(l.entries, normalized)
	}
}

func (l *lazyCacheFS) releaseCacheReservation(normalized string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if reservation, ok := l.reservations[normalized]; ok {
		l.usedBytes -= reservation
		delete(l.reservations, normalized)
	}
}

func lazyCacheMaxBytesFromEnv() int64 {
	value := strings.TrimSpace(os.Getenv("MO_TOOL_REMOTE_CACHE_SIZE"))
	if value == "" {
		return defaultLazyCacheMaxBytes
	}
	if n, ok := parseLazyCacheSize(value); ok && n > 0 {
		return n
	}
	return defaultLazyCacheMaxBytes
}

func parseLazyCacheSize(value string) (int64, bool) {
	value = strings.TrimSpace(strings.ToLower(value))
	if value == "" {
		return 0, false
	}
	multiplier := int64(1)
	for _, suffix := range []struct {
		s string
		m int64
	}{
		{s: "gib", m: 1 << 30},
		{s: "gb", m: 1 << 30},
		{s: "g", m: 1 << 30},
		{s: "mib", m: 1 << 20},
		{s: "mb", m: 1 << 20},
		{s: "m", m: 1 << 20},
		{s: "kib", m: 1 << 10},
		{s: "kb", m: 1 << 10},
		{s: "k", m: 1 << 10},
	} {
		if strings.HasSuffix(value, suffix.s) {
			multiplier = suffix.m
			value = strings.TrimSpace(strings.TrimSuffix(value, suffix.s))
			break
		}
	}
	n, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0, false
	}
	return n * multiplier, true
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
