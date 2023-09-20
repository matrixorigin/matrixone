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
	"io/fs"
	"os"
	pathpkg "path"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"go.uber.org/zap"
)

// LocalFS is a FileService implementation backed by local file system
type LocalFS struct {
	name     string
	rootPath string

	sync.RWMutex
	dirFiles map[string]*os.File

	memCache    *MemCache
	diskCache   *DiskCache
	remoteCache *RemoteCache
	asyncUpdate bool

	perfCounterSets []*perfcounter.CounterSet

	ioLocks IOLocks
}

var _ FileService = new(LocalFS)

func NewLocalFS(
	ctx context.Context,
	name string,
	rootPath string,
	cacheConfig CacheConfig,
	perfCounterSets []*perfcounter.CounterSet,
) (*LocalFS, error) {

	// ensure dir
	f, err := os.Open(rootPath)
	if os.IsNotExist(err) {
		// not exists, create
		err := os.MkdirAll(rootPath, 0755)
		if err != nil {
			return nil, err
		}

	} else if err != nil {
		// stat error
		return nil, err

	} else {
		defer f.Close()
	}

	// create tmp dir
	if err := os.MkdirAll(filepath.Join(rootPath, ".tmp"), 0755); err != nil {
		return nil, err
	}

	fs := &LocalFS{
		name:            name,
		rootPath:        rootPath,
		dirFiles:        make(map[string]*os.File),
		asyncUpdate:     true,
		perfCounterSets: perfCounterSets,
	}

	if err := fs.initCaches(ctx, cacheConfig); err != nil {
		return nil, err
	}

	return fs, nil
}

func (l *LocalFS) initCaches(ctx context.Context, config CacheConfig) error {
	config.setDefaults()

	if config.RemoteCacheEnabled {
		if config.CacheClient == nil {
			return moerr.NewInternalError(ctx, "cache client is nil")
		}
		l.remoteCache = NewRemoteCache(config.CacheClient, config.KeyRouterFactory)
		logutil.Info("fileservice: remote cache initialized",
			zap.Any("fs-name", l.name),
		)
	}

	if *config.MemoryCapacity > DisableCacheCapacity { // 1 means disable
		l.memCache = NewMemCache(
			NewLRUCache(int64(*config.MemoryCapacity), true, &config.CacheCallbacks),
			l.perfCounterSets,
		)
		logutil.Info("fileservice: memory cache initialized",
			zap.Any("fs-name", l.name),
			zap.Any("config", config),
		)
	}

	if config.enableDiskCacheForLocalFS {
		if *config.DiskCapacity > DisableCacheCapacity && config.DiskPath != nil {
			var err error
			l.diskCache, err = NewDiskCache(
				ctx,
				*config.DiskPath,
				int64(*config.DiskCapacity),
				config.DiskMinEvictInterval.Duration,
				*config.DiskEvictTarget,
				l.perfCounterSets,
			)
			if err != nil {
				return err
			}
			logutil.Info("fileservice: disk cache initialized",
				zap.Any("fs-name", l.name),
				zap.Any("config", config),
			)
		}
	}

	return nil
}

func (l *LocalFS) Name() string {
	return l.name
}

func (l *LocalFS) Write(ctx context.Context, vector IOVector) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	var err error
	ctx, span := trace.Start(ctx, "LocalFS.Write", trace.WithKind(trace.SpanKindLocalFSVis))
	defer func() {
		// cover another func to catch the err when process Write
		span.End(trace.WithFSReadWriteExtra(vector.FilePath, err, vector.EntriesSize()))
	}()

	path, err := ParsePathAtService(vector.FilePath, l.name)
	if err != nil {
		return err
	}
	nativePath := l.toNativeFilePath(path.File)

	// check existence
	_, err = os.Stat(nativePath)
	if err == nil {
		// existed
		err = moerr.NewFileAlreadyExistsNoCtx(path.File)
		return err
	}

	err = l.write(ctx, vector)
	return err
}

func (l *LocalFS) write(ctx context.Context, vector IOVector) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	t0 := time.Now()
	defer func() {
		FSProfileHandler.AddSample(time.Since(t0))
	}()

	path, err := ParsePathAtService(vector.FilePath, l.name)
	if err != nil {
		return err
	}
	nativePath := l.toNativeFilePath(path.File)

	// sort
	sort.Slice(vector.Entries, func(i, j int) bool {
		return vector.Entries[i].Offset < vector.Entries[j].Offset
	})

	// size
	var size int64
	if len(vector.Entries) > 0 {
		last := vector.Entries[len(vector.Entries)-1]
		size = int64(last.Offset + last.Size)
	}

	// write
	f, err := os.CreateTemp(
		filepath.Join(l.rootPath, ".tmp"),
		"*.tmp",
	)
	if err != nil {
		return err
	}
	fileWithChecksum, put := NewFileWithChecksumOSFile(ctx, f, _BlockContentSize, l.perfCounterSets)
	defer put.Put()

	var r io.Reader
	r = newIOEntriesReader(ctx, vector.Entries)
	if vector.Hash.Sum != nil && vector.Hash.New != nil {
		h := vector.Hash.New()
		r = io.TeeReader(r, h)
		defer func() {
			*vector.Hash.Sum = h.Sum(nil)
		}()
	}

	var buf []byte
	putBuf := ioBufferPool.Get(&buf)
	defer putBuf.Put()
	n, err := io.CopyBuffer(fileWithChecksum, r, buf)
	if err != nil {
		return err
	}
	if n != size {
		sizeUnknown := false
		for _, entry := range vector.Entries {
			if entry.Size < 0 {
				sizeUnknown = true
				break
			}
		}
		if !sizeUnknown {
			return moerr.NewSizeNotMatchNoCtx(path.File)
		}
	}
	if err := f.Sync(); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}

	// ensure parent dir
	parentDir, _ := filepath.Split(nativePath)
	err = l.ensureDir(parentDir)
	if err != nil {
		return err
	}

	// move
	if err := os.Rename(f.Name(), nativePath); err != nil {
		return err
	}

	if err := l.syncDir(parentDir); err != nil {
		return err
	}

	return nil
}

func (l *LocalFS) Read(ctx context.Context, vector *IOVector) (err error) {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if len(vector.Entries) == 0 {
		return moerr.NewEmptyVectorNoCtx()
	}

	unlock, wait := l.ioLocks.Lock(IOLockKey{
		File: vector.FilePath,
	})
	if unlock != nil {
		defer unlock()
	} else {
		wait()
	}

	if l.memCache != nil {
		if err := l.memCache.Read(ctx, vector); err != nil {
			return err
		}
		defer func() {
			if err != nil {
				return
			}
			err = l.memCache.Update(ctx, vector, l.asyncUpdate)
		}()
	}

	if l.diskCache != nil {
		if err := l.diskCache.Read(ctx, vector); err != nil {
			return err
		}
		defer func() {
			if err != nil {
				return
			}
			err = l.diskCache.Update(ctx, vector, l.asyncUpdate)
		}()
	}

	if l.remoteCache != nil {
		if err := l.remoteCache.Read(ctx, vector); err != nil {
			return err
		}
	}

	return l.read(ctx, vector)
}

func (l *LocalFS) ReadCache(ctx context.Context, vector *IOVector) (err error) {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	ctx, span := trace.Start(ctx, "LocalFS.ReadCache")
	defer span.End()

	if len(vector.Entries) == 0 {
		return moerr.NewEmptyVectorNoCtx()
	}

	unlock, wait := l.ioLocks.Lock(IOLockKey{
		File: vector.FilePath,
	})
	if unlock != nil {
		defer unlock()
	} else {
		wait()
	}

	if l.memCache != nil {
		if err := l.memCache.Read(ctx, vector); err != nil {
			return err
		}
	}

	return nil
}

func (l *LocalFS) read(ctx context.Context, vector *IOVector) (err error) {
	if vector.allDone() {
		// all cache hit
		return nil
	}

	// collect read info only when cache missing
	size := vector.EntriesSize()
	ctx, span := trace.Start(ctx, "LocalFS.read", trace.WithKind(trace.SpanKindLocalFSVis))
	defer func() {
		span.End(trace.WithFSReadWriteExtra(vector.FilePath, err, size))
	}()

	t0 := time.Now()
	defer func() {
		FSProfileHandler.AddSample(time.Since(t0))
	}()

	path, err := ParsePathAtService(vector.FilePath, l.name)
	if err != nil {
		return err
	}
	nativePath := l.toNativeFilePath(path.File)

	file, err := os.Open(nativePath)
	if os.IsNotExist(err) {
		return moerr.NewFileNotFoundNoCtx(path.File)
	}
	if err != nil {
		return err
	}
	defer file.Close()

	for i, entry := range vector.Entries {
		if entry.Size == 0 {
			return moerr.NewEmptyRangeNoCtx(path.File)
		}

		if entry.done {
			continue
		}

		if entry.WriterForRead != nil {
			fileWithChecksum, put := NewFileWithChecksumOSFile(ctx, file, _BlockContentSize, l.perfCounterSets)
			defer put.Put()

			if entry.Offset > 0 {
				_, err = fileWithChecksum.Seek(int64(entry.Offset), io.SeekStart)
				if err != nil {
					return err
				}
			}
			r := (io.Reader)(fileWithChecksum)
			if entry.Size > 0 {
				r = io.LimitReader(r, int64(entry.Size))
			}

			if entry.ToCacheData != nil {
				r = io.TeeReader(r, entry.WriterForRead)
				cr := &countingReader{
					R: r,
				}
				var bs CacheData
				bs, err = entry.ToCacheData(cr, nil, DefaultCacheDataAllocator)
				if err != nil {
					return err
				}
				vector.Entries[i].CachedData = bs
				if entry.Size > 0 && cr.N != entry.Size {
					return moerr.NewUnexpectedEOFNoCtx(path.File)
				}

			} else {
				var buf []byte
				put := ioBufferPool.Get(&buf)
				defer put.Put()
				n, err := io.CopyBuffer(entry.WriterForRead, r, buf)
				if err != nil {
					return err
				}
				if entry.Size > 0 && n != int64(entry.Size) {
					return moerr.NewUnexpectedEOFNoCtx(path.File)
				}
			}

		} else if entry.ReadCloserForRead != nil {
			file, err = os.Open(nativePath)
			if os.IsNotExist(err) {
				return moerr.NewFileNotFoundNoCtx(path.File)
			}
			if err != nil {
				return err
			}
			fileWithChecksum := NewFileWithChecksum(ctx, file, _BlockContentSize, l.perfCounterSets)

			if entry.Offset > 0 {
				_, err = fileWithChecksum.Seek(int64(entry.Offset), io.SeekStart)
				if err != nil {
					return err
				}
			}
			r := (io.Reader)(fileWithChecksum)
			if entry.Size > 0 {
				r = io.LimitReader(r, int64(entry.Size))
			}

			if entry.ToCacheData == nil {
				*entry.ReadCloserForRead = &readCloser{
					r:         r,
					closeFunc: file.Close,
				}

			} else {
				buf := new(bytes.Buffer)
				*entry.ReadCloserForRead = &readCloser{
					r: io.TeeReader(r, buf),
					closeFunc: func() error {
						defer file.Close()
						var bs CacheData
						bs, err = entry.ToCacheData(buf, buf.Bytes(), DefaultCacheDataAllocator)
						if err != nil {
							return err
						}
						vector.Entries[i].CachedData = bs
						return nil
					},
				}
			}

		} else {
			fileWithChecksum, put := NewFileWithChecksumOSFile(ctx, file, _BlockContentSize, l.perfCounterSets)
			defer put.Put()

			if entry.Offset > 0 {
				_, err = fileWithChecksum.Seek(int64(entry.Offset), io.SeekStart)
				if err != nil {
					return err
				}
			}
			r := (io.Reader)(fileWithChecksum)
			if entry.Size > 0 {
				r = io.LimitReader(r, int64(entry.Size))
			}

			if entry.Size < 0 {
				var data []byte
				data, err = io.ReadAll(r)
				if err != nil {
					return err
				}
				entry.Data = data
				entry.Size = int64(len(data))

			} else {
				if int64(len(entry.Data)) < entry.Size {
					entry.Data = make([]byte, entry.Size)
				}
				var n int
				n, err = io.ReadFull(r, entry.Data)
				if err != nil {
					return err
				}
				if int64(n) != entry.Size {
					return moerr.NewUnexpectedEOFNoCtx(path.File)
				}
			}

			if err = entry.setCachedData(); err != nil {
				return err
			}

			vector.Entries[i] = entry

		}

	}

	return nil

}

func (l *LocalFS) List(ctx context.Context, dirPath string) (ret []DirEntry, err error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	ctx, span := trace.Start(ctx, "LocalFS.List", trace.WithKind(trace.SpanKindLocalFSVis))
	defer func() {
		span.AddExtraFields([]zap.Field{zap.String("list", dirPath)}...)
		span.End()
	}()

	_ = ctx

	t0 := time.Now()
	defer func() {
		FSProfileHandler.AddSample(time.Since(t0))
	}()

	path, err := ParsePathAtService(dirPath, l.name)
	if err != nil {
		return nil, err
	}
	nativePath := l.toNativeFilePath(path.File)

	f, err := os.Open(nativePath)
	if os.IsNotExist(err) {
		err = nil
		return
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()

	entries, err := f.ReadDir(-1)
	for _, entry := range entries {
		name := entry.Name()
		if strings.HasPrefix(name, ".") {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			return nil, err
		}
		fileSize := info.Size()
		nBlock := ceilingDiv(fileSize, _BlockSize)
		contentSize := fileSize - _ChecksumSize*nBlock

		isDir, err := entryIsDir(nativePath, name, info)
		if err != nil {
			return nil, err
		}
		ret = append(ret, DirEntry{
			Name:  name,
			IsDir: isDir,
			Size:  contentSize,
		})
	}

	sort.Slice(ret, func(i, j int) bool {
		return ret[i].Name < ret[j].Name
	})

	if err != nil {
		return ret, err
	}

	return
}

func (l *LocalFS) StatFile(ctx context.Context, filePath string) (*DirEntry, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	ctx, span := trace.Start(ctx, "LocalFS.StatFile", trace.WithKind(trace.SpanKindLocalFSVis))
	defer func() {
		span.AddExtraFields([]zap.Field{zap.String("stat", filePath)}...)
		span.End()
	}()

	t0 := time.Now()
	defer func() {
		FSProfileHandler.AddSample(time.Since(t0))
	}()

	path, err := ParsePathAtService(filePath, l.name)
	if err != nil {
		return nil, err
	}
	nativePath := l.toNativeFilePath(path.File)

	stat, err := os.Stat(nativePath)
	if os.IsNotExist(err) {
		return nil, moerr.NewFileNotFound(ctx, filePath)
	}

	if stat.IsDir() {
		return nil, moerr.NewFileNotFound(ctx, filePath)
	}

	fileSize := stat.Size()
	nBlock := ceilingDiv(fileSize, _BlockSize)
	contentSize := fileSize - _ChecksumSize*nBlock

	return &DirEntry{
		Name:  pathpkg.Base(filePath),
		IsDir: false,
		Size:  contentSize,
	}, nil
}

func (l *LocalFS) Delete(ctx context.Context, filePaths ...string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	ctx, span := trace.Start(ctx, "LocalFS.Delete", trace.WithKind(trace.SpanKindLocalFSVis))
	defer func() {
		span.AddExtraFields([]zap.Field{zap.String("delete", strings.Join(filePaths, "|"))}...)
		span.End()
	}()

	t0 := time.Now()
	defer func() {
		FSProfileHandler.AddSample(time.Since(t0))
	}()

	for _, filePath := range filePaths {
		if err := l.deleteSingle(ctx, filePath); err != nil {
			return err
		}
	}
	return nil
}

func (l *LocalFS) deleteSingle(ctx context.Context, filePath string) error {
	path, err := ParsePathAtService(filePath, l.name)
	if err != nil {
		return err
	}
	nativePath := l.toNativeFilePath(path.File)

	_, err = os.Stat(nativePath)
	if os.IsNotExist(err) {
		return moerr.NewFileNotFoundNoCtx(path.File)
	}
	if err != nil {
		return err
	}

	err = os.Remove(nativePath)
	if err != nil {
		return err
	}

	parentDir, _ := filepath.Split(nativePath)
	err = l.syncDir(parentDir)
	if err != nil {
		return err
	}

	return nil
}

func (l *LocalFS) ensureDir(nativePath string) error {
	nativePath = filepath.Clean(nativePath)
	if nativePath == "" {
		return nil
	}

	// check existence by l.dirFiles
	l.RLock()
	_, ok := l.dirFiles[nativePath]
	if ok {
		// dir existed
		l.RUnlock()
		return nil
	}
	l.RUnlock()

	// check existence by fstat
	_, err := os.Stat(nativePath)
	if err == nil {
		// existed
		return nil
	}

	// ensure parent
	parent, _ := filepath.Split(nativePath)
	if parent != nativePath {
		if err := l.ensureDir(parent); err != nil {
			return err
		}
	}

	// create
	if err := os.Mkdir(nativePath, 0755); err != nil {
		return err
	}

	// sync parent dir
	if err := l.syncDir(parent); err != nil {
		return err
	}

	return nil
}

func (l *LocalFS) syncDir(nativePath string) error {
	l.Lock()
	f, ok := l.dirFiles[nativePath]
	if !ok {
		var err error
		f, err = os.Open(nativePath)
		if err != nil {
			l.Unlock()
			return err
		}
		l.dirFiles[nativePath] = f
	}
	l.Unlock()
	if err := f.Sync(); err != nil {
		return err
	}
	return nil
}

func (l *LocalFS) toNativeFilePath(filePath string) string {
	return filepath.Join(l.rootPath, toOSPath(filePath))
}

var _ MutableFileService = new(LocalFS)

func (l *LocalFS) NewMutator(ctx context.Context, filePath string) (Mutator, error) {
	path, err := ParsePathAtService(filePath, l.name)
	if err != nil {
		return nil, err
	}
	nativePath := l.toNativeFilePath(path.File)
	f, err := os.OpenFile(nativePath, os.O_RDWR, 0644)
	if os.IsNotExist(err) {
		return nil, moerr.NewFileNotFoundNoCtx(path.File)
	}
	return &LocalFSMutator{
		osFile:           f,
		fileWithChecksum: NewFileWithChecksum(ctx, f, _BlockContentSize, l.perfCounterSets),
	}, nil
}

type LocalFSMutator struct {
	osFile           *os.File
	fileWithChecksum *FileWithChecksum[*os.File]
}

func (l *LocalFSMutator) Mutate(ctx context.Context, entries ...IOEntry) error {

	ctx, span := trace.Start(ctx, "LocalFS.Mutate")
	defer span.End()

	return l.mutate(ctx, 0, entries...)
}

func (l *LocalFSMutator) Append(ctx context.Context, entries ...IOEntry) error {
	ctx, span := trace.Start(ctx, "LocalFS.Append")
	defer span.End()

	offset, err := l.fileWithChecksum.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}
	return l.mutate(ctx, offset, entries...)
}

func (l *LocalFSMutator) mutate(ctx context.Context, baseOffset int64, entries ...IOEntry) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	t0 := time.Now()
	defer func() {
		FSProfileHandler.AddSample(time.Since(t0))
	}()

	// write
	for _, entry := range entries {

		if entry.ReaderForWrite != nil {
			// seek and copy
			_, err := l.fileWithChecksum.Seek(entry.Offset+baseOffset, 0)
			if err != nil {
				return err
			}
			var buf []byte
			put := ioBufferPool.Get(&buf)
			defer put.Put()
			n, err := io.CopyBuffer(l.fileWithChecksum, entry.ReaderForWrite, buf)
			if err != nil {
				return err
			}
			if int64(n) != entry.Size {
				return moerr.NewSizeNotMatchNoCtx("")
			}

		} else {
			// WriteAt
			n, err := l.fileWithChecksum.WriteAt(entry.Data, int64(entry.Offset+baseOffset))
			if err != nil {
				return err
			}
			if int64(n) != entry.Size {
				return moerr.NewSizeNotMatchNoCtx("")
			}
		}
	}

	return nil
}

func (l *LocalFSMutator) Close() error {
	// sync
	if err := l.osFile.Sync(); err != nil {
		return err
	}

	// close
	if err := l.osFile.Close(); err != nil {
		return err
	}

	return nil
}

var _ ReplaceableFileService = new(LocalFS)

func (l *LocalFS) Replace(ctx context.Context, vector IOVector) error {
	ctx, span := trace.Start(ctx, "LocalFS.Replace")
	defer span.End()
	return l.write(ctx, vector)
}

var _ CachingFileService = new(LocalFS)

func (l *LocalFS) FlushCache() {
	if l.memCache != nil {
		l.memCache.Flush()
	}
	if l.diskCache != nil {
		l.diskCache.Flush()
	}
}

func (l *LocalFS) SetAsyncUpdate(b bool) {
	l.asyncUpdate = b
}

func entryIsDir(path string, name string, entry fs.FileInfo) (bool, error) {
	if entry.IsDir() {
		return true, nil
	}
	if entry.Mode().Type()&fs.ModeSymlink > 0 {
		stat, err := os.Stat(filepath.Join(path, name))
		if err != nil {
			return false, err
		}
		return entryIsDir(path, name, stat)
	}
	return false, nil
}
