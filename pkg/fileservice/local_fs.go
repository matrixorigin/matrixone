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
	"errors"
	"io"
	"io/fs"
	"os"
	pathpkg "path"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/fileservice/memorycache"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace/statistic"
	"go.uber.org/zap"
)

// LocalFS is a FileService implementation backed by local file system
type LocalFS struct {
	name     string
	rootPath string

	sync.RWMutex
	dirFiles map[string]*os.File

	allocator   CacheDataAllocator
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

	// get absolute path
	if rootPath != "" {
		var err error
		rootPath, err = filepath.Abs(rootPath)
		if err != nil {
			return nil, err
		}

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

	if fs.memCache != nil {
		fs.allocator = fs.memCache
	} else {
		fs.allocator = DefaultCacheDataAllocator
	}

	return fs, nil
}

func (l *LocalFS) initCaches(ctx context.Context, config CacheConfig) error {
	config.setDefaults()

	if config.RemoteCacheEnabled {
		if config.QueryClient == nil {
			return moerr.NewInternalError(ctx, "query client is nil")
		}
		l.remoteCache = NewRemoteCache(config.QueryClient, config.KeyRouterFactory)
		logutil.Info("fileservice: remote cache initialized",
			zap.Any("fs-name", l.name),
		)
	}

	if *config.MemoryCapacity > DisableCacheCapacity { // 1 means disable
		l.memCache = NewMemCache(
			NewMemoryCache(int64(*config.MemoryCapacity), true, &config.CacheCallbacks),
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
				int(*config.DiskCapacity),
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
	if err := ctx.Err(); err != nil {
		return err
	}

	v2.FSWriteLocalCounter.Add(float64(len(vector.Entries)))

	var err error
	var bytesWritten int
	start := time.Now()
	ctx, span := trace.Start(ctx, "LocalFS.Write", trace.WithKind(trace.SpanKindLocalFSVis))
	defer func() {
		// cover another func to catch the err when process Write
		span.End(trace.WithFSReadWriteExtra(vector.FilePath, err, int64(bytesWritten)))
		v2.LocalWriteIODurationHistogram.Observe(time.Since(start).Seconds())
		v2.LocalWriteIOBytesHistogram.Observe(float64(bytesWritten))
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

	bytesWritten, err = l.write(ctx, vector)
	return err
}

func (l *LocalFS) write(ctx context.Context, vector IOVector) (bytesWritten int, err error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}

	t0 := time.Now()
	defer func() {
		FSProfileHandler.AddSample(time.Since(t0))
	}()

	path, err := ParsePathAtService(vector.FilePath, l.name)
	if err != nil {
		return 0, err
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
		l.rootPath,
		".tmp.*",
	)
	if err != nil {
		return 0, err
	}
	fileWithChecksum, put := NewFileWithChecksumOSFile(ctx, f, _BlockContentSize, l.perfCounterSets)
	defer put.Put()

	r := newIOEntriesReader(ctx, vector.Entries)

	var buf []byte
	putBuf := ioBufferPool.Get(&buf)
	defer putBuf.Put()
	n, err := io.CopyBuffer(fileWithChecksum, r, buf)
	if err != nil {
		return 0, err
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
			return 0, moerr.NewSizeNotMatchNoCtx(path.File)
		}
	}
	bytesWritten = int(n)
	if err := f.Sync(); err != nil {
		return 0, err
	}
	if err := f.Close(); err != nil {
		return 0, err
	}

	// ensure parent dir
	parentDir, _ := filepath.Split(nativePath)
	err = l.ensureDir(parentDir)
	if err != nil {
		return 0, err
	}

	// move
	if err := os.Rename(f.Name(), nativePath); err != nil {
		return 0, err
	}

	if err := l.syncDir(parentDir); err != nil {
		return 0, err
	}

	return
}

func (l *LocalFS) Read(ctx context.Context, vector *IOVector) (err error) {
	if err := ctx.Err(); err != nil {
		return err
	}

	bytesCounter := new(atomic.Int64)
	start := time.Now()
	defer func() {
		LocalReadIODuration := time.Since(start)

		v2.LocalReadIODurationHistogram.Observe(LocalReadIODuration.Seconds())
		v2.LocalReadIOBytesHistogram.Observe(float64(bytesCounter.Load()))
	}()

	if len(vector.Entries) == 0 {
		return moerr.NewEmptyVectorNoCtx()
	}
	startLock := time.Now()
	unlock, wait := l.ioLocks.Lock(IOLockKey{
		File: vector.FilePath,
	})
	if unlock != nil {
		defer unlock()
	} else {
		wait()
	}

	stats := statistic.StatsInfoFromContext(ctx)
	stats.AddLockTimeConsumption(time.Since(startLock))

	allocator := l.allocator
	if vector.Policy.Any(SkipMemoryCache) {
		allocator = DefaultCacheDataAllocator
	}
	for i := range vector.Entries {
		vector.Entries[i].allocator = allocator
	}

	for _, cache := range vector.Caches {
		cache := cache
		if err := readCache(ctx, cache, vector); err != nil {
			return err
		}
		defer func() {
			if err != nil {
				return
			}
			err = cache.Update(ctx, vector, false)
		}()
	}

	if l.memCache != nil {
		if err := readCache(ctx, l.memCache, vector); err != nil {
			return err
		}
		defer func() {
			if err != nil {
				return
			}
			err = l.memCache.Update(ctx, vector, l.asyncUpdate)
		}()
	}

	ioStart := time.Now()
	defer func() {
		stats.AddIOAccessTimeConsumption(time.Since(ioStart))
	}()

	if l.diskCache != nil {
		if err := readCache(ctx, l.diskCache, vector); err != nil {
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
		if err := readCache(ctx, l.remoteCache, vector); err != nil {
			return err
		}
	}

	err = l.read(ctx, vector, bytesCounter)
	if err != nil {
		return err
	}

	return nil
}

func (l *LocalFS) ReadCache(ctx context.Context, vector *IOVector) (err error) {
	if err := ctx.Err(); err != nil {
		return err
	}

	ctx, span := trace.Start(ctx, "LocalFS.ReadCache")
	defer span.End()

	if len(vector.Entries) == 0 {
		return moerr.NewEmptyVectorNoCtx()
	}

	startLock := time.Now()
	unlock, wait := l.ioLocks.Lock(IOLockKey{
		File: vector.FilePath,
	})

	if unlock != nil {
		defer unlock()
	} else {
		wait()
	}
	statistic.StatsInfoFromContext(ctx).AddLockTimeConsumption(time.Since(startLock))

	for _, cache := range vector.Caches {
		cache := cache
		if err := readCache(ctx, cache, vector); err != nil {
			return err
		}
		defer func() {
			if err != nil {
				return
			}
			err = cache.Update(ctx, vector, false)
		}()
	}

	if l.memCache != nil {
		if err := readCache(ctx, l.memCache, vector); err != nil {
			return err
		}
	}

	return nil
}

func (l *LocalFS) read(ctx context.Context, vector *IOVector, bytesCounter *atomic.Int64) (err error) {
	if vector.allDone() {
		// all cache hit
		return nil
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
			r = &countingReader{
				R: r,
				C: bytesCounter,
			}

			if entry.ToCacheData != nil {
				r = io.TeeReader(r, entry.WriterForRead)
				counter := new(atomic.Int64)
				cr := &countingReader{
					R: r,
					C: counter,
				}
				var bs memorycache.CacheData
				bs, err = entry.ToCacheData(cr, nil, DefaultCacheDataAllocator)
				if err != nil {
					return err
				}
				vector.Entries[i].CachedData = bs
				if entry.Size > 0 && counter.Load() != entry.Size {
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
			r = &countingReader{
				R: r,
				C: bytesCounter,
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
						var bs memorycache.CacheData
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
			r = &countingReader{
				R: r,
				C: bytesCounter,
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
	if err := ctx.Err(); err != nil {
		return nil, err
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
	if err := ctx.Err(); err != nil {
		return nil, err
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

func (l *LocalFS) PrefetchFile(ctx context.Context, filePath string) error {
	return nil
}

func (l *LocalFS) Delete(ctx context.Context, filePaths ...string) error {
	if err := ctx.Err(); err != nil {
		return err
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

	return errors.Join(
		func() error {
			if l.memCache == nil {
				return nil
			}
			return l.memCache.DeletePaths(ctx, filePaths)
		}(),
		func() error {
			if l.diskCache == nil {
				return nil
			}
			return l.diskCache.DeletePaths(ctx, filePaths)
		}(),
		func() error {
			if l.remoteCache == nil {
				return nil
			}
			return l.remoteCache.DeletePaths(ctx, filePaths)
		}(),
	)
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
		if os.IsExist(err) {
			// existed
			return nil
		}
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
	if err := ctx.Err(); err != nil {
		return err
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
	_, err := l.write(ctx, vector)
	if err != nil {
		return err
	}
	return nil
}

var _ CachingFileService = new(LocalFS)

func (l *LocalFS) Close() {
	l.FlushCache()
}

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
			if os.IsNotExist(err) {
				// invalid sym link
				return false, nil
			}
			return false, err
		}
		return entryIsDir(path, name, stat)
	}
	return false, nil
}
