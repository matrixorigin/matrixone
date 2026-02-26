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
	"iter"
	pathpkg "path"
	"runtime"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/malloc"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	metric "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace/statistic"
	"go.uber.org/zap"
)

// S3FS is a FileService implementation backed by S3
type S3FS struct {
	name      string
	storage   ObjectStorage
	keyPrefix string

	memCache    *MemCache
	diskCache   *DiskCache
	remoteCache *RemoteCache
	asyncUpdate bool

	perfCounterSets []*perfcounter.CounterSet

	ioMerger *IOMerger

	parallelMode ParallelMode
}

// key mapping scheme:
// <KeyPrefix>/<file path> -> file content

var _ FileService = new(S3FS)

func NewS3FS(
	ctx context.Context,
	args ObjectStorageArguments,
	cacheConfig CacheConfig,
	perfCounterSets []*perfcounter.CounterSet,
	noCache bool,
	noDefaultCredential bool,
) (*S3FS, error) {

	args.NoDefaultCredentials = noDefaultCredential

	fs := &S3FS{
		name:            args.Name,
		keyPrefix:       args.KeyPrefix,
		asyncUpdate:     true,
		perfCounterSets: perfCounterSets,
		ioMerger:        NewIOMerger(),
		parallelMode:    args.ParallelMode,
	}

	var err error
	switch {

	case args.IsHDFS || strings.HasPrefix(strings.ToLower(args.Endpoint), "hdfs"):
		// HDFS
		fs.storage, err = NewHDFS(ctx, args, perfCounterSets)
		if err != nil {
			return nil, err
		}

	case args.IsMinio ||
		// 天翼云，使用SignatureV2验证，其他SDK不再支持
		strings.Contains(args.Endpoint, "ctyunapi.cn"):
		// MinIO SDK
		fs.storage, err = NewMinioSDK(ctx, args, perfCounterSets)
		if err != nil {
			return nil, err
		}

	case strings.Contains(args.Endpoint, "myqcloud.com"):
		// 腾讯云
		fs.storage, err = NewQCloudSDK(ctx, args, perfCounterSets)
		if err != nil {
			return nil, err
		}

	case strings.Contains(args.Endpoint, "aliyuncs.com"):
		// 阿里云
		fs.storage, err = NewAliyunSDK(ctx, args, perfCounterSets)
		if err != nil {
			return nil, err
		}

	case strings.EqualFold(args.Endpoint, "disk"):
		// disk based
		fs.storage, err = newDiskObjectStorage(ctx, args, perfCounterSets)
		if err != nil {
			return nil, err
		}

	default:
		// AWS SDK
		fs.storage, err = NewAwsSDKv2(ctx, args, perfCounterSets)
		if err != nil {
			return nil, err
		}

	}

	// limit number of concurrent operations
	concurrency := args.Concurrency
	if concurrency == 0 {
		concurrency = 1024
	}
	fs.storage = newObjectStorageSemaphore(
		fs.storage,
		concurrency,
	)

	// metrics
	fs.storage = newObjectStorageMetrics(
		fs.storage,
		"s3",
	)

	// http trace
	fs.storage = newObjectStorageHTTPTrace(fs.storage)

	// cache
	if !noCache {
		if err := fs.initCaches(ctx, cacheConfig); err != nil {
			return nil, err
		}
	}

	return fs, nil
}

func (s *S3FS) AllocateCacheData(ctx context.Context, size int) fscache.Data {
	if s.memCache != nil {
		s.memCache.cache.EnsureNBytes(ctx, size)
	}
	return DefaultCacheDataAllocator().AllocateCacheData(ctx, size)
}

func (s *S3FS) AllocateCacheDataWithHint(ctx context.Context, size int, hints malloc.Hints) fscache.Data {
	if s.memCache != nil {
		s.memCache.cache.EnsureNBytes(ctx, size)
	}
	return DefaultCacheDataAllocator().AllocateCacheDataWithHint(ctx, size, hints)
}

func (s *S3FS) CopyToCacheData(ctx context.Context, data []byte) fscache.Data {
	if s.memCache != nil {
		s.memCache.cache.EnsureNBytes(ctx, len(data))
	}
	return DefaultCacheDataAllocator().CopyToCacheData(ctx, data)
}

func (s *S3FS) initCaches(ctx context.Context, config CacheConfig) error {
	config.setDefaults()

	// Init the remote cache first, because the callback needs to be set for mem and disk cache.
	if config.RemoteCacheEnabled {
		if config.QueryClient == nil {
			return moerr.NewInternalError(ctx, "query client is nil")
		}
		s.remoteCache = NewRemoteCache(config.QueryClient, config.KeyRouterFactory)
		logutil.Info("fileservice: remote cache initialized",
			zap.Any("fs-name", s.name),
		)
	}

	// memory cache
	if config.MemoryCapacity != nil &&
		*config.MemoryCapacity > DisableCacheCapacity {
		s.memCache = NewMemCache(
			fscache.ConstCapacity(int64(*config.MemoryCapacity)),
			&config.CacheCallbacks,
			s.perfCounterSets,
			s.name,
		)
		logutil.Info("fileservice: memory cache initialized",
			zap.Any("fs-name", s.name),
			zap.Any("capacity", config.MemoryCapacity),
		)
	}

	// disk cache
	if config.DiskCapacity != nil &&
		*config.DiskCapacity > DisableCacheCapacity &&
		config.DiskPath != nil {
		var err error
		s.diskCache, err = NewDiskCache(
			ctx,
			*config.DiskPath,
			fscache.ConstCapacity(int64(*config.DiskCapacity)),
			s.perfCounterSets,
			true,
			s,
			s.name,
		)
		if err != nil {
			return err
		}
		logutil.Info("fileservice: disk cache initialized",
			zap.Any("fs-name", s.name),
			zap.Any("config", config),
		)
	}

	return nil
}

func (s *S3FS) Name() string {
	return s.name
}

func (s *S3FS) pathToKey(filePath string) string {
	return pathpkg.Join(s.keyPrefix, filePath)
}

func (s *S3FS) keyToPath(key string) string {
	path := strings.TrimPrefix(key, s.keyPrefix)
	path = strings.TrimLeft(path, "/")
	return path
}

func (s *S3FS) List(ctx context.Context, dirPath string) iter.Seq2[*DirEntry, error] {
	return func(yield func(*DirEntry, error) bool) {
		ctx, span := trace.Start(ctx, "S3FS.List")
		defer span.End()
		start := time.Now()
		defer func() {
			metric.FSReadDurationList.Observe(time.Since(start).Seconds())
		}()

		path, err := ParsePathAtService(dirPath, s.name)
		if err != nil {
			yield(nil, err)
			return
		}
		prefix := s.pathToKey(path.File)
		if prefix != "" {
			prefix += "/"
		}

		for entry, err := range s.storage.List(ctx, prefix) {
			if err != nil {
				yield(nil, err)
				return
			}

			if entry.IsDir {
				filePath := s.keyToPath(entry.Name)
				filePath = strings.TrimRight(filePath, "/")
				_, name := pathpkg.Split(filePath)
				if !yield(&DirEntry{
					Name:  name,
					IsDir: true,
				}, nil) {
					break
				}

			} else {
				filePath := s.keyToPath(entry.Name)
				filePath = strings.TrimRight(filePath, "/")
				_, name := pathpkg.Split(filePath)
				if !yield(&DirEntry{
					Name:  name,
					IsDir: false,
					Size:  entry.Size,
				}, nil) {
					break
				}
			}

		}

	}
}

func (s *S3FS) StatFile(ctx context.Context, filePath string) (*DirEntry, error) {
	ctx, span := trace.Start(ctx, "S3FS.StatFile")
	defer span.End()
	start := time.Now()
	defer func() {
		metric.FSReadDurationStat.Observe(time.Since(start).Seconds())
	}()
	path, err := ParsePathAtService(filePath, s.name)
	if err != nil {
		return nil, err
	}
	key := s.pathToKey(path.File)

	size, err := s.storage.Stat(ctx, key)
	if err != nil {
		return nil, err
	}

	return &DirEntry{
		Name:  pathpkg.Base(filePath),
		IsDir: false,
		Size:  size,
	}, nil
}

func (s *S3FS) PrefetchFile(ctx context.Context, filePath string) error {
	path, err := ParsePathAtService(filePath, s.name)
	if err != nil {
		return err
	}

	startLock := time.Now()
	defer func() {
		statistic.StatsInfoFromContext(ctx).AddS3FSPrefetchFileIOMergerTimeConsumption(time.Since(startLock))
	}()

	done, _ := s.ioMerger.Merge(IOMergeKey{
		Path: filePath,
	}, maxIOWaitDuration)
	if done != nil {
		defer done()
	} else {
		// not wait in prefetch, return
		return nil
	}

	// load to disk cache
	if s.diskCache != nil {
		if err := s.diskCache.SetFile(
			ctx, path.File,
			func(ctx context.Context) (io.ReadCloser, error) {
				return s.newReadCloser(ctx, filePath)
			},
		); err != nil {
			return err
		}
	}
	return nil
}

func (s *S3FS) newReadCloser(ctx context.Context, filePath string) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	key := s.pathToKey(filePath)
	r, err := s.storage.Read(ctx, key, ptrTo[int64](0), (*int64)(nil))
	if err != nil {
		return nil, err
	}

	return r, nil
}

func (s *S3FS) Write(ctx context.Context, vector IOVector) (err error) {
	defer func() {
		if errors.Is(err, io.EOF) {
			panic("found EOF in Write")
		}
	}()

	if err := ctx.Err(); err != nil {
		return err
	}

	var bytesWritten int
	start := time.Now()
	defer func() {
		metric.FSWriteDurationWrite.Observe(time.Since(start).Seconds())
		metric.S3WriteIOBytesHistogram.Observe(float64(bytesWritten))
	}()

	// check existence
	path, err := ParsePathAtService(vector.FilePath, s.name)
	if err != nil {
		return err
	}
	key := s.pathToKey(path.File)
	existsStart := time.Now()
	exists, err := s.storage.Exists(ctx, key)
	metric.FSWriteDurationExists.Observe(time.Since(existsStart).Seconds())
	if err != nil {
		return err
	}
	if exists {
		return moerr.NewFileAlreadyExistsNoCtx(vector.FilePath)
	}

	bytesWritten, err = s.write(ctx, vector)
	return err
}

func (s *S3FS) write(ctx context.Context, vector IOVector) (bytesWritten int, err error) {
	ctx, span := trace.Start(ctx, "S3FS.write")
	defer span.End()

	path, err := ParsePathAtService(vector.FilePath, s.name)
	if err != nil {
		return 0, err
	}

	// sort
	sort.Slice(vector.Entries, func(i, j int) bool {
		return vector.Entries[i].Offset < vector.Entries[j].Offset
	})

	// reader
	var reader io.Reader
	reader = newIOEntriesReader(ctx, vector.Entries)
	var n atomic.Int64
	reader = &countingReader{
		R: reader,
		C: &n,
	}

	// disk cache
	writeDiskCache := s.diskCache != nil && !vector.Policy.Any(SkipDiskCacheWrites)
	var diskCacheBuf *bytes.Buffer
	if writeDiskCache {
		diskCacheBuf = new(bytes.Buffer)
		reader = io.TeeReader(reader, diskCacheBuf)
	}

	// some SDKs can't handle unknown reader type
	size := vector.size()
	if size != nil {
		reader = io.LimitReader(reader, *size)
	}

	var expire *time.Time
	if !vector.ExpireAt.IsZero() {
		expire = &vector.ExpireAt
	}
	key := s.pathToKey(path.File)
	enableParallel := false
	switch s.parallelMode {
	case ParallelForce:
		enableParallel = true
	case ParallelAuto:
		if size == nil || *size >= minMultipartPartSize {
			enableParallel = true
		}
	}

	if pmw, ok := s.storage.(ParallelMultipartWriter); ok && pmw.SupportsParallelMultipart() &&
		enableParallel {
		opt := &ParallelMultipartOption{
			PartSize:    defaultParallelMultipartPartSize,
			Concurrency: runtime.NumCPU(),
			Expire:      expire,
		}
		storageStart := time.Now()
		if err := pmw.WriteMultipartParallel(ctx, key, reader, size, opt); err != nil {
			return 0, err
		}
		metric.FSWriteDurationStorage.Observe(time.Since(storageStart).Seconds())
	} else {
		storageStart := time.Now()
		if err := s.storage.Write(ctx, key, reader, size, expire); err != nil {
			return 0, err
		}
		metric.FSWriteDurationStorage.Observe(time.Since(storageStart).Seconds())
	}

	// write to disk cache
	if writeDiskCache {
		content := diskCacheBuf.Bytes()
		diskCacheStart := time.Now()
		if err := s.diskCache.SetFile(ctx, vector.FilePath, func(context.Context) (io.ReadCloser, error) {
			return io.NopCloser(bytes.NewReader(content)), nil
		}); err != nil {
			return 0, err
		}
		metric.FSWriteDurationDiskCacheSet.Observe(time.Since(diskCacheStart).Seconds())
	}

	return int(n.Load()), nil
}

func (s *S3FS) Read(ctx context.Context, vector *IOVector) (err error) {
	// Record S3 IO and netwokIO(un memory IO) time Consumption
	stats := statistic.StatsInfoFromContext(ctx)
	ioStart := time.Now()
	defer func() {
		stats.AddIOAccessTimeConsumption(time.Since(ioStart))
		metric.FSReadDurationTotal.Observe(time.Since(ioStart).Seconds())
	}()

	if err := ctx.Err(); err != nil {
		return err
	}

	ctx = WithEventLogger(ctx)
	LogEvent(ctx, str_s3fs_read, vector)
	defer func() {
		LogEvent(ctx, str_read_return)
		LogSlowEvent(ctx, time.Millisecond*500)
	}()

	if len(vector.Entries) == 0 {
		return moerr.NewEmptyVectorNoCtx()
	}

	for _, cache := range vector.Caches {

		t0 := time.Now()
		LogEvent(ctx, str_read_vector_Caches_begin)
		err := readCache(ctx, cache, vector)
		LogEvent(ctx, str_read_vector_Caches_end)
		metric.FSReadDurationReadVectorCache.Observe(time.Since(t0).Seconds())
		if err != nil {
			return err
		}
		if vector.allDone() {
			return nil
		}

		defer func() {
			if err != nil {
				return
			}
			t0 := time.Now()
			LogEvent(ctx, str_update_vector_Caches_begin)
			err = cache.Update(ctx, vector, false)
			LogEvent(ctx, str_update_vector_Caches_end)
			metric.FSReadDurationUpdateVectorCache.Observe(time.Since(t0).Seconds())
		}()
	}

read_memory_cache:
	if s.memCache != nil {

		t0 := time.Now()
		LogEvent(ctx, str_read_memory_cache_Caches_begin)
		err := readCache(ctx, s.memCache, vector)
		LogEvent(ctx, str_read_memory_cache_Caches_end)
		metric.FSReadDurationReadMemoryCache.Observe(time.Since(t0).Seconds())
		if err != nil {
			return err
		}
		if vector.allDone() {
			return nil
		}

		defer func() {
			if err != nil {
				return
			}
			t0 := time.Now()
			LogEvent(ctx, str_update_memory_cache_begin)
			err = s.memCache.Update(ctx, vector, s.asyncUpdate)
			LogEvent(ctx, str_update_memory_cache_end)
			metric.FSReadDurationUpdateMemoryCache.Observe(time.Since(t0).Seconds())
		}()
	}

read_disk_cache:
	if s.diskCache != nil {

		t0 := time.Now()
		LogEvent(ctx, str_read_disk_cache_Caches_begin)
		// Record which entries are not done before reading from disk cache
		undoneBefore := make(map[int]bool)
		for i, entry := range vector.Entries {
			undoneBefore[i] = !entry.done
		}
		err := readCache(ctx, s.diskCache, vector)
		LogEvent(ctx, str_read_disk_cache_Caches_end)
		metric.FSReadDurationReadDiskCache.Observe(time.Since(t0).Seconds())
		if err != nil {
			return err
		}
		// Count bytes actually read from disk cache (entries that became done and from disk cache)
		var actualDiskReadBytes int64
		for i, entry := range vector.Entries {
			if undoneBefore[i] && entry.done && entry.fromCache == s.diskCache {
				actualDiskReadBytes += entry.Size
			}
		}
		// Record disk read size
		if actualDiskReadBytes > 0 {
			perfcounter.Update(ctx, func(counter *perfcounter.CounterSet) {
				counter.FileService.DiskReadSize.Add(actualDiskReadBytes)
			})
		}
		if vector.allDone() {
			return nil
		}

		// try to cache IOEntry if not caching the full file
		if vector.Policy.CacheIOEntry() {
			defer func() {
				if err != nil {
					return
				}
				t0 := time.Now()
				LogEvent(ctx, str_update_disk_cache_Caches_begin)
				err = s.diskCache.Update(ctx, vector, s.asyncUpdate)
				LogEvent(ctx, str_update_disk_cache_Caches_end)
				metric.FSReadDurationUpdateDiskCache.Observe(time.Since(t0).Seconds())
			}()
		}

	}

	if s.remoteCache != nil {
		t0 := time.Now()
		LogEvent(ctx, str_read_remote_cache_Caches_begin)
		err := readCache(ctx, s.remoteCache, vector)
		LogEvent(ctx, str_read_remote_cache_Caches_end)
		metric.FSReadDurationReadRemoteCache.Observe(time.Since(t0).Seconds())
		if err != nil {
			return err
		}
		if vector.allDone() {
			return nil
		}
	}

	mayReadMemoryCache := vector.Policy&SkipMemoryCacheReads == 0
	mayReadDiskCache := vector.Policy&SkipDiskCacheReads == 0
	if mayReadMemoryCache || mayReadDiskCache {
		// may read caches, merge
		LogEvent(ctx, str_ioMerger_Merge_begin)
		startLock := time.Now()
		done, wait := s.ioMerger.Merge(vector.ioMergeKey(), maxIOWaitDuration)
		if done != nil {
			defer done()
			stats.AddS3FSReadIOMergerTimeConsumption(time.Since(startLock))
			metric.FSReadDurationIOMerger.Observe(time.Since(startLock).Seconds())
			LogEvent(ctx, str_ioMerger_Merge_initiate)
			LogEvent(ctx, str_ioMerger_Merge_end)
		} else {
			LogEvent(ctx, str_ioMerger_Merge_wait)
			wait()
			stats.AddS3FSReadIOMergerTimeConsumption(time.Since(startLock))
			metric.FSReadDurationIOMerger.Observe(time.Since(startLock).Seconds())
			LogEvent(ctx, str_ioMerger_Merge_end)
			if mayReadMemoryCache {
				goto read_memory_cache
			} else {
				goto read_disk_cache
			}
		}
	}

	// Count bytes that will be read from S3 (entries that are not done yet)
	var s3ReadBytes int64
	for _, entry := range vector.Entries {
		if !entry.done {
			s3ReadBytes += entry.Size
		}
	}
	s3ReadStart := time.Now()
	if err := s.read(ctx, vector); err != nil {
		return err
	}
	metric.FSReadDurationS3Read.Observe(time.Since(s3ReadStart).Seconds())
	// Record S3 read size (all bytes read from S3)
	if s3ReadBytes > 0 {
		perfcounter.Update(ctx, func(counter *perfcounter.CounterSet) {
			counter.FileService.S3ReadSize.Add(s3ReadBytes)
		})
	}

	return nil
}

func (s *S3FS) ReadCache(ctx context.Context, vector *IOVector) (err error) {
	if err := ctx.Err(); err != nil {
		return err
	}

	if len(vector.Entries) == 0 {
		return moerr.NewEmptyVectorNoCtx()
	}

	for _, cache := range vector.Caches {
		if err := readCache(ctx, cache, vector); err != nil {
			return err
		}
		if vector.allDone() {
			return nil
		}
		defer func() {
			if err != nil {
				return
			}
			err = cache.Update(ctx, vector, false)
		}()
	}

	if s.memCache != nil {
		if err := readCache(ctx, s.memCache, vector); err != nil {
			return err
		}
		if vector.allDone() {
			return nil
		}
	}

	return nil
}

func (s *S3FS) read(ctx context.Context, vector *IOVector) (err error) {
	if vector.allDone() {
		// all cache hit
		return nil
	}

	path, err := ParsePathAtService(vector.FilePath, s.name)
	if err != nil {
		return err
	}

	min, max, readFullObject := vector.readRange()

	// a function to get an io.ReadCloser
	getReader := func(ctx context.Context, min *int64, max *int64) (io.ReadCloser, error) {
		LogEvent(ctx, str_get_reader_begin)
		t0 := time.Now()
		defer func() {
			LogEvent(ctx, str_get_reader_end)
			metric.FSReadDurationGetReader.Observe(time.Since(t0).Seconds())
		}()
		bytesCounter := new(atomic.Int64)
		ctx, spanR := trace.Start(ctx, "S3FS.read.getReader")
		defer spanR.End()
		key := s.pathToKey(path.File)
		r, err := s.storage.Read(ctx, key, min, max)
		if err != nil {
			return nil, err
		}
		return &readCloser{
			r: &countingReader{
				R: r,
				C: bytesCounter,
			},
			closeFunc: func() error {
				LogEvent(ctx, str_reader_close)
				metric.S3ReadIOBytesHistogram.Observe(float64(bytesCounter.Load()))
				return r.Close()
			},
		}, nil
	}

	// a function to get data lazily
	var contentBytes []byte
	var contentErr error
	var getContentDone bool
	getContent := func(ctx context.Context) (bs []byte, err error) {
		t0 := time.Now()
		LogEvent(ctx, str_get_content_begin)
		defer func() {
			LogEvent(ctx, str_get_content_end, len(bs))
			metric.FSReadDurationGetContent.Observe(time.Since(t0).Seconds())
		}()
		ctx, spanC := trace.Start(ctx, "S3FS.read.getContent")
		defer spanC.End()
		if getContentDone {
			return contentBytes, contentErr
		}
		defer func() {
			contentBytes = bs
			contentErr = err
			getContentDone = true
		}()

		reader, err := getReader(ctx, min, max)
		if err != nil {
			return nil, err
		}
		defer reader.Close()
		tStart := time.Now()
		LogEvent(ctx, str_io_readall_begin)
		bs, err = io.ReadAll(reader)
		LogEvent(ctx, str_io_readall_end, len(bs))
		metric.FSReadDurationIOReadAll.Observe(time.Since(tStart).Seconds())
		if err != nil {
			return nil, err
		}

		return
	}

	numNotDoneEntries := 0
	defer func() {
		metric.FSReadS3Counter.Add(float64(numNotDoneEntries))
	}()
	for i, entry := range vector.Entries {
		if entry.done {
			continue
		}
		numNotDoneEntries++

		start := entry.Offset - *min

		if entry.Size == 0 {
			return moerr.NewEmptyRangeNoCtx(path.File)
		}

		// a function to get entry data lazily
		getData := func(ctx context.Context) ([]byte, error) {
			LogEvent(ctx, str_get_data_begin)
			t0 := time.Now()
			defer func() {
				LogEvent(ctx, str_get_data_end)
				metric.FSReadDurationGetEntryData.Observe(time.Since(t0).Seconds())
			}()
			ctx, spanD := trace.Start(ctx, "S3FS.reader.getData")
			defer spanD.End()
			if entry.Size < 0 {
				// read to end
				content, err := getContent(ctx)
				if err != nil {
					return nil, err
				}
				if start >= int64(len(content)) {
					return nil, moerr.NewEmptyRangeNoCtx(path.File)
				}
				return content[start:], nil
			}
			content, err := getContent(ctx)
			if err != nil {
				return nil, err
			}
			end := start + entry.Size
			if end > int64(len(content)) {
				return nil, moerr.NewUnexpectedEOFNoCtx(path.File)
			}
			if start == end {
				return nil, moerr.NewEmptyRangeNoCtx(path.File)
			}
			return content[start:end], nil
		}

		setData := true
		var data []byte

		if w := vector.Entries[i].WriterForRead; w != nil {
			setData = false
			if getContentDone {
				// data is ready
				data, err = getData(ctx)
				if err != nil {
					return err
				}
				t0 := time.Now()
				LogEvent(ctx, str_write_writerforread_begin)
				_, err = w.Write(data)
				LogEvent(ctx, str_write_writerforread_end)
				metric.FSReadDurationWriteToWriter.Observe(time.Since(t0).Seconds())
				if err != nil {
					return err
				}

			} else {
				// get a reader and copy
				min := &entry.Offset
				var max *int64
				if entry.Size > 0 {
					max = ptrTo(entry.Offset + entry.Size)
				}
				reader, err := getReader(ctx, min, max)
				if err != nil {
					return err
				}
				defer reader.Close()
				var buf []byte
				put := ioBufferPool.Get(&buf)
				defer put.Put()
				t0 := time.Now()
				LogEvent(ctx, str_io_copybuffer_begin)
				_, err = io.CopyBuffer(w, reader, buf)
				LogEvent(ctx, str_io_copybuffer_end)
				metric.FSReadDurationWriteToWriter.Observe(time.Since(t0).Seconds())
				if err != nil {
					return err
				}
			}
		}

		if ptr := vector.Entries[i].ReadCloserForRead; ptr != nil {
			setData = false
			if getContentDone {
				// data is ready
				data, err = getData(ctx)
				if err != nil {
					return err
				}
				*ptr = io.NopCloser(bytes.NewReader(data))

			} else {
				// get a new reader
				min := &entry.Offset
				var max *int64
				if entry.Size > 0 {
					max = ptrTo(entry.Offset + entry.Size)
				}
				reader, err := getReader(ctx, min, max)
				if err != nil {
					return err
				}
				ret := &readCloser{
					r:         reader,
					closeFunc: reader.Close,
				}
				// to avoid potential leaks
				runtime.SetFinalizer(ret, func(_ *readCloser) {
					_ = reader.Close() // ignore return
				})
				*ptr = ret
			}
		}

		// set Data field
		if setData {
			data, err = getData(ctx)
			if err != nil {
				return err
			}
			if int64(len(entry.Data)) < entry.Size || entry.Size < 0 {
				entry.Data = data
				if entry.Size < 0 {
					entry.Size = int64(len(data))
				}
			} else {
				copy(entry.Data, data)
			}
		}

		if err = entry.setCachedData(ctx, s); err != nil {
			return err
		}

		vector.Entries[i] = entry
	}

	// write to disk cache
	if readFullObject &&
		contentErr == nil &&
		len(contentBytes) > 0 &&
		s.diskCache != nil &&
		!vector.Policy.Any(SkipDiskCacheWrites) {
		t0 := time.Now()
		LogEvent(ctx, str_disk_cache_setfile_begin)
		err := s.diskCache.SetFile(ctx, vector.FilePath, func(context.Context) (io.ReadCloser, error) {
			return io.NopCloser(bytes.NewReader(contentBytes)), nil
		})
		LogEvent(ctx, str_disk_cache_setfile_end)
		metric.FSReadDurationSetCachedData.Observe(time.Since(t0).Seconds())
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *S3FS) Delete(ctx context.Context, filePaths ...string) error {
	ctx, span := trace.Start(ctx, "S3FS.Delete")
	defer span.End()

	keys := make([]string, 0, len(filePaths))
	for _, filePath := range filePaths {
		path, err := ParsePathAtService(filePath, s.name)
		if err != nil {
			return err
		}
		keys = append(keys, s.pathToKey(path.File))
	}

	return errors.Join(
		s.storage.Delete(ctx, keys...),
		func() error {
			if s.memCache == nil {
				return nil
			}
			return s.memCache.DeletePaths(ctx, filePaths)
		}(),
		func() error {
			if s.diskCache == nil {
				return nil
			}
			return s.diskCache.DeletePaths(ctx, filePaths)
		}(),
		func() error {
			if s.remoteCache == nil {
				return nil
			}
			return s.remoteCache.DeletePaths(ctx, filePaths)
		}(),
	)
}

var _ ETLFileService = new(S3FS)

func (*S3FS) ETLCompatible() {}

var _ CachingFileService = new(S3FS)

func (s *S3FS) Close(ctx context.Context) {
	if s.memCache != nil {
		s.memCache.Close(ctx)
	}
	if s.diskCache != nil {
		s.diskCache.Close(ctx)
	}
}

func (s *S3FS) FlushCache(ctx context.Context) {
	if s.memCache != nil {
		s.memCache.Flush(ctx)
	}
}

func (s *S3FS) SetAsyncUpdate(b bool) {
	s.asyncUpdate = b
}

func (s *S3FS) Cost() *CostAttr {
	return &CostAttr{
		List: CostHigh,
	}
}
