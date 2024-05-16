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
	"crypto/tls"
	"errors"
	"io"
	"net/http/httptrace"
	pathpkg "path"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
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
	}

	var err error
	switch {

	case strings.Contains(args.Endpoint, "ctyunapi.cn"):
		fs.storage, err = NewMinioSDK(ctx, args, perfCounterSets)
		if err != nil {
			return nil, err
		}

	case strings.Contains(args.Endpoint, "aliyuncs.com"):
		fs.storage, err = NewAliyunSDK(ctx, args, perfCounterSets)
		if err != nil {
			return nil, err
		}

	default:
		fs.storage, err = NewAwsSDKv2(ctx, args, perfCounterSets)
		if err != nil {
			return nil, err
		}

	}

	if !noCache {
		if err := fs.initCaches(ctx, cacheConfig); err != nil {
			return nil, err
		}
	}

	return fs, nil
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
	if *config.MemoryCapacity > DisableCacheCapacity {
		s.memCache = NewMemCache(
			NewMemoryCache(int64(*config.MemoryCapacity), true, &config.CacheCallbacks),
			s.perfCounterSets,
		)
		logutil.Info("fileservice: memory cache initialized",
			zap.Any("fs-name", s.name),
			zap.Any("capacity", config.MemoryCapacity),
		)
	}

	// disk cache
	if *config.DiskCapacity > DisableCacheCapacity && config.DiskPath != nil {
		var err error
		s.diskCache, err = NewDiskCache(
			ctx,
			*config.DiskPath,
			int(*config.DiskCapacity),
			s.perfCounterSets,
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

func (s *S3FS) List(ctx context.Context, dirPath string) (entries []DirEntry, err error) {
	ctx, span := trace.Start(ctx, "S3FS.List")
	defer span.End()
	start := time.Now()
	defer func() {
		metric.S3ListIODurationHistogram.Observe(time.Since(start).Seconds())
	}()

	path, err := ParsePathAtService(dirPath, s.name)
	if err != nil {
		return nil, err
	}
	prefix := s.pathToKey(path.File)
	if prefix != "" {
		prefix += "/"
	}

	if err := s.storage.List(ctx, prefix, func(isPrefix bool, key string, size int64) (bool, error) {

		if isPrefix {
			filePath := s.keyToPath(key)
			filePath = strings.TrimRight(filePath, "/")
			_, name := pathpkg.Split(filePath)
			entries = append(entries, DirEntry{
				Name:  name,
				IsDir: true,
			})

		} else {
			filePath := s.keyToPath(key)
			filePath = strings.TrimRight(filePath, "/")
			_, name := pathpkg.Split(filePath)
			entries = append(entries, DirEntry{
				Name:  name,
				IsDir: false,
				Size:  size,
			})
		}

		return true, nil
	}); err != nil {
		return nil, err
	}

	return
}

func (s *S3FS) StatFile(ctx context.Context, filePath string) (*DirEntry, error) {
	ctx, span := trace.Start(ctx, "S3FS.StatFile")
	defer span.End()
	start := time.Now()
	defer func() {
		metric.S3StatIODurationHistogram.Observe(time.Since(start).Seconds())
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
	done, wait := s.ioMerger.Merge(IOMergeKey{
		Path: filePath,
	})
	if done != nil {
		defer done()
	} else {
		wait()
	}
	statistic.StatsInfoFromContext(ctx).AddS3FSPrefetchFileIOMergerTimeConsumption(time.Since(startLock))

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

func (s *S3FS) Write(ctx context.Context, vector IOVector) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	metric.FSWriteS3Counter.Add(float64(len(vector.Entries)))

	tp := reuse.Alloc[tracePoint](nil)
	defer reuse.Free(tp, nil)
	ctx = httptrace.WithClientTrace(ctx, tp.getClientTrace())

	var bytesWritten int
	start := time.Now()
	defer func() {
		metric.S3WriteIODurationHistogram.Observe(time.Since(start).Seconds())
		metric.S3WriteIOBytesHistogram.Observe(float64(bytesWritten))
	}()

	// check existence
	path, err := ParsePathAtService(vector.FilePath, s.name)
	if err != nil {
		return err
	}
	key := s.pathToKey(path.File)
	exists, err := s.storage.Exists(ctx, key)
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

	// size
	var size int64
	if len(vector.Entries) > 0 {
		last := vector.Entries[len(vector.Entries)-1]
		size = int64(last.Offset + last.Size)
	}

	// content
	var content []byte
	if len(vector.Entries) == 1 &&
		vector.Entries[0].Size > 0 &&
		int(vector.Entries[0].Size) == len(vector.Entries[0].Data) {
		// one piece of data
		content = vector.Entries[0].Data

	} else {
		r := newIOEntriesReader(ctx, vector.Entries)
		content, err = io.ReadAll(r)
		if err != nil {
			return 0, err
		}
	}

	r := bytes.NewReader(content)
	var expire *time.Time
	if !vector.ExpireAt.IsZero() {
		expire = &vector.ExpireAt
	}
	key := s.pathToKey(path.File)
	if err := s.storage.Write(ctx, key, r, size, expire); err != nil {
		return 0, err
	}

	// write to disk cache
	if s.diskCache != nil && !vector.Policy.Any(SkipDiskCacheWrites) {
		if err := s.diskCache.SetFile(ctx, vector.FilePath, func(context.Context) (io.ReadCloser, error) {
			return io.NopCloser(bytes.NewReader(content)), nil
		}); err != nil {
			return 0, err
		}
	}

	return len(content), nil
}

func (s *S3FS) Read(ctx context.Context, vector *IOVector) (err error) {
	if err := ctx.Err(); err != nil {
		return err
	}

	tp := reuse.Alloc[tracePoint](nil)
	defer reuse.Free(tp, nil)
	ctx = httptrace.WithClientTrace(ctx, tp.getClientTrace())

	if len(vector.Entries) == 0 {
		return moerr.NewEmptyVectorNoCtx()
	}

	startLock := time.Now()
	done, wait := s.ioMerger.Merge(vector.ioMergeKey())
	if done != nil {
		defer done()
	} else {
		wait()
	}
	stats := statistic.StatsInfoFromContext(ctx)
	stats.AddS3FSReadIOMergerTimeConsumption(time.Since(startLock))

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

	if s.memCache != nil {
		if err := readCache(ctx, s.memCache, vector); err != nil {
			return err
		}
		defer func() {
			if err != nil {
				return
			}
			err = s.memCache.Update(ctx, vector, s.asyncUpdate)
		}()
	}

	ioStart := time.Now()
	defer func() {
		stats.AddIOAccessTimeConsumption(time.Since(ioStart))
	}()

	if s.diskCache != nil {
		if err := readCache(ctx, s.diskCache, vector); err != nil {
			return err
		}
		// try to cache IOEntry if not caching the full file
		if vector.Policy.CacheIOEntry() {
			defer func() {
				if err != nil {
					return
				}
				err = s.diskCache.Update(ctx, vector, s.asyncUpdate)
			}()
		}
	}

	if s.remoteCache != nil {
		if err := readCache(ctx, s.remoteCache, vector); err != nil {
			return err
		}
	}

	return s.read(ctx, vector)
}

func (s *S3FS) ReadCache(ctx context.Context, vector *IOVector) (err error) {
	if err := ctx.Err(); err != nil {
		return err
	}

	if len(vector.Entries) == 0 {
		return moerr.NewEmptyVectorNoCtx()
	}

	startLock := time.Now()
	done, wait := s.ioMerger.Merge(vector.ioMergeKey())
	if done != nil {
		defer done()
	} else {
		wait()
	}
	statistic.StatsInfoFromContext(ctx).AddS3FSReadCacheIOMergerTimeConsumption(time.Since(startLock))

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

	if s.memCache != nil {
		if err := readCache(ctx, s.memCache, vector); err != nil {
			return err
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
		t0 := time.Now()
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
				s3ReadIODuration := time.Since(t0)

				metric.S3ReadIODurationHistogram.Observe(s3ReadIODuration.Seconds())
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
		bs, err = io.ReadAll(reader)
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
		entry := entry
		numNotDoneEntries++

		start := entry.Offset - *min

		if entry.Size == 0 {
			return moerr.NewEmptyRangeNoCtx(path.File)
		}

		// a function to get entry data lazily
		getData := func(ctx context.Context) ([]byte, error) {
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
				_, err = w.Write(data)
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
				_, err = io.CopyBuffer(w, reader, buf)
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
				*ptr = &readCloser{
					r:         reader,
					closeFunc: reader.Close,
				}
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

		if err = setCachedData(&entry, s.memCache); err != nil {
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
		if err := s.diskCache.SetFile(ctx, vector.FilePath, func(context.Context) (io.ReadCloser, error) {
			return io.NopCloser(bytes.NewReader(contentBytes)), nil
		}); err != nil {
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

func (s *S3FS) Close() {
	s.FlushCache()
}

func (s *S3FS) FlushCache() {
	if s.memCache != nil {
		s.memCache.Flush()
	}
}

func (s *S3FS) SetAsyncUpdate(b bool) {
	s.asyncUpdate = b
}

type tracePoint struct {
	start             time.Time
	dnsStart          time.Time
	connectStart      time.Time
	tlsHandshakeStart time.Time
	ct                *httptrace.ClientTrace
}

func newTracePoint() *tracePoint {
	tp := &tracePoint{
		ct: &httptrace.ClientTrace{},
	}
	tp.ct.GetConn = tp.getConnPoint
	tp.ct.GotConn = tp.gotConnPoint
	tp.ct.DNSStart = tp.dnsStartPoint
	tp.ct.DNSDone = tp.dnsDonePoint
	tp.ct.ConnectStart = tp.connectStartPoint
	tp.ct.ConnectDone = tp.connectDonePoint
	tp.ct.TLSHandshakeStart = tp.tlsHandshakeStartPoint
	tp.ct.TLSHandshakeDone = tp.tlsHandshakeDonePoint
	return tp
}

func (tp tracePoint) TypeName() string {
	return "fileservice.tracePoint"
}

func resetTracePoint(tp *tracePoint) {
	tp.start = time.Time{}
	tp.dnsStart = time.Time{}
	tp.connectStart = time.Time{}
	tp.tlsHandshakeStart = time.Time{}
}

func (tp *tracePoint) getClientTrace() *httptrace.ClientTrace {
	return tp.ct
}

func (tp *tracePoint) getConnPoint(hostPort string) {
	tp.start = time.Now()
}

func (tp *tracePoint) gotConnPoint(info httptrace.GotConnInfo) {
	metric.S3GetConnDurationHistogram.Observe(time.Since(tp.start).Seconds())
}

func (tp *tracePoint) dnsStartPoint(di httptrace.DNSStartInfo) {
	metric.S3DNSResolveCounter.Inc()
	tp.dnsStart = time.Now()
}

func (tp *tracePoint) dnsDonePoint(di httptrace.DNSDoneInfo) {
	metric.S3DNSResolveDurationHistogram.Observe(time.Since(tp.dnsStart).Seconds())
}

func (tp *tracePoint) connectStartPoint(network, addr string) {
	metric.S3ConnectCounter.Inc()
	tp.connectStart = time.Now()
}

func (tp *tracePoint) connectDonePoint(network, addr string, err error) {
	metric.S3ConnectDurationHistogram.Observe(time.Since(tp.connectStart).Seconds())
}

func (tp *tracePoint) tlsHandshakeStartPoint() {
	tp.tlsHandshakeStart = time.Now()
}

func (tp *tracePoint) tlsHandshakeDonePoint(cs tls.ConnectionState, err error) {
	metric.S3TLSHandshakeDurationHistogram.Observe(time.Since(tp.tlsHandshakeStart).Seconds())
}
