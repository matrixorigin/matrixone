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
	"io"
	"math"
	"net/http/httptrace"
	pathpkg "path"
	"sort"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"go.uber.org/zap"
)

// S3FS is a FileService implementation backed by S3
type S3FS struct {
	name      string
	storage   ObjectStorage
	keyPrefix string

	memCache              *MemCache
	diskCache             *DiskCache
	remoteCache           *RemoteCache
	asyncUpdate           bool
	writeDiskCacheOnWrite bool

	perfCounterSets []*perfcounter.CounterSet

	ioLocks IOLocks
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
) (*S3FS, error) {

	fs := &S3FS{
		name:            args.Name,
		keyPrefix:       args.KeyPrefix,
		asyncUpdate:     true,
		perfCounterSets: perfCounterSets,
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

// NewS3FSOnMinio creates S3FS on minio server
// this is needed because the URL scheme of minio server does not compatible with AWS'
func NewS3FSOnMinio(
	ctx context.Context,
	args ObjectStorageArguments,
	cacheConfig CacheConfig,
	perfCounterSets []*perfcounter.CounterSet,
	noCache bool,
) (*S3FS, error) {
	args.IsMinio = true
	return NewS3FS(ctx, args, cacheConfig, perfCounterSets, noCache)
}

func (s *S3FS) initCaches(ctx context.Context, config CacheConfig) error {
	config.setDefaults()

	// Init the remote cache first, because the callback needs to be set for mem and disk cache.
	if config.RemoteCacheEnabled {
		if config.CacheClient == nil {
			return moerr.NewInternalError(ctx, "cache client is nil")
		}
		s.remoteCache = NewRemoteCache(config.CacheClient, config.KeyRouterFactory)
		logutil.Info("fileservice: remote cache initialized",
			zap.Any("fs-name", s.name),
		)
	}

	// memory cache
	if *config.MemoryCapacity > DisableCacheCapacity {
		s.memCache = NewMemCache(
			NewLRUCache(int64(*config.MemoryCapacity), true, &config.CacheCallbacks),
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
			int64(*config.DiskCapacity),
			config.DiskMinEvictInterval.Duration,
			*config.DiskEvictTarget,
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

func (s *S3FS) Write(ctx context.Context, vector IOVector) error {
	ctx = addGetConnMetric(ctx)

	v2.GetS3FSWriteCounter().Inc()
	v2.GetS3FSWriteSizeGauge().Set(float64(vector.EntriesSize()))

	start := time.Now()
	defer v2.GetS3WriteDurationHistogram().Observe(time.Since(start).Seconds())

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
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

	err = s.write(ctx, vector)
	return err
}

func (s *S3FS) write(ctx context.Context, vector IOVector) (err error) {
	ctx, span := trace.Start(ctx, "S3FS.write")
	defer span.End()

	path, err := ParsePathAtService(vector.FilePath, s.name)
	if err != nil {
		return err
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
	if s.writeDiskCacheOnWrite && s.diskCache != nil {
		// also write to disk cache
		w, done, closeW, err := s.diskCache.newFileContentWriter(vector.FilePath)
		if err != nil {
			return err
		}
		defer closeW()
		defer func() {
			if err != nil {
				return
			}
			err = done(ctx)
		}()
		r := io.TeeReader(
			newIOEntriesReader(ctx, vector.Entries),
			w,
		)
		content, err = io.ReadAll(r)
		if err != nil {
			return err
		}

	} else if len(vector.Entries) == 1 &&
		vector.Entries[0].Size > 0 &&
		int(vector.Entries[0].Size) == len(vector.Entries[0].Data) {
		// one piece of data
		content = vector.Entries[0].Data

	} else {
		r := newIOEntriesReader(ctx, vector.Entries)
		content, err = io.ReadAll(r)
		if err != nil {
			return err
		}
	}

	if vector.Hash.Sum != nil && vector.Hash.New != nil {
		h := vector.Hash.New()
		if _, err := h.Write(content); err != nil {
			return err
		}
		*vector.Hash.Sum = h.Sum(nil)
	}

	r := bytes.NewReader(content)
	var expire *time.Time
	if !vector.ExpireAt.IsZero() {
		expire = &vector.ExpireAt
	}
	key := s.pathToKey(path.File)
	if err := s.storage.Write(ctx, key, r, size, expire); err != nil {
		return err
	}

	return nil
}

func (s *S3FS) Read(ctx context.Context, vector *IOVector) (err error) {
	ctx = addGetConnMetric(ctx)

	v2.GetS3FSReadCounter().Inc()
	defer v2.GetS3FSReadSizeGauge().Set(float64(vector.EntriesSize()))

	start := time.Now()
	defer v2.GetS3ReadDurationHistogram().Observe(time.Since(start).Seconds())

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if len(vector.Entries) == 0 {
		return moerr.NewEmptyVectorNoCtx()
	}

	unlock, wait := s.ioLocks.Lock(IOLockKey{
		File: vector.FilePath,
	})
	if unlock != nil {
		defer unlock()
	} else {
		wait()
	}

	if s.memCache != nil {
		if err := s.memCache.Read(ctx, vector); err != nil {
			return err
		}
		defer func() {
			if err != nil {
				return
			}
			err = s.memCache.Update(ctx, vector, s.asyncUpdate)
		}()
	}

	if s.diskCache != nil {
		if err := s.diskCache.Read(ctx, vector); err != nil {
			return err
		}
		defer func() {
			if err != nil {
				return
			}
			err = s.diskCache.Update(ctx, vector, s.asyncUpdate)
		}()
	}

	if s.remoteCache != nil {
		if err := s.remoteCache.Read(ctx, vector); err != nil {
			return err
		}
	}

	return s.read(ctx, vector)
}

func (s *S3FS) ReadCache(ctx context.Context, vector *IOVector) (err error) {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if len(vector.Entries) == 0 {
		return moerr.NewEmptyVectorNoCtx()
	}

	unlock, wait := s.ioLocks.Lock(IOLockKey{
		File: vector.FilePath,
	})
	if unlock != nil {
		defer unlock()
	} else {
		wait()
	}

	if s.memCache != nil {
		if err := s.memCache.Read(ctx, vector); err != nil {
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

	// calculate object read range
	min := ptrTo(int64(math.MaxInt))
	max := ptrTo(int64(0))
	readToEnd := false
	for _, entry := range vector.Entries {
		entry := entry
		if entry.done {
			continue
		}
		if entry.Offset < *min {
			min = &entry.Offset
		}
		if entry.Size < 0 {
			entry.Size = 0
			readToEnd = true
		}
		if end := entry.Offset + entry.Size; end > *max {
			max = &end
		}
	}
	if readToEnd {
		max = nil
	}

	// a function to get an io.ReadCloser
	getReader := func(ctx context.Context, min *int64, max *int64) (io.ReadCloser, error) {
		ctx, spanR := trace.Start(ctx, "S3FS.read.getReader")
		defer spanR.End()
		key := s.pathToKey(path.File)
		return s.storage.Read(ctx, key, min, max)
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

	for i, entry := range vector.Entries {
		if entry.done {
			continue
		}
		entry := entry

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

		if err = entry.setCachedData(); err != nil {
			return err
		}

		vector.Entries[i] = entry
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

	return s.storage.Delete(ctx, keys...)
}

var _ ETLFileService = new(S3FS)

func (*S3FS) ETLCompatible() {}

var _ CachingFileService = new(S3FS)

func (s *S3FS) FlushCache() {
	if s.memCache != nil {
		s.memCache.Flush()
	}
}

func (s *S3FS) SetAsyncUpdate(b bool) {
	s.asyncUpdate = b
}

func addGetConnMetric(ctx context.Context) context.Context {
	var start time.Time
	var dnsStart time.Time
	var connectStart time.Time
	var tlsHandshakeStart time.Time
	return httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{
		GetConn: func(hostPort string) {
			start = time.Now()
		},

		GotConn: func(info httptrace.GotConnInfo) {
			v2.S3GetConnDurationHistogram.Observe(time.Since(start).Seconds())
		},

		DNSStart: func(di httptrace.DNSStartInfo) {
			dnsStart = time.Now()
		},

		DNSDone: func(di httptrace.DNSDoneInfo) {
			v2.S3DNSDurationHistogram.Observe(time.Since(dnsStart).Seconds())
		},

		ConnectStart: func(network, addr string) {
			connectStart = time.Now()
		},

		ConnectDone: func(network, addr string, err error) {
			v2.S3ConnectDurationHistogram.Observe(time.Since(connectStart).Seconds())
		},

		TLSHandshakeStart: func() {
			tlsHandshakeStart = time.Now()
		},

		TLSHandshakeDone: func(cs tls.ConnectionState, err error) {
			v2.S3TLSHandshakeDurationHistogram.Observe(time.Since(tlsHandshakeStart).Seconds())
		},
	})
}
