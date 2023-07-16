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
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/fileservice/workerpool"
	"io"
	"math"
	stdhttp "net/http"
	"net/url"
	"os"
	pathpkg "path"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"go.uber.org/zap"
)

// S3FS is a FileService implementation backed by S3
type S3FS struct {
	name      string
	s3Client  *s3.Client
	bucket    string
	keyPrefix string

	memCache              *MemCache
	diskCache             *DiskCache
	asyncUpdate           bool
	writeDiskCacheOnWrite bool

	perfCounterSets []*perfcounter.CounterSet
	listMaxKeys     int32

	fileLoaderWorkerPool *workerpool.WorkerPool
}

// key mapping scheme:
// <KeyPrefix>/<file path> -> file content

var _ FileService = new(S3FS)

func NewS3FS(
	ctx context.Context,
	sharedConfigProfile string,
	name string,
	endpoint string,
	bucket string,
	keyPrefix string,
	cacheConfig CacheConfig,
	perfCounterSets []*perfcounter.CounterSet,
	noCache bool,
) (*S3FS, error) {

	fs, err := newS3FS([]string{
		"shared-config-profile=" + sharedConfigProfile,
		"name=" + name,
		"endpoint=" + endpoint,
		"bucket=" + bucket,
		"prefix=" + keyPrefix,
	})
	if err != nil {
		return nil, err
	}

	fs.perfCounterSets = perfCounterSets

	if !noCache {
		if err := fs.initCaches(ctx, cacheConfig); err != nil {
			return nil, err
		}
	}

	if cacheConfig.DiskAsyncFileLoad {
		fs.fileLoaderWorkerPool = workerpool.NewWorkerPool(16)
		fs.fileLoaderWorkerPool.Start()
	}

	return fs, nil
}

// NewS3FSOnMinio creates S3FS on minio server
// this is needed because the URL scheme of minio server does not compatible with AWS'
func NewS3FSOnMinio(
	ctx context.Context,
	sharedConfigProfile string,
	name string,
	endpoint string,
	bucket string,
	keyPrefix string,
	cacheConfig CacheConfig,
	perfCounterSets []*perfcounter.CounterSet,
	noCache bool,
) (*S3FS, error) {

	fs, err := newS3FS([]string{
		"shared-config-profile=" + sharedConfigProfile,
		"name=" + name,
		"endpoint=" + endpoint,
		"bucket=" + bucket,
		"prefix=" + keyPrefix,
		"is-minio=true",
	})
	if err != nil {
		return nil, err
	}

	fs.perfCounterSets = perfCounterSets

	if !noCache {
		if err := fs.initCaches(ctx, cacheConfig); err != nil {
			return nil, err
		}
	}

	if cacheConfig.DiskAsyncFileLoad {
		fs.fileLoaderWorkerPool = workerpool.NewWorkerPool(16)
		fs.fileLoaderWorkerPool.Start()
	}

	return fs, nil
}

func (s *S3FS) initCaches(ctx context.Context, config CacheConfig) error {
	config.setDefaults()

	// memory cache
	if *config.MemoryCapacity > DisableCacheCapacity {
		s.memCache = NewMemCache(
			WithLRU(int64(*config.MemoryCapacity)),
			WithPerfCounterSets(s.perfCounterSets),
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

func (s *S3FS) List(ctx context.Context, dirPath string) (entries []DirEntry, err error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	ctx, span := trace.Start(ctx, "S3FS.List")
	defer span.End()
	if ctx == nil {
		ctx = context.Background()
	}

	path, err := ParsePathAtService(dirPath, s.name)
	if err != nil {
		return nil, err
	}
	prefix := s.pathToKey(path.File)
	if prefix != "" {
		prefix += "/"
	}
	var marker *string

	for {
		output, err := s.s3ListObjects(
			ctx,
			&s3.ListObjectsInput{
				Bucket:    ptrTo(s.bucket),
				Delimiter: ptrTo("/"),
				Prefix:    ptrTo(prefix),
				Marker:    marker,
				MaxKeys:   s.listMaxKeys,
			},
		)
		if err != nil {
			return nil, err
		}

		for _, obj := range output.Contents {
			filePath := s.keyToPath(*obj.Key)
			filePath = strings.TrimRight(filePath, "/")
			_, name := pathpkg.Split(filePath)
			entries = append(entries, DirEntry{
				Name:  name,
				IsDir: false,
				Size:  obj.Size,
			})
		}

		for _, prefix := range output.CommonPrefixes {
			filePath := s.keyToPath(*prefix.Prefix)
			filePath = strings.TrimRight(filePath, "/")
			_, name := pathpkg.Split(filePath)
			entries = append(entries, DirEntry{
				Name:  name,
				IsDir: true,
			})
		}

		if !output.IsTruncated {
			break
		}
		marker = output.NextMarker
	}

	return
}

func (s *S3FS) StatFile(ctx context.Context, filePath string) (*DirEntry, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	ctx, span := trace.Start(ctx, "S3FS.StatFile")
	defer span.End()
	if ctx == nil {
		ctx = context.Background()
	}

	path, err := ParsePathAtService(filePath, s.name)
	if err != nil {
		return nil, err
	}
	key := s.pathToKey(path.File)

	output, err := s.s3HeadObject(
		ctx,
		&s3.HeadObjectInput{
			Bucket: ptrTo(s.bucket),
			Key:    ptrTo(key),
		},
	)
	if err != nil {
		var httpError *http.ResponseError
		if errors.As(err, &httpError) {
			if httpError.Response.StatusCode == 404 {
				return nil, moerr.NewFileNotFound(ctx, filePath)
			}
		}
		return nil, err
	}

	return &DirEntry{
		Name:  pathpkg.Base(filePath),
		IsDir: false,
		Size:  output.ContentLength,
	}, nil
}

func (s *S3FS) Write(ctx context.Context, vector IOVector) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	ctx, span := trace.Start(ctx, "S3FS.Write")
	defer span.End()

	// check existence
	path, err := ParsePathAtService(vector.FilePath, s.name)
	if err != nil {
		return err
	}
	key := s.pathToKey(path.File)
	output, err := s.s3HeadObject(
		ctx,
		&s3.HeadObjectInput{
			Bucket: ptrTo(s.bucket),
			Key:    ptrTo(key),
		},
	)
	if err != nil {
		var httpError *http.ResponseError
		if errors.As(err, &httpError) {
			if httpError.Response.StatusCode == 404 {
				// key not exists, ok
				err = nil
			}
		}
		if err != nil {
			return err
		}
	}
	if output != nil {
		// key existed
		return moerr.NewFileAlreadyExistsNoCtx(path.File)
	}

	return s.write(ctx, vector)
}

func (s *S3FS) write(ctx context.Context, vector IOVector) (err error) {
	ctx, span := trace.Start(ctx, "S3FS.write")
	defer span.End()
	path, err := ParsePathAtService(vector.FilePath, s.name)
	if err != nil {
		return err
	}
	key := s.pathToKey(path.File)

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

	// put
	var expire *time.Time
	if !vector.ExpireAt.IsZero() {
		expire = &vector.ExpireAt
	}
	_, err = s.s3PutObject(
		ctx,
		&s3.PutObjectInput{
			Bucket:        ptrTo(s.bucket),
			Key:           ptrTo(key),
			Body:          bytes.NewReader(content),
			ContentLength: size,
			Expires:       expire,
		},
	)
	if err != nil {
		return err
	}

	return nil
}

func (s *S3FS) Read(ctx context.Context, vector *IOVector) (err error) {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if len(vector.Entries) == 0 {
		return moerr.NewEmptyVectorNoCtx()
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
			if s.fileLoaderWorkerPool != nil {
				err = s.fileLoaderWorkerPool.Submit(func() {
					err = s.Preload(ctx, vector.FilePath)
				})
				if err != nil {
					return
				}
			} else {
				err = s.diskCache.Update(ctx, vector, s.asyncUpdate)
			}
		}()
	}

	if err := s.read(ctx, vector); err != nil {
		return err
	}

	return nil
}

func (s *S3FS) read(ctx context.Context, vector *IOVector) error {
	if vector.allDone() {
		return nil
	}

	ctx, span := trace.Start(ctx, "S3FS.read")
	defer span.End()
	path, err := ParsePathAtService(vector.FilePath, s.name)
	if err != nil {
		return err
	}
	key := s.pathToKey(path.File)

	// calculate object read range
	min := int64(math.MaxInt)
	max := int64(0)
	readToEnd := false
	for _, entry := range vector.Entries {
		if entry.done {
			continue
		}
		if entry.Offset < min {
			min = entry.Offset
		}
		if entry.Size < 0 {
			entry.Size = 0
			readToEnd = true
		}
		if end := entry.Offset + entry.Size; end > max {
			max = end
		}
	}

	// a function to get an io.ReadCloser
	getReader := func(ctx context.Context, readToEnd bool, min int64, max int64) (io.ReadCloser, error) {
		ctx, spanR := trace.Start(ctx, "S3FS.read.getReader")
		defer spanR.End()

		// try to load from disk cache
		if s.diskCache != nil {
			r, err := s.diskCache.GetFileContent(ctx, vector.FilePath, min)
			if moerr.IsMoErrCode(err, moerr.ErrFileNotFound) ||
				os.IsNotExist(err) {
				err = nil
			}
			if err != nil {
				return nil, err
			}
			if r != nil {
				// cache hit
				if readToEnd {
					return r, nil
				} else {
					return &readCloser{
						r:         io.LimitReader(r, max-min),
						closeFunc: r.Close,
					}, nil
				}
			}
		}

		if readToEnd {
			r, err := s.s3GetObject(
				ctx,
				min,
				-1,
				&s3.GetObjectInput{
					Bucket: ptrTo(s.bucket),
					Key:    ptrTo(key),
				},
			)
			err = s.mapError(err, key)
			if err != nil {
				return nil, err
			}
			return r, nil
		}

		r, err := s.s3GetObject(
			ctx,
			min,
			max,
			&s3.GetObjectInput{
				Bucket: ptrTo(s.bucket),
				Key:    ptrTo(key),
			},
		)
		err = s.mapError(err, key)
		if err != nil {
			return nil, err
		}
		return &readCloser{
			r:         io.LimitReader(r, int64(max-min)),
			closeFunc: r.Close,
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

		reader, err := getReader(ctx, readToEnd, min, max)
		if err != nil {
			return nil, err
		}
		defer reader.Close()
		bs, err = io.ReadAll(reader)
		err = s.mapError(err, key)
		if err != nil {
			return nil, err
		}

		return
	}

	for i, entry := range vector.Entries {
		if entry.done {
			continue
		}

		start := entry.Offset - min

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

		if w := vector.Entries[i].WriterForRead; w != nil {
			setData = false
			if getContentDone {
				// data is ready
				data, err := getData(ctx)
				if err != nil {
					return err
				}
				_, err = w.Write(data)
				if err != nil {
					return err
				}

			} else {
				// get a reader and copy
				reader, err := getReader(ctx, entry.Size < 0, entry.Offset, entry.Offset+entry.Size)
				if err != nil {
					return err
				}
				defer reader.Close()
				var buf []byte
				put := ioBufferPool.Get(&buf)
				defer put.Put()
				_, err = io.CopyBuffer(w, reader, buf)
				err = s.mapError(err, key)
				if err != nil {
					return err
				}
			}
		}

		if ptr := vector.Entries[i].ReadCloserForRead; ptr != nil {
			setData = false
			if getContentDone {
				// data is ready
				data, err := getData(ctx)
				if err != nil {
					return err
				}
				*ptr = io.NopCloser(bytes.NewReader(data))

			} else {
				// get a new reader
				reader, err := getReader(ctx, entry.Size < 0, entry.Offset, entry.Offset+entry.Size)
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
			data, err := getData(ctx)
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

		// set ObjectBytes field
		if err := entry.setObjectBytesFromData(); err != nil {
			return err
		}

		vector.Entries[i] = entry
	}

	return nil
}

func (s *S3FS) Preload(ctx context.Context, filePath string) error {
	if s.diskCache != nil {
		err := s.diskCache.SetFileContent(ctx, filePath, s.read)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *S3FS) Delete(ctx context.Context, filePaths ...string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	ctx, span := trace.Start(ctx, "S3FS.Delete")
	defer span.End()

	if len(filePaths) == 0 {
		return nil
	}
	if len(filePaths) == 1 {
		return s.deleteSingle(ctx, filePaths[0])
	}

	objs := make([]types.ObjectIdentifier, 0, 1000)
	for _, filePath := range filePaths {
		path, err := ParsePathAtService(filePath, s.name)
		if err != nil {
			return err
		}
		objs = append(objs, types.ObjectIdentifier{Key: ptrTo(s.pathToKey(path.File))})
		if len(objs) == 1000 {
			if err := s.deleteMultiObj(ctx, objs); err != nil {
				return err
			}
			objs = objs[:0]
		}
	}
	if err := s.deleteMultiObj(ctx, objs); err != nil {
		return err
	}
	return nil
}

func (s *S3FS) deleteMultiObj(ctx context.Context, objs []types.ObjectIdentifier) error {
	ctx, span := trace.Start(ctx, "S3FS.deleteMultiObj")
	defer span.End()
	output, err := s.s3DeleteObjects(ctx, &s3.DeleteObjectsInput{
		Bucket: ptrTo(s.bucket),
		Delete: &types.Delete{
			Objects: objs,
			// In quiet mode the response includes only keys where the delete action encountered an error.
			Quiet: true,
		},
	})
	// delete api failed
	if err != nil {
		return err
	}
	// delete api success, but with delete file failed.
	message := strings.Builder{}
	if len(output.Errors) > 0 {
		for _, Error := range output.Errors {
			if *Error.Code == (*types.NoSuchKey)(nil).ErrorCode() {
				continue
			}
			message.WriteString(fmt.Sprintf("%s: %s, %s;", *Error.Key, *Error.Code, *Error.Message))
		}
	}
	if message.Len() > 0 {
		return moerr.NewInternalErrorNoCtx("S3 Delete failed: %s", message.String())
	}
	return nil
}

func (s *S3FS) deleteSingle(ctx context.Context, filePath string) error {
	ctx, span := trace.Start(ctx, "S3FS.deleteSingle")
	defer span.End()
	path, err := ParsePathAtService(filePath, s.name)
	if err != nil {
		return err
	}
	_, err = s.s3DeleteObject(
		ctx,
		&s3.DeleteObjectInput{
			Bucket: ptrTo(s.bucket),
			Key:    ptrTo(s.pathToKey(path.File)),
		},
	)
	if err != nil {
		return err
	}

	return nil
}

func (s *S3FS) pathToKey(filePath string) string {
	return pathpkg.Join(s.keyPrefix, filePath)
}

func (s *S3FS) keyToPath(key string) string {
	path := strings.TrimPrefix(key, s.keyPrefix)
	path = strings.TrimLeft(path, "/")
	return path
}

func (s *S3FS) mapError(err error, path string) error {
	if err == nil {
		return nil
	}
	var httpError *http.ResponseError
	if errors.As(err, &httpError) {
		if httpError.Response.StatusCode == 404 {
			return moerr.NewFileNotFoundNoCtx(path)
		}
	}
	return err
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

func newS3FS(arguments []string) (*S3FS, error) {
	if len(arguments) == 0 {
		return nil, moerr.NewInvalidInputNoCtx("invalid S3 arguments")
	}

	// arguments
	var endpoint, region, bucket, apiKey, apiSecret, prefix, roleARN, externalID, name, sharedConfigProfile, isMinio string
	for _, pair := range arguments {
		key, value, ok := strings.Cut(pair, "=")
		if !ok {
			return nil, moerr.NewInvalidInputNoCtx("invalid S3 argument: %s", pair)
		}
		switch key {
		case "endpoint":
			endpoint = value
		case "region":
			region = value
		case "bucket":
			bucket = value
		case "key":
			apiKey = value
		case "secret":
			apiSecret = value
		case "prefix":
			prefix = value
		case "role-arn":
			roleARN = value
		case "external-id":
			externalID = value
		case "name":
			name = value
		case "shared-config-profile":
			sharedConfigProfile = value
		case "is-minio":
			isMinio = value
		default:
			return nil, moerr.NewInvalidInputNoCtx("invalid S3 argument: %s", pair)
		}
	}

	// validate endpoint
	var endpointURL *url.URL
	if endpoint != "" {
		var err error
		endpointURL, err = url.Parse(endpoint)
		if err != nil {
			return nil, err
		}
		if endpointURL.Scheme == "" {
			endpointURL.Scheme = "https"
		}
		endpoint = endpointURL.String()
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	// region
	if region == "" {
		// try to get region from bucket
		// only works for AWS S3
		resp, err := stdhttp.Head("https://" + bucket + ".s3.amazonaws.com")
		if err == nil {
			if value := resp.Header.Get("x-amz-bucket-region"); value != "" {
				region = value
			}
		}
	}

	// options for loading configs
	loadConfigOptions := []func(*config.LoadOptions) error{
		config.WithLogger(logutil.GetS3Logger()),
		config.WithClientLogMode(
			aws.LogSigning |
				aws.LogRetries |
				aws.LogRequest |
				aws.LogResponse |
				aws.LogDeprecatedUsage |
				aws.LogRequestEventMessage |
				aws.LogResponseEventMessage,
		),
	}

	// shared config profile
	if sharedConfigProfile != "" {
		loadConfigOptions = append(loadConfigOptions,
			config.WithSharedConfigProfile(sharedConfigProfile),
		)
	}

	credentialProvider := getCredentialsProvider(
		ctx,
		endpoint,
		region,
		apiKey,
		apiSecret,
		roleARN,
		externalID,
	)

	// load configs
	if credentialProvider != nil {
		loadConfigOptions = append(loadConfigOptions,
			config.WithCredentialsProvider(
				credentialProvider,
			),
		)
	}
	config, err := config.LoadDefaultConfig(ctx, loadConfigOptions...)
	if err != nil {
		return nil, err
	}

	// options for s3 client
	s3Options := []func(*s3.Options){
		func(opts *s3.Options) {
			opts.Retryer = retry.NewStandard(func(o *retry.StandardOptions) {
				o.MaxAttempts = maxRetryAttemps
				o.RateLimiter = noOpRateLimit{}
			})
		},
	}

	// credential provider for s3 client
	if credentialProvider != nil {
		s3Options = append(s3Options,
			func(opt *s3.Options) {
				opt.Credentials = credentialProvider
			},
		)
	}

	// endpoint for s3 client
	if endpoint != "" {
		if isMinio != "" {
			// special handling for MinIO
			s3Options = append(s3Options,
				s3.WithEndpointResolver(
					s3.EndpointResolverFunc(
						func(
							region string,
							_ s3.EndpointResolverOptions,
						) (
							ep aws.Endpoint,
							err error,
						) {
							ep.URL = endpoint
							ep.Source = aws.EndpointSourceCustom
							ep.HostnameImmutable = true
							ep.SigningRegion = region
							return
						},
					),
				),
			)
		} else {
			s3Options = append(s3Options,
				s3.WithEndpointResolver(
					s3.EndpointResolverFromURL(endpoint),
				),
			)
		}
	}

	// region for s3 client
	if region != "" {
		s3Options = append(s3Options,
			func(opt *s3.Options) {
				opt.Region = region
			},
		)
	}

	// new s3 client
	client := s3.NewFromConfig(
		config,
		s3Options...,
	)

	fs := &S3FS{
		name:        name,
		s3Client:    client,
		bucket:      bucket,
		keyPrefix:   prefix,
		asyncUpdate: true,
	}

	// head bucket to validate
	_, err = fs.s3HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: ptrTo(bucket),
	})
	if err != nil {
		return nil, moerr.NewInternalErrorNoCtx("bad s3 config: %v", err)
	}

	return fs, nil
}

const maxRetryAttemps = 128

func (s *S3FS) s3ListObjects(ctx context.Context, params *s3.ListObjectsInput, optFns ...func(*s3.Options)) (*s3.ListObjectsOutput, error) {
	t0 := time.Now()
	defer func() {
		FSProfileHandler.AddSample(time.Since(t0))
	}()
	perfcounter.Update(ctx, func(counter *perfcounter.CounterSet) {
		counter.FileService.S3.List.Add(1)
	}, s.perfCounterSets...)
	return doWithRetry(
		"s3 list objects",
		func() (*s3.ListObjectsOutput, error) {
			return s.s3Client.ListObjects(ctx, params, optFns...)
		},
		maxRetryAttemps,
		isRetryableError,
	)
}

func (s *S3FS) s3HeadBucket(ctx context.Context, params *s3.HeadBucketInput, optFns ...func(*s3.Options)) (*s3.HeadBucketOutput, error) {
	return doWithRetry(
		"s3 head bucket",
		func() (*s3.HeadBucketOutput, error) {
			return s.s3Client.HeadBucket(ctx, params, optFns...)
		},
		maxRetryAttemps,
		isRetryableError,
	)
}

func (s *S3FS) s3HeadObject(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	t0 := time.Now()
	defer func() {
		FSProfileHandler.AddSample(time.Since(t0))
	}()
	perfcounter.Update(ctx, func(counter *perfcounter.CounterSet) {
		counter.FileService.S3.Head.Add(1)
	}, s.perfCounterSets...)
	return doWithRetry(
		"s3 head object",
		func() (*s3.HeadObjectOutput, error) {
			return s.s3Client.HeadObject(ctx, params, optFns...)
		},
		maxRetryAttemps,
		isRetryableError,
	)
}

func (s *S3FS) s3PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	t0 := time.Now()
	defer func() {
		FSProfileHandler.AddSample(time.Since(t0))
	}()
	perfcounter.Update(ctx, func(counter *perfcounter.CounterSet) {
		counter.FileService.S3.Put.Add(1)
	}, s.perfCounterSets...)
	// not retryable because Reader may be half consumed
	return s.s3Client.PutObject(ctx, params, optFns...)
}

func (s *S3FS) s3GetObject(ctx context.Context, min int64, max int64, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (io.ReadCloser, error) {
	t0 := time.Now()
	defer func() {
		FSProfileHandler.AddSample(time.Since(t0))
	}()
	perfcounter.Update(ctx, func(counter *perfcounter.CounterSet) {
		counter.FileService.S3.Get.Add(1)
	}, s.perfCounterSets...)
	r, err := newRetryableReader(
		func(offset int64) (io.ReadCloser, error) {
			var rang string
			if max >= 0 {
				rang = fmt.Sprintf("bytes=%d-%d", offset, max)
			} else {
				rang = fmt.Sprintf("bytes=%d-", offset)
			}
			params.Range = &rang
			output, err := doWithRetry(
				"s3 get object",
				func() (*s3.GetObjectOutput, error) {
					return s.s3Client.GetObject(ctx, params, optFns...)
				},
				maxRetryAttemps,
				isRetryableError,
			)
			if err != nil {
				return nil, err
			}
			return output.Body, nil
		},
		min,
		isRetryableError,
	)
	if err != nil {
		return nil, err
	}
	return r, nil
}

func (s *S3FS) s3DeleteObjects(ctx context.Context, params *s3.DeleteObjectsInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectsOutput, error) {
	t0 := time.Now()
	defer func() {
		FSProfileHandler.AddSample(time.Since(t0))
	}()
	perfcounter.Update(ctx, func(counter *perfcounter.CounterSet) {
		counter.FileService.S3.DeleteMulti.Add(1)
	}, s.perfCounterSets...)
	return doWithRetry(
		"s3 delete objects",
		func() (*s3.DeleteObjectsOutput, error) {
			return s.s3Client.DeleteObjects(ctx, params, optFns...)
		},
		maxRetryAttemps,
		isRetryableError,
	)
}

func (s *S3FS) s3DeleteObject(ctx context.Context, params *s3.DeleteObjectInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	t0 := time.Now()
	defer func() {
		FSProfileHandler.AddSample(time.Since(t0))
	}()
	perfcounter.Update(ctx, func(counter *perfcounter.CounterSet) {
		counter.FileService.S3.Delete.Add(1)
	}, s.perfCounterSets...)
	return doWithRetry(
		"s3 delete object",
		func() (*s3.DeleteObjectOutput, error) {
			return s.s3Client.DeleteObject(ctx, params, optFns...)
		},
		maxRetryAttemps,
		isRetryableError,
	)
}

// from https://github.com/aws/aws-sdk-go-v2/issues/543
type noOpRateLimit struct{}

func (noOpRateLimit) AddTokens(uint) error { return nil }
func (noOpRateLimit) GetToken(context.Context, uint) (func() error, error) {
	return noOpToken, nil
}
func noOpToken() error { return nil }
