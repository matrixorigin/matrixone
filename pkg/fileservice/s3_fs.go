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
	"io"
	"math"
	stdhttp "net/http"
	"net/url"
	pathpkg "path"
	"sort"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"go.uber.org/zap"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

// S3FS is a FileService implementation backed by S3
type S3FS struct {
	name      string
	s3Client  *s3.Client
	bucket    string
	keyPrefix string

	memCache    *MemCache
	diskCache   *DiskCache
	asyncUpdate bool

	perfCounterSets []*perfcounter.CounterSet
}

// key mapping scheme:
// <KeyPrefix>/<file path> -> file content

var _ FileService = new(S3FS)

func NewS3FS(
	sharedConfigProfile string,
	name string,
	endpoint string,
	bucket string,
	keyPrefix string,
	memCacheCapacity int64,
	diskCacheCapacity int64,
	diskCachePath string,
	perfCounterSets []*perfcounter.CounterSet,
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

	if err := fs.initCaches(
		memCacheCapacity,
		diskCacheCapacity,
		diskCachePath,
	); err != nil {
		return nil, err
	}

	return fs, nil
}

// NewS3FSOnMinio creates S3FS on minio server
// this is needed because the URL scheme of minio server does not compatible with AWS'
func NewS3FSOnMinio(
	sharedConfigProfile string,
	name string,
	endpoint string,
	bucket string,
	keyPrefix string,
	memCacheCapacity int64,
	diskCacheCapacity int64,
	diskCachePath string,
	perfCounterSets []*perfcounter.CounterSet,
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

	if err := fs.initCaches(
		memCacheCapacity,
		diskCacheCapacity,
		diskCachePath,
	); err != nil {
		return nil, err
	}

	return fs, nil
}

func (s *S3FS) initCaches(
	memCacheCapacity int64,
	diskCacheCapacity int64,
	diskCachePath string,
) error {

	// memory cache
	if memCacheCapacity == 0 {
		memCacheCapacity = 512 << 20
	}
	if memCacheCapacity > 0 {
		s.memCache = NewMemCache(
			WithLRU(memCacheCapacity),
			WithPerfCounterSets(s.perfCounterSets),
		)
		logutil.Info("fileservice: mem cache initialized", zap.Any("fs-name", s.name), zap.Any("capacity", memCacheCapacity))
	}

	// disk cache
	if diskCacheCapacity == 0 {
		diskCacheCapacity = 8 << 30
	}
	if diskCacheCapacity > 0 && diskCachePath != "" {
		var err error
		s.diskCache, err = NewDiskCache(
			diskCachePath,
			diskCacheCapacity,
			s.perfCounterSets,
		)
		if err != nil {
			return err
		}
		logutil.Info("fileservice: disk cache initialized", zap.Any("fs-name", s.name), zap.Any("capacity", diskCacheCapacity))
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
	var cont *string

	for {
		output, err := s.s3ListObjects(
			ctx,
			&s3.ListObjectsV2Input{
				Bucket:            ptrTo(s.bucket),
				Delimiter:         ptrTo("/"),
				Prefix:            ptrTo(prefix),
				ContinuationToken: cont,
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

		if output.ContinuationToken == nil ||
			*output.ContinuationToken == "" {
			break
		}
		cont = output.ContinuationToken
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

func (s *S3FS) write(ctx context.Context, vector IOVector) error {
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

	// put
	content, err := io.ReadAll(newIOEntriesReader(ctx, vector.Entries))
	if err != nil {
		return err
	}
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
			err = s.diskCache.Update(ctx, vector, s.asyncUpdate)
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
		if readToEnd {
			rang := fmt.Sprintf("bytes=%d-", min)
			output, err := s.s3GetObject(
				ctx,
				&s3.GetObjectInput{
					Bucket: ptrTo(s.bucket),
					Key:    ptrTo(key),
					Range:  ptrTo(rang),
				},
			)
			err = s.mapError(err, key)
			if err != nil {
				return nil, err
			}
			return output.Body, nil
		}

		rang := fmt.Sprintf("bytes=%d-%d", min, max)
		output, err := s.s3GetObject(
			ctx,
			&s3.GetObjectInput{
				Bucket: ptrTo(s.bucket),
				Key:    ptrTo(key),
				Range:  ptrTo(rang),
			},
		)
		err = s.mapError(err, key)
		if err != nil {
			return nil, err
		}
		return &readCloser{
			r:         io.LimitReader(output.Body, int64(max-min)),
			closeFunc: output.Body.Close,
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
				_, err = io.Copy(w, reader)
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

		// set Object field
		if err := entry.setObjectFromData(); err != nil {
			return err
		}

		vector.Entries[i] = entry
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

	if endpoint != "" {
		u, err := url.Parse(endpoint)
		if err != nil {
			return nil, err
		}
		if u.Scheme == "" {
			u.Scheme = "https"
		}
		endpoint = u.String()
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	if region == "" {
		// try to get region from bucket
		resp, err := stdhttp.Head("https://" + bucket + ".s3.amazonaws.com")
		if err == nil {
			if value := resp.Header.Get("x-amz-bucket-region"); value != "" {
				region = value
			}
		}
	}

	var credentialProvider aws.CredentialsProvider

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
	if sharedConfigProfile != "" {
		loadConfigOptions = append(loadConfigOptions,
			config.WithSharedConfigProfile(sharedConfigProfile),
		)
	}

	if apiKey != "" && apiSecret != "" {
		// static
		credentialProvider = credentials.NewStaticCredentialsProvider(apiKey, apiSecret, "")
	}

	if roleARN != "" {
		// role arn
		awsConfig, err := config.LoadDefaultConfig(ctx, loadConfigOptions...)
		if err != nil {
			return nil, err
		}

		stsSvc := sts.NewFromConfig(awsConfig, func(options *sts.Options) {
			if region == "" {
				options.Region = "ap-northeast-1"
			} else {
				options.Region = region
			}
		})
		credentialProvider = stscreds.NewAssumeRoleProvider(
			stsSvc,
			roleARN,
			func(opts *stscreds.AssumeRoleOptions) {
				if externalID != "" {
					opts.ExternalID = &externalID
				}
			},
		)
		// validate
		_, err = credentialProvider.Retrieve(ctx)
		if err != nil {
			return nil, err
		}
	}

	if credentialProvider != nil {
		credentialProvider = aws.NewCredentialsCache(credentialProvider)
	}

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

	s3Options := []func(*s3.Options){}

	if credentialProvider != nil {
		s3Options = append(s3Options,
			func(opt *s3.Options) {
				opt.Credentials = credentialProvider
			},
		)
	}

	if endpoint != "" {
		if isMinio != "" {
			// for minio
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

	if region != "" {
		s3Options = append(s3Options,
			func(opt *s3.Options) {
				opt.Region = region
			},
		)
	}

	client := s3.NewFromConfig(
		config,
		s3Options...,
	)

	_, err = client.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: ptrTo(bucket),
	})
	if err != nil {
		return nil, moerr.NewInternalErrorNoCtx("bad s3 config: %v", err)
	}

	fs := &S3FS{
		name:        name,
		s3Client:    client,
		bucket:      bucket,
		keyPrefix:   prefix,
		asyncUpdate: true,
	}

	return fs, nil

}

func (s *S3FS) s3ListObjects(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	FSProfileHandler.AddSample()
	perfcounter.Update(ctx, func(counter *perfcounter.CounterSet) {
		counter.S3.List.Add(1)
	}, s.perfCounterSets...)
	return s.s3Client.ListObjectsV2(ctx, params, optFns...)
}

func (s *S3FS) s3HeadObject(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	FSProfileHandler.AddSample()
	perfcounter.Update(ctx, func(counter *perfcounter.CounterSet) {
		counter.S3.Head.Add(1)
	}, s.perfCounterSets...)
	return s.s3Client.HeadObject(ctx, params, optFns...)
}

func (s *S3FS) s3PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	FSProfileHandler.AddSample()
	perfcounter.Update(ctx, func(counter *perfcounter.CounterSet) {
		counter.S3.Put.Add(1)
	}, s.perfCounterSets...)
	return s.s3Client.PutObject(ctx, params, optFns...)
}

func (s *S3FS) s3GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	FSProfileHandler.AddSample()
	perfcounter.Update(ctx, func(counter *perfcounter.CounterSet) {
		counter.S3.Get.Add(1)
	}, s.perfCounterSets...)
	return s.s3Client.GetObject(ctx, params, optFns...)
}

func (s *S3FS) s3DeleteObjects(ctx context.Context, params *s3.DeleteObjectsInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectsOutput, error) {
	FSProfileHandler.AddSample()
	perfcounter.Update(ctx, func(counter *perfcounter.CounterSet) {
		counter.S3.DeleteMulti.Add(1)
	}, s.perfCounterSets...)
	return s.s3Client.DeleteObjects(ctx, params, optFns...)
}

func (s *S3FS) s3DeleteObject(ctx context.Context, params *s3.DeleteObjectInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	FSProfileHandler.AddSample()
	perfcounter.Update(ctx, func(counter *perfcounter.CounterSet) {
		counter.S3.Delete.Add(1)
	}, s.perfCounterSets...)
	return s.s3Client.DeleteObject(ctx, params, optFns...)
}
