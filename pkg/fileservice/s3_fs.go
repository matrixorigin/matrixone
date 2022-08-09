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
	"net/url"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// S3FS is a FileService implementation backed by S3
type S3FS struct {
	client    *s3.Client
	bucket    string
	keyPrefix string

	memCache *MemCache
}

// key mapping scheme:
// <KeyPrefix>/<file path> -> file content

var _ FileService = new(S3FS)

func NewS3FS(
	endpoint string,
	bucket string,
	keyPrefix string,
	memCacheCapacity int,
) (*S3FS, error) {

	u, err := url.Parse(endpoint)
	if err != nil {
		return nil, err
	}
	if u.Scheme == "" {
		u.Scheme = "https"
	}
	endpoint = u.String()

	return newS3FS(
		endpoint,
		bucket,
		keyPrefix,
		memCacheCapacity,
		s3.WithEndpointResolver(
			s3.EndpointResolverFromURL(endpoint),
		),
	)
}

// NewS3FSOnMinio creates S3FS on minio server
// this is needed because the URL scheme of minio server does not compatible with AWS'
func NewS3FSOnMinio(
	endpoint string,
	bucket string,
	keyPrefix string,
	memCacheCapacity int,
) (*S3FS, error) {

	u, err := url.Parse(endpoint)
	if err != nil {
		return nil, err
	}
	if u.Scheme == "" {
		u.Scheme = "https"
	}
	endpoint = u.String()

	return newS3FS(
		endpoint,
		bucket,
		keyPrefix,
		memCacheCapacity,
		s3.WithEndpointResolver(
			s3.EndpointResolverFunc(
				func(
					region string,
					options s3.EndpointResolverOptions,
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
}

func newS3FS(
	endpoint string,
	bucket string,
	keyPrefix string,
	memCacheCapacity int,
	options ...func(*s3.Options),
) (*S3FS, error) {

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*7)
	defer cancel()
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, err
	}

	client := s3.NewFromConfig(
		cfg,
		options...,
	)

	fs := &S3FS{
		client:    client,
		bucket:    bucket,
		keyPrefix: keyPrefix,
	}
	if memCacheCapacity > 0 {
		fs.memCache = NewMemCache(memCacheCapacity)
	}

	return fs, nil
}

func (s *S3FS) List(ctx context.Context, dirPath string) (entries []DirEntry, err error) {

	var cont *string
	prefix := s.pathToKey(dirPath)
	if prefix != "" {
		prefix += "/"
	}
	for {
		output, err := s.client.ListObjectsV2(
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
			_, name := path.Split(filePath)
			entries = append(entries, DirEntry{
				Name:  name,
				IsDir: false,
				Size:  int(obj.Size),
			})
		}

		for _, prefix := range output.CommonPrefixes {
			filePath := s.keyToPath(*prefix.Prefix)
			filePath = strings.TrimRight(filePath, "/")
			entries = append(entries, DirEntry{
				Name:  filePath,
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

func (s *S3FS) Write(ctx context.Context, vector IOVector) error {

	// check existence
	key := s.pathToKey(vector.FilePath)
	output, err := s.client.HeadObject(
		ctx,
		&s3.HeadObjectInput{
			Bucket: ptrTo(s.bucket),
			Key:    ptrTo(key),
		},
	)
	err = s.mapError(err)
	if errors.Is(err, ErrFileNotFound) {
		// key not exists
		err = nil
	}
	if err != nil {
		// other error
		return err
	}
	if output != nil {
		// key existed
		return ErrFileExisted
	}

	return s.write(ctx, vector)
}

func (s *S3FS) write(ctx context.Context, vector IOVector) error {
	key := s.pathToKey(vector.FilePath)

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
	content, err := io.ReadAll(newIOEntriesReader(vector.Entries))
	if err != nil {
		return err
	}
	_, err = s.client.PutObject(
		ctx,
		&s3.PutObjectInput{
			Bucket:        ptrTo(s.bucket),
			Key:           ptrTo(key),
			Body:          bytes.NewReader(content),
			ContentLength: size,
		},
	)
	if err != nil {
		return err
	}

	return nil
}

func (s *S3FS) Read(ctx context.Context, vector *IOVector) error {

	if len(vector.Entries) == 0 {
		return ErrEmptyVector
	}

	if s.memCache == nil {
		// no cache
		return s.read(ctx, vector)
	}

	if err := s.memCache.Read(ctx, vector, s.read); err != nil {
		return err
	}

	return nil
}

func (s *S3FS) read(ctx context.Context, vector *IOVector) error {

	min, max, readToEnd := vector.offsetRange()

	var content []byte

	if readToEnd {
		rang := fmt.Sprintf("bytes=%d-", min)
		output, err := s.client.GetObject(
			ctx,
			&s3.GetObjectInput{
				Bucket: ptrTo(s.bucket),
				Key:    ptrTo(s.pathToKey(vector.FilePath)),
				Range:  ptrTo(rang),
			},
		)
		err = s.mapError(err)
		if err != nil {
			return err
		}
		defer output.Body.Close()
		content, err = io.ReadAll(output.Body)
		err = s.mapError(err)
		if err != nil {
			return err
		}

	} else {
		rang := fmt.Sprintf("bytes=%d-%d", min, max)
		output, err := s.client.GetObject(
			ctx,
			&s3.GetObjectInput{
				Bucket: ptrTo(s.bucket),
				Key:    ptrTo(s.pathToKey(vector.FilePath)),
				Range:  ptrTo(rang),
			},
		)
		err = s.mapError(err)
		if err != nil {
			return err
		}
		defer output.Body.Close()
		content, err = io.ReadAll(io.LimitReader(output.Body, int64(max-min)))
		err = s.mapError(err)
		if err != nil {
			return err
		}
	}

	for i, entry := range vector.Entries {
		if entry.ignore {
			continue
		}

		start := entry.Offset - min
		if start >= len(content) {
			return ErrEmptyRange
		}

		var data []byte
		if entry.Size < 0 {
			// read to end
			data = content[start:]
		} else {
			end := start + entry.Size
			if end > len(content) {
				return ErrUnexpectedEOF
			}
			data = content[start:end]
		}
		if len(data) == 0 {
			return ErrEmptyRange
		}

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
			if len(entry.Data) < entry.Size || entry.Size < 0 {
				entry.Data = data
			} else {
				copy(entry.Data, data)
			}
		}

		if err := entry.setObjectFromData(); err != nil {
			return err
		}

		vector.Entries[i] = entry
	}

	return nil
}

func (s *S3FS) Delete(ctx context.Context, filePath string) error {

	_, err := s.client.DeleteObject(
		ctx,
		&s3.DeleteObjectInput{
			Bucket: ptrTo(s.bucket),
			Key:    ptrTo(s.pathToKey(filePath)),
		},
	)
	if err != nil {
		return err
	}

	return nil
}

func (s *S3FS) pathToKey(filePath string) string {
	return path.Join(s.keyPrefix, filePath)
}

func (s *S3FS) keyToPath(key string) string {
	path := strings.TrimPrefix(key, s.keyPrefix)
	path = strings.TrimLeft(path, "/")
	return path
}

func (s *S3FS) mapError(err error) error {
	if err == nil {
		return nil
	}
	var httpError *http.ResponseError
	if errors.As(err, &httpError) {
		if httpError.Response.StatusCode == 404 {
			return ErrFileNotFound
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

func (s *S3FS) CacheStats() *CacheStats {
	if s.memCache != nil {
		return s.memCache.CacheStats()
	}
	return nil
}
