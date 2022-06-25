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
	"os"
	"path"
	"sort"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go/logging"
)

// S3FS is a FileService implementation backed by S3
type S3FS struct {
	config S3Config
	client *s3.Client
}

// key mapping scheme:
// <KeyPrefix>/<file path> -> file content

var _ FileService = new(S3FS)

func NewS3FS(config S3Config) (*S3FS, error) {
	u, err := url.Parse(config.Endpoint)
	if err != nil {
		return nil, err
	}
	if u.Scheme == "" {
		u.Scheme = "https"
	}
	endpoint := u.String()
	client := s3.New(s3.Options{
		Credentials: credentials.NewStaticCredentialsProvider(
			config.APIKey,
			config.APISecret,
			"",
		),
		EndpointResolver: s3.EndpointResolverFromURL(endpoint),
		Region:           config.Region,
		Logger:           logging.NewStandardLogger(os.Stdout),
	})

	return &S3FS{
		config: config,
		client: client,
	}, nil
}

func (m *S3FS) List(ctx context.Context, dirPath string) (entries []DirEntry, err error) {

	var cont *string
	for {
		output, err := m.client.ListObjectsV2(
			ctx,
			&s3.ListObjectsV2Input{
				Bucket:            ptrTo(m.config.Bucket),
				Delimiter:         ptrTo("/"),
				Prefix:            ptrTo(m.pathToKey(dirPath) + "/"),
				ContinuationToken: cont,
			},
		)
		if err != nil {
			return nil, err
		}

		for _, obj := range output.Contents {
			filePath := m.keyToPath(*obj.Key)
			filePath = strings.TrimRight(filePath, "/")
			_, name := path.Split(filePath)
			entries = append(entries, DirEntry{
				Name:  name,
				IsDir: false,
				Size:  int(obj.Size),
			})
		}

		for _, prefix := range output.CommonPrefixes {
			filePath := m.keyToPath(*prefix.Prefix)
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

func (m *S3FS) Write(ctx context.Context, vector IOVector) error {

	// check existence
	key := m.pathToKey(vector.FilePath)
	output, err := m.client.HeadObject(
		ctx,
		&s3.HeadObjectInput{
			Bucket: ptrTo(m.config.Bucket),
			Key:    ptrTo(key),
		},
	)
	err = m.mapError(err)
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
	_, err = m.client.PutObject(
		ctx,
		&s3.PutObjectInput{
			Bucket:        ptrTo(m.config.Bucket),
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

func (m *S3FS) Read(ctx context.Context, vector *IOVector) error {

	min, max := vector.offsetRange()
	readLen := max - min

	rang := fmt.Sprintf("bytes=%d-%d", min, max)
	output, err := m.client.GetObject(
		ctx,
		&s3.GetObjectInput{
			Bucket: ptrTo(m.config.Bucket),
			Key:    ptrTo(m.pathToKey(vector.FilePath)),
			Range:  ptrTo(rang),
		},
	)
	err = m.mapError(err)
	if err != nil {
		return err
	}
	defer output.Body.Close()

	content, err := io.ReadAll(io.LimitReader(output.Body, int64(readLen)))
	err = m.mapError(err)
	if err != nil {
		return err
	}

	for i, entry := range vector.Entries {
		start := entry.Offset - min
		if start >= len(content) {
			return ErrEmptyRange
		}
		end := start + entry.Size
		if end > len(content) {
			return ErrUnexpectedEOF
		}
		data := content[start:end]
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
			if len(entry.Data) < entry.Size {
				vector.Entries[i].Data = data
			} else {
				copy(entry.Data, data)
			}
		}
	}

	return nil
}

func (m *S3FS) Delete(ctx context.Context, filePath string) error {

	_, err := m.client.DeleteObject(
		ctx,
		&s3.DeleteObjectInput{
			Bucket: ptrTo(m.config.Bucket),
			Key:    ptrTo(m.pathToKey(filePath)),
		},
	)
	if err != nil {
		return err
	}

	return nil
}

func (m *S3FS) pathToKey(filePath string) string {
	return path.Join(m.config.KeyPrefix, filePath)
}

func (m *S3FS) keyToPath(key string) string {
	path := strings.TrimPrefix(key, m.config.KeyPrefix)
	path = strings.TrimLeft(path, "/")
	return path
}

func (m *S3FS) mapError(err error) error {
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
