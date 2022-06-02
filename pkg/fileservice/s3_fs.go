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
	"path"
	"sort"
	"strings"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// S3FS is a FileService implementation backed by S3
type S3FS struct {
	config S3Config
	client *minio.Client
}

type S3Config struct {
	Endpoint  string
	APIKey    string
	APISecret string
	Bucket    string
	// KeyPrefix enables multiple fs instances in one bucket
	KeyPrefix string
}

// key mapping scheme:
// <KeyPrefix>/<file path> -> file content

var _ FileService = new(S3FS)

func NewS3FS(config S3Config) (*S3FS, error) {
	client, err := minio.New(config.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(config.APIKey, config.APISecret, ""),
		Secure: true,
	})
	if err != nil {
		return nil, err
	}

	return &S3FS{
		config: config,
		client: client,
	}, nil
}

func (m *S3FS) List(ctx context.Context, dirPath string) (entries []DirEntry, err error) {

	for info := range m.client.ListObjects(
		ctx,
		m.config.Bucket,
		minio.ListObjectsOptions{
			Prefix: m.pathToKey(dirPath) + "/",
		},
	) {
		filePath := m.keyToPath(info.Key)
		isDir := strings.HasSuffix(filePath, "/")
		filePath = strings.TrimRight(filePath, "/")
		_, name := path.Split(filePath)
		entries = append(entries, DirEntry{
			Name:  name,
			IsDir: isDir,
		})
	}

	return
}

func (m *S3FS) Write(ctx context.Context, vector IOVector) error {

	// check existence
	key := m.pathToKey(vector.FilePath)
	obj, err := m.client.GetObject(
		ctx,
		m.config.Bucket,
		key,
		minio.GetObjectOptions{},
	)
	err = mapS3Error(err)
	if errors.Is(err, ErrFileNotFound) {
		// key not exists
		err = nil
	}
	if err != nil {
		// other error
		return err
	}
	if obj != nil {
		_, err = obj.Stat()
		err = mapS3Error(err)
		if errors.Is(err, ErrFileNotFound) {
			// key not exists
		} else if err != nil {
			// othter error
			return err
		} else {
			// stat ok, key existed
			return ErrFileExisted
		}
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
	_, err = m.client.PutObject(
		ctx,
		m.config.Bucket,
		key,
		newIOEntriesReader(vector.Entries),
		size,
		minio.PutObjectOptions{},
	)
	if err != nil {
		return err
	}

	return nil
}

func (m *S3FS) Read(ctx context.Context, vector *IOVector) error {

	min, max := vector.offsetRange()
	readLen := max - min

	obj, err := m.client.GetObject(
		ctx,
		m.config.Bucket,
		m.pathToKey(vector.FilePath),
		minio.GetObjectOptions{},
	)
	err = mapS3Error(err)
	if err != nil {
		return err
	}
	defer obj.Close()
	_, err = obj.Seek(int64(min), io.SeekStart)
	err = mapS3Error(err)
	if err != nil {
		return err
	}
	content, err := io.ReadAll(io.LimitReader(obj, int64(readLen)))
	err = mapS3Error(err)
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
			vector.Entries[i].Data = data
		}
	}

	return nil
}

func (m *S3FS) Delete(ctx context.Context, filePath string) error {

	err := m.client.RemoveObject(
		ctx,
		m.config.Bucket,
		m.pathToKey(filePath),
		minio.RemoveObjectOptions{},
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

func mapS3Error(err error) error {
	if err == nil {
		return err
	}
	return _mapS3Error(err)
}

func _mapS3Error(err error) error {
	resp, ok := err.(minio.ErrorResponse)
	if ok {
		if resp.Code == "NoSuchKey" {
			err = ErrFileNotFound
		}
	}
	return err
}
