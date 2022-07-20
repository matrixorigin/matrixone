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

// S3FSMinio is a FileService implementation backed by S3, using minio client
type S3FSMinio struct {
	config S3Config
	client *minio.Client
}

var _ FileService = new(S3FSMinio)

func NewS3FSMinio(config S3Config) (*S3FSMinio, error) {
	client, err := minio.New(config.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(config.APIKey, config.APISecret, ""),
		Secure: true,
	})
	if err != nil {
		return nil, err
	}

	return &S3FSMinio{
		config: config,
		client: client,
	}, nil
}

func (m *S3FSMinio) List(ctx context.Context, dirPath string) (entries []DirEntry, err error) {

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
			Size:  int(info.Size),
		})
	}

	return
}

func (m *S3FSMinio) Write(ctx context.Context, vector IOVector) error {

	// check existence
	key := m.pathToKey(vector.FilePath)
	//TODO use HEAD
	obj, err := m.client.GetObject(
		ctx,
		m.config.Bucket,
		key,
		minio.GetObjectOptions{},
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
	if obj != nil {
		_, err = obj.Stat()
		err = m.mapError(err)
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

	return m.write(ctx, vector)
}

func (m *S3FSMinio) write(ctx context.Context, vector IOVector) error {

	key := m.pathToKey(vector.FilePath)

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
	_, err := m.client.PutObject(
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

func (m *S3FSMinio) Read(ctx context.Context, vector *IOVector) error {

	min, max, readToEnd := vector.offsetRange()

	var content []byte

	if readToEnd {
		obj, err := m.client.GetObject(
			ctx,
			m.config.Bucket,
			m.pathToKey(vector.FilePath),
			minio.GetObjectOptions{},
		)
		err = m.mapError(err)
		if err != nil {
			return err
		}
		defer obj.Close()
		_, err = obj.Seek(int64(min), io.SeekStart)
		err = m.mapError(err)
		if err != nil {
			return err
		}
		content, err = io.ReadAll(obj)
		err = m.mapError(err)
		if err != nil {
			return err
		}

	} else {
		obj, err := m.client.GetObject(
			ctx,
			m.config.Bucket,
			m.pathToKey(vector.FilePath),
			minio.GetObjectOptions{},
		)
		err = m.mapError(err)
		if err != nil {
			return err
		}
		defer obj.Close()
		_, err = obj.Seek(int64(min), io.SeekStart)
		err = m.mapError(err)
		if err != nil {
			return err
		}
		content, err = io.ReadAll(io.LimitReader(obj, int64(max-min)))
		err = m.mapError(err)
		if err != nil {
			return err
		}
	}

	for i, entry := range vector.Entries {
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
				vector.Entries[i].Data = data
			} else {
				copy(entry.Data, data)
			}
		}
	}

	return nil
}

func (m *S3FSMinio) Delete(ctx context.Context, filePath string) error {

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

func (m *S3FSMinio) pathToKey(filePath string) string {
	return path.Join(m.config.KeyPrefix, filePath)
}

func (m *S3FSMinio) keyToPath(key string) string {
	path := strings.TrimPrefix(key, m.config.KeyPrefix)
	path = strings.TrimLeft(path, "/")
	return path
}

func (m *S3FSMinio) mapError(err error) error {
	if err == nil {
		return err
	}
	return m._mapS3Error(err)
}

func (m *S3FSMinio) _mapS3Error(err error) error {
	resp, ok := err.(minio.ErrorResponse)
	if ok {
		if resp.Code == "NoSuchKey" {
			err = ErrFileNotFound
		}
	}
	return err
}
