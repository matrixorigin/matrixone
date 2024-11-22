// Copyright 2024 Matrix Origin
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
	"io"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/stretchr/testify/assert"
)

func testObjectStorage[T ObjectStorage](
	t *testing.T,
	sdkName string,
	newStorage func(t *testing.T) T,
) {

	t.Run(sdkName, func(t *testing.T) {

		t.Run("basic", func(t *testing.T) {
			storage := newStorage(t)
			ctx := context.Background()

			prefix := time.Now().Format("2006-01-02-15-04-05.000000")
			name := path.Join(prefix, "foo")

			// write
			err := storage.Write(ctx, name, bytes.NewReader([]byte("foo")), ptrTo[int64](3), nil)
			assert.Nil(t, err)

			// list
			n := 0
			for entry, err := range storage.List(ctx, prefix+"/") {
				assert.Nil(t, err)
				n++
				assert.Equal(t, false, entry.IsDir)
				assert.Equal(t, name, entry.Name)
				assert.Equal(t, int64(3), entry.Size)
			}
			assert.Equal(t, 1, n)

			// stat
			size, err := storage.Stat(ctx, name)
			assert.Nil(t, err)
			assert.Equal(t, int64(3), size)

			// exists
			exists, err := storage.Exists(ctx, name)
			assert.Nil(t, err)
			assert.True(t, exists)
			exists, err = storage.Exists(ctx, "bar")
			assert.Nil(t, err)
			assert.False(t, exists)

			// read
			r, err := storage.Read(ctx, name, nil, nil)
			assert.Nil(t, err)
			content, err := io.ReadAll(r)
			assert.Nil(t, err)
			assert.Equal(t, []byte("foo"), content)
			assert.Nil(t, r.Close())

			// delete
			err = storage.Delete(ctx, name)
			assert.Nil(t, err)
			err = storage.Delete(ctx, name, name)
			assert.Nil(t, err)

			// file not found
			_, err = storage.Stat(ctx, "filenotexists")
			assert.True(t, moerr.IsMoErrCode(err, moerr.ErrFileNotFound))
			_, err = storage.Read(ctx, "filenotexists", nil, nil)
			assert.True(t, moerr.IsMoErrCode(err, moerr.ErrFileNotFound))

			// sub dirs
			err = storage.Write(ctx, "a/1/1", bytes.NewReader([]byte{'a'}), ptrTo[int64](1), nil)
			assert.Nil(t, err)
			err = storage.Write(ctx, "a/1/2", bytes.NewReader([]byte{'a'}), nil, nil)
			assert.Nil(t, err)
			err = storage.Write(ctx, "a/2", bytes.NewReader([]byte{'a'}), ptrTo[int64](1), nil)
			assert.Nil(t, err)

			// set list max entries
			switch s := any(storage).(type) {
			case *AliyunSDK:
				s.listMaxKeys = 1
			case *MinioSDK:
				s.listMaxKeys = 1
			case *QCloudSDK:
				s.listMaxKeys = 1
			case *AwsSDKv2:
				s.listMaxKeys = 1
			}

			// list dir
			n = 0
			for _, err := range storage.List(ctx, "a/1") {
				assert.Nil(t, err)
				n++
			}
			assert.Equal(t, 1, n) // a/1/

			// list files
			n = 0
			for _, err := range storage.List(ctx, "a/1/") {
				assert.Nil(t, err)
				n++
			}
			assert.Equal(t, 2, n)

			// list mixed
			n = 0
			for _, err := range storage.List(ctx, "a/") {
				assert.Nil(t, err)
				n++
			}
			assert.Equal(t, 2, n)

			// early break
			for entry, err := range storage.List(ctx, "a/") {
				assert.Nil(t, err)
				if entry.IsDir {
					break
				}
			}
			for entry, err := range storage.List(ctx, "a/") {
				assert.Nil(t, err)
				if !entry.IsDir {
					break
				}
			}

			// list empty
			for range storage.List(ctx, "notexistsd") {
				t.Fatal()
			}

		})

		t.Run("invalid write length", func(t *testing.T) {
			storage := newStorage(t)
			ctx := context.Background()
			prefix := time.Now().Format("2006-01-02-15-04-05.000000")

			name := path.Join(prefix, "foo")

			err := storage.Write(ctx, name, bytes.NewReader([]byte("")), ptrTo[int64](1), nil)
			if !moerr.IsMoErrCode(err, moerr.ErrSizeNotMatch) {
				t.Fatalf("got %v", err)
			}

			err = storage.Write(ctx, name, bytes.NewReader([]byte("a")), ptrTo[int64](2), nil)
			if !moerr.IsMoErrCode(err, moerr.ErrSizeNotMatch) {
				t.Fatalf("got %v", err)
			}

			err = storage.Write(ctx, name, bytes.NewReader([]byte("ab")), ptrTo[int64](1), nil)
			if !moerr.IsMoErrCode(err, moerr.ErrSizeNotMatch) {
				t.Fatalf("got %v", err)
			}

		})

		t.Run("write empty", func(t *testing.T) {
			storage := newStorage(t)
			ctx := context.Background()
			prefix := time.Now().Format("2006-01-02-15-04-05.000000")

			name := path.Join(prefix, "foo")

			reader := newIOEntriesReader(ctx, []IOEntry{
				{
					Data: []byte{},
					Size: 0,
				},
			})
			err := storage.Write(ctx, name, reader, ptrTo[int64](0), nil)
			assert.Nil(t, err)

		})

	})
}

func TestObjectStorages(t *testing.T) {
	for _, args := range objectStorageArgumentsForTest("test", t) {

		t.Run(args.Name, func(t *testing.T) {

			switch {

			case args.Endpoint == "disk":
				// disk
				testObjectStorage(t, "disk", func(t *testing.T) *diskObjectStorage {
					storage, err := newDiskObjectStorage(context.Background(), args, nil)
					if err != nil {
						t.Fatal(err)
					}
					return storage
				})

			case strings.Contains(args.Endpoint, "aliyun"):
				// aliyun
				testObjectStorage(t, "aliyun", func(t *testing.T) *AliyunSDK {
					storage, err := NewAliyunSDK(context.Background(), args, nil)
					if err != nil {
						t.Fatal(err)
					}
					return storage
				})
				if args.RoleARN == "" {
					testObjectStorage(t, "minio", func(t *testing.T) *MinioSDK {
						storage, err := NewMinioSDK(context.Background(), args, nil)
						if err != nil {
							t.Fatal(err)
						}
						return storage
					})
					testObjectStorage(t, "aws", func(t *testing.T) *AwsSDKv2 {
						storage, err := NewAwsSDKv2(context.Background(), args, nil)
						if err != nil {
							t.Fatal(err)
						}
						return storage
					})
				}

			case strings.Contains(args.Endpoint, "qcloud"):
				// qcloud
				testObjectStorage(t, "aws", func(t *testing.T) *AwsSDKv2 {
					storage, err := NewAwsSDKv2(context.Background(), args, nil)
					if err != nil {
						t.Fatal(err)
					}
					return storage
				})
				testObjectStorage(t, "minio", func(t *testing.T) *MinioSDK {
					storage, err := NewMinioSDK(context.Background(), args, nil)
					if err != nil {
						t.Fatal(err)
					}
					return storage
				})
				testObjectStorage(t, "qcloud", func(t *testing.T) *QCloudSDK {
					storage, err := NewQCloudSDK(context.Background(), args, nil)
					if err != nil {
						t.Fatal(err)
					}
					return storage
				})

			case strings.Contains(args.Endpoint, "aws"):
				// AWS
				testObjectStorage(t, "aws", func(t *testing.T) *AwsSDKv2 {
					storage, err := NewAwsSDKv2(context.Background(), args, nil)
					if err != nil {
						t.Fatal(err)
					}
					return storage
				})
				testObjectStorage(t, "minio", func(t *testing.T) *MinioSDK {
					storage, err := NewMinioSDK(context.Background(), args, nil)
					if err != nil {
						t.Fatal(err)
					}
					return storage
				})

			case strings.Contains(args.Endpoint, "qiniu"):
				// qiniu
				testObjectStorage(t, "minio", func(t *testing.T) *MinioSDK {
					storage, err := NewMinioSDK(context.Background(), args, nil)
					if err != nil {
						t.Fatal(err)
					}
					return storage
				})
				testObjectStorage(t, "aws", func(t *testing.T) *AwsSDKv2 {
					storage, err := NewAwsSDKv2(context.Background(), args, nil)
					if err != nil {
						t.Fatal(err)
					}
					return storage
				})

			default:
				t.Fatalf("not handle spec: %+v", args)

			}

		})
	}
}
