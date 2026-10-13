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
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type publicationReader struct {
	io.Reader
	entered, release chan struct{}
	once             sync.Once
}

func (r *publicationReader) Read(p []byte) (int, error) {
	r.once.Do(func() { close(r.entered); <-r.release })
	return r.Reader.Read(p)
}
func publicationVector(path, data string) IOVector {
	return IOVector{FilePath: path, Entries: []IOEntry{{Data: []byte(data), Size: int64(len(data))}}}
}

// One-byte manifests and channel barriers force the old publisher to resume
// after the winner has published. No scheduler timing determines the oracle.
func TestWriteExclusiveDelayedPublisher(t *testing.T) {
	root := t.TempDir()
	fs, err := NewLocalETLFS("etl", root)
	require.NoError(t, err)
	defer fs.Close(context.Background())
	r := &publicationReader{Reader: bytes.NewBufferString("A"), entered: make(chan struct{}), release: make(chan struct{})}
	var release sync.Once
	defer release.Do(func() { close(r.release) })
	done := make(chan error, 1)
	go func() {
		done <- WriteExclusive(context.Background(), fs, IOVector{FilePath: "manifest.json", Entries: []IOEntry{{ReaderForWrite: r, Size: 1}}})
	}()
	<-r.entered
	require.NoError(t, WriteExclusive(context.Background(), fs, publicationVector("manifest.json", "B")))
	require.NoError(t, WriteExclusive(context.Background(), fs, publicationVector("READY", "")))
	release.Do(func() { close(r.release) })
	require.True(t, moerr.IsMoErrCode(<-done, moerr.ErrFileAlreadyExists))
	data, err := os.ReadFile(filepath.Join(root, "manifest.json"))
	require.NoError(t, err)
	require.Equal(t, "B", string(data))
	entries, err := os.ReadDir(root)
	require.NoError(t, err)
	require.Len(t, entries, 2)
	require.NoError(t, WriteExclusive(context.Background(), fs, publicationVector("other/manifest.json", "C")))
}

func TestWriteExclusiveFilesystemAliases(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "Destination")
	require.NoError(t, os.Mkdir(dir, 0755))
	alias := filepath.Join(root, "alias")
	require.NoError(t, os.Symlink(dir, alias))
	fs, err := NewLocalETLFS("etl", root)
	require.NoError(t, err)
	defer fs.Close(context.Background())
	upper, err := os.Stat(dir)
	require.NoError(t, err)
	lower, lowerErr := os.Stat(filepath.Join(root, "destination"))
	aliases := []string{"alias"}
	if lowerErr == nil && os.SameFile(upper, lower) {
		aliases = append(aliases, "destination")
	} else {
		require.True(t, os.IsNotExist(lowerErr))
		require.NoError(t, WriteExclusive(context.Background(), fs, publicationVector("destination/manifest.json", "independent")))
	}
	for index, alias := range aliases {
		name := fmt.Sprintf("manifest%d.json", index)
		r := &publicationReader{Reader: bytes.NewBufferString("loser"), entered: make(chan struct{}), release: make(chan struct{})}
		var release sync.Once
		func() {
			defer release.Do(func() { close(r.release) })
			done := make(chan error, 1)
			go func() {
				done <- WriteExclusive(context.Background(), fs, IOVector{FilePath: alias + "/" + name, Entries: []IOEntry{{ReaderForWrite: r, Size: 5}}})
			}()
			<-r.entered
			require.NoError(t, WriteExclusive(context.Background(), fs, publicationVector("Destination/"+name, "winner")))
			release.Do(func() { close(r.release) })
			require.True(t, moerr.IsMoErrCode(<-done, moerr.ErrFileAlreadyExists))
			data, err := os.ReadFile(filepath.Join(dir, name))
			require.NoError(t, err)
			require.Equal(t, "winner", string(data))
		}()
	}
	require.NoError(t, WriteExclusive(context.Background(), fs, publicationVector("Destination/manifest.json", "winner")))
	data, err := os.ReadFile(filepath.Join(dir, "manifest.json"))
	require.NoError(t, err)
	require.Equal(t, "winner", string(data))
}

func TestWriteExclusiveObjectStorage(t *testing.T) {
	for _, provider := range []string{"aws", "minio", "disk"} {
		t.Run(provider, func(t *testing.T) {
			ctx := context.WithValue(context.Background(), exclusiveWriteKey{}, true)
			var storage ObjectStorage
			var mu sync.Mutex
			var object []byte
			puts := 0
			if provider == "disk" {
				storage = &diskObjectStorage{path: t.TempDir()}
			} else {
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.Method == http.MethodGet && r.URL.Query().Has("location") {
						_, _ = io.WriteString(w, `<LocationConstraint>us-east-1</LocationConstraint>`)
						return
					}
					assert.Equal(t, http.MethodPut, r.Method)
					assert.False(t, r.URL.Query().Has("uploads"))
					assert.False(t, r.URL.Query().Has("uploadId"))
					assert.Equal(t, "*", r.Header.Get("If-None-Match"))
					var body io.Reader = r.Body
					if r.Header.Get("Content-Encoding") == "aws-chunked" || r.Header.Get("X-Amz-Decoded-Content-Length") != "" {
						body = httputil.NewChunkedReader(body)
					}
					data, err := io.ReadAll(body)
					assert.NoError(t, err)
					mu.Lock()
					defer mu.Unlock()
					puts++
					if object != nil {
						w.Header().Set("Content-Type", "application/xml")
						w.WriteHeader(http.StatusPreconditionFailed)
						_, _ = io.WriteString(w, `<Error><Code>PreconditionFailed</Code><Message>exists</Message></Error>`)
						return
					}
					object = data
					w.Header().Set("ETag", `"etag"`)
				}))
				defer server.Close()
				if provider == "aws" {
					awsStorage := newTestAWSClient(t, server)
					// The client uses the httptest transport, while this field
					// identifies the production endpoint capability being tested.
					awsStorage.endpoint = "https://s3.us-east-1.amazonaws.com"
					storage = awsStorage
				} else {
					endpoint, err := url.Parse(server.URL)
					require.NoError(t, err)
					client, err := minio.New(endpoint.Host, &minio.Options{Creds: credentials.NewStaticV4("id", "secret", ""), Region: "us-east-1", Secure: false, Transport: server.Client().Transport})
					require.NoError(t, err)
					storage = &MinioSDK{bucket: "bucket", client: client}
				}
			}
			fs := &S3FS{name: "etl", storage: storage, rawStorage: storage, parallelMode: ParallelForce}
			require.NoError(t, WriteExclusive(ctx, fs, publicationVector("manifest.json", "B")))
			require.True(t, moerr.IsMoErrCode(WriteExclusive(ctx, fs, publicationVector("manifest.json", "A")), moerr.ErrFileAlreadyExists))
			if provider == "disk" {
				data, err := os.ReadFile(filepath.Join(storage.(*diskObjectStorage).path, "manifest.json"))
				require.NoError(t, err)
				require.Equal(t, "B", string(data))
			} else {
				mu.Lock()
				require.Equal(t, []byte("B"), object)
				require.Equal(t, 2, puts)
				mu.Unlock()
			}
		})
	}
}

func TestWriteExclusiveRejectsUnsupported(t *testing.T) {
	require.Error(t, CheckExclusiveWrite(&S3FS{rawStorage: &AliyunSDK{}}))
	require.Error(t, CheckExclusiveWrite(&S3FS{rawStorage: &QCloudSDK{}}))
	require.Error(t, CheckExclusiveWrite(&S3FS{rawStorage: &HDFS{}}))
	for _, endpoint := range []string{
		"cos.ap-guangzhou.myqcloud.com",
		"oss-cn-shenzhen.aliyuncs.com",
		"tos-cn-shanghai.ctyunapi.cn",
	} {
		require.Error(t, CheckExclusiveWrite(&S3FS{rawStorage: &MinioSDK{endpoint: endpoint}}), endpoint)
	}
	fs, err := NewLocalETLFS("etl", t.TempDir())
	require.NoError(t, err)
	defer fs.Close(context.Background())
	require.Error(t, WriteExclusive(context.Background(), fs, IOVector{FilePath: "large", Entries: []IOEntry{{Size: smallObjectThreshold + 1}}}))
	require.Error(t, WriteExclusive(context.Background(), fs, IOVector{FilePath: "unknown", Entries: []IOEntry{{Size: -1}}}))
}

func TestCheckExclusiveWriteAWSEndpoints(t *testing.T) {
	for _, tc := range []struct {
		endpoint string
		allowed  bool
	}{
		{endpoint: "", allowed: true}, // AWS SDK default endpoint.
		{endpoint: "s3.us-east-1.amazonaws.com", allowed: true},
		{endpoint: "https://bucket.s3.cn-north-1.amazonaws.com.cn", allowed: true},
		{endpoint: "https://s3.example.com", allowed: false},
		{endpoint: "http://localhost:9000", allowed: false},
		{endpoint: "https://s3.amazonaws.com.example.com", allowed: false},
	} {
		err := CheckExclusiveWrite(&S3FS{rawStorage: &AwsSDKv2{endpoint: tc.endpoint}})
		if tc.allowed {
			require.NoError(t, err, tc.endpoint)
		} else {
			require.Error(t, err, tc.endpoint)
		}
	}
}

func TestWriteExclusiveCancellation(t *testing.T) {
	root := t.TempDir()
	fs, err := NewLocalETLFS("etl", root)
	require.NoError(t, err)
	defer fs.Close(context.Background())
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, WriteExclusive(ctx, fs, publicationVector("manifest.json", "A")), context.Canceled)
	entries, err := os.ReadDir(root)
	require.NoError(t, err)
	require.Empty(t, entries)
	require.NoError(t, WriteExclusive(context.Background(), fs, publicationVector("manifest.json", "B")))
}
